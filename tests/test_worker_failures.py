"""Failure paths preserve results or reconciliation evidence before releasing ownership."""
import asyncio
import json
import logging
from datetime import timedelta
from itertools import pairwise
from pathlib import Path
from sqlite3 import SQLITE_BUSY
from sqlite3 import OperationalError as SQLiteError
from threading import Event
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from conftest import runtime_client_for
from sqlalchemy import select, update
from sqlalchemy.exc import OperationalError

from elephant.domain.messages import AccessContext, ConversationRef, InboundEvent, PrincipalRef, Surface
from elephant.harness.gateway import RunGateway
from elephant.harness.pi import PiRunnerAdapter
from elephant.harness.pool import SessionPool
from elephant.jobs.service import TaskService
from elephant.jobs.worker import Spool, Worker
from elephant.outbox.service import Outbox
from elephant.storage.schema import outbox
from elephant.storage.task_schema import processes, runs, tasks


@pytest.fixture
def environment(db, clock):
    service = TaskService(db, clock, Outbox(db, clock))
    conversation = ConversationRef(deployment_id="d", adapter_instance_id="a", bot_identity_id="b",
                                   surface=Surface.GROUP, remote_conversation_id="g")
    principal = PrincipalRef(adapter_instance_id="a", identity_namespace="qq", remote_user_id="u")
    event = InboundEvent(event_id="source", event_type="message.created", conversation=conversation,
                         principal=principal, platform_message_id="m", occurred_at=clock.now(),
                         received_at=clock.now(), trace_id="t")
    task = service.accept(event, AccessContext(principal=principal, conversation=conversation), "Original goal")
    return service, task, runtime_client_for(service)


async def test_threaded_recovery_wakes_a_waiting_debug_loop(environment):
    service, _task, _app = environment
    loop = asyncio.get_running_loop()
    debug = loop.get_debug()
    loop.set_debug(True)
    service.wakeup.clear()
    waiting = asyncio.create_task(service.wakeup.wait())
    try:
        await asyncio.sleep(0)
        await asyncio.to_thread(service.wakeup.set)
        assert await asyncio.wait_for(waiting, 2)
        service.wakeup.clear()
        service.wakeup.set()
        assert service.wakeup.is_set()
    finally:
        waiting.cancel()
        await asyncio.gather(waiting, return_exceptions=True)
        loop.set_debug(debug)


class SlowStopRunner(PiRunnerAdapter):
    def __init__(self, directory, claim, service, process_id, error="413 context exceeds application budget", verified=True):
        super().__init__(Path(__file__), directory, timeout=30)
        self.service, self.process_id, self.claim = service, process_id, claim
        self.error_kind, self.verified = error, verified
        self.archive = directory / "archive"
        directory.mkdir(parents=True, exist_ok=True)
        self.process = object()
        self.started = asyncio.Event()
        self.stop_states = []
        self.gateway = SimpleNamespace(binding="binding", waiting=False, claim=claim, outcome=None,
            provider=SimpleNamespace(background=False), tools=SimpleNamespace(max_calls=8), key="",
            model_token="", tool_token="", process_token="", evidence=[], artifacts=[], effects={},
            observations={}, workspace=directory, drain=self.drain, session_receipt=None)
        self.gateway.result = lambda summary: RunGateway.result(self.gateway, summary)
        if claim.get("notification_status"):
            self.queue.put_nowait({"type": "message_end", "message": {"role": "assistant",
                "content": [{"type": "text", "text": "任务未完成，执行时遇到了错误。"}]}})
            self.gateway.session_receipt = {"session_id": claim["session_id"], "run_id": claim["run_id"],
                "session_revision": "fixture-final", "kind": "turn_finished", "input_ids": ["task:" + claim["task_id"]],
                "tools_paired": True, "inflight": 0}
            self.service.session_receipt(claim["run_id"], claim["fencing_token"], "worker", self.gateway.session_receipt)
            self.queue.put_nowait({"type": "agent_settled"})
        elif error == "413 context exceeds application budget":
            self.queue.put_nowait({"type": "message_end", "message": {"role": "assistant",
                "errorMessage": error, "stopReason": "error", "content": []}})
            self.queue.put_nowait({"type": "agent_settled"})

    async def bind(self, claim):
        self.claim = self.gateway.claim = claim
        self.service.bind_process(claim["run_id"], claim["fencing_token"], "worker", {
            "process_id": self.process_id, "session_id": str(uuid4()), "epoch": 1, "generation": 1,
            "scope_key": "fixture", "session_file": str(self.session_path), "identity": {},
        })

    async def command(self, *args, **kwargs):
        self.started.set()
        if self.error_kind == "plain" and not self.claim.get("notification_status"):
            raise ValueError("ordinary fixture exception")
        return "prompt"

    async def rpc(self, kind, **kwargs):
        return {"sessionFile": str(self.session_path), "sessionId": "fixture"}

    async def drain(self):
        return True

    async def stop(self):
        self.stop_states.append(self.run_timeout)
        await asyncio.sleep(0.35)
        self.stopped_verified = self.verified
        self.service.process_stopped("worker", {"process_id": self.process_id, "epoch": 1, "stopped": self.verified})
        return self.verified

    def process_identity(self):
        return {"id": self.process_id, "epoch": 1, "generation": 1}


def pool_for(tmp_path, service, error="413 context exceeds application budget", verified=True):
    created = []
    def factory(claim, process_id, session_id):
        runner = SlowStopRunner(tmp_path / process_id, claim, service, process_id, error, verified)
        created.append(runner)
        return runner
    return SessionPool(factory), created


@pytest.mark.parametrize("error", ["413 context exceeds application budget", "plain"])
async def test_actual_pi_failure_slow_stop_commits_and_queues_natural_notice(environment, tmp_path, caplog, error):
    service, task, client = environment
    pool, created = pool_for(tmp_path, service, error)
    caplog.set_level(logging.ERROR)
    worker = Worker(client, "worker", Spool(tmp_path / "spool"), None, pool=pool)
    assert await worker.once()
    with service.db.engine.connect() as conn:
        failed_run = conn.execute(select(runs)).mappings().one()
        saved = conn.execute(select(tasks)).mappings().one()
        assert conn.execute(select(processes.c.state)).scalar_one() == "STOPPED"
        assert conn.execute(select(outbox.c.id)).first() is None
    assert failed_run["slot_reserved"] == 0 and failed_run["runner_stopped_at"]
    assert json.loads(failed_run["result_json"])["status"] == "failed"
    assert saved["state"] == "QUEUED" and saved["failure_notice_json"]
    assert all(deadline is None for deadline in created[0].stop_states)
    assert list(worker.spool.directory.glob("*.json")) == []
    assert failed_run["id"] in caplog.text and task["id"] in caplog.text
    assert await worker.once()
    with service.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.state)).scalar_one() == "FAILED"
        assert "任务未完成" in json.loads(conn.execute(select(outbox.c.envelope_json)).scalar_one())["text"]
    await pool.close()


async def test_supervisor_error_stops_run_and_persists_failure(environment, tmp_path):
    service, _task, client = environment
    pool, created = pool_for(tmp_path, service, error="wait")
    class FailingControl:
        async def post(self, path, **kwargs):
            if path.endswith("/control"):
                raise httpx.ReadTimeout("control connection lost")
            return await client.post(path, **kwargs)
    worker = Worker(FailingControl(), "worker", Spool(tmp_path / "spool"), None, pool=pool)
    assert await worker.once()
    with service.db.engine.connect() as conn:
        row = conn.execute(select(runs)).mappings().one()
        assert row["slot_reserved"] == 0 and json.loads(row["result_json"])["status"] == "failed"
    assert created[0].stopped_verified


async def test_unknown_stop_retains_slot_and_durable_reconciliation_evidence(environment, tmp_path):
    service, _task, client = environment
    pool, _created = pool_for(tmp_path, service, verified=False)
    worker = Worker(client, "worker", Spool(tmp_path / "spool"), None, pool=pool)
    assert not await worker.once()
    with service.db.engine.connect() as conn:
        assert conn.execute(select(runs.c.slot_reserved)).scalar_one() == 1
        assert conn.execute(select(tasks.c.state)).scalar_one() == "NEEDS_RECONCILIATION"
    files = list(worker.spool.directory.glob("*.json"))
    assert len(files) == 1
    evidence = json.loads(files[0].read_text())
    assert evidence["stopped"] is False and evidence["quiescent"] is False
    assert evidence["result"]["status"] == "failed"
    assert next(iter(pool.entries.values()))["state"] == "UNKNOWN"
    await worker.replay()
    assert files[0].exists()


async def test_expired_control_stops_harness_and_preserves_result(environment, tmp_path, clock):
    service, _task, client = environment
    pool, created = pool_for(tmp_path, service, error="wait")
    class ExpiredControl:
        async def post(self, path, **kwargs):
            if path.endswith("/control"):
                clock.advance(31)
            return await client.post(path, **kwargs)

    worker = Worker(ExpiredControl(), "worker", Spool(tmp_path / "spool"), None, pool=pool)
    with pytest.raises(httpx.HTTPStatusError) as failure:
        await worker.once()
    assert failure.value.response.status_code == 409
    assert created[0].stopped_verified
    with service.db.engine.connect() as conn:
        assert conn.execute(select(processes.c.state)).scalar_one() == "STOPPED"
    evidence = json.loads(next(worker.spool.directory.glob("*.json")).read_text())
    assert evidence["stopped"] is True
    assert evidence["result"]["status"] == "failed"


async def test_stop_heartbeat_logs_lost_lease_and_leaves_recovery_evidence(environment, tmp_path, clock, caplog):
    service, _task, client = environment
    claim = service.claim("worker")
    clock.advance(31)
    worker = Worker(client, "worker", Spool(tmp_path / "spool"), None)
    await worker.lease_heartbeat({key: claim[key] for key in ("run_id", "fencing_token")})
    assert "Lease ownership lost" in caplog.text
    with service.db.engine.connect() as conn:
        assert conn.execute(select(runs.c.slot_reserved)).scalar_one() == 1


async def test_cancellation_records_failure_after_verified_stop(environment, tmp_path):
    service, _task, client = environment
    pool, created = pool_for(tmp_path, service, error="wait")
    worker = Worker(client, "worker", Spool(tmp_path / "spool"), None, pool=pool)
    execution = asyncio.create_task(worker.once())
    while not created:
        await asyncio.sleep(0)
    await created[0].started.wait()
    execution.cancel()
    with pytest.raises(asyncio.CancelledError):
        await execution
    with service.db.engine.connect() as conn:
        assert conn.execute(select(runs.c.slot_reserved)).scalar_one() == 0
        assert conn.execute(select(tasks.c.failure_notice_json)).scalar_one()
    assert list(worker.spool.directory.glob("*.json")) == []


@pytest.mark.parametrize("exhaustion", ["deadline", "active", "models"])
def test_failure_explanation_has_separate_bounded_budget(environment, clock, exhaustion):
    service, task, _app = environment
    claim = service.claim("worker")
    original_plan = json.loads(task["plan_json"])
    used = {"model_calls": original_plan["budget"]["model_calls"],
            "active_seconds": original_plan["budget"]["active_seconds"]}
    with service.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.id == task["id"]).values(budget_json=json.dumps(used)))
        if exhaustion == "deadline":
            conn.execute(update(tasks).where(tasks.c.id == task["id"]).values(deadline=clock.now().isoformat()))
        elif exhaustion == "active":
            conn.execute(update(tasks).where(tasks.c.id == task["id"]).values(stop_reason="active_budget"))
    result = {"status": "failed", "agent_authored": False, "summary": "Verified host failure facts"}
    assert service.submit(claim["run_id"], 1, "worker", result)["state"] == "QUEUED"
    with service.db.engine.connect() as conn:
        row = conn.execute(select(runs)).mappings().one()
        assert row["slot_reserved"] == 0 and row["result_json"]
    notice = service.claim("worker")
    assert notice["notification_status"] == "failed"
    assert notice["plan"]["budget"]["model_calls"] == 2
    assert notice["plan"]["budget"]["tool_calls"] == 0
    assert notice["active_seconds_remaining"] == 30
    assert service.authorize(notice["run_id"], notice["fencing_token"], "worker", {"action": "model.request"})["allowed"]
    for kind in ("completion", "compaction"):
        service.reserve_budget(notice["run_id"], notice["fencing_token"], "worker", {"request_id": kind, "kind": kind})
        with pytest.raises(ValueError, match="explanation model budget"):
            service.reserve_budget(notice["run_id"], notice["fencing_token"], "worker", {"request_id": kind + "-again", "kind": kind})
    with pytest.raises(PermissionError):
        service.reserve_budget(notice["run_id"], notice["fencing_token"], "worker", {"request_id": "tool", "kind": "tool"})
    with service.db.engine.connect() as conn:
        saved = conn.execute(select(tasks)).mappings().one()
    assert saved["plan_json"] == task["plan_json"]
    remaining = json.loads(saved["budget_json"])
    assert remaining["model_calls"] == used["model_calls"] and remaining["active_seconds"] == used["active_seconds"]
    assert remaining["notification_model_calls"] == 2
    service.submit(notice["run_id"], notice["fencing_token"], "worker",
                   {"status": "failed", "summary": "任务没有完成，执行预算已经用尽。"})
    with service.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.state)).scalar_one() == ("EXPIRED" if exhaustion == "deadline" else "FAILED")
        assert len(conn.execute(select(outbox.c.id)).all()) == 1


def test_notification_wall_bound_preserves_original_failure_and_cancellation(environment, clock):
    service, _task, _app = environment
    initial = service.claim("worker")
    service.submit(initial["run_id"], 1, "worker", {"status": "failed", "agent_authored": False, "summary": "Host failure"})
    notice = service.claim("worker")
    clock.advance(29)
    assert service.renew(notice["run_id"], notice["fencing_token"], "worker")["state"] == "RUNNING"
    clock.advance(2)
    with pytest.raises(PermissionError, match="no longer authorized"):
        service.authorize(notice["run_id"], notice["fencing_token"], "worker", {"action": "model.request"})
    assert service.control(notice["run_id"], notice["fencing_token"], "worker")["state"] == "CANCEL_REQUESTED"
    service.stopped(notice["run_id"], notice["fencing_token"], "worker")
    with service.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.state)).scalar_one() == "FAILED"
        assert len(conn.execute(select(outbox.c.id)).all()) == 0
        assert conn.execute(select(tasks.c.failure_notice_json)).scalar_one()
    assert service.claim("worker") is None


async def test_partial_bind_without_verified_stop_is_not_released(environment, tmp_path):
    service, _task, client = environment
    class BadBinding(SlowStopRunner):
        async def bind(self, claim):
            await super().bind(claim)
            raise RuntimeError("binding failed after registration")
    pool = SessionPool(lambda claim, process_id, _sid: BadBinding(tmp_path / process_id, claim, service, process_id, verified=False))
    worker = Worker(client, "worker", Spool(tmp_path / "spool"), None, pool=pool)
    assert not await worker.once()
    with service.db.engine.connect() as conn:
        assert conn.execute(select(runs.c.slot_reserved)).scalar_one() == 1
        assert conn.execute(select(tasks.c.state)).scalar_one() == "NEEDS_RECONCILIATION"
    assert json.loads(next(worker.spool.directory.glob("*.json")).read_text())["stopped"] is False


async def test_event_persistence_error_does_not_drop_failure_result(environment, tmp_path):
    service, _task, client = environment
    pool, _created = pool_for(tmp_path, service)
    class BrokenEventSpool(Spool):
        def save(self, payload, **kwargs):
            if "seq" in kwargs:
                raise OSError("event spool unavailable")
            return super().save(payload, **kwargs)
    worker = Worker(client, "worker", BrokenEventSpool(tmp_path / "spool"), None, pool=pool)
    assert await worker.once()
    with service.db.engine.connect() as conn:
        assert conn.execute(select(runs.c.slot_reserved)).scalar_one() == 0
        assert conn.execute(select(tasks.c.failure_notice_json)).scalar_one()


@pytest.mark.parametrize("busy_once", [False, True])
async def test_active_heartbeat_renews_while_event_flush_is_waiting(environment, tmp_path, clock, monkeypatch, busy_once):
    from elephant.jobs import worker as worker_module

    service, _task, client = environment
    claim = service.claim("worker")
    blocked = asyncio.Event()
    renewals = asyncio.Queue()
    busy = Event()
    renew = service.renew

    def renew_with_busy(*args):
        if busy_once and blocked.is_set() and not busy.is_set():
            busy.set()
            error = SQLiteError("database is locked")
            error.sqlite_errorcode = SQLITE_BUSY
            raise OperationalError(None, None, error)
        return renew(*args)

    monkeypatch.setattr(service, "renew", renew_with_busy)

    class WaitingWorker(Worker):
        async def flush_events(self, run_id=None):
            blocked.set()
            await asyncio.Future()

        async def call(self, operation, **payload):
            result = await super().call(operation, **payload)
            if operation == "renew" and blocked.is_set():
                renewals.put_nowait(result["lease_expires_at"])
            return result

    class WaitingRunner:
        stopped_verified = False

        async def run(self, _goal):
            await asyncio.Future()

        async def stop(self):
            self.stopped_verified = True
            return True

    monkeypatch.setattr(worker_module, "LEASE_HEARTBEAT_SECONDS", 0.01)
    worker = WaitingWorker(client, "worker", Spool(tmp_path / "spool"), lambda _: WaitingRunner())
    run = asyncio.create_task(worker.once(claim))
    try:
        await asyncio.wait_for(blocked.wait(), 2)
        old_expiry = (clock.now() + timedelta(seconds=30)).isoformat()
        clock.advance(25)
        # Thread dispatch can complete the initial renewal after blocked is set.
        # Wait for a transaction that actually observed the advanced fake clock.
        while await asyncio.wait_for(renewals.get(), 2) <= old_expiry:
            pass
        clock.advance(10)
        assert await asyncio.wait_for(renewals.get(), 2) > clock.now().isoformat()
        assert busy.is_set() == busy_once
    finally:
        worker.begin_shutdown()
        run.cancel()
        await asyncio.gather(run, return_exceptions=True)
    assert not any(task.get_name() == "elephant-lease:" + claim["run_id"] for task in asyncio.all_tasks())


async def test_stop_heartbeat_preserves_lease_beyond_old_expiry(environment, tmp_path, clock, monkeypatch):
    from elephant.jobs import worker as worker_module

    service, _task, client = environment
    sleeping, ticks = asyncio.Queue(), asyncio.Queue()
    renewals = []
    original_expiry = None

    class HeartbeatClock:
        def __getattr__(self, name):
            return getattr(asyncio, name)

        async def sleep(self, delay):
            # Only the worker's heartbeat uses this clock. Do not mix accelerated
            # Core time with real heartbeat timers: the old test advanced 30 fake
            # seconds during one 5-second interval and raced exact lease expiry.
            assert delay == 5
            with service.db.engine.connect() as conn:
                renewals.append(conn.execute(select(runs.c.lease)).scalar_one())
            sleeping.put_nowait(None)
            await ticks.get()

    monkeypatch.setattr(worker_module, "asyncio", HeartbeatClock())

    class LongStop(SlowStopRunner):
        async def run(self, *args, **kwargs):
            nonlocal original_expiry
            with service.db.engine.connect() as conn:
                original_expiry = conn.execute(select(runs.c.lease)).scalar_one()
            for _ in range(5):
                # Drive the active heartbeat at the same pace as the fake database clock.
                await asyncio.wait_for(sleeping.get(), timeout=5)
                clock.advance(5)
                ticks.put_nowait(None)
            raise ValueError("Failure before verified stop")

        async def stop(self):
            if self.stopped_verified:
                return True
            for _ in range(9):
                # Reaching sleep proves the real renew request has completed.
                await asyncio.wait_for(sleeping.get(), timeout=5)
                clock.advance(5)
                assert renewals[-1] > clock.now().isoformat()
                ticks.put_nowait(None)
            await asyncio.wait_for(sleeping.get(), timeout=5)
            return await super().stop()

    pool = SessionPool(lambda claim, pid, _sid: LongStop(tmp_path / pid, claim, service, pid, error="plain"))
    worker = Worker(client, "worker", Spool(tmp_path / "spool"), None, pool=pool)
    assert await worker.once()
    assert clock.now().isoformat() > original_expiry
    assert len(renewals) == 15  # Five renewals during execution and ten through verified stop.
    assert all(a < b for a, b in pairwise(renewals))
    with service.db.engine.connect() as conn:
        assert conn.execute(select(runs.c.slot_reserved)).scalar_one() == 0
        assert conn.execute(select(tasks.c.failure_notice_json)).scalar_one()
    assert list(worker.spool.directory.glob("*.json")) == []


async def test_rejected_live_result_preserves_original_and_queues_failure(environment, tmp_path, caplog):
    from elephant.domain.tasks import TaskResult

    service, _task, client = environment

    class InvalidResult(SlowStopRunner):
        async def run(self, *args, **kwargs):
            if self.claim.get("notification_status"):
                return await super().run(*args, **kwargs)
            self.quiescent_verified = True
            return TaskResult(status="failed", summary="Original model result", effect_refs=("unknown-operation",))

    pool = SessionPool(lambda claim, pid, _sid: InvalidResult(tmp_path / pid, claim, service, pid))
    worker = Worker(client, "worker", Spool(tmp_path / "spool"), None, pool=pool)
    assert await worker.once()
    with service.db.engine.connect() as conn:
        result = json.loads(conn.execute(select(runs.c.result_json)).scalar_one())
        assert "result references unknown operations" in result["summary"]
    assert await worker.once()  # The rejected result cannot poison the queued failure explanation.
    rejected = list((worker.spool.directory / "rejected").glob("*.json"))
    assert len(rejected) == 1
    assert json.loads(rejected[0].read_text())["result"]["effect_refs"] == ["unknown-operation"]
    assert "detail=result references unknown operations" in caplog.text
    with service.db.engine.connect() as conn:
        assert all(row == 0 for row in conn.execute(select(runs.c.slot_reserved)).scalars())
    assert list(worker.spool.directory.glob("*.json")) == []


async def test_rejected_replay_isolated_and_new_claim_runs(environment, tmp_path, caplog):
    service, _task, client = environment
    claim = service.claim("worker")
    spool = Spool(tmp_path / "spool")
    spool.save({"run_id": claim["run_id"], "fencing_token": claim["fencing_token"],
                "stopped": True, "quiescent": False,
                "result": {"status": "failed", "summary": "rejected", "effect_refs": ["unknown-operation"]}})
    with service.db.engine.connect() as conn:
        original = conn.execute(select(tasks)).mappings().one()
    event = InboundEvent.model_validate_json(original["event_json"]).model_copy(update={"event_id": "new-event"})
    new_task = service.accept(event, AccessContext(principal=event.principal, conversation=event.conversation), "Another goal")
    pool, _created = pool_for(tmp_path, service)
    worker = Worker(client, "worker", spool, None, pool=pool)
    assert await worker.once()
    assert len(list((spool.directory / "rejected").glob("*.json"))) == 1
    assert "detail=result references unknown operations" in caplog.text
    remaining = [json.loads(path.read_text()) for path in spool.directory.glob("*.json")]
    assert any("result references unknown operations" in payload["result"]["summary"] for payload in remaining)
    with service.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == claim["task_id"])).scalar_one() == "NEEDS_RECONCILIATION"
        assert conn.execute(select(tasks.c.failure_notice_json).where(tasks.c.id == new_task["id"])).scalar_one()


async def test_finalizer_does_not_reexport_child_paths_from_its_workspace(environment, tmp_path):
    from elephant.domain.tasks import TaskResult
    service, task, client = environment
    class FinalizerRunner(SlowStopRunner):
        async def run(self, goal, **kwargs):
            self.quiescent_verified = True
            return TaskResult(status="completed", summary="Verified child artifact is ready.")
    pool = SessionPool(lambda claim, pid, sid: FinalizerRunner(tmp_path / pid, claim, service, pid, error="wait"))
    worker = Worker(client, "worker", Spool(tmp_path / "spool"), None, pool=pool)
    original = worker.call
    async def finalizer(operation, **payload):
        value = await original(operation, **payload)
        if operation == "claim" and value:
            value.update(role="leader", purpose="finalize", goal_spec={"requested_outputs": ["outputs/child.txt"]})
        return value
    worker.call = finalizer
    try:
        assert await worker.once()
        assert service.get(task["public_id"], service._access_for(task))["state"] == "SUCCEEDED"
    finally:
        await pool.close()


async def test_real_sqlite_writer_does_not_starve_renew_timer_and_recovers(environment, clock):
    service, _, client = environment
    claim = service.claim("worker")
    request = {"worker_id": "worker", **{key: claim[key] for key in ("run_id", "fencing_token")}}
    clock.advance(1)
    loop = asyncio.get_running_loop()
    with service.db.transaction():
        started = loop.time()
        renewing = asyncio.create_task(client.post("/internal/tasks/renew", json=request))
        await asyncio.sleep(0.025)
        assert loop.time() - started < 0.5
        assert not renewing.done()
        # Keep a real competing writer through SQLite's production busy timeout.
        response = await renewing
        assert response.status_code == 503
    assert (await client.post("/internal/tasks/renew", json=request)).status_code == 200
    with service.db.engine.connect() as conn:
        lease = conn.execute(select(runs.c.lease).where(runs.c.id == claim["run_id"])).scalar_one()
    assert lease > clock.now().isoformat()
    clock.advance(31)
    assert (await client.post("/internal/tasks/renew", json=request)).status_code == 409


async def test_cancelled_renew_waits_for_sqlite_transaction(environment):
    service, _, client = environment
    claim = service.claim("worker")
    request = {"worker_id": "worker", **{key: claim[key] for key in ("run_id", "fencing_token")}}
    with service.db.transaction():
        renewing = asyncio.create_task(client.post("/internal/tasks/renew", json=request))
        await asyncio.sleep(0.05)
        renewing.cancel()
        await asyncio.sleep(0.025)
        renewing.cancel()
        await asyncio.sleep(0.025)
        assert not renewing.done()
    with pytest.raises(asyncio.CancelledError):
        await renewing
    with service.db.transaction():
        pass
