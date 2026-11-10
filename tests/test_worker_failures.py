"""Failure paths preserve results or reconciliation evidence before releasing ownership."""
import asyncio
import json
import logging
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import select, update

from qqbot.domain.messages import AccessContext, ConversationRef, InboundEvent, PrincipalRef, Surface
from qqbot.harness.pi import PiRunnerAdapter
from qqbot.harness.pool import SessionPool
from qqbot.jobs.api import internal_router
from qqbot.jobs.service import TaskService
from qqbot.jobs.worker import Spool, Worker
from qqbot.outbox.service import Outbox
from qqbot.storage.schema import outbox
from qqbot.storage.task_schema import processes, runs, tasks


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
    app = FastAPI()
    app.include_router(internal_router(service, "x" * 32))
    return service, task, app


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
            observations={}, workspace=directory, drain=self.drain)
        if claim.get("notification_status"):
            self.queue.put_nowait({"type": "message_end", "message": {"role": "assistant",
                "content": [{"type": "text", "text": "任务未完成，执行时遇到了错误。"}]}})
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
    service, task, app = environment
    pool, created = pool_for(tmp_path, service, error)
    caplog.set_level(logging.ERROR)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://core",
                                headers={"Authorization": "Bearer " + "x" * 32}) as client:
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
    service, _task, app = environment
    pool, created = pool_for(tmp_path, service, error="wait")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://core",
                                headers={"Authorization": "Bearer " + "x" * 32}) as client:
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
    service, _task, app = environment
    pool, _created = pool_for(tmp_path, service, verified=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://core",
                                headers={"Authorization": "Bearer " + "x" * 32}) as client:
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


async def test_cancellation_records_failure_after_verified_stop(environment, tmp_path):
    service, _task, app = environment
    pool, created = pool_for(tmp_path, service, error="wait")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://core",
                                headers={"Authorization": "Bearer " + "x" * 32}) as client:
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
    service, _task, app = environment
    class BadBinding(SlowStopRunner):
        async def bind(self, claim):
            await super().bind(claim)
            raise RuntimeError("binding failed after registration")
    pool = SessionPool(lambda claim, process_id, _sid: BadBinding(tmp_path / process_id, claim, service, process_id, verified=False))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://core",
                                headers={"Authorization": "Bearer " + "x" * 32}) as client:
        worker = Worker(client, "worker", Spool(tmp_path / "spool"), None, pool=pool)
        assert not await worker.once()
        with service.db.engine.connect() as conn:
            assert conn.execute(select(runs.c.slot_reserved)).scalar_one() == 1
            assert conn.execute(select(tasks.c.state)).scalar_one() == "NEEDS_RECONCILIATION"
        assert json.loads(next(worker.spool.directory.glob("*.json")).read_text())["stopped"] is False


async def test_event_persistence_error_does_not_drop_failure_result(environment, tmp_path):
    service, _task, app = environment
    pool, _created = pool_for(tmp_path, service)
    class BrokenEventSpool(Spool):
        def save(self, payload, **kwargs):
            if "seq" in kwargs:
                raise OSError("event spool unavailable")
            return super().save(payload, **kwargs)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://core",
                                headers={"Authorization": "Bearer " + "x" * 32}) as client:
        worker = Worker(client, "worker", BrokenEventSpool(tmp_path / "spool"), None, pool=pool)
        assert await worker.once()
        with service.db.engine.connect() as conn:
            assert conn.execute(select(runs.c.slot_reserved)).scalar_one() == 0
            assert conn.execute(select(tasks.c.failure_notice_json)).scalar_one()


async def test_stop_heartbeat_preserves_lease_beyond_old_expiry(environment, tmp_path, clock):
    service, _task, app = environment

    class LongStop(SlowStopRunner):
        async def run(self, *args, **kwargs):
            clock.advance(25)
            raise ValueError("Failure before verified stop")

        async def stop(self):
            if self.stopped_verified:
                return True
            for _ in range(7):
                await asyncio.sleep(1)
                # Advance beyond the original lease without racing the five-second renewal tick.
                clock.advance(4)
            return await super().stop()

    pool = SessionPool(lambda claim, pid, _sid: LongStop(tmp_path / pid, claim, service, pid, error="plain"))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://core",
                                headers={"Authorization": "Bearer " + "x" * 32}) as client:
        worker = Worker(client, "worker", Spool(tmp_path / "spool"), None, pool=pool)
        assert await worker.once()
    with service.db.engine.connect() as conn:
        assert conn.execute(select(runs.c.slot_reserved)).scalar_one() == 0
        assert conn.execute(select(tasks.c.failure_notice_json)).scalar_one()
    assert list(worker.spool.directory.glob("*.json")) == []


async def test_rejected_live_result_preserves_original_and_queues_failure(environment, tmp_path):
    from qqbot.domain.tasks import TaskResult

    service, _task, app = environment

    class InvalidResult(SlowStopRunner):
        async def run(self, *args, **kwargs):
            if self.claim.get("notification_status"):
                return await super().run(*args, **kwargs)
            self.quiescent_verified = True
            return TaskResult(status="failed", summary="Original model result", effect_refs=("unknown-operation",))

    pool = SessionPool(lambda claim, pid, _sid: InvalidResult(tmp_path / pid, claim, service, pid))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://core",
                                headers={"Authorization": "Bearer " + "x" * 32}) as client:
        worker = Worker(client, "worker", Spool(tmp_path / "spool"), None, pool=pool)
        assert await worker.once()
        assert await worker.once()  # The rejected result cannot poison the queued failure explanation.
    rejected = list((worker.spool.directory / "rejected").glob("*.json"))
    assert len(rejected) == 1
    assert json.loads(rejected[0].read_text())["result"]["effect_refs"] == ["unknown-operation"]
    with service.db.engine.connect() as conn:
        assert all(row == 0 for row in conn.execute(select(runs.c.slot_reserved)).scalars())
    assert list(worker.spool.directory.glob("*.json")) == []


async def test_rejected_replay_isolated_and_new_claim_runs(environment, tmp_path):
    service, _task, app = environment
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
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://core",
                                headers={"Authorization": "Bearer " + "x" * 32}) as client:
        worker = Worker(client, "worker", spool, None, pool=pool)
        assert await worker.once()
    assert len(list((spool.directory / "rejected").glob("*.json"))) == 1
    with service.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == claim["task_id"])).scalar_one() == "NEEDS_RECONCILIATION"
        assert conn.execute(select(tasks.c.failure_notice_json).where(tasks.c.id == new_task["id"])).scalar_one()
