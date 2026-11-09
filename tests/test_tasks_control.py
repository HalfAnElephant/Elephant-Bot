import pytest
from sqlalchemy import select

from qqbot.clock import FakeClock
from qqbot.domain.messages import AccessContext, ConversationRef, InboundEvent, PrincipalRef, Surface
from qqbot.harness.artifacts import export_artifacts
from qqbot.jobs.service import TaskService
from qqbot.outbox.service import Outbox
from qqbot.storage.db import Database
from qqbot.storage.schema import metadata, outbox
from qqbot.storage.task_schema import runs


@pytest.fixture
def setup(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    metadata.create_all(db.engine)
    clock = FakeClock()
    service = TaskService(db, clock, Outbox(db, clock))
    conversation = ConversationRef(
        deployment_id="d",
        adapter_instance_id="a",
        bot_identity_id="b",
        surface=Surface.GROUP,
        remote_conversation_id="g",
    )
    principal = PrincipalRef(adapter_instance_id="a", identity_namespace="qq", remote_user_id="u")
    event = InboundEvent(
        event_id="e",
        event_type="message.created",
        conversation=conversation,
        principal=principal,
        platform_message_id="m",
        occurred_at=clock.now(),
        received_at=clock.now(),
        trace_id="t",
    )
    access = AccessContext(principal=principal, conversation=conversation, role="owner")
    yield service, event, access, clock
    db.close()


def test_accept_complete_delivery_and_fence(setup):
    service, event, access, clock = setup
    task = service.accept(event, access, "Explain a concept")
    assert service.accept(event, access, "Explain a concept")["id"] == task["id"]
    claim = service.claim("worker")
    assert service.claim("other") is None
    with pytest.raises(PermissionError):
        service.submit(claim["run_id"], 99, "worker", {"status": "failed", "summary": "done"})
    workspace = service.archive_root.parent / "workspace"
    workspace.mkdir()
    (workspace / "report.md").write_text("report")
    artifacts = export_artifacts(workspace, service.archive_root, ["report.md"])
    result = {
        "status": "completed",
        "summary": "done",
        "artifacts": artifacts,
        "evidence": [
            {"url": "https://example.com", "retrieved_at": clock.now().isoformat(), "sha256": "a" * 64}
        ],
    }
    service.submit(claim["run_id"], claim["fencing_token"], "worker", result)
    assert service.submit(claim["run_id"], claim["fencing_token"], "worker", result)["state"] == "SUCCEEDED"
    assert service.get(task["public_id"], access)["state"] == "SUCCEEDED"
    with service.db.engine.connect() as c:
        assert len(c.execute(select(outbox)).all()) == 2
        assert c.execute(select(runs.c.slot_reserved)).scalar_one() == 0


def test_cancel_rejects_late_success_keeps_slot(setup):
    service, event, access, _clock = setup
    task = service.accept(event, access, "work")
    claim = service.claim("worker")
    service.cancel(task["public_id"], access)
    with pytest.raises(PermissionError):
        service.submit(
            claim["run_id"], claim["fencing_token"], "worker", {"status": "failed", "summary": "done"}
        )
    assert service.claim("other") is None
    assert service.stopped(claim["run_id"], claim["fencing_token"], "worker")["state"] == "CANCELLED"


def test_expired_lease_never_reexecutes(setup):
    service, event, access, clock = setup
    task = service.accept(event, access, "work")
    claim = service.claim("worker")
    clock.advance(31)
    assert service.claim("other") is None
    assert service.get(task["public_id"], access)["state"] == "NEEDS_RECONCILIATION"
    with pytest.raises(PermissionError):
        service.renew(claim["run_id"], claim["fencing_token"], "worker")


def test_member_cannot_submit(setup):
    service, event, access, _clock = setup
    with pytest.raises(PermissionError):
        service.accept(event, access.model_copy(update={"role": "member"}), "work")


def test_completion_requires_real_manifest(setup):
    service, event, access, _clock = setup
    service.accept(event, access, "work")
    claim = service.claim("w")
    with pytest.raises(ValueError, match="exported artifacts"):
        service.submit(
            claim["run_id"], claim["fencing_token"], "w", {"status": "completed", "summary": "claimed done"}
        )
    assert service.append_event(
        claim["run_id"], claim["fencing_token"], "w", "ev1", 1, {"kind": "accepted"}
    ) == {"acknowledged": 1}
    assert service.append_event(
        claim["run_id"], claim["fencing_token"], "w", "ev1", 1, {"kind": "accepted"}
    ) == {"acknowledged": 1}
    with pytest.raises(ValueError, match="sequence gap"):
        service.append_event(claim["run_id"], claim["fencing_token"], "w", "ev3", 3, {})


def test_fair_conversation_rotation(setup):
    service, event, access, _clock = setup
    first = service.accept(event, access, "one")
    event2 = event.model_copy(update={"event_id": "e2"})
    second = service.accept(event2, access, "two")
    claim = service.claim("w")
    assert claim["task_id"] == first["id"] or claim["task_id"] == second["id"]
    service.submit(claim["run_id"], claim["fencing_token"], "w", {"status": "failed", "summary": "test"})
    conversation = event.conversation.model_copy(update={"remote_conversation_id": "other"})
    event3 = event.model_copy(update={"event_id": "e3", "conversation": conversation})
    other = service.accept(event3, access.model_copy(update={"conversation": conversation}), "other group")
    assert service.claim("w")["task_id"] == other["id"]


@pytest.mark.asyncio
async def test_internal_worker_http_contract(setup):
    import httpx
    from fastapi import FastAPI

    from qqbot.jobs.api import internal_router

    service, event, access, _clock = setup
    service.accept(event, access, "work")
    app = FastAPI()
    app.include_router(internal_router(service, "a" * 32))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://core") as client:
        assert (await client.post("/internal/tasks/claim", json={"worker_id": "w"})).status_code == 401
        client.headers["Authorization"] = "Bearer " + "a" * 32
        claim = (await client.post("/internal/tasks/claim", json={"worker_id": "w"})).json()
        payload = {
            "worker_id": "w",
            "run_id": claim["run_id"],
            "fencing_token": claim["fencing_token"],
            "result": {
                "status": "failed",
                "summary": "offline failure",
                "artifacts": [],
                "evidence": [],
                "limitations": ["offline"],
            },
        }
        response = await client.post("/internal/tasks/result", json=payload)
        assert response.status_code == 200
        assert response.json()["state"] == "FAILED"


def test_reconcile_requires_stop_proof_and_new_ownership(setup):
    service, event, access, clock = setup
    task = service.accept(event, access, "work")
    claim = service.claim("w")
    clock.advance(31)
    service.recover()
    with pytest.raises(ValueError, match="stop proof"):
        service.reconcile(task["public_id"], access, claim["run_id"])
    service.stop_verifier = lambda task_id, run_id: task_id == task["id"] and run_id == claim["run_id"]
    result = service.reconcile(task["public_id"], access, claim["run_id"])
    assert result["state"] == "FAILED" and result["fencing_token"] == 2
    assert service.recovery_status(claim["run_id"], 1, "w")["discard_spool"]
    assert service.claim("w") is None
    retry_event = event.model_copy(update={"event_id": "retry-command"})
    retried = service.retry(task["public_id"], retry_event, access)
    assert retried["id"] != task["id"] and retried["retry_of_task_id"] == task["id"]
    with pytest.raises(ValueError, match="different goal"):
        service.accept(retry_event, access, "changed goal")


@pytest.mark.asyncio
async def test_worker_lost_result_response_replays_without_model(setup, tmp_path):
    import asyncio

    import httpx
    from fastapi import FastAPI

    from qqbot.domain.tasks import TaskResult
    from qqbot.jobs.api import internal_router
    from qqbot.jobs.worker import Spool, Worker
    from qqbot.storage.task_schema import events

    service, event, access, _clock = setup
    service.accept(event, access, "work")
    app = FastAPI()
    app.include_router(internal_router(service, "a" * 32))
    starts = []

    class FakeHarness:
        stopped_verified = True

        async def run(self, goal):
            starts.append(goal)
            self.event_sink(
                {"type": "tool_execution_start", "toolName": "research_search", "toolCallId": "t"}
            )
            await asyncio.sleep(0)
            return TaskResult(status="failed", summary="Offline acceptance fixture")

        async def stop(self):
            return True

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://core",
        headers={"Authorization": "Bearer " + "a" * 32},
    ) as client:

        class LoseFirstResponse:
            lost = False

            async def post(self, path, **kwargs):
                response = await client.post(path, **kwargs)
                if path.endswith("/result") and not self.lost:
                    self.lost = True
                    raise httpx.ReadTimeout("lost acknowledgement")
                return response

        worker = Worker(LoseFirstResponse(), "w", Spool(tmp_path / "spool"), lambda claim: FakeHarness())
        with pytest.raises(httpx.ReadTimeout):
            await worker.once()
        assert list(worker.spool.directory.glob("*.json"))
        assert not await worker.once()
        assert not list(worker.spool.directory.glob("*.json"))
    assert len(starts) == 1
    with service.db.engine.connect() as c:
        assert len(c.execute(select(events)).all()) == 2


@pytest.mark.asyncio
async def test_worker_cancel_at_result_commit_releases_slot(setup, tmp_path):
    import httpx
    from fastapi import FastAPI

    from qqbot.domain.tasks import TaskResult
    from qqbot.jobs.api import internal_router
    from qqbot.jobs.worker import Spool, Worker

    service, event, access, _clock = setup
    task = service.accept(event, access, "work")
    app = FastAPI()
    app.include_router(internal_router(service, "a" * 32))

    class FakeHarness:
        stopped_verified = True

        async def run(self, goal):
            service.cancel(task["public_id"], access)
            return TaskResult(status="failed", summary="Late output must be suppressed")

        async def stop(self):
            return True

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://core",
        headers={"Authorization": "Bearer " + "a" * 32},
    ) as client:
        worker = Worker(client, "w", Spool(tmp_path / "spool"), lambda claim: FakeHarness())
        assert await worker.once()
        assert not list(worker.spool.directory.rglob("*.json"))
    assert service.get(task["public_id"], access)["state"] == "CANCELLED"
    with service.db.engine.connect() as c:
        assert c.execute(select(runs.c.slot_reserved)).scalar_one() == 0
        assert len(c.execute(select(outbox)).all()) == 1


@pytest.mark.asyncio
async def test_supervisor_cancellation_stops_and_commits_failure(setup, tmp_path):
    import asyncio

    import httpx
    from fastapi import FastAPI

    from qqbot.jobs.api import internal_router
    from qqbot.jobs.worker import Spool, Worker

    service, event, access, _clock = setup
    task = service.accept(event, access, "work")
    app = FastAPI()
    app.include_router(internal_router(service, "a" * 32))
    started = asyncio.Event()

    class FakeHarness:
        stopped_verified = False

        async def run(self, goal):
            started.set()
            await asyncio.Event().wait()

        async def stop(self):
            self.stopped_verified = True
            return True

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://core",
        headers={"Authorization": "Bearer " + "a" * 32},
    ) as client:
        worker = Worker(client, "w", Spool(tmp_path / "spool"), lambda claim: FakeHarness())
        execution = asyncio.create_task(worker.once())
        await started.wait()
        execution.cancel()
        with pytest.raises(asyncio.CancelledError):
            await execution
        assert not list(worker.spool.directory.rglob("*.json"))
    assert service.get(task["public_id"], access)["state"] == "FAILED"
    with service.db.engine.connect() as c:
        assert c.execute(select(runs.c.slot_reserved)).scalar_one() == 0
