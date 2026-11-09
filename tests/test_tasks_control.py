import pytest
from sqlalchemy import select

from qqbot.clock import FakeClock
from qqbot.domain.execution import TaskGoal
from qqbot.domain.messages import AccessContext, ConversationRef, InboundEvent, PrincipalRef, Surface
from qqbot.harness.artifacts import export_artifacts
from qqbot.jobs.service import TaskService
from qqbot.outbox.service import Outbox
from qqbot.permissions import PermissionRule
from qqbot.storage.db import Database
from qqbot.storage.schema import metadata, outbox
from qqbot.storage.task_schema import inputs, notifications, runs, waits


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
        assert len(c.execute(select(outbox)).all()) == 1
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


def test_member_uses_same_permission_service(setup):
    service, event, access, _clock = setup
    member = access.model_copy(update={"role": "member"})
    assert service.accept(event, member, "work")["requester"] == member.principal.key
    service.permissions.upsert(
        PermissionRule(id="deny-create", effect="deny", actions=("task.create",)),
        expected_version=0,
        actor="test",
    )
    with pytest.raises(PermissionError):
        service.accept(event.model_copy(update={"event_id": "another"}), member, "work")


def test_completion_requires_real_manifest(setup):
    service, event, access, _clock = setup
    service.accept(event, access, TaskGoal(text="work", requested_outputs=("report.md",)))
    claim = service.claim("w")
    with pytest.raises(ValueError, match="requested outputs missing"):
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
        assert (await client.post("/internal/tasks/claim", json={"worker_id": "w"})).json() is None
        claim = (
            await client.post("/internal/tasks/claim", json={"worker_id": "w", "protocol_version": 2})
        ).json()
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
        assert len(c.execute(select(outbox)).all()) == 0


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
    assert service.get(task["public_id"], access)["state"] == "QUEUED"
    with service.db.engine.connect() as c:
        assert c.execute(select(runs.c.slot_reserved)).scalar_one() == 0
    explanation = service.claim("explainer")
    assert explanation["notification_status"] == "failed"
    assert explanation["context"]["control_response"]
    with pytest.raises(PermissionError):
        service.authorize(explanation["run_id"], explanation["fencing_token"], "explainer", {"action": "github.issue.write"})
    service.submit(explanation["run_id"], explanation["fencing_token"], "explainer",
        {"status": "failed", "summary": "执行已中断，尚未完成。"})
    assert service.get(task["public_id"], access)["state"] == "FAILED"


def test_no_artifact_success_and_notification_identity(setup):
    service, event, access, _clock = setup
    task = service.accept(event, access.model_copy(update={"role": "member"}), "Say hello")
    assert service.claim("old-worker", protocol_version=1) is None
    claim = service.claim("worker", protocol_version=2)
    packet = {"text": "正在核实当前资料。", "notification_id": "progress-1"}
    first = service.notify(claim["run_id"], claim["fencing_token"], "worker", packet)
    assert service.notify(claim["run_id"], claim["fencing_token"], "worker", packet) == first
    with pytest.raises(ValueError, match="identity conflict"):
        service.notify(claim["run_id"], claim["fencing_token"], "worker", {**packet, "text": "changed"})
    result = {"status": "completed", "summary": "你好。"}
    assert service.submit(claim["run_id"], claim["fencing_token"], "worker", result)["state"] == "SUCCEEDED"
    assert service.submit(claim["run_id"], claim["fencing_token"], "worker", result)["state"] == "SUCCEEDED"
    with service.db.engine.connect() as conn:
        assert (
            len(conn.execute(select(notifications).where(notifications.c.task_id == task["id"])).all()) == 2
        )
        assert len(conn.execute(select(outbox).where(outbox.c.origin_id == "progress-1")).all()) == 1


@pytest.mark.parametrize("reply_before_settle", [True, False])
def test_wait_answer_race_continues_same_task_once(setup, reply_before_settle):
    service, event, access, _clock = setup
    task = service.accept(event, access, "Need a chosen repository")
    claim = service.claim("worker")
    packet = {
        "question_id": "question-1",
        "text": "要操作哪个仓库？",
        "checkpoint": "No external changes yet",
    }
    assert service.ask(claim["run_id"], claim["fencing_token"], "worker", packet)["waiting"]
    answer = event.model_copy(update={"event_id": "answer", "platform_message_id": "answer-message"})
    if reply_before_settle:
        service.add_input(task["public_id"], answer, access, "org/repo", question_id="question-1")
    waiting_result = {"status": "waiting_input", "summary": "已向用户询问目标仓库。"}
    state = service.submit(claim["run_id"], claim["fencing_token"], "worker", waiting_result)["state"]
    assert state == ("QUEUED" if reply_before_settle else "WAITING_INPUT")
    if not reply_before_settle:
        service.add_input(task["public_id"], answer, access, "org/repo", question_id="question-1")
    replay = service.add_input(task["public_id"], answer, access, "org/repo", question_id="question-1")
    assert replay["accepted"]
    resumed = service.claim("worker")
    assert resumed["task_id"] == task["id"] and resumed["run_id"] != claim["run_id"]
    assert resumed["continuation"]["checkpoint"] == packet["checkpoint"]
    assert [value["text"] for value in resumed["continuation"]["inputs"]] == ["org/repo"]
    assert service.claim("another") is None
    with service.db.engine.connect() as conn:
        assert len(conn.execute(select(inputs)).all()) == 1
        assert conn.execute(select(waits.c.state)).scalar_one() == "CONSUMED"
        assert len(conn.execute(select(notifications)).all()) == 1


@pytest.mark.parametrize("settled", [True, False])
def test_cancel_dominates_wait_and_late_reply(setup, settled):
    service, event, access, _clock = setup
    task = service.accept(event, access, "Ask a question")
    claim = service.claim("worker")
    service.ask(
        claim["run_id"],
        claim["fencing_token"],
        "worker",
        {
            "question_id": "question-1",
            "text": "目标是什么？",
            "checkpoint": "Awaiting goal",
        },
    )
    if settled:
        service.submit(
            claim["run_id"],
            claim["fencing_token"],
            "worker",
            {
                "status": "waiting_input",
                "summary": "正在等待目标。",
            },
        )
    assert service.cancel(task["public_id"], access)["state"] == (
        "CANCELLED" if settled else "CANCEL_REQUESTED"
    )
    answer = event.model_copy(update={"event_id": "late-answer"})
    with pytest.raises(PermissionError):
        service.add_input(task["public_id"], answer, access, "Too late", question_id="question-1")
    assert service.question_for(answer) is None
    if not settled:
        with pytest.raises(PermissionError):
            service.submit(
                claim["run_id"], claim["fencing_token"], "worker", {"status": "completed", "summary": "Late"}
            )
        assert service.stopped(claim["run_id"], claim["fencing_token"], "worker")["state"] == "CANCELLED"
    assert service.claim("another") is None


def test_question_reply_mapping_disambiguates_multiple_waits(setup):
    from sqlalchemy import update

    service, event, access, _clock = setup
    ids = []
    for index in range(2):
        source = event.model_copy(update={"event_id": f"source-{index}"})
        task = service.accept(source, access, "Choose target")
        claim = service.claim("worker")
        question_id = f"question-{index}"
        service.ask(
            claim["run_id"],
            claim["fencing_token"],
            "worker",
            {
                "question_id": question_id,
                "text": "请提供目标。",
                "checkpoint": "Awaiting target",
            },
        )
        service.submit(
            claim["run_id"],
            claim["fencing_token"],
            "worker",
            {
                "status": "waiting_input",
                "summary": "等待目标。",
            },
        )
        with service.db.transaction() as conn:
            conn.execute(
                update(outbox)
                .where(outbox.c.origin_id == question_id)
                .values(state="SENT", platform_message_id=f"bot-question-{index}")
            )
        ids.append((question_id, task["public_id"]))
    answer = event.model_copy(update={"event_id": "answer"})
    assert service.question_for(answer) is None
    mapped = service.question_for(answer.model_copy(update={"reply_to": "bot-question-1"}))
    assert mapped == {"id": ids[1][0], "public_id": ids[1][1]}
