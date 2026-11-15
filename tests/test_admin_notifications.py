"""Console tasks persist visible replies without pretending to send to an adapter."""
import json
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, update
from test_agent_runtime import leader_complete, root_goal, worker_complete
from test_agent_runtime import runtime as runtime  # noqa: PLC0414

from qqbot.app import create_app
from qqbot.config import Settings
from qqbot.domain import InboundEvent
from qqbot.storage.schema import outbox
from qqbot.storage.task_schema import notifications, tasks


def test_console_task_progress_and_result_are_visible_without_external_delivery(tmp_path, clock, monkeypatch):
    monkeypatch.setenv("QQBOT_ADMIN_TOKEN", "console-token")
    app = create_app(Settings(database=str(tmp_path / "console.db"), tasks_enabled=True),
        clock=clock, run_background=False)
    auth = {"Authorization": "Bearer console-token", "Idempotency-Key": "console-goal"}
    with TestClient(app) as client:
        created = client.post("/api/v1/tasks", headers=auth, json={"goal": "Review project status"})
        assert created.status_code == 200, created.text
        assert client.post("/api/v1/tasks", headers=auth,
            json={"goal": "Review project status"}).json() == created.json()
        core = app.state.core
        runtime = SimpleNamespace(service=core.tasks, leader=core.leader)
        worker_complete(runtime)
        claim = core.sessions.claim("leader")
        assert claim["purpose"] == "finalize"
        identity = (claim["run_id"], claim["fencing_token"], "leader")
        progress = client.portal.call(core.leader.invoke, "notify", identity, {"text": "核对已完成，正在整理结果"})
        assert progress["accepted"]
        assert client.portal.call(core.leader.invoke, "notify", identity,
            {"text": "核对已完成，正在整理结果"})["notification_id"] == progress["notification_id"]
        url = "/api/v1/tasks/" + created.json()["public_id"]
        assert client.get(url, headers=auth).json()["notifications"][0]["body"] == "核对已完成，正在整理结果"
        leader_complete(runtime, claim, "项目状态已核实")
        leader_complete(runtime, claim, "项目状态已核实")
        result = client.get(url, headers=auth).json()
        assert result["state"] == "SUCCEEDED" and result["result"]["summary"] == "项目状态已核实"
        assert [notice["body"] for notice in result["notifications"]] == ["核对已完成，正在整理结果", "项目状态已核实"]
        assert all(notice["state"] == "ACCEPTED" for notice in result["notifications"])
        assert result["progress"]["delivery"]["required"] is False
        assert result["progress"]["delivery"]["delivered"] is False
        assert result["progress"]["current_action"] == "目标已完成，结果可在控制台查看"
        assert result["deliveries"] == []
        with core.db.engine.connect() as conn:
            assert conn.execute(select(outbox)).first() is None
            assert len(conn.execute(select(notifications)).all()) == 2
        core.tasks.context_validator = lambda *_: False
        hidden = client.get(url, headers=auth).json()
        assert hidden["notifications"] == [] and hidden["result"] is None
        listed = client.get("/api/v1/tasks", headers=auth).json()
        assert listed[0]["notifications"] == [] and listed[0]["result"] is None


def host_task(runtime):
    result = root_goal(runtime)
    with runtime.db.transaction() as conn:
        task = conn.execute(select(tasks).where(tasks.c.id == result["task_id"])).mappings().one()
        event = InboundEvent.model_validate_json(task["event_json"])
        event = event.model_copy(update={
            "conversation": event.conversation.model_copy(update={"adapter_instance_id": "admin_api", "bot_identity_id": "host"}),
            "principal": event.principal.model_copy(update={"adapter_instance_id": "admin_api", "identity_namespace": "admin_api",
                "remote_user_id": event.conversation.remote_conversation_id})})
        conn.execute(update(tasks).where(tasks.c.id == task["id"]).values(event_json=event.model_dump_json(),
            conversation=event.conversation.key, requester=event.principal.key))
    return result


def test_internal_progress_deduplicates_and_drives_notification_due(runtime):
    root = host_task(runtime)
    claim = runtime.service.claim("worker")
    with runtime.db.transaction() as conn:
        task = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
        runtime.clock.advance(100)
        assert runtime.service._notification_due(conn, task)
        first = runtime.service._notification(conn, task, claim["run_id"], "已核对资料", "progress", "normal", "one")
        assert runtime.service._notification(conn, task, claim["run_id"], "已核对资料", "progress", "normal", "two") == first
        assert not runtime.service._notification_due(conn, task)
        with pytest.raises(ValueError, match="identity conflict"):
            runtime.service._notification(conn, task, claim["run_id"], "不同内容", "progress", "normal", "one")
        assert conn.execute(select(outbox)).first() is None
        runtime.clock.advance(90)
        assert runtime.service._notification_due(conn, task)


def test_notification_status_is_bounded_and_filters_old_revision_and_revocation(runtime):
    root = host_task(runtime)
    claim = runtime.service.claim("worker")
    with runtime.db.transaction() as conn:
        task = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
        for number in range(22):
            runtime.service._notification(conn, task, claim["run_id"], f"资料核对记录 {number}", "progress", "normal", str(number))
        view = runtime.service.graph.status(conn, task)
        assert len(view["notifications"]) == 20
        assert view["notifications"][0]["body"] == "资料核对记录 2"
        conn.execute(update(tasks).where(tasks.c.id == task["id"]).values(goal_revision=1))
        revised = conn.execute(select(tasks).where(tasks.c.id == task["id"])).mappings().one()
        assert runtime.service.graph.status(conn, revised)["notifications"] == []
        runtime.service.context_validator = lambda *_: False
        assert runtime.service.graph.status(conn, task, access=runtime.access)["notifications"] == []


async def test_qq_notifications_still_queue_and_missing_adapter_fails(runtime):
    root = root_goal(runtime)
    claim = runtime.service.claim("worker")
    with runtime.db.transaction() as conn:
        task = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
        runtime.service._notification(conn, task, claim["run_id"], "资料核对完成", "result", "important", "qq-result")
        assert runtime.service.graph.status(conn, task)["progress"]["delivery"]["required"] is True
        assert conn.execute(select(outbox.c.state)).scalar_one() == "PENDING"
    await runtime.service.outbox.dispatch_once({})
    with runtime.db.engine.connect() as conn:
        row = conn.execute(select(outbox)).mappings().one()
        assert row["state"] == "FAILED" and json.loads(row["receipt_json"])["error_code"] == "ADAPTER_REMOVED"
        assert conn.execute(select(notifications.c.state)).scalar_one() == "ACCEPTED"


@pytest.mark.parametrize("field,value", [("bot_identity_id", "other"), ("surface", "group")])
def test_reserved_adapter_name_alone_does_not_suppress_delivery(runtime, field, value):
    from qqbot.domain import Surface

    root = host_task(runtime)
    claim = runtime.service.claim("worker")
    with runtime.db.transaction() as conn:
        task = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
        event = InboundEvent.model_validate_json(task["event_json"])
        changed = event.model_copy(update={"conversation": event.conversation.model_copy(
            update={field: Surface.GROUP if field == "surface" else value})})
        task = {**task, "event_json": changed.model_dump_json()}
        runtime.service._notification(conn, task, claim["run_id"], "资料核对完成", "result", "important", "external")
        assert conn.execute(select(outbox)).first() is not None
