"""Timers use real task trees and transactions, with no model or wall-clock waits."""
import json
from types import SimpleNamespace

import pytest
from sqlalchemy import func, select, update
from test_agent_runtime import runtime as runtime  # noqa: PLC0414
from test_host_controls import control_runtime as control_runtime  # noqa: PLC0414
from test_project_scopes import project_core as project_core  # noqa: PLC0414

from qqbot.domain.messages import AccessContext
from qqbot.runtime.schedules import ScheduleService, ScheduleSpec
from qqbot.storage.schedule_schema import schedule_fires
from qqbot.storage.task_schema import tasks


@pytest.fixture
def timers(runtime):
    core = SimpleNamespace(db=runtime.db, clock=runtime.clock, sessions=runtime.sessions,
        tasks=runtime.service, allowed=lambda _: True, permissions=runtime.service.permissions,
        access=lambda event: AccessContext(principal=event.principal, conversation=event.conversation, role="owner"),
        context_for=lambda event, access, **kwargs: {"scope": event.conversation.key, "principal": event.principal.key})
    core.structured_goal_context = lambda event, spec, workspace_id, conn: (
        event, core.context_for(event, core.access(event), workspace_id=workspace_id))
    return ScheduleService(core), runtime


def roots(runtime):
    with runtime.db.engine.connect() as conn:
        return conn.execute(select(tasks).where(tasks.c.kind == "goal")).mappings().all()


def test_cron_catches_up_only_latest_and_recovery_is_idempotent(timers):
    service, runtime = timers
    schedule = service.create(runtime.event, {"goal": "检查项目", "cron": "* * * * *", "timezone": "UTC"},
                              idempotency_key="minute")
    runtime.clock.advance(3600)
    service.once()
    assert len(roots(runtime)) == 1
    receipt = service.get(runtime.event, schedule["id"])
    assert receipt["last_fire_at"] == runtime.clock.now().isoformat()
    ScheduleService(service.core).once()
    assert len(roots(runtime)) == 1
    runtime.clock.advance(60)
    service.once()
    assert len(roots(runtime)) == 2  # Identical goals from distinct timer fires stay independent.
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(schedule_fires)).scalar_one() == 2


def test_one_shot_timezone_and_disable_resume(timers):
    service, runtime = timers
    item = service.create(runtime.event, {"goal": "检查项目", "run_at": "2026-11-08T08:01:00+08:00"},
                          idempotency_key="once")
    service.update(runtime.event, item["id"], {"enabled": False}, expected_revision=1)
    runtime.clock.advance(120)
    service.once()
    assert roots(runtime) == []
    service.update(runtime.event, item["id"], {"enabled": True}, expected_revision=2)
    service.once()
    assert len(roots(runtime)) == 1
    assert not service.get(runtime.event, item["id"])["enabled"]
    service.once()
    assert len(roots(runtime)) == 1


def test_create_update_delete_replay_and_scope(timers):
    service, runtime = timers
    spec = {"goal": "检查项目", "cron": "0 9 * * *"}
    item = service.create(runtime.event, spec, idempotency_key="daily")
    assert service.create(runtime.event, spec, idempotency_key="daily")["id"] == item["id"]
    with pytest.raises(ValueError, match="different content"):
        service.create(runtime.event, spec | {"goal": "另一个目标"}, idempotency_key="daily")
    other = runtime.event.model_copy(update={"conversation": runtime.event.conversation.model_copy(
        update={"remote_conversation_id": "elsewhere"})})
    assert service.list(other) == []
    with pytest.raises(PermissionError):
        service.get(other, item["id"])
    with pytest.raises(ValueError, match="revision"):
        service.update(runtime.event, item["id"], {"enabled": False}, expected_revision=2)
    service.delete(runtime.event, item["id"], expected_revision=1)
    assert service.list(runtime.event) == []
    with pytest.raises(ValueError, match="deleted"):
        service.create(runtime.event, spec, idempotency_key="daily")


def test_fire_failure_rolls_back_task_and_receipt(timers, monkeypatch):
    service, runtime = timers
    item = service.create(runtime.event, {"goal": "检查项目", "cron": "* * * * *"}, idempotency_key="rollback")
    runtime.clock.advance(60)
    original = service.core.tasks.graph.submit

    def fail_after_submit(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("crash after goal insert")

    monkeypatch.setattr(service.core.tasks.graph, "submit", fail_after_submit)
    with pytest.raises(RuntimeError):
        service.once()
    assert roots(runtime) == []
    assert service.get(runtime.event, item["id"])["last_fire_at"] is None
    monkeypatch.setattr(service.core.tasks.graph, "submit", original)
    service.once()
    assert len(roots(runtime)) == 1


def test_revoked_owner_stops_future_firing(timers):
    service, runtime = timers
    item = service.create(runtime.event, {"goal": "检查项目", "cron": "* * * * *"}, idempotency_key="revoke")
    runtime.clock.advance(60)
    service.core.allowed = lambda _: False
    service.once()
    assert roots(runtime) == []
    with runtime.db.engine.connect() as conn:
        from qqbot.storage.schedule_schema import schedules
        row = conn.execute(select(schedules).where(schedules.c.id == item["id"])).mappings().one()
        assert not row["enabled"]
        assert row["last_error"] == "PermissionError"


@pytest.mark.parametrize("spec", [
    {"cron": "* * * * * *"}, {"cron": "bad"}, {"cron": "R * * * *"}, {"run_at": "2026-11-08T01:00:00"},
    {"cron": "* * * * *", "timezone": "Invalid/Zone"},
    {"cron": "* * * * *", "success_checks": ["made_up"]},
    {"cron": "* * * * *", "requested_outputs": ["../secret"]},
])
def test_schedule_validation(spec):
    with pytest.raises(ValueError):
        ScheduleSpec.model_validate_json(json.dumps({"goal": "检查项目"} | spec))


async def test_real_core_fire_persists_structured_source_and_authorized_project(control_runtime):
    runtime = control_runtime
    service = ScheduleService(runtime.core)
    source, _ = runtime.core.inbox.accept(runtime.event("定期检查项目"))
    runtime.core.inbox.finish(source.event_id)
    schedule = service.create(source, {"goal": "生成检查记录", "cron": "* * * * *",
        "success_checks": ["artifact"], "requested_outputs": ["outputs/report.txt"]}, idempotency_key="real-source")
    runtime.core.clock.advance(60)
    service.once()
    receipt = service.get(source, schedule["id"])
    with runtime.core.db.engine.connect() as conn:
        task = conn.execute(select(tasks).where(tasks.c.id == receipt["last_task_id"])).mappings().one()
        context = json.loads(task["context_json"])
        from qqbot.domain import InboundEvent
        event = InboundEvent.model_validate_json(task["event_json"])
    assert context["workspace_id"] == "personal"
    assert context["schedule_fire_id"] == event.event_id
    assert context["structured_request"]["request"]["success_checks"] == ["artifact"]
    assert context["structured_request"]["request"]["requested_outputs"] == ["outputs/report.txt"]
    assert runtime.core.validate_context(runtime.core.access(event), context)
    service.once()
    with runtime.core.db.engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(schedule_fires)).scalar_one() == 1


@pytest.mark.parametrize(("operation", "arguments"), [
    ("unknown", {}), ("list", {"owner_id": "someone"}), ("get", {}),
    ("get", {"schedule_id": "x", "ignored": True}), ("get", {"schedule_id": 2}),
    ("cancel", {"schedule_id": "x"}), ("cancel", {"schedule_id": "x", "expected_revision": True}),
    ("update", {"schedule_id": "x", "expected_revision": 1, "changes": []}),
    ("delete", {"schedule_id": "x", "expected_revision": 0}), ("list", []),
])
def test_schedule_operation_rejects_malformed_arguments(timers, operation, arguments):
    service, runtime = timers
    with pytest.raises(ValueError):
        service.invoke(runtime.event, operation, arguments)


def project_event(core, name):
    from qqbot.domain import ConversationRef, InboundEvent, PrincipalRef, Surface, TextSegment
    return InboundEvent(event_id=name, event_type="message.created", platform_message_id=name,
        conversation=ConversationRef(deployment_id=core.settings.deployment_id, adapter_instance_id="qq",
            bot_identity_id="bot", surface=Surface.PRIVATE, remote_conversation_id="300"),
        principal=PrincipalRef(adapter_instance_id="qq", identity_namespace="qq_app:app", remote_user_id="300"),
        occurred_at=core.clock.now(), received_at=core.clock.now(), trace_id=name,
        segments=(TextSegment(text="定时检查项目"),))


def test_schedule_creation_freezes_original_claim_project_after_focus_switch(project_core):
    core = project_core
    service = ScheduleService(core)
    source, _ = core.inbox.accept(project_event(core, "schedule-alpha-input"))
    original = core.sessions.enqueue(source, core.access(source), context=core.context_for(source, workspace_id="alpha"))
    claim = core.sessions.claim("alpha-leader")
    assert claim["context"]["workspace_id"] == "alpha"
    with core.db.transaction() as conn:
        core.sessions.dialogue(conn, project_event(core, "switch-beta"), workspace_id="beta")
    spec = {"goal": "检查原项目", "cron": "* * * * *"}
    item = service.create(source, spec, idempotency_key="alpha-timer")
    assert item["spec"]["workspace_id"] == "alpha"
    assert core.sessions.workspace_for(project_event(core, "current-focus")) == "beta"
    assert service.create(source, spec, idempotency_key="alpha-timer")["id"] == item["id"]
    with core.db.engine.connect() as conn:
        from qqbot.storage.schedule_schema import schedules
        row = conn.execute(select(schedules).where(schedules.c.id == item["id"])).mappings().one()
        assert row["dialogue_id"] == original["dialogue_id"]
    core.clock.advance(60)
    service.once()
    receipt = service.get(source, item["id"])
    with core.db.engine.connect() as conn:
        root = conn.execute(select(tasks).where(tasks.c.id == receipt["last_task_id"])).mappings().one()
    assert root["origin_dialogue_id"] == original["dialogue_id"]
    assert json.loads(root["context_json"])["workspace_id"] == "alpha"


def test_schedule_queries_and_mutations_cannot_cross_frozen_project(project_core):
    core = project_core
    service = ScheduleService(core)
    source, _ = core.inbox.accept(project_event(core, "alpha-schedule-query"))
    core.sessions.enqueue(source, core.access(source), context=core.context_for(source, workspace_id="alpha"))
    alpha = service.create(source, {"goal": "alpha私有计划", "cron": "0 8 * * *"}, idempotency_key="alpha")
    beta_event, _ = core.inbox.accept(project_event(core, "beta-schedule-create"))
    with core.db.transaction() as conn:
        core.sessions.dialogue(conn, beta_event, workspace_id="beta")
    beta = service.create(beta_event, {"goal": "beta私有计划", "cron": "0 9 * * *"}, idempotency_key="beta")
    assert [row["id"] for row in service.list(source)] == [alpha["id"]]
    for operation, arguments in (
        ("get", {"schedule_id": beta["id"]}),
        ("update", {"schedule_id": beta["id"], "changes": {"enabled": False}, "expected_revision": 1}),
        ("cancel", {"schedule_id": beta["id"], "expected_revision": 1}),
    ):
        with pytest.raises(PermissionError, match="another authenticated scope"):
            service.invoke(source, operation, arguments)
    from test_project_scopes import host_event
    admin_event = host_event(core, "schedule-admin")
    assert {row["id"] for row in service.list(admin_event)} == {alpha["id"], beta["id"]}


def attached_schedule(core, key):
    from qqbot.domain import MediaSegment
    source = project_event(core, key)
    source = source.model_copy(update={"segments": (*source.segments,
        MediaSegment(type="file", attachment_ref="original-sheet", source_url="https://cdn.example/input.xlsx",
            filename="input.xlsx", mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", size=128),
        MediaSegment(type="image", attachment_ref="original-image", image_url="https://cdn.example/chart.png",
            filename="chart.png", mime_type="image/png", size=64))})
    source, _ = core.inbox.accept(source)
    core.sessions.enqueue(source, core.access(source), context=core.context_for(source, workspace_id="alpha"))
    service = ScheduleService(core)
    item = service.create(source, {"goal": "每天分析这份表格和图片", "cron": "* * * * *"}, idempotency_key=key)
    return source, service, item


def test_schedule_fire_keeps_original_attachment_source_and_metadata(project_core):
    from qqbot.domain import InboundEvent, MediaSegment
    core = project_core
    source, service, schedule = attached_schedule(core, "attached-timer")
    core.clock.advance(1)
    unrelated = project_event(core, "unrelated-later-file")
    unrelated = unrelated.model_copy(update={"segments": (*unrelated.segments,
        MediaSegment(type="file", attachment_ref="unrelated", source_url="https://cdn.example/other.xlsx",
            filename="other.xlsx"))})
    unrelated, _ = core.inbox.accept(unrelated)
    core.sessions.enqueue(unrelated, core.access(unrelated), context=core.context_for(unrelated, workspace_id="alpha"))
    core.clock.advance(59)
    service.once()
    receipt = service.get(source, schedule["id"])
    with core.db.engine.connect() as conn:
        root = conn.execute(select(tasks).where(tasks.c.id == receipt["last_task_id"])).mappings().one()
    context = json.loads(root["context_json"])
    fire_event = InboundEvent.model_validate_json(root["event_json"])
    assert len(context["files"]) == len(context["images"]) == 1
    file = context["files"][0]
    assert file["source"] == source.event_id and file["source"] != fire_event.event_id
    assert file["attachment_ref"] == "original-sheet" and file["url"] == "https://cdn.example/input.xlsx"
    assert file["filename"] == "input.xlsx" and file["size"] == 128
    assert file["mime_type"] == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    assert context["images"][0]["source"] == source.event_id
    assert {item["id"] for item in context["source_manifest"]} == {source.event_id, fire_event.event_id}
    assert core.validate_context(core.access(fire_event), context)


@pytest.mark.parametrize("revocation", ["recalled", "forgotten"])
def test_schedule_attachment_revocation_blocks_later_fires(project_core, revocation):
    from qqbot.storage.memory_sync_schema import memory_barriers, memory_owners
    core = project_core
    source, service, schedule = attached_schedule(core, "revoked-attached-timer")
    core.clock.advance(60)
    service.once()
    first = service.get(source, schedule["id"])
    assert first["last_task_id"] is not None
    if revocation == "recalled":
        recalled = source.model_copy(update={"event_id": "recall-upload", "event_type": "message.recalled", "segments": ()})
        core.inbox.accept(recalled)
        core.inbox.finish(recalled.event_id)
    else:
        scope = core.knowledge.scope(source, workspace_id="alpha")
        with core.db.transaction() as conn:
            conn.execute(update(memory_owners).where(memory_owners.c.id == scope.key).values(epoch=1))
            conn.execute(memory_barriers.insert().values(id="forget-upload", scope_id=scope.key,
                epoch=1, operation_id="forget", source_id=source.event_id, created_at=core.clock.now().isoformat()))
    core.clock.advance(60)
    service.once()
    current = service.get(source, schedule["id"])
    assert not current["enabled"] and current["last_error"] == "PermissionError"
    assert current["last_task_id"] == first["last_task_id"]
    with core.db.engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(schedule_fires)).scalar_one() == 1
        assert conn.execute(select(func.count()).select_from(tasks).where(tasks.c.kind == "goal")).scalar_one() == 1


def test_real_qq_missing_original_inbox_source_does_not_create_fake_fire_source(project_core):
    from qqbot.storage.schema import inbox
    core = project_core
    source = project_event(core, "missing-original-upload")
    service = ScheduleService(core)
    with pytest.raises(PermissionError, match="original source"):
        service.create(source, {"goal": "检查原文件", "cron": "* * * * *"}, idempotency_key="missing-source")
    with core.db.engine.connect() as conn:
        assert not conn.execute(select(inbox).where(inbox.c.id == source.event_id)).first()
        assert not conn.execute(select(schedule_fires)).first()
        assert not conn.execute(select(tasks)).first()


def test_schedule_freezes_referenced_upload_before_dialogue_history_rolls_over(project_core):
    from qqbot.domain import MediaSegment, TextSegment
    from qqbot.storage.schedule_schema import schedules
    core = project_core
    upload = project_event(core, "earlier-scheduled-upload")
    upload = upload.model_copy(update={"segments": (TextSegment(text="这是原始表格"), MediaSegment(type="file",
        attachment_ref="earlier-sheet", source_url="https://cdn.example/earlier.xlsx", filename="earlier.xlsx"))})
    upload, _ = core.inbox.accept(upload)
    core.sessions.enqueue(upload, core.access(upload), context=core.context_for(upload, workspace_id="alpha"))
    core.clock.advance(1)
    request = project_event(core, "schedule-referencing-upload")
    request = request.model_copy(update={"segments": (TextSegment(text="每天处理上面的文件"),)})
    request, _ = core.inbox.accept(request)
    core.sessions.enqueue(request, core.access(request), context=core.context_for(request, workspace_id="alpha"))
    service = ScheduleService(core)
    schedule = service.create(request, {"goal": "每天处理上面的文件", "cron": "* * * * *"},
                              idempotency_key="reference-earlier-upload")
    with core.db.engine.connect() as conn:
        frozen = json.loads(conn.execute(select(schedules.c.source_context_json).where(
            schedules.c.id == schedule["id"])).scalar_one())
    assert frozen["files"][0]["source"] == upload.event_id
    assert "retrieved_data" not in frozen and "messages" not in frozen
    for index in range(12):
        core.clock.advance(1)
        later, _ = core.inbox.accept(project_event(core, f"later-message-{index}"))
        core.sessions.enqueue(later, core.access(later), context=core.context_for(later, workspace_id="alpha"))
    assert core.context_for(request, workspace_id="alpha")["files"] == []
    core.clock.advance(60)
    service.once()
    fired = service.get(request, schedule["id"])
    with core.db.engine.connect() as conn:
        context = json.loads(conn.execute(select(tasks.c.context_json).where(
            tasks.c.id == fired["last_task_id"])).scalar_one())
    assert context["files"][0]["source"] == upload.event_id
    assert context["files"][0]["attachment_ref"] == "earlier-sheet"
    assert {item["id"] for item in context["source_manifest"]} == {
        upload.event_id, request.event_id, context["schedule_fire_id"]}
