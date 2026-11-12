"""Real ingress routes into persistent turns; controls never consume model slots."""
import json

from sqlalchemy import select

from qqbot.app import create_app
from qqbot.config import Settings
from qqbot.domain import Surface, TextSegment
from qqbot.storage.agent_schema import dialogue_inputs, dialogues
from qqbot.storage.task_schema import runs, tasks


def make_core(tmp_path, official_config, clock):
    from qqbot.config import OwnerConfig, QQOwnerBinding
    settings = Settings(database=str(tmp_path / "leader.db"), adapters=(official_config,), tasks_enabled=True,
        owner=OwnerConfig(qq_bindings=(QQOwnerBinding(app_id="app", user_openid="300", verified_at=clock.now().isoformat()),)))
    return create_app(settings, clock=clock, run_background=False).state.core


async def test_ingress_persists_turn_and_deduplicates(tmp_path, official_config, official_event, clock):
    from conftest import official_payload
    core = make_core(tmp_path, official_config, clock)
    try:
        event = core.adapters["qq"].normalize(official_payload(official_event, text="帮我处理这个任务"))
        core.inbox.accept(event)
        await core.tick(dispatch=False)
        await core.process(event)
        with core.db.engine.connect() as conn:
            items = conn.execute(select(dialogue_inputs)).mappings().all()
            assert len(items) == 1
            assert items[0]["state"] == "PENDING"
            assert json.loads(items[0]["context_json"])["source_manifest"][0]["id"] == event.event_id
            assert not conn.execute(select(tasks)).first()
        claim = core.sessions.claim("fixture")
        assert claim["role"] == "leader" and claim["purpose"] == "user"
        assert core.sessions.claim("other") is None
    finally:
        await core.stop()
        core.db.close()


async def test_status_does_not_need_free_execution_capacity(tmp_path, official_config, official_event, clock):
    core = make_core(tmp_path, official_config, clock)
    try:
        event = core.adapters["qq"].normalize(official_event)
        core.inbox.accept(event)
        await core.tick(dispatch=False)
        with core.db.engine.connect() as conn:
            assert not conn.execute(select(runs)).first()
            assert not conn.execute(select(dialogue_inputs)).first()
        from qqbot.storage.schema import outbox
        with core.db.engine.connect() as conn:
            assert len(conn.execute(select(outbox).where(outbox.c.state == "PENDING")).all()) == 1
    finally:
        await core.stop()
        core.db.close()


async def test_group_requires_invitation_and_private_uses_own_dialogue(tmp_path, official_config, official_event, clock):
    from conftest import official_payload
    core = make_core(tmp_path, official_config, clock)
    try:
        event = core.adapters["qq"].normalize(official_payload(official_event, text="普通群消息"))
        event = event.model_copy(update={"mentions_bot": False})
        core.inbox.accept(event)
        await core.process(event)
        with core.db.engine.connect() as conn:
            assert not conn.execute(select(dialogue_inputs)).first()
        private = core.adapters["qq"].normalize(official_payload(official_event, private=True, message_id="private", text="你好"))
        assert private.conversation.surface == Surface.PRIVATE
        core.inbox.accept(private)
        await core.process(private)
        with core.db.engine.connect() as conn:
            assert len(conn.execute(select(dialogues)).all()) == 1
    finally:
        await core.stop()
        core.db.close()


async def test_recalled_source_blocks_old_task_context(tmp_path, official_config, official_event, clock):
    core = make_core(tmp_path, official_config, clock)
    try:
        event = core.adapters["qq"].normalize(official_event).model_copy(update={"segments": (TextSegment(text="帮忙"),)})
        event, _ = core.inbox.accept(event)
        context = core.context_for(event)
        assert core.validate_context(core.access(event), context)
        recall = event.model_copy(update={"event_id": "recall", "event_type": "message.recalled"})
        core.inbox.accept(recall)
        assert not core.validate_context(core.access(event), context)
    finally:
        await core.stop()
        core.db.close()


async def test_followup_keeps_only_same_dialogue_live_images(tmp_path, official_config, official_event, clock):
    from conftest import official_payload

    from qqbot.domain import MediaSegment
    core = make_core(tmp_path, official_config, clock)
    try:
        image = core.adapters["qq"].normalize(official_payload(official_event, private=True, text="图片", message_id="pic"))
        image = image.model_copy(update={"segments": (MediaSegment(type="image", attachment_ref="image", image_url="https://example.com/a.png"),)})
        image, _ = core.inbox.accept(image)
        await core.process(image)
        follow = core.adapters["qq"].normalize(official_payload(official_event, private=True, text="解释上图", message_id="follow"))
        follow, _ = core.inbox.accept(follow)
        assert core.context_for(follow)["images"][0]["source"] == image.event_id
        core.inbox.accept(image.model_copy(update={"event_id": "recalled-pic", "event_type": "message.recalled"}))
        assert core.context_for(follow)["images"] == []
        with core.db.transaction() as conn:
            core.sessions.dialogue(conn, follow, new_title="另一对话")
        assert core.context_for(follow)["images"] == []
    finally:
        await core.stop()
        core.db.close()


async def test_memory_outage_keeps_chat_and_epoch_barrier(tmp_path, official_config, official_event, clock):
    from unittest.mock import AsyncMock

    from qqbot.contracts.memory import MemoryScope, MemorySearchResult
    from qqbot.storage.memory_sync_schema import memory_owners
    core = make_core(tmp_path, official_config, clock)
    try:
        scope = MemoryScope(owner_id=core.settings.owner.id)
        core.knowledge.scope = lambda event: scope
        core.openviking.config = core.openviking.config.model_copy(update={"enabled": True})
        core.openviking.enabled = lambda scope: True
        core.openviking._owner = lambda scope: None
        core.openviking.search = AsyncMock(side_effect=TimeoutError("offline"))
        event = core.adapters["qq"].normalize(official_event)
        result = await core.knowledge.prepare(event, {})
        assert result["memory_status"] == "degraded"
        assert core.knowledge.valid(result)
        core.openviking.search = AsyncMock(return_value=MemorySearchResult(status="blocked", memory_epoch=2))
        result = await core.knowledge.prepare(event, {})
        # A new turn without recalled data can proceed while a mutation is pending.
        with core.db.transaction() as conn:
            conn.execute(memory_owners.insert().values(id=scope.key, scope_json=scope.model_dump_json(),
                binding_json="{}", binding_hash="x", remote_identity="test", epoch=2, state="MUTATING", updated_at=clock.now().isoformat()))
        assert core.knowledge.valid(result)
        assert not core.knowledge.valid({**result, "memory_epoch": 1})
        assert not core.knowledge.valid({**result, "retrieved_data": [{"content": "old"}]})
    finally:
        await core.stop()
        core.db.close()


async def test_retired_transport_inputs_and_tasks_cannot_execute(tmp_path, official_config, official_event, clock):
    from qqbot.domain import AccessContext
    from qqbot.storage.schema import inbox
    core = make_core(tmp_path, official_config, clock)
    try:
        event = core.adapters["qq"].normalize(official_event)
        retired = event.model_copy(update={"event_id": "retired-input", "conversation": event.conversation.model_copy(
            update={"adapter_instance_id": "retired"}), "principal": event.principal.model_copy(update={"adapter_instance_id": "retired"})})
        retired, _ = core.inbox.accept(retired)
        access = AccessContext(conversation=retired.conversation, principal=retired.principal, role="admin")
        old = core.tasks.accept(retired, access, "old unfinished task")
        current = core.enqueue_admin_task("current host task", idempotency_key="new-task")
        core.retire_adapters()
        with core.db.engine.connect() as conn:
            assert conn.execute(select(inbox.c.state).where(inbox.c.id == retired.event_id)).scalar_one() == "FAILED"
            assert conn.execute(select(tasks.c.state).where(tasks.c.id == old["id"])).scalar_one() == "FAILED"
            assert conn.execute(select(tasks.c.state).where(tasks.c.id == current["task_id"])).scalar_one() == "WAITING_CHILDREN"
    finally:
        await core.stop()
        core.db.close()


async def test_reply_image_and_source_permission_preserve_dialogue_boundary(tmp_path, official_config, official_event, clock):
    from conftest import official_payload

    from qqbot.domain import MediaSegment
    from qqbot.permissions import PermissionRule
    core = make_core(tmp_path, official_config, clock)
    try:
        image = core.adapters["qq"].normalize(official_payload(official_event, private=True, text="", message_id="old-pic"))
        image = image.model_copy(update={"segments": (MediaSegment(type="image", attachment_ref="image", image_url="https://example.com/old.png"),)})
        image, _ = core.inbox.accept(image)
        await core.process(image)
        clock.advance(601)
        follow = core.adapters["qq"].normalize(official_payload(official_event, private=True, text="解释回复图片", message_id="new"))
        follow, _ = core.inbox.accept(follow)
        assert core.context_for(follow)["images"] == []
        follow = follow.model_copy(update={"reply_to": image.platform_message_id})
        context = core.context_for(follow)
        assert context["images"][0]["source"] == image.event_id
        assert "seed" not in context
        core.permissions.upsert(PermissionRule(id="deny-image", effect="deny", actions=("history.source.read",),
            resources=("event:" + image.event_id,)), expected_version=0, actor="test")
        assert not core.validate_context(core.access(follow), context)
        assert core.context_for(follow)["images"] == []
    finally:
        await core.stop()
        core.db.close()


def test_admin_task_has_host_identity_explicit_contract_and_idempotency(tmp_path, official_config, clock, monkeypatch):
    from fastapi.testclient import TestClient
    monkeypatch.setenv("QQBOT_ADMIN_TOKEN", "admin-fixture")
    app = create_app(Settings(database=str(tmp_path / "admin.db"), adapters=(official_config,), tasks_enabled=True),
        clock=clock, run_background=False)
    auth = {"Authorization": "Bearer admin-fixture", "Idempotency-Key": "host-contract"}
    body = {"goal": "生成报告", "requested_outputs": ["reports/result.txt"], "success_checks": ["artifact"]}
    with TestClient(app) as client:
        assert client.post("/api/v1/tasks", json=body).status_code == 401
        response = client.post("/api/v1/tasks", headers=auth, json=body)
        assert response.status_code == 200, response.text
        assert client.post("/api/v1/tasks", headers=auth, json=body).json() == response.json()
        with app.state.db.engine.connect() as conn:
            saved = conn.execute(select(tasks).where(tasks.c.kind == "work")).mappings().one()
        goal = json.loads(saved["goal_json"])
        assert goal["requested_outputs"] == ["reports/result.txt"]
        assert goal["success_checks"] == ["artifact"]
        identity = json.loads(saved["event_json"])
        assert identity["principal"]["identity_namespace"] == "admin_api"
        assert identity["conversation"]["adapter_instance_id"] == "admin_api"


async def test_recovery_has_job_quiescence_guard_before_any_admission(tmp_path, official_config, clock, monkeypatch):
    core = make_core(tmp_path, official_config, clock)
    recovered = []
    def recover():
        assert core.scheduler is not None
        assert core.tasks.job_tree_quiescent.__self__ is core.job_runtime
        assert not core.scheduler.jobs_recovered
        recovered.append(True)
    monkeypatch.setattr(core.tasks, "recover", recover)
    monkeypatch.setattr(core.recovery, "recover_workers", recover)
    monkeypatch.setattr(core.leader, "recover", recover)
    monkeypatch.setattr(core.supervisor, "start", lambda *args, **kwargs: None)
    core.stopping = True
    try:
        await core.run()
        assert len(recovered) == 3
    finally:
        await core.stop()
        core.db.close()
