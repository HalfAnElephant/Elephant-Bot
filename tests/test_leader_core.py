"""Real ingress routes into persistent turns; controls never consume model slots."""
import json

import pytest
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
        core.knowledge.scope = lambda event, **_: scope
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


async def test_idle_direct_transport_serializes_no_claim_as_json_null(tmp_path, official_config, clock):
    from qqbot.runtime.scheduler import Scheduler
    core = make_core(tmp_path, official_config, clock)
    core.scheduler = Scheduler(core, core.leader)
    try:
        assert await core.scheduler.worker.once() is False
        assert not core.scheduler.pool.entries
    finally:
        await core.stop()
        core.db.close()


def finish_leader(core, claim, summary):
    return core.leader.complete(claim["run_id"], claim["fencing_token"], "fixture",
        {"status": "completed", "summary": summary}, stopped=True, quiescent=False,
        receipt={"kind": "turn_finished", "session_id": claim["session_id"], "run_id": claim["run_id"],
                 "session_revision": "leaf", "input_ids": claim["source_ids"], "inflight": 0, "tools_paired": True})


async def test_fast_final_prevents_delayed_dispatch_ack_and_unknown_result_resend(tmp_path, official_config, official_event, clock):
    from datetime import timedelta
    from types import SimpleNamespace

    from conftest import official_payload
    from sqlalchemy import update

    from qqbot.domain import DeliveryCheck, OutboundEnvelope, SendReceipt
    from qqbot.storage.schema import outbox
    core = make_core(tmp_path, official_config, clock)
    sent = []
    class Adapter:
        config = SimpleNamespace(kind="fixture")
        async def validate_delivery(self, envelope):
            return DeliveryCheck(allowed=True, reason="fixture")
        async def send(self, envelope):
            sent.append(envelope)
            return SendReceipt(state="UNKNOWN")
    try:
        event = core.adapters["qq"].normalize(official_payload(official_event, private=True, text="检查项目", message_id="fast"))
        event, _ = core.inbox.accept(event)
        await core.process(event)
        user = core.sessions.claim("fixture")
        with core.db.transaction() as conn:
            run, claim = core.leader.lease(conn, user["run_id"], user["fencing_token"], "fixture")
            root = core.leader.task_tool(conn, run, claim, {"operation": "tasks.submit", "call_id": "dispatch",
                "arguments": {"goal": "检查项目", "children": [{"goal": "检查", "delegation_key": "only"}]}})
        worker = core.tasks.claim("worker")
        core.tasks.submit(worker["run_id"], worker["fencing_token"], "worker",
            {"status": "completed", "summary": "检查完成"}, receipt={"kind": "turn_finished",
             "session_id": worker["context"]["session_id"], "run_id": worker["run_id"], "session_revision": "leaf",
             "input_ids": worker["context"]["source_input_ids"], "inflight": 0, "tools_paired": True})
        finish_leader(core, user, "已接受，等待执行。")
        with core.db.transaction() as conn:
            ack = conn.execute(select(outbox).where(outbox.c.origin_kind == "chat")).mappings().one()
            conn.execute(update(outbox).where(outbox.c.id == ack["id"]).values(
                state="RETRYABLE", next_attempt_at=(clock.now() + timedelta(seconds=60)).isoformat()))
        finalizer = core.sessions.claim("fixture")
        assert finalizer["purpose"] == "finalize"
        finish_leader(core, finalizer, "项目检查完成。")
        assert not core.pre_send(OutboundEnvelope.model_validate_json(ack["envelope_json"]))
        await core.outbox.dispatch_once({"qq": Adapter()}, pre_send=core.pre_send)
        assert [envelope.text for envelope in sent] == ["项目检查完成。"]
        clock.advance(61)
        with core.db.transaction() as conn:
            core.outbox.retrigger(conn, event.model_copy(update={"event_id": "new-trigger", "platform_message_id": "new"}))
        await core.outbox.dispatch_once({"qq": Adapter()}, pre_send=core.pre_send)
        assert len(sent) == 1
        with core.db.engine.connect() as conn:
            assert conn.execute(select(tasks.c.state).where(tasks.c.id == root["task_id"])).scalar_one() == "SUCCEEDED"
            states = dict(conn.execute(select(outbox.c.origin_kind, outbox.c.state)).all())
            assert states == {"chat": "FAILED", "task": "UNKNOWN"}
            assert conn.execute(select(outbox.c.id).where(outbox.c.origin_kind == "task")).all() == [(sent[0].delivery_id,)]
        assert finish_leader(core, finalizer, "项目检查完成。") == {"state": "FINISHED"}
        with pytest.raises(PermissionError, match="conflicts"):
            finish_leader(core, finalizer, "换个说法，项目检查已办完。")
        await core.outbox.dispatch_once({"qq": Adapter()}, pre_send=core.pre_send)
        assert len(sent) == 1
    finally:
        await core.stop()
        core.db.close()


@pytest.mark.parametrize("mutation", [False, True])
async def test_delayed_chat_revalidates_original_memory_epoch(tmp_path, official_config, official_event, clock, mutation):
    from conftest import official_payload
    from sqlalchemy import insert, update

    from qqbot.contracts.memory import MemoryScope
    from qqbot.domain import OutboundEnvelope
    from qqbot.storage.memory_sync_schema import memory_owners
    from qqbot.storage.schema import outbox
    core = make_core(tmp_path, official_config, clock)
    try:
        event = core.adapters["qq"].normalize(official_payload(official_event, private=True, text="解释项目偏好", message_id="memory-chat"))
        event, _ = core.inbox.accept(event)
        await core.process(event)
        scope = MemoryScope(owner_id=core.settings.owner.id)
        with core.db.transaction() as conn:
            row = conn.execute(select(dialogue_inputs).where(dialogue_inputs.c.id == event.event_id)).mappings().one()
            context = json.loads(row["context_json"])
            context.update(memory_scope=scope.model_dump(), memory_epoch=1, retrieved_data=[{"content": "旧偏好"}])
            conn.execute(update(dialogue_inputs).where(dialogue_inputs.c.id == row["id"]).values(context_json=json.dumps(context)))
            conn.execute(insert(memory_owners).values(id=scope.key, scope_json=scope.model_dump_json(), binding_json="{}",
                binding_hash="fixture", remote_identity="fixture", epoch=1, state="READY", updated_at=clock.now().isoformat()))
        claim = core.sessions.claim("fixture")
        if mutation:
            from qqbot.contracts.memory import MutationReceipt
            core.knowledge.rotate(scope, (claim["run_id"], claim["fencing_token"], "fixture"),
                MutationReceipt(operation_id="verified-forget", state="COMPLETED", completed=True, memory_epoch=2))
        finish_leader(core, claim, "目前偏好为旧值。")
        with core.db.engine.connect() as conn:
            envelope = OutboundEnvelope.model_validate_json(conn.execute(select(outbox.c.envelope_json)).scalar_one())
        assert core.pre_send(envelope)
        with core.db.transaction() as conn:
            conn.execute(update(memory_owners).where(memory_owners.c.id == scope.key).values(epoch=2))
        if mutation:
            assert envelope.text == "记忆更改已完成。"
            assert core.pre_send(envelope)
            assert not core.pre_send(envelope.model_copy(update={"text": "目前偏好为旧值。"}))
        else:
            assert not core.pre_send(envelope)
    finally:
        await core.stop()
        core.db.close()


async def test_leader_can_acknowledge_mid_turn_and_late_progress_is_fenced(tmp_path, official_config, official_event, clock):
    from conftest import official_payload

    from qqbot.domain import OutboundEnvelope
    from qqbot.storage.schema import outbox

    core = make_core(tmp_path, official_config, clock)
    try:
        event = core.adapters["qq"].normalize(official_payload(official_event, private=True,
            text="查看一下 yourtjhub 现在有多少 Issue"))
        event = event.model_copy(update={"event_id": "progress:source"})
        event, _ = core.inbox.accept(event)
        await core.process(event)
        claim = core.sessions.claim("leader")
        identity = (claim["run_id"], claim["fencing_token"], "leader")
        payload = {"text": "收到，我现在去查一下～", "notification_id": "first"}
        first = await core.leader.invoke("notify", identity, payload)
        repeated = await core.leader.invoke("notify", identity, {**payload, "notification_id": "retried"})
        assert first == repeated and first["accepted"]
        assert first["reply_messages_remaining"] == event.reply_context.max_replies - 1
        with core.db.engine.connect() as conn:
            rows = conn.execute(select(outbox)).mappings().all()
            assert len(rows) == 1
            progress = OutboundEnvelope.model_validate_json(rows[0]["envelope_json"])
            assert progress.purpose == "progress" and core.pre_send(progress)
            assert conn.execute(select(runs.c.slot_reserved).where(runs.c.id == claim["run_id"])).scalar_one() == 1
            assert conn.execute(select(dialogue_inputs.c.handled_at)).scalar_one() is None
        receipt = {"kind": "turn_finished", "session_id": claim["session_id"], "run_id": claim["run_id"],
            "session_revision": "leaf", "input_ids": claim["source_ids"], "inflight": 0, "tools_paired": True}
        core.leader.complete(*identity, {"status": "completed", "summary": "当前有 64 个未关闭的 Issue。"},
            stopped=True, quiescent=False, receipt=receipt)
        assert not core.pre_send(progress)
        with core.db.engine.connect() as conn:
            final = conn.execute(select(outbox.c.envelope_json).where(outbox.c.origin_id.like("turn:%"))).scalar_one()
        assert core.pre_send(OutboundEnvelope.model_validate_json(final))
    finally:
        await core.stop()
        core.db.close()


def test_admin_can_manage_verified_legacy_tasks_without_stored_owner(tmp_path, official_config, clock, monkeypatch):
    from fastapi.testclient import TestClient
    from sqlalchemy import update

    from qqbot.domain import InboundEvent

    monkeypatch.setenv("QQBOT_ADMIN_TOKEN", "admin-fixture")
    app = create_app(Settings(database=str(tmp_path / "legacy-admin.db"), tasks_enabled=True),
        clock=clock, run_background=False)
    core = app.state.core
    auth = {"Authorization": "Bearer admin-fixture"}
    with TestClient(app) as client:
        created = core.enqueue_admin_task("legacy request", idempotency_key="legacy-task")
        with core.db.transaction() as conn:
            row = conn.execute(select(tasks).where(tasks.c.id == created["task_id"])).mappings().one()
            event = InboundEvent.model_validate_json(row["event_json"])
        legacy = core.tasks.accept(event.model_copy(update={"event_id": "legacy-source"}), core.access(event), "legacy")
        url = "/api/v1/tasks/" + legacy["public_id"]
        assert client.get(url, headers=auth).status_code == 200
        assert legacy["public_id"] in [row["public_id"] for row in client.get("/api/v1/tasks", headers=auth).json()]
        with core.db.transaction() as conn:
            conn.execute(update(tasks).where(tasks.c.id == legacy["id"]).values(owner_id="other-owner"))
        assert client.get(url, headers=auth).status_code == 404
        assert legacy["public_id"] not in [row["public_id"] for row in client.get("/api/v1/tasks", headers=auth).json()]
        with core.db.transaction() as conn:
            conn.execute(update(tasks).where(tasks.c.id == legacy["id"]).values(owner_id=None))
        assert client.post(url + "/cancel", headers=auth).status_code == 200
