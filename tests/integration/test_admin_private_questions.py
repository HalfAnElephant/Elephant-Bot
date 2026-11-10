"""An admin-owned task may ask its evidenced private recipient without changing authority."""
import json

import httpx
import pytest
from sqlalchemy import insert, select

from qqbot.app import create_app
from qqbot.config import Settings
from qqbot.domain import PrincipalRef
from qqbot.storage.schema import inbox
from qqbot.storage.social_schema import social_tombstones
from qqbot.storage.task_schema import inputs, tasks, waits


@pytest.fixture
async def admin_app(tmp_path, nap_config, official_config, clock, monkeypatch):
    monkeypatch.setenv("QQBOT_ADMIN_TOKEN", "admin-test-token")
    app = create_app(Settings(database=str(tmp_path / "core.db"), adapters=(nap_config, official_config),
                              tasks_enabled=True), clock=clock, run_background=False)
    core = app.state.core
    core.adapters["nap"].account_online = True
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://core",
                                headers={"Authorization": "Bearer admin-test-token"}) as client:
        yield core, client
    await core.stop()
    core.db.close()


def incoming(core, packet, adapter, number, text="建立会话", user="300"):
    if adapter == "nap":
        packet = {**packet, "message_type": "private", "user_id": int(user), "message_id": number,
                  "message": [{"type": "text", "data": {"text": text}}]}
        packet.pop("group_id", None)
    else:
        packet = {**packet, "t": "C2C_MESSAGE_CREATE", "d": {
            **packet["d"], "id": str(number), "content": text, "author": {"user_openid": user},
        }}
    return core.adapters[adapter].normalize(packet)


async def create_question(core, client, conversation):
    response = await client.post("/api/v1/tasks", headers={"Idempotency-Key": "private-question"}, json={
        "conversation_id": conversation.key, "goal": "处理任务，有不明确的地方先问我。",
    })
    assert response.status_code == 200
    with core.db.engine.connect() as c:
        task = dict(c.execute(select(tasks)).mappings().one())
    claim = core.tasks.claim("worker")
    core.tasks.ask(claim["run_id"], claim["fencing_token"], "worker", {
        "question_id": "admin-question", "text": "请提供目标名称。", "checkpoint": "No effects yet",
        "condition": {"expected_responders": ["untrusted-model-choice"]},
    })
    core.tasks.submit(claim["run_id"], claim["fencing_token"], "worker", {
        "status": "waiting_input", "summary": "等待你的回答。",
    })
    with core.db.engine.connect() as c:
        condition = json.loads(c.execute(select(waits.c.condition_json)).scalar_one())
    assert "untrusted-model-choice" not in condition["expected_responders"]
    return task, condition


@pytest.mark.parametrize("adapter", ["nap", "qq"])
async def test_admin_created_private_question_accepts_real_recipient_plain_answer(
    admin_app, nap_event, official_event, adapter,
):
    core, client = admin_app
    packet = nap_event if adapter == "nap" else official_event
    source = core.inbox.accept(incoming(core, packet, adapter, 3100))[0]
    core.inbox.finish(source.event_id)
    task, condition = await create_question(core, client, source.conversation)
    owner = core.tasks._access_for(task)
    assert owner.principal.identity_namespace == "admin_api" and owner.role == "owner"
    assert condition["expected_responders"] == [owner.principal.key, source.principal.key]
    assert json.loads(task["context_json"])["additional_expected_responders"] == [source.principal.key]
    answer = core.inbox.accept(incoming(core, packet, adapter, 3101, "org/repo"))[0]
    await core.process(answer)
    with core.db.engine.connect() as c:
        updated = c.execute(select(tasks)).mappings().one()
        actual = c.execute(select(inputs)).mappings().one()
    assert updated["state"] == "QUEUED"
    assert updated["requester"] == task["requester"] == owner.principal.key
    assert json.loads(updated["permission_json"])["role"] == "owner"
    assert actual["task_id"] == task["id"] and actual["source_id"] == answer.event_id
    assert actual["text"] == "org/repo"
    manifest = json.loads(updated["context_json"])["source_manifest"]
    assert {source.event_id, answer.event_id} <= {item["id"] for item in manifest}
    resumed = core.tasks.claim("worker")
    assert resumed["task_id"] == task["id"]
    assert resumed["continuation"]["inputs"] == [{"id": actual["id"], "text": "org/repo"}]
    assert core.tasks.authorize(resumed["run_id"], resumed["fencing_token"], "worker", {
        "action": "model.request",
    })["allowed"]


@pytest.mark.parametrize("evidence", ["other_conversation", "other_adapter", "other_recipient", "bot", "admin_only", "revoked"])
async def test_admin_private_question_keeps_default_when_recipient_evidence_is_unavailable(
    admin_app, nap_event, evidence,
):
    core, client = admin_app
    candidate = incoming(core, nap_event, "nap", 3110)
    owner_principal = PrincipalRef(adapter_instance_id="nap", identity_namespace="admin_api", remote_user_id="owner")
    target_seed = candidate.model_copy(update={
        "principal": owner_principal, "platform_message_id": "admin:seed", "event_id": "admin-seed",
    })
    core.inbox.accept(target_seed)
    if evidence == "other_conversation":
        candidate = incoming(core, nap_event, "nap", 3111, user="301")
    elif evidence == "other_adapter":
        candidate = candidate.model_copy(update={"principal": candidate.principal.model_copy(update={"adapter_instance_id": "qq"})})
    elif evidence == "other_recipient":
        candidate = candidate.model_copy(update={"principal": candidate.principal.model_copy(update={"remote_user_id": "301"})})
    elif evidence == "bot":
        candidate = candidate.model_copy(update={"sender_is_bot": True})
    elif evidence == "admin_only":
        candidate = candidate.model_copy(update={"principal": candidate.principal.model_copy(update={"identity_namespace": "admin_api"})})
    saved = core.inbox.accept(candidate)[0]
    if evidence == "revoked":
        with core.db.transaction() as c:
            c.execute(insert(social_tombstones).values(source_id=saved.event_id, conversation_id=saved.conversation.key))
    task, condition = await create_question(core, client, target_seed.conversation)
    assert condition["expected_responders"] == [owner_principal.key]
    assert "additional_expected_responders" not in json.loads(task["context_json"])
    answer = incoming(core, nap_event, "nap", 3112, "org/repo")
    assert core.tasks.question_for(answer) is None
    with core.db.engine.connect() as c:
        assert c.execute(select(inputs.c.id)).first() is None


async def test_admin_group_question_does_not_authorize_existing_members_by_default(admin_app, nap_event):
    core, client = admin_app
    source = core.inbox.accept(core.adapters["nap"].normalize(nap_event))[0]
    task, condition = await create_question(core, client, source.conversation)
    assert condition["expected_responders"] == [task["requester"]]
    assert "additional_expected_responders" not in json.loads(task["context_json"])
    answer = core.adapters["nap"].normalize({**nap_event, "message_id": 3120,
        "message": [{"type": "at", "data": {"qq": "100"}}, {"type": "text", "data": {"text": "org/repo"}}]})
    assert core.tasks.question_for(answer) is None


async def test_recipient_answer_is_recorded_with_its_own_source_even_after_admin_context_binding(admin_app, nap_event):
    core, client = admin_app
    source = core.inbox.accept(incoming(core, nap_event, "nap", 3130))[0]
    task, _ = await create_question(core, client, source.conversation)
    other = core.inbox.accept(incoming(core, nap_event, "nap", 3131, "无关回答", user="301"))[0]
    assert core.tasks.question_for(other) is None
    answer = core.inbox.accept(incoming(core, nap_event, "nap", 3132, "org/repo"))[0]
    await core.process(answer)
    with core.db.engine.connect() as c:
        actual_source = c.execute(select(inputs.c.source_id)).scalar_one()
        raw = c.execute(select(inbox.c.event_json).where(inbox.c.id == actual_source)).scalar_one()
    assert json.loads(raw)["principal"] == answer.principal.model_dump()
    assert actual_source != task["source"]
