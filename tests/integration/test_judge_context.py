import json

import pytest
from sqlalchemy import insert, select, update

from qqbot.app import create_app
from qqbot.config import Settings
from qqbot.domain.messages import (
    AdapterCapabilities,
    FaceSegment,
    MediaSegment,
    MentionSegment,
    ObservationMode,
)
from qqbot.domain.social import JudgmentSignals
from qqbot.permissions import PermissionPolicy
from qqbot.storage.schema import decision_logs, inbox, outbox
from qqbot.storage.social_schema import memory_source_retention, social_tombstones


@pytest.fixture
def core(tmp_path, nap_config, clock):
    app = create_app(Settings(database=str(tmp_path / "core.db"), adapters=(nap_config,)),
                     clock=clock, run_background=False)
    yield app.state.core
    app.state.core.db.close()


def accept(core, packet, message_id, text, *, sender=300, group=200):
    event = core.adapters["nap"].normalize({
        **packet, "message_id": message_id, "user_id": sender, "group_id": group,
        "message": [{"type": "text", "data": {"text": text}}],
    }).model_copy(update={"observation_mode": ObservationMode.FULL})
    return core.inbox.accept(event)[0]


def sent_reply(core, event, text, message_id):
    with core.db.transaction() as c:
        delivery = core.outbox.enqueue_text(c, target=event.conversation, text=text,
                                           origin_kind="chat", origin_id=event.event_id)[0]
        c.execute(update(outbox).where(outbox.c.id == delivery).values(
            state="SENT", platform_message_id=message_id, sent_at=core.clock.now().isoformat(),
        ))
    return delivery


def test_context_identifies_speakers_replies_media_and_exact_target(core, nap_event, clock):
    source = accept(core, nap_event, 500, "先前的问题")
    sent_reply(core, source, "这是大象的真实回答", "bot-500")
    clock.advance(1)
    target = accept(core, nap_event, 501, "新问题", sender=301)
    target = target.model_copy(update={
        "reply_to": "bot-500", "segments": (*target.segments, MentionSegment(remote_user_id="300"),
            FaceSegment(face_id="1"), MediaSegment(type="image", attachment_ref="secret-image-url")),
    })
    with core.db.transaction() as c:
        c.execute(update(inbox).where(inbox.c.id == target.event_id).values(event_json=target.model_dump_json()))
    clock.advance(1)
    accept(core, nap_event, 502, "后来的消息不能成为本次的目标")
    other = accept(core, nap_event, 503, "其他群的秘密", group=201)
    sent_reply(core, other, "其他群的机器人回答", "other-bot")
    core.personas.set_override(target.conversation.key, {"name": "群象"}, authorized=True)
    context = core.judge_context(target)
    assert context["target_message_id"] == "501"
    assert [m["id"] for m in context["messages"]] == ["500", "501"]
    assert context["messages"][0]["speaker"] != context["messages"][1]["speaker"]
    assert context["messages"][1]["mentions"] == [context["messages"][0]["speaker"]]
    assert context["messages"][1]["reply_to"] == "bot-500"
    assert context["messages"][1]["non_text_types"] == ["face", "image", "mention"]
    assert context["bot"]["name"] == "群象" and "大象" in context["bot"]["aliases"]
    assert [m["text"] for m in context["recent_bot_messages"]] == ["这是大象的真实回答"]
    assert context["recent_bot_messages"][0]["is_self"] is True
    encoded = json.dumps(context, ensure_ascii=False)
    assert "秘密" not in encoded and "secret-image-url" not in encoded and "后来的消息" not in encoded
    clock.advance(16 * 60)
    later = accept(core, nap_event, 504, "很久以后的问题")
    assert core.judge_context(later)["recent_bot_messages"] == []


@pytest.mark.parametrize("hidden_by", ["revoked", "retained", "recalled", "permission"])
def test_context_excludes_hidden_sources_and_their_bot_replies(core, nap_event, hidden_by):
    source = accept(core, nap_event, 600, "需要被隐藏的原文")
    sent_reply(core, source, "需要被隐藏的派生回答", "bot-600")
    target = accept(core, nap_event, 601, "当前消息")
    with core.db.transaction() as c:
        if hidden_by == "revoked":
            c.execute(insert(social_tombstones).values(
                source_id=source.event_id, conversation_id=source.conversation.key,
            ))
        elif hidden_by == "retained":
            c.execute(insert(memory_source_retention).values(
                source_id=source.event_id, conversation_id=source.conversation.key,
                redacted_at=core.clock.now().isoformat(),
            ))
    if hidden_by == "recalled":
        core.inbox.accept(source.model_copy(update={"event_id": "recall-600", "event_type": "message.recalled"}))
    elif hidden_by == "permission":
        core.permissions.set_policy(PermissionPolicy.model_validate({"rules": [{
            "id": "hide-source", "effect": "deny", "actions": ["history.source.read"],
            "resources": ["event:" + source.event_id],
        }]}), expected_version=0, actor="fixture")
    context = core.judge_context(target)
    assert [m["id"] for m in context["messages"]] == ["601"]
    assert context["recent_bot_messages"] == []
    assert "需要被隐藏" not in json.dumps(context, ensure_ascii=False)


async def test_normal_entry_passes_persisted_preferences_and_logs_preference_veto(core, nap_event):
    from qqbot.dialogue import PersonaStore

    policy = "公开玩梗积极参与；政治、争吵、健康等严肃话题不主动参与。"
    core.personas.set_core(core.personas._core.model_copy(update={"group_chat_prompt": policy}))
    restored = PersonaStore(core.personas.path, global_path=core.personas.global_path)
    assert json.loads(restored.snapshot("")[0])["core"]["group_chat_prompt"] == policy

    async def capabilities(target):
        return AdapterCapabilities(can_reply=True, can_observe_unaddressed_messages=True,
                                   observation_mode=ObservationMode.FULL)

    captured = []

    class Judge:
        async def judge(self, context):
            captured.append(context)
            return JudgmentSignals(invites_participation=0.99, useful_contribution=0.99,
                                   private_exchange=0.01, proactive_allowed=0.01)

    core.judge = Judge()
    core.adapters["nap"].capabilities = capabilities
    for index in range(3):
        event = accept(core, nap_event, 700 + index, "政治争论")
        if index < 2:
            core.inbox.finish(event.event_id)
    core.save_state(event.conversation.key, mode="social")
    await core.process(event)
    assert len(captured) == 1 and captured[0]["participation_policy"] == policy
    assert captured[0]["target_message_id"] == event.platform_message_id
    with core.db.engine.connect() as c:
        row = c.execute(select(decision_logs).where(decision_logs.c.event_id == event.event_id)).mappings().one()
    assert json.loads(row["decision_json"])["reason_codes"] == ["PARTICIPATION_PREFERENCE"]
    assert json.loads(row["context_json"])["judge_context_version"] == "speech-context-v2"


async def test_judge_hot_switch_logs_actual_backend_and_schema(core, nap_event):
    from qqbot.policy.judge import Jev1Judge, RuleOnlyJudge, StructuredLLMJudge

    async def capabilities(target):
        return AdapterCapabilities(can_reply=True, can_observe_unaddressed_messages=True,
                                   observation_mode=ObservationMode.FULL)

    core.adapters["nap"].capabilities = capabilities
    for number in [780, 781]:
        seed = accept(core, nap_event, number, "人类背景")
        core.inbox.finish(seed.event_id)
    for index, judge in enumerate([
        RuleOnlyJudge(), StructuredLLMJudge(None, "fixture-structured", "https://example.invalid"),
        Jev1Judge(None, "jev1", "https://example.invalid"),
    ]):
        core.judge = judge
        target = accept(core, nap_event, 782 + index, "大家怎么看")
        core.save_state(target.conversation.key, mode="social")
        await core.process(target)
        with core.db.engine.connect() as conn:
            row = conn.execute(select(decision_logs).where(decision_logs.c.event_id == target.event_id)).mappings().one()
        context = json.loads(row["context_json"])
        assert context["judge_backend"] == judge.backend
        assert context["judge_model_id"] == getattr(judge, "model", "rule-only")
        assert context["judge_schema_version"] == "judgment-v2"
        assert json.loads(row["decision_json"])["signals"]["proactive_allowed"] == 0


async def test_judge_swapped_during_request_logs_the_backend_that_ran(core, nap_event):
    import httpx

    from qqbot.policy.judge import SIGNALS, RuleOnlyJudge, StructuredLLMJudge

    async def capabilities(target):
        return AdapterCapabilities(can_reply=True, can_observe_unaddressed_messages=True,
                                   observation_mode=ObservationMode.FULL)

    async def switch_backend(request):
        core.judge = RuleOnlyJudge()
        values = {**dict.fromkeys(SIGNALS, 0.99), "proactive_allowed": 0}
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(values)}}]})

    core.adapters["nap"].capabilities = capabilities
    for number in [790, 791]:
        seed = accept(core, nap_event, number, "人类背景")
        core.inbox.finish(seed.event_id)
    target = accept(core, nap_event, 792, "大家怎么看")
    core.save_state(target.conversation.key, mode="social")
    async with httpx.AsyncClient(transport=httpx.MockTransport(switch_backend)) as client:
        core.judge = StructuredLLMJudge("key", "actual-model", "https://example.invalid", client)
        await core.process(target)
    assert isinstance(core.judge, RuleOnlyJudge)
    with core.db.engine.connect() as conn:
        row = conn.execute(select(decision_logs).where(decision_logs.c.event_id == target.event_id)).mappings().one()
    context = json.loads(row["context_json"])
    assert context["judge_backend"] == "structured" and context["judge_model_id"] == "actual-model"
    assert json.loads(row["decision_json"])["signals"]["backend"] == "structured"
