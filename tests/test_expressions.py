import asyncio
import json

import httpx
import pytest
from sqlalchemy import insert, select

from qqbot.domain import AccessContext, ConversationRef, InboundEvent, PrincipalRef, Surface, TextSegment
from qqbot.expressions import (
    ExpressionConfig,
    ExpressionService,
    ExpressionVersionConflict,
    HTTPLearner,
    LearnedExpression,
    LearningResult,
    build_habits,
    invalidate_source,
)
from qqbot.expressions.config import BlacklistedUser, LearningRule
from qqbot.expressions.learner import parse_learning_response
from qqbot.expressions.weighted_sampling import _compute_weights, weighted_sample
from qqbot.permissions import PermissionPolicy, PermissionRule, PermissionService, SubjectSelector
from qqbot.storage.expression_schema import expression_tombstones, expressions
from qqbot.storage.social_schema import memory_source_retention, social_tombstones


@pytest.fixture
def world(db, clock, incoming):
    conversation = ConversationRef(
        deployment_id="test", adapter_instance_id="nap", bot_identity_id="bot",
        surface=Surface.GROUP, remote_conversation_id="group-a",
    )
    principal = PrincipalRef(adapter_instance_id="nap", identity_namespace="qq", remote_user_id="member")
    access = AccessContext(conversation=conversation, principal=principal, role="member")
    counter = 0

    def event(text="今天这个安排有点意思", *, target=None, user=None, bot=False):
        nonlocal counter
        counter += 1
        identity = "message-" + str(counter)
        return incoming.accept(InboundEvent(
            event_id=identity, event_type="message.created", conversation=target or conversation,
            principal=principal.model_copy(update={"remote_user_id": user}) if user else principal,
            platform_message_id=identity, occurred_at=clock.now(), received_at=clock.now(),
            segments=(TextSegment(text=text),), trace_id=identity, sender_is_bot=bot,
        ))[0]

    service = ExpressionService(db, clock, ExpressionConfig(min_messages=1))
    return service, access, event


class Learner:
    configured = True

    def __init__(self, result=None):
        self.calls, self.result = [], result

    async def learn(self, events, *, self_reflect):
        self.calls.append(events)
        if self.result is not None:
            return self.result
        return LearningResult(state="DONE", candidates=tuple(
            LearnedExpression(situation="回应话题" + event.event_id.split("-")[-1],
                              style="有点意思" + event.event_id.split("-")[-1], source_event_id=event.event_id)
            for event in events[:10]
        ))


async def test_learn_cold_start_and_upstream_fast_selection(world, clock):
    service, access, event = world
    learner = Learner()
    for _ in range(9):
        event()
    assert (await service.maintenance_once(access, learner))["written"] == 9
    assert service.select(access) == []
    clock.advance(301)
    event()
    assert (await service.maintenance_once(access, learner))["written"] == 1
    selected = service.select(access)
    assert len(selected) == 5 and len({row["id"] for row in selected}) == 5
    assert all(service.validate_record(access, row["id"], row["revision"]) for row in selected)
    assert build_habits(selected).startswith("你可以视情况自由使用以下句式：")
    assert len(learner.calls) == 2  # Selection has no model round-trip.
    assert _compute_weights([{"count": 1}, {"count": 5}]) == [1, 5]
    assert len(weighted_sample([{"id": i} for i in range(20)], 5)) == 5
    clock.advance(301)
    assert (await service.maintenance_once(access, learner))["state"] == "PENDING"
    assert len(learner.calls) == 2


async def test_sources_filter_self_commands_secrets_blacklist_and_recall(world, clock, db):
    service, access, event = world
    service.config = service.config.model_copy(update={"learning_blacklist": (
        BlacklistedUser(adapter_instance_id="nap", identity_namespace="qq", user_id="blocked"),
    )})
    good = event()
    event(bot=True)
    event("/状态")
    event("password=do-not-learn-this")
    event(user="blocked")
    recalled = event()
    retained = event()
    with db.transaction() as conn:
        conn.execute(insert(social_tombstones).values(source_id=recalled.event_id, conversation_id=access.conversation.key))
        conn.execute(insert(memory_source_retention).values(
            source_id=retained.event_id, conversation_id=access.conversation.key, redacted_at=clock.now().isoformat(),
        ))
    learner = Learner()
    assert (await service.maintenance_once(access, learner))["written"] == 1
    assert [item.event_id for item in learner.calls[0]] == [good.event_id]
    record = service.list(access)[0]
    with db.transaction() as conn:
        conn.execute(insert(social_tombstones).values(source_id=good.event_id, conversation_id=access.conversation.key))
    assert not service.validate_record(access, record["id"], record["revision"])
    assert service.list(access) == []
    with db.transaction() as conn:
        invalidate_source(conn, good.event_id, now=clock.now())
        removed = conn.execute(select(expressions).where(expressions.c.id == record["id"])).mappings().one()
        assert removed["style"] == "" and removed["situation"] == "" and removed["sources_json"] == "[]"


async def test_durable_failure_retry_pending_and_unique_counts(world, clock, db):
    service, access, event = world
    first = event()
    failed = Learner(LearningResult(state="FAILED", reason="provider_or_schema_error"))
    assert (await service.maintenance_once(access, failed))["state"] == "FAILED"
    restarted = ExpressionService(db, clock, service.config)
    assert restarted.status(access)["pending_count"] == 1
    good = Learner()
    assert (await restarted.maintenance_once(access, good))["reason"] == "cooldown"
    clock.advance(601)
    assert (await restarted.maintenance_once(access, good))["written"] == 1
    assert good.calls[0][0].event_id == first.event_id
    original = restarted.list(access)[0]
    second = event()
    repeated = LearnedExpression(situation=original["situation"], style=original["style"], source_event_id=second.event_id)
    clock.advance(301)
    assert (await restarted.maintenance_once(access, Learner(LearningResult(
        state="DONE", candidates=(repeated, repeated),
    ))))["written"] == 1
    current = restarted.list(access)[0]
    assert current["count"] == 2 and current["revision"] == original["revision"]
    assert restarted.validate_record(access, original["id"], original["revision"])


async def test_review_revision_delete_and_no_relearning(world, clock, db):
    service, access, event = world
    for _ in range(10):
        event()
    await service.maintenance_once(access, Learner())
    original = service.select(access)[0]
    service.config = service.config.model_copy(update={"expression_checked_only": True})
    assert service.select(access) == []
    assert len(service.list(access)) == 10  # Unreviewed material remains visible to reviewers.
    reviewed = service.review(access, original["id"], checked=True, expected_revision=original["revision"])
    assert reviewed["checked"] and reviewed["revision"] == original["revision"] + 1
    assert not service.validate_record(access, original["id"], original["revision"])
    assert service.validate_record(access, reviewed["id"], reviewed["revision"])
    with pytest.raises(ExpressionVersionConflict):
        service.review(access, original["id"], checked=False, expected_revision=original["revision"])
    service.delete(access, reviewed["id"], expected_revision=reviewed["revision"])
    assert not service.validate_record(access, reviewed["id"], reviewed["revision"])
    new_event = event()
    clock.advance(301)
    result = await service.maintenance_once(access, Learner(LearningResult(state="DONE", candidates=(
        LearnedExpression(situation=original["situation"], style=original["style"], source_event_id=new_event.event_id),
    ))))
    assert result["written"] == 0
    with db.engine.connect() as conn:
        assert conn.execute(select(expression_tombstones.c.expression_id).where(
            expression_tombstones.c.expression_id == original["id"],
        )).scalar_one() == original["id"]


async def test_explicit_sharing_never_exposes_private_and_checks_source_permissions(world, clock):
    service, access, event = world
    group_b = access.conversation.model_copy(update={"remote_conversation_id": "group-b"})
    private = access.conversation.model_copy(update={"surface": Surface.PRIVATE, "remote_conversation_id": "friend"})
    b_access = access.model_copy(update={"conversation": group_b})
    p_access = access.model_copy(update={"conversation": private})
    for _ in range(10):
        event()
    await service.maintenance_once(access, Learner())
    assert service.select(b_access) == []
    assert (await service.maintenance_once(p_access, Learner()))["reason"] == "learning_disabled"
    service.config = service.config.model_copy(update={
        "learning_list": (LearningRule(), LearningRule(type="private")),
        "expression_groups": ((access.conversation.key, group_b.key, private.key),),
    })
    for _ in range(10):
        event("私聊风格素材", target=private)
    await service.maintenance_once(p_access, Learner())
    assert {row["conversation_id"] for row in service.select(b_access)} == {access.conversation.key}
    assert {row["conversation_id"] for row in service.select(p_access)} == {private.key}
    record = service.select(b_access)[0]
    service.permissions = PermissionService(policy=PermissionPolicy(rules=(PermissionRule(
        id="deny-expression-source", actions=("history.source.read",),
        resources=("event:" + record["source_event_ids"][0],), effect="deny",
    ),)))
    assert not service.validate_record(b_access, record["id"], record["revision"])
    assert service.select(b_access) == []  # Nine eligible expressions are below the upstream threshold.


async def test_revocation_during_model_request_and_learning_capacity(world, clock, db):
    service, access, event = world
    source = event()
    started, resume = asyncio.Event(), asyncio.Event()

    class WaitingLearner(Learner):
        async def learn(self, events, *, self_reflect):
            started.set()
            await resume.wait()
            return await super().learn(events, self_reflect=self_reflect)

    task = asyncio.create_task(service.maintenance_once(access, WaitingLearner()))
    await started.wait()
    assert (await service.maintenance_once(access, Learner()))["reason"] == "learner_capacity"
    with db.transaction() as conn:
        conn.execute(insert(social_tombstones).values(source_id=source.event_id, conversation_id=access.conversation.key))
    resume.set()
    result = await task
    assert result["written"] == 0 and result["rejected"] == 1
    assert service.list(access) == []


async def test_http_learning_self_reflection_and_strict_sources(world):
    _service, _access, event = world
    events = [event(), event()]
    requests = []
    sessions = []
    responses = [
        json.dumps([
            {"situation": "看到有趣安排", "style": "使用 有点东西", "id": "1"},
            {"situation": "想休息", "style": "先歇会儿", "id": "2"},
            {"situation": "莫名出现", "style": "捏造来源", "id": "99"},
            {"situation": "跟随示例", "style": "对对对", "id": "1"},
        ], ensure_ascii=False),
        '{"suitable": true, "reason": "适合作为通用表达"}',
        '{"suitable": false, "reason": "与当前情境不匹配"}',
    ]

    def handler(request):
        requests.append(json.loads(request.content))
        sessions.append(request.headers.get("x-opencode-session"))
        assert request.headers["User-Agent"] == "ElephantBot/0.1"
        return httpx.Response(200, json={"choices": [{"message": {"content": responses.pop(0)}}]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        learner = HTTPLearner("https://model.test/chat/completions", "test", "key", client=client)
        result = await learner.learn(events)
    assert result.state == "DONE" and len(result.candidates) == 1 and result.rejected == 3
    assert result.candidates[0].source_event_id == events[0].event_id
    assert result.candidates[0].style == "有点东西"
    assert len(requests) == 3
    assert [request["max_tokens"] for request in requests] == [8192, 4096, 4096]
    assert sessions[0] and len(set(sessions)) == 1
    assert "只考虑文字" in requests[0]["messages"][0]["content"]
    assert "[speaker:USER]" in requests[0]["messages"][1]["content"]
    assert "允许部分语法错误或口语化或缺省出现" in requests[1]["messages"][1]["content"]
    candidates, rejected = parse_learning_response('[{"situation":"a","style":"b","id":true}]', events)
    assert candidates == () and rejected == 1
    assert (await HTTPLearner(None, None).learn(events)).state == "SKIPPED"


async def test_http_failures_are_retryable_without_publishing(world):
    _service, _access, event = world
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(503))) as client:
        result = await HTTPLearner("https://model.test", "test", "key", client=client).learn([event()])
    assert result.state == "FAILED" and result.candidates == ()
    assert result.reason == "extraction_http_503"


@pytest.mark.parametrize("model,finish,content,reason", [
    ("deepseek-v4.1-flash", "stop", "[]", ""),
    ("deepseek-v4.1-flash", "length", "[]", "extraction_output_truncated"),
    ("test", "stop", None, "extraction_empty_content"),
])
async def test_learning_reasoning_budget_and_incomplete_responses(world, model, finish, content, reason):
    _service, _access, event = world

    def handler(request):
        payload = json.loads(request.content)
        if model.startswith("deepseek-v4"):
            assert payload["thinking"] == {"type": "enabled"}
            assert payload["reasoning_effort"] == "low"
        else:
            assert "thinking" not in payload
            assert "reasoning_effort" not in payload
        return httpx.Response(200, json={"choices": [{
            "finish_reason": finish, "message": {"content": content},
        }]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        learner = HTTPLearner("https://model.test", model, "key", client=client)
        assert learner.timeout_seconds == ExpressionConfig().timeout_seconds == 300
        assert ExpressionConfig(timeout_seconds=1800).timeout_seconds == 1800
        result = await learner.learn([event()])
    assert result.reason == reason
    assert result.state == ("FAILED" if reason else "DONE")
    assert result.candidates == ()


async def test_pending_messages_survive_filtered_scan_and_restart(world, clock, db):
    service, access, event = world
    service.config = service.config.model_copy(update={"min_messages": 2, "max_messages": 10})
    first = event()
    for _ in range(120):
        event(bot=True)
    learner = Learner()
    assert (await service.maintenance_once(access, learner))["state"] == "PENDING"
    assert service.status(access)["pending_count"] == 1
    second = event()
    clock.advance(301)
    restarted = ExpressionService(db, clock, service.config)
    assert (await restarted.maintenance_once(access, learner))["written"] == 2
    assert {item.event_id for item in learner.calls[0]} == {first.event_id, second.event_id}


def test_configuration_exact_identity_and_private_opt_in(world):
    _service, access, _event = world
    config = ExpressionConfig(learning_list=(
        LearningRule(),
        LearningRule(adapter_instance_id="nap", learn=False),
        LearningRule(conversation_id=access.conversation.key, learn=True, use=False),
    ))
    assert config.rule(access.conversation).learn and not config.rule(access.conversation).use
    private = access.conversation.model_copy(update={"surface": Surface.PRIVATE})
    assert not config.rule(private).use and not config.rule(private).learn
    with pytest.raises(ValueError):
        LearningRule(conversation_id="*")
    with pytest.raises(ValueError):
        ExpressionConfig(expression_groups=(("same", "same"),))


async def test_queued_recall_and_denied_author_are_not_expression_sources(world, clock, incoming):
    service, access, event = world
    sources = [event() for _ in range(10)]
    await service.maintenance_once(access, Learner())
    row = service.select(access)[0]
    source = next(item for item in sources if item.event_id in row["source_event_ids"])
    incoming.accept(source.model_copy(update={
        "event_id": "queued-recall", "event_type": "message.recalled", "segments": (),
    }))
    assert not service.validate_record(access, row["id"], row["revision"])
    service.permissions = PermissionService(policy=PermissionPolicy(rules=(PermissionRule(
        id="blocked-author", actions=("conversation.receive",), effect="deny",
        subject=SubjectSelector(kind="person", adapter_instance_id="nap", identity_namespace="qq", user_id="blocked"),
    ),)))
    blocked = event(user="blocked")
    good = event()
    learner = Learner()
    clock.advance(301)
    await service.maintenance_once(access, learner)
    assert [item.event_id for item in learner.calls[0]] == [good.event_id]
    assert blocked.event_id not in {source for row in service.list(access) for source in row["source_event_ids"]}
