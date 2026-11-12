import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest
from sqlalchemy import select

from qqbot.adapters.qq_official.interaction_delivery import handle_callback, run_workflow
from qqbot.adapters.qq_official.interactions import (
    InteractionStore,
    acknowledge_interaction,
    parse_interaction,
    workflow_keyboard,
)
from qqbot.domain import AccessContext, ConversationRef, PrincipalRef, Surface
from qqbot.permissions import PermissionService
from qqbot.storage.interaction_schema import qq_interaction_events
from qqbot.storage.schema import outbox


def refs():
    return (ConversationRef(deployment_id="d", adapter_instance_id="qq", bot_identity_id="bot",
                            surface=Surface.GROUP, remote_conversation_id="g"),
            PrincipalRef(adapter_instance_id="qq", identity_namespace="qq_app:app", remote_user_id="u"))


def payload(clock, token, ident="i"):
    return {"op": 0, "t": "INTERACTION_CREATE", "id": "outer-" + ident, "d": {
        "application_id": "app", "id": ident, "type": 11, "scene": "group",
        "timestamp": clock.now().isoformat(), "group_openid": "g", "group_member_openid": "u",
        "data": {"type": 11, "resolved": {"button_data": token, "button_id": "workflow_0"}}}}


def parse(clock, token, ident="i"):
    return parse_interaction(payload(clock, token, ident), deployment_id="d", instance_id="qq",
                             app_id="app", bot_id="bot")


def token(keyboard, index=0):
    return keyboard.to_payload()["content"]["rows"][0]["buttons"][index]["action"]["data"]


def owned_task(db, clock):
    from qqbot.domain import InboundEvent
    from qqbot.jobs.service import TaskService
    from qqbot.outbox.service import Outbox
    target, principal = refs()
    source = InboundEvent(event_id="owned-source", event_type="message.created", conversation=target,
        principal=principal, platform_message_id="m", occurred_at=clock.now(), received_at=clock.now(), trace_id="t")
    service = TaskService(db, clock, Outbox(db, clock))
    task = service.accept(source, AccessContext(principal=principal, conversation=target), "work")
    return task["public_id"]


def core_for(db, clock, tasks):
    return SimpleNamespace(db=db, clock=clock, tasks=tasks, permissions=PermissionService(db, clock),
        access=lambda e: AccessContext(principal=e.principal, conversation=e.conversation))


def test_choice_payload_survives_restart_and_reaches_task_with_question(db, clock):
    store = InteractionStore(db, clock)
    task_id = owned_task(db, clock)
    keyboard = workflow_keyboard(store, *refs(), [{"label": "选择 A", "action": "choice",
        "payload": {"value": "A", "task_id": task_id, "question_id": "question-1"}}])
    event = parse(clock, token(keyboard))
    decision = InteractionStore(db, clock).consume(event)
    assert decision.payload["value"] == "A"
    assert store.consume(event).payload == decision.payload
    received = []
    tasks = SimpleNamespace(add_input=lambda *args, **kwargs: received.append((args, kwargs)))
    text, _ = run_workflow(core_for(db, clock, tasks), store, event, decision)
    assert text == "已提交选择：A"
    args, kwargs = received[0]
    assert args[0] == task_id and args[1].text == "A" and kwargs == {"question_id": "question-1"}
    assert args[1].event_id == "interaction:" + event.key


def test_page_navigation_bounded_and_cancel_requires_confirmation(db, clock):
    store = InteractionStore(db, clock)
    keyboard = workflow_keyboard(store, *refs(), [{"label": "查看", "action": "page",
        "payload": {"pages": ["first", "second"], "page": 0}}])
    core = core_for(db, clock, None)
    event = parse(clock, token(keyboard))
    text, next_keyboard = run_workflow(core, store, event, store.consume(event))
    assert text == "1/2\nfirst"
    next_event = parse(clock, token(next_keyboard), "i2")
    text, _ = run_workflow(core, store, next_event, store.consume(next_event))
    assert text == "2/2\nsecond"
    with pytest.raises(ValueError):
        workflow_keyboard(store, *refs(), [{"label": "cancel", "action": "cancel_task",
                                          "payload": {"task_id": "T-1"}}])
    with pytest.raises(ValueError):
        workflow_keyboard(store, *refs(), [{"label": "page", "action": "page",
                                          "payload": {"pages": ["a"], "page": 1}}])


def test_cancel_is_two_step_scoped_and_mutually_exclusive(db, clock):
    store = InteractionStore(db, clock)
    calls = []
    tasks = SimpleNamespace(get=lambda public_id, access: {"id": "task-id"},
        cancel=lambda public_id, access: calls.append(public_id) or {"state": "CANCEL_REQUESTED"})
    core = core_for(db, clock, tasks)
    task_id = owned_task(db, clock)
    keyboard = workflow_keyboard(store, *refs(), [{"label": "取消任务", "action": "cancel_prompt",
                                                "payload": {"task_id": task_id}}])
    event = parse(clock, token(keyboard))
    text, confirmation = run_workflow(core, store, event, store.consume(event))
    assert "确认取消" in text and not calls
    yes, no = parse(clock, token(confirmation), "yes"), parse(clock, token(confirmation, 1), "no")
    accepted = store.consume(yes)
    run_workflow(core, store, yes, accepted)
    assert calls == [task_id] and store.consume(no).code == 3


async def test_whole_ack_including_token_refresh_has_deadline(db, clock):
    async def slow_token():
        await asyncio.sleep(10)
    adapter = SimpleNamespace(access_token=slow_token)
    start = asyncio.get_running_loop().time()
    assert await acknowledge_interaction(adapter, parse(clock, "token"), 0) == "UNKNOWN"
    assert asyncio.get_running_loop().time() - start < 3.5


async def test_permission_revoked_during_ack_blocks_workflow_and_enqueue(db, clock):
    from qqbot.outbox.service import Outbox
    store = InteractionStore(db, clock)
    keyboard = workflow_keyboard(store, *refs(), [{"label": "选择", "action": "choice", "payload": {"value": "A"}}])
    allowed = True
    async def access_token():
        return "t"
    def transport(request):
        nonlocal allowed
        allowed = False
        return httpx.Response(200)
    core = core_for(db, clock, None)
    core.permissions = SimpleNamespace(check=lambda *args: SimpleNamespace(allowed=allowed))
    core.outbox = Outbox(db, clock)
    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        adapter = SimpleNamespace(verify=lambda *args: None, deployment_id="d", access_token=access_token,
            client=client, config=SimpleNamespace(instance_id="qq", app_id="app", bot_id="bot", api_base="https://qq.test"))
        assert await handle_callback(adapter, core, store, json.dumps(payload(clock, token(keyboard))).encode(), {}) == {"op": 12}
    with db.engine.connect() as c:
        assert not c.execute(select(outbox)).first()
        assert c.execute(select(qq_interaction_events.c.state)).scalar_one() == "FAILED"


def test_real_task_selection_and_cancel_enforce_owner(db, clock):
    from qqbot.domain import InboundEvent, TextSegment
    from qqbot.jobs.service import TaskService
    from qqbot.outbox.service import Outbox
    target, principal = refs()
    source = InboundEvent(event_id="task-source", event_type="message.created", conversation=target,
        principal=principal, platform_message_id="m", occurred_at=clock.now(), received_at=clock.now(),
        segments=(TextSegment(text="work"),), trace_id="trace")
    access = AccessContext(principal=principal, conversation=target)
    tasks = TaskService(db, clock, Outbox(db, clock))
    task = tasks.accept(source, access, "work")
    store = InteractionStore(db, clock)
    core = core_for(db, clock, tasks)
    keyboard = workflow_keyboard(store, target, principal, [{"label": "A", "action": "choice",
        "payload": {"value": "A", "task_id": task["public_id"]}}])
    event = parse(clock, token(keyboard))
    run_workflow(core, store, event, store.consume(event))
    from qqbot.storage.task_schema import inputs
    with db.engine.connect() as c:
        assert c.execute(select(inputs.c.text)).scalar_one() == "A"
    other = principal.model_copy(update={"remote_user_id": "other"})
    malicious = store.issue(target, other, "cancel_prompt", payload={"task_id": task["public_id"]})
    raw = payload(clock, malicious, "other-event")
    raw["d"]["group_member_openid"] = "other"
    foreign_event = parse_interaction(raw, deployment_id="d", instance_id="qq", app_id="app", bot_id="bot")
    with pytest.raises((PermissionError, LookupError)):
        run_workflow(core, store, foreign_event, store.consume(foreign_event))
    assert tasks.get(task["public_id"], access)["state"] == "QUEUED"
    prompt = workflow_keyboard(store, target, principal, [{"label": "取消任务", "action": "cancel_prompt",
        "payload": {"task_id": task["public_id"]}}])
    prompt_event = parse(clock, token(prompt), "prompt")
    _, confirmation = run_workflow(core, store, prompt_event, store.consume(prompt_event))
    confirm_event = parse(clock, token(confirmation), "confirm")
    run_workflow(core, store, confirm_event, store.consume(confirm_event))
    assert tasks.get(task["public_id"], access)["state"] == "CANCELLED"


def test_ack_retry_does_not_renew_expired_action(db, clock):
    store = InteractionStore(db, clock)
    issued = store.issue(*refs(), "help", ttl_seconds=1)
    event = parse(clock, issued)
    assert store.consume(event).action == "help"
    clock.advance(2)
    assert store.consume(event).action is None
    assert not store.claim(event.key)
