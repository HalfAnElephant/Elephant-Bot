import json
from datetime import timedelta

import httpx
import pytest
from fastapi import FastAPI
from pydantic import ValidationError
from sqlalchemy import select, update

from qqbot.adapters.qq_official.adapter import OfficialAdapter
from qqbot.capabilities.gateway import CapabilityRegistry
from qqbot.capabilities.qq import register_qq
from qqbot.domain import AccessContext, ConversationRef, InboundEvent, PrincipalRef, ReplyContext, Surface
from qqbot.jobs.api import internal_router
from qqbot.jobs.rich_delivery import RichMessage, rich_send, rich_status
from qqbot.jobs.service import TaskService
from qqbot.outbox.service import Outbox
from qqbot.storage.schema import outbox
from qqbot.storage.task_schema import tasks


@pytest.fixture
def rich_setup(db, clock, incoming, official_config):
    calls = []
    def request(req):
        if req.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "fixture", "expires_in": 7200})
        calls.append(json.loads(req.content))
        return httpx.Response(200, json={"id": "sent"})
    adapter = OfficialAdapter(official_config, "d", incoming, clock,
        httpx.AsyncClient(transport=httpx.MockTransport(request)))
    conversation = ConversationRef(deployment_id="d", adapter_instance_id="qq", bot_identity_id="bot",
        surface=Surface.PRIVATE, remote_conversation_id="300")
    principal = PrincipalRef(adapter_instance_id="qq", identity_namespace="qq_app:app", remote_user_id="300")
    event = InboundEvent(event_id="e", event_type="message.created", conversation=conversation,
        principal=principal, platform_message_id="m", occurred_at=clock.now(), received_at=clock.now(),
        trace_id="t", reply_context=ReplyContext(platform_message_id="m",
            expires_at=clock.now() + timedelta(minutes=5), max_replies=4))
    service = TaskService(db, clock, Outbox(db, clock))
    service.qq_adapters = {"qq": adapter}
    task = service.accept(event, AccessContext(principal=principal, conversation=conversation), "send image")
    claim = service.claim("worker")
    return service, (claim["run_id"], claim["fencing_token"], "worker"), task, calls


async def test_task_scoped_rich_enqueue_idempotency_and_receipt(rich_setup):
    service, args, _task, calls = rich_setup
    body = {"request_id": "one", "text": "**hello**", "content_format": "markdown"}
    result = await rich_send(service, args, body)
    assert result["state"] == "PENDING" and result["delivered"] is False
    assert await rich_send(service, args, body) == result
    with pytest.raises(ValueError, match="different content"):
        await rich_send(service, args, {**body, "text": "different"})
    await service.outbox.dispatch_once(service.qq_adapters)
    assert calls[0]["markdown"] == {"content": "**hello**"}
    assert rich_status(service, args, {"delivery_id": result["delivery_id"]})["delivered"] is True
    with pytest.raises(ValueError, match="does not belong"):
        rich_status(service, args, {"delivery_id": "foreign"})


async def test_cancelled_task_cannot_enqueue_and_model_cannot_select_target(rich_setup):
    service, args, task, _calls = rich_setup
    with pytest.raises(ValidationError):
        await rich_send(service, args, {"request_id": "one", "text": "hello", "conversation_id": "foreign"})
    with service.db.transaction() as c:
        c.execute(update(tasks).where(tasks.c.id == task["id"]).values(state="CANCEL_REQUESTED"))
    with pytest.raises(PermissionError):
        await rich_send(service, args, {"request_id": "one", "text": "hello"})
    with service.db.engine.connect() as c:
        assert not c.execute(select(outbox)).first()


async def test_worker_http_auth_and_qq_capability_routing(rich_setup):
    service, args, _task, _calls = rich_setup
    app = FastAPI()
    app.include_router(internal_router(service, "t" * 32))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://core") as client:
        async def call(op, payload):
            body = {"run_id": args[0], "fencing_token": args[1], "worker_id": args[2], "payload": payload}
            response = await client.post("/internal/tasks/" + op, json=body,
                headers={"Authorization": "Bearer " + "t" * 32})
            response.raise_for_status()
            return response.json()
        registry = CapabilityRegistry()
        register_qq(registry, call)
        capability, execute, _action = registry.entries["qq.message.send"]
        assert "official_content" in capability.parameters["properties"]
        result = await execute({"request_id": "one", "text": "hello"}, None)
        assert result["delivered"] is False
        response = await client.post("/internal/tasks/qq-status", json={"worker_id": "worker"})
        assert response.status_code == 401


@pytest.mark.parametrize("content", [{"media": {"type": "file", "url": "file:///etc/passwd"}},
    {"media": {"type": "image", "url": "https://127.0.0.1/a"}}, {"unknown": 1}])
def test_rich_media_rejects_local_and_unknown(content):
    with pytest.raises(ValidationError):
        RichMessage(request_id="one", text="hello", official_content=content)


def test_agent_buttons_are_scoped_and_no_direct_cancellation(rich_setup):
    from qqbot.jobs.rich_delivery import rich_buttons
    service, args, task, _calls = rich_setup
    result = rich_buttons(service, args, {"buttons": [{"label": "选这个", "action": "choice",
        "payload": {"value": "first"}}]})
    assert result["sent"] is False
    assert result["keyboard"]["content"]["rows"][0]["buttons"][0]["action"]["data"]
    with pytest.raises(ValueError, match="this task"):
        rich_buttons(service, args, {"buttons": [{"label": "取消", "action": "cancel_prompt",
            "payload": {"task_id": "T-other"}}]})
    with pytest.raises(ValidationError):
        rich_buttons(service, args, {"buttons": [{"label": "取消", "action": "cancel_task",
            "payload": {"task_id": task["public_id"]}}]})


async def test_button_keyboard_can_flow_through_send(rich_setup):
    from qqbot.jobs.rich_delivery import rich_buttons
    service, args, _task, calls = rich_setup
    result = rich_buttons(service, args, {"buttons": [{"label": "下一页", "action": "page",
        "payload": {"pages": ["one", "two"], "page": 1}}]})
    await rich_send(service, args, {"request_id": "page", "text": "one", "content_format": "markdown",
        "official_content": {"keyboard": result["keyboard"]}})
    await service.outbox.dispatch_once(service.qq_adapters)
    assert calls[0]["keyboard"] == result["keyboard"]


async def test_run_gateway_verifies_enqueue_not_remote_delivery(rich_setup, tmp_path):
    from qqbot.harness.gateway import RunGateway
    service, args, task, _calls = rich_setup
    async def call(op, payload):
        if op == "authorize":
            return service.authorize(*args, payload)
        if op == "ledger":
            return await service.ledger(*args, payload)
        if op == "qq-send":
            return await rich_send(service, args, payload)
        if op == "qq-status":
            return rich_status(service, args, payload)
        raise AssertionError(op)
    gateway = RunGateway(tmp_path, "offline", "fixture", "https://model.invalid/v1/chat/completions", call=call)
    gateway.bind({"task_id": task["id"], "run_id": args[0], "fencing_token": args[1],
        "permission_snapshot": {"principal": json.loads(task["permission_json"])["principal"]}})
    result = await gateway.tools.invoke("qq.message.send", {"request_id": "gateway", "text": "hello"}, gateway.context)
    assert result.status == "verified"
    assert result.data["verified_effect"] == "durable_enqueue"
    assert result.data["delivered"] is False
    assert gateway.registry.search("发送图片")[0]["name"] == "qq.message.send"


@pytest.mark.parametrize("buttons", [None, 1, {}, "bad"])
def test_invalid_buttons_shape_is_validation_error(buttons):
    from qqbot.jobs.rich_delivery import RichButtons
    with pytest.raises(ValidationError):
        RichButtons(buttons=buttons)
