import copy
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient
from pydantic import ValidationError
from sqlalchemy import select

from qqbot.adapters.qq_official.interactions import (
    InteractionStore,
    acknowledge_interaction,
    action_keyboard,
    parse_interaction,
    router,
)
from qqbot.domain import ConversationRef, PrincipalRef, Surface
from qqbot.domain.interactions import QQButtonAction, QQButtonPermission, QQKeyboard
from qqbot.permissions import PermissionService
from qqbot.storage.interaction_schema import qq_interaction_events


def refs():
    return (ConversationRef(deployment_id="d", adapter_instance_id="qq", bot_identity_id="bot",
                            surface=Surface.GROUP, remote_conversation_id="g"),
            PrincipalRef(adapter_instance_id="qq", identity_namespace="qq_app:app", remote_user_id="u"))


def payload(clock, token, *, event_id="i1", user="u", group="g"):
    return {"op": 0, "t": "INTERACTION_CREATE", "d": {"application_id": "app", "id": event_id,
        "type": 11, "scene": "group", "timestamp": clock.now().isoformat(),
        "group_openid": group, "group_member_openid": user,
        "data": {"type": 11, "resolved": {"button_data": token, "button_id": "status"}}}}


def parse(value):
    return parse_interaction(value, deployment_id="d", instance_id="qq", app_id="app", bot_id="bot")


def test_keyboard_serialization_and_validation(db, clock):
    store = InteractionStore(db, clock)
    keyboard = action_keyboard(store, *refs())
    raw = keyboard.to_payload()
    assert raw["content"]["rows"][0]["buttons"][0]["action"]["permission"] == {
        "type": 0, "specify_user_ids": ["u"], "specify_role_ids": []}
    assert QQKeyboard.model_validate_json(keyboard.model_dump_json()) == keyboard
    with pytest.raises(ValidationError):
        QQKeyboard()
    with pytest.raises(ValidationError):
        QQButtonPermission(type=0)
    with pytest.raises(ValidationError):
        QQButtonAction(type=0, data="javascript:alert(1)")
    with pytest.raises(ValidationError):
        QQButtonAction(type=1, data="token", enter=True)
    duplicate = copy.deepcopy(raw)
    duplicate["content"]["rows"][0]["buttons"][1]["id"] = "status"
    with pytest.raises(ValidationError):
        QQKeyboard.model_validate_json(__import__("json").dumps(duplicate))


def test_action_is_scoped_expiring_single_use_and_durable(db, clock):
    store = InteractionStore(db, clock)
    token = store.issue(*refs(), "status")
    assert store.consume(parse(payload(clock, token, user="other"))).code == 4
    assert store.consume(parse(payload(clock, token, group="other", event_id="i2"))).code == 4
    good = parse(payload(clock, token, event_id="i3"))
    accepted = InteractionStore(db, clock).consume(good)
    assert accepted.action == "status" and accepted.code == 0
    assert store.consume(good).duplicate
    assert store.consume(parse(payload(clock, token, event_id="i4"))).code == 3
    assert store.claim(good.key)
    assert not store.claim(good.key)
    store.finish(good.key)
    with db.engine.connect() as c:
        assert c.execute(select(qq_interaction_events.c.state).where(
            qq_interaction_events.c.event_key == good.key)).scalar() == "DONE"
    token2 = store.issue(*refs(), "help", ttl_seconds=1)
    clock.advance(2)
    assert store.consume(parse(payload(clock, token2, event_id="i5"))).code == 4
    token3 = store.issue(*refs(), "help")
    store.revoke(token3)
    assert store.consume(parse(payload(clock, token3, event_id="i6"))).code == 3


def test_mutually_exclusive_buttons_are_atomic(db, clock):
    store = InteractionStore(db, clock)
    first = store.issue(*refs(), "help", group_key="choice")
    second = store.issue(*refs(), "status", group_key="choice")
    events = [parse(payload(clock, first)), parse(payload(clock, second, event_id="i2"))]
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(store.consume, events))
    assert sorted(r.code for r in results) == [0, 3]


def test_untrusted_payload_cannot_dispatch_commands(db, clock):
    store = InteractionStore(db, clock)
    assert store.consume(parse(payload(clock, "rm -rf /"))).action is None
    with pytest.raises(ValueError):
        store.issue(*refs(), "run_command")
    value = payload(clock, "token")
    value["d"]["application_id"] = "another-app"
    with pytest.raises(ValueError):
        parse(value)
    value = payload(clock, "token")
    value["d"]["data"]["type"] = 12
    with pytest.raises(ValueError):
        parse(value)
    value = payload(clock, "token")
    del value["d"]["group_member_openid"]
    with pytest.raises(ValueError):
        parse(value)


def test_stale_signed_event_is_rejected(db, clock):
    store = InteractionStore(db, clock)
    token = store.issue(*refs(), "status", ttl_seconds=3600)
    event = parse(payload(clock, token))
    clock.advance(301)
    assert store.consume(event).code == 4


@pytest.mark.asyncio
async def test_ack_uses_inner_id_and_has_no_uncertain_retry(clock):
    requests = []
    def handle(request):
        requests.append(request)
        raise httpx.ReadTimeout("lost", request=request)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    async def token():
        return "test-token"
    adapter = SimpleNamespace(client=client, access_token=token,
                              config=SimpleNamespace(api_base="https://api.bot.qq.com", app_id="app"))
    event = parse(payload(clock, "not-a-command", event_id="id/with space"))
    assert await acknowledge_interaction(adapter, event, 4) == "UNKNOWN"
    assert len(requests) == 1
    assert requests[0].url.raw_path == b"/interactions/id%2Fwith%20space"
    assert __import__("json").loads(requests[0].content) == {"code": 4}
    await client.aclose()


def test_button_admin_endpoint_requires_auth_and_identity(db, clock):
    app = FastAPI()
    def auth(request: Request):
        if request.headers.get("authorization") != "Bearer test":
            raise HTTPException(401)
    adapter = SimpleNamespace(deployment_id="d", config=SimpleNamespace(kind="official", bot_id="bot", app_id="app"))
    app.include_router(router({"qq": adapter}, InteractionStore(db, clock), auth, PermissionService()))
    client = TestClient(app)
    target, principal = refs()
    body = {"target": target.model_dump(mode="json"), "principal": principal.model_dump(mode="json")}
    assert client.post("/api/v1/qq/qq/buttons", json=body).status_code == 401
    response = client.post("/api/v1/qq/qq/buttons", json=body, headers={"authorization": "Bearer test"})
    assert response.status_code == 200, response.text
    assert len(response.json()["keyboard"]["content"]["rows"][0]["buttons"]) == 2
    body["principal"]["identity_namespace"] = "onebot:qq"
    assert client.post("/api/v1/qq/qq/buttons", json=body,
                       headers={"authorization": "Bearer test"}).status_code == 422
