"""Regression coverage for adapter compatibility and persisted rich payloads."""
import json
from datetime import timedelta
from uuid import uuid4

import httpx
from sqlalchemy import select

from qqbot.adapters.qq_official.adapter import OfficialAdapter
from qqbot.domain import ConversationRef, OutboundEnvelope, ReplyContext, Surface
from qqbot.domain.official import OfficialContent, TemplateMarkdown
from qqbot.outbox.service import Outbox
from qqbot.storage.schema import outbox


def message(clock, *, instance="qq", bot="bot", **kwargs):
    return OutboundEnvelope(
        delivery_id=str(uuid4()),
        target=ConversationRef(deployment_id="d", adapter_instance_id=instance,
            bot_identity_id=bot, surface=Surface.PRIVATE, remote_conversation_id="300"),
        text="Choose", origin_id=str(uuid4()), expires_at=clock.now() + timedelta(minutes=5),
        reply_context=ReplyContext(platform_message_id="source", msg_seq=1,
            expires_at=clock.now() + timedelta(minutes=5)), **kwargs,
    )


async def test_keyboard_json_survives_restart_and_outbox_dispatch(db, clock, official_config, incoming):
    keyboard = {"content": {"rows": [{"buttons": [{
        "id": "help", "render_data": {"label": "Help"},
        "action": {"type": 2, "data": "/help"},
    }]}]}}
    envelope = message(clock, official_content=OfficialContent(keyboard=keyboard))
    assert OutboundEnvelope.model_validate_json(envelope.model_dump_json()).official_content.keyboard == keyboard
    with db.transaction() as c:
        ident = Outbox(db, clock).enqueue(c, envelope)
    calls = []

    def handle(request):
        calls.append(json.loads(request.content))
        return httpx.Response(200, json={"access_token": "tok", "expires_in": 7200}
            if "Token" in request.url.path else {"id": "sent"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        adapter = OfficialAdapter(official_config, "d", incoming, clock, client)
        await Outbox(db, clock).dispatch_once({"qq": adapter})
    assert calls[-1]["keyboard"]["content"]["rows"][0]["buttons"][0]["action"]["data"] == "/help"
    with db.engine.connect() as c:
        assert c.execute(select(outbox.c.state).where(outbox.c.id == ident)).scalar_one() == "SENT"


async def test_template_obeys_markdown_flag(clock, official_config, incoming):
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(500))) as client:
        adapter = OfficialAdapter(official_config.model_copy(update={"markdown_enabled": False}),
                                  "d", incoming, clock, client)
        result = await adapter.validate_delivery(message(clock,
            official_content=OfficialContent(template_markdown=TemplateMarkdown(template_id="tpl"))))
    assert not result.allowed and result.reason == "MARKDOWN_DISABLED"


async def test_signed_callback_ack_recovery_and_outer_reply_id(tmp_path, clock, official_config, monkeypatch):
    from qqbot.app import create_app
    from qqbot.config import Settings
    from qqbot.domain import PrincipalRef

    monkeypatch.setenv("QQBOT_ADMIN_TOKEN", "admin-test")
    calls = []
    ack_count = 0

    def handle(request):
        nonlocal ack_count
        calls.append((request.url.path, json.loads(request.content)))
        if "Token" in request.url.path:
            return httpx.Response(200, json={"access_token": "tok", "expires_in": 7200})
        if request.url.path.startswith("/interactions/"):
            ack_count += 1
            return httpx.Response(503 if ack_count == 1 else 200, json={})
        return httpx.Response(200, json={"id": "sent"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as platform:
        app = create_app(Settings(deployment_id="d", database=str(tmp_path / "callback.db"),
            adapters=(official_config,)), clock=clock, run_background=False, official_client=platform)
        adapter = app.state.adapters["qq"]
        target = message(clock).target
        principal = PrincipalRef(adapter_instance_id="qq", identity_namespace="qq_app:app", remote_user_id="300")
        token = app.state.qq_interactions.issue(target, principal, "status")
        payload = {"op": 0, "t": "INTERACTION_CREATE", "id": "outer-event", "d": {
            "application_id": "app", "id": "inner-interaction", "type": 11, "scene": "c2c",
            "timestamp": clock.now().isoformat(), "user_openid": "300",
            "data": {"type": 11, "resolved": {"button_data": token, "button_id": "status"}}}}
        body = json.dumps(payload).encode()
        stamp = str(int(clock.now().timestamp()))
        headers = {"x-signature-timestamp": stamp,
            "x-signature-ed25519": adapter.key().sign(stamp.encode() + body).hex()}
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
            assert (await client.post("/webhooks/qq/qq", content=body)).status_code == 401
            assert not calls
            assert (await client.post("/webhooks/qq/qq", content=body, headers=headers)).status_code == 200
            with app.state.db.engine.connect() as c:
                assert c.execute(select(outbox.c.id)).all() == []
            # A rejected ACK may be retried via a duplicated signed callback; claim is once-only.
            assert (await client.post("/webhooks/qq/qq", content=body, headers=headers)).status_code == 200
            assert (await client.post("/webhooks/qq/qq", content=body, headers=headers)).status_code == 200
            with app.state.db.engine.connect() as c:
                rows = c.execute(select(outbox)).mappings().all()
                assert len(rows) == 1
                restored = OutboundEnvelope.model_validate_json(rows[0]["envelope_json"])
                assert restored.reply_context.event_id == "outer-event"
            await app.state.core.outbox.dispatch_once(app.state.adapters)
            sends = [body for path, body in calls if path.endswith("/messages")]
            assert len(sends) == 1
            assert sends[0]["event_id"] == "outer-event" and "msg_id" not in sends[0]
            assert all(path == "/interactions/inner-interaction" for path, _ in calls if "interactions" in path)
        await app.state.qq_onboarding.close()
        app.state.db.close()


async def test_admin_rich_endpoint_auth_validation_and_idempotence(tmp_path, clock, official_config,
                                                                  official_event, monkeypatch):
    from qqbot.app import create_app
    from qqbot.config import Settings
    from qqbot.ingress.service import Inbox

    monkeypatch.setenv("QQBOT_ADMIN_TOKEN", "admin-test")
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(500))) as platform:
        app = create_app(Settings(deployment_id="d", database=str(tmp_path / "admin-rich.db"),
            adapters=(official_config,)), clock=clock, run_background=False, official_client=platform)
        event = app.state.adapters["qq"].normalize(official_event)
        Inbox(app.state.db).accept(event)
        body = {"conversation_id": event.conversation.key, "request_id": "unique-admin-send",
                "text": "**message**", "content_format": "markdown", "reply_to_event_id": event.event_id}
        headers = {"authorization": "Bearer admin-test"}
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
            url = "/api/v1/qq/qq/messages"
            assert (await client.post(url, json=body)).status_code == 401
            assert (await client.post(url, json=body | {"text": ""}, headers=headers)).status_code == 422
            assert (await client.post(url, json=body | {"reply_to_event_id": "foreign-event"}, headers=headers)).status_code == 422
            one = await client.post(url, json=body, headers=headers)
            assert one.status_code == 200, one.text
            two = await client.post(url, json=body, headers=headers)
            assert one.json()["delivery_id"] == two.json()["delivery_id"]
            with app.state.db.engine.connect() as c:
                rows = c.execute(select(outbox)).mappings().all()
                assert len(rows) == 1
                assert OutboundEnvelope.model_validate_json(rows[0]["envelope_json"]).content_format == "markdown"
        await app.state.qq_onboarding.close()
        app.state.db.close()
