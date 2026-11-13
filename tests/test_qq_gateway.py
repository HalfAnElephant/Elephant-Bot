"""Authenticated QQ gateway lifecycle with deterministic sockets and durable ingress."""
import asyncio
import json
import logging

import httpx
import pytest
from pydantic import ValidationError
from sqlalchemy import func, select
from websockets.exceptions import ConnectionClosedError
from websockets.frames import Close

from qqbot.adapters.qq_official import gateway as module
from qqbot.adapters.qq_official.adapter import OfficialAdapter
from qqbot.config import AdapterConfig
from qqbot.storage.schema import inbox


class Socket:
    def __init__(self, *payloads):
        self.frames = asyncio.Queue()
        self.sent = []
        self.closed = False
        for payload in payloads:
            self.feed(payload)

    def feed(self, payload):
        self.frames.put_nowait(payload if isinstance(payload, Exception) else json.dumps(payload))

    async def recv(self):
        frame = await self.frames.get()
        if isinstance(frame, Exception):
            raise frame
        return frame

    async def send(self, payload):
        self.sent.append(json.loads(payload))


class Connections:
    def __init__(self, *sockets):
        self.sockets = list(sockets)
        self.calls = []

    def __call__(self, url, **kwargs):
        self.calls.append((url, kwargs))
        socket = self.sockets.pop(0)
        class Context:
            async def __aenter__(self):
                return socket
            async def __aexit__(self, *_):
                socket.closed = True
        return Context()


def hello(interval=100000):
    return {"op": 10, "d": {"heartbeat_interval": interval}}


def ready(seq=1):
    return {"op": 0, "s": seq, "t": "READY", "d": {"session_id": "qq-session"}}


async def until(condition):
    async with asyncio.timeout(5):
        while not condition():
            await asyncio.sleep(0.001)


@pytest.fixture
async def setup(incoming, clock, official_config, monkeypatch):
    requests = []
    async def rest(request):
        requests.append(request)
        if request.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "PRIVATE_TOKEN", "expires_in": 3600})
        assert request.url.path == "/gateway/bot"
        assert request.headers["authorization"] == "QQBot PRIVATE_TOKEN"
        return httpx.Response(200, json={"url": "wss://api.bot.qq.com/websocket/"})
    client = httpx.AsyncClient(transport=httpx.MockTransport(rest))
    adapter = OfficialAdapter(official_config.model_copy(update={"transport": "websocket"}), "d", incoming, clock, client)
    yield adapter, requests
    await adapter.stop()
    assert not client.is_closed
    await client.aclose()


async def test_identify_ready_ingress_dedup_relationship_and_interaction(setup, official_event, monkeypatch, db):
    adapter, requests = setup
    socket = Socket(hello())
    connections = Connections(socket)
    monkeypatch.setattr(module, "OfficialConnect", connections)
    received = []
    async def interaction(payload):
        received.append(payload)
    adapter.interaction_handler = interaction
    await adapter.start()
    await until(lambda: bool(socket.sent))
    assert not adapter.gateway.snapshot()["healthy"]
    assert socket.sent == [{"op": 2, "d": {"token": "QQBot PRIVATE_TOKEN", "intents": 1 << 25,
                                           "shard": [0, 1], "properties": {}}}]
    socket.feed(ready())
    await until(lambda: adapter.gateway.snapshot()["connected"])
    message = {**official_event, "s": 2}
    socket.feed(message)
    socket.feed(message)
    socket.feed({"op": 0, "s": 3, "t": "FRIEND_DEL", "id": "removed", "d": {
        "openid": "300", "timestamp": adapter.clock.now().isoformat()}})
    button = {"op": 0, "s": 4, "t": "INTERACTION_CREATE", "id": "button", "d": {"id": "interaction"}}
    socket.feed(button)
    await until(lambda: adapter.gateway.seq == 4)
    with db.engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(inbox)).scalar_one() == 1
    private = adapter.normalize({**official_event, "t": "C2C_MESSAGE_CREATE", "d": {
        **official_event["d"], "author": {"user_openid": "300"}}})
    assert adapter.relationships.denied(private.conversation) == "RELATIONSHIP_REMOVED"
    assert received == [button]
    socket.feed(message)
    socket.feed({"op": 1})
    await until(lambda: socket.sent[-1]["op"] == 1)
    assert socket.sent[-1]["d"] == 4
    assert adapter.gateway.snapshot()["interaction_events"] is False
    assert "PRIVATE_TOKEN" not in json.dumps(adapter.gateway.snapshot())
    assert not connections.calls[0][1]["logger"].isEnabledFor(logging.DEBUG)
    assert len([request for request in requests if request.url.path == "/gateway/bot"]) == 1
    await adapter.gateway.stop()
    assert socket.closed and adapter.gateway.snapshot()["healthy"] is False
    assert adapter.gateway.task is None


async def test_heartbeats_ack_server_request_and_missing_ack_disconnect(setup, monkeypatch):
    adapter, _ = setup
    socket = Socket(hello(20), ready())
    monkeypatch.setattr(module, "OfficialConnect", Connections(socket))
    await adapter.start()
    await until(lambda: any(packet["op"] == 1 for packet in socket.sent))
    assert socket.sent[-1] == {"op": 1, "d": 1}
    socket.feed({"op": 11})
    await until(lambda: adapter.gateway.last_ack_at is not None)
    socket.feed({"op": 1})
    await until(lambda: len([packet for packet in socket.sent if packet["op"] == 1]) >= 2)
    await until(lambda: adapter.gateway.snapshot()["error"] == "HEARTBEAT_ACK_MISSING")
    snapshot = adapter.gateway.snapshot()
    assert snapshot["state"] == "RECONNECTING" and not snapshot["healthy"]
    assert snapshot["next_retry_seconds"] >= 1 and socket.closed


async def test_reconnect_resume_and_invalid_session_fresh_identify(setup, monkeypatch):
    adapter, _ = setup
    first, second, third = Socket(hello(), ready(7)), Socket(hello()), Socket(hello())
    connections = Connections(first, second, third)
    monkeypatch.setattr(module, "OfficialConnect", connections)
    await adapter.start()
    await until(lambda: adapter.gateway.snapshot()["connected"])
    first.feed({"op": 7})
    await until(lambda: bool(second.sent))
    assert second.sent[0] == {"op": 6, "d": {"token": "QQBot PRIVATE_TOKEN", "session_id": "qq-session", "seq": 7}}
    assert not adapter.gateway.snapshot()["connected"]
    second.feed({"op": 0, "s": 8, "t": "RESUMED", "d": ""})
    await until(lambda: adapter.gateway.snapshot()["connected"])
    second.feed({"op": 9, "d": False})
    await until(lambda: bool(third.sent))
    assert third.sent[0]["op"] == 2
    assert adapter.gateway.session_id is None and adapter.gateway.seq is None
    assert adapter.gateway.snapshot()["retry_count"] == 2
    await adapter.gateway.stop()
    assert first.closed and second.closed and third.closed


async def test_fatal_intents_stop_without_fallback_or_secret_diagnostics(setup, monkeypatch):
    adapter, _ = setup
    socket = Socket(hello(), ConnectionClosedError(Close(4014, "PRIVATE_TOKEN"), None))
    connections = Connections(socket)
    monkeypatch.setattr(module, "OfficialConnect", connections)
    await adapter.start()
    await until(lambda: adapter.gateway.task.done())
    state = adapter.gateway.snapshot()
    assert state["state"] == "FATAL" and state["error"] == "GATEWAY_CLOSED_4014"
    assert not state["healthy"] and len(connections.calls) == 1
    assert socket.sent[0]["d"]["intents"] == 1 << 25
    assert "PRIVATE_TOKEN" not in json.dumps(state)


@pytest.mark.parametrize("url", ["ws://api.bot.qq.com/websocket", "wss://evil.test/", "wss://api.bot.qq.com.evil.test/",
    "wss://token@api.bot.qq.com/", "wss://api.bot.qq.com:444/", "wss://api.bot.qq.com/#fragment", None])
def test_gateway_url_rejects_nonofficial_or_insecure_targets(url):
    with pytest.raises(module.GatewayError, match="GATEWAY_URL_REJECTED"):
        module.validate_gateway_url(url)


def test_official_connection_never_follows_redirect():
    connection = module.OfficialConnect("wss://api.bot.qq.com/websocket/")
    failure = ValueError("redirect")
    assert connection.process_redirect(failure) is failure


@pytest.mark.parametrize("intents,full", [(0, False), (True, False), (1 << 28, False), (1 << 9, False)])
def test_gateway_intent_schema_requires_supported_explicit_permissions(official_config, intents, full):
    with pytest.raises(ValidationError):
        AdapterConfig.model_validate({**official_config.model_dump(), "transport": "websocket",
                                      "intents": intents, "full_mode_enabled": full})


async def test_webhook_start_preserves_signature_validation_without_gateway(official_config, incoming, clock):
    adapter = OfficialAdapter(official_config, "d", incoming, clock)
    try:
        await adapter.start()
        assert adapter.gateway is None
        with pytest.raises(ValueError, match="invalid callback signature"):
            adapter.callback(b'{"op":0,"t":"INTERACTION_CREATE","d":{}}', {})
    finally:
        await adapter.stop()


async def test_stop_during_hello_and_repeat_start_preserve_shared_client(setup, monkeypatch):
    adapter, _ = setup
    first, second = Socket(), Socket(hello(), ready())
    connections = Connections(first, second)
    monkeypatch.setattr(module, "OfficialConnect", connections)
    await adapter.start()
    await until(lambda: adapter.gateway.socket is first)
    assert not adapter.gateway.snapshot()["healthy"]
    await adapter.stop()
    assert first.closed and not first.sent
    assert not adapter.client.is_closed
    assert adapter.gateway.task is None and adapter.gateway.socket is None
    await adapter.start()
    await until(lambda: adapter.gateway.snapshot()["healthy"])
    assert second.sent[0]["op"] == 2
    await adapter.stop()
    assert second.closed and adapter.gateway.snapshot()["state"] == "STOPPED"


async def test_discovered_gateway_url_is_validated_before_any_websocket_auth(setup, monkeypatch):
    adapter, _ = setup
    async def unsafe(request):
        if request.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "PRIVATE_TOKEN", "expires_in": 3600})
        return httpx.Response(200, json={"url": "wss://attacker.test/websocket"})
    adapter.client._transport = httpx.MockTransport(unsafe)
    connections = Connections()
    monkeypatch.setattr(module, "OfficialConnect", connections)
    await adapter.start()
    await until(lambda: adapter.gateway.task.done())
    assert connections.calls == []
    snapshot = adapter.gateway.snapshot()
    assert snapshot["state"] == "FATAL" and snapshot["error"] == "GATEWAY_URL_REJECTED"
    assert not snapshot["healthy"]
