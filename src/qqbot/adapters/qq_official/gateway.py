"""QQ's authenticated WebSocket transport over the adapter's existing event boundary."""
import asyncio
import json
import logging
from urllib.parse import urlsplit

import httpx
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, WebSocketException

OFFICIAL_HOSTS = frozenset({"api.bot.qq.com", "api.sgroup.qq.com", "sandbox.api.sgroup.qq.com"})
FATAL_CLOSES = frozenset({4001, 4002, 4010, 4011, 4012, 4013, 4014, 4914, 4915})
# WebSocket debug frame logging would include Identify credentials.
SOCKET_LOGGER = logging.Logger("qqbot.qq_gateway.socket", level=logging.CRITICAL + 1)  # noqa: LOG001 — isolated from frame-log configuration.


class OfficialConnect(connect):
    def process_redirect(self, exc):
        return exc


class GatewayError(Exception):
    def __init__(self, code, *, fatal=False):
        self.code, self.fatal = code, fatal
        super().__init__(code)


def validate_gateway_url(url):
    if not isinstance(url, str):
        raise GatewayError("GATEWAY_URL_REJECTED", fatal=True)
    try:
        parsed = urlsplit(url)
        valid = (parsed.scheme == "wss" and parsed.hostname in OFFICIAL_HOSTS
                 and parsed.port in {None, 443} and not parsed.username and not parsed.password and not parsed.fragment)
    except ValueError:
        valid = False
    if not valid:
        raise GatewayError("GATEWAY_URL_REJECTED", fatal=True)
    return url


class QQGateway:
    def __init__(self, adapter):
        self.adapter = adapter
        self.task = None
        self.socket = None
        self.session_id = None
        self.seq = None
        self.state = "STOPPED"
        self.error = None
        self.retry_count = 0
        self.next_retry_seconds = None
        self.last_event_at = self.last_ack_at = None
        self.awaiting_ack = False

    def snapshot(self):
        connected = self.state == "CONNECTED" and self.task is not None and not self.task.done()
        return {"state": self.state, "connected": connected, "healthy": connected,
            "error": self.error, "retry_count": self.retry_count, "next_retry_seconds": self.next_retry_seconds,
            "last_event_at": self.last_event_at, "last_ack_at": self.last_ack_at,
            "intents": self.adapter.config.intents, "interaction_events": bool(self.adapter.config.intents & (1 << 26))}

    async def start(self):
        if self.task is None or self.task.done():
            self.state, self.error, self.retry_count = "CONNECTING", None, 0
            self.task = asyncio.create_task(self._run(), name="qq-gateway:" + self.adapter.config.instance_id)

    async def stop(self):
        if self.task is not None:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
            self.task = None
        self.state, self.next_retry_seconds = "STOPPED", None

    async def _gateway(self):
        token = await self.adapter.access_token()
        response = await self.adapter.client.get(self.adapter.config.api_base.rstrip("/") + "/gateway/bot",
            headers={"Authorization": "QQBot " + token, "X-Union-Appid": str(self.adapter.config.app_id)},
            follow_redirects=False)
        if response.status_code == 401:
            self.adapter.token = None
        if response.status_code == 403:
            raise GatewayError("GATEWAY_ACCESS_DENIED", fatal=True)
        response.raise_for_status()
        return validate_gateway_url(response.json()["url"]), token

    async def _run(self):
        while True:
            began = asyncio.get_running_loop().time()
            try:
                self.state, self.next_retry_seconds = "CONNECTING", None
                url, token = await self._gateway()
                await self._session(url, token)
                raise GatewayError("CONNECTION_CLOSED")
            except asyncio.CancelledError:
                raise
            except ConnectionClosed as exc:
                code = exc.rcvd.code if exc.rcvd is not None else None
                self.error = "GATEWAY_CLOSED_" + str(code or "UNKNOWN")
                if code in FATAL_CLOSES:
                    self.state = "FATAL"
                    return
                if code == 4004:
                    self.adapter.token = None
                if code in {4006, 4007} or code is not None and 4900 <= code <= 4913:
                    self.session_id = self.seq = None
                minimum_delay = 60 if code == 4008 else 1
            except GatewayError as exc:
                self.error = exc.code
                if exc.fatal:
                    self.state = "FATAL"
                    return
                minimum_delay = 1
            except (httpx.HTTPError, WebSocketException, OSError, ValueError, KeyError, TypeError, TimeoutError):
                # Fixed diagnostics exclude access tokens, raw frames, URLs and platform error bodies.
                self.error = "GATEWAY_UNAVAILABLE"
                minimum_delay = 1
            except Exception:  # noqa: BLE001 — callback failures retain the last committed sequence for Resume.
                self.error = "EVENT_HANDLER_FAILED"
                minimum_delay = 1
            if asyncio.get_running_loop().time() - began >= 60:
                self.retry_count = 0
            self.retry_count += 1
            self.next_retry_seconds = max(minimum_delay, min(60, 2 ** min(self.retry_count - 1, 6)))
            self.state = "RECONNECTING"
            await asyncio.sleep(self.next_retry_seconds)

    @staticmethod
    def _payload(raw):
        payload = json.loads(raw)
        if not isinstance(payload, dict) or type(payload.get("op")) is not int:
            raise GatewayError("INVALID_GATEWAY_FRAME")
        return payload

    async def _session(self, url, token):
        async with OfficialConnect(url, open_timeout=10, close_timeout=3, ping_interval=None,
                                   max_size=2 * 1024 * 1024, max_queue=16, logger=SOCKET_LOGGER) as socket:
            self.socket = socket
            try:
                hello = self._payload(await asyncio.wait_for(socket.recv(), 10))
                interval = hello.get("d", {}).get("heartbeat_interval")
                if hello["op"] != 10 or type(interval) not in {int, float} or not 0 < interval <= 3600000:
                    raise GatewayError("INVALID_GATEWAY_HELLO")
                self.state, self.awaiting_ack = "AUTHENTICATING", False
                if self.session_id is not None and self.seq is not None:
                    packet = {"op": 6, "d": {"token": "QQBot " + token, "session_id": self.session_id, "seq": self.seq}}
                else:
                    packet = {"op": 2, "d": {"token": "QQBot " + token,
                        "intents": self.adapter.config.intents, "shard": [0, 1], "properties": {}}}
                await socket.send(json.dumps(packet))
                reader = asyncio.create_task(self._read(socket))
                heartbeat = asyncio.create_task(self._heartbeat(socket, interval / 1000))
                try:
                    done, _ = await asyncio.wait({reader, heartbeat}, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        task.result()
                finally:
                    reader.cancel()
                    heartbeat.cancel()
                    await asyncio.gather(reader, heartbeat, return_exceptions=True)
            finally:
                self.socket = None
                if self.state != "FATAL":
                    self.state = "DISCONNECTED"

    async def _heartbeat(self, socket, interval):
        while True:
            await asyncio.sleep(interval)
            if self.awaiting_ack:
                raise GatewayError("HEARTBEAT_ACK_MISSING")
            await self._send_heartbeat(socket)

    async def _send_heartbeat(self, socket):
        self.awaiting_ack = True
        await socket.send(json.dumps({"op": 1, "d": self.seq}))

    async def _read(self, socket):
        deadline = asyncio.get_running_loop().time() + 15
        while True:
            timeout = None if self.state == "CONNECTED" else max(0, deadline - asyncio.get_running_loop().time())
            payload = self._payload(await asyncio.wait_for(socket.recv(), timeout))
            op = payload["op"]
            if op == 11:
                self.awaiting_ack = False
                self.last_ack_at = self.adapter.clock.now().isoformat()
            elif op == 1:
                await self._send_heartbeat(socket)
            elif op == 7:
                raise GatewayError("RECONNECT_REQUESTED")
            elif op == 9:
                if payload.get("d") is not True:
                    self.session_id = self.seq = None
                raise GatewayError("INVALID_SESSION")
            elif op == 0:
                seq, kind = payload.get("s"), payload.get("t")
                if type(seq) is not int or seq < 0 or not isinstance(kind, str):
                    raise GatewayError("INVALID_GATEWAY_DISPATCH")
                if kind == "READY":
                    session = payload.get("d", {}).get("session_id")
                    if not isinstance(session, str) or not session or len(session) > 512:
                        raise GatewayError("INVALID_GATEWAY_READY")
                    self.session_id = session
                    self.state, self.error = "CONNECTED", None
                elif kind == "RESUMED":
                    if self.session_id is None:
                        raise GatewayError("INVALID_GATEWAY_RESUME")
                    self.state, self.error = "CONNECTED", None
                else:
                    if self.session_id is None:
                        raise GatewayError("GATEWAY_NOT_AUTHENTICATED")
                    await self.adapter.accept_payload(payload)
                if kind == "READY" or self.seq is None or seq > self.seq:
                    self.seq = seq
                self.last_event_at = self.adapter.clock.now().isoformat()
