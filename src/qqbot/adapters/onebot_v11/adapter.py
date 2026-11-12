import asyncio
import hashlib
import json
import re
import secrets
from datetime import UTC, datetime
from uuid import uuid4

from websockets.asyncio.client import connect

from qqbot.domain import (
    AdapterCapabilities,
    ConversationRef,
    DeliveryCheck,
    FaceSegment,
    InboundEvent,
    MediaSegment,
    MentionSegment,
    ObservationMode,
    PrincipalRef,
    ReplyContext,
    ReplySegment,
    SendReceipt,
    Surface,
    TextSegment,
    UnknownSegment,
    stable_id,
)
from qqbot.permissions import PermissionService, PermissionSubject


def decode_cq(value):
    return value.replace("&#44;", ",").replace("&#91;", "[").replace("&#93;", "]").replace("&amp;", "&")


def segments(raw):
    if isinstance(raw, str):
        parts, cursor = [], 0
        for match in re.finditer(r"\[CQ:([^,\]]+)((?:,[^\]]*)?)\]", raw):
            if match.start() > cursor:
                parts.append({"type": "text", "data": {"text": decode_cq(raw[cursor : match.start()])}})
            data = {}
            for pair in match.group(2).lstrip(",").split(","):
                if "=" in pair:
                    key, value = pair.split("=", 1)
                    data[key] = decode_cq(value)
            parts.append({"type": match.group(1), "data": data})
            cursor = match.end()
        if cursor < len(raw):
            parts.append({"type": "text", "data": {"text": decode_cq(raw[cursor:])}})
        raw = parts
    if not isinstance(raw, list):
        raise ValueError("invalid message segments")  # noqa: TRY004 - Protocol validation uses ValueError.
    result = []
    for part in raw:
        kind, data = part.get("type", "unknown"), part.get("data", {})
        if kind == "text":
            result.append(TextSegment(text=str(data.get("text", ""))))
        elif kind == "at":
            result.append(MentionSegment(remote_user_id=str(data["qq"])))
        elif kind == "reply":
            result.append(ReplySegment(platform_message_id=str(data["id"])))
        elif kind == "face":
            result.append(FaceSegment(face_id=str(data["id"])))
        elif kind in {"image", "record", "file"}:
            ref = stable_id(json.dumps(data, sort_keys=True, ensure_ascii=False))
            image_url = next((value for value in (data.get("url"), data.get("file"))
                              if isinstance(value, str) and len(value) <= 8192
                              and value.startswith(("https://", "http://"))), None) if kind == "image" else None
            result.append(
                MediaSegment(type="audio" if kind == "record" else kind, attachment_ref="unresolved:" + ref,
                             image_url=image_url)
            )
        else:
            result.append(
                UnknownSegment(
                    original_type=kind,
                    digest=hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest(),
                )
            )
    return tuple(result)


class ForwardSocket:
    def __init__(self, connection):
        self.connection = connection

    async def send_json(self, payload):
        await self.connection.send(json.dumps(payload, ensure_ascii=False))

    async def close(self):
        await self.connection.close()


class OneBotAdapter:
    def __init__(self, config, deployment_id, inbox, clock, *, permissions=None):
        self.config, self.deployment_id, self.inbox, self.clock = config, deployment_id, inbox, clock
        self.permissions = permissions or PermissionService()
        self.websocket = None
        self.generation = 0
        self.pending = {}
        self.last_heartbeat = None
        self.context_gap = False
        self.account_online = False
        self.forward_task = None
        self.last_error = None
        self.conversation_names = {}

    async def start(self):
        self.config.secret()
        if self.config.websocket_url:
            self.forward_task = asyncio.create_task(self._forward_loop())

    async def stop(self):
        if self.forward_task:
            self.forward_task.cancel()
            await asyncio.gather(self.forward_task, return_exceptions=True)
            self.forward_task = None
        if self.websocket:
            await self.websocket.close()
        self._disconnect()

    async def _request(self, action, params):
        echo = str(uuid4())
        key = (self.generation, echo)
        future = asyncio.get_running_loop().create_future()
        self.pending[key] = future
        try:
            await self.websocket.send_json({"action": action, "params": params, "echo": echo})
            return await asyncio.wait_for(future, 15)
        finally:
            self.pending.pop(key, None)
            if not future.done():
                future.cancel()

    async def resolve_conversation_names(self, refs):
        if self.websocket is None:
            return dict(self.conversation_names)
        semaphore = asyncio.Semaphore(4)

        async def resolve(ref):
            key = (ref.surface, ref.remote_conversation_id)
            async with semaphore:
                is_group = ref.surface == Surface.GROUP
                try:
                    response = await asyncio.wait_for(self._request(
                        "get_group_info" if is_group else "get_stranger_info",
                        {"group_id" if is_group else "user_id": ref.remote_conversation_id},
                    ), 2)
                    data = response.get("data") or {}
                    name = data.get("group_name" if is_group else "nickname")
                    if response.get("status") == "ok" and isinstance(name, str) and name.strip():
                        self.conversation_names[key] = name.strip()
                except (TimeoutError, ConnectionError, OSError, RuntimeError, AttributeError, TypeError):
                    pass

        pending = [asyncio.create_task(resolve(ref)) for ref in refs]
        if pending:
            try:
                await asyncio.wait(pending, timeout=3)
            finally:
                for task in pending:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
        return dict(self.conversation_names)

    async def _read_forward(self, connection, generation):
        async for raw in connection:
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                continue
            if "echo" in payload:
                future = self.pending.pop((generation, str(payload["echo"])), None)
                if future and not future.done():
                    future.set_result(payload)
            elif payload.get("meta_event_type") == "heartbeat":
                if str(payload.get("self_id")) != self.config.bot_id:
                    raise ValueError("bot identity mismatch")
                self.last_heartbeat = self.clock.now()
                self.account_online = bool(payload.get("status", {}).get("online", False))
            else:
                event = self.normalize(payload)
                if event:
                    self.inbox.accept(event)

    async def _forward_loop(self):
        delay = 1
        while True:
            reader = None
            try:
                async with connect(
                    self.config.websocket_url,
                    additional_headers={"Authorization": "Bearer " + self.config.secret()},
                    open_timeout=10,
                    max_size=2 * 1024 * 1024,
                    ping_interval=30,
                ) as connection:
                    if self.websocket is not None:
                        raise RuntimeError("another connection is already active")
                    self.generation += 1
                    self.websocket = ForwardSocket(connection)
                    reader = asyncio.create_task(self._read_forward(connection, self.generation))
                    login = await self._request("get_login_info", {})
                    if (
                        login.get("status") != "ok"
                        or str(login.get("data", {}).get("user_id")) != self.config.bot_id
                    ):
                        raise ValueError("bot identity mismatch")
                    self.account_online = True
                    self.last_heartbeat = self.clock.now()
                    self.last_error = None
                    delay = 1
                    await reader
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - Reconnect protocol boundary, record only the error type.
                self.last_error = type(exc).__name__
            finally:
                if reader:
                    reader.cancel()
                    await asyncio.gather(reader, return_exceptions=True)
                self._disconnect()
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)

    def _disconnect(self):
        self.websocket = None
        self.account_online = False
        self.context_gap = True
        for future in self.pending.values():
            if not future.done():
                future.set_exception(ConnectionError("connection lost"))
        self.pending.clear()

    def normalize(self, payload):
        if str(payload.get("self_id")) != self.config.bot_id:
            raise ValueError("bot identity mismatch")
        post_type = payload.get("post_type")
        if post_type == "message":
            event_type = "message.created"
        elif post_type == "notice" and payload.get("notice_type") in {"group_recall", "friend_recall"}:
            event_type = "message.recalled"
        else:
            return None
        is_group = payload.get("message_type") == "group" or "group_id" in payload
        surface = Surface.GROUP if is_group else Surface.PRIVATE
        user = str(payload["user_id"])
        remote = str(payload["group_id"]) if is_group else user
        ref = ConversationRef(
            deployment_id=self.deployment_id,
            adapter_instance_id=self.config.instance_id,
            bot_identity_id=self.config.bot_id,
            surface=surface,
            remote_conversation_id=remote,
        )
        message_id = str(payload["message_id"])
        chain = segments(payload.get("message", []))
        stamp = datetime.fromtimestamp(payload["time"], UTC)
        event_id = stable_id(ref.key, event_type, message_id, user, str(payload["time"]))
        return InboundEvent(
            event_id=event_id,
            platform_event_id=event_id,
            event_type=event_type,
            conversation=ref,
            principal=PrincipalRef(
                adapter_instance_id=self.config.instance_id, identity_namespace="qq", remote_user_id=user
            ),
            platform_message_id=message_id,
            occurred_at=stamp,
            received_at=self.clock.now(),
            segments=chain,
            reply_to=next((s.platform_message_id for s in chain if isinstance(s, ReplySegment)), None),
            mentions_bot=any(
                isinstance(s, MentionSegment) and s.remote_user_id == self.config.bot_id for s in chain
            ),
            sender_is_bot=user == self.config.bot_id
            or user in self.config.known_bot_ids
            or bool(payload.get("sender", {}).get("is_bot", False)),
            observation_mode=ObservationMode.FULL,
            reply_context=ReplyContext(platform_message_id=message_id),
            trace_id=str(uuid4()),
        )

    async def handle_socket(self, websocket):
        auth = websocket.headers.get("authorization", "")
        if (
            not secrets.compare_digest(auth, "Bearer " + self.config.secret())
            or websocket.headers.get("x-self-id") != self.config.bot_id
        ):
            await websocket.close(code=1008)
            return
        if self.websocket is not None:
            await websocket.close(code=1008)
            return
        await websocket.accept()
        self.generation += 1
        generation = self.generation
        self.websocket = websocket
        self.account_online = True
        self.last_heartbeat = self.clock.now()
        try:
            while True:
                raw = await asyncio.wait_for(websocket.receive_text(), timeout=90)
                if len(raw.encode()) > 2 * 1024 * 1024:
                    await websocket.close(code=1009)
                    break
                payload = json.loads(raw)
                if not isinstance(payload, dict):
                    continue
                if "echo" in payload:
                    future = self.pending.pop((generation, str(payload["echo"])), None)
                    if future and not future.done():
                        future.set_result(payload)
                    continue
                if payload.get("meta_event_type") == "heartbeat":
                    if str(payload.get("self_id")) != self.config.bot_id:
                        raise ValueError("bot identity mismatch")
                    self.last_heartbeat = self.clock.now()
                    self.account_online = bool(payload.get("status", {}).get("online", False))
                    continue
                event = self.normalize(payload)
                if event:
                    self.inbox.accept(event)
        finally:
            if generation == self.generation:
                self._disconnect()

    async def capabilities(self, target):
        return AdapterCapabilities(
            can_reply=self.account_online,
            can_initiate_message=self.account_online,
            can_observe_unaddressed_messages=True,
            supports_reply_reference=True,
            supports_mention=True,
            supports_recall=True,
            observation_mode=ObservationMode.FULL,
            delivery_constraints=("text_only", "history_gap_after_disconnect"),
        )

    async def validate_delivery(self, envelope):
        if envelope.official_content is not None or envelope.content_format != "text":
            return DeliveryCheck(allowed=False, reason="UNSUPPORTED_RICH_CONTENT")
        if (
            envelope.target.bot_identity_id != self.config.bot_id
            or envelope.target.adapter_instance_id != self.config.instance_id
        ):
            return DeliveryCheck(allowed=False, reason="IDENTITY_MISMATCH")
        if not self.permissions.check(
            PermissionSubject.from_refs(envelope.target), "notification.send", "conversation:" + envelope.target.key
        ).allowed:
            return DeliveryCheck(allowed=False, reason="TARGET_DENIED")
        if envelope.target.surface == Surface.GUILD_CHANNEL:
            return DeliveryCheck(allowed=False, reason="UNSUPPORTED_SURFACE")
        return DeliveryCheck(
            allowed=self.websocket is not None and self.account_online,
            reason="OK" if self.account_online else "OFFLINE",
        )

    async def send(self, envelope):
        check = await self.validate_delivery(envelope)
        if not check.allowed:
            return SendReceipt(state="RETRYABLE", error_code=check.reason)
        echo = str(uuid4())
        key = (self.generation, echo)
        future = asyncio.get_running_loop().create_future()
        self.pending[key] = future
        is_group = envelope.target.surface == Surface.GROUP
        chain = [{"type": "text", "data": {"text": envelope.text}}]
        payload = {
            "action": "send_group_msg" if is_group else "send_private_msg",
            "echo": echo,
            "params": {
                "group_id" if is_group else "user_id": envelope.target.remote_conversation_id,
                "message": chain,
            },
        }
        try:
            await self.websocket.send_json(payload)
            response = await asyncio.wait_for(future, timeout=15)
            if response.get("status") == "ok" and response.get("retcode") == 0:
                message_id = response.get("data", {}).get("message_id")
                if message_id is None:
                    return SendReceipt(state="UNKNOWN", error_code="MISSING_MESSAGE_ID")
                return SendReceipt(state="SENT", platform_message_id=str(message_id))
            return SendReceipt(state="FAILED", error_code=str(response.get("retcode", "API_ERROR")))
        except (TimeoutError, ConnectionError, RuntimeError, OSError):
            return SendReceipt(state="UNKNOWN", error_code="RECEIPT_LOST")
        finally:
            self.pending.pop(key, None)
            if not future.done():
                future.cancel()
