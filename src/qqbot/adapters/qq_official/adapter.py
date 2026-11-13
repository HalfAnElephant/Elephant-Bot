import asyncio
import json
from datetime import UTC, datetime, timedelta
from urllib.parse import quote
from uuid import uuid4

import httpx
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from qqbot.domain import (
    AdapterCapabilities,
    ConversationRef,
    DeliveryCheck,
    InboundEvent,
    ObservationMode,
    PrincipalRef,
    ReplyContext,
    SendReceipt,
    Surface,
    TextSegment,
    stable_id,
)
from qqbot.permissions import PermissionService, PermissionSubject

from .events import extra_segments
from .relationships import RelationshipStore
from .rendering import message_body


class OfficialAdapter:
    def __init__(self, config, deployment_id, inbox, clock, client=None, *, permissions=None):
        self.config, self.deployment_id, self.inbox, self.clock = config, deployment_id, inbox, clock
        self.permissions = permissions or PermissionService()
        self._owns_client = client is None
        self.client = client if client is not None else httpx.AsyncClient(timeout=15, follow_redirects=False)
        self.token = None
        self.token_expires = clock.now()
        self.token_lock = asyncio.Lock()
        self.full_event_observed = False
        self.relationships = RelationshipStore(inbox.db, clock)
        self.interaction_handler = None
        self.gateway = None

    def key(self):
        secret = self.config.secret().encode()
        seed = (secret * ((32 + len(secret) - 1) // len(secret)))[:32]
        return Ed25519PrivateKey.from_private_bytes(seed)

    async def start(self):
        self.key()
        if self.config.transport == "websocket":
            from .gateway import QQGateway
            if self.gateway is None:
                self.gateway = QQGateway(self)
            await self.gateway.start()

    async def stop(self):
        if self.gateway is not None:
            await self.gateway.stop()
        if self._owns_client:
            await self.client.aclose()

    def verify(self, body: bytes, timestamp: str, signature: str):
        try:
            age = abs(self.clock.now().timestamp() - int(timestamp))
            if age > self.config.signature_max_age_seconds:
                raise ValueError("signature timestamp expired")
            self.key().public_key().verify(bytes.fromhex(signature), timestamp.encode() + body)
        except (InvalidSignature, ValueError, TypeError) as exc:
            raise ValueError("invalid callback signature") from exc

    def callback(self, body, headers):
        payload = json.loads(body)
        if not isinstance(payload, dict):
            raise ValueError("invalid payload")  # noqa: TRY004 - Callback boundary uses ValueError.
        if payload.get("op") == 13:
            data = payload["d"]
            token, stamp = data["plain_token"], data["event_ts"]
            if not isinstance(token, str) or not isinstance(stamp, str) or len(token) > 2048:
                raise ValueError("invalid challenge")
            if abs(self.clock.now().timestamp() - int(stamp)) > self.config.signature_max_age_seconds:
                raise ValueError("challenge timestamp expired")
            return {"plain_token": token, "signature": self.key().sign((stamp + token).encode()).hex()}
        self.verify(body, headers.get("x-signature-timestamp", ""), headers.get("x-signature-ed25519", ""))
        self._accept_event(payload)
        return {"op": 12}

    def _accept_event(self, payload):
        if payload.get("op") != 0 or self.relationships.accept(payload, self):
            return
        event = self.normalize(payload)
        if event:
            self.inbox.accept(event)

    async def accept_payload(self, payload):
        """Accept only payloads received through an authenticated transport."""
        if payload.get("op") == 0 and payload.get("t") == "INTERACTION_CREATE":
            if self.interaction_handler is None:
                raise RuntimeError("interaction handler is unavailable")
            await self.interaction_handler(payload)
            return
        self._accept_event(payload)

    def normalize(self, payload):
        kind, data = payload.get("t"), payload["d"]
        if kind not in {
            "GROUP_AT_MESSAGE_CREATE",
            "GROUP_MESSAGE_CREATE",
            "C2C_MESSAGE_CREATE",
            "AT_MESSAGE_CREATE",
            "MESSAGE_CREATE",
            "DIRECT_MESSAGE_CREATE",
        }:
            return None
        author = data["author"]
        if kind.startswith("GROUP_"):
            surface, remote = Surface.GROUP, str(data["group_openid"])
            user = str(author.get("member_openid") or author.get("id") or "")
            if kind == "GROUP_MESSAGE_CREATE":
                if not self.config.full_mode_enabled:
                    return None
                self.full_event_observed = True
        elif kind == "C2C_MESSAGE_CREATE":
            user = str(author.get("user_openid") or author.get("id") or "")
            surface, remote = Surface.PRIVATE, user
        elif kind == "DIRECT_MESSAGE_CREATE":
            user = str(author["id"])
            surface, remote = Surface.GUILD_PRIVATE, str(data["guild_id"])
        else:
            user = str(author["id"])
            surface, remote = Surface.GUILD_CHANNEL, str(data["channel_id"])
        if not user or not remote:
            raise ValueError("missing remote identity")
        ref = ConversationRef(
            deployment_id=self.deployment_id,
            adapter_instance_id=self.config.instance_id,
            bot_identity_id=self.config.bot_id,
            surface=surface,
            remote_conversation_id=remote,
        )
        stamp = datetime.fromisoformat(data["timestamp"])
        if stamp.tzinfo is None:
            raise ValueError("timezone required")
        msg_id = str(data["id"])
        platform_id = str(payload.get("id") or stable_id(kind, ref.key, msg_id, user, data["timestamp"]))
        full = self.config.full_mode_enabled and (self.config.full_mode_verified or self.full_event_observed)
        chain = [TextSegment(text=str(data.get("content", "")))]
        chain.extend(extra_segments(data))
        reference = data.get("message_reference")
        reference = reference if isinstance(reference, dict) else {}
        return InboundEvent(
            event_id=stable_id(ref.key, "message.created", msg_id),
            platform_event_id=platform_id,
            event_type="message.created",
            conversation=ref,
            principal=PrincipalRef(
                adapter_instance_id=self.config.instance_id,
                identity_namespace="qq_app:" + str(self.config.app_id),
                remote_user_id=user,
            ),
            platform_message_id=msg_id,
            occurred_at=stamp.astimezone(UTC),
            received_at=self.clock.now(),
            segments=tuple(chain),
            reply_to=reference.get("message_id"),
            mentions_bot=kind in {"GROUP_AT_MESSAGE_CREATE", "AT_MESSAGE_CREATE"}
            or any(isinstance(m, dict) and str(m.get("id")) == self.config.bot_id
                   for m in (data.get("mentions") if isinstance(data.get("mentions"), list) else [])),
            sender_is_bot=bool(author.get("bot", False)) or user == self.config.bot_id,
            observation_mode=ObservationMode.FULL
            if full or surface == Surface.PRIVATE
            else ObservationMode.MENTIONS_ONLY,
            reply_context=ReplyContext(
                platform_message_id=msg_id,
                event_id=platform_id,
                expires_at=stamp + timedelta(seconds=self.reply_policy(surface)[0]),
                max_replies=self.reply_policy(surface)[1],
            ),
            trace_id=str(uuid4()),
        )

    def reply_policy(self, surface):
        if surface == Surface.PRIVATE:
            return (self.config.c2c_reply_window_seconds or self.config.reply_window_seconds,
                    self.config.c2c_max_replies)
        if surface == Surface.GROUP:
            return (self.config.group_reply_window_seconds or min(self.config.reply_window_seconds, 300),
                    self.config.group_max_replies or self.config.max_replies)
        return self.config.reply_window_seconds, self.config.max_replies

    async def capabilities(self, target):
        full = self.config.full_mode_enabled and (self.config.full_mode_verified or self.full_event_observed)
        rich = self.config.rich_messages_enabled
        qq_media = rich and target.surface in {Surface.PRIVATE, Surface.GROUP}
        return AdapterCapabilities(
            can_reply=True, can_initiate_message=self.config.proactive_verified,
            can_observe_unaddressed_messages=full or target.surface == Surface.PRIVATE,
            supports_reply_reference=True, supports_mention=True,
            supports_images=rich, supports_files=qq_media, supports_audio=qq_media, supports_video=qq_media,
            supports_markdown=rich and self.config.markdown_enabled, supports_keyboard=rich,
            supports_typing=target.surface == Surface.PRIVATE and self.config.typing_enabled,
            supports_streaming=target.surface == Surface.PRIVATE and self.config.streaming_enabled,
            observation_mode=ObservationMode.FULL if full or target.surface == Surface.PRIVATE
            else ObservationMode.MENTIONS_ONLY,
            delivery_constraints=(f"reply_window={self.reply_policy(target.surface)[0]}s",
                f"max_replies={self.reply_policy(target.surface)[1]}", "actual_permissions_require_smoke_test"),
        )

    async def validate_delivery(self, envelope):
        target = envelope.target
        if (
            target.adapter_instance_id != self.config.instance_id
            or target.bot_identity_id != self.config.bot_id
        ):
            return DeliveryCheck(allowed=False, reason="IDENTITY_MISMATCH")
        if not self.permissions.check(
            PermissionSubject.from_refs(target, envelope.requester), "notification.send", "conversation:" + target.key
        ).allowed:
            return DeliveryCheck(allowed=False, reason="TARGET_DENIED")
        try:
            message_body(envelope, markdown_enabled=self.config.markdown_enabled,
                         rich_enabled=self.config.rich_messages_enabled)
        except ValueError as exc:
            return DeliveryCheck(allowed=False, reason=str(exc))
        reason = self.relationships.denied(target)
        if reason:
            return DeliveryCheck(allowed=False, reason=reason)
        reply = envelope.reply_context
        if (
            reply
            and reply.expires_at
            and reply.expires_at > self.clock.now()
            and reply.msg_seq
            and reply.msg_seq <= reply.max_replies
        ):
            return DeliveryCheck(allowed=True, reason="VALID_REPLY")
        if self.config.proactive_verified and envelope.allow_proactive:
            reason = self.relationships.denied(target, proactive=True)
            if reason:
                return DeliveryCheck(allowed=False, reason=reason)
            return DeliveryCheck(allowed=True, reason="AUTHORIZED_PROACTIVE")
        return DeliveryCheck(allowed=False, reason="REPLY_EXPIRED_OR_EXHAUSTED", wait_for_trigger=True)

    async def access_token(self):
        async with self.token_lock:
            if self.token and self.token_expires > self.clock.now() + timedelta(seconds=60):
                return self.token
            response = await self.client.post(
                self.config.token_url,
                json={"appId": self.config.app_id, "clientSecret": self.config.secret()},
            )
            response.raise_for_status()
            data = response.json()
            token, ttl = data["access_token"], int(data["expires_in"])
            if not isinstance(token, str) or not token or ttl <= 0:
                raise ValueError("invalid token response")
            self.token, self.token_expires = token, self.clock.now() + timedelta(seconds=ttl)
            return token

    async def send(self, envelope, *, authorize=None):
        check = await self.validate_delivery(envelope)
        if not check.allowed:
            return SendReceipt(
                state="WAITING_FOR_TRIGGER" if check.wait_for_trigger else "FAILED", error_code=check.reason
            )
        try:
            token = await self.access_token()
        except (httpx.HTTPError, ValueError, KeyError):
            return SendReceipt(state="RETRYABLE", error_code="TOKEN_UNAVAILABLE")
        def authorized():
            if authorize is None:
                return True
            try:
                return bool(authorize())
            except Exception:  # noqa: BLE001 - Policy resolution failures must fail closed.
                return False

        if not authorized():
            return SendReceipt(state="FAILED", error_code="AUTHORIZATION_CHANGED")
        remote = quote(envelope.target.remote_conversation_id, safe="")
        if envelope.target.surface == Surface.GROUP:
            path = f"/v2/groups/{remote}/messages"
        elif envelope.target.surface == Surface.PRIVATE:
            path = f"/v2/users/{remote}/messages"
        elif envelope.target.surface == Surface.GUILD_PRIVATE:
            path = f"/dms/{remote}/messages"
        else:
            path = f"/channels/{remote}/messages"
        body = message_body(envelope, markdown_enabled=self.config.markdown_enabled,
                            rich_enabled=self.config.rich_messages_enabled)
        if (envelope.official_content and envelope.official_content.media
                and envelope.official_content.media.local_ref):
            from .local_media import LocalMediaStore
            from .local_upload import LocalUploadError, upload_local
            media = envelope.official_content.media
            if not envelope.parent_task_id or not envelope.requester:
                return SendReceipt(state="FAILED", error_code="LOCAL_MEDIA_SCOPE_REQUIRED")
            content = None
            try:
                opening = asyncio.create_task(asyncio.to_thread(LocalMediaStore(self.inbox.db, self.clock).open,
                    media.local_ref, task_id=envelope.parent_task_id, conversation_id=envelope.target.key,
                    principal_key=envelope.requester.key))
                try:
                    metadata, content = await asyncio.shield(opening)
                except asyncio.CancelledError:
                    def close_late(done):
                        if not done.cancelled() and done.exception() is None:
                            done.result()[1].close()
                    opening.add_done_callback(close_late)
                    raise
                if metadata["media_type"] != media.type or (media.filename and metadata["filename"] != media.filename):
                    raise ValueError("media metadata mismatch")
                info = await upload_local(self, envelope, content, metadata["filename"], media.type,
                                          metadata["mime_type"], authorized)
                body["media"] = {"file_info": info}
            except LocalUploadError as exc:
                return SendReceipt(state="RETRYABLE" if exc.retryable else "FAILED", error_code=exc.code)
            except (ValueError, PermissionError, OSError):
                return SendReceipt(state="FAILED", error_code="LOCAL_MEDIA_UNAVAILABLE")
            finally:
                if content is not None:
                    content.close()
        elif (envelope.official_content and envelope.official_content.media
                and envelope.target.surface in {Surface.PRIVATE, Surface.GROUP}):
            media = envelope.official_content.media
            try:
                upload = await self.client.post(
                    self.config.api_base.rstrip("/") + path.removesuffix("messages") + "files",
                    headers={"Authorization": "QQBot " + token, "X-Union-Appid": str(self.config.app_id)},
                    json={"file_type": {"image": 1, "video": 2, "audio": 3, "file": 4}[media.type],
                          "url": media.url, "srv_send_msg": False,
                          **({"file_name": media.filename} if media.filename else {})},
                )
                if upload.status_code == 401:
                    self.token = None
                if upload.status_code in {401, 429} or upload.status_code >= 500:
                    return SendReceipt(state="RETRYABLE", error_code="MEDIA_UPLOAD_UNAVAILABLE")
                if not upload.is_success:
                    return SendReceipt(state="FAILED", error_code="MEDIA_UPLOAD_REJECTED")
                info = upload.json()["file_info"]
                if not isinstance(info, str) or not info:
                    raise ValueError("invalid file_info")
                body["media"] = {"file_info": info}
            except (httpx.HTTPError, ValueError, KeyError):
                # srv_send_msg=False means upload cannot itself deliver a message.
                return SendReceipt(state="RETRYABLE", error_code="MEDIA_UPLOAD_UNAVAILABLE")
        if envelope.official_content and envelope.official_content.media and envelope.official_content.media.local_ref:
            try:
                token = await self.access_token()
            except (httpx.HTTPError, ValueError, KeyError):
                return SendReceipt(state="RETRYABLE", error_code="TOKEN_UNAVAILABLE")
        # Token refresh and file upload may outlive the reply window or policy.
        # Nothing that awaits is permitted between this recheck and message POST.
        check = await self.validate_delivery(envelope)
        if not check.allowed:
            return SendReceipt(state="WAITING_FOR_TRIGGER" if check.wait_for_trigger else "FAILED",
                               error_code=check.reason)
        if not authorized():
            return SendReceipt(state="FAILED", error_code="AUTHORIZATION_CHANGED")
        if check.reason == "VALID_REPLY":
            if envelope.reply_context.source == "interaction":
                body["event_id"] = envelope.reply_context.event_id
            else:
                body["msg_id"] = envelope.reply_context.platform_message_id
            if envelope.target.surface not in {Surface.GUILD_CHANNEL, Surface.GUILD_PRIVATE}:
                body["msg_seq"] = envelope.reply_context.msg_seq
        try:
            response = await self.client.post(
                self.config.api_base.rstrip("/") + path,
                headers={"Authorization": "QQBot " + token, "X-Union-Appid": str(self.config.app_id)},
                json=body,
            )
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout):
            return SendReceipt(state="RETRYABLE", error_code="NOT_CONNECTED")
        except httpx.HTTPError:
            return SendReceipt(state="UNKNOWN", error_code="RECEIPT_LOST")
        if response.status_code == 401:
            self.token = None
            return SendReceipt(state="RETRYABLE", error_code="TOKEN_REJECTED")
        if response.status_code == 429:
            return SendReceipt(state="RETRYABLE", error_code="RATE_LIMITED")
        if response.status_code >= 500:
            return SendReceipt(state="UNKNOWN", error_code="SERVER_ERROR")
        if not response.is_success:
            # Preserve bounded platform business codes for operator reconciliation;
            # do not guess that an application rejection is safe to retry.
            try:
                error = response.json().get("code")
            except (ValueError, AttributeError):
                error = None
            code = str(error) if isinstance(error, (int, str)) else ""
            return SendReceipt(state="FAILED", error_code=("QQ_" + code[:64]) if code else
                               f"HTTP_{response.status_code}")
        try:
            message_id = response.json()["id"]
            return SendReceipt(state="SENT", platform_message_id=str(message_id))
        except (ValueError, KeyError):
            return SendReceipt(state="UNKNOWN", error_code="MISSING_MESSAGE_ID")
