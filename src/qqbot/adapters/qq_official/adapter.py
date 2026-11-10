import asyncio
import base64
import hashlib
import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import PurePosixPath
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
    MediaSegment,
    ObservationMode,
    PrincipalRef,
    ReplyContext,
    SendReceipt,
    Surface,
    TextSegment,
    stable_id,
)
from qqbot.permissions import PermissionService, PermissionSubject

# https://bot.q.qq.com/wiki/develop/api-v2/autogen/api/v2_users_user_openid_files.post.html
DIRECT_UPLOAD_MAX_BYTES = 5_000_000


class _CosUploadLogFilter(logging.Filter):
    def filter(self, record):
        if isinstance(record.args, tuple):
            record.args = tuple(
                value.copy_with(query=None) if isinstance(value, httpx.URL)
                and value.host.endswith(".myqcloud.com") else value
                for value in record.args
            )
        return True


# Install once at module import, including when several official clients share HTTPX.
logging.getLogger("httpx").addFilter(_CosUploadLogFilter())


class OfficialAdapter:
    def __init__(self, config, deployment_id, inbox, clock, client=None, *, permissions=None):
        self.config, self.deployment_id, self.inbox, self.clock = config, deployment_id, inbox, clock
        self.permissions = permissions or PermissionService()
        self.client = client or httpx.AsyncClient(timeout=15, follow_redirects=False)
        self.token = None
        self.token_expires = clock.now()
        self.token_lock = asyncio.Lock()
        self.full_event_observed = False

    def key(self):
        secret = self.config.secret().encode()
        seed = (secret * ((32 + len(secret) - 1) // len(secret)))[:32]
        return Ed25519PrivateKey.from_private_bytes(seed)

    async def start(self):
        self.key()

    async def stop(self):
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
        if payload.get("op") == 0:
            event = self.normalize(payload)
            if event:
                self.inbox.accept(event)
        return {"op": 12}

    def normalize(self, payload):
        kind, data = payload.get("t"), payload["d"]
        if kind not in {
            "GROUP_AT_MESSAGE_CREATE",
            "GROUP_MESSAGE_CREATE",
            "C2C_MESSAGE_CREATE",
            "AT_MESSAGE_CREATE",
            "MESSAGE_CREATE",
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
        for attachment in data.get("attachments", []):
            chain.append(
                MediaSegment(
                    type="image" if str(attachment.get("content_type", "")).startswith("image/") else "file",
                    attachment_ref="unresolved:" + stable_id(json.dumps(attachment, sort_keys=True)),
                    mime_type=attachment.get("content_type"),
                )
            )
        reference = data.get("message_reference") or {}
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
            or any(str(m.get("id")) == self.config.bot_id for m in data.get("mentions", [])),
            sender_is_bot=bool(author.get("bot", False)) or user == self.config.bot_id,
            observation_mode=ObservationMode.FULL
            if full or surface == Surface.PRIVATE
            else ObservationMode.MENTIONS_ONLY,
            reply_context=ReplyContext(
                platform_message_id=msg_id,
                event_id=platform_id,
                expires_at=stamp + timedelta(seconds=self.config.reply_window_seconds),
                max_replies=min(self.config.max_replies, 4 if surface == Surface.PRIVATE else 5),
            ),
            trace_id=str(uuid4()),
        )

    async def capabilities(self, target):
        full = self.config.full_mode_enabled and (self.config.full_mode_verified or self.full_event_observed)
        return AdapterCapabilities(
            can_reply=True,
            can_initiate_message=self.config.proactive_verified,
            can_observe_unaddressed_messages=full,
            supports_reply_reference=target.surface == Surface.GUILD_CHANNEL,
            supports_images=True,
            supports_files=target.surface != Surface.GUILD_CHANNEL,
            observation_mode=ObservationMode.FULL if full else ObservationMode.MENTIONS_ONLY,
            delivery_constraints=(
                f"reply_window={self.config.reply_window_seconds}s",
                f"max_replies={self.config.max_replies}",
                "actual_permissions_require_smoke_test",
            ),
        )

    async def validate_delivery(self, envelope):
        target = envelope.target
        if (
            target.adapter_instance_id != self.config.instance_id
            or target.bot_identity_id != self.config.bot_id
        ):
            return DeliveryCheck(allowed=False, reason="IDENTITY_MISMATCH")
        if not self.permissions.check(
            PermissionSubject.from_refs(target), "notification.send", "conversation:" + target.key
        ).allowed:
            return DeliveryCheck(allowed=False, reason="TARGET_DENIED")
        if envelope.artifact and envelope.expires_at <= self.clock.now():
            return DeliveryCheck(allowed=False, reason="EXPIRED")
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

    async def _upload_post(self, path, token, body):
        response = await self.client.post(
            self.config.api_base.rstrip("/") + path,
            headers={"Authorization": "QQBot " + token, "X-Union-Appid": str(self.config.app_id)},
            json=body, follow_redirects=False,
        )
        response.raise_for_status()
        result = response.json()
        if not isinstance(result, dict):
            raise ValueError("UPLOAD_INVALID_RESPONSE")  # noqa: TRY004 - Media protocol boundary.
        code = result.get("code", 0)
        if code != 0:
            if type(code) is int:
                raise ValueError("UPLOAD_API_" + str(code))
            raise ValueError("UPLOAD_INVALID_RESPONSE")
        return result

    async def _upload_media(self, envelope, path, token):
        artifact, content = envelope.artifact, envelope.artifact_data
        body = {
            "file_type": 1 if artifact.mime_type.startswith("image/") else 4,
            "file_name": PurePosixPath(artifact.relative_path).name,
            "srv_send_msg": False,
        }
        if len(content) <= DIRECT_UPLOAD_MAX_BYTES:
            body["file_data"] = base64.b64encode(content).decode("ascii")
        else:
            prepared = await self._upload_post(path + "/upload_prepare", token, {
                "file_type": body["file_type"], "file_name": body["file_name"],
                "file_size": str(len(content)), "md5": hashlib.md5(content).hexdigest(),
                "sha1": hashlib.sha1(content).hexdigest(),
                "md5_10m": hashlib.md5(content[:10002432]).hexdigest(),
            })
            upload_id = prepared["upload_id"]
            block_size = int(prepared["block_size"])
            parts = prepared["parts"]
            if (not isinstance(upload_id, str) or not upload_id or len(upload_id) > 4096
                    or not 0 < block_size <= 200 * 1024 * 1024
                    or not isinstance(parts, list) or not 1 <= len(parts) <= 128):
                raise ValueError("UPLOAD_INVALID_RESPONSE")
            expected = list(range((len(content) + block_size - 1) // block_size))
            if (any(not isinstance(part, dict) or type(part.get("index")) is not int for part in parts)
                    or sorted(part["index"] for part in parts) != expected):
                raise ValueError("UPLOAD_INVALID_RESPONSE")
            for part in sorted(parts, key=lambda item: item["index"]):
                offset = part["index"] * block_size
                chunk = content[offset:offset + block_size]
                if int(part["block_size"]) not in {block_size, len(chunk)}:
                    raise ValueError("UPLOAD_INVALID_RESPONSE")
                url = httpx.URL(part["presigned_url"])
                if (url.scheme != "https" or url.userinfo or url.port not in {None, 443}
                        or not url.host.endswith(".myqcloud.com") or len(str(url)) > 8192):
                    raise ValueError("UPLOAD_INVALID_RESPONSE")
                # Presigned COS PUTs carry no QQ headers, client authentication or cookies.
                response = await self.client.send(
                    httpx.Request("PUT", url, content=chunk,
                                  headers={"Content-Type": "application/octet-stream"}),
                    auth=None, follow_redirects=False,
                )
                response.raise_for_status()
                await self._upload_post(path + "/upload_part_finish", token, {
                    "upload_id": upload_id, "part_index": part["index"],
                    "block_size": str(len(chunk)), "md5": hashlib.md5(chunk).hexdigest(),
                })
            body["upload_id"] = upload_id
        uploaded = await self._upload_post(path + "/files", token, body)
        if (not isinstance(uploaded.get("file_info"), str) or not uploaded["file_info"]
                or len(uploaded["file_info"]) > 65536
                or type(uploaded.get("ttl")) is not int or uploaded["ttl"] < 0):
            raise ValueError("UPLOAD_INVALID_RESPONSE")
        return uploaded

    async def send_artifact(self, envelope, *, pre_send=None):
        # Optional Outbox extension keeps send(envelope) compatible with other adapters.
        return await self.send(envelope, pre_send=pre_send)

    async def send(self, envelope, *, pre_send=None):
        artifact = envelope.artifact
        guild = envelope.target.surface == Surface.GUILD_CHANNEL
        if artifact:
            if guild and not artifact.mime_type.startswith("image/"):
                return SendReceipt(state="FAILED", error_code="UNSUPPORTED_ARTIFACT")
            content = envelope.artifact_data
            if (content is None or len(content) != artifact.size
                    or len(content) > 20 * 1024 * 1024
                    or hashlib.sha256(content).hexdigest() != artifact.sha256):
                return SendReceipt(state="FAILED", error_code="ARTIFACT_UNAVAILABLE")
        check = await self.validate_delivery(envelope)
        if not check.allowed:
            return SendReceipt(
                state="WAITING_FOR_TRIGGER" if check.wait_for_trigger else "FAILED", error_code=check.reason
            )
        if artifact and pre_send and not pre_send(envelope):
            return SendReceipt(state="FAILED", error_code="STALE_CONTEXT")
        try:
            token = await self.access_token()
        except (httpx.HTTPError, ValueError, KeyError):
            return SendReceipt(state="RETRYABLE", error_code="TOKEN_UNAVAILABLE")
        remote = quote(envelope.target.remote_conversation_id, safe="")
        if envelope.target.surface == Surface.GROUP:
            path = f"/v2/groups/{remote}"
        elif envelope.target.surface == Surface.PRIVATE:
            path = f"/v2/users/{remote}"
        else:
            path = f"/channels/{remote}"
        body = {"content": envelope.text}
        media_expires = None
        if not guild:
            body["msg_type"] = 0
        if artifact and not guild:
            try:
                uploaded = await self._upload_media(envelope, path, token)
            except httpx.HTTPStatusError as error:
                status = error.response.status_code
                if status == 401:
                    self.token = None
                return SendReceipt(state="RETRYABLE" if status in {401, 429} or status >= 500 else "FAILED",
                                   error_code=f"UPLOAD_HTTP_{status}")
            except httpx.HTTPError:
                # Uploads use srv_send_msg=false, so a lost upload receipt cannot duplicate a message.
                return SendReceipt(state="RETRYABLE", error_code="UPLOAD_UNAVAILABLE")
            except (ValueError, KeyError, TypeError, httpx.InvalidURL) as error:
                reason = str(error)
                transient = reason in {"UPLOAD_API_850026", "UPLOAD_API_850027", "UPLOAD_API_40093001"}
                return SendReceipt(state="RETRYABLE" if transient else "FAILED",
                                   error_code=reason if reason.startswith("UPLOAD_API_") else "UPLOAD_INVALID_RESPONSE")
            body.update(msg_type=7, media={"file_info": uploaded["file_info"]})
            if uploaded["ttl"]:
                media_expires = self.clock.now() + timedelta(seconds=uploaded["ttl"])
        if artifact:
            if pre_send and not pre_send(envelope):
                return SendReceipt(state="FAILED", error_code="STALE_CONTEXT")
            check = await self.validate_delivery(envelope)
            if not check.allowed:
                return SendReceipt(state="WAITING_FOR_TRIGGER" if check.wait_for_trigger else "FAILED",
                                   error_code=check.reason)
            if media_expires and media_expires <= self.clock.now():
                return SendReceipt(state="RETRYABLE", error_code="UPLOAD_EXPIRED")
        if check.reason == "VALID_REPLY":
            body["msg_id"] = envelope.reply_context.platform_message_id
            if not guild:
                body["msg_seq"] = envelope.reply_context.msg_seq
        headers = {"Authorization": "QQBot " + token, "X-Union-Appid": str(self.config.app_id)}
        try:
            if artifact and guild:
                response = await self.client.post(
                    self.config.api_base.rstrip("/") + path + "/messages", headers=headers, data=body,
                    follow_redirects=False,
                    files={"file_image": (PurePosixPath(artifact.relative_path).name,
                                           envelope.artifact_data, artifact.mime_type)},
                )
            else:
                response = await self.client.post(
                    self.config.api_base.rstrip("/") + path + "/messages", headers=headers, json=body,
                    follow_redirects=False,
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
            return SendReceipt(state="FAILED", error_code=f"HTTP_{response.status_code}")
        try:
            message_id = response.json()["id"]
            if not isinstance(message_id, str) or not message_id.strip():
                raise ValueError("invalid message id")
            return SendReceipt(state="SENT", platform_message_id=message_id)
        except (ValueError, KeyError, TypeError):
            return SendReceipt(state="UNKNOWN", error_code="MISSING_MESSAGE_ID")
