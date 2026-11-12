"""C2C-only QQ streaming protocol; no automatic replay of uncertain sends.

Protocol: https://bot.q.qq.com/wiki/develop/api-v2/autogen/
api/v2_users_user_openid_stream_messages.post.html (checked 2026-11-12).

The caller owns reply-sequence allocation, durable delivery intent and authorization.
A stream consumes ONE reply sequence; typing consumes a separate sequence. These
helpers must not be called with a sequence already reserved for another delivery.
Group callers should use the normal outbox, not emulate streaming with repeated sends.
"""

import asyncio
import time
from urllib.parse import quote

import httpx

from qqbot.domain import SendReceipt, Surface
from qqbot.domain.tasks import validate_user_facing_text


async def _post(adapter, envelope, path, body, *, require_id=True, authorize=None):
    if envelope.target.surface != Surface.PRIVATE:
        return SendReceipt(state="FAILED", error_code="C2C_ONLY"), None
    if envelope.expires_at <= adapter.clock.now():
        return SendReceipt(state="FAILED", error_code="DELIVERY_EXPIRED"), None
    check = await adapter.validate_delivery(envelope)
    # Do not use is_wakeup to bypass reply expiry or infer a proactive grant.
    if not check.allowed or check.reason != "VALID_REPLY":
        return SendReceipt(state="WAITING_FOR_TRIGGER" if check.wait_for_trigger else "FAILED",
                           error_code=check.reason if not check.allowed else "STREAM_REQUIRES_REPLY"), None
    try:
        token = await adapter.access_token()
    except (httpx.HTTPError, ValueError, KeyError):
        return SendReceipt(state="RETRYABLE", error_code="TOKEN_UNAVAILABLE"), None
    # Token refresh can outlast a reply window. Revalidate immediately before I/O.
    if envelope.expires_at <= adapter.clock.now():
        return SendReceipt(state="FAILED", error_code="DELIVERY_EXPIRED"), None
    check = await adapter.validate_delivery(envelope)
    if not check.allowed or check.reason != "VALID_REPLY":
        return SendReceipt(state="FAILED", error_code="STREAM_REQUIRES_REPLY"), None
    # Recheck after token refresh (and any caller throttle), immediately before I/O.
    if authorize is not None and not authorize():
        return SendReceipt(state="FAILED", error_code="AUTHORIZATION_CHANGED"), None
    try:
        response = await adapter.client.post(
            adapter.config.api_base.rstrip("/") + path,
            headers={"Authorization": "QQBot " + token, "X-Union-Appid": str(adapter.config.app_id)},
            json=body,
        )
    except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout):
        return SendReceipt(state="RETRYABLE", error_code="NOT_CONNECTED"), None
    except httpx.HTTPError:
        return SendReceipt(state="UNKNOWN", error_code="RECEIPT_LOST"), None
    if response.status_code == 401:
        adapter.token = None
        return SendReceipt(state="RETRYABLE", error_code="TOKEN_REJECTED"), None
    if response.status_code == 429:
        return SendReceipt(state="RETRYABLE", error_code="RATE_LIMITED"), None
    if response.status_code >= 500:
        return SendReceipt(state="UNKNOWN", error_code="SERVER_ERROR"), None
    if not response.is_success:
        return SendReceipt(state="FAILED", error_code=f"HTTP_{response.status_code}"), None
    try:
        data = response.json()
        if not isinstance(data, dict):
            raise TypeError("invalid response")
        message_id = data.get("id")
        if require_id and (not isinstance(message_id, str) or not message_id):
            raise ValueError("missing message ID")
    except (ValueError, TypeError):
        return SendReceipt(state="UNKNOWN", error_code="INVALID_RECEIPT"), None
    return SendReceipt(state="SENT", platform_message_id=message_id), data


async def send_typing(adapter, envelope, *, seconds=30, authorize=None):
    """Send the C2C input indicator using a separately reserved reply sequence."""
    if isinstance(seconds, bool) or not isinstance(seconds, int) or not 1 <= seconds <= 60:
        raise ValueError("typing duration must be 1..60 seconds")
    reply = envelope.reply_context
    body = {"msg_type": 6, "input_notify": {"input_type": 1, "input_second": seconds}}
    if reply:
        body.update(msg_id=reply.platform_message_id, msg_seq=reply.msg_seq)
    remote = quote(envelope.target.remote_conversation_id, safe="")
    receipt, _ = await _post(adapter, envelope, f"/v2/users/{remote}/messages", body, require_id=False, authorize=authorize)
    return receipt


class OfficialStream:
    """One serialized append-only logical stream with a bounded pending buffer.

    `append` coalesces rapid deltas. The caller periodically calls `flush` or
    calls `finish`; there is no unowned background task. On UNKNOWN/FAILED the
    stream is terminal. The caller must NOT fall back to sending the full answer
    after UNKNOWN, because the previous request may already be visible.
    """

    def __init__(self, adapter, envelope, *, content_type="markdown", min_interval=0.5,
                 max_chars=10000, monotonic=time.monotonic, sleep=asyncio.sleep, authorize=None):
        if envelope.target.surface != Surface.PRIVATE:
            raise ValueError("native streaming is C2C only")
        if content_type not in {"text", "markdown"}:
            raise ValueError("unsupported stream content type")
        if not 0.05 <= min_interval <= 60 or not 1 <= max_chars <= 10000:
            raise ValueError("invalid stream resource limits")
        self.adapter, self.envelope = adapter, envelope
        self.authorize = authorize
        self.content_type, self.min_interval, self.max_chars = content_type, min_interval, max_chars
        self.monotonic, self.sleep = monotonic, sleep
        self.lock = asyncio.Lock()
        self.text = self.sent_text = ""
        self.index = 0
        self.stream_id = None
        self.state = "NEW"
        self.remain_msg_len = None
        self.last_attempt = None
        self.last_receipt = None

    def _open(self):
        if self.state in {"FINISHED", "CANCELLED", "UNKNOWN", "FAILED", "WAITING_FOR_TRIGGER"}:
            raise RuntimeError("stream is terminal: " + self.state)

    async def append(self, delta):
        async with self.lock:
            self._open()
            if not isinstance(delta, str):
                raise TypeError("stream delta must be text")
            candidate = self.text + delta
            if len(candidate) > self.max_chars:
                raise ValueError("stream character limit exceeded")
            if self.remain_msg_len is not None and len(candidate) - len(self.sent_text) > self.remain_msg_len:
                raise ValueError("platform stream capacity exceeded")
            validate_user_facing_text(candidate)
            self.text = candidate
            return await self._flush()

    async def flush(self):
        async with self.lock:
            self._open()
            return await self._flush()

    async def finish(self):
        async with self.lock:
            if self.state == "FINISHED":
                return self.last_receipt
            self._open()
            if not self.text:
                self.state = "FINISHED"
                return None
            return await self._flush(final=True)

    async def cancel(self):
        async with self.lock:
            if self.state in {"FINISHED", "CANCELLED", "UNKNOWN", "FAILED", "WAITING_FOR_TRIGGER"}:
                return self.last_receipt
            # Do not leak buffered text after cancellation; finalize only a known prefix.
            self.text = self.sent_text
            if self.stream_id:
                receipt = await self._flush(final=True)
                if receipt.state != "SENT":
                    return receipt
            self.state = "CANCELLED"
            return self.last_receipt

    async def _flush(self, *, final=False):
        if not final and (not self.text or self.text == self.sent_text):
            return None
        if self.last_attempt is not None:
            delay = self.min_interval - (self.monotonic() - self.last_attempt)
            if delay > 0:
                if not final:
                    return None
                await self.sleep(delay)
        reply = self.envelope.reply_context
        body = {"input_mode": "replace", "input_state": 10 if final else 1,
                "index": self.index, "content_type": self.content_type, "content_raw": self.text}
        if reply:
            body.update(msg_id=reply.platform_message_id, msg_seq=reply.msg_seq)
        if self.stream_id:
            body["stream_msg_id"] = self.stream_id
        remote = quote(self.envelope.target.remote_conversation_id, safe="")
        self.last_attempt = self.monotonic()
        try:
            receipt, data = await _post(self.adapter, self.envelope,
                                       f"/v2/users/{remote}/stream_messages", body, authorize=self.authorize)
        except asyncio.CancelledError:
            self.state = "UNKNOWN"
            self.last_receipt = SendReceipt(state="UNKNOWN", error_code="SEND_CANCELLED")
            raise
        self.last_receipt = receipt
        if receipt.state != "SENT":
            self.state = receipt.state
            return receipt
        if self.stream_id is not None and receipt.platform_message_id != self.stream_id:
            self.state = "UNKNOWN"
            self.last_receipt = SendReceipt(state="UNKNOWN", error_code="STREAM_ID_CHANGED")
            return self.last_receipt
        self.stream_id = receipt.platform_message_id
        self.sent_text = self.text
        self.index += 1
        remaining = data.get("remain_msg_len")
        if isinstance(remaining, int) and not isinstance(remaining, bool) and remaining >= 0:
            self.remain_msg_len = remaining
        self.state = "FINISHED" if final else "OPEN"
        return receipt
