"""Bounded QR onboarding sessions; credentials never appear in public state.

Protocol reference: Tencent qqbot-agent-sdk onboard.py (MIT). No SDK code is vendored.
"""
import asyncio
import base64
import os
import re
import time
from dataclasses import dataclass, field
from urllib.parse import urlencode
from uuid import uuid4

import httpx
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


@dataclass(repr=False)
class Session:
    id: str
    owner: str
    instance_id: str
    created: float
    expires: float
    replace_app_id: str | None = None
    state: str = "creating"
    task_id: str = ""
    key: bytes = b""
    app_id: str | None = None
    secret: str | None = None
    error: str | None = None
    task: asyncio.Task | None = field(default=None, repr=False)
    automatic: bool = False
    scanner_openid: str | None = None
    provisioned: bool = False
    result: dict = field(default_factory=dict)

    def public(self):
        url = "https://q.qq.com/qqbot/openclaw/connect.html?" + urlencode(
            {"task_id": self.task_id, "_wv": "2", "source": "elephant-bot"})
        return {"session_id": self.id, "instance_id": self.instance_id, "state": self.state,
                "app_id": self.app_id, "error": self.error, "replace_app_id": self.replace_app_id,
                "connect_url": url if self.state == "waiting" else None,
                "expires_in": max(0, int(self.expires - time.monotonic())),
                "restart_required": self.state == "saved" and not self.automatic,
                "automatic": self.automatic, "provisioned": self.provisioned,
                **{key: self.result[key] for key in ("saved_version", "saved", "activated", "connection", "owner_bound")
                   if key in self.result}}

    def clear(self):
        self.key, self.secret, self.task_id, self.scanner_openid = b"", None, "", None


class Onboarding:
    def __init__(self, *, client=None, timeout=300, interval=2, now=time.monotonic, on_ready=None):
        self.client = client or httpx.AsyncClient(timeout=10, follow_redirects=False)
        self.owns_client = client is None
        self.timeout, self.interval, self.now = timeout, interval, now
        self.sessions = {}
        self.lock = asyncio.Lock()
        self.on_ready = on_ready

    async def _post(self, path, payload):
        response = await self.client.post("https://q.qq.com/lite/" + path, json=payload,
                                          timeout=10, follow_redirects=False, headers={
                                              "Accept": "application/json",
                                              "Content-Type": "application/json",
                                              "User-Agent": "ElephantBot/0.1 QQOfficial-Onboarding",
                                          })
        if response.status_code != 200 or len(response.content) > 65536:
            raise ValueError("official onboarding service unavailable")
        result = response.json()
        if not isinstance(result, dict) or result.get("retcode") != 0 or not isinstance(result.get("data"), dict):
            raise ValueError("official onboarding request rejected")
        return result["data"]

    def _expire(self, session):
        if not session.provisioned and self.now() >= session.expires and session.state not in {"saved", "cancelled", "expired", "failed"}:
            session.state = "timed_out"
            session.clear()
            if session.task and session.task is not asyncio.current_task():
                session.task.cancel()

    def get(self, session_id, owner):
        session = self.sessions.get(session_id)
        if session is None or session.owner != owner:
            raise LookupError("onboarding session unavailable")
        self._expire(session)
        return session

    async def create(self, owner, instance_id, *, automatic=False):
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", instance_id):
            raise ValueError("invalid adapter instance ID")
        async with self.lock:
            for key, old in list(self.sessions.items()):
                self._expire(old)
                if self.now() > old.expires + 300:
                    del self.sessions[key]
            active = [s for s in self.sessions.values() if s.state in {"creating", "waiting", "ready", "validating", "connecting"}]
            if len(self.sessions) >= 128 or len(active) >= 8:
                raise ValueError("too many onboarding sessions")
            if any(s.instance_id == instance_id for s in active):
                raise ValueError("adapter already has an onboarding session")
            session = Session(uuid4().hex, owner, instance_id, self.now(), self.now() + self.timeout)
            session.automatic = automatic
            session.key = os.urandom(32)
            self.sessions[session.id] = session
            session.task = asyncio.create_task(self._run(session))
            return session

    async def _run(self, session):
        try:
            data = await self._post("create_bind_task", {"key": base64.b64encode(session.key).decode()})
            if session.state == "cancelled":
                return
            task_id = data.get("task_id")
            if not isinstance(task_id, str) or not 1 <= len(task_id) <= 1024:
                raise ValueError("invalid task")
            session.task_id, session.state = task_id, "waiting"
            while session.state == "waiting":
                self._expire(session)
                if session.state != "waiting":
                    break
                data = await self._post("poll_bind_result", {"task_id": session.task_id})
                self._expire(session)
                if session.state != "waiting":
                    break
                status = data.get("status")
                if status == 3:
                    session.state = "expired"
                    break
                if status == 2:
                    app_id = str(data.get("bot_appid", ""))
                    if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", app_id):
                        raise ValueError("invalid app ID")
                    raw = base64.b64decode(data.get("bot_encrypt_secret", ""), validate=True)
                    if not 29 <= len(raw) <= 8192:
                        raise ValueError("invalid credential")
                    secret = AESGCM(session.key).decrypt(raw[:12], raw[12:], None).decode()
                    if not 1 <= len(secret) <= 4096 or any(ord(c) < 32 for c in secret):
                        raise ValueError("invalid credential")
                    # Owner binding requires the administrator's explicit automatic setup request.
                    session.app_id, session.secret, session.state = app_id, secret, "ready"
                    session.key, session.task_id = b"", ""
                    if session.automatic:
                        scanner = data.get("user_openid")
                        if not isinstance(scanner, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,256}", scanner):
                            raise ValueError("official scanner identity unavailable")
                        session.scanner_openid = scanner
                        if self.on_ready is None:
                            raise ValueError("automatic provisioning unavailable")
                        await self.on_ready(session)
                        return
                    await asyncio.sleep(max(0, session.expires - self.now()))
                    self._expire(session)
                    return
                if status not in (0, 1):
                    raise ValueError("invalid status")
                await asyncio.sleep(self.interval)
        except asyncio.CancelledError:
            if session.state not in {"cancelled", "timed_out", "saved", "expired", "failed"}:
                session.state = "cancelled"
        except (httpx.HTTPError, ValueError, TypeError, KeyError, InvalidTag):
            session.state, session.error = "failed", "onboarding_failed"
        finally:
            if session.state != "ready":
                session.clear()

    async def cancel(self, session_id, owner):
        async with self.lock:
            session = self.get(session_id, owner)
            if session.provisioned or session.state == "saved":
                raise ValueError("saved credentials cannot be cancelled; remove adapter separately")
            session.state = "cancelled"
            session.clear()
            if session.task:
                session.task.cancel()
            return session

    async def close(self):
        for session in self.sessions.values():
            session.clear()
            if session.task:
                session.task.cancel()
        await asyncio.gather(*(s.task for s in self.sessions.values() if s.task), return_exceptions=True)
        if self.owns_client:
            await self.client.aclose()
