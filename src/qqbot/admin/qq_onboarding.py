"""Administrator-only, two-phase QQ onboarding. Never returns credentials."""
import asyncio
import hashlib
import io
import json
from pathlib import Path

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, Response
from pydantic import BaseModel, ConfigDict, Field

from qqbot.adapters.qq_official.credential_store import master_key, remove_credential, save_credential
from qqbot.adapters.qq_official.onboarding import Onboarding
from qqbot.config import AdapterConfig, Settings
from qqbot.configuration import version
from qqbot.permissions import PermissionSubject


class CreateSession(BaseModel):
    model_config = ConfigDict(extra="forbid")
    instance_id: str = Field(pattern=r"^[A-Za-z0-9_-]{1,64}$")
    replace_app_id: str | None = Field(default=None, min_length=1, max_length=128)


class ApplySession(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expected_version: str
    expected_app_id: str = Field(min_length=1, max_length=128)


def install_qq_onboarding(app, core, authenticate, *, client=None):
    service = Onboarding(client=client)
    app.state.qq_onboarding = service
    subject = PermissionSubject(adapter_instance_id="admin_api", identity_namespace="admin_api", user_id="owner")

    def authorize(request: Request):
        core.permissions.require(subject, "admin." + request.method.lower(), "http:" + request.url.path)

    router = APIRouter(prefix="/api/v1/qq/onboarding", dependencies=[Depends(authenticate), Depends(authorize)])

    def owner(request):
        # Bind sessions to this authenticated token, never trust a client-supplied owner.
        return hashlib.sha256(request.headers.get("authorization", "").encode()).hexdigest()

    @router.post("/sessions")
    async def create(body: CreateSession, request: Request):
        try:
            master_key()
        except ValueError:
            raise HTTPException(503, "configure QQBOT_CREDENTIAL_KEY before onboarding") from None
        with core.config_store.lock:
            existing = next((a for a in core.config_store.read().adapters
                             if a.instance_id == body.instance_id), None)
            if existing:
                if existing.kind != "official" or body.replace_app_id != existing.app_id:
                    raise HTTPException(409, "explicit matching app identity required for rotation")
                if existing.enabled or body.instance_id in getattr(core, "adapters", {}):
                    raise HTTPException(409, "disable adapter and restart before credential rotation")
            elif body.replace_app_id is not None:
                raise HTTPException(409, "rotation target does not exist")
        session = await service.create(owner(request), body.instance_id)
        session.replace_app_id = body.replace_app_id
        return session.public()

    @router.get("/sessions/{session_id}")
    async def status(session_id: str, request: Request):
        return service.get(session_id, owner(request)).public()

    @router.get("/sessions/{session_id}/qr")
    async def qr(session_id: str, request: Request):
        session = service.get(session_id, owner(request))
        if session.state != "waiting":
            raise HTTPException(409, "QR code is no longer active")
        import qrcode
        from qrcode.image.svg import SvgPathImage
        output = io.BytesIO()
        qrcode.make(session.public()["connect_url"], image_factory=SvgPathImage).save(output)
        return Response(output.getvalue(), media_type="image/svg+xml", headers={"Cache-Control": "no-store"})

    @router.delete("/sessions/{session_id}")
    async def cancel(session_id: str, request: Request):
        return (await service.cancel(session_id, owner(request))).public()

    @router.post("/sessions/{session_id}/apply")
    async def apply(session_id: str, body: ApplySession, request: Request):
        async with service.lock:
            session = service.get(session_id, owner(request))
            if session.state != "ready" or session.app_id != body.expected_app_id:
                raise HTTPException(409, "session is not ready or app identity changed")
            if session.replace_app_id is not None and session.app_id != session.replace_app_id:
                raise HTTPException(409, "rotation cannot change app identity; create a new instance")
            session.state = "validating"
            secret, app_id = session.secret, session.app_id
        # Validate the credentials without sending messages or starting an adapter.
        try:
            response = await service.client.post("https://bots.qq.com/app/getAppAccessToken",
                json={"appId": app_id, "clientSecret": secret}, timeout=10, follow_redirects=False)
            if response.status_code != 200 or len(response.content) > 65536:
                raise ValueError
            token = response.json().get("access_token")
            if not isinstance(token, str) or not token:
                raise ValueError
        except asyncio.CancelledError:
            async with service.lock:
                if session.state == "validating":
                    session.state = "ready"
            raise
        except (httpx.HTTPError, ValueError, TypeError, KeyError, AttributeError):
            async with service.lock:
                if session.state == "validating":
                    session.state = "ready"
            raise HTTPException(502, "credential validation failed") from None
        finally:
            secret = None
        try:
            authorize(request)
        except Exception:
            async with service.lock:
                if session.state == "validating":
                    session.state = "ready"
            raise
        async with service.lock:
            session = service.get(session_id, owner(request))
            if session.state != "validating":
                raise HTTPException(409, "session was cancelled or expired")
            with core.config_store.lock:
                previous = core.config_store.read()
                if version(previous) != body.expected_version:
                    session.state = "ready"
                    raise HTTPException(409, "configuration changed; reload before saving")
                existing = next((a for a in previous.adapters if a.instance_id == session.instance_id), None)
                rotating = session.replace_app_id is not None
                if rotating and (existing is None or existing.kind != "official"
                                 or existing.app_id != session.replace_app_id or existing.enabled
                                 or session.instance_id in getattr(core, "adapters", {})):
                    session.state = "ready"
                    raise HTTPException(409, "rotation target changed or is still running")
                if any((a.instance_id == session.instance_id or a.app_id == app_id)
                       and not (rotating and a.instance_id == session.instance_id) for a in previous.adapters):
                    session.state = "ready"
                    raise HTTPException(409, "adapter or app already configured; no credentials overwritten")
                directory = core.config_store.path.parent / "qq-credentials"
                reference = None
                try:
                    reference = save_credential(directory, app_id, session.secret)
                    cfg = AdapterConfig(instance_id=session.instance_id, kind="official", bot_id=app_id,
                        app_id=app_id, secret_env="", enabled=False, credential_ref=reference,
                        credential_store_dir=str(directory.resolve()))
                    values = previous.model_dump(mode="json")
                    if rotating:
                        # Preserve policies and tuning, but never enable a rotated credential implicitly.
                        replacement = existing.model_copy(update={"secret_env": "", "credential_ref": reference,
                            "credential_store_dir": str(directory.resolve()), "enabled": False})
                        values["adapters"] = [replacement.model_dump(mode="json")
                            if a["instance_id"] == session.instance_id else a for a in values["adapters"]]
                    else:
                        values["adapters"].append(cfg.model_dump(mode="json"))
                    candidate = Settings.model_validate_json(json.dumps(values), strict=False)
                    core.config_store.save(previous, candidate)
                except (OSError, ValueError, TypeError):
                    if reference:
                        remove_credential(directory, reference)
                    session.state = "ready"
                    raise HTTPException(500, "credential configuration could not be saved") from None
            session.state = "saved"
            session.clear()
            return {**session.public(), "saved_version": version(candidate),
                    "note": "Saved disabled. Configure unified PermissionPolicy and webhook, then explicitly enable and restart."}

    app.include_router(router)
    assets = Path(__file__).parent

    @app.get("/admin/qq-onboarding", response_class=HTMLResponse)
    async def page():
        return HTMLResponse((assets / "qq_onboarding.html").read_text(), headers={
            "Content-Security-Policy": "default-src 'none'; script-src 'self'; style-src 'self'; "
                "connect-src 'self'; img-src 'self' blob:; base-uri 'none'; frame-ancestors 'none'; form-action 'self'",
            "Cache-Control": "no-store", "Referrer-Policy": "no-referrer"})

    @app.get("/admin/qq-onboarding.js")
    async def javascript():
        return FileResponse(assets / "qq_onboarding.js", media_type="text/javascript")
    return service
