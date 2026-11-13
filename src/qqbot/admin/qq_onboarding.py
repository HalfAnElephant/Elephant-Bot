"""Authenticated QQ setup with automatic owner provisioning and manual credential save."""
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
    automatic: bool = False


class ApplySession(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expected_version: str
    expected_app_id: str = Field(min_length=1, max_length=128)


def install_qq_onboarding(app, core, authenticate, *, client=None, activate=None):
    service = Onboarding(client=client)
    app.state.qq_onboarding = service
    subject = PermissionSubject(adapter_instance_id="admin_api", identity_namespace="admin_api", user_id="owner")

    def authorize(request: Request):
        core.permissions.require(subject, "admin." + request.method.lower(), "http:" + request.url.path)

    router = APIRouter(prefix="/api/v1/qq/onboarding", dependencies=[Depends(authenticate), Depends(authorize)])

    def owner(request):
        # Bind sessions to this authenticated token, never trust a client-supplied owner.
        return hashlib.sha256(request.headers.get("authorization", "").encode()).hexdigest()

    def automatic_authority():
        core.permissions.require(subject, "permission.set_policy", "permission:policy")
        core.permissions.require(subject, "admin.post", "http:/api/v1/config/save")

    def audit(session, stage, error=None):
        if hasattr(core, "audit"):
            core.audit("qq.onboarding", session.id, "admin_api:owner", json.dumps({
                "instance_id": session.instance_id, "app_id": session.app_id, "stage": stage, "error": error}))

    async def automatic_apply(session):
        from qqbot.admin.qq_provisioning import provision_owner

        try:
            automatic_authority()
            session.state = "validating"
            audit(session, session.state)
            session.result = await provision_owner(core, session, client=service.client, activate=activate)
            session.state = "connected" if session.result.get("activated") else "connecting"
            audit(session, session.state)
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 — isolate a background setup failure and publish a credential-safe result.
            session.state = "connecting" if session.provisioned else "failed"
            session.error = getattr(error, "code", "automatic_provisioning_failed")
            audit(session, session.state, session.error)

    service.on_ready = automatic_apply

    def connection_state(instance_id, expected=None):
        adapter = getattr(core, "adapters", {}).get(instance_id)
        if expected is None:
            expected = next((cfg for cfg in core.config_store.read().adapters if cfg.instance_id == instance_id), None)
        if adapter is None or adapter.config != expected:
            return {"state": "CONFIG_PENDING", "connected": False, "healthy": False}
        gateway = getattr(adapter, "gateway", None)
        return gateway.snapshot() if gateway else {"state": "LISTENING", "connected": False, "healthy": True}

    @router.get("/status")
    async def configuration_status():
        saved = core.config_store.read()
        try:
            master_key()
            key_ready = True
        except ValueError:
            key_ready = False
        return {"credential_key_ready": key_ready, "automatic_available": activate is not None,
            "adapters": [{"instance_id": cfg.instance_id, "app_id": cfg.app_id, "enabled": cfg.enabled,
                "transport": cfg.transport, "owner_bound": any(binding.app_id == cfg.app_id for binding in saved.owner.qq_bindings),
                "connection": connection_state(cfg.instance_id, cfg)} for cfg in saved.adapters]}

    @router.post("/sessions")
    async def create(body: CreateSession, request: Request):
        try:
            master_key()
        except ValueError:
            raise HTTPException(503, "configure QQBOT_CREDENTIAL_KEY before onboarding") from None
        if body.automatic:
            automatic_authority()
            if activate is None:
                raise HTTPException(503, "automatic adapter activation is unavailable")
        with core.config_store.lock:
            existing = next((a for a in core.config_store.read().adapters
                             if a.instance_id == body.instance_id), None)
            if existing:
                if existing.kind != "official" or (body.replace_app_id != existing.app_id and not (
                        body.automatic and body.replace_app_id is None)):
                    raise HTTPException(409, "explicit matching app identity required for rotation")
                if not body.automatic and (existing.enabled or body.instance_id in getattr(core, "adapters", {})):
                    raise HTTPException(409, "disable adapter and restart before credential rotation")
            elif body.replace_app_id is not None:
                raise HTTPException(409, "rotation target does not exist")
        session = await service.create(owner(request), body.instance_id, automatic=body.automatic)
        session.replace_app_id = existing.app_id if existing and body.automatic else body.replace_app_id
        audit(session, "creating")
        return session.public()

    @router.get("/sessions/{session_id}")
    async def status(session_id: str, request: Request):
        session = service.get(session_id, owner(request))
        if session.automatic and session.provisioned:
            connection = connection_state(session.instance_id)
            if connection["state"] == "CONFIG_PENDING" and session.result.get("connection", {}).get("error"):
                connection = session.result["connection"]
            session.result.update(connection=connection, activated=connection["connected"])
            session.state = "connected" if connection["connected"] else "failed" if connection["state"] in {"FAILED", "FATAL"} else "connecting"
            session.error = connection.get("error")
        return session.public()

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
            if session.automatic:
                raise HTTPException(409, "automatic sessions are applied by the server")
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
