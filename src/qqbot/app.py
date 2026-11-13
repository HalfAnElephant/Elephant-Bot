import asyncio
import logging
import os
import secrets
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError

from qqbot.adapters.qq_official.adapter import OfficialAdapter
from qqbot.adapters.qq_official.interaction_delivery import handle_callback, handle_verified_interaction
from qqbot.adapters.qq_official.interactions import InteractionStore
from qqbot.adapters.qq_official.interactions import router as interaction_router
from qqbot.adapters.qq_official.menus import router as menu_router
from qqbot.admin.api import ADMIN_HTML
from qqbot.admin.api import router as admin_router
from qqbot.admin.qq_delivery import router as qq_delivery_router
from qqbot.admin.qq_onboarding import install_qq_onboarding
from qqbot.cli import migrate
from qqbot.clock import Clock
from qqbot.configuration import ConfigurationStore
from qqbot.domain import ConversationRef
from qqbot.ingress.service import Inbox
from qqbot.observability import configure_logging
from qqbot.permissions import PermissionSubject
from qqbot.runtime import Core
from qqbot.storage.db import Database
from qqbot.storage.schema import conversations, inbox, outbox


def create_app(settings, *, clock=None, run_background=True, official_client=None, model_client=None,
               harnesses=None, default_harness="pi", execution=None):
    configure_logging()
    config_store = ConfigurationStore(settings)
    settings = config_store.read()
    clock = clock or Clock()
    migrate(settings.database)
    db = Database(settings.database)
    incoming = Inbox(db)
    adapters = {
        cfg.instance_id: OfficialAdapter(cfg, settings.deployment_id, incoming, clock, official_client)
        for cfg in settings.adapters if cfg.enabled
    }
    core = Core(settings, db, clock, adapters, model_client, harnesses=harnesses,
                default_harness=default_harness, execution=execution)
    core.config_store = config_store

    @asynccontextmanager
    async def lifespan(app):
        loop = None
        try:
            for adapter in adapters.values():
                await adapter.start()
            loop = asyncio.create_task(core.run()) if run_background else None
            app.state.core_task = loop
            if loop:
                def core_finished(task):
                    if task.cancelled() or core.stopping:
                        return
                    error = task.exception()
                    app.state.core_error = type(error).__name__ if error else "UnexpectedExit"
                    logging.getLogger(__name__).error(
                        "Core background task exited", extra={"data": {"error_type": app.state.core_error}},
                    )
                loop.add_done_callback(core_finished)
            app.state.ready = True
            yield
        finally:
            app.state.ready = False
            await app.state.qq_onboarding.close()
            await core.stop()
            if loop:
                loop.cancel()
                await asyncio.gather(loop, return_exceptions=True)
            for adapter in adapters.values():
                await adapter.stop()
            db.close()

    app = FastAPI(title="Elephant Bot", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    app.state.core, app.state.adapters, app.state.db, app.state.ready = core, adapters, db, False

    @app.middleware("http")
    async def private_console_responses(request, call_next):
        response = await call_next(request)
        if request.url.path.startswith(("/admin", "/api/v1/")):
            response.headers["Cache-Control"] = "no-store"
            response.headers["X-Content-Type-Options"] = "nosniff"
            response.headers["Referrer-Policy"] = "no-referrer"
        return response

    @app.exception_handler(ValueError)
    async def invalid_request(request, exc):
        return JSONResponse({"detail": str(exc)[:240]}, status_code=422)

    @app.exception_handler(PermissionError)
    async def denied(request, exc):
        return JSONResponse({"detail": str(exc)[:240]}, status_code=403)

    @app.exception_handler(LookupError)
    async def unavailable(request, exc):
        return JSONResponse({"detail": "object unavailable"}, status_code=404)

    def background_health():
        loops = core.supervisor.snapshot()
        task = getattr(app.state, "core_task", None)
        running = not run_background or (task is not None and not task.done())
        healthy = running and all(state["healthy"] for state in loops.values() if state["critical"])
        return {
            "enabled": run_background, "running": running, "healthy": healthy,
            "error_type": getattr(app.state, "core_error", None), "loops": loops,
            "degraded": [name for name, state in loops.items() if not state["healthy"] or state["failures"]],
        }

    @app.get("/healthz")
    @app.get("/health/live")
    def health():
        return {"status": "ok", "source": core.settings.source_url or "not_configured"}

    @app.get("/health/ready")
    def ready():
        database_ready = True
        try:
            with db.engine.connect() as c:
                c.exec_driver_sql("SELECT 1")
        except SQLAlchemyError:
            database_ready = False
        background = background_health()
        connections = {key: adapter.gateway.snapshot() for key, adapter in adapters.items() if adapter.gateway}
        disconnected = [key for key, connection in connections.items() if not connection["healthy"]]
        is_ready = app.state.ready and database_ready and background["healthy"] and not disconnected
        return JSONResponse(
            {
                "ready": is_ready,
                "database": "ok" if database_ready else "unavailable",
                "background": background,
                "connections": connections,
                "disconnected": disconnected,
                "external_acceptance": "not_implied_by_readiness",
            },
            status_code=200 if is_ready else 503,
        )

    def admin(request: Request):
        token = os.environ.get(settings.admin_token_env, "")
        if not token or not secrets.compare_digest(
            request.headers.get("authorization", ""), "Bearer " + token
        ):
            raise HTTPException(401, "administrator authentication required")

    app.include_router(admin_router(core, admin))
    interaction_store = InteractionStore(db, clock)
    app.state.qq_interactions = interaction_store

    def attach_interactions(adapter):
        async def interaction(payload):
            return await handle_verified_interaction(adapter, core, interaction_store, payload)
        adapter.interaction_handler = interaction

    for adapter in adapters.values():
        attach_interactions(adapter)
    activation_lock = asyncio.Lock()

    async def activate_official(candidate, cfg):
        async with activation_lock:
            if core.config_store.read() != candidate:
                raise ValueError("configuration changed before adapter activation")
            expected = core.settings.model_copy(update={"adapters": candidate.adapters, "permissions": candidate.permissions,
                "owner": core.settings.owner.model_copy(update={"qq_bindings": candidate.owner.qq_bindings})})
            if (expected != candidate
                    or tuple(item for item in core.settings.adapters if item.instance_id != cfg.instance_id)
                    != tuple(item for item in candidate.adapters if item.instance_id != cfg.instance_id)
                    or tuple(item for item in core.settings.owner.qq_bindings if item.app_id != cfg.app_id)
                    != tuple(item for item in candidate.owner.qq_bindings if item.app_id != cfg.app_id)):
                raise ValueError("adapter activation cannot apply unrelated pending configuration")
            old = adapters.get(cfg.instance_id)
            if old and old.config == cfg and old.gateway and old.gateway.snapshot()["healthy"]:
                return old.gateway.snapshot()
            if old:
                await old.stop()
            adapter = OfficialAdapter(cfg, candidate.deployment_id, incoming, clock, official_client)
            adapter.permissions = core.permissions
            attach_interactions(adapter)
            core.settings = candidate
            core.sessions.settings = candidate
            core.sessions.learning.settings = candidate
            core.tasks.graph.settings = candidate
            if core.scheduler:
                core.scheduler.settings = candidate
            core.outbox.markdown_adapters = frozenset(item.instance_id for item in candidate.adapters
                if item.enabled and item.markdown_enabled)
            adapters[cfg.instance_id] = adapter
            try:
                await adapter.start()
                for _ in range(100):
                    snapshot = adapter.gateway.snapshot()
                    if snapshot["connected"] or snapshot["state"] in {"FAILED", "FATAL"}:
                        return snapshot
                    await asyncio.sleep(0.1)
                return adapter.gateway.snapshot()
            except BaseException:
                await adapter.stop()
                if adapters.get(cfg.instance_id) is adapter:
                    adapters.pop(cfg.instance_id)
                raise

    install_qq_onboarding(app, core, admin, client=official_client, activate=activate_official)
    app.include_router(interaction_router(adapters, interaction_store, admin, core.permissions))
    app.include_router(menu_router(adapters, admin, core.permissions))
    app.include_router(qq_delivery_router(core, admin))

    @app.get("/admin", response_class=HTMLResponse)
    def management_page():
        return HTMLResponse(
            ADMIN_HTML,
            headers={"Content-Security-Policy": (
                "default-src 'none'; script-src 'self'; style-src 'self'; "
                "connect-src 'self'; img-src 'self' data:; base-uri 'none'; "
                "frame-ancestors 'none'; form-action 'self'"
            )},
        )

    @app.get("/admin/assets/{name}")
    def console_asset(name: str):
        media_types = {"console.js": "text/javascript", "console.css": "text/css"}
        if name not in media_types:
            raise HTTPException(404)
        return FileResponse(Path(__file__).parent / "admin" / name, media_type=media_types[name])

    @app.get("/api/v1/status")
    async def status(request: Request):
        admin(request)
        core.permissions.require(
            PermissionSubject(
                adapter_instance_id="admin_api", identity_namespace="admin_api", user_id="owner"
            ),
            "service.status.read",
            "service:core",
        )
        with db.engine.connect() as c:
            pending = c.execute(
                select(func.count()).select_from(inbox).where(inbox.c.state == "PENDING")
            ).scalar_one()
            delivery_states = c.execute(select(outbox.c.state, func.count()).group_by(outbox.c.state)).all()
        return {
            "inbox_pending": pending,
            "background": background_health(),
            "deliveries": dict(delivery_states),
            "model_configured": bool(core.settings.model.endpoint and core.settings.model.model
                                     and os.environ.get(core.settings.model.api_key_env)),
            "model_execution": "pi"
            if core.settings.interactive_execution_enabled or core.settings.tasks_enabled
            else "disabled",
            "tasks_enabled": core.settings.tasks_enabled,
            "interactive_execution_enabled": core.settings.interactive_execution_enabled,
            "adapters": [
                {
                    "id": key,
                    "kind": a.config.kind,
                    "full_event_observed": getattr(a, "full_event_observed", None),
                    "transport": a.config.transport,
                    "connection": a.gateway.snapshot() if a.gateway else None,
                }
                for key, a in adapters.items()
            ],
        }

    @app.get("/api/v1/conversations")
    async def list_conversations(request: Request):
        admin(request)
        core.permissions.require(
            PermissionSubject(
                adapter_instance_id="admin_api", identity_namespace="admin_api", user_id="owner"
            ),
            "conversation.list",
            "conversation:*",
        )
        with db.engine.connect() as c:
            rows = [dict(r._mapping) for r in c.execute(select(conversations).limit(200))]
        refs = {row["id"]: ConversationRef.model_validate_json(row["ref_json"]) for row in rows}
        result = []
        for row in rows:
            ref = refs[row["id"]]
            name = "群聊" if ref.surface == "group" else "私聊"
            result.append({**row, "policy": core.policy_view(row["id"]), "display_name": name,
                           "remote_id": ref.remote_conversation_id,
                           "display_label": f"{name}（{ref.remote_conversation_id}）"})
        return result

    @app.post("/webhooks/qq/{instance_id}")
    async def official(request: Request, instance_id: str):
        adapter = adapters.get(instance_id)
        if not isinstance(adapter, OfficialAdapter):
            raise HTTPException(404)
        if adapter.config.transport != "webhook":
            raise HTTPException(409, "adapter receives through the official WebSocket")
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > 2 * 1024 * 1024:
                raise HTTPException(413)
        try:
            return await handle_callback(adapter, core, interaction_store, bytes(body), request.headers)
        except (ValueError, KeyError, TypeError):
            raise HTTPException(401, "invalid callback") from None

    return app
