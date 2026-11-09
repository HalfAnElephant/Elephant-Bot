import asyncio
import os
import secrets
import socket
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from sqlalchemy import func, select

from qqbot.adapters.onebot_v11.adapter import OneBotAdapter
from qqbot.adapters.qq_official.adapter import OfficialAdapter
from qqbot.admin.api import ADMIN_HTML
from qqbot.admin.api import router as admin_router
from qqbot.cli import migrate
from qqbot.clock import Clock
from qqbot.configuration import ConfigurationStore
from qqbot.domain import ConversationRef
from qqbot.ingress.service import Inbox
from qqbot.jobs.api import internal_router
from qqbot.observability import configure_logging
from qqbot.permissions import PermissionSubject
from qqbot.runtime import Core
from qqbot.storage.db import Database
from qqbot.storage.schema import conversations, inbox, outbox


class InternalServer(uvicorn.Server):
    @contextmanager
    def capture_signals(self):
        yield


def create_app(settings, *, clock=None, run_background=True, official_client=None, model_client=None):
    configure_logging()
    config_store = ConfigurationStore(settings)
    settings = config_store.read()
    clock = clock or Clock()
    migrate(settings.database)
    db = Database(settings.database)
    incoming = Inbox(db)
    adapters = {
        cfg.instance_id: (
            OneBotAdapter(cfg, settings.deployment_id, incoming, clock)
            if cfg.kind == "onebot"
            else OfficialAdapter(cfg, settings.deployment_id, incoming, clock, official_client)
        )
        for cfg in settings.adapters
    }
    core = Core(settings, db, clock, adapters, model_client)
    core.config_store = config_store

    @asynccontextmanager
    async def lifespan(app):
        loop = server_task = server = listener = None
        socket_path = None
        try:
            for adapter in adapters.values():
                await adapter.start()
            if (settings.tasks_enabled or settings.interactive_execution_enabled) and run_background:
                token = os.environ.get(settings.internal_token_env, "")
                internal = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
                internal.include_router(internal_router(core.tasks, token))
                socket_path = Path(settings.internal_socket).resolve()
                socket_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                if socket_path.exists():
                    raise RuntimeError(
                        "internal socket already exists; reconcile the old Core before removing it"
                    )
                listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                listener.bind(str(socket_path))
                os.chmod(socket_path, 0o660)
                listener.listen(16)
                server = InternalServer(uvicorn.Config(internal, log_level="warning", access_log=False))
                server_task = asyncio.create_task(server.serve(sockets=[listener]))
                for _ in range(100):
                    if server.started:
                        break
                    if server_task.done():
                        await server_task
                    await asyncio.sleep(0.01)
                if not server.started:
                    raise RuntimeError("internal listener failed to start")
            loop = asyncio.create_task(core.run()) if run_background else None
            app.state.ready = True
            yield
        finally:
            app.state.ready = False
            await core.stop()
            if loop:
                loop.cancel()
                await asyncio.gather(loop, return_exceptions=True)
            if server:
                server.should_exit = True
            if server_task:
                try:
                    await asyncio.wait_for(asyncio.shield(server_task), timeout=3)
                except TimeoutError:
                    server_task.cancel()
                    await asyncio.gather(server_task, return_exceptions=True)
            if listener:
                listener.close()
                socket_path.unlink(missing_ok=True)
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

    @app.get("/healthz")
    @app.get("/health/live")
    def health():
        return {"status": "ok", "source": core.settings.source_url or "not_configured"}

    @app.get("/health/ready")
    def ready():
        with db.engine.connect() as c:
            c.exec_driver_sql("SELECT 1")
        disconnected = [
            a.config.instance_id
            for a in adapters.values()
            if isinstance(a, OneBotAdapter) and not a.account_online
        ]
        return JSONResponse(
            {
                "ready": app.state.ready and not disconnected,
                "disconnected": disconnected,
                "external_acceptance": "not_implied_by_readiness",
            },
            status_code=503 if disconnected or not app.state.ready else 200,
        )

    def admin(request: Request):
        token = os.environ.get(settings.admin_token_env, "")
        if not token or not secrets.compare_digest(
            request.headers.get("authorization", ""), "Bearer " + token
        ):
            raise HTTPException(401, "administrator authentication required")

    app.include_router(admin_router(core, admin))

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
            "deliveries": dict(delivery_states),
            "model_configured": None,
            "model_execution": "worker_pi"
            if core.settings.interactive_execution_enabled or core.settings.tasks_enabled
            else "disabled",
            "tasks_enabled": core.settings.tasks_enabled,
            "interactive_execution_enabled": core.settings.interactive_execution_enabled,
            "adapters": [
                {
                    "id": key,
                    "kind": a.config.kind,
                    "connected": bool(getattr(a, "websocket", None)),
                    "context_gap": getattr(a, "context_gap", None),
                    "full_event_observed": getattr(a, "full_event_observed", None),
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
        names = {}

        async def resolve_names(instance_id, adapter):
            selected = [ref for ref in refs.values() if ref.adapter_instance_id == instance_id]
            if selected and isinstance(adapter, OneBotAdapter):
                names[instance_id] = await adapter.resolve_conversation_names(selected)

        await asyncio.gather(*(resolve_names(key, adapter) for key, adapter in adapters.items()))
        result = []
        for row in rows:
            ref = refs[row["id"]]
            name = names.get(ref.adapter_instance_id, {}).get((ref.surface, ref.remote_conversation_id))
            name = name or ("群聊" if ref.surface == "group" else "私聊")
            result.append({**row, "policy": core.policy_view(row["id"]), "display_name": name,
                           "remote_id": ref.remote_conversation_id,
                           "display_label": f"{name}（{ref.remote_conversation_id}）"})
        return result

    @app.websocket("/onebot/{instance_id}")
    async def onebot(websocket: WebSocket, instance_id: str):
        adapter = adapters.get(instance_id)
        if not isinstance(adapter, OneBotAdapter):
            await websocket.close(code=1008)
            return
        try:
            await adapter.handle_socket(websocket)
        except (WebSocketDisconnect, TimeoutError, ValueError, KeyError):
            return

    @app.post("/webhooks/qq/{instance_id}")
    async def official(request: Request, instance_id: str):
        adapter = adapters.get(instance_id)
        if not isinstance(adapter, OfficialAdapter):
            raise HTTPException(404)
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > 2 * 1024 * 1024:
                raise HTTPException(413)
        try:
            return adapter.callback(bytes(body), request.headers)
        except (ValueError, KeyError, TypeError):
            raise HTTPException(401, "invalid callback") from None

    return app
