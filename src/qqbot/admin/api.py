import asyncio
import hashlib
import json
import os
import re
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Literal
from urllib.parse import quote

import httpx
from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import select

from qqbot.config import Settings
from qqbot.configuration import changes
from qqbot.configuration import version as config_digest
from qqbot.contracts.memory import MemoryScope
from qqbot.domain import AccessContext, ConversationRef, InboundEvent, PrincipalRef
from qqbot.domain.messages import SendReceipt
from qqbot.domain.tasks import TaskResult
from qqbot.permissions import (
    PermissionPolicy,
    PermissionRule,
    PermissionService,
    PermissionSubject,
    PolicyVersionConflict,
)
from qqbot.storage.schema import conversations, outbox
from qqbot.storage.task_schema import events, runs, tasks


class PolicyPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expected_version: int
    mode: Literal["addressed_only", "silent"]


class TaskRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    goal: str = Field(min_length=1, max_length=16000)
    success_checks: tuple[str, ...] = Field(default=(), max_length=20)
    requested_outputs: tuple[str, ...] = Field(default=(), max_length=20)
    workspace_id: str | None = Field(default=None, min_length=1, max_length=64)


class ConfigRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expected_version: str | None = None
    config: Settings

    @field_validator("config", mode="before")
    @classmethod
    def parse_json_config(cls, value):
        return value if isinstance(value, Settings) else Settings.model_validate_json(json.dumps(value), strict=False)


class DeliveryResolution(BaseModel):
    model_config = ConfigDict(extra="forbid")
    state: Literal["SENT", "FAILED"]
    evidence: str = Field(min_length=1, max_length=1000)
    platform_message_id: str | None = Field(default=None, max_length=128)


class TaskReconciliationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    result: TaskResult | None = None
    resume: bool = False

    @model_validator(mode="after")
    def choose_one_recovery_action(self):
        if self.resume == (self.result is not None):
            raise ValueError("choose exactly one of resume or result")
        return self


class PermissionRuleRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expected_version: int = Field(ge=0)
    rule: PermissionRule


class PermissionCheckRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    subject: PermissionSubject
    action: str = Field(min_length=1)
    resource: str = Field(min_length=1)


class PermissionPolicyRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expected_version: int = Field(ge=0)
    policy: PermissionPolicy


def router(core, authenticate):
    permissions = getattr(core, "permissions", None) or PermissionService(core.db, core.clock)
    admin_subject = PermissionSubject(
        adapter_instance_id="admin_api", identity_namespace="admin_api", user_id="owner"
    )

    def authorize_admin(request: Request):
        action = "admin." + request.method.lower()
        if not permissions.check(admin_subject, action, "http:" + request.url.path).allowed:
            raise HTTPException(403, "permission denied")

    router = APIRouter(dependencies=[Depends(authenticate), Depends(authorize_admin)])

    def require_permission(action, resource):
        if not permissions.check(admin_subject, action, resource).allowed:
            raise HTTPException(403, "permission denied")

    def permission_management(action):
        if not permissions.check(admin_subject, "permission." + action, "permission:policy").allowed:
            raise HTTPException(403, "permission denied")

    def preserve_console_access(policy):
        proposed = PermissionService(policy=policy)
        for action, resource in (
            ("admin.get", "http:/api/v1/config"),
            ("admin.get", "http:/api/v1/config/schema"),
            ("admin.get", "http:/api/v1/permissions"),
            ("admin.get", "http:/api/v1/permissions/schema"),
            ("admin.put", "http:/api/v1/permissions"),
            ("permission.list", "permission:policy"),
            ("permission.set_policy", "permission:policy"),
            ("service.status.read", "service:core"),
        ):
            if not proposed.check(admin_subject, action, resource).allowed:
                raise HTTPException(422, "policy would lock the administrator out of the console")

    @router.get("/api/v1/permissions")
    def list_permissions():
        permission_management("list")
        return permissions.list()

    @router.get("/api/v1/permissions/schema")
    def permission_schema():
        permission_management("list")
        return PermissionPolicy.model_json_schema()

    @router.put("/api/v1/permissions")
    def replace_permissions(body: PermissionPolicyRequest):
        permission_management("set_policy")
        preserve_console_access(body.policy)
        try:
            return permissions.set_policy(
                body.policy, expected_version=body.expected_version, actor="admin_api:owner"
            )
        except PolicyVersionConflict as error:
            raise HTTPException(409, str(error)) from error

    @router.post("/api/v1/permissions/check")
    def check_permission(body: PermissionCheckRequest):
        permission_management("check")
        return permissions.check(body.subject, body.action, body.resource).model_dump(mode="json")

    @router.post("/api/v1/permissions/explain")
    def explain_permission(body: PermissionCheckRequest):
        permission_management("explain")
        return permissions.explain(body.subject, body.action, body.resource).model_dump(mode="json")

    @router.get("/api/v1/permissions/{rule_id}")
    def get_permission(rule_id: str):
        permission_management("get")
        rule = permissions.get(rule_id)
        if rule is None:
            raise HTTPException(404, "permission rule unavailable")
        return {"policy_version": permissions.policy_version, "rule": rule.model_dump(mode="json")}

    @router.put("/api/v1/permissions/{rule_id}")
    def upsert_permission(rule_id: str, body: PermissionRuleRequest):
        permission_management("upsert")
        if rule_id != body.rule.id:
            raise HTTPException(422, "rule ID differs from path")
        try:
            return permissions.upsert(
                body.rule, expected_version=body.expected_version, actor="admin_api:owner"
            )
        except PolicyVersionConflict as error:
            raise HTTPException(409, str(error)) from error

    @router.delete("/api/v1/permissions/{rule_id}")
    def delete_permission(rule_id: str, expected_version: int):
        permission_management("delete")
        try:
            return permissions.delete(rule_id, expected_version=expected_version, actor="admin_api:owner")
        except PolicyVersionConflict as error:
            raise HTTPException(409, str(error)) from error
        except LookupError as error:
            raise HTTPException(404, str(error)) from error

    def conversation(cid):
        with core.db.engine.connect() as c:
            raw = c.execute(
                select(conversations.c.ref_json).where(conversations.c.id == cid)
            ).scalar_one_or_none()
        if not raw:
            raise HTTPException(404, "conversation unavailable")
        ref = ConversationRef.model_validate_json(raw)
        if ref.adapter_instance_id not in core.adapters:
            raise HTTPException(404, "adapter unavailable")
        if not permissions.check(admin_subject, "conversation.manage", "conversation:" + ref.key).allowed:
            raise HTTPException(403, "permission denied")
        return ref

    def owner(ref, principal=None):
        return AccessContext(
            conversation=ref,
            principal=principal
            or PrincipalRef(
                adapter_instance_id=ref.adapter_instance_id,
                identity_namespace="admin_api",
                remote_user_id="owner",
            ),
            role="owner",
        )

    def task_row(task_ref):
        with core.db.engine.connect() as c:
            row = c.execute(select(tasks).where(
                (tasks.c.public_id == task_ref) | (tasks.c.id == task_ref)
            )).mappings().first()
        if not row:
            raise HTTPException(404, "task unavailable")
        row = dict(row)
        if row.get("owner_id") not in {None, core.settings.owner.id}:
            raise HTTPException(404, "task unavailable")
        try:
            event = InboundEvent.model_validate_json(row["event_json"])
        except ValueError as error:
            raise HTTPException(409, "task source is unavailable") from error
        if core.sessions.owner(event) != core.settings.owner.id:
            raise HTTPException(404, "task unavailable")
        return row, core.access(event)

    @router.get("/api/v1/adapters")
    async def adapters():
        result = []
        for key, adapter in core.adapters.items():
            cfg = adapter.config
            result.append(
                {
                    "id": key,
                    "kind": cfg.kind,
                    "configured_full": cfg.full_mode_enabled,
                    "verified_full": cfg.full_mode_verified or getattr(adapter, "full_event_observed", False),
                    "proactive_verified": cfg.proactive_verified,
                    "credentials_configured": bool(os.environ.get(cfg.secret_env)),
                }
            )
        return result

    @router.patch("/api/v1/conversations/{cid}/policy")
    async def policy(cid: str, body: PolicyPatch):
        ref = conversation(cid)
        if core.state(cid)["policy_version"] != body.expected_version:
            raise HTTPException(409, "policy version changed")
        try:
            await core.set_mode(ref, body.mode, owner(ref), expected_version=body.expected_version)
        except (ValueError, PermissionError) as exc:
            raise HTTPException(422, str(exc)) from exc
        return core.policy_view(cid)

    @router.post("/api/v1/tasks")
    def create_task(body: TaskRequest, idempotency_key: str = Header(default="")):
        if not core.settings.tasks_enabled:
            raise HTTPException(409, "tasks disabled")
        try:
            return core.enqueue_admin_task(body.goal, idempotency_key=idempotency_key,
                success_checks=body.success_checks, requested_outputs=body.requested_outputs,
                workspace_id=body.workspace_id)
        except (ValueError, PermissionError) as error:
            raise HTTPException(422, str(error)) from error

    @router.get("/api/v1/tasks")
    def list_tasks():
        require_permission("task.read", "task:*")
        with core.db.engine.connect() as c:
            rows = c.execute(select(tasks).where(
                (tasks.c.owner_id == core.settings.owner.id) | tasks.c.owner_id.is_(None),
                (tasks.c.kind == "goal") | tasks.c.owner_id.is_(None),
                tasks.c.parent_task_id.is_(None),
            ).order_by(tasks.c.created_at.desc()).limit(100)).mappings().all()
            visible = []
            for row in rows:
                try:
                    task_row(row["id"])
                except HTTPException:
                    continue
                visible.append({**core.tasks.graph.status(c, row), "created_at": row["created_at"]})
            return visible

    @router.get("/api/v1/tasks/{public_id}")
    def get_task(public_id: str):
        row, access = task_row(public_id)
        require_permission("task.read", "task:" + row["id"])
        try:
            core.tasks.get(row["public_id"], access)
            with core.db.engine.connect() as c:
                result = core.tasks.graph.status(c, row)
        except (KeyError, PermissionError) as error:
            raise HTTPException(404, "task unavailable") from error
        with core.db.engine.connect() as c:
            result["deliveries"] = [
                dict(r._mapping)
                for r in c.execute(select(outbox.c.id, outbox.c.state).where(
                    (outbox.c.task_id == row["id"]) | (outbox.c.origin_id == row["id"])))
            ]
        return result

    @router.post("/api/v1/deliveries/{delivery_id}/resolve")
    def resolve_delivery(delivery_id: str, body: DeliveryResolution):
        from sqlalchemy import update

        with core.db.engine.connect() as c:
            row = c.execute(select(outbox).where(outbox.c.id == delivery_id)).mappings().first()
        if not row:
            raise HTTPException(404, "delivery unavailable")
        conversation(row["conversation_id"])
        with core.db.transaction() as c:
            changed = c.execute(update(outbox).where(outbox.c.id == delivery_id, outbox.c.state == "UNKNOWN").values(
                state=body.state, platform_message_id=body.platform_message_id,
                sent_at=core.clock.now().isoformat() if body.state == "SENT" else None,
                receipt_json=json.dumps({"state":body.state,"operator_evidence":body.evidence}),
            ))
            if changed.rowcount != 1:
                raise HTTPException(409, "only UNKNOWN deliveries can be reconciled")
            core.outbox._receipt(delivery_id, SendReceipt(state=body.state,
                platform_message_id=body.platform_message_id), connection=c)
        core.audit("delivery.resolve", delivery_id, "admin_api:owner", body.model_dump_json())
        return {"state":body.state, "resent":False}

    @router.post("/api/v1/tasks/{public_id}/cancel")
    def cancel_task(public_id: str):
        row, access = task_row(public_id)
        require_permission("task.cancel", "task:" + row["id"])
        try:
            return core.tasks.cancel(row["public_id"], access)
        except (KeyError, PermissionError) as error:
            raise HTTPException(404, "task unavailable") from error

    @router.get("/api/v1/tasks/{public_id}/events")
    async def task_events(public_id: str, request: Request, last_event_id: str = Header(default="0")):
        row, _ = task_row(public_id)
        require_permission("task.read", "task:" + row["id"])
        try:
            after = int(last_event_id)
        except ValueError:
            raise HTTPException(400, "invalid Last-Event-ID") from None

        async def stream():
            cursor = after
            while not await request.is_disconnected():
                with core.db.engine.connect() as c:
                    rows = (
                        c.execute(
                            select(events)
                            .join(runs, events.c.run_id == runs.c.id)
                            .where(runs.c.task_id == row["id"], events.c.seq > cursor)
                            .order_by(events.c.seq)
                            .limit(100)
                        )
                        .mappings()
                        .all()
                    )
                for item in rows:
                    cursor = item["seq"]
                    yield f"id: {cursor}\ndata: {json.dumps(dict(item), ensure_ascii=False)}\n\n"
                yield ": heartbeat\n\n"
                await asyncio.sleep(2)

        return StreamingResponse(stream(), media_type="text/event-stream")

    @router.get("/api/v1/tasks/{public_id}/operations")
    def task_operations(public_id: str):
        row, access = task_row(public_id)
        require_permission("task.operations.read", "task:" + row["id"])
        try:
            return core.recovery.list_operations(row["public_id"], access)
        except KeyError as error:
            raise HTTPException(404, "task operation unavailable") from error
        except PermissionError as error:
            raise HTTPException(409, str(error)) from error

    @router.get("/api/v1/tasks/{public_id}/operations/{operation_id}/readback")
    async def readback_operation(public_id: str, operation_id: str):
        row, access = task_row(public_id)
        require_permission("task.operations.read", "task:" + row["id"])
        try:
            return await core.recovery.readback_operation(row["public_id"], access, operation_id)
        except KeyError as error:
            raise HTTPException(404, "task operation unavailable") from error
        except PermissionError as error:
            raise HTTPException(409, str(error)) from error
        except ValueError as error:
            raise HTTPException(409, str(error)) from error

    @router.post("/api/v1/tasks/{public_id}/runs/{run_id}/reconcile")
    def reconcile_task(public_id: str, run_id: str, body: TaskReconciliationRequest):
        row, access = task_row(public_id)
        require_permission("task.reconcile", "task:" + row["id"])
        try:
            return core.recovery.reconcile_task(
                row["public_id"], access, run_id,
                result=body.result.model_dump(mode="json") if body.result else None,
                resume=body.resume,
            )
        except KeyError as error:
            raise HTTPException(404, "recovery source unavailable") from error
        except PermissionError as error:
            raise HTTPException(409, str(error)) from error
        except ValueError as error:
            raise HTTPException(422, str(error)) from error

    def memory_scope():
        return MemoryScope(owner_id=core.settings.owner.id)

    @router.get("/api/v1/memory/status")
    async def memory_status():
        scope = memory_scope()
        require_permission("memory.read", "memory:" + scope.key)
        try:
            return await core.openviking.status(scope)
        except PermissionError as error:
            raise HTTPException(409, str(error)) from error

    @router.get("/api/v1/memory/changes")
    def memory_changes(after: int = Query(default=0, ge=0)):
        scope = memory_scope()
        require_permission("memory.read", "memory:" + scope.key)
        if not core.openviking.enabled(scope):
            return []
        try:
            changes = core.openviking.changes_since(scope, after=after)
        except PermissionError as error:
            raise HTTPException(409, str(error)) from error
        fields = ("seq", "id", "operation_id", "epoch", "kind", "refs", "created_at")
        return [{key: item[key] for key in fields} for item in changes]

    @router.post("/api/v1/memory/reconcile")
    async def reconcile_memory_restore():
        scope = memory_scope()
        require_permission("memory.write", "memory:" + scope.key)
        try:
            return await core.openviking.reconcile_restore(scope)
        except PermissionError as error:
            raise HTTPException(409, str(error)) from error
        except (RuntimeError, ValueError) as error:
            raise HTTPException(409, str(error)) from error
        except httpx.HTTPError as error:
            raise HTTPException(503, "OpenViking restore verification is unavailable") from error

    @router.get("/api/v1/config")
    def config_view():
        with core.config_store.lock:
            saved = core.config_store.read()
            names = {
                saved.admin_token_env, saved.internal_token_env, saved.model.api_key_env,
                saved.memory.openviking.api_key_env,
                *(adapter.secret_env for adapter in saved.adapters),
            }
            return {
                "config": core.settings.model_dump(mode="json"),
                "version": config_digest(core.settings),
                "saved_config": saved.model_dump(mode="json"),
                "saved_version": config_digest(saved),
                "model_key_configured": bool(os.environ.get(core.settings.model.api_key_env)),
                "credentials": [{"name": name, "configured": bool(os.environ.get(name))} for name in sorted(names)],
                **changes(core.settings, saved),
                "restart_required": saved != core.settings,
            }

    @router.get("/api/v1/config/schema")
    def config_schema():
        return Settings.model_json_schema()

    @router.post("/api/v1/config/validate")
    def validate_config(body: ConfigRequest):
        return {"valid": True, **changes(core.settings, body.config)}

    @router.get("/api/v1/config/versions")
    def config_versions(include_metadata: bool = False):
        directory = core.config_store.directory
        paths = [p for p in directory.glob("*.json") if re.fullmatch(r"[a-f0-9]{64}", p.stem)]
        if include_metadata:
            return [
                {"version": path.stem, "saved_at": datetime.fromtimestamp(path.stat().st_mtime, UTC).isoformat()}
                for path in sorted(paths, key=lambda path: path.stat().st_mtime, reverse=True)
            ]
        return sorted(path.stem for path in paths)

    @router.get("/api/v1/config/versions/{version}")
    def config_version(version: str):
        if not re.fullmatch(r"[a-f0-9]{64}", version):
            raise HTTPException(400, "invalid config version")
        path = core.config_store.directory / (version + ".json")
        if not path.is_file():
            raise HTTPException(404, "config version unavailable")
        return Settings.model_validate_json(path.read_text(), strict=False).model_dump(mode="json")

    @router.post("/api/v1/config/save")
    def save_config(body: ConfigRequest):
        with core.config_store.lock:
            previous = core.config_store.read()
            if body.expected_version != config_digest(previous):
                raise HTTPException(409, "saved config version changed; reload before saving")
            if not permissions.policy_version and body.config.permissions != core.settings.permissions:
                preserve_console_access(body.config.permissions)
            core.config_store.save(previous, body.config)
            version = config_digest(body.config)
            core.audit("config.save", version, "admin_api:owner", "saved configuration for next Core startup")
            return {
                "version": version, "saved": True, "applied": False,
                **changes(core.settings, body.config), "restart_required": body.config != core.settings,
            }

    @router.post("/api/v1/config/apply")
    async def apply_config(body: ConfigRequest):
        with core.config_store.lock:
            if body.expected_version != config_digest(core.settings):
                raise HTTPException(409, "config version changed")
            if core.config_store.read() != core.settings:
                raise HTTPException(409, "saved configuration pending; restart or restore it before hot apply")
            # Connection/credential/process changes never produce a half-applied runtime.
            if body.config.model != core.settings.model and (
                core.settings.interactive_execution_enabled or core.settings.tasks_enabled
            ):
                raise HTTPException(
                    409, "model changes require restarting Core and Worker with matching startup configuration"
                )
            if changes(core.settings, body.config)["restart_required"]:
                raise HTTPException(409, "connection, identity or isolation changes require restart")
            core.config_store.save(core.settings, body.config, hot=True)
            core.settings = body.config
            version = config_digest(body.config)
            core.audit("config.apply", version, "admin_api:owner", "validated model/budget config")
            return {"version": version, "applied": True}

    @router.get("/api/v1/artifacts/{task_id}/{digest}/download")
    def artifact(task_id: str, digest: str):
        row, access = task_row(task_id)
        require_permission("task.read", "task:" + row["id"])
        visible = core.tasks.get(row["public_id"], access)
        if visible.get("context_revoked"):
            raise HTTPException(409, "artifact handoff sources are no longer authorized")
        if core.clock.now() > datetime.fromisoformat(row["created_at"]) + timedelta(days=30):
            raise HTTPException(410, "artifact download expired")
        result = json.loads(row["result_json"] or "{}")
        item = next((a for a in result.get("artifacts", []) if a.get("sha256") == digest), None)
        if item is None:
            raise HTTPException(404)
        root = Path(core.tasks.archive_root).resolve()
        if not re.fullmatch(r"[a-f0-9-]{36}", item["id"]):
            raise HTTPException(409, "invalid artifact reference")
        try:
            fd = os.open(root / item["id"], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(fd, "rb") as source:
                info = os.fstat(source.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > 20 * 1024 * 1024:
                    raise HTTPException(409, "artifact unavailable")
                content = source.read(20 * 1024 * 1024 + 1)
        except OSError as error:
            raise HTTPException(409, "artifact unavailable") from error
        if len(content) != item["size"] or hashlib.sha256(content).hexdigest() != digest:
            raise HTTPException(409, "artifact digest changed")
        visible = core.tasks.get(row["public_id"], access)
        if visible.get("context_revoked"):
            raise HTTPException(409, "artifact handoff sources are no longer authorized")
        filename = PurePosixPath(item.get("relative_path") or item["id"]).name[:255] or item["id"]
        return Response(
            content,
            media_type="application/octet-stream",
            headers={
                "X-Content-Type-Options": "nosniff",
                "Content-Disposition": "attachment; filename*=UTF-8''" + quote(filename, safe=""),
            },
        )

    return router


ADMIN_HTML = Path(__file__).with_name("page.html").read_text()
