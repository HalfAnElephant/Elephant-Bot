import asyncio
import hashlib
import json
import os
import re
import stat
from datetime import datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Literal
from urllib.parse import quote
from uuid import uuid4

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import select

from qqbot.config import Settings
from qqbot.domain import AccessContext, ConversationRef, InboundEvent, PrincipalRef, TextSegment, stable_id
from qqbot.domain.messages import SendReceipt
from qqbot.domain.tasks import TaskResult
from qqbot.memory import MemoryRecord
from qqbot.permissions import PermissionRule, PermissionService, PermissionSubject, PolicyVersionConflict
from qqbot.storage.schema import conversations, decisions, inbox, outbox
from qqbot.storage.social_schema import social_records
from qqbot.storage.task_schema import events, runs, tasks


class PolicyPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expected_version: int
    mode: str | None = None
    persona: dict | None = None

    @model_validator(mode="after")
    def one_change(self):
        if (self.mode is None) == (self.persona is None):
            raise ValueError("change exactly one of mode or persona per versioned request")
        return self


class TaskRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    conversation_id: str
    goal: str = Field(min_length=1, max_length=16000)


class MemoryPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source_event_id: str
    text: str = Field(min_length=1, max_length=4000)


class ConfigRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expected_version: str | None = None
    config: Settings

    @field_validator("config", mode="before")
    @classmethod
    def parse_json_config(cls, value):
        return value if isinstance(value, Settings) else Settings.model_validate_json(json.dumps(value))


class ReconcileRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    run_id: str
    result: TaskResult | None = None


class DeliveryResolution(BaseModel):
    model_config = ConfigDict(extra="forbid")
    state: Literal["SENT", "FAILED"]
    evidence: str = Field(min_length=1, max_length=1000)
    platform_message_id: str | None = Field(default=None, max_length=128)


class PermissionRuleRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expected_version: int = Field(ge=0)
    rule: PermissionRule


class PermissionCheckRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    subject: PermissionSubject
    action: str = Field(min_length=1)
    resource: str = Field(min_length=1)


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

    @router.get("/api/v1/permissions")
    def list_permissions():
        permission_management("list")
        return permissions.list()

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

    def task_row(public_id):
        with core.db.engine.connect() as c:
            row = c.execute(select(tasks).where(tasks.c.public_id == public_id)).mappings().first()
        if not row:
            raise HTTPException(404, "task unavailable")
        conversation(row["conversation"])
        return dict(row)

    def memory_row(memory_id):
        require_permission("memory.manage", "memory:" + memory_id)
        with core.db.engine.connect() as c:
            row = c.execute(select(social_records).where(social_records.c.id == memory_id)).mappings().first()
        if not row:
            raise HTTPException(404, "memory unavailable")
        ref = conversation(row["conversation_id"])
        record = MemoryRecord.model_validate_json(row["record_json"])
        with core.db.engine.connect() as c:
            raw = c.execute(
                select(inbox.c.event_json).where(inbox.c.id == record.source_event_ids[0])
            ).scalar_one_or_none()
        if not raw:
            raise HTTPException(409, "source no longer available")
        event = InboundEvent.model_validate_json(raw)
        return record, owner(ref, event.principal)

    def action_event(ref, key, goal):
        if not key or len(key) > 128:
            raise HTTPException(400, "Idempotency-Key (1-128 characters) required")
        event_id = stable_id("admin_action", ref.key, key)
        with core.db.engine.connect() as c:
            old = c.execute(select(inbox.c.event_json).where(inbox.c.id == event_id)).scalar_one_or_none()
        if old:
            parsed = InboundEvent.model_validate_json(old)
            if parsed.text != goal:
                raise HTTPException(409, "idempotency key reused for a different request")
            return parsed
        event = InboundEvent(
            event_id=event_id,
            event_type="message.created",
            conversation=ref,
            principal=owner(ref).principal,
            platform_message_id="admin:" + key,
            occurred_at=core.clock.now(),
            received_at=core.clock.now(),
            segments=(TextSegment(text=goal),),
            trace_id=str(uuid4()),
        )
        event = core.inbox.accept(event)[0]
        core.inbox.finish(event_id)
        return event

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
                    "online": getattr(adapter, "account_online", None),
                    "context_gap": getattr(adapter, "context_gap", False),
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
            if body.mode is not None:
                await core.set_mode(ref, body.mode, owner(ref), expected_version=body.expected_version)
            if body.persona is not None:
                core.personas.set_override(cid, body.persona, authorized=True)
                core.save_state(cid, mode=core.state(cid)["mode"], expected_version=body.expected_version)
        except (ValueError, PermissionError) as exc:
            raise HTTPException(422, str(exc)) from exc
        return core.state(cid)

    @router.get("/api/v1/conversations/{cid}/decisions")
    def recent_decisions(cid: str):
        require_permission("history.read", "conversation:" + cid)
        conversation(cid)
        with core.db.engine.connect() as c:
            return [
                json.loads(raw)
                for raw in c.execute(
                    select(decisions.c.decision_json)
                    .where(decisions.c.conversation_id == cid)
                    .order_by(decisions.c.created_at.desc())
                    .limit(100)
                ).scalars()
            ]

    @router.post("/api/v1/tasks")
    def create_task(body: TaskRequest, idempotency_key: str = Header(default="")):
        if not core.settings.tasks_enabled:
            raise HTTPException(409, "tasks disabled")
        ref = conversation(body.conversation_id)
        event = action_event(ref, idempotency_key, body.goal)
        result = core.submit_turn(event, background=True, text=body.goal, invited=True, access=owner(ref))
        return {"public_id": result["public_id"], "state": result["state"]}

    @router.get("/api/v1/tasks")
    def list_tasks(conversation_id: str):
        require_permission("task.read", "conversation:" + conversation_id)
        conversation(conversation_id)
        with core.db.engine.connect() as c:
            return [
                dict(r._mapping)
                for r in c.execute(
                    select(tasks.c.public_id, tasks.c.state, tasks.c.created_at)
                    .where(tasks.c.conversation == conversation_id)
                    .order_by(tasks.c.created_at.desc())
                    .limit(100)
                )
            ]

    @router.get("/api/v1/tasks/{public_id}")
    def get_task(public_id: str):
        row = task_row(public_id)
        result = core.tasks.get(public_id, owner(conversation(row["conversation"])))
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
        row = task_row(public_id)
        return core.tasks.cancel(public_id, owner(conversation(row["conversation"])))

    @router.post("/api/v1/tasks/{public_id}/reconcile")
    def reconcile_task(public_id: str, body: ReconcileRequest):
        row = task_row(public_id)
        return core.tasks.reconcile(
            public_id, owner(conversation(row["conversation"])), body.run_id,
            body.result.model_dump() if body.result else None,
        )

    @router.post("/api/v1/tasks/{public_id}/inputs")
    def task_input(public_id: str):
        task_row(public_id)
        raise HTTPException(409, "configured harness does not support additional input")

    @router.post("/api/v1/tasks/{public_id}/retry")
    def retry_task(public_id: str, idempotency_key: str = Header(default="")):
        row = task_row(public_id)
        ref = conversation(row["conversation"])
        event = action_event(ref, idempotency_key, row["goal"])
        context = core.context_for(event, owner(ref), invited=True)
        result = core.tasks.retry(public_id, event, owner(ref), context=context)
        return {"public_id": result["public_id"], "state": result["state"]}

    @router.get("/api/v1/tasks/{public_id}/events")
    async def task_events(public_id: str, request: Request, last_event_id: str = Header(default="0")):
        row = task_row(public_id)
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

    @router.get("/api/v1/memories")
    def memories(conversation_id: str, status: str | None = None):
        require_permission("memory.read", "conversation:" + conversation_id)
        conversation(conversation_id)
        with core.db.engine.connect() as c:
            query = select(social_records.c.record_json).where(
                social_records.c.conversation_id == conversation_id
            )
            if status:
                query = query.where(social_records.c.status == status)
            return [json.loads(raw) for raw in c.execute(query.limit(100)).scalars()]

    @router.patch("/api/v1/memories/{memory_id}")
    def correct_memory(memory_id: str, body: MemoryPatch):
        record, access = memory_row(memory_id)
        with core.db.engine.connect() as c:
            raw = c.execute(
                select(inbox.c.event_json).where(
                    inbox.c.id == body.source_event_id, inbox.c.conversation_id == access.conversation.key
                )
            ).scalar_one_or_none()
        if not raw:
            raise HTTPException(422, "correction needs a new visible source event")
        source = InboundEvent.model_validate_json(raw)
        replacement = core.record(source, body.text, record.predicate)
        return core.social.correct(access, memory_id, replacement).model_dump(mode="json")

    @router.delete("/api/v1/memories/{memory_id}")
    def delete_memory(memory_id: str):
        _, access = memory_row(memory_id)
        core.social.forget(access, memory_id)
        return {"deleted": True}

    @router.get("/api/v1/jargon")
    def jargon(conversation_id: str):
        return [r for r in memories(conversation_id) if r["predicate"].startswith("jargon:")]

    @router.patch("/api/v1/jargon/{memory_id}")
    def review_jargon(memory_id: str, publish: bool = False):
        record, access = memory_row(memory_id)
        if not record.predicate.startswith("jargon:"):
            raise HTTPException(422, "not a jargon record")
        if not publish:
            core.social.forget(access, memory_id)
            return {"disabled": True}
        entry = json.loads(record.value_json)
        verified = core.social.jargon(
            access, entry["term"], entry["sense_id"], entry["definition"], record.source_event_ids
        )
        if verified.status != "active":
            raise HTTPException(409, "insufficient independent evidence")
        return core.social.commit(access, memory_id, authorized=True).model_dump(mode="json")

    @router.get("/api/v1/config")
    def config_view():
        encoded = core.settings.model_dump_json()
        return {
            "config": json.loads(encoded),
            "version": hashlib.sha256(encoded.encode()).hexdigest(),
            "model_key_configured": bool(os.environ.get(core.settings.model.api_key_env)),
        }

    @router.post("/api/v1/config/validate")
    def validate_config(body: ConfigRequest):
        return {"valid": True, "restart_required": body.config != core.settings}

    @router.get("/api/v1/config/versions")
    def config_versions():
        directory = Path(core.db.path).parent / "config-versions"
        return sorted(p.stem for p in directory.glob("*.json") if re.fullmatch(r"[a-f0-9]{64}", p.stem))

    @router.get("/api/v1/config/versions/{version}")
    def config_version(version: str):
        if not re.fullmatch(r"[a-f0-9]{64}", version):
            raise HTTPException(400, "invalid config version")
        path = Path(core.db.path).parent / "config-versions" / (version + ".json")
        if not path.is_file():
            raise HTTPException(404, "config version unavailable")
        return Settings.model_validate_json(path.read_text()).model_dump(mode="json")

    @router.post("/api/v1/config/apply")
    async def apply_config(body: ConfigRequest):
        if body.expected_version != config_view()["version"]:
            raise HTTPException(409, "config version changed")
        # Connection/credential/process changes need an explicit restart, never a half-applied runtime.
        current = core.settings.model_dump()
        candidate = body.config.model_dump()
        if candidate["model"] != current["model"] and (
            core.settings.interactive_execution_enabled or core.settings.tasks_enabled
        ):
            raise HTTPException(
                409, "model changes require restarting Core and Worker with matching startup configuration"
            )
        mutable = {"model", "send_budget_per_minute"}
        if any(candidate[k] != current[k] for k in current if k not in mutable):
            raise HTTPException(409, "connection, identity, judge or isolation changes require restart")
        directory = Path(core.db.path).parent / "config-versions"
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        previous = directory / (config_view()["version"] + ".json")
        if not previous.exists():
            with previous.open("x") as saved:
                os.fchmod(saved.fileno(), 0o600)
                saved.write(core.settings.model_dump_json(indent=2))
        version = hashlib.sha256(body.config.model_dump_json().encode()).hexdigest()
        destination = directory / (version + ".json")
        temporary = destination.with_suffix(".tmp")
        temporary.write_text(body.config.model_dump_json(indent=2))
        os.chmod(temporary, 0o600)
        temporary.replace(destination)
        active = Path(core.db.path).parent / "runtime-config.json"
        active_tmp = active.with_suffix(".tmp")
        active_tmp.write_text(json.dumps({k: candidate[k] for k in mutable}, ensure_ascii=False))
        os.chmod(active_tmp, 0o600)
        active_tmp.replace(active)
        core.settings = body.config
        core.audit("config.apply", version, "admin_api:owner", "validated model/budget config")
        return {"version": version, "applied": True}

    @router.get("/api/v1/artifacts/{task_id}/{digest}/download")
    def artifact(task_id: str, digest: str):
        row = task_row(task_id)
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
