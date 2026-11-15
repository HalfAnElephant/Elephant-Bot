"""Durable timers submit ordinary task trees under their original user's authority."""
import asyncio
import hashlib
import json
import re
from datetime import UTC, datetime, timedelta
from uuid import uuid4
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import croniter
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import insert, select, update

from qqbot.domain import InboundEvent, stable_id
from qqbot.domain.agents import GoalSpec
from qqbot.domain.messages import TextSegment
from qqbot.storage.agent_schema import dialogue_inputs, dialogues
from qqbot.storage.schedule_schema import schedule_fires, schedules
from qqbot.storage.schema import inbox
from qqbot.storage.task_schema import tasks


class ScheduleSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    goal: str = Field(min_length=1, max_length=16000)
    cron: str | None = Field(default=None, max_length=128)
    run_at: AwareDatetime | None = None
    timezone: str = Field(default="Asia/Shanghai", max_length=128)
    success_checks: tuple[str, ...] = ()
    requested_outputs: tuple[str, ...] = ()
    workspace_id: str | None = Field(default=None, max_length=256)
    enabled: bool = True

    @model_validator(mode="after")
    def valid_schedule(self):
        if (self.cron is None) == (self.run_at is None):
            raise ValueError("provide exactly one of cron or run_at")
        try:
            zone = ZoneInfo(self.timezone)
        except ZoneInfoNotFoundError as error:
            raise ValueError("unknown IANA timezone") from error
        if self.cron is not None:
            if len(self.cron.split()) != 5 or re.search(r"\b[HR](?:\b|\()", self.cron, re.IGNORECASE) or not croniter.is_valid(self.cron):
                raise ValueError("cron must have five valid minute/hour/day/month/weekday fields")
            croniter(self.cron, datetime.now(zone), max_years_between_matches=8).get_next(datetime)
        GoalSpec.model_validate_json(json.dumps(self.goal_spec()))
        from qqbot.harness.artifacts import artifact_path
        for path in self.requested_outputs:
            artifact_path(path)
        return self

    def goal_spec(self):
        child = {"mode": "work", "goal": self.goal, "delegation_key": "primary",
                 "success_checks": self.success_checks, "requested_outputs": self.requested_outputs}
        return {"goal": self.goal, "success_checks": self.success_checks,
                "requested_outputs": self.requested_outputs, "children": [child]}

    def next_after(self, moment):
        if self.run_at is not None:
            return self.run_at.astimezone(UTC)
        return croniter(self.cron, moment.astimezone(ZoneInfo(self.timezone)),
                        max_years_between_matches=8).get_next(datetime).astimezone(UTC)


class ScheduleService:
    def __init__(self, core):
        self.core, self.db, self.clock = core, core.db, core.clock

    def _identity(self, event, *, write=False):
        owner = self.core.sessions.owner(event)
        if not owner or not self.core.allowed(event):
            raise PermissionError("schedule requires a verified active owner")
        access = self.core.access(event)
        self.core.permissions.require(access, "task.create" if write else "task.read",
                                      "conversation:" + event.conversation.key)
        return owner

    def _owned(self, conn, event, schedule_id, *, write=False):
        owner = self._identity(event, write=write)
        row = conn.execute(select(schedules).where(schedules.c.id == schedule_id,
            schedules.c.owner_id == owner, schedules.c.deleted_at.is_(None))).mappings().first()
        if row is None:
            raise LookupError("schedule unavailable")
        if event.principal.identity_namespace != "admin_api":
            workspace_id = self.core.sessions.workspace_for(event, conn)
            schedule_workspace = conn.execute(select(dialogues.c.workspace_id).where(
                dialogues.c.id == row["dialogue_id"])).scalar_one()
            if (row["principal"] != event.principal.key or row["conversation"] != event.conversation.key
                    or schedule_workspace != workspace_id):
                raise PermissionError("schedule belongs to another authenticated scope")
        self.core.permissions.require(self.core.access(event), "task.create" if write else "task.read",
                                      "conversation:" + row["conversation"])
        return row

    @staticmethod
    def _view(row):
        return {key: row[key] for key in ("id", "revision", "next_fire_at", "last_fire_at", "last_task_id",
            "last_error", "created_at", "updated_at")} | {"spec": json.loads(row["spec_json"]), "enabled": bool(row["enabled"])}

    def _capture_sources(self, conn, event, workspace_id):
        stored = conn.execute(select(dialogue_inputs.c.context_json).join(dialogues,
            dialogues.c.id == dialogue_inputs.c.dialogue_id).where(dialogue_inputs.c.id == event.event_id,
            dialogues.c.workspace_id == workspace_id, dialogues.c.conversation_id == event.conversation.key)).scalar_one_or_none()
        context = json.loads(stored) if stored else self.core.context_for(event, self.core.access(event),
            workspace_id=workspace_id, conn=conn)
        snapshot = {key: context.get(key, []) for key in ("source_manifest", "images", "files", "task_sources")}
        for kind in ("images", "files"):
            snapshot[kind] = [{key: item[key] for key in ("source", "message_id", "url", "filename", "mime_type",
                "size", "attachment_ref", "sha256") if key in item} for item in snapshot[kind]]
        snapshot.update(scope=event.conversation.key, principal=event.principal.key, workspace_id=workspace_id)
        self._validate_sources(conn, event, snapshot)
        return snapshot

    def _validate_sources(self, conn, event, context):
        # Source checks use this transaction so newly accepted HOST input is visible too.
        if getattr(self.core, "validate_context", None) is None:
            if context.get("source_manifest") or context.get("images") or context.get("files"):
                raise PermissionError("source receipts require a production context validator")
            return
        if (context.get("scope") != event.conversation.key or context.get("principal") != event.principal.key
                or not context.get("source_manifest")):
            raise PermissionError("schedule original source snapshot is unavailable")
        access = self.core.access(event)
        workspace_id = self.core.sessions.validate_workspace(event, context["workspace_id"])
        self.core.permissions.require(access, "history.read", "conversation:" + event.conversation.key)
        source_ids = {source["id"] for source in context["source_manifest"]}
        if event.event_id not in source_ids:
            raise PermissionError("schedule original request is missing from source receipts")
        if any(item.get("source") not in source_ids for kind in ("images", "files") for item in context.get(kind, ())):
            raise PermissionError("schedule attachment has no original source receipt")
        if not self.core.knowledge.sources_valid(event, source_ids, workspace_id=workspace_id, conn=conn):
            raise PermissionError("schedule original sources have been revoked")
        for source in context["source_manifest"]:
            if source["kind"] != "inbox":
                raise PermissionError("unsupported schedule source receipt")
            raw = conn.execute(select(inbox.c.event_json).where(inbox.c.id == source["id"],
                inbox.c.conversation_id == event.conversation.key)).scalar_one_or_none()
            if raw is None or hashlib.sha256(raw.encode()).hexdigest() != source["digest"]:
                raise PermissionError("schedule original source is no longer available")
            self.core.permissions.require(access, "history.source.read", "event:" + source["id"])
            original = InboundEvent.model_validate_json(raw)
            original_workspace = self.core.sessions.workspace_for(original, conn)
            if original_workspace != workspace_id and not self.core.knowledge.sources_valid(
                    event, [source["id"]], workspace_id=original_workspace, conn=conn):
                raise PermissionError("schedule source was revoked in its original project")
            if conn.execute(select(inbox.c.id).where(inbox.c.conversation_id == event.conversation.key,
                    inbox.c.event_type == "message.recalled", inbox.c.message_id == original.platform_message_id)).first():
                raise PermissionError("schedule original source was recalled")
        for task_id in context.get("task_sources", ()):
            task = conn.execute(select(tasks).where(tasks.c.id == task_id,
                tasks.c.requester == event.principal.key, tasks.c.conversation == event.conversation.key)).mappings().first()
            if task is None or not self.core.validate_context(access, json.loads(task["context_json"])):
                raise PermissionError("schedule task source is no longer authorized")
            self.core.permissions.require(access, "task.read", "task:" + task_id)

    @staticmethod
    def _merge_sources(original, current):
        merged = dict(original)
        manifests = {}
        for source in (*original.get("source_manifest", ()), *current.get("source_manifest", ())):
            key = (source["kind"], source["id"])
            if key in manifests and manifests[key] != source:
                raise PermissionError("schedule source receipt changed")
            manifests[key] = source
        merged["source_manifest"] = list(manifests.values())
        for kind in ("images", "files"):
            merged[kind] = list({json.dumps(item, sort_keys=True, ensure_ascii=False): item for item in (
                *original.get(kind, ()), *current.get(kind, ()))}.values())
        merged["task_sources"] = sorted(set(original.get("task_sources", ())) | set(current.get("task_sources", ())))
        return merged

    def create(self, event, spec, *, idempotency_key):
        spec = ScheduleSpec.model_validate(spec)
        owner = self._identity(event, write=True)
        if not idempotency_key or len(idempotency_key) > 200:
            raise ValueError("schedule creation requires an idempotency key of at most 200 characters")
        encoded = spec.model_dump_json()
        now = self.clock.now().astimezone(UTC)
        with self.db.transaction() as conn:
            old = conn.execute(select(schedules).where(schedules.c.owner_id == owner,
                schedules.c.principal == event.principal.key, schedules.c.conversation == event.conversation.key,
                schedules.c.creation_key == idempotency_key)).mappings().first()
            if old:
                if old["creation_json"] != encoded:
                    raise ValueError("schedule creation key reused with different content")
                if old["deleted_at"]:
                    raise ValueError("schedule was deleted")
                return self._view(self._owned(conn, event, old["id"], write=True))
            workspace_id = self.core.sessions.validate_workspace(event,
                spec.workspace_id or self.core.sessions.workspace_for(event, conn))
            resolved_spec = spec.model_copy(update={"workspace_id": workspace_id})
            if event.principal.identity_namespace == "admin_api":
                event, _ = self.core.inbox.accept(event, conn=conn)
                conn.execute(update(inbox).where(inbox.c.id == event.event_id).values(state="DONE"))
            dialogue = conn.execute(select(dialogues).join(dialogue_inputs,
                dialogue_inputs.c.dialogue_id == dialogues.c.id).where(dialogue_inputs.c.id == event.event_id,
                dialogues.c.owner_id == owner, dialogues.c.conversation_id == event.conversation.key,
                dialogues.c.workspace_id == workspace_id)).mappings().first()
            if dialogue is None:
                dialogue = self.core.sessions.dialogue(conn, event, workspace_id=workspace_id)
            source_context = self._capture_sources(conn, event, workspace_id)
            schedule_id = str(uuid4())
            conn.execute(insert(schedules).values(id=schedule_id, owner_id=owner, principal=event.principal.key,
                conversation=event.conversation.key, creation_key=idempotency_key, creation_json=encoded,
                event_json=event.model_dump_json(), dialogue_id=dialogue["id"], spec_json=resolved_spec.model_dump_json(),
                source_context_json=json.dumps(source_context, ensure_ascii=False), revision=1, enabled=int(spec.enabled), next_fire_at=spec.next_after(now).isoformat(),
                created_at=now.isoformat(), updated_at=now.isoformat()))
            return self._view(conn.execute(select(schedules).where(schedules.c.id == schedule_id)).mappings().one())

    def get(self, event, schedule_id):
        with self.db.engine.connect() as conn:
            return self._view(self._owned(conn, event, schedule_id))

    def list(self, event):
        owner = self._identity(event)
        query = select(schedules).where(schedules.c.owner_id == owner, schedules.c.deleted_at.is_(None))
        with self.db.engine.connect() as conn:
            if event.principal.identity_namespace != "admin_api":
                workspace_id = self.core.sessions.workspace_for(event, conn)
                query = query.join(dialogues, dialogues.c.id == schedules.c.dialogue_id).where(
                    schedules.c.principal == event.principal.key, schedules.c.conversation == event.conversation.key,
                    dialogues.c.workspace_id == workspace_id)
            return [self._view(row) for row in conn.execute(query.order_by(schedules.c.created_at)).mappings()
                    if self.core.permissions.check(self.core.access(event), "task.read", "conversation:" + row["conversation"]).allowed]

    def update(self, event, schedule_id, changes, *, expected_revision):
        if not isinstance(changes, dict):
            raise TypeError("schedule changes must be an object")
        with self.db.transaction() as conn:
            row = self._owned(conn, event, schedule_id, write=True)
            if row["revision"] != expected_revision:
                raise ValueError("schedule revision conflict")
            spec = ScheduleSpec.model_validate(json.loads(row["spec_json"]) | changes)
            if spec.workspace_id != json.loads(row["spec_json"]).get("workspace_id"):
                raise ValueError("create a new schedule to change its workspace")
            now = self.clock.now().astimezone(UTC)
            timing_changed = bool(set(changes) & {"cron", "run_at", "timezone"}) or (spec.enabled and not row["enabled"])
            next_fire = spec.next_after(now).isoformat() if timing_changed else row["next_fire_at"]
            if spec.run_at is not None and row["last_fire_at"] == next_fire:
                next_fire = None
            source_context = json.loads(row["source_context_json"])
            if set(changes) & {"goal", "success_checks", "requested_outputs"}:
                stored_source = conn.execute(select(inbox.c.id).where(inbox.c.id == event.event_id,
                    inbox.c.conversation_id == row["conversation"])).first()
                if (stored_source and event.principal.key == row["principal"]
                        and self.core.sessions.workspace_for(event, conn) == spec.workspace_id):
                    current_sources = self._capture_sources(conn, event, spec.workspace_id)
                    source_context = self._merge_sources(source_context, current_sources)
            conn.execute(update(schedules).where(schedules.c.id == schedule_id).values(
                source_context_json=json.dumps(source_context, ensure_ascii=False),
                spec_json=spec.model_dump_json(), enabled=int(spec.enabled and next_fire is not None), revision=row["revision"] + 1,
                next_fire_at=next_fire, last_error=None, updated_at=now.isoformat()))
            return self._view(conn.execute(select(schedules).where(schedules.c.id == schedule_id)).mappings().one())

    def delete(self, event, schedule_id, *, expected_revision):
        with self.db.transaction() as conn:
            row = self._owned(conn, event, schedule_id)
            self.core.permissions.require(self.core.access(event), "task.cancel", "conversation:" + row["conversation"])
            if row["revision"] != expected_revision:
                raise ValueError("schedule revision conflict")
            conn.execute(update(schedules).where(schedules.c.id == schedule_id).values(
                enabled=0, deleted_at=self.clock.now().isoformat(), updated_at=self.clock.now().isoformat(),
                revision=row["revision"] + 1))
        return {"id": schedule_id, "deleted": True}

    def invoke(self, event, operation, arguments, *, idempotency_key=None):
        fields = {"create": set(ScheduleSpec.model_fields), "list": set(), "get": {"schedule_id"},
                  "update": {"schedule_id", "changes", "expected_revision"},
                  "delete": {"schedule_id", "expected_revision"}, "cancel": {"schedule_id", "expected_revision"}}
        if not isinstance(operation, str) or operation not in fields:
            raise ValueError("unknown schedule operation")
        if not isinstance(arguments, dict) or set(arguments) - fields[operation]:
            raise ValueError("unexpected schedule arguments")
        required = {"goal"} if operation == "create" else fields[operation]
        if required - set(arguments):
            raise ValueError("missing required schedule arguments")
        if operation == "create":
            return self.create(event, arguments, idempotency_key=idempotency_key)
        if operation == "list":
            return self.list(event)
        schedule_id = arguments["schedule_id"]
        if not isinstance(schedule_id, str) or not schedule_id or len(schedule_id) > 256:
            raise ValueError("invalid schedule ID")
        if operation == "get":
            return self.get(event, schedule_id)
        revision = arguments["expected_revision"]
        if type(revision) is not int or revision < 1:
            raise ValueError("expected_revision must be a positive integer")
        if operation == "update":
            if not isinstance(arguments["changes"], dict):
                raise ValueError("schedule changes must be an object")
            return self.update(event, schedule_id, arguments["changes"], expected_revision=revision)
        return self.delete(event, schedule_id, expected_revision=revision)

    def _fire(self, schedule_id):
        # Compare the definition revision again when atomically recording the firing and goal.
        with self.db.engine.connect() as conn:
            row = conn.execute(select(schedules).where(schedules.c.id == schedule_id)).mappings().one()
        now = self.clock.now().astimezone(UTC)
        if not row["enabled"] or row["deleted_at"] or not row["next_fire_at"] or row["next_fire_at"] > now.isoformat():
            return
        event = InboundEvent.model_validate_json(row["event_json"])
        self._identity(event, write=True)
        spec = ScheduleSpec.model_validate_json(row["spec_json"])
        with self.db.engine.connect() as conn:
            dialogue = conn.execute(select(dialogues).where(dialogues.c.id == row["dialogue_id"])).mappings().one()
        workspace_id = spec.workspace_id or dialogue.get("workspace_id")
        due = datetime.fromisoformat(row["next_fire_at"])
        if spec.cron:
            latest = croniter(spec.cron, (now + timedelta(microseconds=1)).astimezone(ZoneInfo(spec.timezone)),
                              max_years_between_matches=8).get_prev(datetime).astimezone(UTC)
            due = max(due, latest)
        fire_at = due.isoformat()
        fire_id = stable_id("schedule-fire", schedule_id, fire_at)
        trigger = event.model_copy(update={"event_id": fire_id, "platform_message_id": fire_id,
            "occurred_at": due, "received_at": now,
            "segments": (TextSegment(text=spec.goal),),
            "reply_to": None, "reply_context": None})
        with self.db.transaction() as conn:
            current = conn.execute(select(schedules).where(schedules.c.id == schedule_id)).mappings().one()
            if (current["revision"] != row["revision"] or current["next_fire_at"] != row["next_fire_at"]
                    or not current["enabled"] or current["deleted_at"]):
                return
            self._identity(event, write=True)
            dialogue = conn.execute(select(dialogues).where(dialogues.c.id == row["dialogue_id"],
                dialogues.c.owner_id == row["owner_id"])).mappings().one()
            old = conn.execute(select(schedule_fires).where(schedule_fires.c.id == fire_id)).mappings().first()
            if old:
                task_id = old["task_id"]
            else:
                source_context = json.loads(row["source_context_json"])
                if source_context.get("workspace_id") != workspace_id:
                    raise PermissionError("schedule source project changed")
                self._validate_sources(conn, event, source_context)
                trigger, context = self.core.structured_goal_context(trigger, spec.goal_spec(), workspace_id, conn)
                source_ids = {source["id"] for source in source_context.get("source_manifest", ())} | {fire_id}
                manifests = [source for source in context.get("source_manifest", ()) if source["id"] in source_ids]
                manifests.extend(source_context.get("source_manifest", ()))
                context["source_manifest"] = list({(source["kind"], source["id"]): source for source in manifests}.values())
                for kind in ("images", "files"):
                    attachments = list(source_context.get(kind, ()))
                    attachments.extend(item for item in context.get(kind, ()) if item["source"] in source_ids)
                    context[kind] = list({json.dumps(item, sort_keys=True, ensure_ascii=False): item for item in attachments}.values())
                context["task_sources"] = sorted(set(context.get("task_sources", ())) | set(source_context.get("task_sources", ())))
                context["schedule_fire_id"] = fire_id
                result = self.core.tasks.graph.submit(conn, dialogue, trigger, self.core.access(event),
                    spec.goal_spec(), fire_id, self.core.sessions.binding(), context)
                task_id = result["task_id"]
                conn.execute(insert(schedule_fires).values(id=fire_id, schedule_id=schedule_id,
                    scheduled_fire_at=fire_at, schedule_revision=row["revision"], task_id=task_id,
                    created_at=now.isoformat()))
            conn.execute(update(schedules).where(schedules.c.id == schedule_id).values(
                last_fire_at=fire_at, last_task_id=task_id, last_error=None,
                next_fire_at=spec.next_after(now).isoformat() if spec.cron else None,
                enabled=int(bool(spec.cron)), updated_at=now.isoformat()))

    def once(self):
        now = self.clock.now().astimezone(UTC).isoformat()
        with self.db.engine.connect() as conn:
            due = list(conn.execute(select(schedules.c.id, schedules.c.revision).where(schedules.c.enabled == 1,
                schedules.c.deleted_at.is_(None), schedules.c.next_fire_at <= now)))
        for schedule_id, revision in due:
            try:
                self._fire(schedule_id)
            except (PermissionError, LookupError, ValueError) as error:
                with self.db.transaction() as conn:
                    conn.execute(update(schedules).where(schedules.c.id == schedule_id, schedules.c.revision == revision).values(
                        enabled=0, last_error=type(error).__name__, updated_at=now))
        return True

    async def step(self):
        return await asyncio.to_thread(self.once)
