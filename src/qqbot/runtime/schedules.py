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
from qqbot.domain.messages import StructuredSegment, TextSegment
from qqbot.permissions import PermissionRule, PermissionSubject
from qqbot.runtime.mail_filters import MailTrigger
from qqbot.runtime.mail_source import MailBinding, SyntheticMailSource
from qqbot.storage.agent_schema import dialogue_inputs, dialogues
from qqbot.storage.schedule_schema import schedule_fires, schedules
from qqbot.storage.schema import inbox
from qqbot.storage.task_schema import tasks


class ScheduleSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    goal: str = Field(min_length=1, max_length=16000)
    cron: str | None = Field(default=None, max_length=128)
    run_at: AwareDatetime | None = None
    mail: MailTrigger | None = Field(default=None,
        description="Mail source and deterministic filters. Enabling requires a configured account in this scope.")
    mail_permissions: tuple[PermissionRule, ...] = Field(default=(), max_length=32,
        description="Mail-task action/resource grants explicitly authorized by the user. Default denies business tools. "
        "For local output allow workspace.file.* on workspace:* and file:outputs/*. "
        "For sending also allow mail.send on mail-account:<account UUID> and each mail-recipient:<address>. "
        "mail-recipient:$sender permits only the triggering message's original From address.")
    mail_replay_from_uid: int | None = Field(default=None, ge=1,
        description="Explicit historical replay start UID. Omit to process only new mail after activation.")
    timezone: str = Field(default="Asia/Shanghai", max_length=128)
    success_checks: tuple[str, ...] = ()
    requested_outputs: tuple[str, ...] = ()
    requested_actions: tuple[str, ...] = Field(default=(), max_length=32,
        description="External actions explicitly authorized for this goal, such as mail.send for a timer. "
        "Mail triggers additionally enforce mail_permissions.")
    workspace_id: str | None = Field(default=None, max_length=256)
    enabled: bool = True

    @model_validator(mode="after")
    def valid_schedule(self):
        if sum(trigger is not None for trigger in (self.cron, self.run_at, self.mail)) != 1:
            raise ValueError("provide exactly one of cron, run_at, or mail")
        if self.mail is None and (self.mail_permissions or self.mail_replay_from_uid is not None):
            raise ValueError("mail permissions and replay require a mail trigger")
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
        return {"goal": self.goal, "requested_actions": self.requested_actions, "success_checks": self.success_checks,
                "requested_outputs": self.requested_outputs, "children": [child]}

    def next_after(self, moment):
        if self.mail is not None:
            return None
        if self.run_at is not None:
            return self.run_at.astimezone(UTC)
        return croniter(self.cron, moment.astimezone(ZoneInfo(self.timezone)),
                        max_years_between_matches=8).get_next(datetime).astimezone(UTC)


class ScheduleService:
    def __init__(self, core, *, synthetic_mail: SyntheticMailSource | None = None):
        self.core, self.db, self.clock = core, core.db, core.clock
        self.synthetic_mail = synthetic_mail

    @staticmethod
    def _mail_binding(event, owner, workspace, mail):
        return MailBinding(owner, event.principal.key, event.conversation.key, workspace, mail.account_id, mail.folder)

    def _require_mail(self, event, owner, workspace, spec):
        if spec.mail and spec.enabled:
            if self.synthetic_mail is None:
                runtime = getattr(self.core, "mail", None)
                if runtime is None:
                    raise ValueError("mail trigger drafts require enabled=false; no mailbox is connected")
                try:
                    return runtime.accounts.resolve(event, workspace, spec.mail.account_id, require_ready=True)
                except LookupError:
                    raise ValueError("mail triggers require enabled=false until their account is configured") from None
            elif not self.synthetic_mail.allows(self._mail_binding(event, owner, workspace, spec.mail)):
                raise PermissionError("mail account is not available in this authenticated scope")


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
        activation = None
        if spec.mail and spec.enabled and self.synthetic_mail is None and getattr(self.core, "mail", None):
            with self.db.engine.connect() as conn:
                existing = conn.execute(select(schedules.c.id).where(schedules.c.owner_id == owner,
                    schedules.c.principal == event.principal.key, schedules.c.conversation == event.conversation.key,
                    schedules.c.creation_key == idempotency_key)).first()
                workspace = self.core.sessions.validate_workspace(event,
                    spec.workspace_id or self.core.sessions.workspace_for(event, conn))
                if existing is None and event.principal.identity_namespace != "admin_api":
                    self._capture_sources(conn, event, workspace)
            if existing is None:
                self._require_mail(event, owner, workspace, spec)
                activation = self.core.mail.triggers.prepare_activation(event, workspace, spec,
                    replay_from_uid=spec.mail_replay_from_uid)
        with self.db.transaction() as conn:
            old = conn.execute(select(schedules).where(schedules.c.owner_id == owner,
                schedules.c.principal == event.principal.key, schedules.c.conversation == event.conversation.key,
                schedules.c.creation_key == idempotency_key)).mappings().first()
            if old:
                if ScheduleSpec.model_validate_json(old["creation_json"]) != spec:
                    raise ValueError("schedule creation key reused with different content")
                if old["deleted_at"]:
                    raise ValueError("schedule was deleted")
                current = self._owned(conn, event, old["id"], write=True)
                current_spec = ScheduleSpec.model_validate_json(current["spec_json"])
                if current["enabled"]:
                    self._require_mail(event, owner, current_spec.workspace_id, current_spec)
                return self._view(current)
            workspace_id = self.core.sessions.validate_workspace(event,
                spec.workspace_id or self.core.sessions.workspace_for(event, conn))
            self._require_mail(event, owner, workspace_id, spec)
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
            next_fire = spec.next_after(now)
            watermark = self.synthetic_mail.watermark() if spec.mail and spec.enabled and self.synthetic_mail else None
            conn.execute(insert(schedules).values(id=schedule_id, owner_id=owner, principal=event.principal.key,
                conversation=event.conversation.key, creation_key=idempotency_key, creation_json=encoded,
                event_json=event.model_dump_json(), dialogue_id=dialogue["id"], spec_json=resolved_spec.model_dump_json(),
                source_context_json=json.dumps(source_context, ensure_ascii=False), revision=1, enabled=int(spec.enabled), next_fire_at=next_fire.isoformat() if next_fire is not None else None,
                synthetic_mail_last_uid=watermark, synthetic_mail_activation_uid=watermark,
                created_at=now.isoformat(), updated_at=now.isoformat()))
            if spec.mail and spec.enabled and self.synthetic_mail is None:
                self.core.mail.triggers.activate(conn, schedule_id, event, resolved_spec, activation)
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

    @staticmethod
    def _updated_spec(row, changes):
        previous = ScheduleSpec.model_validate_json(row["spec_json"])
        spec = ScheduleSpec.model_validate(previous.model_dump()
            | {"enabled": bool(row["enabled"]), "mail_replay_from_uid": None} | changes)
        if (previous.mail is None) != (spec.mail is None):
            raise ValueError("create a new schedule to change between time and mail triggers")
        if previous.mail and (previous.mail.account_id, previous.mail.folder) != (spec.mail.account_id, spec.mail.folder):
            raise ValueError("create a new schedule to change its mail account or folder")
        if spec.workspace_id != previous.workspace_id:
            raise ValueError("create a new schedule to change its workspace")
        return spec

    def _update_identity(self, event, row, workspace_id):
        if event.principal.identity_namespace != "admin_api":
            return event
        self.core.permissions.require(PermissionSubject(adapter_instance_id="admin_api",
            identity_namespace="admin_api", user_id="owner"), "admin.patch", "http:/api/v1/schedules/" + row["id"])
        original = InboundEvent.model_validate_json(row["event_json"])
        if self._identity(original, write=True) != row["owner_id"]:
            raise PermissionError("schedule original owner changed")
        self.core.sessions.validate_workspace(original, workspace_id)
        return event.model_copy(update={"event_type": "host.schedule.updated",
            "principal": original.principal, "conversation": original.conversation})

    def update(self, event, schedule_id, changes, *, expected_revision):
        if not isinstance(changes, dict):
            raise TypeError("schedule changes must be an object")
        activation = None
        if self.synthetic_mail is None and getattr(self.core, "mail", None):
            with self.db.engine.connect() as conn:
                pending = self._owned(conn, event, schedule_id, write=True)
                if pending["revision"] != expected_revision:
                    raise ValueError("schedule revision conflict")
                spec = self._updated_spec(pending, changes)
                activation_event = self._update_identity(event, pending, spec.workspace_id) if spec.mail and spec.enabled else event
                if spec.mail and spec.enabled:
                    original = InboundEvent.model_validate_json(pending["event_json"])
                    self._validate_sources(conn, original, json.loads(pending["source_context_json"]))
            if spec.mail and spec.enabled and (not pending["enabled"]
                    or "mail_replay_from_uid" in changes and spec.mail_replay_from_uid is not None):
                self._require_mail(activation_event, pending["owner_id"], spec.workspace_id, spec)
                activation = self.core.mail.triggers.prepare_activation(activation_event, spec.workspace_id, spec,
                    replay_from_uid=spec.mail_replay_from_uid)
        request_event = event
        with self.db.transaction() as conn:
            row = self._owned(conn, event, schedule_id, write=True)
            if row["revision"] != expected_revision:
                raise ValueError("schedule revision conflict")
            spec = self._updated_spec(row, changes)
            source_change = bool(set(changes) & {"goal", "success_checks", "requested_outputs", "requested_actions", "mail", "mail_permissions", "mail_replay_from_uid"})
            if (spec.mail or "requested_actions" in changes) and (spec.enabled or source_change):
                event = self._update_identity(request_event, row, spec.workspace_id)
            self._require_mail(event, row["owner_id"], spec.workspace_id, spec)
            now = self.clock.now().astimezone(UTC)
            timing_changed = bool(set(changes) & {"cron", "run_at", "timezone"}) or (spec.enabled and not row["enabled"])
            fire = spec.next_after(now) if timing_changed else None
            next_fire = (fire.isoformat() if fire is not None else None) if timing_changed else row["next_fire_at"]
            if spec.run_at is not None and row["last_fire_at"] == next_fire:
                next_fire = None
            source_context = json.loads(row["source_context_json"])
            if source_change:
                admin_source = (request_event.principal.identity_namespace == "admin_api"
                    and event.principal.key == row["principal"] and event.conversation.key == row["conversation"])
                authorization_changed = bool(set(changes) & {"mail", "mail_permissions", "mail_replay_from_uid", "requested_actions"})
                if admin_source and (authorization_changed or spec.mail is not None):
                    event = event.model_copy(update={"segments": (*event.segments, StructuredSegment(
                        original_type="schedule_update", payload={"schedule_id": schedule_id,
                            "expected_revision": expected_revision,
                            "actor": request_event.principal.model_dump(),
                            "actor_conversation": request_event.conversation.model_dump(),
                            "spec_sha256": hashlib.sha256(spec.model_dump_json().encode()).hexdigest()}))})
                    event, _ = self.core.inbox.accept(event, conn=conn)
                    conn.execute(update(inbox).where(inbox.c.id == event.event_id).values(state="DONE"))
                    context = self.core.context_for(event, self.core.access(event),
                        workspace_id=spec.workspace_id, conn=conn)
                    conn.execute(insert(dialogue_inputs).values(id=event.event_id, dialogue_id=row["dialogue_id"],
                        owner_id=row["owner_id"], event_json=event.model_dump_json(),
                        context_json=json.dumps(context, ensure_ascii=False), state="HANDLED",
                        created_at=now.isoformat(), handled_at=now.isoformat()))
                stored_source = conn.execute(select(inbox.c.id).where(inbox.c.id == event.event_id,
                    inbox.c.conversation_id == row["conversation"])).first()
                if (stored_source and event.principal.key == row["principal"]
                        and self.core.sessions.workspace_for(event, conn) == spec.workspace_id):
                    current_sources = self._capture_sources(conn, event, spec.workspace_id)
                    source_context = self._merge_sources(source_context, current_sources)
                elif (authorization_changed or spec.mail is not None) and getattr(self.core, "validate_context", None) is not None:
                    raise PermissionError("schedule authorization updates require a verified user message in the original scope")
            watermark = None
            if spec.mail and spec.enabled and not row["enabled"] and self.synthetic_mail:
                watermark = self.synthetic_mail.watermark()
                if row["synthetic_mail_last_uid"] is not None:
                    watermark = max(watermark, row["synthetic_mail_last_uid"])
            conn.execute(update(schedules).where(schedules.c.id == schedule_id).values(
                source_context_json=json.dumps(source_context, ensure_ascii=False),
                spec_json=spec.model_dump_json(), enabled=int(spec.enabled and (spec.mail is not None or next_fire is not None)), revision=row["revision"] + 1,
                next_fire_at=next_fire, last_error=None, updated_at=now.isoformat(),
                synthetic_mail_last_uid=watermark if watermark is not None else row["synthetic_mail_last_uid"],
                synthetic_mail_activation_uid=(watermark if watermark is not None
                                               else row["synthetic_mail_activation_uid"])))
            if activation is not None:
                self.core.mail.triggers.activate(conn, schedule_id, event, spec, activation)
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
                task_id = self._submit_fire(conn, dialogue, event, trigger, spec, row, workspace_id, fire_id)
                conn.execute(insert(schedule_fires).values(id=fire_id, schedule_id=schedule_id,
                    scheduled_fire_at=fire_at, schedule_revision=row["revision"], task_id=task_id,
                    created_at=now.isoformat()))
            conn.execute(update(schedules).where(schedules.c.id == schedule_id).values(
                last_fire_at=fire_at, last_task_id=task_id, last_error=None,
                next_fire_at=spec.next_after(now).isoformat() if spec.cron else None,
                enabled=int(bool(spec.cron)), updated_at=now.isoformat()))

    def _submit_fire(self, conn, dialogue, event, trigger, spec, row, workspace_id, fire_id, *, mail_receipt_id=None):
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
        if mail_receipt_id is not None:
            context["mail_receipt_id"] = mail_receipt_id
        elif spec.mail is not None:
            context["synthetic_mail_schedule_id"] = row["id"]
        result = self.core.tasks.graph.submit(conn, dialogue, trigger, self.core.access(event),
            spec.goal_spec(), fire_id, self.core.sessions.binding(), context)
        return result["task_id"]

    def synthetic_fire_authorized(self, conn, event, context):
        from qqbot.storage.schedule_schema import synthetic_mail_receipts

        source = self.synthetic_mail
        if source is None:
            return False
        receipt = conn.execute(select(schedules, synthetic_mail_receipts.c.uid.label("fired_uid")).join(synthetic_mail_receipts,
            synthetic_mail_receipts.c.schedule_id == schedules.c.id).where(
            synthetic_mail_receipts.c.id == context.get("schedule_fire_id"),
            schedules.c.id == context.get("synthetic_mail_schedule_id"),
            schedules.c.enabled == 1, schedules.c.deleted_at.is_(None))).mappings().first()
        if (receipt is None or receipt["owner_id"] != self.core.sessions.owner(event)
                or receipt["synthetic_mail_activation_uid"] is None
                or receipt["fired_uid"] <= receipt["synthetic_mail_activation_uid"]):
            return False
        spec = ScheduleSpec.model_validate_json(receipt["spec_json"])
        return bool(spec.mail and source.allows(self._mail_binding(event, receipt["owner_id"],
            spec.workspace_id, spec.mail)))

    def scan_synthetic_mail(self):
        """Exercise trigger transactions using the explicitly host-injected fixture only."""
        from qqbot.storage.schedule_schema import synthetic_mail_receipts

        source = self.synthetic_mail
        if source is None or not source.enabled:
            return
        with self.db.engine.connect() as conn:
            rows = conn.execute(select(schedules).where(schedules.c.enabled == 1,
                schedules.c.deleted_at.is_(None), schedules.c.next_fire_at.is_(None))).mappings().all()
        messages = sorted(source.read(), key=lambda message: message.uid)
        for row in rows:
            spec = ScheduleSpec.model_validate_json(row["spec_json"])
            if spec.mail is None:
                continue
            event = InboundEvent.model_validate_json(row["event_json"])
            binding = self._mail_binding(event, row["owner_id"], spec.workspace_id, spec.mail)
            if not source.allows(binding):
                continue
            for message in messages:
                if row["synthetic_mail_last_uid"] is None or message.uid <= row["synthetic_mail_last_uid"]:
                    continue
                matched = spec.mail.matches(sender_address=message.sender_address, subject=message.subject,
                    body=message.body, body_complete=message.body_complete)
                now = self.clock.now().astimezone(UTC)
                fire_id = stable_id("synthetic-mail-fire", row["id"], str(message.uid))
                with self.db.transaction() as conn:
                    current = conn.execute(select(schedules).where(schedules.c.id == row["id"])).mappings().one()
                    if (current["revision"] != row["revision"] or not current["enabled"]
                            or current["deleted_at"] or not source.allows(binding)):
                        continue
                    if current["synthetic_mail_last_uid"] is None or message.uid <= current["synthetic_mail_last_uid"]:
                        continue
                    if self._identity(event, write=True) != binding.owner:
                        raise PermissionError("mail schedule owner changed")
                    conn.execute(update(schedules).where(schedules.c.id == row["id"]).values(
                        synthetic_mail_last_uid=message.uid))
                    if not matched:
                        continue
                    if conn.execute(select(synthetic_mail_receipts.c.id).where(
                        synthetic_mail_receipts.c.id == fire_id,
                    )).first():
                        continue
                    dialogue = conn.execute(select(dialogues).where(dialogues.c.id == row["dialogue_id"],
                        dialogues.c.owner_id == binding.owner)).mappings().one()
                    trigger = event.model_copy(update={"event_id": fire_id, "platform_message_id": fire_id,
                        "occurred_at": now, "received_at": now, "segments": (TextSegment(text=spec.goal),),
                        "reply_to": None, "reply_context": None})
                    task_id = self._submit_fire(conn, dialogue, event, trigger, spec, row, binding.workspace, fire_id)
                    conn.execute(insert(synthetic_mail_receipts).values(id=fire_id, schedule_id=row["id"],
                        uid=message.uid, task_id=task_id, created_at=now.isoformat()))
                    conn.execute(update(schedules).where(schedules.c.id == row["id"]).values(
                        last_fire_at=now.isoformat(), last_task_id=task_id, updated_at=now.isoformat()))

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
