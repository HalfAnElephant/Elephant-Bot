"""One host runtime: QQ ingress, persistent Leader turns and on-demand Workers."""
import asyncio
import hashlib
import json
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

import httpx
from sqlalchemy import insert, select, update

from qqbot.adapters.hermes_skills import HermesSkills
from qqbot.adapters.openviking import OpenVikingMemory
from qqbot.domain import (
    AccessContext,
    ConversationRef,
    InboundEvent,
    PrincipalRef,
    Surface,
    TextSegment,
    stable_id,
)
from qqbot.ingress.service import Inbox
from qqbot.jobs.service import TaskService
from qqbot.jobs.streaming import StreamDelivery
from qqbot.observability.supervision import LoopSupervisor
from qqbot.outbox.service import Outbox
from qqbot.permissions import PermissionService, PermissionSubject
from qqbot.storage.schema import audit_events, inbox, outbox, runtime_state
from qqbot.storage.task_schema import notifications, runs, tasks

from .control import HostTaskControls
from .leader import LeaderRuntime
from .sessions import SessionService
from .task_graph import TaskGraph

logger = logging.getLogger(__name__)
CONTROL_COMMANDS = {"/帮助", "/状态", "/任务", "/取消", "/补充", "/静默", "/恢复", "/模式", "/新对话", "/切换对话", "/对话"}


class Core:
    def __init__(self, settings, db, clock, adapters, model_client=None):
        self.settings, self.db, self.clock, self.adapters = settings, db, clock, adapters
        self.permissions = PermissionService(db, clock, settings.permissions)
        for adapter in adapters.values():
            adapter.permissions = self.permissions
        self.inbox = Inbox(db)
        self.outbox = Outbox(db, clock, markdown_adapters={cfg.instance_id for cfg in settings.adapters
                                                       if cfg.kind == "official" and cfg.markdown_enabled})
        self.stream_delivery = StreamDelivery(self)
        self.tasks = TaskService(db, clock, self.outbox, permissions=self.permissions,
            budget_ceiling=settings.execution.budget, context_validator=self.validate_context,
            stream_eligibility=self.stream_delivery.eligible)
        self.tasks.qq_adapters = adapters
        self.tasks.skills_settings = lambda: self.settings.skills
        self.tasks.skills_provider = self.skills_for
        self.tasks.graph = TaskGraph(self.tasks, settings)
        self.sessions = SessionService(self.tasks, settings, allowed=self.allowed)
        self.leader = LeaderRuntime(self, self.sessions)
        self.controls = HostTaskControls(self)
        self.model_client = model_client or httpx.AsyncClient(timeout=settings.model.timeout_seconds)
        self.openviking = OpenVikingMemory(db, clock, settings.memory.openviking)
        from .knowledge import KnowledgeRuntime
        self.knowledge = KnowledgeRuntime(self)
        self.stopping = False
        self.supervisor = LoopSupervisor(lambda: self.stopping)
        from .recovery import RuntimeRecovery
        self.recovery = RuntimeRecovery(self)
        self.tasks.stop_verifier = self.recovery.stopped
        self.scheduler = None
        self.active, self.active_ids, self.windows = {}, set(), {}

    def skills_for(self, access):
        event = type("Identity", (), {"principal": access.principal, "conversation": access.conversation})()
        if not self.settings.skills.enabled or self.sessions.owner(event) is None:
            return None
        if access.conversation.surface != Surface.PRIVATE and not self.settings.skills.group_enabled:
            return None
        root = self.settings.skills.owner_storage_root or str(Path(self.db.path).resolve().parent / "hermes")
        return HermesSkills(Path(root) / self.settings.owner.id, hermes=self.settings.skills.hermes)

    def access(self, event):
        if event.conversation.adapter_instance_id == "admin_api" and event.principal.identity_namespace == "admin_api":
            return AccessContext(principal=event.principal, conversation=event.conversation, role="admin")
        config = self.adapters[event.conversation.adapter_instance_id].config
        role = "admin" if event.principal.remote_user_id in config.admin_users else "member"
        return AccessContext(principal=event.principal, conversation=event.conversation, role=role)

    def allowed(self, event):
        if event.conversation.adapter_instance_id == "admin_api":
            return (event.principal.adapter_instance_id == "admin_api" and event.principal.identity_namespace == "admin_api"
                    and event.principal.remote_user_id == self.settings.owner.id
                    and self.permissions.check(self.access(event), "task.execute", "conversation:" + event.conversation.key).allowed)
        adapter = self.adapters.get(event.conversation.adapter_instance_id)
        if (adapter is None or event.conversation.bot_identity_id != adapter.config.bot_id
                or event.principal.adapter_instance_id != event.conversation.adapter_instance_id):
            return False
        return self.permissions.check(self.access(event), "conversation.receive", "conversation:" + event.conversation.key).allowed

    def valid_bot_reply(self, event):
        if not event.reply_to:
            return False
        with self.db.engine.connect() as conn:
            return conn.execute(select(outbox.c.id).where(outbox.c.conversation_id == event.conversation.key,
                outbox.c.state == "SENT", outbox.c.platform_message_id == event.reply_to).limit(1)).first() is not None

    def state(self, conversation_id):
        with self.db.engine.connect() as c:
            row = (
                c.execute(select(runtime_state).where(runtime_state.c.conversation_id == conversation_id))
                .mappings()
                .first()
            )
        return dict(row) if row else {"mode": "addressed_only", "policy_version": 1, "state_json": "{}"}

    def policy_view(self, conversation_id):
        return self.state(conversation_id)

    def save_state(self, conversation_id, *, mode=None, state_json=None, expected_version=None):
        with self.db.transaction() as c:
            old = (
                c.execute(select(runtime_state).where(runtime_state.c.conversation_id == conversation_id))
                .mappings()
                .first()
            )
            if expected_version is not None and expected_version != (old["policy_version"] if old else 1):
                raise ValueError("policy version changed")
            values = {
                "mode": mode or (old["mode"] if old else "addressed_only"),
                "policy_version": (old["policy_version"] if old else 1) + int(mode is not None),
                "state_json": state_json if state_json is not None else old["state_json"] if old else "{}",
            }
            if old:
                c.execute(
                    update(runtime_state)
                    .where(runtime_state.c.conversation_id == conversation_id)
                    .values(**values)
                )
            else:
                c.execute(insert(runtime_state).values(conversation_id=conversation_id, **values))

    async def set_mode(
        self,
        conversation,
        mode,
        access,
        expected_version=None,
        *,
        duration_seconds=None,
        silence_started_at=None,
    ):
        if access.conversation != conversation:
            raise PermissionError("conversation identity mismatch")
        self.permissions.require(access, "conversation.policy.set", "conversation:" + conversation.key)
        if mode not in {"addressed_only", "silent"}:
            raise ValueError("未知模式")
        old = self.state(conversation.key)
        saved = json.loads(old["state_json"])
        resume_mode = (
            saved.get("silent_resume_mode", "addressed_only") if old["mode"] == "silent" else old["mode"]
        )
        saved.pop("silent_until", None)
        saved.pop("silent_resume_mode", None)
        if duration_seconds is not None:
            if mode != "silent" or not 1 <= duration_seconds <= 86400:
                raise ValueError("静默时长应为 1 秒至 24 小时")
            saved.update(
                silent_until=(
                    (silence_started_at or self.clock.now()) + timedelta(seconds=duration_seconds)
                ).isoformat(),
                silent_resume_mode=resume_mode,
            )
        self.save_state(
            conversation.key, mode=mode, state_json=json.dumps(saved), expected_version=expected_version
        )
        self.audit("policy.mode", conversation.key, access.principal.key, mode)

    def restore_silences(self):
        with self.db.transaction() as c:
            rows = c.execute(select(runtime_state).where(runtime_state.c.mode == "silent")).mappings().all()
            for row in rows:
                saved = json.loads(row["state_json"])
                until = saved.get("silent_until")
                if until is None or datetime.fromisoformat(until) > self.clock.now():
                    continue
                mode = saved.pop("silent_resume_mode", "addressed_only")
                saved.pop("silent_until", None)
                if mode not in {"addressed_only"}:
                    mode = "addressed_only"
                c.execute(
                    update(runtime_state)
                    .where(runtime_state.c.conversation_id == row["conversation_id"])
                    .values(mode=mode, policy_version=row["policy_version"] + 1, state_json=json.dumps(saved))
                )
                c.execute(
                    insert(audit_events).values(
                        id=str(uuid4()),
                        kind="policy.mode",
                        target=row["conversation_id"],
                        actor="timer",
                        created_at=self.clock.now().isoformat(),
                        detail="timed silence expired: " + mode,
                    )
                )

    def audit(self, kind, target, actor, detail):
        with self.db.transaction() as c:
            c.execute(
                insert(audit_events).values(
                    id=str(uuid4()),
                    kind=kind,
                    target=target,
                    actor=actor,
                    created_at=self.clock.now().isoformat(),
                    detail=detail,
                )
            )

    def enqueue_admin_task(self, goal, *, idempotency_key, success_checks=(), requested_outputs=()):
        if not idempotency_key or len(idempotency_key) > 200:
            raise ValueError("an Idempotency-Key of at most 200 characters is required")
        now = self.clock.now()
        conversation = ConversationRef(deployment_id=self.settings.deployment_id, adapter_instance_id="admin_api",
            bot_identity_id="host", surface=Surface.PRIVATE, remote_conversation_id=self.settings.owner.id)
        principal = PrincipalRef(adapter_instance_id="admin_api", identity_namespace="admin_api", remote_user_id=self.settings.owner.id)
        event = InboundEvent(event_id=stable_id("admin-task", idempotency_key), event_type="message.created",
            conversation=conversation, principal=principal, platform_message_id=idempotency_key,
            occurred_at=now, received_at=now, segments=(TextSegment(text=goal),), trace_id=str(uuid4()))
        access = self.access(event)
        self.permissions.require(access, "task.create", "conversation:" + conversation.key)
        with self.db.transaction() as conn:
            from qqbot.storage.schema import conversations
            if not conn.execute(select(conversations.c.id).where(conversations.c.id == conversation.key)).first():
                conn.execute(insert(conversations).values(id=conversation.key, ref_json=conversation.model_dump_json(), seq=0, revocation=0))
            dialogue = self.sessions.dialogue(conn, event)
            return self.tasks.graph.submit(conn, dialogue, event, access,
                {"goal": goal, "success_checks": list(success_checks), "requested_outputs": list(requested_outputs),
                 "children": [{"mode": "work", "goal": goal, "delegation_key": "primary",
                               "success_checks": list(success_checks), "requested_outputs": list(requested_outputs)}]},
                stable_id("admin-task", idempotency_key), self.sessions.binding(), {})

    def context_for(self, event, access=None, **_):
        access = access or self.access(event)
        self.permissions.require(access, "history.source.read", "event:" + event.event_id)
        raw = event.model_dump_json()
        with self.db.engine.connect() as conn:
            stored = conn.execute(select(inbox.c.event_json).where(inbox.c.id == event.event_id)).scalar_one_or_none()
        sources = [(event, stored or raw)]
        from qqbot.storage.agent_schema import dialogue_inputs, focus
        with self.db.engine.connect() as conn:
            dialogue_id = conn.execute(select(focus.c.dialogue_id).where(
                focus.c.key == stable_id(event.conversation.key, event.principal.key))).scalar_one_or_none()
            previous = conn.execute(select(dialogue_inputs.c.event_json).where(
                dialogue_inputs.c.dialogue_id == dialogue_id, dialogue_inputs.c.id != event.event_id)
                .order_by(dialogue_inputs.c.created_at.desc()).limit(8)).scalars().all() if dialogue_id else []
            if dialogue_id and event.reply_to:
                replied = conn.execute(select(dialogue_inputs.c.event_json).join(inbox,
                    inbox.c.id == dialogue_inputs.c.id).where(
                    dialogue_inputs.c.dialogue_id == dialogue_id,
                    inbox.c.message_id == event.reply_to, inbox.c.conversation_id == event.conversation.key,
                ).limit(1)).scalar_one_or_none()
                if replied and replied not in previous:
                    previous.insert(0, replied)
        for candidate in previous:
            prior = InboundEvent.model_validate_json(candidate)
            age = (event.received_at - prior.received_at).total_seconds()
            if (prior.conversation != event.conversation or prior.principal != event.principal
                    or age < 0 or (age > 600 and prior.platform_message_id != event.reply_to)
                    or not any(s.type == "image" for s in prior.segments)):
                continue
            manifest = {"kind": "inbox", "id": prior.event_id, "digest": hashlib.sha256(candidate.encode()).hexdigest()}
            if self.validate_context(access, {"source_manifest": [manifest]}):
                sources.append((prior, candidate))
        return {"scope": event.conversation.key, "principal": event.principal.key,
            "source_manifest": [{"kind": "inbox", "id": source.event_id,
                                 "digest": hashlib.sha256(body.encode()).hexdigest()} for source, body in sources],
            "images": [{"source": source.event_id, "message_id": source.platform_message_id, "url": segment.image_url}
                       for source, _body in sources for segment in source.segments if segment.type == "image"][:4],
            "now": self.clock.now().isoformat(), "timezone": self.settings.timezone,
            "limits": self.settings.context.limits(self.settings.model.context_window,
                self.settings.execution.interactive_budget.output_tokens_per_call),
            "model_context_window": self.settings.model.context_window,
            "preferences": getattr(self.settings.owner, "preferences", "")}

    def validate_context(self, access, context):
        if not self.knowledge.valid(context):
            return False
        if context.get("scope", access.conversation.key) != access.conversation.key:
            return False
        if context.get("principal", access.principal.key) != access.principal.key:
            return False
        if not self.permissions.check(access, "history.read", "conversation:" + access.conversation.key).allowed:
            return False
        with self.db.engine.connect() as conn:
            for source in context.get("source_manifest", ()):
                if source["kind"] != "inbox":
                    return False
                raw = conn.execute(select(inbox.c.event_json).where(inbox.c.id == source["id"],
                    inbox.c.conversation_id == access.conversation.key)).scalar_one_or_none()
                if not raw or hashlib.sha256(raw.encode()).hexdigest() != source["digest"]:
                    return False
                if not self.permissions.check(access, "history.source.read", "event:" + source["id"]).allowed:
                    return False
                event = InboundEvent.model_validate_json(raw)
                if conn.execute(select(inbox.c.id).where(inbox.c.conversation_id == access.conversation.key,
                    inbox.c.event_type == "message.recalled", inbox.c.message_id == event.platform_message_id)).first():
                    return False
        return True

    def submit_turn(self, event, fragments=(), *, text=None, access=None, **_):
        if fragments:
            raise ValueError("each user input is persisted independently")
        if text is not None:
            event = event.model_copy(update={"segments": ({"type": "text", "text": text},)})
            event = InboundEvent.model_validate_json(event.model_dump_json())
        return self.sessions.enqueue(event, access or self.access(event), self.context_for(event, access))

    def reply(self, event, text):
        with self.db.transaction() as conn:
            self.outbox.enqueue_text(conn, target=event.conversation, text=text, origin_kind="chat",
                origin_id=event.event_id, reply_context=event.reply_context, requester=event.principal,
                ttl_seconds=300, anchor_message_id=event.platform_message_id)

    def explain_control(self, event, facts):
        self.reply(event, json.dumps(facts, ensure_ascii=False, default=str))

    async def command(self, event):
        command, _, argument = event.text.strip().partition(" ")
        access = self.access(event)
        if command == "/帮助":
            return "直接告诉我需要做什么，或问“进度怎么样”、说“取消刚才那个”。补充内容时可以引用对应的进度消息。\n/状态 /新对话 <名称> /切换对话 <名称> /对话 /静默 [30m] /恢复"
        if command == "/状态":
            return "我在，可以正常接收消息。想了解正在处理的事情，可以问“进度怎么样”。"
        if command in {"/静默", "/恢复", "/模式"}:
            mode = "silent" if command == "/静默" else "addressed_only" if command == "/恢复" else argument.strip()
            duration = None
            if command == "/静默" and argument:
                match = re.fullmatch(r"([1-9]\d*)([smh])", argument.strip())
                if not match:
                    return "静默时长用 s/m/h，例如 30m"
                duration = int(match[1]) * {"s": 1, "m": 60, "h": 3600}[match[2]]
            await self.set_mode(event.conversation, mode, access, duration_seconds=duration)
            return "已静默；发送 /恢复 可恢复。" if mode == "silent" else "已恢复。"
        if command in {"/任务", "/取消", "/补充"}:
            return self.controls.command(event, command, argument)
        from qqbot.storage.agent_schema import dialogues
        with self.db.transaction() as conn:
            if command == "/新对话":
                selected = self.sessions.dialogue(conn, event, new_title=argument.strip() or "新对话")
            elif command == "/切换对话":
                try:
                    selected = self.sessions.dialogue(conn, event, select_ref=argument.strip())
                except ValueError:
                    return "请提供一个明确的对话名称，可以用 /对话 查看已有对话。"
            elif command == "/对话":
                owner = self.sessions.owner(event) or "guest-" + event.principal.key
                rows = conn.execute(select(dialogues).where(dialogues.c.owner_id == owner,
                    dialogues.c.conversation_id == event.conversation.key, dialogues.c.state == "ACTIVE")).mappings()
                return "\n".join(row["title"] for row in rows) or "暂无对话。"
            else:
                raise ValueError("未知命令")
        return "当前对话：" + selected["title"]

    async def process(self, event):
        try:
            if not self.allowed(event) or event.sender_is_bot or event.event_type != "message.created":
                return
            command = (event.text.strip().split(maxsplit=1) or [""])[0]
            if command in CONTROL_COMMANDS:
                text = await self.command(event)
                if text:
                    self.reply(event, text)
                return
            if self.state(event.conversation.key)["mode"] == "silent":
                return
            if event.conversation.surface not in {Surface.PRIVATE, Surface.GUILD_PRIVATE} and not (
                    event.mentions_bot or self.valid_bot_reply(event)):
                return
            controlled = self.controls.route(event)
            if controlled is not None:
                self.reply(event, controlled)
                return
            if not self.settings.interactive_execution_enabled:
                return
            context = await self.knowledge.prepare(event, self.context_for(event))
            self.sessions.enqueue(event, access=self.access(event), context=context)
        except (PermissionError, ValueError, LookupError) as error:
            logger.warning("Input rejected", extra={"data": {"error_type": type(error).__name__}})
            if event.conversation.surface == Surface.PRIVATE:
                if isinstance(error, PermissionError):
                    text = "这次无法操作这项内容，请确认你是在原来的对话中继续。"
                elif isinstance(error, LookupError):
                    text = "没有找到对应内容，请引用之前的进度消息，或重新说明目标。"
                else:
                    text = "这次请求暂时无法处理，请检查内容后再试。"
                self.reply(event, text)
        except Exception:
            logger.exception("Input failed")
            return
        finally:
            self.inbox.finish(event.event_id, expected_seq=event.ingest_seq)

    def pre_send(self, envelope, *, stream_cleanup=False):
        if stream_cleanup:
            from qqbot.storage.stream_schema import stream_messages
            with self.db.engine.connect() as conn:
                persisted = conn.execute(select(stream_messages.c.envelope_json).where(
                    stream_messages.c.id == envelope.origin_id, stream_messages.c.kind == "answer",
                    stream_messages.c.stream_id.is_not(None), stream_messages.c.state == "SENDING",
                )).scalar_one_or_none()
            if persisted != envelope.model_dump_json():
                return False
        subject = PermissionSubject.from_refs(envelope.target, envelope.requester)
        if not self.permissions.check(subject, "notification.send", "conversation:" + envelope.target.key).allowed:
            return False
        origin_id = envelope.origin_id
        if envelope.origin_kind == "chat" and origin_id.startswith("progress:"):
            origin_id = origin_id.split(":", 2)[-1]
        if envelope.origin_kind == "chat" and origin_id.startswith("turn:"):
            from qqbot.storage.agent_schema import dialogue_inputs

            from .task_graph import TERMINAL

            session_id, _, source_id = origin_id.removeprefix("turn:").partition(":")
            with self.db.engine.connect() as conn:
                source = conn.execute(select(dialogue_inputs).where(dialogue_inputs.c.id == source_id)).mappings().first()
                if not source or envelope.purpose == "progress" and source["handled_at"]:
                    return False
                completed = conn.execute(select(runs.c.request_json, runs.c.result_json).where(
                    runs.c.id == source["claimed_attempt_id"], runs.c.session_id == session_id,
                    runs.c.role == "leader", runs.c.state == "FINISHED")).mappings().first()
                settled = conn.execute(select(tasks.c.id).where(tasks.c.kind == "goal",
                    tasks.c.origin_dialogue_id == source["dialogue_id"], tasks.c.event_json == source["event_json"],
                    tasks.c.state.in_(TERMINAL))).first()
            event = InboundEvent.model_validate_json(source["event_json"])
            if event.conversation != envelope.target or event.principal != envelope.requester:
                return False
            mutation = json.loads(completed["request_json"]).get("memory_mutation", {}) if completed else {}
            result = json.loads(completed["result_json"] or "{}") if completed else {}
            if (mutation.get("summary") == envelope.text and result.get("summary") == envelope.text
                    and result.get("agent_authored") is False):
                return self.sessions.owner(event) is not None and self.allowed(event)
            if settled or not self.validate_context(self.access(event), json.loads(source["context_json"])):
                return False
        if envelope.origin_kind == "task":
            with self.db.engine.connect() as conn:
                task = conn.execute(select(tasks, notifications.c.run_id.label("notification_run_id"),
                    notifications.c.state.label("notification_state"), notifications.c.purpose.label("notice_purpose"),
                    runs.c.run_reason.label("notice_run_reason"),
                    runs.c.basis_goal_revision.label("notice_revision")).join(notifications, notifications.c.task_id == tasks.c.id)
                    .outerjoin(runs, runs.c.id == notifications.c.run_id)
                    .where(notifications.c.id == envelope.origin_id)).mappings().first()
            if not task:
                return False
            if not stream_cleanup and task["state"] in {"CANCELLED", "CANCEL_REQUESTED", "NEEDS_RECONCILIATION"}:
                return False
            if not stream_cleanup and task["state"] == "EXPIRED" and not (
                task["notice_run_reason"] == "notification" and task["notice_purpose"] == "result"
            ):
                return False
            with self.db.engine.connect() as conn:
                if not stream_cleanup and task["root_task_id"] and task["notification_run_id"]:
                    root_revision = conn.execute(select(tasks.c.goal_revision).where(
                        tasks.c.id == task["root_task_id"])).scalar_one()
                    if task["notice_revision"] != root_revision:
                        return False
                if not self.knowledge.delivery_valid(conn, task, task):
                    return False
            event = InboundEvent.model_validate_json(task["event_json"])
            access = self.access(event)
            return (self.permissions.check(access, "notification.send", "conversation:" + envelope.target.key).allowed
                    and self.validate_context(access, json.loads(task["context_json"])))
        return True

    async def tick(self, *, dispatch=True):
        self.restore_silences()
        for event in self.inbox.pending():
            await self.process(event)
        if dispatch:
            await self.deliver()

    async def deliver(self):
        await self.stream_delivery.dispatch_once()
        await self.outbox.dispatch_once(self.adapters, self.settings.send_budget_per_minute, self.pre_send)

    async def maintenance(self):
        for recover in (self.recovery.recover_workers, self.leader.recover):
            pending = asyncio.create_task(asyncio.to_thread(recover))
            try:
                await asyncio.shield(pending)
            except asyncio.CancelledError:
                # Drain recovery before shutdown closes its database and clients.
                await pending
                raise
            self.knowledge.recover_mutations()
        await self.knowledge.sync()

    async def cleanup_artifacts(self):
        from qqbot.harness.artifacts import cleanup_artifacts
        pending = asyncio.create_task(asyncio.to_thread(cleanup_artifacts, self.db, self.tasks.archive_root, self.clock.now()))
        try:
            await asyncio.shield(pending)
        except asyncio.CancelledError:
            await pending
            raise

    def retire_adapters(self):
        """Retain unsupported transport inputs as explicit failures, never execute them."""
        from qqbot.storage.action_schema import operations
        from qqbot.storage.task_schema import runs
        terminal = {"SUCCEEDED", "PARTIAL", "BLOCKED", "FAILED", "CANCELLED", "EXPIRED"}
        with self.db.transaction() as conn:
            for row in conn.execute(select(inbox).where(inbox.c.state.in_(["PENDING", "WAITING"]))).mappings():
                event = InboundEvent.model_validate_json(row["event_json"])
                if event.conversation.adapter_instance_id not in self.adapters:
                    conn.execute(update(inbox).where(inbox.c.id == row["id"]).values(state="FAILED", error="ADAPTER_REMOVED"))
            for row in conn.execute(select(tasks).where(tasks.c.state.not_in(terminal))).mappings():
                event = InboundEvent.model_validate_json(row["event_json"])
                if event.conversation.adapter_instance_id in self.adapters or self.sessions.owner(event) == self.settings.owner.id and event.conversation.adapter_instance_id == "admin_api":
                    continue
                active = conn.execute(select(runs.c.id).where(runs.c.task_id == row["id"], runs.c.slot_reserved == 1)).first()
                unknown = conn.execute(select(operations.c.id).where(operations.c.task_id == row["id"],
                    operations.c.state.in_(["DISPATCHED", "UNKNOWN", "PARTIAL"]))).first()
                state = "CANCEL_REQUESTED" if active else "NEEDS_RECONCILIATION" if unknown else "FAILED"
                conn.execute(update(tasks).where(tasks.c.id == row["id"]).values(state=state, stop_reason="adapter_removed"))

    async def run(self):
        from .scheduler import Scheduler
        self.scheduler = Scheduler(self, self.leader)
        self.retire_adapters()
        self.outbox.recover()
        self.stream_delivery.recover()
        self.recovery.recover_processes()
        self.tasks.recover()
        self.recovery.recover_workers()
        self.knowledge.recover_mutations()
        self.leader.recover()
        self.knowledge.recover_mutations()
        self.supervisor.start("ingress", lambda: self.tick(dispatch=False), interval=0.05, critical=True)
        self.supervisor.start("delivery", self.deliver, interval=0.1, critical=True)
        self.supervisor.start("scheduler", self.scheduler.step, interval=0.1, critical=True)
        self.supervisor.start("external_waiters", self.scheduler.waiters.once, interval=1, critical=True)
        self.supervisor.start("memory", self.maintenance, interval=10)
        self.supervisor.start("artifacts", self.cleanup_artifacts, interval=3600)
        while not self.stopping:
            await asyncio.sleep(0.1)

    async def stop(self):
        self.stopping = True
        await self.supervisor.stop()
        if self.scheduler:
            await self.scheduler.close()
        await self.openviking.close()
        await self.model_client.aclose()
