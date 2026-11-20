"""One host runtime: QQ ingress, persistent Leader turns and on-demand Workers."""
import asyncio
import hashlib
import json
import logging
import re
from contextlib import nullcontext
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

import httpx
from sqlalchemy import insert, select, update

from elephant.adapters.hermes_skills import HermesSkills
from elephant.adapters.openviking import OpenVikingMemory
from elephant.domain import (
    AccessContext,
    ConversationRef,
    InboundEvent,
    PrincipalRef,
    Surface,
    TextSegment,
    stable_id,
)
from elephant.ingress.service import Inbox
from elephant.jobs.service import TaskService
from elephant.jobs.streaming import StreamDelivery
from elephant.observability.supervision import LoopSupervisor
from elephant.outbox.service import Outbox
from elephant.permissions import PermissionService, PermissionSubject
from elephant.pi_installation import runtime_pi_path
from elephant.storage.schema import audit_events, inbox, outbox, runtime_state
from elephant.storage.task_schema import (
    notification_question_ids,
    notifications,
    runs,
    sessions,
    tasks,
    waits,
)

from .control import TurnRouter
from .leader import LeaderRuntime
from .sessions import SessionService
from .task_graph import TaskGraph

logger = logging.getLogger(__name__)
CONTROL_COMMANDS = {"/project", "/help", "/status", "/tasks", "/cancel", "/followup", "/mute", "/resume", "/mode", "/new", "/switch", "/dialogs", "/clear"}


class Core:
    def __init__(self, settings, db, clock, adapters, model_client=None, *, harnesses=None,
                 default_harness="pi", execution=None):
        self.settings, self.db, self.clock, self.adapters = settings, db, clock, adapters
        from elephant.adapters.execution.world import LocalExecution
        from elephant.adapters.pi.adapter import PiAdapter
        from elephant.contracts.harness import resolve_harness
        self.harnesses = {"pi": PiAdapter(runtime_pi_path(settings.orchestration.pi_executable)), **(harnesses or {})}
        self.execution = execution or LocalExecution()
        harness = resolve_harness(self, {"harness": default_harness})
        self.permissions = PermissionService(db, clock, settings.permissions)
        for adapter in adapters.values():
            adapter.permissions = self.permissions
        self.inbox = Inbox(db)
        self.outbox = Outbox(db, clock, markdown_adapters={cfg.instance_id for cfg in settings.adapters
                                                       if cfg.kind == "official" and cfg.markdown_enabled})
        self.stream_delivery = StreamDelivery(self)
        from elephant.mail.runtime import MailRuntime
        self.mail = MailRuntime(self)
        self.tasks = TaskService(db, clock, self.outbox, permissions=self.permissions,
            budget_ceiling=settings.execution.budget, context_validator=self.validate_context,
            stream_eligibility=self.stream_delivery.eligible, qq_adapters=adapters,
            skills_settings=lambda: self.settings.skills, skills_provider=self.skills_for,
            action_authorizer=self.mail.authorize_context)
        self.tasks.graph = TaskGraph(self.tasks, settings)
        self.sessions = SessionService(self.tasks, settings, allowed=self.allowed)
        self.sessions.harnesses = self.harnesses
        self.sessions.runtime_binding = {**harness.binding, "execution_world": self.execution.world_id}
        self.leader = LeaderRuntime(self, self.sessions)
        self.controls = TurnRouter(self)
        self.model_client = model_client or httpx.AsyncClient(timeout=settings.model.timeout_seconds)
        self.openviking = OpenVikingMemory(db, clock, settings.memory.openviking)
        from .knowledge import KnowledgeRuntime
        self.knowledge = KnowledgeRuntime(self)
        from .schedules import ScheduleService
        self.schedules = ScheduleService(self)
        self.stopping = False
        self.supervisor = LoopSupervisor(lambda: self.stopping)
        from .recovery import RuntimeRecovery
        self.recovery = RuntimeRecovery(self)
        self.tasks.stop_verifier = self.recovery.stopped
        self.scheduler = None
        self.active, self.active_ids, self.windows = {}, set(), {}

    def skills_for(self, access, *, workspace_id="personal"):
        event = type("Identity", (), {"principal": access.principal, "conversation": access.conversation})()
        if not self.settings.skills.enabled or self.sessions.owner(event) is None:
            return None
        if access.conversation.surface != Surface.PRIVATE and not self.settings.skills.group_enabled:
            return None
        root = self.settings.skills.owner_storage_root or str(Path(self.db.path).resolve().parent / "hermes")
        self.sessions.validate_workspace(event, workspace_id)
        storage = Path(root) / self.settings.owner.id
        options = self.settings.skills.hermes
        if workspace_id != "personal":
            storage = storage / "projects" / workspace_id
            options = options.model_copy(update={"create_dir": None})
        return HermesSkills(storage, hermes=options)

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

    def structured_goal_context(self, event, spec, workspace_id, conn):
        """Persist the authenticated structured source before its task graph in one transaction."""
        from elephant.domain.agents import GoalSpec
        from elephant.domain.messages import StructuredSegment
        goal_spec = GoalSpec.model_validate(spec).model_dump(mode="json")
        workspace_id = self.sessions.validate_workspace(event, workspace_id or self.sessions.workspace_for(event, conn))
        request = {"workspace_id": workspace_id, "goal": goal_spec["goal"],
            "success_checks": goal_spec["success_checks"], "requested_outputs": goal_spec["requested_outputs"]}
        if goal_spec["requested_actions"]:
            request["requested_actions"] = goal_spec["requested_actions"]
        source_payload = {key: value for key, value in request.items() if key != "goal"}
        source_payload["goal_sha256"] = hashlib.sha256(request["goal"].encode()).hexdigest()
        encoded = json.dumps(request, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        event = event.model_copy(update={"segments": (TextSegment(text=goal_spec["goal"]),
            StructuredSegment(original_type="host_goal_request", payload=source_payload))})
        event, _ = self.inbox.accept(event, conn=conn)
        original = next((segment.payload for segment in event.segments if segment.type == "structured"
            and segment.original_type == "host_goal_request"), None)
        if original != source_payload or event.text != request["goal"]:
            raise ValueError("structured request key reused with different content")
        conn.execute(update(inbox).where(inbox.c.id == event.event_id).values(state="DONE"))
        context = self.context_for(event, workspace_id=workspace_id, conn=conn)
        context["structured_request"] = {"source_event_id": event.event_id, "request": request,
            "sha256": hashlib.sha256(encoded.encode()).hexdigest()}
        scope = self.knowledge.scope(event, workspace_id=workspace_id)
        if scope is not None and self.openviking.enabled(scope):
            owner = self.openviking._owner(scope, conn=conn)
            context.update(memory_scope=scope.model_dump(), memory_epoch=owner["epoch"] if owner else 0,
                memory_status="ready" if owner is None or owner["state"] == "READY" else "blocked", retrieved_data=[])
        return event, context

    def enqueue_admin_task(self, goal, *, idempotency_key, success_checks=(), requested_outputs=(),
                           requested_actions=(), workspace_id=None, viewer=None):
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
            workspace_id = workspace_id or self.sessions.workspace_for(event, conn)
            dialogue = self.sessions.dialogue(conn, event, workspace_id=workspace_id)
            spec = {"goal": goal, "success_checks": list(success_checks), "requested_outputs": list(requested_outputs),
                "requested_actions": list(requested_actions),
                "children": [{"mode": "work", "goal": goal, "delegation_key": "primary",
                    "success_checks": list(success_checks), "requested_outputs": list(requested_outputs)}]}
            event, context = self.structured_goal_context(event, spec, workspace_id, conn)
            return self.tasks.graph.submit(conn, dialogue, event, access, spec,
                stable_id("admin-task", idempotency_key), self.sessions.binding(), context, viewer=viewer)

    def context_for(self, event, access=None, workspace_id=None, *, conn=None, **_):
        access = access or self.access(event)
        self.permissions.require(access, "history.source.read", "event:" + event.event_id)
        raw = event.model_dump_json()
        workspace_id = self.sessions.validate_workspace(event, workspace_id or self.sessions.workspace_for(event, conn))
        with (nullcontext(conn) if conn is not None else self.db.engine.connect()) as connection:
            stored = connection.execute(select(inbox.c.event_json).where(inbox.c.id == event.event_id)).scalar_one_or_none()
        sources = [(event, stored or raw)]
        from elephant.storage.agent_schema import dialogue_inputs, dialogues, focus
        owner = self.sessions.owner(event) or "guest-" + event.principal.key
        with (nullcontext(conn) if conn is not None else self.db.engine.connect()) as connection:
            dialogue_id = connection.execute(select(dialogue_inputs.c.dialogue_id).join(dialogues,
                dialogues.c.id == dialogue_inputs.c.dialogue_id).where(dialogue_inputs.c.id == event.event_id,
                dialogues.c.workspace_id == workspace_id, dialogues.c.owner_id == owner)).scalar_one_or_none()
            if dialogue_id is None:
                dialogue_id = connection.execute(select(focus.c.dialogue_id).join(dialogues,
                    dialogues.c.id == focus.c.dialogue_id).where(focus.c.key == stable_id(event.conversation.key,
                    event.principal.key), dialogues.c.workspace_id == workspace_id,
                    dialogues.c.owner_id == owner)).scalar_one_or_none()
            previous = connection.execute(select(dialogue_inputs.c.event_json).where(
                dialogue_inputs.c.dialogue_id == dialogue_id, dialogue_inputs.c.id != event.event_id)
                .order_by(dialogue_inputs.c.created_at.desc()).limit(8)).scalars().all() if dialogue_id else []
            if dialogue_id and event.reply_to:
                replied = connection.execute(select(dialogue_inputs.c.event_json).join(inbox,
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
                    or not any(s.type in {"image", "file", "audio"} for s in prior.segments)):
                continue
            manifest = {"kind": "inbox", "id": prior.event_id, "digest": hashlib.sha256(candidate.encode()).hexdigest()}
            if self.validate_context(access, {"source_manifest": [manifest], "workspace_id": workspace_id}):
                sources.append((prior, candidate))
        return {"scope": event.conversation.key, "principal": event.principal.key, "workspace_id": workspace_id,
            "source_manifest": [{"kind": "inbox", "id": source.event_id,
                                 "digest": hashlib.sha256(body.encode()).hexdigest()} for source, body in sources],
            "images": [{"source": source.event_id, "message_id": source.platform_message_id, "url": segment.image_url or segment.source_url,
                        "filename": segment.filename, "mime_type": segment.mime_type, "size": segment.size,
                        "attachment_ref": segment.attachment_ref}
                       for source, _body in reversed(sources) for segment in source.segments if segment.type == "image"],
            "files": [{"source": source.event_id, "message_id": source.platform_message_id,
                "url": segment.source_url, "filename": segment.filename, "mime_type": segment.mime_type, "size": segment.size,
                "attachment_ref": segment.attachment_ref,
                **({"kind": "audio", "transcript": segment.transcript, "quoted": segment.quoted,
                    "is_current_input": source.event_id == event.event_id} if segment.type == "audio" else {})}
                for source, _body in reversed(sources) for segment in source.segments if segment.type in {"file", "audio"}],
            "now": self.clock.now().isoformat(), "timezone": self.settings.timezone,
            "limits": self.settings.context.limits(self.settings.model.context_window,
                self.settings.execution.interactive_budget.output_tokens_per_call),
            "model_context_window": self.settings.model.context_window,
            "preferences": getattr(self.settings.owner, "preferences", "")}

    def validate_context(self, access, context):
        if context.get("scope", access.conversation.key) != access.conversation.key:
            return False
        if context.get("principal", access.principal.key) != access.principal.key:
            return False
        workspace_id = context.get("workspace_id", "personal")
        event = type("Identity", (), {"principal": access.principal, "conversation": access.conversation})()
        with self.db.engine.connect() as conn:
            conn.exec_driver_sql("BEGIN")
            policy = self.permissions.snapshot(conn)
            try:
                self.sessions.validate_workspace(event, workspace_id, policy_snapshot=policy)
                if context.get("owner_id") == self.settings.owner.id and self.sessions.owner(event) != self.settings.owner.id:
                    return False
            except (PermissionError, ValueError):
                return False
            if (not self.knowledge.valid(context, conn=conn) or not self.permissions.check(
                    access, "history.read", "conversation:" + access.conversation.key, snapshot=policy).allowed):
                return False
            if context.get("synthetic_mail_schedule_id") and not self.schedules.synthetic_fire_authorized(
                    conn, event, context):
                return False
            if context.get("session_id"):
                history = conn.execute(select(sessions.c.manifest_json).where(
                    sessions.c.id == context["session_id"])).scalar_one_or_none()
                history = json.loads(history) if history else {}
                if history.get("task_sources") or history.get("steering_sources"):
                    context = dict(context)
                    self.tasks.graph.merge_source_dependencies(context, history.get("task_sources", ()),
                        history.get("source_manifest", ()))
            try:
                self.mail.receipts(conn, event, context, access=access, policy_snapshot=policy)
            except (PermissionError, LookupError, ValueError):
                return False
            sources = context.get("source_manifest", ())
            if not sources:
                return True
            if any(source["kind"] != "inbox" for source in sources):
                return False
            ids = {source["id"] for source in sources}
            raw_events = dict(conn.execute(select(inbox.c.id, inbox.c.event_json).where(
                inbox.c.id.in_(ids), inbox.c.conversation_id == access.conversation.key)).all())
            by_workspace = {workspace_id: ids}
            message_ids = set()
            for source in sources:
                raw = raw_events.get(source["id"])
                if not raw or hashlib.sha256(raw.encode()).hexdigest() != source["digest"]:
                    return False
                if not self.permissions.check(access, "history.source.read", "event:" + source["id"], snapshot=policy).allowed:
                    return False
                source_event = InboundEvent.model_validate_json(raw)
                original_workspace = self.sessions.workspace_for(source_event, conn)
                by_workspace.setdefault(original_workspace, set()).add(source["id"])
                message_ids.add(source_event.platform_message_id)
            if any(not self.knowledge.sources_valid(event, source_ids, workspace_id=workspace, conn=conn)
                   for workspace, source_ids in by_workspace.items()):
                return False
            if conn.execute(select(inbox.c.id).where(inbox.c.conversation_id == access.conversation.key,
                inbox.c.event_type == "message.recalled", inbox.c.message_id.in_(message_ids)).limit(1)).first():
                return False
        return True

    def submit_turn(self, event, fragments=(), *, text=None, access=None, **_):
        if fragments:
            raise ValueError("each user input is persisted independently")
        if text is not None:
            event = event.model_copy(update={"segments": ({"type": "text", "text": text},)})
            event = InboundEvent.model_validate_json(event.model_dump_json())
        return self.sessions.enqueue(event, access or self.access(event), self.context_for(event, access))

    def reply(self, event, text, *, conn=None):
        with (nullcontext(conn) if conn is not None else self.db.transaction()) as connection:
            self.outbox.enqueue_text(connection, target=event.conversation, text=text, origin_kind="chat",
                origin_id=event.event_id, reply_context=event.reply_context, requester=event.principal,
                ttl_seconds=300, anchor_message_id=event.platform_message_id)

    def explain_control(self, event, facts):
        self.reply(event, json.dumps(facts, ensure_ascii=False, default=str))

    async def command(self, event):
        command, *arguments = event.text.strip().split(maxsplit=1)
        argument = arguments[0] if arguments else ""
        access = self.access(event)
        if command == "/help":
            return (
                "/help 查看命令\n"
                "/status 查看接收状态\n"
                "/tasks [ID] 查看任务\n"
                "/cancel [ID] 取消任务\n"
                "/followup <ID> <内容> 补充任务\n"
                "/new [名称] 开启新对话\n"
                "/switch <名称或 ID> 切换对话\n"
                "/dialogs 列出对话\n"
                "/clear 清空当前对话上下文并切到新对话\n"
                "/project [名称或 personal] 查看或切换项目\n"
                "/mute [30m] 静默\n"
                "/resume 恢复接收\n"
                "/mode <silent|addressed_only> 设置接收模式"
            )
        if command == "/clear":
            if not self.allowed(event):
                raise PermissionError("conversation access required")
            if event.conversation.surface not in {Surface.PRIVATE, Surface.GUILD_PRIVATE} and not (
                    event.mentions_bot or self.valid_bot_reply(event)):
                return None
            if argument:
                return "请单独发送 /clear。"
            with self.db.transaction() as conn:
                if not conn.scalar(select(outbox.c.id).where(outbox.c.origin_kind == "chat",
                        outbox.c.origin_id == event.event_id).limit(1)):
                    self.sessions.clear_context(conn, event)
                    self.reply(event, "已清空当前对话上下文，可以开始新的话题。", conn=conn)
            return None
        if command == "/project":
            if self.sessions.owner(event) is None:
                raise PermissionError("project selection requires the owner")
            if not argument.strip():
                return "当前项目 " + self.sessions.workspace_for(event) + "\n可用项目\n" + "\n".join(["personal", *self.sessions.configured_workspaces()])
            with self.db.transaction() as conn:
                selected = self.sessions.dialogue(conn, event, workspace_id=argument.strip())
            return "当前项目 " + selected["workspace_id"] + "，对话 " + selected["title"]
        if command == "/status":
            return "我在，可以正常接收消息。想了解正在处理的事情，可以问“进度怎么样”。"
        if command in {"/mute", "/resume", "/mode"}:
            mode = "silent" if command == "/mute" else "addressed_only" if command == "/resume" else argument.strip()
            duration = None
            if command == "/mute" and argument:
                match = re.fullmatch(r"([1-9]\d*)([smh])", argument.strip())
                if not match:
                    return "静默时长用 s/m/h，例如 30m"
                duration = int(match[1]) * {"s": 1, "m": 60, "h": 3600}[match[2]]
            await self.set_mode(event.conversation, mode, access, duration_seconds=duration)
            return "已静默；发送 /resume 可恢复。" if mode == "silent" else "已恢复。"
        if command in {"/tasks", "/cancel", "/followup"}:
            return self.controls.command(event, command, argument)
        from elephant.storage.agent_schema import dialogues
        with self.db.transaction() as conn:
            if command == "/new":
                selected = self.sessions.dialogue(conn, event, new_title=argument.strip() or "新对话")
            elif command == "/switch":
                try:
                    selected = self.sessions.dialogue(conn, event, select_ref=argument.strip())
                except ValueError:
                    return "请提供一个明确的对话名称，可以用 /dialogs 查看已有对话。"
            elif command == "/dialogs":
                owner = self.sessions.owner(event) or "guest-" + event.principal.key
                rows = conn.execute(select(dialogues).where(dialogues.c.owner_id == owner,
                    dialogues.c.conversation_id == event.conversation.key, dialogues.c.state == "ACTIVE")).mappings()
                return "\n".join(row["title"] for row in rows) or "暂无对话。"
            else:
                raise ValueError("未知命令")
        return "当前对话：" + selected["title"]

    async def process(self, event):
        retry, error_code = False, None
        try:
            if not self.allowed(event) or event.sender_is_bot or event.event_type != "message.created":
                return
            command = (event.text.strip().split(maxsplit=1) or [""])[0]
            if command in CONTROL_COMMANDS:
                text = await self.command(event)
                if text:
                    self.reply(event, text)
                if command != "/clear" and self.state(event.conversation.key)["mode"] != "silent":
                    with self.db.transaction() as conn:
                        self.outbox.retrigger(conn, event)
                return
            if self.state(event.conversation.key)["mode"] == "silent":
                return
            if event.conversation.surface not in {Surface.PRIVATE, Surface.GUILD_PRIVATE} and not (
                    event.mentions_bot or self.valid_bot_reply(event)):
                return
            controlled = self.controls.route(event)
            if controlled is not None:
                self.reply(event, controlled)
                with self.db.transaction() as conn:
                    self.outbox.retrigger(conn, event)
                return
            if not self.settings.interactive_execution_enabled:
                return
            context = await self.knowledge.prepare(event, self.context_for(event))
            self.sessions.enqueue(event, access=self.access(event), context=context)
            with self.db.transaction() as conn:
                self.outbox.retrigger(conn, event)
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
        except asyncio.CancelledError:
            retry = True
            raise
        except Exception as error:
            logger.exception("Input failed")
            retry = True  # Preserve pending ingress if the receipt lookup also fails.
            retry = event.text.strip() == "/clear" or self.controls.replayable(event)
            error_code = type(error).__name__
            return False if retry else None
        finally:
            if not retry:
                self.inbox.finish(event.event_id, error=error_code, expected_seq=event.ingest_seq)

    def pre_send(self, envelope, *, stream_cleanup=False):
        if stream_cleanup:
            from elephant.storage.stream_schema import stream_messages
            with self.db.engine.connect() as conn:
                persisted = conn.execute(select(stream_messages.c.envelope_json).where(
                    stream_messages.c.id == envelope.origin_id.removeprefix("qq-stream:"),
                    stream_messages.c.kind == "answer",
                    stream_messages.c.stream_id.is_not(None), stream_messages.c.state == "SENDING",
                )).scalar_one_or_none()
            if persisted != envelope.model_dump_json():
                return False
        subject = PermissionSubject.from_refs(envelope.target, envelope.requester)
        if not self.permissions.check(subject, "notification.send", "conversation:" + envelope.target.key).allowed:
            return False
        if envelope.origin_kind == "chat" and envelope.origin_id.startswith("file:"):
            from elephant.storage.agent_schema import dialogue_inputs

            origin = envelope.origin_id.split(":", 3)
            if len(origin) != 4:
                return False
            _, _, run_id, turn = origin
            with self.db.engine.connect() as conn:
                run = conn.execute(select(runs.c.request_json).where(runs.c.id == run_id,
                    runs.c.role == "leader", runs.c.state.in_(["RUNNING", "FINISHED", "RECOVERED"]))).scalar_one_or_none()
                if not run:
                    return False
                claim = json.loads(run)
                if (claim["purpose"] != "user" or claim.get("target_root_task_id")
                        or claim["task_id"] != envelope.parent_task_id
                        or turn != "turn:" + claim["session_id"] + ":" + claim["source_ids"][0]):
                    return False
                if conn.scalar(select(dialogue_inputs.c.id).where(dialogue_inputs.c.id == claim["source_ids"][0],
                        dialogue_inputs.c.state == "CLEARED")):
                    return False
            event = InboundEvent.model_validate_json(claim["event_json"])
            access = self.access(event)
            return (event.conversation == envelope.target and event.principal == envelope.requester
                    and self.sessions.owner(event) is not None and self.allowed(event)
                    and self.permissions.check(access, "notification.send", "task:" + claim["task_id"]).allowed
                    and self.validate_context(access, claim["context"]))
        if envelope.origin_kind == "chat" and not envelope.origin_id.startswith(("turn:", "progress:", "qq-stream:")):
            controlled = self.controls.delivery_valid(envelope)
            if controlled is not None:
                return controlled
        if envelope.origin_kind == "chat" and envelope.origin_id.startswith("qq-stream:"):
            from elephant.storage.agent_schema import dialogue_inputs
            from elephant.storage.stream_schema import stream_messages, stream_runs

            with self.db.engine.connect() as conn:
                stream = conn.execute(select(stream_messages.c.envelope_json, stream_messages.c.kind, runs)
                    .join(runs, runs.c.id == stream_messages.c.run_id)
                    .join(stream_runs, stream_runs.c.run_id == runs.c.id).where(
                        stream_messages.c.id == envelope.origin_id.removeprefix("qq-stream:"),
                        runs.c.role == "leader")).mappings().first()
                if not stream or stream["envelope_json"] != envelope.model_dump_json():
                    return False
                claim = json.loads(stream["request_json"])
                if claim["purpose"] != "user" or not (claim.get("public_answer_events")
                        or (stream["kind"] == "typing" and claim.get("typing_only"))):
                    return False
                source = conn.execute(select(dialogue_inputs).where(
                    dialogue_inputs.c.id == claim["source_ids"][0],
                    dialogue_inputs.c.dialogue_id == claim["context"]["dialogue_id"])).mappings().first()
            if not source or (not stream_cleanup and source["claimed_attempt_id"] != stream["id"]):
                return False
            if not stream_cleanup and (stream["state"] not in {"RUNNING", "FINISHED"} or (
                    not stream["result_json"] and (stream["lease"] <= self.clock.now().isoformat()
                        or claim["deadline"] <= self.clock.now().isoformat()))):
                return False
            event = InboundEvent.model_validate_json(claim["event_json"])
            return (event.conversation == envelope.target and event.principal == envelope.requester
                    and self.allowed(event) and self.validate_context(self.access(event), claim["context"]))
        origin_id = envelope.origin_id
        if envelope.origin_kind == "chat" and origin_id.startswith("progress:"):
            origin_id = origin_id.split(":", 2)[-1]
        if envelope.origin_kind == "chat" and origin_id.startswith("turn:"):
            from elephant.storage.agent_schema import dialogue_inputs

            from .task_graph import TERMINAL

            session_id, _, source_id = origin_id.removeprefix("turn:").partition(":")
            with self.db.engine.connect() as conn:
                source = conn.execute(select(dialogue_inputs).where(dialogue_inputs.c.id == source_id)).mappings().first()
                if not source or source["state"] == "CLEARED" or envelope.purpose == "progress" and source["handled_at"]:
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
            committed_claim = json.loads(completed["request_json"]) if completed else {}
            mutation = committed_claim.get("memory_mutation", {})
            result = json.loads(completed["result_json"] or "{}") if completed else {}
            if (mutation.get("summary") == envelope.text and result.get("summary") == envelope.text
                    and result.get("agent_authored") is False):
                return self.sessions.owner(event) is not None and self.allowed(event)
            delivery_context = ({key: value for key, value in committed_claim["context"].items() if key != "session_id"}
                if envelope.fixed_publication and committed_claim.get("commit_publication") else {
                    **json.loads(source["context_json"]), "session_id": session_id})
            if settled or not self.validate_context(self.access(event), delivery_context):
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
            event = InboundEvent.model_validate_json(task["event_json"])
            access = self.access(event)
            with self.db.engine.connect() as conn:
                if not stream_cleanup and task["notice_purpose"] == "question":
                    question_ids = notification_question_ids(conn, envelope.origin_id)
                    open_questions = self.tasks.open_input_waits().where(waits.c.id.in_(question_ids))
                    if not conn.execute(open_questions.limit(1)).first():
                        return False
                    question_tasks = conn.execute(select(tasks).join(waits, waits.c.task_id == tasks.c.id)
                        .where(waits.c.id.in_(question_ids))).mappings()
                    if any(not self.tasks.graph.may_read(row["id"], access)
                           or not self.validate_context(access, json.loads(row["context_json"])) for row in question_tasks):
                        return False
                elif not stream_cleanup and task["root_task_id"] and task["notification_run_id"]:
                    root_revision = conn.execute(select(tasks.c.goal_revision).where(
                        tasks.c.id == task["root_task_id"])).scalar_one()
                    if task["notice_revision"] != root_revision:
                        return False
                if not self.knowledge.delivery_valid(conn, task, task):
                    return False
            return (self.permissions.check(access, "notification.send", "conversation:" + envelope.target.key).allowed
                    and self.validate_context(access, json.loads(task["context_json"])))
        return True

    async def tick(self, *, dispatch=True):
        self.restore_silences()
        retry = False
        for event in self.inbox.pending():
            if await self.process(event) is False:
                retry = True
        if retry:
            raise RuntimeError("persisted task control needs replay")
        if dispatch:
            await self.deliver()

    async def deliver(self, *, wait=True):
        await self.stream_delivery.dispatch_once(include_outbox=True, wait=wait)

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
        from elephant.harness.artifacts import cleanup_artifacts
        from elephant.harness.attachments import cleanup_attachment_cache
        def cleanup():
            now = self.clock.now()
            cleanup_artifacts(self.db, self.tasks.archive_root, now)
            cleanup_attachment_cache(Path(self.db.path).resolve().parent / "spool" / "content", self.db, now)
        pending = asyncio.create_task(asyncio.to_thread(cleanup))
        try:
            await asyncio.shield(pending)
        except asyncio.CancelledError:
            await pending
            raise

    def retire_adapters(self):
        """Record explicit failures for work assigned to removed transports."""
        from elephant.storage.action_schema import operations
        from elephant.storage.task_schema import runs
        terminal = {"SUCCEEDED", "PARTIAL", "BLOCKED", "FAILED", "CANCELLED", "EXPIRED"}
        with self.db.transaction() as conn:
            self.outbox.retire_adapters(conn, self.adapters)
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
        self.mail.sender.recover()
        self.stream_delivery.recover()
        self.recovery.recover_processes()
        self.tasks.recover()
        self.recovery.recover_workers()
        self.knowledge.recover_mutations()
        self.leader.recover()
        self.knowledge.recover_mutations()
        self.supervisor.start("ingress", lambda: self.tick(dispatch=False), interval=0.05, critical=True)
        self.supervisor.start("delivery", lambda: self.deliver(wait=False), interval=0.1, critical=True)
        self.supervisor.start("scheduler", self.scheduler.step, interval=0.1, critical=True)
        self.supervisor.start("external_waiters", self.scheduler.waiters.once, interval=1, critical=True)
        self.supervisor.start("schedules", self.schedules.step, interval=1, critical=True)
        self.supervisor.start("mail", self.mail.triggers.step, interval=30)
        self.supervisor.start("memory", self.maintenance, interval=10)
        self.supervisor.start("artifacts", self.cleanup_artifacts, interval=3600)
        while not self.stopping:
            await asyncio.sleep(0.1)

    async def stop(self):
        self.stopping = True
        await self.supervisor.stop()
        await self.stream_delivery.stop()
        if self.scheduler:
            await self.scheduler.close()
        await self.openviking.close()
        await self.model_client.aclose()
