"""Bind memory operations to verified owners and export only native Pi sources."""
import hashlib
import json
from contextlib import nullcontext
from pathlib import Path
from uuid import uuid4

from sqlalchemy import func, insert, or_, select, update

from qqbot.adapters.openviking.service import BACKEND_ERRORS, affected_sources, digest
from qqbot.contracts.harness import resolve_harness
from qqbot.contracts.memory import MemoryCorrection, MemoryDeletion, MemoryScope, MutationReceipt, SourceBatch
from qqbot.dialogue.planner import plan_turn
from qqbot.domain import InboundEvent, Surface, stable_id
from qqbot.domain.execution import TaskGoal, TurnInput
from qqbot.storage.action_schema import operations
from qqbot.storage.agent_schema import agent_events, dialogue_inputs, dialogues, leader_inbox
from qqbot.storage.memory_sync_schema import (
    memory_barriers,
    memory_operations,
    memory_owners,
    memory_sources,
    memory_streams,
)
from qqbot.storage.schema import outbox
from qqbot.storage.stream_schema import stream_messages, stream_runs
from qqbot.storage.task_schema import inputs, notifications, observations, processes, runs, sessions, tasks

from .task_graph import TERMINAL


class KnowledgeRuntime:
    def __init__(self, core):
        self.core, self.memory = core, core.openviking
        self.memory.source_loader = self.load_source

    def scope(self, event, *, workspace_id=None):
        owner = self.core.sessions.owner(event)
        if owner is None:
            return None
        workspace_id = self.core.sessions.validate_workspace(event,
            workspace_id or self.core.sessions.workspace_for(event))
        private = event.conversation.surface in {Surface.PRIVATE, Surface.GUILD_PRIVATE}
        project = workspace_id != "personal"
        scope = MemoryScope(owner_id=owner, workspace_id=workspace_id if project or private else event.conversation.key,
                            audience="project" if project else "private" if private else "group")
        if not private and project and not self.memory.config.group_enabled:
            return None
        if not any((b.owner_id, b.workspace_id) == (scope.owner_id, scope.workspace_id)
                   for b in self.memory.config.bindings):
            return None
        return scope

    def sources_valid(self, event, source_ids, *, workspace_id="personal", conn=None):
        """Deletion barriers revoke raw attachment replay even in a fresh memory epoch."""
        owner = self.core.sessions.owner(event)
        if owner is None or not source_ids:
            return True
        private = event.conversation.surface in {Surface.PRIVATE, Surface.GUILD_PRIVATE}
        scope = MemoryScope(owner_id=owner,
            workspace_id=workspace_id if workspace_id != "personal" or private else event.conversation.key,
            audience="project" if workspace_id != "personal" else "private" if private else "group")
        ids = set(source_ids)
        with (nullcontext(conn) if conn is not None else self.core.db.engine.connect()) as connection:
            rows = connection.execute(select(memory_sources).where(memory_sources.c.scope_id == scope.key,
                or_(memory_sources.c.source_id.in_(ids), memory_sources.c.event_id.in_(ids)))).mappings().all()
            if any(row["state"] in {"FROZEN", "REVOKED"} for row in rows):
                return False
            ids.update(row[key] for row in rows for key in ("source_id", "event_id"))
            return connection.execute(select(memory_barriers.c.id).where(memory_barriers.c.scope_id == scope.key,
                memory_barriers.c.source_id.in_(ids)).limit(1)).first() is None

    def load_source(self, scope, session_id, after, to_seq, branch, projection_version):
        with self.core.db.engine.connect() as conn:
            record = conn.execute(select(sessions).where(sessions.c.id == session_id)).mappings().first()
        event, binding, turns = self._session_context(session_id)
        if not record or event is None or not self.core.allowed(event) or self.scope(event, workspace_id=binding.get("workspace_id", "personal")) != scope:
            raise PermissionError("memory export session is not bound to this owner")
        exported = self._export(record, binding, turns, after=after, projection_version=projection_version)
        events = tuple(e for e in exported["events"] if e.seq <= to_seq)
        return SourceBatch(scope=scope, session_id=session_id, from_seq=after, to_seq=to_seq,
                           events=events, branch=branch, projection_version=projection_version)

    def _session_context(self, session_id):
        with self.core.db.engine.connect() as conn:
            records = conn.execute(select(runs).where(runs.c.session_id == session_id)
                .order_by(runs.c.started_at, runs.c.id)).mappings().all()
            event, binding, turns = None, {}, {}
            for run in records:
                claim = json.loads(run["request_json"])
                task = conn.execute(select(tasks).where(tasks.c.id == run["task_id"])).mappings().first()
                raw_event = claim.get("event_json") or (task["event_json"] if task else None)
                if raw_event:
                    event = InboundEvent.model_validate_json(raw_event)
                binding = claim.get("binding") or (json.loads(task["binding_json"]) if task else binding)
                result = json.loads(run["result_json"]) if run["result_json"] else {}
                evidence = []
                context = json.loads(task["context_json"]) if task else claim.get("context", {})
                workspace_id = claim.get("context", {}).get("workspace_id", context.get("workspace_id", "personal"))
                binding = {**binding, "workspace_id": workspace_id}
                scope = self.scope(event, workspace_id=workspace_id) if event else None
                epoch = conn.execute(select(memory_owners.c.epoch).where(
                    memory_owners.c.id == scope.key)).scalar_one_or_none() if scope else None
                if (event is not None and run["role"] == "worker" and run["state"] == "FINISHED"
                        and result.get("status") == "completed"
                        and self.core.allowed(event) and context.get("memory_epoch", 0) == (epoch or 0)):
                    evidence = list(conn.execute(select(observations.c.id).where(
                        observations.c.task_id == run["task_id"], observations.c.run_id == run["id"],
                        observations.c.id.in_(result.get("observation_refs", ())))).scalars())
                    evidence.extend(conn.execute(select(operations.c.id).where(
                        operations.c.task_id == run["task_id"], operations.c.run_id == run["id"],
                        operations.c.id.in_(result.get("effect_refs", ())), operations.c.state == "VERIFIED")).scalars())
                turns[run["id"]] = {"run_id": run["id"], "role": run["role"], "purpose": run["purpose"],
                    "summary": result.get("summary"), "evidence_refs": evidence}
        return event, binding, turns

    def _export(self, record, binding, turns, **options):
        input_origins = {}
        if any(turn["role"] == "leader" for turn in turns.values()):
            with self.core.db.engine.connect() as conn:
                relays = conn.execute(select(leader_inbox.c.id, agent_events.c.attempt_id)
                    .join(agent_events, leader_inbox.c.event_id == agent_events.c.id)
                    .join(runs, agent_events.c.attempt_id == runs.c.id)
                    .where(or_(leader_inbox.c.target_session_id == record["id"],
                        leader_inbox.c.claimed_attempt_id.in_(turns)), runs.c.role == "worker")).all()
            input_origins = {input_id: ["result:" + run_id] for input_id, run_id in relays}
        return resolve_harness(self.core, binding).export_session(record["session_file"], session_id=record["id"],
            turns=turns, input_origins=input_origins, **options)

    async def prepare(self, event, context):
        scope = self.scope(event, workspace_id=context.get("workspace_id"))
        if scope is None or not self.memory.enabled(scope):
            return context
        query = event.text.strip()[:self.memory.config.max_query_chars]
        if not query:
            return context
        try:
            result = await self.memory.search(scope, query)
        except BACKEND_ERRORS:
            context["memory_status"] = "degraded"
            return context
        context["memory_scope"] = scope.model_dump()
        context["memory_epoch"] = result.memory_epoch
        context["memory_status"] = result.status
        docs = []
        for hit in result.hits[:4]:
            try:
                doc = await self.memory.read(scope, hit.uri)
            except (PermissionError, *BACKEND_ERRORS):
                context["memory_status"] = "degraded"
                continue
            docs.append(doc.model_dump(mode="json"))
        context["retrieved_data"] = docs
        return context

    def valid(self, context):
        if "memory_scope" not in context:
            return True
        scope = MemoryScope.model_validate(context["memory_scope"])
        with self.core.db.engine.connect() as conn:
            row = conn.execute(select(memory_owners).where(memory_owners.c.id == scope.key)).mappings().first()
        return bool(row and row["epoch"] == context.get("memory_epoch") and (row["state"] == "READY" or not context.get("retrieved_data")))

    async def sync(self):
        await self.replan()
        await self.memory.reconcile()
        with self.core.db.engine.connect() as conn:
            records = conn.execute(select(sessions).where(sessions.c.active_run_id.is_(None))).mappings().all()
        for record in records:
            event, binding, turns = self._session_context(record["id"])
            if (event is None or not self.core.allowed(event) or not turns
                    or all(turn["role"] == "skill_review" for turn in turns.values())):
                continue
            scope = self.scope(event, workspace_id=binding.get("workspace_id", "personal"))
            if scope is None or not self.memory.enabled(scope):
                continue
            with self.core.db.engine.connect() as conn:
                projection = getattr(resolve_harness(self.core, binding), "projection_version", "pi-original-v2")
                after = conn.execute(select(memory_streams.c.exported_seq).where(
                    memory_streams.c.session_id == record["id"], memory_streams.c.scope_id == scope.key,
                    memory_streams.c.projection_version == projection)
                    .order_by(memory_streams.c.epoch.desc()).limit(1)).scalar_one_or_none() or 0
            exported = self._export(record, binding, turns, after=after)
            limit = len(exported["events"])
            while limit > 1 and sum(len(event.content.encode()) for event in exported["events"]) > 512 * 1024:
                limit = max(1, limit // 2)
                exported = self._export(record, binding, turns, after=after, limit=limit)
            if exported["next_cursor"] <= after:
                continue
            await self.memory.sync(SourceBatch(scope=scope, session_id=record["id"], from_seq=after,
                to_seq=exported["next_cursor"], events=exported["events"], branch=exported["branch"],
                projection_version=exported.get("projection_version", "pi-original-v2")))

    def invalidate_root(self, conn, root, scope, operation_id, source_ids, session_ids=()):
        root = conn.execute(select(tasks).where(tasks.c.id == root["id"])).mappings().one()
        if root["state"] in TERMINAL | {"CANCEL_REQUESTED"}:
            return
        context = json.loads(root["context_json"])
        previous = context.get("memory_replan_pending") or context.get("memory_replan") or {}
        operation_ids = list(dict.fromkeys([*previous.get("operation_ids", []), operation_id]))
        if operation_id in previous.get("operation_ids", []):
            return
        tree_ids = list(conn.execute(select(tasks.c.id).where(tasks.c.root_task_id == root["id"])).scalars())
        session_ids = [*session_ids, *conn.execute(select(runs.c.session_id).where(runs.c.task_id.in_(tree_ids))).scalars()]
        session_ids.extend([root["coordinator_session_id"], *previous.get("session_ids", [])])
        marker = {"operation_ids": operation_ids, "source_ids": sorted(set(source_ids) | set(previous.get("source_ids", []))),
                  "session_ids": list(dict.fromkeys(item for item in session_ids if item))}
        context["memory_replan_pending"] = marker
        conn.execute(update(tasks).where(tasks.c.id == root["id"]).values(context_json=json.dumps(context),
            state="WAITING_CHILDREN", stop_reason="revoked", wait_json=None, fencing=root["fencing"] + 1))
        children = conn.execute(select(tasks).where(tasks.c.root_task_id == root["id"],
            tasks.c.id != root["id"], tasks.c.parent_task_id == root["id"])).mappings().all()
        for child in children:
            self.core.tasks.graph.cancel_tree(conn, child, reason="revoked")
        notice_ids = list(conn.execute(select(notifications.c.id).where(notifications.c.task_id.in_(tree_ids))).scalars())
        for row in conn.execute(select(outbox).where(outbox.c.state.in_(
                ["PENDING", "RETRYABLE", "WAITING_FOR_TRIGGER", "STREAM_PENDING", "SENDING"]))).mappings():
            envelope = json.loads(row["envelope_json"])
            if envelope.get("origin_kind") == "task" and envelope.get("origin_id") in notice_ids:
                state = "UNKNOWN" if row["state"] == "SENDING" else "FAILED"
                conn.execute(update(outbox).where(outbox.c.id == row["id"]).values(state=state,
                    receipt_json=json.dumps({"state": state, "error_code": "MEMORY_EPOCH_CHANGED"})))
                if state == "UNKNOWN":
                    conn.execute(update(notifications).where(notifications.c.id == envelope["origin_id"]).values(state="UNKNOWN"))
        conn.execute(update(notifications).where(notifications.c.task_id.in_(tree_ids),
            notifications.c.state.in_(["ACCEPTED", "QUEUED", "PENDING", "RETRYABLE", "WAITING_FOR_TRIGGER", "STREAMING"])).values(state="CANCELLED"))
        old_runs = select(runs.c.id).where(runs.c.task_id.in_(tree_ids))
        conn.execute(update(stream_messages).where(stream_messages.c.run_id.in_(old_runs),
            stream_messages.c.state == "SENDING").values(state="UNKNOWN", error="MEMORY_EPOCH_CHANGED"))
        conn.execute(update(stream_messages).where(stream_messages.c.run_id.in_(old_runs),
            stream_messages.c.state.not_in(["FINISHED", "CANCELLED", "ABANDONED", "UNKNOWN"])).values(
                state="ABANDONED", error="MEMORY_EPOCH_CHANGED"))
        conn.execute(update(notifications).where(notifications.c.id.in_(select(stream_messages.c.id).where(
            stream_messages.c.run_id.in_(old_runs), stream_messages.c.state == "UNKNOWN"))).values(state="UNKNOWN"))
        conn.execute(update(stream_runs).where(stream_runs.c.run_id.in_(old_runs),
            stream_runs.c.state.not_in(["DONE", "UNKNOWN", "RECONCILIATION"])).values(
                state="DISABLED", error="MEMORY_EPOCH_CHANGED"))
        conn.execute(update(leader_inbox).where(leader_inbox.c.root_task_id == root["id"],
            leader_inbox.c.handled_at.is_(None)).values(handled_at=self.core.clock.now().isoformat(), output_ref="SUPERSEDED"))

    def delivery_valid(self, conn, task, notice):
        root = self.core.tasks.graph.root(conn, task)
        context = json.loads(root["context_json"])
        if context.get("memory_replan_pending"):
            return False
        if notice.get("notification_state", notice.get("state")) in {"CANCELLED", "UNKNOWN", "FAILED"}:
            return False
        if not context.get("memory_replan"):
            return True
        run_id = notice.get("notification_run_id", notice.get("run_id"))
        raw = conn.execute(select(runs.c.request_json).where(runs.c.id == run_id)).scalar_one_or_none() if run_id else None
        claim = json.loads(raw) if raw else {}
        return claim.get("context", {}).get("memory_epoch") == context.get("memory_epoch")

    async def replan(self):
        with self.core.db.engine.connect() as conn:
            roots = [dict(row) for row in conn.execute(select(tasks).where(tasks.c.kind == "goal",
                tasks.c.state.not_in(TERMINAL | {"CANCEL_REQUESTED"}))).mappings()]
        for root in roots:
            context = json.loads(root["context_json"])
            marker = context.get("memory_replan_pending")
            if not marker and context.get("memory_scope"):
                scope = MemoryScope.model_validate(context["memory_scope"])
                with self.core.db.engine.connect() as conn:
                    epoch = conn.execute(select(memory_owners.c.epoch).where(memory_owners.c.id == scope.key)).scalar_one_or_none()
                    fences = [dict(row) for row in conn.execute(select(memory_barriers).where(
                        memory_barriers.c.scope_id == scope.key, memory_barriers.c.epoch > context.get("memory_epoch", -1))
                        .order_by(memory_barriers.c.epoch)).mappings()]
                    frozen_sources = list(conn.execute(select(memory_sources.c.source_id).where(
                        memory_sources.c.scope_id == scope.key, memory_sources.c.state == "FROZEN")).scalars())
                if epoch is not None and epoch != context.get("memory_epoch") and fences:
                    with self.core.db.transaction() as conn:
                        self.invalidate_root(conn, root, scope, fences[-1]["operation_id"],
                            [item["source_id"] for item in fences if item["source_id"]] + frozen_sources)
                    continue
            if not marker:
                continue
            scope = MemoryScope.model_validate(context["memory_scope"])
            memory_status = await self.memory.status(scope)
            if memory_status["state"] != "READY":
                continue
            with self.core.db.engine.connect() as conn:
                worker_runs = [dict(row) for row in conn.execute(select(runs).where(runs.c.task_id.in_(
                    select(tasks.c.id).where(tasks.c.root_task_id == root["id"])))).mappings()]
                session_runs = [dict(row) for row in conn.execute(select(runs).where(
                    runs.c.session_id.in_(marker["session_ids"]))).mappings()]
                unsettled_child = conn.execute(select(tasks.c.id).where(tasks.c.root_task_id == root["id"],
                    tasks.c.id != root["id"], tasks.c.state.not_in(TERMINAL))).first()
            if unsettled_child or any(row["slot_reserved"] for row in [*worker_runs, *session_runs]):
                continue
            scheduler = getattr(self.core, "scheduler", None)
            if scheduler and not await scheduler.pool.retire_sessions(set(marker["session_ids"])):
                continue
            with self.core.db.engine.connect() as conn:
                stopped = True
                for row in worker_runs:
                    if row["runner_stopped_at"]:
                        continue
                    process = conn.execute(select(processes).where(processes.c.id == row["process_id"],
                        processes.c.epoch == row["process_epoch"], processes.c.generation == row["binding_generation"],
                        processes.c.state == "STOPPED")).mappings().first() if row["process_id"] else None
                    stopped = stopped and bool(process)
                uncertain = conn.execute(select(operations.c.id).where(operations.c.task_id.in_(
                    select(tasks.c.id).where(tasks.c.root_task_id == root["id"])),
                    operations.c.state.in_(["PREPARED", "DISPATCHED", "UNKNOWN", "PARTIAL", "RECONCILIATION"]))).first()
            if not stopped or uncertain or not getattr(self.core.tasks, "job_tree_quiescent", lambda _: True)(root):
                continue
            event = InboundEvent.model_validate_json(root["event_json"])
            source_ids = {event.event_id, *(item["id"] for item in context.get("source_manifest", []))}
            with self.core.db.engine.connect() as conn:
                affected = set(marker["source_ids"]) | set(conn.execute(select(memory_sources.c.source_id).where(
                    memory_sources.c.scope_id == scope.key, memory_sources.c.state == "FROZEN")).scalars())
                business_inputs = [dict(row) for row in conn.execute(select(inputs).where(
                    inputs.c.task_id == root["id"], inputs.c.kind.in_(["followup", "answer"]),
                    inputs.c.actor == root["requester"]).order_by(inputs.c.created_at, inputs.c.id)).mappings()]
                revoked_hashes = set(conn.execute(select(memory_sources.c.content_hash).where(
                    memory_sources.c.scope_id == scope.key, memory_sources.c.source_id.in_(affected))).scalars())
                retained_sources = {event.event_id, *((item.get("source_event_id") or item["source_id"]) for item in business_inputs)}
                sources_valid = self.sources_valid(event, source_ids | retained_sources,
                    workspace_id=context.get("workspace_id", "personal"), conn=conn)
            blocked = not sources_valid or bool(source_ids.intersection(affected)) or any((item.get("source_event_id") or item["source_id"]) in affected or
                hashlib.sha256(item["text"].encode()).hexdigest() in revoked_hashes for item in business_inputs)
            goal = "\n".join([event.text, *(item["text"] for item in business_inputs)])
            fresh = {key: value for key, value in context.items() if key in {
                "limits", "timezone", "model_context_window", "scope", "principal", "preferences", "owner_id",
                "workspace_id", "structured_request"}}
            fresh["source_manifest"] = [item for item in context.get("source_manifest", []) if item["id"] in retained_sources]
            fresh["images"] = [item for item in context.get("images", []) if item.get("source") in retained_sources]
            fresh["files"] = [item for item in context.get("files", []) if item.get("source") in retained_sources]
            if blocked:
                fresh.pop("structured_request", None)
                goal = "原始任务输入已被撤销，无法自动重规划。"
                goal_spec = TaskGoal(text=goal)
                fresh.update(source_manifest=[], images=[], files=[], memory_scope=scope.model_dump(),
                    memory_epoch=memory_status["memory_epoch"], memory_status="blocked", retrieved_data=[])
            else:
                goal_spec, _ = plan_turn(TurnInput(conversation=event.conversation, principal=event.principal,
                    text=goal, source_event_ids=(event.event_id, *((item.get("source_event_id") or item["source_id"]) for item in business_inputs)),
                    anchor_event_id=event.event_id), self.core.sessions.settings, background=True)
                structured = context.get("structured_request")
                if structured:
                    from qqbot.storage.schema import inbox
                    with self.core.db.engine.connect() as conn:
                        raw = conn.execute(select(inbox.c.event_json).where(
                            inbox.c.id == structured["source_event_id"])).scalar_one_or_none()
                    original = InboundEvent.model_validate_json(raw) if raw else None
                    request = structured["request"]
                    encoded = json.dumps(request, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
                    payload = next((segment.payload for segment in original.segments if segment.type == "structured"
                        and segment.original_type == "host_goal_request"), None) if original else None
                    expected_payload = {key: value for key, value in request.items() if key != "goal"}
                    expected_payload["goal_sha256"] = hashlib.sha256(request["goal"].encode()).hexdigest()
                    if (original is None or original.event_id != event.event_id or original.principal != event.principal
                            or original.conversation != event.conversation or payload != expected_payload
                            or original.text != request["goal"]
                            or hashlib.sha256(encoded.encode()).hexdigest() != structured["sha256"]
                            or request["workspace_id"] != fresh.get("workspace_id", "personal")):
                        raise PermissionError("structured replan source cannot be verified")
                    accepted = TaskGoal(text=request["goal"], success_checks=tuple(request["success_checks"]),
                        requested_outputs=tuple(request["requested_outputs"]))
                    goal_spec = goal_spec.model_copy(update={
                        "success_checks": tuple(dict.fromkeys([*accepted.success_checks, *goal_spec.success_checks])),
                        "requested_outputs": tuple(dict.fromkeys([*accepted.requested_outputs, *goal_spec.requested_outputs]))})
                fresh = await self.prepare(event, fresh)
            if not blocked and (fresh.get("memory_status") != "ready" or not self.valid(fresh)):
                continue
            fresh["memory_replan"] = {"operation_ids": marker["operation_ids"],
                "source_ids": sorted(affected), "from_revision": root["goal_revision"],
                "instruction": "依据这条真实用户输入和当前记忆重新规划。旧任务仅提供关联ID和操作ID。新增工作必须使用rework_of_task_id关联已停止的旧子任务。"}
            now = self.core.clock.now().isoformat()
            with self.core.db.transaction() as conn:
                current = conn.execute(select(tasks).where(tasks.c.id == root["id"])).mappings().one()
                # Recall may yield while a real followup changes the accepted source snapshot.
                if (current["goal_revision"] != root["goal_revision"]
                        or current["context_json"] != root["context_json"]
                        or current["fencing"] != root["fencing"]
                        or current["state"] in TERMINAL | {"CANCEL_REQUESTED"}):
                    continue
                revision = current["goal_revision"] + 1
                conn.execute(update(tasks).where(tasks.c.id == root["id"]).values(
                    context_json=json.dumps(fresh), goal=goal, goal_json=goal_spec.model_dump_json(),
                    state="BLOCKED" if blocked else "WAITING_CHILDREN", stop_reason="business_source_revoked" if blocked else None,
                    result_json=None, goal_revision=revision, wait_json=None))
                conn.execute(update(inputs).where(inputs.c.task_id.in_(select(tasks.c.id).where(
                    tasks.c.root_task_id == root["id"])), inputs.c.consumed_at.is_(None)).values(
                        consumed_at=now, state="SUPERSEDED"))
                if not blocked:
                    conn.execute(insert(leader_inbox).values(id=stable_id(root["id"], "memory-replan", str(revision)),
                        target_session_id=current["coordinator_session_id"], root_task_id=root["id"],
                        event_id=marker["operation_ids"][-1], resume_reason="memory_replan", source_revision=revision, created_at=now))
            self.core.tasks.wakeup.set()

    def rotate(self, scope, identity, receipt):
        # A clean Pi history prevents deleted facts surviving in compressed session context.
        with self.core.db.transaction() as conn:
            request_json = conn.execute(select(runs.c.request_json).where(runs.c.id == identity[0])).scalar_one()
            claim = json.loads(request_json)
            if claim.get("memory_mutation"):
                if claim["memory_mutation"]["operation_id"] != receipt.operation_id:
                    raise PermissionError("memory mutation receipt conflicts with its original turn")
                return
            claim["memory_mutation"] = {"summary": "记忆更改已完成。" if receipt.completed else "记忆更改已登记，旧内容已停止召回，后端清理仍在处理。",
                                        **receipt.model_dump(mode="json")}
            conn.execute(update(runs).where(runs.c.id == identity[0]).values(request_json=json.dumps(claim)))
            old_session_ids = []
            dialogue_scope = dialogues.c.workspace_id == (scope.workspace_id if scope.audience == "project" else "personal")
            if scope.audience == "group":
                dialogue_scope = dialogue_scope & (dialogues.c.conversation_id == scope.workspace_id)
            for row in conn.execute(select(dialogues).where(dialogues.c.owner_id == scope.owner_id, dialogue_scope)).mappings():
                old_session_ids.append(row["session_id"])
                replacement = str(uuid4())
                conn.execute(update(dialogues).where(dialogues.c.id == row["id"]).values(session_id=replacement))
                conn.execute(update(tasks).where(tasks.c.origin_dialogue_id == row["id"],
                    tasks.c.kind == "goal").values(coordinator_session_id=replacement))
                conn.execute(update(leader_inbox).where(leader_inbox.c.target_session_id == row["session_id"],
                    leader_inbox.c.handled_at.is_(None)).values(target_session_id=replacement))
            for root in conn.execute(select(tasks).where(tasks.c.owner_id == scope.owner_id,
                    tasks.c.kind == "goal", tasks.c.state.not_in(TERMINAL | {"CANCEL_REQUESTED"}))).mappings():
                if json.loads(root["context_json"]).get("memory_scope") == scope.model_dump():
                    self.invalidate_root(conn, root, scope, receipt.operation_id, receipt.affected_source_ids, old_session_ids)

    def mutation_request(self, scope, claim, call_id, operation, args, session_file, *, projection_version=None):
        event = InboundEvent.model_validate_json(claim["event_json"])
        key = hashlib.sha256((event.event_id + ":" + call_id).encode()).hexdigest()
        if operation == "memory_forget":
            return MemoryDeletion(scope=scope, idempotency_key=key,
                uris=tuple(args.get("uris", ())), source_event_ids=tuple(args.get("source_event_ids", ())))
        after, source = 0, None
        while True:
            _, binding, turns = self._session_context(claim["session_id"])
            exported = self._export({"id": claim["session_id"], "session_file": session_file},
                binding or claim.get("binding", {}), turns, after=after,
                **({"projection_version": projection_version} if projection_version else {}))
            source = next((item for item in exported["events"] if event.event_id in item.origins and item.role == "user"), None)
            if source or not exported["has_more"]:
                break
            after = exported["next_cursor"]
        if source is None:
            raise PermissionError("the original correction statement is not durably exported")
        return MemoryCorrection(scope=scope, uri=args["uri"], session_id=claim["session_id"],
            source=source, idempotency_key=key,
            projection_version=projection_version or exported.get("projection_version", "pi-original-v2"))

    def recover_mutations(self):
        """Read committed receipts for stopped native calls without repeating the mutation."""
        recovered = []
        self.core.sessions.reconciling_inputs.clear()
        with self.core.db.engine.connect() as conn:
            pending = [dict(row) for row in conn.execute(select(dialogue_inputs).where(
                dialogue_inputs.c.handled_at.is_(None))).mappings()]
        for source in pending:
            event = InboundEvent.model_validate_json(source["event_json"])
            context = json.loads(source["context_json"])
            scope = self.scope(event, workspace_id=context.get("workspace_id", "personal"))
            if scope is None or source["owner_id"] != scope.owner_id or not self.core.allowed(event):
                continue
            if context.get("memory_scope") and self.valid(context):
                continue
            with self.core.db.engine.connect() as conn:
                attempts = [dict(row) for row in conn.execute(select(runs).where(runs.c.role == "leader",
                    runs.c.purpose == "user", func.json_extract(runs.c.request_json, "$.source_ids[0]") == source["id"])
                    .order_by(runs.c.started_at)).mappings()]
                active_sessions = set(conn.execute(select(runs.c.session_id).where(runs.c.slot_reserved == 1)).scalars())
                process_rows = {row["id"]: dict(row) for row in conn.execute(select(processes).where(
                    processes.c.id.in_([item["process_id"] for item in attempts if item["process_id"]]))).mappings()}
            if not attempts:
                continue
            stopped = True
            for attempt in attempts:
                process = process_rows.get(attempt["process_id"])
                if process:
                    identity = json.loads(process["identity_json"])
                    matching = process["epoch"] == attempt["process_epoch"] and process["generation"] == attempt["binding_generation"]
                    stopped = stopped and matching and (process["state"] == "STOPPED" or self.core.tasks.stop_verifier(
                        identity.get("launch_task_id", json.loads(attempt["request_json"])["task_id"]), process["id"]))
                else:
                    stopped = stopped and not attempt["process_id"] and not attempt["slot_reserved"] and bool(attempt["runner_stopped_at"])
            matches = []
            for attempt in attempts:
                if not attempt["process_id"]:
                    continue
                claim = json.loads(attempt["request_json"])
                if claim["event_json"] != source["event_json"] or claim["context"]["dialogue_id"] != source["dialogue_id"]:
                    continue
                process = process_rows[attempt["process_id"]]
                path = Path(json.loads(process["identity_json"]).get("actual_session_file") or attempt["session"])
                try:
                    for call in resolve_harness(self.core, claim.get("binding", {})).mutation_calls(
                            path, run_id=attempt["id"], input_ids=claim["source_ids"]):
                        key = hashlib.sha256((event.event_id + ":" + call["id"]).encode()).hexdigest()
                        operation_id = stable_id("memory-mutation", scope.key, key)
                        with self.core.db.engine.connect() as conn:
                            row = conn.execute(select(memory_operations).join(memory_owners,
                                memory_owners.c.id == memory_operations.c.scope_id).where(memory_operations.c.id == operation_id,
                                memory_operations.c.scope_id == scope.key, memory_operations.c.kind == (
                                    "FORGET" if call["name"] == "memory_forget" else "CORRECT"),
                                memory_owners.c.epoch >= memory_operations.c.epoch,
                                memory_owners.c.state != "RESTORE_REQUIRED", memory_operations.c.id.in_(
                                    select(memory_barriers.c.operation_id).where(memory_barriers.c.scope_id == scope.key)))) .mappings().first()
                        if row:
                            refs = json.loads(row["refs_json"])
                            request = self.mutation_request(scope, claim, call["id"], call["name"], call["arguments"], path,
                                projection_version=refs.get("projection_version"))
                            if row["payload_hash"] != digest(request.model_dump(mode="json")):
                                continue
                            receipt = MutationReceipt(operation_id=row["id"], state=row["state"], completed=row["state"] == "APPLIED",
                                memory_epoch=row["epoch"], changed_uris=tuple(refs.get("uris", ())),
                                affected_source_ids=affected_sources(refs), error_type=row["error_type"])
                            matches.append((attempt, claim, receipt))
                except (OSError, ValueError, TypeError, KeyError, AttributeError, PermissionError):
                    continue
            if len(matches) != 1:
                continue
            attempt, claim, receipt = matches[0]
            self.core.sessions.reconciling_inputs.add(source["id"])
            self.rotate(scope, (attempt["id"], attempt["fencing"], attempt["worker"]), receipt)
            if not stopped:
                continue
            reserved = any(item["slot_reserved"] for item in attempts)
            if attempt["session_id"] in active_sessions or reserved:
                continue
            try:
                with self.core.db.transaction() as conn:
                    current = conn.execute(select(runs).where(runs.c.id == attempt["id"])).mappings().one()
                    claim = json.loads(current["request_json"])
                    outcome = self.core.leader.finalize_committed_mutation(conn, current, claim, stopped=True, recovered=True)
            except (PermissionError, ValueError):
                continue
            recovered.append({"run_id": attempt["id"], "operation_id": receipt.operation_id, **outcome})
            self.core.sessions.reconciling_inputs.discard(source["id"])
        return recovered

    async def invoke(self, identity, payload):
        with self.core.db.transaction() as conn:
            run, claim = self.core.leader.lease(conn, *identity)
            event = InboundEvent.model_validate_json(claim["event_json"])
            scope = self.scope(event, workspace_id=claim["context"].get("workspace_id", "personal"))
            if scope is None:
                raise PermissionError("memory has no verified owner scope")
            operation, args = payload["operation"], payload.get("arguments", {})
            action = "memory.read" if operation in {"memory_search", "memory_read"} else "memory.write"
            self.core.leader.authorize(conn, run, claim, {"action": action})
            if operation in {"memory_correct", "memory_forget"} and claim["purpose"] != "user":
                raise PermissionError("memory mutation requires a real user turn")
        if operation == "memory_search":
            return (await self.memory.search(scope, args["query"])).model_dump(mode="json")
        if operation == "memory_read":
            return (await self.memory.read(scope, args["uri"])).model_dump(mode="json")
        if operation not in {"memory_correct", "memory_forget"}:
            raise ValueError("unknown memory tool")
        with self.core.db.engine.connect() as conn:
            record = conn.execute(select(sessions).where(sessions.c.id == claim["session_id"])).mappings().one()
        request = self.mutation_request(scope, claim, payload["call_id"], operation, args, record["session_file"])
        result = await (self.memory.forget(request) if operation == "memory_forget" else self.memory.correct(request))
        self.rotate(scope, identity, result)
        return {**result.model_dump(mode="json"), "waiting": True, "end_turn": True}
