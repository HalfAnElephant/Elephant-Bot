"""Bind memory operations to verified owners and export only native Pi sources."""
import hashlib
import json
from uuid import uuid4

from sqlalchemy import insert, select, update

from qqbot.adapters.openviking.service import BACKEND_ERRORS
from qqbot.adapters.pi.sessions import export_session
from qqbot.contracts.memory import MemoryCorrection, MemoryDeletion, MemoryScope, SourceBatch
from qqbot.dialogue.planner import plan_turn
from qqbot.domain import InboundEvent, Surface, stable_id
from qqbot.domain.execution import TaskGoal, TurnInput
from qqbot.storage.action_schema import operations
from qqbot.storage.agent_schema import dialogues, leader_inbox
from qqbot.storage.memory_sync_schema import memory_barriers, memory_owners, memory_sources, memory_streams
from qqbot.storage.schema import outbox
from qqbot.storage.stream_schema import stream_messages, stream_runs
from qqbot.storage.task_schema import inputs, notifications, processes, runs, sessions, tasks

from .task_graph import TERMINAL


class KnowledgeRuntime:
    def __init__(self, core):
        self.core, self.memory = core, core.openviking
        self.memory.source_loader = self.load_source

    def scope(self, event):
        owner = self.core.sessions.owner(event)
        if owner is None:
            return None
        private = event.conversation.surface in {Surface.PRIVATE, Surface.GUILD_PRIVATE}
        scope = MemoryScope(owner_id=owner, workspace_id="personal" if private else event.conversation.key,
                            audience="private" if private else "group")
        if not any((b.owner_id, b.workspace_id) == (scope.owner_id, scope.workspace_id)
                   for b in self.memory.config.bindings):
            return None
        return scope

    def load_source(self, scope, session_id, after, to_seq, branch, projection_version):
        with self.core.db.engine.connect() as conn:
            claim_json = conn.execute(select(runs.c.request_json).where(runs.c.session_id == session_id,
                runs.c.role == "leader").order_by(runs.c.started_at.desc()).limit(1)).scalar_one_or_none()
            record = conn.execute(select(sessions).where(sessions.c.id == session_id)).mappings().first()
        claim = json.loads(claim_json) if claim_json else {}
        event = InboundEvent.model_validate_json(claim["event_json"]) if claim else None
        if not record or event is None or self.scope(event) != scope:
            raise PermissionError("memory export session is not bound to this owner")
        exported = export_session(record["session_file"], session_id=session_id, after=after)
        events = tuple(e for e in exported["events"] if e.seq <= to_seq)
        return SourceBatch(scope=scope, session_id=session_id, from_seq=after, to_seq=to_seq,
                           events=events, branch=branch, projection_version=projection_version)

    async def prepare(self, event, context):
        scope = self.scope(event)
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
        with self.core.db.engine.connect() as conn:
            records = conn.execute(select(sessions, dialogues.c.owner_id).join(dialogues,
                dialogues.c.session_id == sessions.c.id).where(sessions.c.active_run_id.is_(None))).mappings().all()
        for record in records:
            with self.core.db.engine.connect() as conn:
                raw = conn.execute(select(runs.c.request_json).where(runs.c.session_id == record["id"],
                    runs.c.role == "leader").order_by(runs.c.started_at.desc()).limit(1)).scalar_one_or_none()
            if not raw:
                continue
            claim = json.loads(raw)
            event = InboundEvent.model_validate_json(claim["event_json"])
            scope = self.scope(event)
            if scope is None or not self.memory.enabled(scope):
                continue
            with self.core.db.engine.connect() as conn:
                after = conn.execute(select(memory_streams.c.exported_seq).where(
                    memory_streams.c.session_id == record["id"], memory_streams.c.scope_id == scope.key)
                    .order_by(memory_streams.c.epoch.desc()).limit(1)).scalar_one_or_none() or 0
            exported = export_session(record["session_file"], session_id=record["id"], after=after)
            if exported["next_cursor"] <= after:
                continue
            await self.memory.sync(SourceBatch(scope=scope, session_id=record["id"], from_seq=after,
                to_seq=exported["next_cursor"], events=exported["events"], branch=exported["branch"]))
        await self.memory.reconcile()

    def invalidate_root(self, conn, root, scope, operation_id, source_ids, session_ids=()):
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
                if epoch is not None and epoch != context.get("memory_epoch") and fences:
                    with self.core.db.transaction() as conn:
                        self.invalidate_root(conn, root, scope, fences[-1]["operation_id"],
                            [item["source_id"] for item in fences if item["source_id"]])
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
            blocked = bool(source_ids.intersection(marker["source_ids"]))
            with self.core.db.engine.connect() as conn:
                business_inputs = [dict(row) for row in conn.execute(select(inputs).where(
                    inputs.c.task_id == root["id"], inputs.c.kind.in_(["followup", "answer"]),
                    inputs.c.actor == root["requester"]).order_by(inputs.c.created_at, inputs.c.id)).mappings()]
                revoked_hashes = set(conn.execute(select(memory_sources.c.content_hash).where(
                    memory_sources.c.scope_id == scope.key, memory_sources.c.source_id.in_(marker["source_ids"]))).scalars())
            blocked = blocked or any(item["source_id"] in marker["source_ids"] or
                hashlib.sha256(item["text"].encode()).hexdigest() in revoked_hashes for item in business_inputs)
            goal = "\n".join([event.text, *(item["text"] for item in business_inputs)])
            fresh = {key: value for key, value in context.items() if key in {
                "limits", "timezone", "model_context_window", "scope", "principal", "preferences", "owner_id"}}
            fresh["source_manifest"] = [item for item in context.get("source_manifest", []) if item["id"] == event.event_id]
            fresh["images"] = [item for item in context.get("images", []) if item.get("source") == event.event_id]
            if blocked:
                goal = "原始任务输入已被撤销，无法自动重规划。"
                goal_spec = TaskGoal(text=goal)
                fresh.update(source_manifest=[], images=[], memory_scope=scope.model_dump(),
                    memory_epoch=memory_status["memory_epoch"], memory_status="blocked", retrieved_data=[])
            else:
                goal_spec, _ = plan_turn(TurnInput(conversation=event.conversation, principal=event.principal,
                    text=goal, source_event_ids=(event.event_id, *(item["source_id"] for item in business_inputs)),
                    anchor_event_id=event.event_id), self.core.sessions.settings, background=True)
                fresh = await self.prepare(event, fresh)
            if not blocked and (fresh.get("memory_status") != "ready" or not self.valid(fresh)):
                continue
            fresh["memory_replan"] = {"operation_ids": marker["operation_ids"],
                "source_ids": marker["source_ids"], "from_revision": root["goal_revision"],
                "instruction": "依据这条真实用户输入和当前记忆重新规划。旧任务仅提供关联ID和操作ID。新增工作必须使用rework_of_task_id关联已停止的旧子任务。"}
            now = self.core.clock.now().isoformat()
            with self.core.db.transaction() as conn:
                current = conn.execute(select(tasks).where(tasks.c.id == root["id"])).mappings().one()
                if json.loads(current["context_json"]).get("memory_replan_pending") != marker:
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
            claim["memory_mutation"] = {"summary": "记忆更改已完成。" if receipt.completed else "记忆更改已登记，旧内容已停止召回，后端清理仍在处理。",
                                        **receipt.model_dump(mode="json")}
            conn.execute(update(runs).where(runs.c.id == identity[0]).values(request_json=json.dumps(claim)))
            old_session_ids = []
            for row in conn.execute(select(dialogues).where(dialogues.c.owner_id == scope.owner_id)).mappings():
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

    async def invoke(self, identity, payload):
        with self.core.db.transaction() as conn:
            run, claim = self.core.leader.lease(conn, *identity)
            event = InboundEvent.model_validate_json(claim["event_json"])
            scope = self.scope(event)
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
        key = hashlib.sha256((event.event_id + ":" + payload["call_id"]).encode()).hexdigest()
        if operation == "memory_forget":
            result = await self.memory.forget(MemoryDeletion(scope=scope, idempotency_key=key,
                uris=tuple(args.get("uris", ())), source_event_ids=tuple(args.get("source_event_ids", ()))))
        elif operation == "memory_correct":
            with self.core.db.engine.connect() as conn:
                record = conn.execute(select(sessions).where(sessions.c.id == claim["session_id"])).mappings().one()
            after, source = 0, None
            while True:
                exported = export_session(record["session_file"], session_id=claim["session_id"], after=after)
                source = next((item for item in exported["events"] if event.event_id in item.origins and item.role == "user"), None)
                if source or not exported["has_more"]:
                    break
                after = exported["next_cursor"]
            if source is None:
                raise PermissionError("the original correction statement is not durably exported")
            result = await self.memory.correct(MemoryCorrection(scope=scope, uri=args["uri"],
                session_id=claim["session_id"], source=source, idempotency_key=key))
        else:
            raise ValueError("unknown memory tool")
        self.rotate(scope, identity, result)
        return {**result.model_dump(mode="json"), "waiting": True, "end_turn": True}
