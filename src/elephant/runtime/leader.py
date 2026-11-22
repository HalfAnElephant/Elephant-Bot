"""Short Leader attempts over the same Harness; the host owns goals and delivery."""
import asyncio
import fcntl
import json
from contextlib import contextmanager
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path

from sqlalchemy import func, insert, or_, select, update

from elephant.domain import InboundEvent, MediaSegment, stable_id
from elephant.domain.agents import TaskInputSpec
from elephant.domain.tasks import Artifact, TaskResult, validate_user_facing_text
from elephant.harness.artifacts import copy_archived_artifact
from elephant.jobs.streaming import PUBLIC_KINDS, reserve_stream_order, validate_public_event
from elephant.storage.action_schema import operations
from elephant.storage.agent_schema import dialogue_inputs, dialogues, focus, leader_events, leader_inbox
from elephant.storage.schema import inbox, outbox, reply_counters
from elephant.storage.stream_schema import stream_runs
from elephant.storage.task_schema import (
    events,
    inputs,
    model_requests,
    notification_question_ids,
    observations,
    processes,
    runs,
    sessions,
    tasks,
    waits,
)

from .control import task_references
from .task_graph import TERMINAL


class PendingOperationInput(PermissionError):
    def __init__(self, input_ids):
        super().__init__("Review pending user input before starting an external operation")
        self.input_ids = input_ids


class LeaderRuntime:
    def __init__(self, core, session_service):
        self.core, self.sessions = core, session_service
        self.service, self.db, self.clock = core.tasks, core.db, core.clock
        self._review_marks = {}
        from .compaction import ContextMaintenance
        self.compaction = ContextMaintenance(self)
        self.sessions.context_maintenance = self.compaction

    def observe_operation(self, identity, operation, payload, result=None):
        self.sessions.learning.observe(identity, operation, payload or {}, result)

    def claim_review(self, worker_id):
        return self.sessions.learning.claim_review(worker_id)

    def lease(self, conn, run_id, fencing, worker_id, *, settling=False):
        run = conn.execute(select(runs).where(runs.c.id == run_id)).mappings().first()
        if (not run or run["role"] not in {"leader", "skill_review"} or run["worker"] != worker_id
                or run["fencing"] != fencing or not run["slot_reserved"]
                or run["lease"] <= self.clock.now().isoformat()
                or (run["state"] == "SETTLING" and not settling)):
            raise PermissionError("stale Leader attempt")
        return run, json.loads(run["request_json"])

    def authorize(self, conn, run, claim, payload, *, ending_wait=False, reporting_failure=False, compaction_saved=False):
        event = InboundEvent.model_validate_json(claim["event_json"])
        if not self.core.allowed(event) or run["state"] != "RUNNING":
            raise PermissionError("Leader input is no longer authorized")
        if claim["purpose"] == "user" and not claim.get("memory_mutation") and conn.scalar(
                select(dialogue_inputs.c.id).where(dialogue_inputs.c.id == claim["source_ids"][0],
                    dialogue_inputs.c.state == "CLEARED")):
            raise PermissionError("Leader input was cleared")
        if not claim.get("memory_mutation") and not reporting_failure and claim["deadline"] <= self.clock.now().isoformat():
            raise PermissionError("Leader turn deadline expired")
        root_id = claim.get("target_root_task_id")
        if root_id:
            root = conn.execute(select(tasks).where(tasks.c.id == root_id)).mappings().one()
            if root["state"] in TERMINAL | {"CANCEL_REQUESTED"} or (
                    root["goal_revision"] != claim["basis_goal_revision"] and not ending_wait):
                raise PermissionError("Leader target version is no longer current")
        action = payload.get("action", "task.execute")
        if claim["purpose"] == "compaction":
            if payload.get("effect", "read") != "read" or action not in {"task.execute", "model.request"}:
                raise PermissionError("maintenance cannot execute business tools")
            if not compaction_saved and self.compaction.foreground(conn):
                raise PermissionError("maintenance yielded to foreground input")
        effect = payload.get("effect", "read")
        if run["role"] == "skill_review" and action not in {"model.request", "task.execute", "skills.read", "skills.write"}:
            raise PermissionError("skill review cannot execute business tools")
        access = self.sessions.access_for_claim(claim)
        mutation = claim.get("memory_mutation")
        if mutation and (action != "task.execute" or effect != "read"):
            raise PermissionError("memory mutation ended this Leader turn")
        validator = getattr(self.core, "validate_context", None)
        if not mutation and validator and not validator(access, claim["context"]):
            raise PermissionError("Leader context sources are no longer authorized")
        history = conn.scalar(select(sessions.c.manifest_json).where(sessions.c.id == claim["session_id"]))
        if not mutation and validator and history and not validator(access, json.loads(history)):
            raise PermissionError("Leader session sources are no longer authorized")
        if not mutation and root_id and not self.service.graph.handoff_valid(root):
            raise PermissionError("Leader target handoff sources are no longer authorized")
        personal = action.startswith(("skills.", "memory."))
        if effect != "read" and not personal:
            if claim["purpose"] == "user" and not root_id and run["role"] == "leader":
                if self.sessions.owner(event) is None:
                    raise PermissionError("Leader business execution requires a verified owner binding")
            else:
                if not claim.get("takeover") or not root_id:
                    raise PermissionError("Leader business execution requires a stopped task takeover")
                self.service.graph.check_authority(conn, root, payload)
        if personal and self.sessions.owner(event) is None:
            raise PermissionError("personal knowledge requires a verified owner binding")
        if getattr(self.core, "mail", None) is not None:
            self.core.mail.authorize_context(access, claim["context"], action,
                payload.get("resources") or ["conversation:" + access.conversation.key])
        for resource in payload.get("resources") or ["conversation:" + access.conversation.key]:
            self.service.permissions.require(access, action, resource)
        if effect != "read":
            if claim.get("yielding_for_inputs"):
                raise PermissionError("Leader turn is yielding for pending user input")
            pending = self.unreviewed_input_ids(conn, claim, claim.get("steering_seen", ()))
            if pending:
                raise PendingOperationInput(pending)
        return {"allowed": True, "policy_version": self.service.permissions.policy_version}

    def file_delivery_scope(self, identity, conn, *, write=True):
        run, claim = self.lease(conn, *identity)
        event = InboundEvent.model_validate_json(claim["event_json"])
        root_id = claim.get("target_root_task_id")
        if not root_id and (claim["purpose"] != "user" or self.sessions.owner(event) is None):
            raise PermissionError("QQ files require a verified Leader user turn or task")
        if write and root_id:
            task = conn.execute(select(tasks).where(tasks.c.id == root_id)).mappings().one()
            if self.service._silent_schedule(task):
                raise PermissionError("silent schedules cannot send QQ messages")
        self.authorize(conn, run, claim, {"action": "notification.send",
            "resources": ["conversation:" + event.conversation.key, "task:" + claim["task_id"]],
            "effect": "write" if write else "read"})
        turn = None if root_id else "turn:" + claim["session_id"] + ":" + claim["source_ids"][0]
        return claim["task_id"], event, turn

    def authorize_call(self, identity, payload):
        with self.defer_blocked_operation(identity), self.db.engine.connect() as conn:
            conn.exec_driver_sql("BEGIN")
            run, claim = self.lease(conn, *identity)
            return self.authorize(conn, run, claim, payload)

    @contextmanager
    def defer_blocked_operation(self, identity):
        try:
            yield
        except PendingOperationInput as error:
            # The rejected authorization transaction must roll back before recording the deferral.
            with self.db.transaction() as conn:
                run, claim = self.lease(conn, *identity)
                old = claim.get("deferred_goal_inputs", ())
                pending = list(dict.fromkeys([*old, *error.input_ids]))
                if pending != old:
                    claim["deferred_goal_inputs"] = pending
                    conn.execute(update(runs).where(runs.c.id == run["id"]).values(
                        request_json=json.dumps(claim, ensure_ascii=False)))
            raise

    def _persist_input_dependencies(self, conn, source_id, pending):
        source = dialogue_inputs.c.id == source_id
        context = json.loads(conn.scalar(select(dialogue_inputs.c.context_json).where(source)))
        context["leader_turn"]["wait_for_inputs"] = pending
        conn.execute(update(dialogue_inputs).where(source).values(context_json=json.dumps(context, ensure_ascii=False)))

    def _persist_turn_budget(self, conn, run, claim, used):
        if claim["purpose"] == "user":
            source = conn.execute(select(dialogue_inputs).where(dialogue_inputs.c.id == claim["source_ids"][0],
                dialogue_inputs.c.claimed_attempt_id == run["id"])).mappings().first()
            if source:
                context = json.loads(source["context_json"])
                turn = context.setdefault("leader_turn", {"plan": claim["plan"], "deadline": claim["deadline"]})
                turn["used"] = used
                conn.execute(update(dialogue_inputs).where(dialogue_inputs.c.id == source["id"])
                    .values(context_json=json.dumps(context, ensure_ascii=False)))
        elif run["role"] == "skill_review":
            from elephant.storage.learning_schema import learning_sessions
            source = conn.execute(select(learning_sessions).where(
                learning_sessions.c.claimed_attempt_id == run["id"])).mappings().first()
            if source and source["pending_json"]:
                pending = json.loads(source["pending_json"])
                pending["used"] = used
                conn.execute(update(learning_sessions).where(learning_sessions.c.session_id == source["session_id"])
                    .values(pending_json=json.dumps(pending, ensure_ascii=False)))

    def _account_turn(self, conn, run, claim, *, through=None):
        if claim["purpose"] != "user" and run["role"] != "skill_review":
            return
        used = json.loads(run["budget_json"])
        now = self.clock.now()
        elapsed = max(0, ((through or now) - datetime.fromisoformat(run["accounted_at"])).total_seconds()) if run["accounted_at"] else 0
        used["active_seconds"] = min(claim["plan"]["budget"]["active_seconds"], used.get("active_seconds", 0) + elapsed)
        conn.execute(update(runs).where(runs.c.id == run["id"]).values(budget_json=json.dumps(used), accounted_at=now.isoformat()))
        self._persist_turn_budget(conn, run, claim, used)

    def steering_candidates(self, conn, claim):
        if not claim.get("steer_until") or claim.get("steer_without_supplements"):
            return []
        trigger = InboundEvent.model_validate_json(claim["event_json"])
        access = self.sessions.access_for_claim(claim)
        offered = claim.get("steered_inputs", ())
        rows = conn.execute(select(dialogue_inputs).outerjoin(leader_events,
            (leader_events.c.source_kind == "user") & (leader_events.c.source_id == dialogue_inputs.c.id)).where(
            dialogue_inputs.c.dialogue_id == claim["context"]["dialogue_id"],
            dialogue_inputs.c.handled_at.is_(None), dialogue_inputs.c.claimed_attempt_id.is_(None),
            dialogue_inputs.c.created_at <= claim["steer_until"],
            func.json_extract(dialogue_inputs.c.event_json, "$.reply_to") == trigger.platform_message_id,
            dialogue_inputs.c.id.in_(offered) if offered else True,
        ).order_by(leader_events.c.seq.nulls_last(), dialogue_inputs.c.created_at, dialogue_inputs.c.id).limit(8)).mappings()
        candidates = []
        for row in rows:
            event = InboundEvent.model_validate_json(row["event_json"])
            if (event.principal != trigger.principal or event.conversation != trigger.conversation or not event.text
                    or any(segment.type not in {"text", "reply", "mention"} for segment in event.segments) or not self.core.allowed(event)):
                continue
            context = json.loads(row["context_json"])
            validator = getattr(self.core, "validate_context", None)
            if validator and not validator(access, context):
                continue
            candidates.append({"id": row["id"], "text":
                "这是一条同会话的新用户消息。若它补充本轮请求，请处理它，并在 task_outcome 的 covered_input_ids 中登记其 ID。"
                "若是独立请求，不要回答它，在 task_outcome 的 deferred_input_ids 中登记它，并继续回答本轮原请求。若用 tasks_send_input 修订任务，source_input_id 填这条补充的 ID。\n" + event.text})
        return candidates

    def unrouted_input_ids(self, conn, claim, *, linked_only=False):
        if not claim.get("steer_until"):
            return []
        event = InboundEvent.model_validate_json(claim["event_json"])
        primary_seq = conn.scalar(select(inbox.c.seq).where(inbox.c.id == event.event_id))
        selected = conn.scalar(select(focus.c.dialogue_id).where(
            focus.c.key == stable_id(event.conversation.key, event.principal.key)))
        if primary_seq is None or selected != claim["context"]["dialogue_id"]:
            return []
        routed = select(dialogue_inputs.c.id).where(dialogue_inputs.c.id == inbox.c.id).exists()
        pending = conn.execute(select(inbox.c.event_json).where(
            inbox.c.conversation_id == event.conversation.key, inbox.c.principal_id == event.principal.key,
            inbox.c.seq > primary_seq, inbox.c.event_type == "message.created", inbox.c.state == "PENDING",
            ~routed, (func.json_extract(inbox.c.event_json, "$.reply_to") == event.platform_message_id)
            if linked_only else True)).scalars()
        return [new.event_id for new in map(InboundEvent.model_validate_json, pending)
            if (new.text or any(isinstance(segment, MediaSegment) for segment in new.segments))
            and self.core.allowed(new) and not new.sender_is_bot and (
                new.conversation.surface in {"private", "guild_private"} or new.mentions_bot or new.reply_to)]

    def unreviewed_input_ids(self, conn, claim, seen):
        if not claim.get("steer_until"):
            return []
        primary = conn.scalar(select(leader_events.c.seq).where(
            leader_events.c.source_kind == "user", leader_events.c.source_id == claim["source_ids"][0]))
        ids = self.unrouted_input_ids(conn, claim)
        if primary is not None:
            pending = conn.execute(select(dialogue_inputs.c.event_json).join(leader_events,
                (leader_events.c.source_kind == "user") & (leader_events.c.source_id == dialogue_inputs.c.id)).where(
                dialogue_inputs.c.dialogue_id == claim["context"]["dialogue_id"],
                leader_events.c.seq > primary, dialogue_inputs.c.handled_at.is_(None),
                dialogue_inputs.c.claimed_attempt_id.is_(None), dialogue_inputs.c.id.not_in(seen))).scalars()
            ids.extend(event.event_id for raw in pending
                if self.core.allowed(event := InboundEvent.model_validate_json(raw)))
        return ids

    def receipt(self, conn, run, claim, payload):
        expected = set(claim["source_ids"]) | set(claim.get("task_input_ids", []))
        confirmed = payload.get("input_ids")
        offered = set(claim.get("steered_inputs", []))
        if (payload.get("session_id") != claim["session_id"] or not isinstance(confirmed, list)
                or not all(isinstance(item, str) for item in confirmed)
                or not expected <= set(confirmed) <= expected | offered
                or not isinstance(payload.get("session_revision"), str)
                or payload.get("run_id") != run["id"]):
            raise PermissionError("session receipt does not match the claimed inputs")
        kind = payload.get("kind")
        if kind not in {"input_persisted", "input_consumed", "turn_finished", "run_yielded"}:
            raise ValueError("invalid session receipt kind")
        if offered and kind != "input_persisted":
            seen = set(claim.get("steering_seen", [])) | (set(confirmed) & offered)
            claim["steering_seen"] = sorted(seen)
            claim["observed_event_seq"] = conn.execute(select(func.max(leader_events.c.seq)).where(
                leader_events.c.dialogue_id == claim["context"]["dialogue_id"],
                leader_events.c.source_kind == "user",
                leader_events.c.source_id.in_([*claim["source_ids"], *seen]))).scalar_one()
            manifest = [item for raw in conn.execute(select(dialogue_inputs.c.context_json).where(
                dialogue_inputs.c.id.in_(seen))).scalars() for item in json.loads(raw).get("source_manifest", ())]
            if manifest:
                claim["context"]["steering_sources"] = True
                graph = self.service.graph
                graph.merge_source_dependencies(claim["context"], (), manifest)
                history = conn.execute(select(sessions.c.manifest_json).where(
                    sessions.c.id == claim["session_id"])).scalar_one_or_none()
                if history:
                    history = json.loads(history)
                    history.update(steering_sources=True, workspace_id=claim["context"].get("workspace_id", "personal"))
                    graph.merge_source_dependencies(history, (), manifest)
                    conn.execute(update(sessions).where(sessions.c.id == claim["session_id"]).values(
                        manifest_json=json.dumps(history, ensure_ascii=False)))
            conn.execute(update(runs).where(runs.c.id == run["id"]).values(
                request_json=json.dumps(claim, ensure_ascii=False)))
        cursor = claim["context"].get("delivery_history_cursor")
        if kind != "input_persisted" and cursor and payload.get("delivery_history_cursor") == cursor:
            stored = conn.scalar(select(sessions.c.manifest_json).where(sessions.c.id == claim["session_id"]))
            if stored:
                history = json.loads(stored)
                history["delivery_history_cursor"] = max(cursor, history.get("delivery_history_cursor", 0))
                conn.execute(update(sessions).where(sessions.c.id == claim["session_id"])
                    .values(manifest_json=json.dumps(history, ensure_ascii=False)))
        now = self.clock.now().isoformat()
        table = dialogue_inputs if claim["purpose"] == "user" else leader_inbox
        values = {"persisted_at": now}
        if kind in {"input_consumed", "turn_finished", "run_yielded"} and table is dialogue_inputs:
            values["consumed_at"] = now
        conn.execute(update(table).where(table.c.id.in_(claim["source_ids"]), table.c.claimed_attempt_id == run["id"]).values(**values))
        if claim.get("task_input_ids") and kind != "input_persisted":
            conn.execute(update(inputs).where(inputs.c.id.in_(claim["task_input_ids"]),
                inputs.c.task_id == claim["target_root_task_id"],
                inputs.c.goal_revision <= claim["basis_goal_revision"]).values(
                    persisted_at=now, consumed_at=now, state="CONSUMED"))
        if kind in {"turn_finished", "run_yielded"}:
            if payload.get("inflight") != 0 or payload.get("tools_paired") is not True:
                raise PermissionError("session still has unfinished activity")
            conn.execute(update(runs).where(runs.c.id == run["id"]).values(receipt_json=json.dumps(payload, ensure_ascii=False)))
        return {"accepted": True}

    def reserve(self, conn, run, claim, payload):
        if claim.get("memory_mutation"):
            raise PermissionError("memory mutation ended this Leader turn")
        request_id, kind = payload["request_id"], payload["kind"]
        if claim["purpose"] == "compaction" and kind != "compaction":
            raise PermissionError("maintenance only permits compaction requests")
        if kind not in {"business", "completion", "compaction", "notification", "tool"}:
            raise ValueError("unknown budget kind")
        previous = conn.execute(select(model_requests).where(model_requests.c.id == request_id)).mappings().first()
        if previous:
            if previous["run_id"] != run["id"] or previous["kind"] != kind:
                raise ValueError("request budget identity conflict")
            return {"accepted": True}
        self.authorize(conn, run, claim, {"action": "task.execute"})
        used = json.loads(run["budget_json"])
        key = "tool_calls" if kind == "tool" else "model_calls"
        budget = claim["plan"]["budget"]
        limit = budget[key] - (budget.get("completion_reserve", 1) if kind in {"business", "compaction", "notification"} else 0)
        if used.get(key, 0) >= limit:
            raise ValueError("Leader turn budget exhausted")
        if claim.get("target_root_task_id"):
            root = conn.execute(select(tasks).where(tasks.c.id == claim["target_root_task_id"])).mappings().one()
            root_used = json.loads(root["budget_json"])
            root_used[key] = root_used.get(key, 0) + 1
            conn.execute(update(tasks).where(tasks.c.id == root["id"]).values(budget_json=json.dumps(root_used)))
        used[key] = used.get(key, 0) + 1
        conn.execute(update(runs).where(runs.c.id == run["id"]).values(budget_json=json.dumps(used)))
        self._persist_turn_budget(conn, run, claim, used)
        conn.execute(insert(model_requests).values(id=request_id, run_id=run["id"], task_id=claim["task_id"],
            kind=kind, state="RESERVED", created_at=self.clock.now().isoformat()))
        remaining = limit - used[key]
        return {"accepted": True, "remaining": remaining}

    def _task_snapshots(self, conn, run, claim, source_tasks, access, *, include_child_sources=False):
        graph = self.service.graph
        snapshots, cited = [], set()
        for task in source_tasks:
            snapshot = graph.status(conn, task, access=access)
            snapshots.append(snapshot)
            if graph.merge_snapshot_dependencies(claim["context"], task, snapshot):
                cited.add(task["id"])
            children = {child["id"]: child for child in snapshot["children"]
                        if include_child_sources and not child.get("rework_required")}
            if children:
                for child in conn.execute(select(tasks.c.id, tasks.c.context_json).where(
                        tasks.c.id.in_(children))).mappings():
                    if graph.merge_snapshot_dependencies(claim["context"], child, children[child["id"]]):
                        cited.add(child["id"])
        if cited:
            context = claim["context"]
            context["published_task_sources"] = sorted(set(context.get("published_task_sources", ())) | cited)
            conn.execute(update(runs).where(runs.c.id == run["id"]).values(
                request_json=json.dumps(claim, ensure_ascii=False)))
            history = conn.execute(select(sessions.c.manifest_json).where(
                sessions.c.id == claim["session_id"])).scalar_one_or_none()
            if history is not None:
                history = json.loads(history)
                graph.merge_source_dependencies(history, claim["context"]["task_sources"],
                    claim["context"].get("source_manifest", ()))
                history["published_task_sources"] = sorted(set(history.get("published_task_sources", ())) | cited)
                conn.execute(update(sessions).where(sessions.c.id == claim["session_id"]).values(
                    manifest_json=json.dumps(history, ensure_ascii=False)))
        return snapshots

    def task_tool(self, conn, run, claim, payload):
        if claim.get("memory_mutation"):
            raise PermissionError("memory mutation ended this Leader turn")
        operation, args = payload["operation"], payload.get("arguments", {})
        access = self.sessions.access_for_claim(claim)
        event = InboundEvent.model_validate_json(claim["event_json"])
        owner = self.sessions.owner(event)
        if not owner or run["role"] != "leader":
            raise PermissionError("task orchestration requires the verified owner Leader")
        purpose = claim["purpose"]
        root_id = claim.get("target_root_task_id")
        graph = self.service.graph
        self.authorize(conn, run, claim, {"action": "task.execute"})
        if operation in {"tasks.submit", "tasks.cancel", "tasks.send_input"}:
            deferred = {"accepted": False, "deferred": True, "reason": "pending_user_input"}
            if claim.get("yielding_for_inputs"):
                return {**deferred, "waiting": True}
            pending = self.unreviewed_input_ids(conn, claim, claim.get("steering_seen", ()))
            if pending or claim.pop("deferred_goal_inputs", None):
                if pending:
                    claim["deferred_goal_inputs"] = pending
                    claim.setdefault("input_deferred_at", self.clock.now().isoformat())
                    if operation == "tasks.submit":
                        steerable = {item["id"] for item in self.steering_candidates(conn, claim)}
                        steerable.update(self.unrouted_input_ids(conn, claim, linked_only=True))
                        if steerable.isdisjoint(pending):
                            claim["yielding_for_inputs"] = True
                            deferred["waiting"] = True
                conn.execute(update(runs).where(runs.c.id == run["id"]).values(
                    request_json=json.dumps(claim, ensure_ascii=False)))
            if pending:
                return deferred
        if operation == "tasks.submit":
            if purpose != "user":
                raise PermissionError("only real user turns can create a root goal")
            dialogue = conn.execute(select(dialogues).where(dialogues.c.session_id == claim["session_id"])).mappings().one()
            return graph.submit(conn, dialogue, event, access, args,
                stable_id(owner, claim["source_ids"][0], payload["call_id"]), claim["binding"], claim["context"])
        if operation == "tasks.list":
            scope = tasks.c.id == root_id if root_id else tasks.c.kind == "goal"
            rows = conn.execute(select(tasks).where(tasks.c.owner_id == owner, scope,
                tasks.c.conversation == access.conversation.key).order_by(tasks.c.created_at.desc()).limit(20)).mappings().all()
            visible = [row for row in rows if graph.may_read(row["id"], access)]
            summaries = []
            for snapshot in self._task_snapshots(conn, run, claim, visible, access):
                action = snapshot["progress"]["current_action"] if not snapshot.get("context_revoked") else None
                item = {"public_id": snapshot["public_id"], "title": (snapshot["goal"] or "")[:100] or None,
                        "state": snapshot["state"], "current_action": action[:100] if action else None}
                if len(json.dumps([*summaries, item]).encode()) > 12 * 1024:
                    break
                summaries.append(item)
            return summaries
        if operation == "tasks.take_over":
            if purpose not in {"finalize", "attention"} or not root_id:
                raise PermissionError("takeover requires a bound result or escalation")
            root = conn.execute(select(tasks).where(tasks.c.id == root_id)).mappings().one()
            if not graph.finalize_ready(conn, root):
                raise PermissionError("takeover requires stopped work and reconciled operations")
            claim["takeover"] = True
            conn.execute(update(runs).where(runs.c.id == run["id"]).values(request_json=json.dumps(claim)))
            conn.execute(update(tasks).where(tasks.c.id == root_id).values(state="RUNNING"))
            return {"accepted": True, "takeover": True}
        if operation in {"tasks.spawn", "tasks.wait"}:
            if purpose not in {"finalize", "attention"} or not root_id or claim.get("takeover"):
                raise PermissionError("rework requires a bound decision turn before takeover")
            root = conn.execute(select(tasks).where(tasks.c.id == root_id)).mappings().one()
            if operation == "tasks.spawn":
                return graph.spawn(conn, root, args, access)
            return graph.wait(conn, root, args, run["id"])
        ref = args.get("task_ref")
        task = conn.execute(select(tasks).where(tasks.c.owner_id == owner,
            tasks.c.conversation == access.conversation.key, (tasks.c.id == ref) | (tasks.c.public_id == ref))).mappings().first()
        if not task:
            raise KeyError("task unavailable")
        if root_id and (task["root_task_id"] or task["id"]) != root_id:
            raise PermissionError("Leader attempt is bound to another root")
        spec = TaskInputSpec.model_validate_json(json.dumps(args)) if operation == "tasks.send_input" else None
        if spec and spec.source_input_id is not None:
            if (spec.source_input_id not in claim.get("steering_seen", ())
                    or not any(item["id"] == spec.source_input_id for item in self.steering_candidates(conn, claim))):
                raise PermissionError("Task revision must reference a verified supplementary input")
            raw = conn.execute(select(dialogue_inputs.c.event_json).where(
                dialogue_inputs.c.id == spec.source_input_id)).scalar_one()
            event = InboundEvent.model_validate_json(raw)
        if purpose == "user" and operation in {"tasks.cancel", "tasks.send_input"}:
            if task["requester"] != event.principal.key or task["origin_dialogue_id"] != claim["context"]["dialogue_id"]:
                raise PermissionError("task belongs to another user or dialogue")
            references = task_references(event.text)
            if references and references != {task["public_id"]} or (
                    not references and len(claim["context"].get("active_tasks", ())) > 1):
                raise PermissionError("ask the user to identify one task before changing it")
        if operation == "tasks.status":
            self.service.permissions.require(access, "task.read", "task:" + task["id"])
            return self._task_snapshots(conn, run, claim, [task], access, include_child_sources=True)[0]
        if operation == "tasks.read_artifact":
            if purpose not in {"user", "finalize", "attention"} or task["kind"] not in {"work", "consult"}:
                raise PermissionError("artifact reads require the producing Worker task")
            self.service.permissions.require(access, "task.read", "task:" + task["id"])
            result = self._task_snapshots(conn, run, claim, [task], access)[0]["result"] or {}
            item = next((item for item in result.get("artifacts", []) if item["id"] == args.get("artifact_id")), None)
            if item is None:
                raise KeyError("artifact unavailable for this task")
            artifact = Artifact.model_validate(item)
            if not (artifact.mime_type.startswith("text/") or artifact.mime_type == "application/json"):
                raise ValueError("only UTF-8 artifacts can be read as text")
            # shortcut: cap verified model text reads at 1 MiB, add indexed paging for larger text if needed.
            if artifact.size > 1024 * 1024:
                raise ValueError("artifact exceeds the text read limit")
            offset, limit = args.get("offset", 0), args.get("limit", 8000)
            if type(offset) is not int or type(limit) is not int or offset < 0 or not 1 <= limit <= 16000:
                raise ValueError("invalid artifact page")
            with BytesIO() as content:
                copy_archived_artifact(self.service.archive_root, artifact, content)
                text = content.getvalue().decode()
            end = offset + limit
            return {"text": text[offset:end], "next_offset": end if end < len(text) else None}
        if operation == "tasks.cancel":
            if purpose not in {"user", "finalize", "attention"}:
                raise PermissionError("this Leader turn cannot cancel work")
            self.service.permissions.require(access, "task.cancel", "task:" + task["id"])
            graph.cancel_tree(conn, task)
            return {"accepted": True, "state": "CANCEL_REQUESTED"}
        if spec:
            if purpose != "user":
                raise PermissionError("this Leader turn cannot revise the goal")
            if spec.kind != "notify":
                # Revisions are linked to the actual incoming instruction, never a made-up actor.
                text = event.text
            else:
                text = spec.text
            source_id = stable_id(event.event_id, payload["call_id"])
            if spec.kind != "notify" and not conn.execute(select(inputs.c.id).where(
                    inputs.c.source_id == source_id)).first():
                source_id = stable_id(event.event_id, task["id"], spec.kind)
            return self.service.add_graph_input(conn, task, event, access, text,
                kind=spec.kind, question_id=spec.question_id, source_id=source_id)
        raise ValueError("unsupported task tool for Leader")

    def finalize_committed_mutation(self, conn, run, claim, *, stopped, recovered=False):
        event = InboundEvent.model_validate_json(claim["event_json"])
        if claim["purpose"] != "user" or not self.core.allowed(event) or self.sessions.owner(event) is None:
            raise PermissionError("memory confirmation requires its currently authorized owner")
        self.service.permissions.require(self.sessions.access_for_claim(claim), "task.execute",
            "conversation:" + event.conversation.key)
        mutation = claim["memory_mutation"]
        source = conn.execute(select(dialogue_inputs).where(dialogue_inputs.c.id == claim["source_ids"][0],
            dialogue_inputs.c.dialogue_id == claim["context"]["dialogue_id"],
            dialogue_inputs.c.event_json == claim["event_json"])).mappings().one()
        if source["handled_at"]:
            return {"state": run["state"]}
        result = TaskResult(status="completed", summary=mutation["summary"], agent_authored=False)
        origin_id = "turn:" + claim["session_id"] + ":" + source["id"]
        for previous in conn.execute(select(outbox.c.envelope_json).where(outbox.c.origin_kind == "chat",
                outbox.c.origin_id == origin_id, outbox.c.conversation_id == event.conversation.key)).scalars():
            if json.loads(previous)["text"] != result.summary:
                raise ValueError("memory confirmation conflicts with an existing delivery")
        now = self.clock.now().isoformat()
        values = {"state": "FINISHED", "slot_reserved": 0, "result_json": result.model_dump_json()}
        if recovered:
            proof = json.loads(run["receipt_json"] or "{}")
            proof.setdefault("recovered_result", run["result_json"])
            values["receipt_json"] = json.dumps(proof)
        else:
            values.update(quiescent_at=now, runner_stopped_at=now if stopped else None)
            if run["process_id"]:
                conn.execute(update(processes).where(processes.c.id == run["process_id"],
                    processes.c.generation == run["binding_generation"]).values(
                        state="STOPPED" if stopped else "IDLE", last_used=now))
        self.core.outbox.enqueue_text(conn, target=event.conversation, text=result.summary,
            origin_kind="chat", origin_id=origin_id,
            reply_context=event.reply_context, requester=event.principal, ttl_seconds=86400,
            anchor_message_id=event.platform_message_id, purpose="result", allow_proactive=True,
            defer_reply=claim.get("public_answer_events", False))
        if claim.get("public_answer_events"):
            self.core.stream_delivery.hold_result(conn, run["id"], "chat", origin_id)
        conn.execute(update(dialogue_inputs).where(dialogue_inputs.c.id == source["id"]).values(
            state="HANDLED", handled_at=now, claimed_attempt_id=run["id"]))
        conn.execute(update(runs).where(runs.c.id == run["id"]).values(**values))
        conn.execute(update(sessions).where(sessions.c.active_run_id == run["id"]).values(active_run_id=None))
        self.service.wakeup.set()
        return {"state": "FINISHED"}

    @contextmanager
    def _settlement_lock(self, run_id):
        # The reserved slot prevents a new session writer. This OS lock serializes
        # host settlement/recovery across threads and Runtime instances, including
        # after the lease expires during native I/O. Never unlink a lock file.
        root = Path(self.db.path).resolve().parent / "leader-settlement-locks"
        root.mkdir(mode=0o700, exist_ok=True)
        with (root / (stable_id(run_id) + ".lock")).open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def complete(self, run_id, fencing, worker_id, result, *, stopped, quiescent, process=None, receipt=None):
        result = TaskResult.model_validate_json(json.dumps(result))
        validate_user_facing_text(result.summary)
        with self._settlement_lock(run_id):
            discarded_entry = None
            if (stopped and not result.agent_authored
                    and "model_tool_protocol_recovery_failed" not in result.limitations):
                with self.db.transaction() as conn:
                    old = conn.execute(select(runs).where(runs.c.id == run_id,
                        runs.c.worker == worker_id, runs.c.fencing == fencing,
                        runs.c.slot_reserved == 0)).mappings().first()
                    if old:
                        run = None
                    else:
                        run, claim = self.lease(conn, run_id, fencing, worker_id, settling=True)
                        process_row = conn.execute(select(processes).where(
                            processes.c.id == run["process_id"])).mappings().first() if run["process_id"] else None
                        if process_row and (process_row["state"] != "STOPPED"
                                or process_row["epoch"] != run["process_epoch"]
                                or process_row["generation"] != run["binding_generation"]):
                            raise PermissionError("process stop is not verified")
                        if not claim.get("memory_mutation"):
                            # Revoke runner requests before releasing the writer lock.
                            # Keep its slot and source claim until native I/O commits.
                            conn.execute(update(runs).where(runs.c.id == run_id).values(state="SETTLING"))
                        else:
                            run = None
                if run is not None:
                    proof = json.loads(run["receipt_json"]) if run["receipt_json"] else receipt
                    try:
                        adapter, path, discarded_entry = self._reconcile_stopped_tools(run, claim, process_row, proof)
                        with self.db.transaction() as conn:
                            self.compaction.recover(conn, run, claim, adapter, path, verify_saved=False)
                    except (OSError, ValueError, KeyError, PermissionError):
                        with self.db.transaction() as conn:
                            current, _ = self.lease(conn, run_id, fencing, worker_id, settling=True)
                            conn.execute(update(runs).where(runs.c.id == current["id"]).values(
                                state="NEEDS_RECONCILIATION"))
                        return {"state": "NEEDS_RECONCILIATION"}
            return self._complete(run_id, fencing, worker_id, result.model_dump(mode="json"),
                stopped=stopped, quiescent=quiescent, process=process, receipt=receipt,
                discarded_entry=discarded_entry)

    def _complete(self, run_id, fencing, worker_id, result, *, stopped, quiescent, process=None, receipt=None,
                  discarded_entry=None):
        result = TaskResult.model_validate_json(json.dumps(result))
        validate_user_facing_text(result.summary)
        submitted_result = result.model_dump_json()
        now = self.clock.now().isoformat()
        with self.db.transaction() as conn:
            old = conn.execute(select(runs).where(runs.c.id == run_id, runs.c.worker == worker_id,
                runs.c.fencing == fencing, runs.c.slot_reserved == 0)).mappings().first()
            if old:
                if not json.loads(old["request_json"]).get("memory_mutation") and old["result_json"] != result.model_dump_json():
                    raise PermissionError("result conflicts with completed Leader turn")
                return {"state": old["state"]}
            run, claim = self.lease(conn, run_id, fencing, worker_id, settling=True)
            mutation = claim.get("memory_mutation")
            if mutation:
                result = TaskResult(status="completed", summary=mutation["summary"], agent_authored=False)
                submitted_result = result.model_dump_json()
            if not stopped and not quiescent:
                raise PermissionError("Leader has not stopped")
            proof = json.loads(run["receipt_json"]) if run["receipt_json"] else receipt
            if result.agent_authored:
                if not proof or proof.get("kind") not in {"turn_finished", "run_yielded"}:
                    raise PermissionError("Leader needs a durable turn boundary")
                self.receipt(conn, run, claim, proof)
            if run["process_id"]:
                identity = conn.execute(select(processes).where(processes.c.id == run["process_id"])).mappings().one()
                if stopped and (identity["state"] != "STOPPED"
                                or identity["epoch"] != run["process_epoch"]
                                or identity["generation"] != run["binding_generation"]):
                    raise PermissionError("process stop is not verified")
                if not stopped and (not process or process.get("id") != run["process_id"]
                        or process.get("generation") != run["binding_generation"]
                        or process.get("epoch") != run["process_epoch"]):
                    raise PermissionError("quiescence proof does not match process")
            self._account_turn(conn, run, claim)
            if claim["purpose"] == "compaction":
                self.compaction.settle(conn, run, claim)
                self.service.wakeup.set()
                conn.execute(update(runs).where(runs.c.id == run_id).values(state="FINISHED", slot_reserved=0,
                    quiescent_at=now, runner_stopped_at=now if stopped else None, result_json=submitted_result))
                conn.execute(update(sessions).where(sessions.c.active_run_id == run_id).values(active_run_id=None))
                if run["process_id"]:
                    conn.execute(update(processes).where(processes.c.id == run["process_id"],
                        processes.c.generation == run["binding_generation"]).values(state="STOPPED" if stopped else "IDLE", last_used=now))
                return {"state": "FINISHED"}
            if mutation:
                self.authorize(conn, run, claim, {"action": "task.execute"})
                return self.finalize_committed_mutation(conn, run, claim, stopped=stopped)
            protocol_failure = (not result.agent_authored and result.status == "failed" and run["role"] == "leader"
                and "model_tool_protocol_recovery_failed" in result.limitations)
            cleared = claim["purpose"] == "user" and conn.scalar(select(dialogue_inputs.c.state)
                .where(dialogue_inputs.c.id == claim["source_ids"][0])) == "CLEARED"
            stale_draft, covered = False, []
            deferred_inputs = claim.get("deferred_goal_inputs")
            if deferred_inputs:
                conn.execute(update(operations).where(operations.c.run_id == run_id,
                    operations.c.state == "PREPARED").values(state="FAILED",
                        result_json=json.dumps({"reason": "stopped_before_dispatch"}), updated_at=now))
                self._persist_input_dependencies(conn, claim["source_ids"][0], deferred_inputs)
            elif not cleared and result.agent_authored and claim.get("steer_until"):
                outcome = proof.get("outcome")
                summary = proof.get("summary")
                if (not isinstance(summary, str) or summary[:10000] != result.summary
                        or (outcome is not None and not isinstance(outcome, dict))):
                    raise PermissionError("Leader publication does not match its durable draft")
                covered = (outcome or {}).get("covered_input_ids", [])
                deferred = (outcome or {}).get("deferred_input_ids", [])
                seen = set(proof["input_ids"])
                available = {item["id"] for item in self.steering_candidates(conn, claim)}
                offered = set(claim["steered_inputs"])
                observed = seen & offered
                described = set()
                if all(isinstance(ids, list) and len(ids) <= 8 and all(isinstance(item, str) for item in ids)
                       for ids in (covered, deferred)):
                    covered = set(covered) - set(claim["source_ids"])
                    described = covered | set(deferred)
                if not described or not described <= observed or set(covered) & set(deferred):
                    covered, described = [], set()
                ambiguous = bool(observed - described)
                primary = conn.execute(select(dialogue_inputs).where(
                    dialogue_inputs.c.id == claim["source_ids"][0])).mappings().one()
                context = json.loads(primary["context_json"])
                turn = context["leader_turn"]
                retries = turn.get("publish_retries", 0)
                stale_draft = ambiguous or bool((seen & offered) - available) or (
                    (bool(available - seen) or (not offered and self.unrouted_input_ids(conn, claim, linked_only=True))) and retries < 1)
                if stale_draft:
                    turn["publish_retries"] = retries + 1
                    if ambiguous:
                        turn["steer_without_supplements"] = True
                    conn.execute(update(dialogue_inputs).where(dialogue_inputs.c.id == primary["id"]).values(
                        context_json=json.dumps(context, ensure_ascii=False)))
                elif covered:
                    conn.execute(update(dialogue_inputs).where(dialogue_inputs.c.id.in_(covered),
                        dialogue_inputs.c.claimed_attempt_id.is_(None), dialogue_inputs.c.handled_at.is_(None)).values(
                        claimed_attempt_id=run_id, persisted_at=now, consumed_at=now, handled_at=now, state="HANDLED"))
            if cleared or deferred_inputs or stale_draft or (not result.agent_authored and not protocol_failure):
                if discarded_entry:
                    conn.execute(update(runs).where(runs.c.id == run_id).values(
                        receipt_json=json.dumps({"discarded_submit_entry": discarded_entry})))
                table = dialogue_inputs if claim["purpose"] == "user" else leader_inbox
                source_values = {"claimed_attempt_id": None}
                conn.execute(update(table).where(table.c.id.in_(claim["source_ids"]),
                    table.c.claimed_attempt_id == run_id, or_(table.c.handled_at.is_(None), cleared)).values(**source_values))
                conn.execute(update(runs).where(runs.c.id == run_id).values(state="FINISHED", slot_reserved=0,
                    quiescent_at=now, runner_stopped_at=now if stopped else None, result_json=submitted_result))
                if run["process_id"]:
                    conn.execute(update(processes).where(processes.c.id == run["process_id"],
                        processes.c.generation == run["binding_generation"]).values(
                            state="STOPPED" if stopped else "IDLE", last_used=now))
                conn.execute(update(sessions).where(sessions.c.active_run_id == run_id).values(active_run_id=None))
                if run["role"] == "skill_review":
                    self.sessions.learning.finish_review(conn, run_id, retry=True)
                    self._review_marks.pop(run_id, None)
                self.service.wakeup.set()
                return {"state": "FINISHED"}
            pending_wait = conn.execute(select(waits).where(waits.c.run_id == run_id,
                waits.c.state.in_(["INTENT", "WAITING"]))).mappings().first()
            self.authorize(conn, run, claim, {"action": "task.execute"}, ending_wait=bool(pending_wait),
                reporting_failure=protocol_failure)
            target = notice = None
            published_chat = False
            if claim.get("target_root_task_id"):
                target = conn.execute(select(tasks).where(tasks.c.id == claim["target_root_task_id"])).mappings().one()
            newer_result = claim["purpose"] == "progress" and conn.scalar(select(leader_inbox.c.id).where(
                leader_inbox.c.target_session_id == claim["session_id"],
                leader_inbox.c.root_task_id == claim["target_root_task_id"],
                leader_inbox.c.source_revision == claim["basis_goal_revision"],
                leader_inbox.c.resume_reason.in_(["child_result", "needs_input", "needs_attention"]),
                leader_inbox.c.handled_at.is_(None), leader_inbox.c.claimed_attempt_id.is_(None)).limit(1))
            if pending_wait:
                conn.execute(update(waits).where(waits.c.id == pending_wait["id"]).values(state="WAITING"))
                conn.execute(update(tasks).where(tasks.c.id == target["id"]).values(state="WAITING_INPUT"))
                self.service._wake(conn, target["id"], pending_wait["id"])
            elif protocol_failure and target is not None:
                if target["wait_json"] and json.loads(target["wait_json"]).get("attempt_id") == run_id:
                    self.service.graph.yield_children(conn, target)
                elif claim["purpose"] == "finalize" and self.service.graph.finalize_ready(conn, target):
                    conn.execute(update(tasks).where(tasks.c.id == target["id"]).values(
                        state="BLOCKED", result_json=result.model_dump_json()))
                    self.service.graph.event(conn, target, "finished", {"state": "BLOCKED"}, attempt_id=run_id,
                        event_id="final:" + target["id"] + ":" + str(target["goal_revision"]))
                notice = self.service._notification(conn, target, run_id, result.summary, "result", "important",
                    "protocol-failure:" + run_id, defer_reply=claim.get("public_answer_events", False))
                if claim.get("public_answer_events"):
                    self.core.stream_delivery.hold_result(conn, run_id, "task", notice["notification_id"])
            elif claim["purpose"] in {"finalize", "attention"} and result.agent_authored and (
                    claim["purpose"] == "finalize"
                    or target["wait_json"] and json.loads(target["wait_json"]).get("attempt_id") == run_id
                    or self.service.graph.finalize_ready(conn, target)):
                if not target or target["fencing"] != fencing or target["goal_revision"] != claim["basis_goal_revision"]:
                    raise PermissionError("stale finalize generation or revision")
                if target["wait_json"] and json.loads(target["wait_json"]).get("attempt_id") == run_id:
                    self.service.graph.yield_children(conn, target)
                else:
                    if not self.service.graph.finalize_ready(conn, target):
                        raise PermissionError("required children, inputs or operations are not settled")
                    descendants = conn.execute(select(tasks).where(tasks.c.root_task_id == target["id"],
                        tasks.c.id != target["id"])).mappings().all()
                    if claim["context"].get("memory_replan"):
                        descendants = [child for child in descendants if
                            json.loads(child["context_json"]).get("memory_epoch") == claim["context"].get("memory_epoch")]
                    if result.observation_refs:
                        self.service.graph.verify_final_result(conn, target, result, checks=("observation",))
                    if result.effect_refs:
                        self.service.graph.verify_final_result(conn, target, result, checks=("effect",))
                    child_results = {child["id"]: json.loads(child["result_json"] or "{}") for child in descendants}
                    scheduled_success = result.status == "completed" and json.loads(target["context_json"]).get("schedule_fire_id")
                    if scheduled_success:
                        result = self._verify_scheduled_completion(conn, claim, descendants, child_results, result)
                    artifacts = {item.id: item.model_dump(mode="json") for item in result.artifacts}
                    evidence = {item.url: item.model_dump(mode="json") for item in result.evidence}
                    observed = set(result.observation_refs)
                    child_effects = set()
                    for child in descendants:
                        if scheduled_success and child["state"] != "SUCCEEDED":
                            continue
                        child_result = child_results[child["id"]]
                        for artifact in child_result.get("artifacts", []):
                            artifacts[artifact["id"]] = artifact
                        for item in child_result.get("evidence", []):
                            evidence[item["url"]] = item
                        child_effects.update(child_result.get("effect_refs", []))
                        observed.update(child_result.get("observation_refs", []))
                    effects = set(result.effect_refs)
                    if child_effects:
                        effects.update(conn.scalars(select(operations.c.id).where(
                            operations.c.task_id.in_(select(tasks.c.id).where(tasks.c.root_task_id == target["id"])),
                            operations.c.id.in_(child_effects), operations.c.state == "VERIFIED")))
                    result = result.model_copy(update={"artifacts": tuple(Artifact.model_validate(a) for a in artifacts.values()),
                        "evidence": tuple(__import__("elephant.domain.tasks", fromlist=["Evidence"]).Evidence.model_validate(e) for e in evidence.values()),
                        "effect_refs": tuple(sorted(effects)), "observation_refs": tuple(sorted(observed))})
                    self.service._validate_result(result)
                    root_goal = json.loads(target["goal_json"])
                    if result.status == "completed":
                        self.service.graph.verify_final_result(conn, target, result)
                        missing = set(root_goal.get("requested_outputs", [])) - {a.relative_path for a in result.artifacts}
                        if missing:
                            raise ValueError("requested root outputs missing")
                    state = {"completed": "SUCCEEDED", "partial": "PARTIAL", "blocked": "BLOCKED", "failed": "FAILED"}.get(result.status)
                    if not state:
                        raise ValueError("finalize must settle or persist a child wait")
                    conn.execute(update(tasks).where(tasks.c.id == target["id"]).values(state=state, result_json=result.model_dump_json()))
                    self.service.graph.event(conn, target, "finished", {"state": state}, attempt_id=run_id,
                        event_id="final:" + target["id"] + ":" + str(target["goal_revision"]))
                    notice = self.service._notification(conn, target, run_id, result.summary, "result", "important",
                        "final:" + target["id"] + ":" + str(target["goal_revision"]),
                        defer_reply=claim.get("public_answer_events", False))
                    if claim.get("public_answer_events"):
                        self.core.stream_delivery.hold_result(conn, run_id, "task", notice["notification_id"])
            elif newer_result:
                pass  # The queued Worker event wakes a new turn with the current snapshot.
            elif claim["purpose"] in {"progress", "question", "attention"} and result.agent_authored:
                purpose = "question" if claim["purpose"] == "question" else "progress"
                notice = self.service._notification(conn, target, run_id, result.summary, purpose,
                    "important" if claim["purpose"] == "attention" else "normal",
                    stable_id("leader", run_id), defer_reply=claim.get("public_answer_events", False))
                if purpose == "question":
                    notice_id = notice["notification_id"]
                    question_ids = claim.get("question_ids") or notification_question_ids(conn, notice_id)
                    conn.execute(update(waits).where(waits.c.id.in_(question_ids)).values(notification_id=notice_id))
                if claim.get("public_answer_events"):
                    self.core.stream_delivery.hold_result(conn, run_id, "task", notice["notification_id"])
            elif run["role"] != "skill_review" and not (target and self.service._silent_schedule(target)):
                event = InboundEvent.model_validate_json(claim["event_json"])
                published_chat = True
                self.core.outbox.enqueue_text(conn, target=event.conversation, text=result.summary,
                    origin_kind="chat", origin_id="turn:" + claim["session_id"] + ":" + claim["source_ids"][0],
                    reply_context=event.reply_context, requester=event.principal, ttl_seconds=86400,
                    anchor_message_id=event.platform_message_id, purpose="result", allow_proactive=True,
                    defer_reply=claim.get("public_answer_events", False), fixed_publication=bool(claim.get("steer_until")))
                if claim.get("public_answer_events"):
                    self.core.stream_delivery.hold_result(conn, run_id, "chat",
                        "turn:" + claim["session_id"] + ":" + claim["source_ids"][0])
            table = dialogue_inputs if claim["purpose"] == "user" else leader_inbox
            handled = {"handled_at": now, **({"state": "HANDLED"} if table is dialogue_inputs else {"output_ref": run_id})}
            if newer_result and not pending_wait:
                handled = {"claimed_attempt_id": None}
            conn.execute(update(table).where(table.c.id.in_(claim["source_ids"]),
                table.c.claimed_attempt_id == run_id).values(**handled))
            if published_chat or notice is not None:
                conn.execute(update(leader_events).where(leader_events.c.source_kind == (
                    "user" if table is dialogue_inputs else "worker"),
                    leader_events.c.source_id.in_([*claim["source_ids"], *covered])).values(published_run_id=run_id))
            if run["role"] == "skill_review":
                self.sessions.learning.finish_review(conn, run_id)
                self._review_marks.pop(run_id, None)
            conn.execute(update(runs).where(runs.c.id == run_id).values(state="FINISHED", slot_reserved=0,
                quiescent_at=now, runner_stopped_at=now if stopped else None, result_json=submitted_result))
            if run["process_id"]:
                conn.execute(update(processes).where(processes.c.id == run["process_id"],
                    processes.c.generation == run["binding_generation"]).values(state="STOPPED" if stopped else "IDLE", last_used=now))
            conn.execute(update(sessions).where(sessions.c.active_run_id == run_id).values(active_run_id=None))
        self.service.wakeup.set()
        return {"state": "FINISHED"}

    def _verify_scheduled_completion(self, conn, claim, descendants, child_results, result):
        work = {child["id"]: child for child in descendants if child["kind"] == "work"}
        reworked = set()
        for child in work.values():
            if child["state"] == "SUCCEEDED":
                previous = child["rework_of_task_id"]
                while previous in work and previous not in reworked:
                    reworked.add(previous)
                    previous = work[previous]["rework_of_task_id"]
        incomplete = [child for task_id, child in work.items()
                      if task_id not in reworked and child["state"] != "SUCCEEDED"]
        unsuccessful = [child_results[child["id"]] for child in descendants
                        if child["state"] != "SUCCEEDED"]
        incomplete_artifacts = {item["id"] for child in unsuccessful for item in child.get("artifacts", [])}
        incomplete_observations = {ref for child in unsuccessful for ref in child.get("observation_refs", [])}
        incomplete_effects = {ref for child in unsuccessful for ref in child.get("effect_refs", [])}
        result = result.model_copy(update={
            "artifacts": tuple(item for item in result.artifacts if item.id not in incomplete_artifacts),
            "observation_refs": tuple(ref for ref in result.observation_refs if ref not in incomplete_observations),
            "effect_refs": tuple(ref for ref in result.effect_refs if ref not in incomplete_effects),
        })
        if not incomplete:
            return result
        if claim.get("takeover"):
            root_id, run_id = claim["target_root_task_id"], claim["run_id"]
            child_artifacts = {item["id"] for child in child_results.values() for item in child.get("artifacts", [])}
            if any(item.id not in child_artifacts for item in result.artifacts) or conn.scalar(
                    select(observations.c.id).where(observations.c.task_id == root_id, observations.c.run_id == run_id,
                        observations.c.id.in_(result.observation_refs)).limit(1)) or conn.scalar(
                    select(operations.c.id).where(operations.c.task_id == root_id, operations.c.run_id == run_id,
                        operations.c.state == "VERIFIED", operations.c.id.in_(result.effect_refs)).limit(1)):
                return result
        return result.model_copy(update={
            "status": "blocked",
            "summary": "任务未能确认完成。仍有子任务未完成，且缺少可核验的接管或返工结果。",
        })

    def _reconcile_stopped_tools(self, run, claim, process, proof):
        """Reconcile native calls only after the caller has verified exclusive stop."""
        from elephant.contracts.harness import resolve_harness
        from elephant.domain.agents import GoalSpec

        mutation = claim.get("memory_mutation")
        run_id = run["id"]
        discarded_entry = None
        adapter = resolve_harness(self.core, claim.get("binding", {}))
        path = adapter.session_path(Path(self.db.path).resolve().parent, run, process)
        discarded = set()
        with self.db.engine.connect() as conn:
            for raw in conn.execute(select(runs.c.receipt_json).where(runs.c.session_id == run["session_id"],
                    runs.c.role == "leader", runs.c.state.in_(["RECOVERED", "FINISHED"]), runs.c.receipt_json.is_not(None))).scalars():
                receipt = json.loads(raw)
                # Only the host writes this receipt shape, not the runner's normal boundary receipts.
                if isinstance(receipt, dict) and set(receipt) == {"discarded_submit_entry"}:
                    discarded.add(receipt["discarded_submit_entry"])
        pending = () if mutation else adapter.unfinished_tools(path, discarded=discarded)
        submitted = [call for call in pending if call.get("name") == "tasks_submit"]
        if claim.get("target_root_task_id") and claim["purpose"] in {"attention", "finalize"}:
            if pending:
                adapter.restore_tool_results(path, [{"call": call, "is_error": True, "result": {
                    "state": "INTERRUPTED", "stopped": True,
                    "instruction": "上次调用随进程中断，结果尚未确认。先读取当前任务和操作记录，核对 UNKNOWN 操作，不重复外部写入。",
                }} for call in pending])
        elif pending:
            if run["role"] != "leader" or claim["purpose"] != "user":
                raise ValueError("native dispatch recovery requires an isolated owner submission")
            receipts, discard = [], None
            with self.db.engine.connect() as conn:
                source = conn.execute(select(dialogue_inputs).where(
                    dialogue_inputs.c.id == claim["source_ids"][0],
                    dialogue_inputs.c.dialogue_id == claim["context"]["dialogue_id"])).mappings().first()
                if not source:
                    raise ValueError("native dispatch source no longer matches its original Dialogue")
                owner = claim["context"]["owner_id"]
                for call in submitted:
                    task, committed_intent = self.service.graph.dispatch_receipt(conn, owner,
                        stable_id(owner, source["id"], call["id"]))
                    if (not task and not proof and len(pending) == 1 and len(claim["source_ids"]) == 1
                            and not claim.get("target_root_task_id") and source["owner_id"] == owner
                            and source["claimed_attempt_id"] == run_id
                            and (source["handled_at"] is None or source["state"] == "CLEARED")):
                        discard = adapter.aborted_submit(path, call, run_id=run_id, input_ids=claim["source_ids"])
                        if discard:
                            break
                    intent = GoalSpec.model_validate_json(json.dumps(call["arguments"])).model_dump(mode="json")
                    if (not task or task["origin_dialogue_id"] != source["dialogue_id"]
                            or task["coordinator_session_id"] != claim["session_id"]
                            or GoalSpec.model_validate(committed_intent).model_dump(mode="json") != intent):
                        raise ValueError("native dispatch has no matching committed host intent")
                    receipts.append({"call": call, "result": self.service.graph.status(conn, task)})
            if discard:
                adapter.discard_aborted_submit(path, discard)
                discarded_entry = discard["entry"]["id"]
            else:
                receipts.extend({"call": call, "is_error": True, "result": {
                    "state": "INTERRUPTED", "stopped": True,
                    "instruction": "上次调用随进程中断。先核对操作记录中的 UNKNOWN 结果，不重复执行。",
                }} for call in pending if call.get("name") != "tasks_submit")
                adapter.restore_tool_results(path, receipts)
        return adapter, path, discarded_entry

    def recover(self):
        """Reconcile expired Leader attempts after the launcher proves their stop."""
        with self.db.engine.connect() as conn:
            expired = conn.execute(select(runs.c.id).where(runs.c.role.in_(["leader", "skill_review"]),
                runs.c.slot_reserved == 1, runs.c.lease <= self.clock.now().isoformat())).scalars().all()
        return [self.recover_attempt(run_id) for run_id in expired]

    def recover_attempt(self, run_id):
        with self._settlement_lock(run_id):
            return self._recover_attempt(run_id)

    def _recover_attempt(self, run_id):
        with self.db.engine.connect() as conn:
            run = conn.execute(select(runs).where(runs.c.id == run_id,
                runs.c.role.in_(["leader", "skill_review"]))).mappings().one()
            if not run["slot_reserved"]:
                return {"run_id": run_id, "state": run["state"]}
            if run["lease"] > self.clock.now().isoformat():
                raise PermissionError("Leader lease is still active")
            claim = json.loads(run["request_json"])
            process = conn.execute(select(processes).where(processes.c.id == run["process_id"])).mappings().first()
        identity = json.loads(process["identity_json"]) if process else {}
        proof_task = identity.get("launch_task_id", claim["task_id"])
        if not self.service.stop_verifier(proof_task, run["process_id"] or run_id):
            with self.db.transaction() as conn:
                conn.execute(update(runs).where(runs.c.id == run_id, runs.c.slot_reserved == 1)
                    .values(state="NEEDS_RECONCILIATION"))
            return {"run_id": run_id, "state": "NEEDS_RECONCILIATION"}
        now = self.clock.now().isoformat()
        with self.db.transaction() as conn:
            conn.execute(update(operations).where(operations.c.run_id == run_id,
                operations.c.state == "DISPATCHED").values(state="UNKNOWN", updated_at=now))
            conn.execute(update(operations).where(operations.c.run_id == run_id,
                operations.c.state == "PREPARED").values(state="FAILED",
                    result_json=json.dumps({"reason": "stopped_before_dispatch"}), updated_at=now))
        proof = json.loads(run["receipt_json"]) if run["receipt_json"] else None
        mutation = claim.get("memory_mutation")
        try:
            adapter, path, discarded_entry = self._reconcile_stopped_tools(run, claim, process, proof)
            with self.db.transaction() as conn:
                self.compaction.recover(conn, run, claim, adapter, path)
        except (OSError, ValueError, KeyError, PermissionError):
            with self.db.transaction() as conn:
                conn.execute(update(runs).where(runs.c.id == run_id, runs.c.slot_reserved == 1)
                    .values(state="NEEDS_RECONCILIATION"))
            return {"run_id": run_id, "state": "NEEDS_RECONCILIATION"}
        try:
            native = [item for item in adapter.boundary_receipts(path) if item.get("run_id") == run_id]
            if native:
                proof = {**native[-1], **(proof or {})}
        except (OSError, ValueError, KeyError):
            # The accepted input remains queued until its native log can be resumed.
            proof = None
        reopen = bool(mutation or (proof and proof.get("summary")))
        now = self.clock.now().isoformat()
        with self.db.transaction() as conn:
            current = conn.execute(select(runs).where(runs.c.id == run_id)).mappings().one()
            if not current["slot_reserved"]:
                return {"run_id": run_id, "state": current["state"]}
            if current["lease"] > now:
                raise PermissionError("Leader lease was renewed during recovery")
            if run["process_id"]:
                conn.execute(update(processes).where(processes.c.id == run["process_id"],
                    processes.c.generation == run["binding_generation"]).values(state="STOPPED", stopped_at=now))
            if claim.get("target_root_task_id"):
                root = conn.execute(select(tasks).where(tasks.c.id == claim["target_root_task_id"])).mappings().one()
                waiting = conn.execute(select(waits).where(waits.c.run_id == run_id,
                    waits.c.state.in_(["INTENT", "WAITING"]))).mappings().first()
                child_wait = root["wait_json"] and json.loads(root["wait_json"]).get("attempt_id") == run_id
                if waiting or child_wait:
                    if root["state"] in TERMINAL | {"CANCEL_REQUESTED"}:
                        if waiting:
                            conn.execute(update(waits).where(waits.c.id == waiting["id"]).values(state="CANCELLED"))
                    elif waiting:
                        conn.execute(update(waits).where(waits.c.id == waiting["id"]).values(state="WAITING"))
                        conn.execute(update(tasks).where(tasks.c.id == root["id"]).values(state="WAITING_INPUT"))
                        self.service._wake(conn, root["id"], waiting["id"])
                    else:
                        self.service.graph.yield_children(conn, root)
                    conn.execute(update(leader_inbox).where(leader_inbox.c.id.in_(claim["source_ids"]),
                        leader_inbox.c.claimed_attempt_id == run_id).values(handled_at=now, output_ref="WAIT_RECOVERED"))
                    self._account_turn(conn, current, claim)
                    conn.execute(update(runs).where(runs.c.id == run_id).values(state="RECOVERED", slot_reserved=0,
                        runner_stopped_at=now, quiescent_at=now))
                    conn.execute(update(sessions).where(sessions.c.active_run_id == run_id).values(active_run_id=None))
                    self.service.wakeup.set()
                    return {"run_id": run_id, "state": "RECOVERED"}
            if reopen:
                conn.execute(update(runs).where(runs.c.id == run_id).values(state="RUNNING",
                    lease=(self.clock.now() + timedelta(seconds=30)).isoformat()))
        if mutation:
            outcome = self._complete(run_id, run["fencing"], run["worker"],
                {"status": "completed", "summary": mutation["summary"], "agent_authored": False},
                stopped=True, quiescent=False)
            return {"run_id": run_id, **outcome}
        if proof and proof.get("summary"):
            try:
                outcome = self._complete(run_id, run["fencing"], run["worker"],
                    proof.get("result") or {"status": (proof.get("outcome") or {}).get("status", "completed"),
                     "summary": proof["summary"]}, stopped=True, quiescent=False,
                    receipt=proof)
                return {"run_id": run_id, **outcome}
            except (PermissionError, ValueError):
                pass
        with self.db.transaction() as conn:
            current = conn.execute(select(runs).where(runs.c.id == run_id)).mappings().one()
            if not current["slot_reserved"]:
                return {"run_id": run_id, "state": current["state"]}
            if not reopen and current["lease"] > self.clock.now().isoformat():
                raise PermissionError("Leader lease was renewed during recovery")
            self._account_turn(conn, current, claim, through=datetime.fromisoformat(run["lease"]))
            if pending := claim.get("deferred_goal_inputs"):
                self._persist_input_dependencies(conn, claim["source_ids"][0], pending)
            table = dialogue_inputs if claim["purpose"] == "user" else leader_inbox
            conn.execute(update(table).where(table.c.id.in_(claim["source_ids"]),
                table.c.claimed_attempt_id == run_id, table.c.handled_at.is_(None)).values(claimed_attempt_id=None))
            if discarded_entry:
                conn.execute(update(runs).where(runs.c.id == run_id).values(
                    receipt_json=json.dumps({"discarded_submit_entry": discarded_entry})))
            if run["role"] == "skill_review":
                self.sessions.learning.finish_review(conn, run_id, retry=True)
                self._review_marks.pop(run_id, None)
            conn.execute(update(runs).where(runs.c.id == run_id).values(state="RECOVERED", slot_reserved=0,
                runner_stopped_at=now, quiescent_at=now))
            conn.execute(update(sessions).where(sessions.c.active_run_id == run_id).values(active_run_id=None))
            if claim.get("takeover"):
                root = conn.execute(select(tasks).where(tasks.c.id == claim["target_root_task_id"])).mappings().one()
                self.service.graph.event(conn, root, "needs_attention", {"reason": "leader_interrupted"},
                    attempt_id=run_id, event_id="leader-interrupted:" + run_id, wake=True)
        self.service.wakeup.set()
        return {"run_id": run_id, "state": "RECOVERED"}

    async def _reconcile_direct_operation(self, identity, claim, payload):
        from elephant.capabilities.types import Action, ToolContext

        operation_id = payload["operation_id"]
        with self.db.engine.connect() as conn:
            record = conn.execute(select(operations).where(operations.c.id == operation_id,
                operations.c.task_id == claim["task_id"])).mappings().first()
            if not record:
                raise PermissionError("operation belongs to another Leader turn")
            original = conn.execute(select(runs).where(runs.c.id == record["run_id"])).mappings().one()
            original_claim = json.loads(original["request_json"])
            if original_claim["task_id"] != claim["task_id"] or original["role"] != "leader":
                raise PermissionError("operation execution does not match its Leader scope")
        if record["state"] not in {"UNKNOWN", "PARTIAL"}:
            return {"operation_id": operation_id, "state": record["state"],
                "result": json.loads(record["result_json"]), "changed": False}
        action_data = json.loads(record["action_json"])
        action = Action(**{**action_data, "resources": tuple(action_data["resources"])})
        recovery = self.core.recovery
        read_action = {"action": recovery._read_action(action), "resources": action.resources}
        def authorize():
            with self.db.engine.connect() as conn:
                current, current_claim = self.lease(conn, *identity)
                self.authorize(conn, current, current_claim, read_action)
        authorize()
        plan, binding = original_claim["plan"], original_claim.get("binding", {})
        workspace = recovery.execution.workspace(Path(self.db.path).resolve().parent,
            original_claim["task_id"], plan.get("workspace_ref"), binding.get("workspace_bindings", {}), strict=False)
        context = ToolContext(task_id=claim["task_id"], run_id=record["run_id"], fence=original["fencing"],
            principal=self.sessions.access_for_claim(claim).principal.model_dump(mode="json"),
            workspace=str(workspace), repository=original_claim.get("repository"),
            host=original_claim.get("host") or self.sessions.settings.orchestration.github_host,
            environment=plan.get("environment", "host"))
        if context.host != self.sessions.settings.orchestration.github_host:
            raise PermissionError("readback credential host differs from the Leader binding")
        verification = await recovery._verify(context, action, json.loads(record["arguments_json"]),
            json.loads(record["result_json"]))
        state, result = verification or ("UNKNOWN", json.loads(record["result_json"]))
        if state not in {"VERIFIED", "FAILED", "PARTIAL", "UNKNOWN"}:
            raise ValueError("readback returned an invalid operation state")
        if len(json.dumps(result).encode()) > 1048576:
            raise ValueError("readback result exceeds the operation evidence limit")
        authorize()
        with self.db.transaction() as conn:
            changed = conn.execute(update(operations).where(operations.c.id == operation_id,
                operations.c.state == record["state"]).values(state=state,
                    result_json=json.dumps(result, ensure_ascii=False), updated_at=self.clock.now().isoformat())).rowcount
            current = conn.execute(select(operations).where(operations.c.id == operation_id)).mappings().one()
        return {"operation_id": operation_id, "state": current["state"],
            "result": json.loads(current["result_json"]), "changed": bool(changed)}

    def _control_state(self, conn, run, claim):
        try:
            self.authorize(conn, run, claim, {"action": "task.execute"})
        except PermissionError:
            return "CANCEL_REQUESTED"
        return run["state"]

    def renew(self, run_id, fencing, worker_id):
        with self.db.transaction() as conn:
            run, claim = self.lease(conn, run_id, fencing, worker_id)
            state = self._control_state(conn, run, claim)
            conn.execute(update(runs).where(runs.c.id == run_id).values(
                lease=(self.clock.now() + timedelta(seconds=30)).isoformat()))
            return {"state": state, "delivery": "ATTACHED"}

    async def invoke(self, operation, identity, payload=None, **values):
        run_id, fencing, worker_id = identity
        payload = payload or {}
        if operation == "renew":
            return self.renew(*identity)
        if operation == "recovery-status":
            # Replay needs to inspect closed attempts, whose execution lease is no longer valid.
            with self.db.engine.connect() as conn:
                run = conn.execute(select(runs.c.state, runs.c.slot_reserved, runs.c.quiescent_at).where(
                    runs.c.id == run_id, runs.c.worker == worker_id, runs.c.fencing == fencing,
                    runs.c.role.in_(["leader", "skill_review"]))).one_or_none()
            if run is None:
                raise PermissionError("unknown recovery source")
            closed = run.state in {"FINISHED", "RECOVERED"} and not run.slot_reserved and run.quiescent_at is not None
            return {"state": run.state, "discard_spool": False, "archive_spool": closed}
        if operation in {"inputs", "input-ack"}:
            with self.db.engine.connect() as conn:
                run, claim = self.lease(conn, *identity)
                seen = claim.get("steering_seen", ())
                if operation == "input-ack":
                    return {"accepted": payload.get("input_id") in seen}
                if run["receipt_json"]:
                    return []
                candidates = [item for item in self.steering_candidates(conn, claim) if item["id"] not in seen]
                if not candidates:
                    return []
                if claim["steered_inputs"]:
                    self.authorize(conn, run, claim, {"action": "task.execute"})
                    return candidates
            with self.db.transaction() as conn:
                run, claim = self.lease(conn, *identity)
                if run["receipt_json"] or claim.get("steered_inputs"):
                    return []
                candidates = self.steering_candidates(conn, claim)
                if candidates:
                    self.authorize(conn, run, claim, {"action": "task.execute"})
                    claim["steered_inputs"] = [item["id"] for item in candidates]
                    conn.execute(update(runs).where(runs.c.id == run_id).values(
                        request_json=json.dumps(claim, ensure_ascii=False)))
                return candidates
        if operation == "authorize":
            return self.authorize_call(identity, payload)
        if operation == "control":
            with self.db.engine.connect() as conn:
                conn.exec_driver_sql("BEGIN")
                run, claim = self.lease(conn, *identity)
                return {"state": self._control_state(conn, run, claim), "delivery": "ATTACHED"}
        if operation == "result":
            return self.complete(*identity, values["result"], stopped=values.get("stopped", False),
                quiescent=values.get("quiescent", False), process=values.get("process"), receipt=values.get("receipt"))
        if operation == "schedules":
            with self.db.engine.connect() as conn:
                run, claim = self.lease(conn, *identity)
                if run["role"] != "leader" or claim["purpose"] != "user":
                    raise PermissionError("schedule controls require a real Leader user turn")
                operation_name = payload.get("operation")
                action = ("task.read" if operation_name in {"get", "list"}
                          else "task.cancel" if operation_name in {"cancel", "delete"} else "task.create")
                self.authorize(conn, run, claim, {"action": action})
                event = InboundEvent.model_validate_json(claim["event_json"])
                workspace_id = claim["context"].get("workspace_id", "personal")
            arguments = dict(payload.get("arguments", {}))
            if operation_name == "create":
                if arguments.get("workspace_id") not in {None, workspace_id}:
                    raise PermissionError("schedule project is bound to this Leader input")
                arguments["workspace_id"] = workspace_id
            return self.core.schedules.invoke(event, operation_name, arguments,
                idempotency_key=stable_id("schedule", claim["source_ids"][0], payload["call_id"]))
        if operation in {"ledger", "ask"}:
            readback_target = None
            direct = False
            with self.db.transaction() as conn:
                run, claim = self.lease(conn, *identity)
                self.authorize(conn, run, claim, {"action": "task.execute"})
                if operation == "ledger":
                    readback = payload.get("operation") in {"read", "list", "reconcile"} or (
                        payload.get("operation") == "transition" and payload.get("expected") == "UNKNOWN")
                    direct = run["role"] == "leader" and claim["purpose"] == "user" and not claim.get("target_root_task_id")
                    if direct and self.sessions.owner(InboundEvent.model_validate_json(claim["event_json"])) is None:
                        raise PermissionError("Leader operations require a verified owner binding")
                    if not direct and not claim.get("takeover") and not (
                            readback and claim["purpose"] in {"attention", "finalize"} and claim.get("target_root_task_id")):
                        raise PermissionError("Leader ledger requires a stopped task takeover")
                    if payload.get("operation") == "reconcile" and not direct:
                        readback_target = conn.execute(select(tasks.c.public_id).join(
                            operations, operations.c.task_id == tasks.c.id).where(
                                operations.c.id == payload["operation_id"],
                                tasks.c.root_task_id == claim["target_root_task_id"])).scalar_one_or_none()
                        if not readback_target:
                            raise PermissionError("operation belongs to another task tree")
                else:
                    if (claim["purpose"] not in {"finalize", "attention"}
                            or not claim.get("target_root_task_id") or payload.get("kind", "input") != "input"):
                        raise PermissionError("Leader questions require a bound decision and user input")
                    conn.execute(update(tasks).where(tasks.c.id == claim["target_root_task_id"]).values(state="RUNNING"))
            if readback_target:
                result = await self.core.recovery.readback_operation(
                    readback_target, self.sessions.access_for_claim(claim), payload["operation_id"])
                with self.db.engine.connect() as conn:
                    run, claim = self.lease(conn, *identity)
                    self.authorize(conn, run, claim, {"action": "task.execute"})
                return result
            if operation == "ledger":
                if direct:
                    if payload.get("operation") == "reconcile":
                        return await self._reconcile_direct_operation(identity, claim, payload)
                    from elephant.capabilities.types import ToolContext

                    context = ToolContext(task_id=claim["task_id"], run_id=run_id, fence=fencing,
                        principal=self.sessions.access_for_claim(claim), workspace="")
                    def authorize(action):
                        with self.db.engine.connect() as conn:
                            current, current_claim = self.lease(conn, *identity)
                            return self.authorize(conn, current, current_claim, action)
                    def authorize_dispatch(conn, action):
                        current, current_claim = self.lease(conn, *identity)
                        return self.authorize(conn, current, current_claim, {
                            "action": action["name"], "resources": action["resources"], "effect": action["effect"]})
                    with self.defer_blocked_operation(identity):
                        return await self.service.ledger_operation(context, payload, [claim["task_id"]],
                            authorize, deduplicate=True, authorize_dispatch=authorize_dispatch)
                return await self.service.ledger(*identity, payload)
            return self.service.ask(*identity, payload)
        if operation == "skills":
            with self.db.engine.connect() as conn:
                run, claim = self.lease(conn, *identity)
                name = payload.get("operation")
                self.authorize(conn, run, claim, {"action": "skills.write" if name == "skill_manage" else "skills.read"})
            adapter = self.service.skills_provider(self.sessions.access_for_claim(claim),
                workspace_id=claim["context"].get("workspace_id", "personal"))
            if adapter is None:
                raise PermissionError("owner skill scope unavailable")
            if run["role"] == "skill_review":
                from elephant.adapters.hermes_skills._vendor.skill_manager_guards import (
                    _BackgroundReviewReadMarks,
                )
                marks = self._review_marks.setdefault(run_id, _BackgroundReviewReadMarks())
                def call_review_tool():
                    with adapter.review_context(marks):
                        return adapter.call(name, payload.get("arguments", {}),
                            task_id=claim["task_id"], session_id=claim["session_id"])
                result = await asyncio.to_thread(call_review_tool)
            else:
                result = await asyncio.to_thread(adapter.call, name, payload.get("arguments", {}),
                    task_id=claim["task_id"], session_id=claim["session_id"])
            if name == "skill_manage":
                from elephant.skills import record_skill_result
                record_skill_result(self.service, run_id, payload.get("call_id", run_id), result)
            return result
        with self.db.transaction() as conn:
            run, claim = self.lease(conn, *identity)
            if operation == "context-compaction":
                return self.compaction.operation(conn, run, claim, payload)
            if operation == "validate-result-refs":
                self.authorize(conn, run, claim, {"action": "task.execute"})
                result = TaskResult.model_validate({"summary": "Outcome reference validation", **payload})
                if claim.get("target_root_task_id"):
                    root = conn.execute(select(tasks).where(tasks.c.id == claim["target_root_task_id"])).mappings().one()
                    checks = tuple(name for name in ("observation", "effect") if getattr(result, name + "_refs"))
                    self.service.graph.verify_final_result(conn, root, result, checks=checks)
                else:
                    self.service._verify_result_refs(conn, result, {"id": claim["task_id"],
                        "retry_of_task_id": None, "rework_of_task_id": None})
                return {"accepted": True}
            if operation == "tasks":
                return self.task_tool(conn, run, claim, payload)
            if operation == "session-receipt":
                return self.receipt(conn, run, claim, payload)
            if operation == "reserve-budget":
                return self.reserve(conn, run, claim, payload)
            if operation == "usage":
                changed = conn.execute(update(model_requests).where(model_requests.c.id == payload["request_id"],
                    model_requests.c.run_id == run_id).values(state="COMPLETED", usage_json=json.dumps(payload.get("usage"))))
                if not changed.rowcount:
                    raise ValueError("usage request was not reserved")
                return {"recorded": True}
            if operation == "notify":
                if run["role"] != "leader":
                    raise PermissionError("only the Leader can send user-facing progress")
                self.authorize(conn, run, claim, {"action": "notification.send"})
                if claim.get("commit_publication"):
                    return {"accepted": False, "reason": "leader_draft_unpublished"}
                text = TaskResult(status="completed", summary=payload["text"]).summary
                validate_user_facing_text(text)
                notice_id = stable_id("leader-progress", claim["session_id"], claim["source_ids"][0], text)
                event = InboundEvent.model_validate_json(claim["event_json"])
                if claim.get("target_root_task_id"):
                    target = conn.execute(select(tasks).where(tasks.c.id == claim["target_root_task_id"])).mappings().one()
                    result = self.service._notification(conn, target, run_id, text, "progress",
                        payload.get("importance", "normal"), notice_id)
                else:
                    deliveries = self.core.outbox.enqueue_text(conn, target=event.conversation, text=text,
                        origin_kind="chat", origin_id="progress:" + notice_id + ":turn:" + claim["session_id"] + ":" + claim["source_ids"][0],
                        reply_context=event.reply_context, requester=event.principal, ttl_seconds=300,
                        anchor_message_id=event.platform_message_id, purpose="progress")
                    result = {"accepted": bool(deliveries), "notification_id": notice_id}
                remaining = None
                if event.reply_context:
                    counter = stable_id(event.conversation.key, event.reply_context.platform_message_id)
                    used = conn.execute(select(reply_counters.c.seq).where(reply_counters.c.key == counter)).scalar_one_or_none() or 0
                    remaining = max(0, event.reply_context.max_replies - used)
                return {**result, "reply_messages_remaining": remaining}
            if operation == "bind-process":
                self.authorize(conn, run, claim, {"action": "task.execute"})
                if payload["session_id"] != claim["session_id"]:
                    raise PermissionError("process bound to a different session")
                old = conn.execute(select(processes).where(processes.c.id == payload["process_id"])).mappings().first()
                if old:
                    if (old["state"] != "IDLE" or old["worker"] != worker_id
                            or old["session_id"] != claim["session_id"] or old["epoch"] != payload["epoch"]
                            or old["generation"] + 1 != payload["generation"]):
                        raise PermissionError("process is not exclusively idle")
                    conn.execute(update(processes).where(processes.c.id == old["id"]).values(state="BUSY", generation=payload["generation"]))
                else:
                    conn.execute(insert(processes).values(id=payload["process_id"], session_id=claim["session_id"], worker=worker_id,
                        epoch=payload["epoch"], generation=payload["generation"], state="BUSY",
                        identity_json=json.dumps(payload.get("identity", {})), last_used=self.clock.now().isoformat()))
                known = conn.execute(select(sessions).where(sessions.c.id == claim["session_id"])).mappings().first()
                if known and known["active_run_id"] not in {None, run_id}:
                    raise PermissionError("session already has a writer")
                history = json.loads(known["manifest_json"]) if known else dict(claim["binding"])
                boundary = claim["context"].get("context_boundary", 0)
                if boundary and boundary != history.get("context_boundary"):
                    history = {**claim["binding"], "context_boundary": boundary, "context_sources_version": 1}
                if known and not history.get("context_sources_version"):
                    earlier = conn.execute(select(
                        func.json_extract(runs.c.request_json, "$.context.task_sources"),
                        func.json_extract(runs.c.request_json, "$.context.source_manifest"),
                        func.json_extract(runs.c.request_json, "$.context.memory_scope"),
                        func.json_extract(runs.c.request_json, "$.context.memory_epoch"),
                    ).where(runs.c.session_id == known["id"], runs.c.id != run_id,
                            runs.c.purpose != "compaction").order_by(runs.c.session_generation)).all()
                    for task_sources, sources, memory_scope, memory_epoch in earlier:
                        if task_sources or sources:
                            self.service.graph.merge_source_dependencies(
                                history, json.loads(task_sources or "[]"), json.loads(sources or "[]"))
                        if memory_scope and type(memory_epoch) is int and not history.get("memory_history"):
                            history.update(memory_history=True, memory_scope=json.loads(memory_scope), memory_epoch=memory_epoch)
                    if history.get("source_manifest"):
                        history["steering_sources"] = True
                history["context_sources_version"] = 1
                if claim["context"].get("retrieved_data") and claim["context"].get("memory_scope") and not history.get("memory_history"):
                    history.update(memory_history=True, memory_scope=claim["context"]["memory_scope"],
                                   memory_epoch=claim["context"]["memory_epoch"])
                if claim["context"].get("published_task_sources"):
                    history["published_task_sources"] = sorted(
                        set(history.get("published_task_sources", ())) | set(claim["context"]["published_task_sources"]))
                if claim["context"].get("task_sources") or claim["context"].get("source_manifest"):
                    history["steering_sources"] = True
                    history["workspace_id"] = claim["context"].get("workspace_id", "personal")
                    self.service.graph.merge_source_dependencies(history, claim["context"].get("task_sources", ()),
                        claim["context"].get("source_manifest", ()))
                if known:
                    conn.execute(update(sessions).where(sessions.c.id == known["id"]).values(active_run_id=run_id,
                        manifest_json=json.dumps(history, ensure_ascii=False)))
                else:
                    conn.execute(insert(sessions).values(id=claim["session_id"], scope_key=claim["session_id"],
                        session_file=payload["session_file"], manifest_json=json.dumps(history, ensure_ascii=False), active_run_id=run_id, epoch=1))
                if run["process_id"] and run["process_id"] != payload["process_id"]:
                    raise PermissionError("attempt already bound to another process")
                conn.execute(update(runs).where(runs.c.id == run_id).values(process_id=payload["process_id"],
                    process_epoch=payload["epoch"], binding_generation=payload["generation"], session=payload["session_file"]))
                return {"bound": True}
            if operation == "observation":
                conn.execute(insert(observations).values(id=payload["id"], task_id=claim["task_id"], run_id=run_id,
                    retrieved_at=payload["retrieved_at"], sha256=payload["sha256"], data_json=json.dumps(payload["data"])))
                return {"accepted": True}
            if operation == "event":
                validate_public_event(payload)
                encoded = json.dumps(payload, ensure_ascii=False)
                seq = values["seq"]
                if not isinstance(seq, int) or seq < 1 or len(encoded.encode()) > 2 * 1024 * 1024:
                    raise ValueError("invalid event sequence or payload")
                if payload.get("kind") in PUBLIC_KINDS and (not claim.get("public_answer_events")
                        or not conn.execute(select(stream_runs.c.run_id).where(stream_runs.c.run_id == run_id)).first()):
                    raise PermissionError("public answer stream was not enabled for this run")
                prior = conn.execute(select(events).where(events.c.event_id == values["event_id"])).mappings().first()
                if prior:
                    if prior["run_id"] != run_id or prior["seq"] != seq or prior["payload"] not in {encoded, json.dumps(payload)}:
                        raise ValueError("conflicting event replay")
                    return {"accepted": True}
                if seq != run["last_seq"] + 1:
                    raise ValueError("event sequence gap")
                reserve_stream_order(conn, run_id, payload)
                conn.execute(insert(events).values(event_id=values["event_id"], run_id=run_id,
                    seq=seq, payload=encoded))
                conn.execute(update(runs).where(runs.c.id == run_id).values(last_seq=seq))
                return {"accepted": True}
            if operation == "uncertain":
                conn.execute(update(runs).where(runs.c.id == run_id).values(state="NEEDS_RECONCILIATION"))
                return {"recorded": True}
        raise ValueError("unsupported Leader operation: " + operation)
