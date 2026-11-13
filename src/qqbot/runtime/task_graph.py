"""Task tree rules on the existing task/attempt/operation repositories."""
import json
from datetime import datetime
from uuid import uuid4

from sqlalchemy import func, insert, or_, select, update

from qqbot.domain import InboundEvent, stable_id
from qqbot.domain.agents import ChildSpec, GoalSpec, ProgressSpec, WaitSpec
from qqbot.domain.execution import ExecutionPlan, TaskGoal
from qqbot.harness.artifacts import artifact_path
from qqbot.storage.action_schema import operations
from qqbot.storage.agent_schema import agent_events, leader_inbox
from qqbot.storage.job_schema import jobs
from qqbot.storage.task_schema import inputs, notifications, observations, runs, tasks

TERMINAL = frozenset({"SUCCEEDED", "PARTIAL", "BLOCKED", "FAILED", "CANCELLED", "EXPIRED"})
UNSETTLED = frozenset({"PREPARED", "DISPATCHED", "UNKNOWN", "PARTIAL", "RECONCILIATION"})


class TaskGraph:
    def __init__(self, service, settings):
        self.service, self.db, self.clock, self.settings = service, service.db, service.clock, settings

    def event(self, conn, task, kind, payload, *, attempt_id=None, event_id=None, wake=False):
        event_id = event_id or str(uuid4())
        previous = conn.execute(select(agent_events).where(agent_events.c.id == event_id)).mappings().first()
        if previous:
            if previous["task_id"] != task["id"] or json.loads(previous["payload_json"]) != payload:
                raise ValueError("conflicting task event replay")
            return event_id
        seq = (conn.execute(select(func.max(agent_events.c.seq)).where(
            agent_events.c.task_id == task["id"])).scalar_one() or 0) + 1
        now = self.clock.now().isoformat()
        conn.execute(insert(agent_events).values(id=event_id, task_id=task["id"], attempt_id=attempt_id,
            seq=seq, generation=task["fencing"], kind=kind, source_revision=task["goal_revision"],
            payload_json=json.dumps(payload, ensure_ascii=False), created_at=now))
        if wake:
            root = self.root(conn, task)
            if (task["id"] == root["id"] or task["parent_task_id"] == root["id"]
                    or kind == "needs_attention") and root["state"] not in TERMINAL | {"CANCEL_REQUESTED"}:
                conn.execute(insert(leader_inbox).values(id=stable_id(root["id"], event_id),
                    target_session_id=root["coordinator_session_id"], root_task_id=root["id"],
                    event_id=event_id, resume_reason=kind, source_revision=root["goal_revision"], created_at=now))
        return event_id

    def root(self, conn, task):
        return conn.execute(select(tasks).where(tasks.c.id == (task["root_task_id"] or task["id"]))).mappings().one()

    def dispatch_receipt(self, conn, owner_id, dispatch_key):
        task = conn.execute(select(tasks).where(tasks.c.owner_id == owner_id,
            tasks.c.dispatch_key == dispatch_key)).mappings().first()
        if task:
            return task, json.loads(task["context_json"]).get("dispatch_spec")
        receipt = conn.execute(select(agent_events).where(
            agent_events.c.id == stable_id("dispatch-reuse", owner_id, dispatch_key),
            agent_events.c.kind == "dispatch_reused")).mappings().first()
        if not receipt:
            return None, None
        task = conn.execute(select(tasks).where(tasks.c.id == receipt["task_id"],
            tasks.c.owner_id == owner_id)).mappings().one()
        return task, json.loads(receipt["payload_json"])["dispatch_spec"]

    @staticmethod
    def _request_context(context):
        return {
            **{key: context.get(key) for key in ("scope", "principal", "preferences", "memory_scope", "memory_epoch", "retrieved_data")},
            "images": [{key: value for key, value in image.items() if key not in {"source", "message_id"}}
                       for image in context.get("images", ())],
            "references": [source for source in context.get("source_manifest", ()) if source.get("kind") != "inbox"],
        }

    @staticmethod
    def _goal_intent(intent):
        return {**intent, "children": [{key: value for key, value in child.items() if key != "delegation_key"}
                                      for child in intent["children"]]}

    def submit(self, conn, dialogue, event, access, spec, dispatch_key, binding, context):
        spec = GoalSpec.model_validate_json(json.dumps(spec))
        for path in spec.requested_outputs:
            artifact_path(path)
        self.service.permissions.require(access, "task.create", "conversation:" + access.conversation.key)
        old, previous_intent = self.dispatch_receipt(conn, dialogue["owner_id"], dispatch_key)
        intent = spec.model_dump(mode="json")
        if old:
            if previous_intent != intent:
                raise ValueError("dispatch key reused for another goal")
            status = self.status(conn, old)
            return {**status, "reused": True} if old["dispatch_key"] != dispatch_key else status
        candidates = conn.execute(select(tasks).where(tasks.c.owner_id == dialogue["owner_id"],
            tasks.c.kind == "goal", tasks.c.origin_dialogue_id == dialogue["id"],
            tasks.c.conversation == event.conversation.key, tasks.c.requester == event.principal.key,
            tasks.c.state.not_in(TERMINAL | {"CANCEL_REQUESTED", "NEEDS_RECONCILIATION"}),
            tasks.c.goal_revision == 0).order_by(tasks.c.created_at, tasks.c.id)).mappings()
        for candidate in candidates:
            previous_context = json.loads(candidate["context_json"])
            previous_spec = previous_context.get("dispatch_spec")
            original = InboundEvent.model_validate_json(candidate["event_json"])
            same_source = (event.event_id != original.event_id and bool(event.text.strip())
                           and original.text == event.text and original.segments == event.segments)
            same_goal = (original.text == event.text and previous_spec
                         and self._goal_intent(previous_spec) == self._goal_intent(intent))
            if (not (same_source or same_goal) or original.reply_to != event.reply_to
                    or tuple(segment for segment in original.segments if segment.type != "text")
                        != tuple(segment for segment in event.segments if segment.type != "text")
                    or json.loads(candidate["binding_json"]) != binding
                    or self._request_context(previous_context) != self._request_context(context)):
                continue
            validator = self.service.context_validator
            if validator and not validator(access, previous_context):
                continue
            self.service.permissions.require(access, "task.read", "task:" + candidate["id"])
            self.event(conn, candidate, "dispatch_reused", {"dispatch_spec": intent},
                event_id=stable_id("dispatch-reuse", dialogue["owner_id"], dispatch_key))
            return {**self.status(conn, candidate), "reused": True}
        root_id = str(uuid4())
        plan = ExecutionPlan(initial_delivery="DETACHED", foreground_wait_ms=0,
                             budget=self.settings.execution.budget)
        task_context = {key: value for key, value in context.items()
                        if key in {"images", "limits", "timezone", "model_context_window", "source_manifest",
                            "scope", "principal", "preferences", "memory_scope", "memory_epoch",
                            "memory_status", "retrieved_data"}}
        task_context.update(dispatch_spec=intent, owner_id=dialogue["owner_id"], role="worker")
        root = self.service.new_task(root_id, event, access,
            TaskGoal(text=spec.goal, success_checks=spec.success_checks, requested_outputs=spec.requested_outputs),
            plan, context=task_context)
        root.update(source="dispatch:" + dispatch_key, kind="goal", root_task_id=root_id,
            owner_id=dialogue["owner_id"], origin_dialogue_id=dialogue["id"],
            coordinator_session_id=dialogue["session_id"], goal_revision=0,
            state="WAITING_CHILDREN", dispatch_key=dispatch_key, binding_json=json.dumps(binding))
        conn.execute(insert(tasks).values(**root))
        root = conn.execute(select(tasks).where(tasks.c.id == root_id)).mappings().one()
        for child in spec.children:
            self.spawn(conn, root, child, access)
        self.event(conn, root, "accepted", {"public_summary": "已接受，等待 Worker 执行"})
        self.service.wakeup.set()
        return self.status(conn, root)

    def spawn(self, conn, parent, spec, access):
        spec = spec if isinstance(spec, ChildSpec) else ChildSpec.model_validate_json(json.dumps(spec))
        for path in spec.requested_outputs:
            artifact_path(path)
        root = self.root(conn, parent)
        root_context = json.loads(root["context_json"])
        if root_context.get("memory_replan_pending"):
            raise PermissionError("memory replan awaits verified execution stop")
        if parent["kind"] == "goal" and root_context.get("memory_replan") and not spec.rework_of_task_id:
            raise PermissionError("memory replanning requires linked rework")
        if root["state"] in TERMINAL | {"CANCEL_REQUESTED"} or parent["state"] in TERMINAL:
            raise PermissionError("task tree is closed")
        if parent["kind"] == "consult" and spec.mode != "consult":
            raise PermissionError("consult cannot delegate business writes")
        old = conn.execute(select(tasks).where(tasks.c.parent_task_id == parent["id"],
            tasks.c.delegation_key == spec.delegation_key)).mappings().first()
        intent = spec.model_dump(mode="json")
        if old:
            if json.loads(old["context_json"]).get("child_spec") != intent:
                raise ValueError("delegation key reused for another child")
            return {"task_id": old["id"], "public_id": old["public_id"], "state": old["state"]}
        if spec.rework_of_task_id:
            previous = conn.execute(select(tasks).where(tasks.c.id == spec.rework_of_task_id,
                tasks.c.root_task_id == root["id"], tasks.c.state.in_(TERMINAL))).mappings().first()
            if not previous:
                raise PermissionError("rework must reference a settled child in this tree")
        allowed_refs = {root["id"], parent["id"]} | set(conn.execute(select(tasks.c.id).where(
            tasks.c.root_task_id == root["id"])).scalars())
        if set(spec.input_refs) - allowed_refs:
            raise PermissionError("input references outside this task tree")
        event = InboundEvent.model_validate_json(parent["event_json"])
        context = json.loads(root["context_json"])
        context.pop("dispatch_spec", None)
        context.update(child_spec=intent, root_task_id=root["id"], role="worker")
        # Workers receive only the task handoff, never the Leader transcript.
        context["key"] = str(uuid4())
        task_id = str(uuid4())
        plan = ExecutionPlan.model_validate_json(root["plan_json"])
        child = self.service.new_task(task_id, event, access,
            TaskGoal(text=spec.goal, success_checks=spec.success_checks, requested_outputs=spec.requested_outputs),
            plan, context=context)
        child.update(source="child:" + stable_id(parent["id"], spec.delegation_key), kind=spec.mode,
            owner_id=root["owner_id"], root_task_id=root["id"], parent_task_id=parent["id"],
            origin_dialogue_id=root["origin_dialogue_id"], coordinator_session_id=root["coordinator_session_id"],
            session_id=str(uuid4()), delegation_key=spec.delegation_key,
            rework_of_task_id=spec.rework_of_task_id, goal_revision=root["goal_revision"],
            deadline=root["deadline"], binding_json=root["binding_json"])
        conn.execute(insert(tasks).values(**child))
        self.event(conn, child, "queued", {"public_summary": "已排队"})
        self.service.wakeup.set()
        return {"task_id": task_id, "public_id": child["public_id"], "state": "QUEUED"}

    def status(self, conn, task):
        root = self.root(conn, task)
        context = json.loads(root["context_json"])
        replanning = bool(context.get("memory_replan_pending") or context.get("memory_replan"))
        stale_task = bool(context.get("memory_replan_pending") or replanning and
            json.loads(task["context_json"]).get("memory_epoch") != context.get("memory_epoch"))
        children = conn.execute(select(tasks.c.id, tasks.c.public_id, tasks.c.kind, tasks.c.state,
            tasks.c.goal, tasks.c.result_json, tasks.c.context_json).where(tasks.c.parent_task_id == task["id"])
            .order_by(tasks.c.created_at, tasks.c.id)).mappings().all()
        child_status = []
        for row in children:
            old_context = json.loads(row["context_json"])
            stale = replanning and (context.get("memory_replan_pending") or
                old_context.get("memory_epoch") != context.get("memory_epoch"))
            if stale:
                operation_ids = list(conn.execute(select(operations.c.id).where(operations.c.task_id == row["id"])).scalars())
                child_status.append({key: row[key] for key in ("id", "public_id", "kind", "state")} | {
                    "rework_required": True, "operation_ids": operation_ids})
            else:
                child_status.append({key: row[key] for key in ("id", "public_id", "kind", "state", "goal")} | {
                    "result_json": row["result_json"],
                    "result": json.loads(row["result_json"]) if row["result_json"] else None})
        last = conn.execute(select(agent_events.c.kind, agent_events.c.payload_json, agent_events.c.created_at)
            .where(agent_events.c.task_id == task["id"]).order_by(agent_events.c.seq.desc()).limit(1)).mappings().first()
        delivery = conn.execute(select(notifications.c.state).where(notifications.c.task_id == task["id"],
            notifications.c.purpose == "result")).scalars().all()
        return {"task_id": task["id"], "public_id": task["public_id"], "kind": task["kind"],
            "state": task["state"], "goal": None if stale_task else task["goal"],
            "goal_revision": task["goal_revision"], "children": child_status,
            "result": json.loads(task["result_json"]) if task["result_json"] and not stale_task else None,
            "last_progress": dict(last) if last and not replanning else None, "delivery_states": delivery}

    def check_authority(self, conn, task, payload):
        if not task["root_task_id"]:
            return
        root = self.root(conn, task)
        if root["state"] in TERMINAL | {"CANCEL_REQUESTED"}:
            raise PermissionError("root task authority revoked")
        write = payload.get("effect") not in {None, "read"}
        personal = payload.get("action", "").startswith(("skills.", "memory."))
        if task["kind"] == "consult" and write and not personal:
            raise PermissionError("consult is business read-only")
        if write and not personal:
            pending = conn.execute(select(inputs.c.id).where(inputs.c.task_id == task["id"],
                inputs.c.kind.in_(["followup", "answer"]), inputs.c.consumed_at.is_(None))).first()
            if pending or task["goal_revision"] != root["goal_revision"]:
                raise PermissionError("unconsumed revision blocks new writes")
            if payload.get("action") != "workspace.job.cancel" and conn.execute(select(jobs.c.id).where(
                    jobs.c.task_id == task["id"], jobs.c.cancel_requested_at.is_not(None),
                    jobs.c.state.not_in({"EXITED", "FAILED", "CANCELLED"}))).first():
                raise PermissionError("revoked Jobs have not been proven stopped")

    def settled(self, conn, task, run_id, result):
        self.event(conn, task, "child_result", {"result": result}, attempt_id=run_id,
                   event_id="result:" + run_id, wake=True)
        if task["state"] in {"FAILED", "BLOCKED", "EXPIRED", "PARTIAL"}:
            self.event(conn, task, "needs_attention", {"result": result, "state": task["state"]},
                attempt_id=run_id, event_id="attention:" + run_id, wake=True)
            return
        parent_id = task["parent_task_id"]
        if parent_id:
            parent = conn.execute(select(tasks).where(tasks.c.id == parent_id)).mappings().one()
            if parent["kind"] != "goal":
                self.wake_parent(conn, parent)

    def wait(self, conn, task, spec, run_id):
        spec = WaitSpec.model_validate_json(json.dumps(spec))
        children = conn.execute(select(tasks).where(tasks.c.parent_task_id == task["id"],
            or_(tasks.c.id.in_(spec.children), tasks.c.public_id.in_(spec.children)))).mappings().all()
        if len(children) != len(set(spec.children)):
            raise PermissionError("wait accepts this task's direct children only")
        condition = {"children": [child["id"] for child in children], "predicate": spec.predicate,
                     "attempt_id": run_id, "yielded": False, "revision": task["continuation_revision"] + 1}
        conn.execute(update(tasks).where(tasks.c.id == task["id"]).values(wait_json=json.dumps(condition),
            continuation_revision=condition["revision"]))
        return {"accepted": True, "waiting": True, "wait_revision": condition["revision"]}

    def wake_parent(self, conn, parent):
        if parent["state"] != "WAITING_CHILDREN" or not parent["wait_json"]:
            return False
        condition = json.loads(parent["wait_json"])
        if not condition["yielded"]:
            return False
        children = conn.execute(select(tasks).where(tasks.c.id.in_(condition["children"]))).mappings().all()
        settled = [child["state"] in TERMINAL for child in children]
        ready = all(settled) if condition["predicate"] == "all" else any(settled)
        if not ready:
            return False
        if condition["predicate"] == "any":
            for child in children:
                if child["state"] not in TERMINAL:
                    self.cancel_tree(conn, child)
        conn.execute(update(tasks).where(tasks.c.id == parent["id"]).values(state="QUEUED"))
        self.event(conn, parent, "children_ready", {"wait_revision": condition["revision"]},
            event_id=stable_id(parent["id"], "wait", str(condition["revision"])))
        self.service.wakeup.set()
        return True

    def finalize_ready(self, conn, root, *, allow_pending_inputs=False):
        if json.loads(root["context_json"]).get("memory_replan_pending"):
            return False
        children = conn.execute(select(tasks.c.state).where(tasks.c.root_task_id == root["id"], tasks.c.id != root["id"])).scalars().all()
        active = conn.execute(select(runs.c.id).where(runs.c.slot_reserved == 1, runs.c.task_id.in_(
            select(tasks.c.id).where(tasks.c.root_task_id == root["id"])), runs.c.role == "worker")).first()
        uncertain = conn.execute(select(operations.c.id).where(operations.c.task_id.in_(
            select(tasks.c.id).where(tasks.c.root_task_id == root["id"])), operations.c.state.in_(UNSETTLED))).first()
        pending = conn.execute(select(inputs.c.id).where(inputs.c.task_id == root["id"],
            inputs.c.kind != "notify", inputs.c.consumed_at.is_(None))).first()
        stopping = conn.execute(select(jobs.c.id).where(jobs.c.root_task_id == root["id"],
            jobs.c.cancel_requested_at.is_not(None), jobs.c.state.not_in({"EXITED", "FAILED", "CANCELLED"}))).first()
        return bool(children and all(state in TERMINAL for state in children) and not active and not uncertain
                    and not stopping and (allow_pending_inputs or not pending))

    def verify_final_result(self, conn, root, result, checks=None):
        if checks is None:
            checks = json.loads(root["goal_json"]).get("success_checks", [])
        tree = select(tasks.c.id).where(tasks.c.root_task_id == root["id"])
        for check in checks:
            if check == "artifact" and result.artifacts:
                continue
            if check == "observation" and result.observation_refs:
                found = set(conn.execute(select(observations.c.id).where(
                    observations.c.task_id.in_(tree), observations.c.id.in_(result.observation_refs))).scalars())
                if found == set(result.observation_refs):
                    continue
            if check == "effect" and result.effect_refs:
                found = set(conn.execute(select(operations.c.id).where(operations.c.task_id.in_(tree),
                    operations.c.id.in_(result.effect_refs), operations.c.state == "VERIFIED")).scalars())
                if found == set(result.effect_refs):
                    continue
            raise ValueError("root success check not verified: " + check)

    def cancel_tree(self, conn, task, reason="user_cancel"):
        ids, frontier = {task["id"]}, {task["id"]}
        while frontier:
            frontier = set(conn.execute(select(tasks.c.id).where(tasks.c.parent_task_id.in_(frontier))).scalars()) - ids
            ids |= frontier
        rows = {row["id"]: row for row in conn.execute(select(tasks).where(tasks.c.id.in_(ids))).mappings()}
        active = set(conn.execute(select(runs.c.task_id).where(runs.c.task_id.in_(ids), runs.c.slot_reserved == 1)).scalars())
        for task_id in tuple(active):
            parent_id = rows[task_id]["parent_task_id"]
            while parent_id in rows and parent_id not in active:
                active.add(parent_id)
                parent_id = rows[parent_id]["parent_task_id"]
        for row in rows.values():
            if row["state"] in TERMINAL:
                continue
            jobs_quiescent = getattr(self.service, "job_tree_quiescent", lambda _task: True)(row)
            conn.execute(update(tasks).where(tasks.c.id == row["id"]).values(
                state="CANCEL_REQUESTED" if row["id"] in active or not jobs_quiescent else "CANCELLED", stop_reason=reason))
        conn.execute(update(leader_inbox).where(leader_inbox.c.root_task_id.in_(ids),
            leader_inbox.c.handled_at.is_(None)).values(handled_at=self.clock.now().isoformat(), output_ref="SUPERSEDED"))
        self.service.wakeup.set()

    def progress(self, conn, task, run_id, payload, call_id):
        spec = ProgressSpec.model_validate_json(json.dumps(payload))
        verified = set(conn.execute(select(observations.c.id).where(observations.c.task_id == task["id"],
            observations.c.id.in_(spec.evidence_refs))).scalars())
        verified |= set(conn.execute(select(operations.c.id).where(operations.c.task_id == task["id"],
            operations.c.id.in_(spec.evidence_refs), operations.c.state == "VERIFIED")).scalars())
        if set(spec.evidence_refs) - verified:
            raise PermissionError("milestone references unverified evidence")
        if not verified:
            return {"accepted": False, "reason": "no_verified_evidence"}
        last = conn.execute(select(agent_events).where(agent_events.c.task_id == task["id"],
            agent_events.c.kind == "milestone").order_by(agent_events.c.seq.desc()).limit(2)).mappings().all()
        now = self.clock.now()
        recent = [row for row in last if (now - datetime.fromisoformat(row["created_at"])).total_seconds() < 600]
        if (now - datetime.fromisoformat(task["created_at"])).total_seconds() < 60 or len(recent) >= 2:
            return {"accepted": False, "reason": "coalesced"}
        if last and ((now - datetime.fromisoformat(last[0]["created_at"])).total_seconds() < 60
                     or json.loads(last[0]["payload_json"]) == spec.model_dump(mode="json")):
            return {"accepted": False, "reason": "coalesced"}
        self.event(conn, task, "milestone", spec.model_dump(mode="json"), attempt_id=run_id,
                   event_id=call_id, wake=True)
        return {"accepted": True}
