"""Short Leader attempts over the same Harness; the host owns goals and delivery."""
import asyncio
import json
from datetime import datetime, timedelta

from sqlalchemy import insert, select, update

from qqbot.domain import InboundEvent, stable_id
from qqbot.domain.agents import TaskInputSpec
from qqbot.domain.tasks import TaskResult, validate_user_facing_text
from qqbot.storage.agent_schema import dialogue_inputs, dialogues, leader_inbox
from qqbot.storage.schema import outbox
from qqbot.storage.task_schema import (
    events,
    inputs,
    model_requests,
    observations,
    processes,
    runs,
    sessions,
    tasks,
)

from .task_graph import TERMINAL


class LeaderRuntime:
    def __init__(self, core, session_service):
        self.core, self.sessions = core, session_service
        self.service, self.db, self.clock = core.tasks, core.db, core.clock
        self._review_marks = {}

    def observe_operation(self, identity, operation, payload, result=None):
        self.sessions.learning.observe(identity, operation, payload or {}, result)

    def claim_review(self, worker_id):
        return self.sessions.learning.claim_review(worker_id)

    def lease(self, conn, run_id, fencing, worker_id):
        run = conn.execute(select(runs).where(runs.c.id == run_id)).mappings().first()
        if (not run or run["role"] not in {"leader", "skill_review"} or run["worker"] != worker_id
                or run["fencing"] != fencing or not run["slot_reserved"]
                or run["lease"] <= self.clock.now().isoformat()):
            raise PermissionError("stale Leader attempt")
        return run, json.loads(run["request_json"])

    def authorize(self, conn, run, claim, payload):
        event = InboundEvent.model_validate_json(claim["event_json"])
        if not self.core.allowed(event) or run["state"] != "RUNNING":
            raise PermissionError("Leader input is no longer authorized")
        if not claim.get("memory_mutation") and claim["deadline"] <= self.clock.now().isoformat():
            raise PermissionError("Leader turn deadline expired")
        root_id = claim.get("target_root_task_id")
        if root_id:
            root = conn.execute(select(tasks).where(tasks.c.id == root_id)).mappings().one()
            if root["state"] in TERMINAL | {"CANCEL_REQUESTED"} or root["goal_revision"] != claim["basis_goal_revision"]:
                raise PermissionError("Leader target version is no longer current")
        action = payload.get("action", "task.execute")
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
        personal = action.startswith(("skills.", "memory."))
        if effect != "read" and not personal:
            raise PermissionError("Leader business execution must be delegated to a Worker")
        if personal and self.sessions.owner(event) is None:
            raise PermissionError("personal knowledge requires a verified owner binding")
        for resource in payload.get("resources") or ["conversation:" + access.conversation.key]:
            self.service.permissions.require(access, action, resource)
        return {"allowed": True, "policy_version": self.service.permissions.policy_version}

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
            from qqbot.storage.learning_schema import learning_sessions
            source = conn.execute(select(learning_sessions).where(
                learning_sessions.c.claimed_attempt_id == run["id"])).mappings().first()
            if source and source["pending_json"]:
                pending = json.loads(source["pending_json"])
                pending["used"] = used
                conn.execute(update(learning_sessions).where(learning_sessions.c.session_id == source["session_id"])
                    .values(pending_json=json.dumps(pending, ensure_ascii=False)))

    def _account_turn(self, conn, run, claim):
        if claim["purpose"] != "user" and run["role"] != "skill_review":
            return
        used = json.loads(run["budget_json"])
        now = self.clock.now()
        elapsed = max(0, (now - datetime.fromisoformat(run["accounted_at"])).total_seconds()) if run["accounted_at"] else 0
        used["active_seconds"] = min(claim["plan"]["budget"]["active_seconds"], used.get("active_seconds", 0) + elapsed)
        conn.execute(update(runs).where(runs.c.id == run["id"]).values(budget_json=json.dumps(used), accounted_at=now.isoformat()))
        self._persist_turn_budget(conn, run, claim, used)

    def receipt(self, conn, run, claim, payload):
        expected = set(claim["source_ids"]) | set(claim.get("task_input_ids", []))
        confirmed = payload.get("input_ids")
        if (payload.get("session_id") != claim["session_id"] or not isinstance(confirmed, list)
                or set(confirmed) != expected or not isinstance(payload.get("session_revision"), str)
                or payload.get("run_id") != run["id"]):
            raise PermissionError("session receipt does not match the claimed inputs")
        kind = payload.get("kind")
        if kind not in {"input_persisted", "input_consumed", "turn_finished", "run_yielded"}:
            raise ValueError("invalid session receipt kind")
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
            root_budget = json.loads(root["plan_json"])["budget"]
            root_limit = root_budget[key] - (root_budget.get("completion_reserve", 1)
                if key == "model_calls" and claim["purpose"] != "finalize" else 0)
            if root_used.get(key, 0) >= root_limit:
                raise ValueError("root budget exhausted")
            root_used[key] = root_used.get(key, 0) + 1
            conn.execute(update(tasks).where(tasks.c.id == root["id"]).values(budget_json=json.dumps(root_used)))
        used[key] = used.get(key, 0) + 1
        conn.execute(update(runs).where(runs.c.id == run["id"]).values(budget_json=json.dumps(used)))
        self._persist_turn_budget(conn, run, claim, used)
        conn.execute(insert(model_requests).values(id=request_id, run_id=run["id"], task_id=claim["task_id"],
            kind=kind, state="RESERVED", created_at=self.clock.now().isoformat()))
        remaining = limit - used[key]
        if claim.get("target_root_task_id"):
            remaining = min(remaining, root_limit - root_used[key])
        return {"accepted": True, "remaining": remaining}

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
        if operation == "tasks.submit":
            if purpose != "user":
                raise PermissionError("only real user turns can create a root goal")
            dialogue = conn.execute(select(dialogues).where(dialogues.c.session_id == claim["session_id"])).mappings().one()
            return graph.submit(conn, dialogue, event, access, args,
                stable_id(owner, claim["source_ids"][0], payload["call_id"]), claim["binding"], claim["context"])
        if operation == "tasks.list":
            rows = conn.execute(select(tasks).where(tasks.c.owner_id == owner, tasks.c.kind == "goal",
                tasks.c.conversation == access.conversation.key).order_by(tasks.c.created_at.desc()).limit(20)).mappings().all()
            return [graph.status(conn, row) for row in rows]
        if operation in {"tasks.spawn", "tasks.wait"}:
            if purpose != "finalize" or not root_id:
                raise PermissionError("only a bound finalize turn may add rework")
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
        if operation == "tasks.status":
            self.service.permissions.require(access, "task.read", "task:" + task["id"])
            return graph.status(conn, task)
        if operation == "tasks.cancel":
            if purpose not in {"user", "finalize"}:
                raise PermissionError("this Leader turn cannot cancel work")
            self.service.permissions.require(access, "task.cancel", "task:" + task["id"])
            graph.cancel_tree(conn, task)
            return {"accepted": True, "state": "CANCEL_REQUESTED"}
        if operation == "tasks.send_input":
            spec = TaskInputSpec.model_validate_json(json.dumps(args))
            if purpose != "user":
                raise PermissionError("this Leader turn cannot revise the goal")
            if spec.kind != "notify":
                # Revisions are linked to the actual incoming instruction, never a made-up actor.
                text = event.text
            else:
                text = spec.text
            return self.service.add_graph_input(conn, task, event, access, text,
                kind=spec.kind, question_id=spec.question_id, source_id=stable_id(event.event_id, payload["call_id"]))
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
            reply_context=event.reply_context, requester=event.principal, ttl_seconds=300,
            anchor_message_id=event.platform_message_id, purpose="result")
        conn.execute(update(dialogue_inputs).where(dialogue_inputs.c.id == source["id"]).values(
            state="HANDLED", handled_at=now, claimed_attempt_id=run["id"]))
        conn.execute(update(runs).where(runs.c.id == run["id"]).values(**values))
        conn.execute(update(sessions).where(sessions.c.active_run_id == run["id"]).values(active_run_id=None))
        self.service.wakeup.set()
        return {"state": "FINISHED"}

    def complete(self, run_id, fencing, worker_id, result, *, stopped, quiescent, process=None, receipt=None):
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
            run, claim = self.lease(conn, run_id, fencing, worker_id)
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
            if mutation:
                self.authorize(conn, run, claim, {"action": "task.execute"})
                return self.finalize_committed_mutation(conn, run, claim, stopped=stopped)
            if not result.agent_authored and not mutation:
                table = dialogue_inputs if claim["purpose"] == "user" else leader_inbox
                source_values = {"claimed_attempt_id": None}
                if claim.get("target_root_task_id") and claim["purpose"] != "finalize":
                    root = conn.execute(select(tasks).where(tasks.c.id == claim["target_root_task_id"])).mappings().one()
                    root_budget = json.loads(root["plan_json"])["budget"]
                    if json.loads(root["budget_json"]).get("model_calls", 0) >= (
                            root_budget["model_calls"] - root_budget.get("completion_reserve", 1)):
                        source_values.update(handled_at=now, output_ref="ROOT_EXECUTION_BUDGET_EXHAUSTED")
                conn.execute(update(table).where(table.c.id.in_(claim["source_ids"]),
                    table.c.claimed_attempt_id == run_id, table.c.handled_at.is_(None)).values(**source_values))
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
                if claim.get("target_root_task_id"):
                    self.service._settle_exhausted_roots(conn)
                self.service.wakeup.set()
                return {"state": "FINISHED"}
            self.authorize(conn, run, claim, {"action": "task.execute"})
            target = None
            if claim.get("target_root_task_id"):
                target = conn.execute(select(tasks).where(tasks.c.id == claim["target_root_task_id"])).mappings().one()
            if claim["purpose"] == "finalize" and result.agent_authored:
                if not target or target["fencing"] != fencing or target["goal_revision"] != claim["basis_goal_revision"]:
                    raise PermissionError("stale finalize generation or revision")
                if target["wait_json"] and json.loads(target["wait_json"]).get("attempt_id") == run_id:
                    condition = json.loads(target["wait_json"])
                    condition["yielded"] = True
                    conn.execute(update(tasks).where(tasks.c.id == target["id"]).values(
                        state="WAITING_CHILDREN", wait_json=json.dumps(condition)))
                else:
                    if not self.service.graph.finalize_ready(conn, target):
                        raise PermissionError("required children, inputs or operations are not settled")
                    descendants = conn.execute(select(tasks).where(tasks.c.root_task_id == target["id"],
                        tasks.c.id != target["id"])).mappings().all()
                    if result.observation_refs:
                        self.service.graph.verify_final_result(conn, target, result, checks=("observation",))
                    if result.effect_refs:
                        self.service.graph.verify_final_result(conn, target, result, checks=("effect",))
                    artifacts = {item.id: item.model_dump(mode="json") for item in result.artifacts}
                    evidence = {item.url: item.model_dump(mode="json") for item in result.evidence}
                    effects, observed = set(result.effect_refs), set(result.observation_refs)
                    for child in descendants:
                        if (claim["context"].get("memory_replan") and
                                json.loads(child["context_json"]).get("memory_epoch") != claim["context"].get("memory_epoch")):
                            continue
                        child_result = json.loads(child["result_json"]) if child["result_json"] else {}
                        for artifact in child_result.get("artifacts", []):
                            artifacts[artifact["id"]] = artifact
                        for item in child_result.get("evidence", []):
                            evidence[item["url"]] = item
                        effects.update(child_result.get("effect_refs", []))
                        observed.update(child_result.get("observation_refs", []))
                    result = result.model_copy(update={"artifacts": tuple(__import__("qqbot.domain.tasks", fromlist=["Artifact"]).Artifact.model_validate(a) for a in artifacts.values()),
                        "evidence": tuple(__import__("qqbot.domain.tasks", fromlist=["Evidence"]).Evidence.model_validate(e) for e in evidence.values()),
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
                    self.service._notification(conn, target, run_id, result.summary, "result", "important",
                        "final:" + target["id"] + ":" + str(target["goal_revision"]))
            elif claim["purpose"] in {"progress", "question"} and result.agent_authored:
                purpose = "question" if claim["purpose"] == "question" else "progress"
                self.service._notification(conn, target, run_id, result.summary, purpose, "normal",
                    stable_id("leader", run_id))
            elif run["role"] != "skill_review":
                event = InboundEvent.model_validate_json(claim["event_json"])
                self.core.outbox.enqueue_text(conn, target=event.conversation, text=result.summary,
                    origin_kind="chat", origin_id="turn:" + claim["session_id"] + ":" + claim["source_ids"][0],
                    reply_context=event.reply_context, requester=event.principal, ttl_seconds=300,
                    anchor_message_id=event.platform_message_id, purpose="result")
            table = dialogue_inputs if claim["purpose"] == "user" else leader_inbox
            conn.execute(update(table).where(table.c.id.in_(claim["source_ids"]), table.c.claimed_attempt_id == run_id).values(
                handled_at=now, **({"state": "HANDLED"} if table is dialogue_inputs else {"output_ref": run_id})))
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

    def recover(self):
        """Reconcile expired Leader attempts after the launcher proves their stop."""
        with self.db.engine.connect() as conn:
            expired = conn.execute(select(runs.c.id).where(runs.c.role.in_(["leader", "skill_review"]),
                runs.c.slot_reserved == 1, runs.c.lease <= self.clock.now().isoformat())).scalars().all()
        return [self.recover_attempt(run_id) for run_id in expired]

    def recover_attempt(self, run_id):
        from qqbot.adapters.pi.sessions import boundary_receipts, restore_tool_results, unfinished_tools
        from qqbot.domain.agents import GoalSpec

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
        proof = json.loads(run["receipt_json"]) if run["receipt_json"] else None
        path = identity.get("actual_session_file") or run["session"]
        mutation = claim.get("memory_mutation")
        try:
            pending = () if mutation else unfinished_tools(path)
            submitted = [call for call in pending if call.get("name") == "tasks_submit"]
            if pending:
                if len(submitted) != len(pending) or run["role"] != "leader" or claim["purpose"] != "user":
                    raise ValueError("native dispatch recovery requires an isolated owner submission")
                receipts = []
                with self.db.engine.connect() as conn:
                    source = conn.execute(select(dialogue_inputs).where(
                        dialogue_inputs.c.id == claim["source_ids"][0],
                        dialogue_inputs.c.dialogue_id == claim["context"]["dialogue_id"])).mappings().first()
                    if not source:
                        raise ValueError("native dispatch source no longer matches its original Dialogue")
                    owner = claim["context"]["owner_id"]
                    for call in submitted:
                        task = conn.execute(select(tasks).where(tasks.c.owner_id == owner,
                            tasks.c.dispatch_key == stable_id(owner, source["id"], call["id"]),
                            tasks.c.origin_dialogue_id == source["dialogue_id"],
                            tasks.c.coordinator_session_id == claim["session_id"])).mappings().first()
                        intent = GoalSpec.model_validate_json(json.dumps(call["arguments"])).model_dump(mode="json")
                        if not task or json.loads(task["context_json"]).get("dispatch_spec") != intent:
                            raise ValueError("native dispatch has no matching committed host intent")
                        receipts.append({"call": call, "result": self.service.graph.status(conn, task)})
                restore_tool_results(path, receipts, pi_executable=self.sessions.settings.orchestration.pi_executable)
        except (OSError, ValueError, KeyError):
            with self.db.transaction() as conn:
                conn.execute(update(runs).where(runs.c.id == run_id, runs.c.slot_reserved == 1)
                    .values(state="NEEDS_RECONCILIATION"))
            return {"run_id": run_id, "state": "NEEDS_RECONCILIATION"}
        try:
            native = [item for item in boundary_receipts(path) if item.get("run_id") == run_id]
            if native:
                proof = {**native[-1], **(proof or {})}
        except (OSError, ValueError, KeyError):
            # The accepted input remains queued until its native log can be resumed.
            proof = None
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
            conn.execute(update(runs).where(runs.c.id == run_id).values(state="RUNNING",
                lease=(self.clock.now() + timedelta(seconds=30)).isoformat()))
        if mutation:
            outcome = self.complete(run_id, run["fencing"], run["worker"],
                {"status": "completed", "summary": mutation["summary"], "agent_authored": False},
                stopped=True, quiescent=False)
            return {"run_id": run_id, **outcome}
        if proof and proof.get("summary"):
            try:
                outcome = self.complete(run_id, run["fencing"], run["worker"],
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
            table = dialogue_inputs if claim["purpose"] == "user" else leader_inbox
            conn.execute(update(table).where(table.c.id.in_(claim["source_ids"]),
                table.c.claimed_attempt_id == run_id, table.c.handled_at.is_(None)).values(claimed_attempt_id=None))
            self._account_turn(conn, current, claim)
            if run["role"] == "skill_review":
                self.sessions.learning.finish_review(conn, run_id, retry=True)
                self._review_marks.pop(run_id, None)
            conn.execute(update(runs).where(runs.c.id == run_id).values(state="RECOVERED", slot_reserved=0,
                runner_stopped_at=now, quiescent_at=now))
            conn.execute(update(sessions).where(sessions.c.active_run_id == run_id).values(active_run_id=None))
        self.service.wakeup.set()
        return {"run_id": run_id, "state": "RECOVERED"}

    async def invoke(self, operation, identity, payload=None, **values):
        run_id, _fencing, worker_id = identity
        payload = payload or {}
        if operation == "result":
            return self.complete(*identity, values["result"], stopped=values.get("stopped", False),
                quiescent=values.get("quiescent", False), process=values.get("process"), receipt=values.get("receipt"))
        if operation == "skills":
            with self.db.engine.connect() as conn:
                run, claim = self.lease(conn, *identity)
                name = payload.get("operation")
                self.authorize(conn, run, claim, {"action": "skills.write" if name == "skill_manage" else "skills.read"})
            adapter = self.service.skills_provider(self.sessions.access_for_claim(claim))
            if adapter is None:
                raise PermissionError("owner skill scope unavailable")
            if run["role"] == "skill_review":
                from qqbot.adapters.hermes_skills._vendor.skill_manager_guards import (
                    _BackgroundReviewReadMarks,
                )
                marks = self._review_marks.setdefault(run_id, _BackgroundReviewReadMarks())
                def call_review_tool():
                    with adapter.review_context(marks):
                        return adapter.call(name, payload.get("arguments", {}),
                            task_id=claim["task_id"], session_id=claim["session_id"])
                return await asyncio.to_thread(call_review_tool)
            return await asyncio.to_thread(adapter.call, name, payload.get("arguments", {}),
                task_id=claim["task_id"], session_id=claim["session_id"])
        with self.db.transaction() as conn:
            run, claim = self.lease(conn, *identity)
            if operation == "authorize":
                return self.authorize(conn, run, claim, payload)
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
            if operation in {"control", "renew"}:
                state = run["state"]
                try:
                    self.authorize(conn, run, claim, {"action": "task.execute"})
                except PermissionError:
                    state = "CANCEL_REQUESTED"
                if operation == "renew":
                    conn.execute(update(runs).where(runs.c.id == run_id).values(lease=(self.clock.now() + timedelta(seconds=30)).isoformat()))
                return {"state": state, "delivery": "ATTACHED"}
            if operation == "inputs":
                return []
            if operation == "notify":
                return {"accepted": False, "reason": "Leader replies when its short turn ends"}
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
                if known:
                    conn.execute(update(sessions).where(sessions.c.id == known["id"]).values(active_run_id=run_id))
                else:
                    conn.execute(insert(sessions).values(id=claim["session_id"], scope_key=claim["session_id"],
                        session_file=payload["session_file"], manifest_json=json.dumps(claim["binding"]), active_run_id=run_id, epoch=1))
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
                prior = conn.execute(select(events).where(events.c.event_id == values["event_id"])).mappings().first()
                if prior:
                    return {"accepted": prior["run_id"] == run_id}
                if values["seq"] != run["last_seq"] + 1:
                    raise ValueError("event sequence gap")
                conn.execute(insert(events).values(event_id=values["event_id"], run_id=run_id,
                    seq=values["seq"], payload=json.dumps(payload)))
                conn.execute(update(runs).where(runs.c.id == run_id).values(last_seq=values["seq"]))
                return {"accepted": True}
            if operation == "recovery-status":
                return {"discard_spool": False}
            if operation == "uncertain":
                conn.execute(update(runs).where(runs.c.id == run_id).values(state="NEEDS_RECONCILIATION"))
                return {"recorded": True}
            if operation == "ask":
                raise PermissionError("Leader asks in its final reply; Worker questions use persistent task waits")
            if operation == "ledger":
                # Leader business writes are disallowed; Hermes owns its file ledger.
                raise PermissionError("Leader cannot dispatch external business writes")
        raise ValueError("unsupported Leader operation: " + operation)
