import asyncio
import hashlib
import json
import os
import stat
from datetime import datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

from sqlalchemy import func, insert, select, update

from qqbot.domain.execution import ExecutionPlan, RunBudget, TaskGoal, WaitCondition
from qqbot.domain.messages import AccessContext, InboundEvent
from qqbot.domain.tasks import TaskResult, validate_user_facing_text
from qqbot.harness.launcher import verify_stop_marker
from qqbot.jobs.streaming import PUBLIC_KINDS, validate_public_event
from qqbot.permissions import PermissionService
from qqbot.storage.schema import outbox as outbox_table
from qqbot.storage.stream_schema import stream_runs
from qqbot.storage.task_schema import events, inputs, model_requests, notifications, runs, tasks, waits

TERMINAL = {"SUCCEEDED", "PARTIAL", "BLOCKED", "FAILED", "CANCELLED", "EXPIRED"}


class TaskService:
    def __init__(self, db, clock, outbox, archive_root=None, stop_verifier=None, *, permissions=None, max_active=4, sustained_limit=2, max_resident=None, context_validator=None, budget_ceiling=None, stream_eligibility=None):
        self.db, self.clock, self.outbox = db, clock, outbox
        self.permissions = permissions or PermissionService(db, clock)
        self.max_active, self.sustained_limit = max_active, sustained_limit
        self.max_resident = max_resident if max_resident is not None else max_active
        self.budget_ceiling = budget_ceiling or RunBudget()
        self.context_validator = context_validator
        self.stream_eligibility = stream_eligibility
        self.wakeup = asyncio.Event()
        self.stop_verifier = stop_verifier or verify_stop_marker
        self.archive_root = Path(archive_root) if archive_root else Path(db.path).parent / "artifacts"

    @staticmethod
    def _time_limited(task):
        return not task["root_task_id"] and task["delivery"] != "DETACHED"

    def _deadline_expired(self, task):
        return self._time_limited(task) and task["deadline"] <= self.clock.now().isoformat()

    def accept(self, event, access, goal, profile="pi", *, retry_of_task_id=None, plan=None, context=None):
        if access.principal != event.principal or access.conversation != event.conversation:
            raise PermissionError("identity mismatch")
        self.permissions.require(access, "task.create", "conversation:" + access.conversation.key)
        if profile != "pi":
            raise ValueError("unsupported runner")
        goal = goal if isinstance(goal, TaskGoal) else TaskGoal(text=goal)
        plan = plan or ExecutionPlan()
        with self.db.transaction() as c:
            old = c.execute(select(tasks).where(tasks.c.source == event.event_id)).mappings().first()
            if old:
                if old["goal"] != goal.text or old["retry_of_task_id"] != retry_of_task_id:
                    raise ValueError("source event already accepted with a different goal")
                return dict(old)
            task_id = str(uuid4())
            row = self.new_task(task_id, event, access, goal, plan, context=context,
                                retry_of_task_id=retry_of_task_id)
            c.execute(insert(tasks).values(**row))
            self.wakeup.set()
            return row

    def new_task(self, task_id, event, access, goal, plan, *, context=None, retry_of_task_id=None):
        now = self.clock.now()
        deadline = (now + timedelta(seconds=plan.budget.total_wall_seconds)).isoformat()
        return {"id": task_id, "public_id": "T-" + task_id[:12], "source": event.event_id,
                       "conversation": event.conversation.key, "requester": event.principal.key,
                       "event_json": event.model_dump_json(), "goal": goal.text, "profile": "pi", "state": "QUEUED",
                       "created_at": now.isoformat(), "deadline": deadline, "fencing": 0,
                       "retry_of_task_id": retry_of_task_id, "protocol_version": 2,
                       "goal_json": goal.model_dump_json(), "plan_json": plan.model_dump_json(),
                       "context_json": json.dumps(context or {}, ensure_ascii=False),
                       "delivery": plan.initial_delivery,
                       "detach_at": (now + timedelta(milliseconds=plan.foreground_wait_ms)).isoformat(),
                       "continuation_revision": 0, "budget_json": json.dumps({"model_calls": 0, "tool_calls": 0}),
                       "permission_json": json.dumps({"principal": access.principal.model_dump(mode="json"),
                           "identity_source": "trusted_access_context", "role": access.role,
                           "scope": access.conversation.key, "budget": plan.budget.model_dump(),
                           "policy_version": self.permissions.check(access, "task.create", "conversation:" + access.conversation.key).policy_version,
                           "expires_at": deadline})}

    def detach_due(self):
        with self.db.transaction() as c:
            c.execute(update(tasks).where(tasks.c.delivery == "ATTACHED",
                tasks.c.detach_at <= self.clock.now().isoformat(), tasks.c.state.not_in(TERMINAL))
                .values(delivery="DETACHED"))

    def _access_for(self, task):
        event = InboundEvent.model_validate_json(task["event_json"])
        return AccessContext(principal=event.principal, conversation=event.conversation,
                             role=json.loads(task["permission_json"]).get("role", "member"))

    def _visible(self, c, public_id, access):
        row = (
            c.execute(
                select(tasks).where(
                    tasks.c.public_id == public_id, tasks.c.conversation == access.conversation.key
                )
            )
            .mappings()
            .first()
        )
        if not row:
            raise KeyError(public_id)
        if row["owner_id"] and row["requester"] != access.principal.key:
            raise PermissionError("task belongs to another subject")
        self.permissions.require(access, "task.read", "task:" + row["id"])
        return row

    def get(self, public_id, access):
        with self.db.engine.connect() as c:
            row = dict(self._visible(c, public_id, access))
        return {
            "task_id": row["id"],
            **{
                k: row[k]
                for k in (
                    "id",
                    "public_id",
                    "goal",
                    "state",
                    "result_json",
                    "stop_reason",
                    "retry_of_task_id",
                )
            },
        }

    def cancel(self, public_id, access):
        with self.db.transaction() as c:
            row = self._visible(c, public_id, access)
            self.permissions.require(access, "task.cancel", "task:" + row["id"])
            if row["root_task_id"] and hasattr(self, "graph"):
                self.graph.cancel_tree(c, row)
                state = c.execute(select(tasks.c.state).where(tasks.c.id == row["id"])).scalar_one()
                return {"public_id": public_id, "state": state}
            state = row["state"]
            if state not in TERMINAL:
                state = "CANCELLED" if state in {"QUEUED", "WAITING_INPUT", "WAITING_EXTERNAL"} else "CANCEL_REQUESTED"
                c.execute(
                    update(tasks)
                    .where(tasks.c.id == row["id"])
                    .values(state=state, stop_reason="user_cancel")
                )
            return {"public_id": public_id, "state": state}

    def retry(self, public_id, event, access, *, context=None):
        with self.db.engine.connect() as c:
            old = self._visible(c, public_id, access)
            if old["state"] not in TERMINAL:
                raise ValueError("only a terminal task can be retried")
            self.permissions.require(access, "task.retry", "task:" + old["id"])
            goal = TaskGoal.model_validate_json(old["goal_json"]) if old["goal_json"] != "{}" else TaskGoal(text=old["goal"])
            plan = ExecutionPlan.model_validate_json(old["plan_json"]) if old["plan_json"] != "{}" else ExecutionPlan()
            profile, task_id = old["profile"], old["id"]
        return self.accept(event, access, goal, profile, retry_of_task_id=task_id, plan=plan, context=context or {})

    @staticmethod
    def _notification_budget(task):
        original = ExecutionPlan.model_validate_json(task["plan_json"]).budget
        return RunBudget(model_calls=2, tool_calls=0, active_seconds=30, total_wall_seconds=30,
                         output_tokens_per_call=min(original.output_tokens_per_call, 1024))

    def _run_limits(self, run, task):
        if run["run_reason"] == "notification":
            deadline = datetime.fromisoformat(run["started_at"]) + timedelta(seconds=30)
            return self._notification_budget(task), deadline.isoformat()
        return ExecutionPlan.model_validate_json(task["plan_json"]).budget, task["deadline"] if self._time_limited(task) else None

    def _queue_failure_notice(self, conn, task, result, state):
        if task["root_task_id"]:
            return state
        if task["failure_notice_json"] or state == "CANCELLED":
            return state
        context = json.loads(task["context_json"])
        # 过期且未调用工具的主动接话直接结束，避免生成多余的失败说明。
        if (context.get("invited") is False and not json.loads(task["budget_json"]).get("tool_calls", 0)
                and self.context_validator and not self.context_validator(self._access_for(task), context)):
            return state
        context.update(control_response=True, allowed_actions=[], failure_state=state)
        conn.execute(update(tasks).where(tasks.c.id == task["id"]).values(
            state="QUEUED", failure_notice_json=result.model_dump_json(), context_json=json.dumps(context)))
        self.wakeup.set()
        return "QUEUED"

    @staticmethod
    def _require_stopped_process(conn, run):
        if run["process_id"]:
            from qqbot.storage.task_schema import processes
            process = conn.execute(select(processes).where(processes.c.id == run["process_id"])).mappings().one()
            if process["state"] != "STOPPED" or process["epoch"] != run["process_epoch"] or process["generation"] != run["binding_generation"]:
                raise PermissionError("active process stop has not been verified")

    def _record_host_terminal(self, conn, task, state, result, event_id, *, run_id=None):
        attention = state in {"BLOCKED", "FAILED", "EXPIRED"}
        stored_state = "WAITING_CHILDREN" if task["kind"] == "goal" and attention else state
        conn.execute(update(tasks).where(tasks.c.id == task["id"]).values(state=stored_state, result_json=result.model_dump_json()))
        if task["root_task_id"] and hasattr(self, "graph"):
            settled = {**dict(task), "state": stored_state}
            self.graph.event(conn, settled, "needs_attention" if attention else "child_result",
                {"result": result.model_dump(mode="json")},
                attempt_id=run_id, event_id=event_id, wake=True)
            if task["parent_task_id"]:
                parent = conn.execute(select(tasks).where(tasks.c.id == task["parent_task_id"])).mappings().one()
                if parent["kind"] != "goal" and not attention:
                    self.graph.wake_parent(conn, parent)
        self.wakeup.set()

    def recover(self):
        now = self.clock.now().isoformat()
        with self.db.transaction() as c:
            expired = select(runs.c.task_id).where(runs.c.slot_reserved == 1, runs.c.lease <= now)
            c.execute(
                update(tasks)
                .where(tasks.c.id.in_(expired), tasks.c.state.not_in(TERMINAL))
                .values(state="NEEDS_RECONCILIATION")
            )
            if hasattr(self, "graph"):
                interrupted = c.execute(select(tasks, runs.c.id.label("interrupted_run_id")).join(
                    runs, runs.c.task_id == tasks.c.id).where(runs.c.slot_reserved == 1,
                    runs.c.role == "worker", runs.c.lease <= now, tasks.c.root_task_id.is_not(None))).mappings().all()
                for task in interrupted:
                    self.graph.event(c, task, "needs_attention", {"reason": "worker_lease_lost"},
                        attempt_id=task["interrupted_run_id"], event_id="lease-lost:" + task["interrupted_run_id"], wake=True)
            from qqbot.storage.task_schema import processes
            c.execute(update(processes).where(processes.c.id.in_(select(runs.c.process_id).where(
                runs.c.slot_reserved == 1, runs.c.lease <= now)), processes.c.state != "STOPPED").values(state="UNKNOWN"))
            expired_waits = c.execute(select(waits, tasks.c.deadline, tasks.c.root_task_id, tasks.c.delivery).join(tasks, tasks.c.id == waits.c.task_id)
                .where(waits.c.state.in_(["INTENT", "WAITING"]), tasks.c.state.in_(["WAITING_INPUT", "WAITING_EXTERNAL"]))) .mappings().all()
            for waiting in expired_waits:
                deadline = waiting["expires_at"] or waiting["delivery_deadline"]
                if self._time_limited(waiting):
                    deadline = min(deadline, waiting["deadline"]) if deadline else waiting["deadline"]
                if deadline and deadline <= now:
                    reason = "input_timeout" if waiting["delivered_at"] else "delivery_timeout"
                    if waiting["kind"] == "external":
                        reason = "external_timeout"
                    c.execute(update(waits).where(waits.c.id == waiting["id"]).values(state="EXPIRED"))
                    task = c.execute(select(tasks).where(tasks.c.id == waiting["task_id"])).mappings().one()
                    previous = TaskResult.model_validate_json(task["result_json"]) if task["result_json"] else TaskResult(
                        status="blocked", agent_authored=False, summary="The wait ended.")
                    result = previous.model_copy(update={"status": "blocked", "agent_authored": False,
                        "summary": "The wait ended before a qualifying input or external observation arrived."})
                    c.execute(update(tasks).where(tasks.c.id == task["id"]).values(stop_reason=reason))
                    self._record_host_terminal(c, task, "BLOCKED", result, "host-wait-expired:" + waiting["id"],
                        run_id=waiting["run_id"])
            for task in c.execute(select(tasks).where(tasks.c.state == "QUEUED", tasks.c.deadline <= now,
                                                      tasks.c.failure_notice_json.is_(None), tasks.c.root_task_id.is_(None), tasks.c.delivery != "DETACHED")).mappings().all():
                result = TaskResult(status="failed", agent_authored=False,
                                    summary="The execution deadline expired before the queued request started.")
                c.execute(update(tasks).where(tasks.c.id == task["id"]).values(stop_reason="deadline"))
                self._record_host_terminal(c, task, "EXPIRED", result, "host-queued-expired:" + task["id"])
                self._queue_failure_notice(c, task, result, "EXPIRED")

    def claim(self, worker_id, protocol_version=2):
        self.recover()
        with self.db.transaction() as c:
            history = tasks.alias("history")
            last_run = (
                select(func.max(runs.c.lease))
                .where(
                    runs.c.task_id.in_(
                        select(history.c.id).where(history.c.conversation == tasks.c.conversation).correlate(tasks)
                    )
                )
                .scalar_subquery()
            )
            candidates = (
                c.execute(
                    select(tasks)
                    .where(tasks.c.state == "QUEUED", tasks.c.kind != "goal", tasks.c.protocol_version <= protocol_version)
                    .order_by(last_run.asc().nulls_first(), tasks.c.created_at, tasks.c.id)
                )
                .mappings()
                .all()
            )
            occupied = {json.loads(value).get("workspace_ref") for value in c.execute(
                select(tasks.c.plan_json).join(runs, runs.c.task_id == tasks.c.id).where(runs.c.slot_reserved == 1)
            ).scalars()}
            busy_sessions = set(c.execute(select(runs.c.session_id).where(runs.c.slot_reserved == 1)).scalars())
            row = None
            for item in candidates:
                if getattr(self, "job_workspace_available", None) and not self.job_workspace_available(item):
                    continue
                workspace = json.loads(item["plan_json"]).get("workspace_ref")
                if workspace and workspace in occupied or item["session_id"] and item["session_id"] in busy_sessions:
                    continue
                if item["root_task_id"] and hasattr(self, "graph"):
                    root = self.graph.root(c, item)
                    if root["state"] in TERMINAL | {"CANCEL_REQUESTED"}:
                        continue
                row = item
                break
            if not row:
                return None
            self.permissions.require(self._access_for(row), "task.execute", "task:" + row["id"])
            parent = c.execute(select(runs.c.id).where(runs.c.task_id == row["id"])
                .order_by(runs.c.fencing.desc()).limit(1)).scalar_one_or_none()
            run_id = str(uuid4())
            fencing = row["fencing"] + 1
            started = self.clock.now()
            lease = (started + timedelta(seconds=30)).isoformat()
            plan = ExecutionPlan.model_validate_json(row["plan_json"])
            session_id = row["session_id"] or str(uuid4())
            budget = self._notification_budget(row) if row["failure_notice_json"] else plan.budget
            if row["failure_notice_json"]:
                plan = plan.model_copy(update={"budget": budget, "capability_hints": ()})
            deadline = (started + timedelta(seconds=30)).isoformat() if row["failure_notice_json"] else row["deadline"]
            c.execute(
                update(tasks)
                .where(tasks.c.id == row["id"], tasks.c.state == "QUEUED")
                .values(state="RUNNING", fencing=fencing, session_id=session_id)
            )
            c.execute(
                insert(runs).values(
                    id=run_id,
                    task_id=row["id"],
                    worker=worker_id,
                    fencing=fencing,
                    lease=lease,
                    slot_reserved=1,
                    session=f"{session_id}/sessions/{session_id}.jsonl",
                    session_id=session_id, role="worker", purpose="work",
                    basis_goal_revision=row["goal_revision"],
                    last_seq=0,
                    parent_run_id=parent,
                    run_reason="notification" if row["failure_notice_json"] else "continuation" if parent else "initial",
                    started_at=started.isoformat(), accounted_at=started.isoformat(),
                )
            )
            continuation = c.execute(select(inputs.c.id, inputs.c.text).where(
                inputs.c.task_id == row["id"], inputs.c.state == "READY")).mappings().all()
            checkpoint = c.execute(select(waits.c.checkpoint).where(waits.c.task_id == row["id"],
                waits.c.state == "CONSUMED").order_by(waits.c.revision.desc()).limit(1)).scalar_one_or_none()
            references = json.loads(row["goal_json"]).get("target_refs", [])
            repository = host = None
            for reference in references:
                if reference.startswith(("pr:", "issue:", "repo:")):
                    parts = reference.split(":", 1)[1].split("#", 1)[0].split("/")
                    if len(parts) == 3:
                        host, repository = parts[0], "/".join(parts[1:])
                        break
            public_answer_events = bool(not row["root_task_id"] and self.stream_eligibility and self.stream_eligibility(
                InboundEvent.model_validate_json(row["event_json"])))
            if public_answer_events:
                c.execute(insert(stream_runs).values(run_id=run_id, task_id=row["id"], state="ACTIVE", cursor_seq=0))
            return {
                "public_answer_events": public_answer_events,
                "repository": repository,
                "host": host,
                "task_id": row["id"],
                "run_id": run_id,
                "fencing_token": fencing,
                "goal": ("请依据以下冻结事实向用户解释任务未完成；禁止继续执行任何业务操作。"
                         "不得编造曾经查询资料或执行了未记录的动作。原目标若只是日常聊天，"
                         "最多用一句短话说明这次暂时没接上，不引入额外的查询任务：\n" + row["failure_notice_json"])
                    if row["failure_notice_json"] else row["goal"],
                "notification_status": "failed" if row["failure_notice_json"] else None,
                "goal_spec": json.loads(row["goal_json"]),
                "plan": plan.model_dump(mode="json"),
                "active_seconds_remaining": 30 if row["failure_notice_json"] else None if not self._time_limited(row) else max(0, budget.active_seconds
                    - json.loads(row["budget_json"]).get("active_seconds", 0)),
                "context": {**json.loads(row["context_json"]), "key": session_id, "session_id": session_id,
                    "role": "worker", "purpose": "work", "source_input_ids": ["task:" + row["id"], *[r["id"] for r in continuation]]},
                "binding": json.loads(row["binding_json"]), "role": "worker", "purpose": "work",
                "root_task_id": row["root_task_id"], "parent_task_id": row["parent_task_id"],
                "basis_goal_revision": row["goal_revision"],
                "delivery": row["delivery"],
                "continuation": {"checkpoint": checkpoint, "inputs": [dict(r) for r in continuation]},
                "protocol_version": row["protocol_version"],
                "profile": row["profile"],
                "lease_expires_at": lease,
                "deadline": deadline if self._time_limited(row) else None,
                "permission_snapshot": json.loads(row["permission_json"]),
                "session_id": session_id,
                "session_relative_path": f"{session_id}/sessions/{session_id}.jsonl",
            }

    def _lease(self, c, run_id, fencing_token, worker_id):
        run = c.execute(select(runs).where(runs.c.id == run_id)).mappings().first()
        if (
            not run
            or run["worker"] != worker_id
            or run["fencing"] != fencing_token
            or not run["slot_reserved"]
        ):
            raise PermissionError("stale run")
        task = c.execute(select(tasks).where(tasks.c.id == run["task_id"])).mappings().one()
        if task["fencing"] != fencing_token or run["lease"] <= self.clock.now().isoformat():
            raise PermissionError("expired lease")
        return run, task

    def renew(self, run_id, fencing_token, worker_id):
        with self.db.transaction() as c:
            run, task = self._lease(c, run_id, fencing_token, worker_id)
            used = self._account_active(c, run, task)
            state = task["state"]
            budget, deadline = self._run_limits(run, task)
            active_key = "notification_active_seconds" if run["run_reason"] == "notification" else "active_seconds"
            if deadline is not None and used.get(active_key, 0) >= budget.active_seconds and state == "RUNNING":
                state = "CANCEL_REQUESTED"
                values = {"state": state}
                if run["run_reason"] != "notification":
                    values["stop_reason"] = "active_budget"
                c.execute(update(tasks).where(tasks.c.id == task["id"]).values(**values))
            if deadline is not None and deadline <= self.clock.now().isoformat() and state == "RUNNING":
                state = "CANCEL_REQUESTED"
                c.execute(
                    update(tasks).where(tasks.c.id == task["id"]).values(state=state, stop_reason="deadline")
                )
            lease = (self.clock.now() + timedelta(seconds=30)).isoformat()
            c.execute(update(runs).where(runs.c.id == run["id"]).values(lease=lease))
            return {"state": state, "lease_expires_at": lease, "delivery": task["delivery"]}

    def submit(self, run_id, fencing_token, worker_id, result, stopped=True, quiescent=False, process=None, receipt=None):
        result = TaskResult.model_validate_json(json.dumps(result))
        if not stopped and not quiescent:
            raise ValueError("runner must be stopped or proven quiescent")
        self._validate_result(result)
        now = self.clock.now().isoformat()
        with self.db.transaction() as c:
            previous = c.execute(select(runs).where(runs.c.id == run_id, runs.c.worker == worker_id,
                runs.c.fencing == fencing_token, runs.c.slot_reserved == 0)).mappings().first()
            if previous:
                if previous["result_json"] == result.model_dump_json():
                    task = c.execute(select(tasks).where(tasks.c.id == previous["task_id"])).mappings().one()
                    return {"state": task["state"]}
                raise PermissionError("result conflicts with committed run")
            run, task = self._lease(c, run_id, fencing_token, worker_id)
            if task["root_task_id"] and result.agent_authored:
                proof = json.loads(run["receipt_json"]) if run["receipt_json"] else receipt
                if not proof or proof.get("kind") not in {"turn_finished", "run_yielded"}:
                    raise PermissionError("task needs a durable Harness boundary receipt")
                self._session_receipt(c, run, task, proof)
                root = self.graph.root(c, task)
                current_run = c.execute(select(runs).where(runs.c.id == run_id)).mappings().one()
                revision_pending = current_run["basis_goal_revision"] != root["goal_revision"] or bool(c.execute(
                    select(inputs.c.id).where(inputs.c.task_id == task["id"], inputs.c.kind != "notify",
                        inputs.c.consumed_at.is_(None))).first())
            else:
                revision_pending = False
            notification_run = run["run_reason"] == "notification"
            _budget, deadline = self._run_limits(run, task)
            failure_after_stop = result.status == "failed" and stopped and task["state"] in {"RUNNING", "CANCEL_REQUESTED"}
            if (task["state"] != "RUNNING" or deadline is not None and deadline <= now) and not failure_after_stop:
                raise PermissionError("task no longer running")
            if stopped:
                self._require_stopped_process(c, run)
            self.permissions.require(self._access_for(task), "task.execute", "task:" + task["id"])
            self._account_active(c, run, task)
            goal = TaskGoal.model_validate_json(task["goal_json"]) if task["goal_json"] != "{}" else TaskGoal(text=task["goal"])
            if result.observation_refs:
                self._verify_checks(c, ("observation",), result, task["id"])
            if result.effect_refs:
                from qqbot.storage.action_schema import operations
                operation_task_id = self._operation_task_id(c, task)
                effects = c.execute(select(operations.c.state).where(operations.c.task_id == operation_task_id,
                    operations.c.id.in_(result.effect_refs))).scalars().all()
                if len(effects) != len(set(result.effect_refs)):
                    raise ValueError("result references unknown operations")
                if result.status == "completed" and any(state != "VERIFIED" for state in effects):
                    raise ValueError("completion contains unverified operations")
            if result.status == "completed":
                missing = set(goal.requested_outputs) - {a.relative_path for a in result.artifacts}
                if missing:
                    raise ValueError("requested outputs missing: " + ", ".join(sorted(missing)))
                self._verify_checks(c, goal.success_checks, result, task["id"])
            wait = c.execute(select(waits).where(waits.c.run_id == run_id, waits.c.state == "INTENT")).mappings().first()
            child_wait = bool(task["wait_json"] and json.loads(task["wait_json"]).get("attempt_id") == run_id)
            if result.status.startswith("waiting_") and not wait and not child_wait:
                raise ValueError("waiting result requires a durable wait intent")
            if not stopped and (not process or process.get("id") != run["process_id"] or process.get("epoch") != run["process_epoch"] or process.get("generation") != run["binding_generation"]):
                raise PermissionError("quiescence proof does not match the active process binding")
            if task["protocol_version"] == 1 and result.status == "completed" and (not result.artifacts or not result.evidence):
                raise ValueError("legacy research completion requires exported artifacts and evidence")
            state = "WAITING_CHILDREN" if child_wait else ("WAITING_INPUT" if wait["kind"] == "input" else "WAITING_EXTERNAL") if wait else {
                "completed": "SUCCEEDED", "partial": "PARTIAL", "blocked": "BLOCKED", "failed": "FAILED"
            }[result.status]
            if task["stop_reason"] in {"user_cancel", "revoked"}:
                state = ("CANCELLED" if not getattr(self, "job_tree_quiescent", None)
                         or self.job_tree_quiescent(task) else "CANCEL_REQUESTED")
            elif notification_run:
                state = json.loads(task["context_json"]).get("failure_state", "FAILED")
            elif self._time_limited(task) and (task["stop_reason"] == "deadline" or task["deadline"] <= now):
                state = "EXPIRED"
            if revision_pending and state not in {"CANCELLED", "CANCEL_REQUESTED", "EXPIRED"}:
                state = "QUEUED"
                c.execute(update(inputs).where(inputs.c.task_id == task["id"], inputs.c.consumed_at.is_(None)).values(state="READY"))
            c.execute(update(tasks).where(tasks.c.id == task["id"]).values(state=state, result_json=result.model_dump_json()))
            values = {"slot_reserved": 0, "quiescent_at": now, "result_json": result.model_dump_json()}
            if stopped:
                values["runner_stopped_at"] = now
            if process:
                values.update(process_id=process["id"], process_epoch=process["epoch"], binding_generation=process["generation"])
            values["state"] = "YIELDED" if wait or child_wait else "FINISHED"
            c.execute(update(runs).where(runs.c.id == run_id).values(**values))
            if run["process_id"]:
                from qqbot.storage.task_schema import processes, sessions
                c.execute(update(processes).where(processes.c.id == run["process_id"],
                    processes.c.generation == run["binding_generation"]).values(
                        state="STOPPED" if stopped else "IDLE", last_used=now, stopped_at=now if stopped else None))
                c.execute(update(sessions).where(sessions.c.active_run_id == run_id).values(active_run_id=None))
                if process and process.get("actual_session_id"):
                    identity = json.loads(c.execute(select(processes.c.identity_json).where(processes.c.id == run["process_id"])).scalar_one())
                    identity.update(actual_session_id=process["actual_session_id"], actual_session_file=process.get("actual_session_file"))
                    c.execute(update(processes).where(processes.c.id == run["process_id"]).values(identity_json=json.dumps(identity)))
            if child_wait:
                condition = json.loads(task["wait_json"])
                condition["yielded"] = True
                c.execute(update(tasks).where(tasks.c.id == task["id"]).values(wait_json=json.dumps(condition)))
                self.graph.wake_parent(c, {**dict(task), "state": "WAITING_CHILDREN", "wait_json": json.dumps(condition)})
            elif wait:
                c.execute(update(waits).where(waits.c.id == wait["id"]).values(
                    state="SUPERSEDED" if state == "QUEUED" else "WAITING"))
                if state != "QUEUED":
                    self._wake(c, task["id"], wait["id"])
                state = c.execute(select(tasks.c.state).where(tasks.c.id == task["id"])).scalar_one()
            elif task["root_task_id"] and state != "QUEUED":
                settled_task = {**dict(task), "state": state}
                self.graph.settled(c, settled_task, run_id, result.model_dump(mode="json"))
            elif not result.agent_authored:
                state = self._queue_failure_notice(c, task, result, state)
            elif state == "CANCELLED":
                pass
            elif notification_run:
                self._notification(c, task, run_id, result.summary, "result", "important", "result:" + run_id)
            elif c.execute(select(inputs.c.id).where(inputs.c.task_id == task["id"], inputs.c.state == "PENDING")).first():
                c.execute(update(inputs).where(inputs.c.task_id == task["id"], inputs.c.state == "PENDING").values(state="READY"))
                c.execute(update(tasks).where(tasks.c.id == task["id"]).values(state="QUEUED"))
                state = "QUEUED"
            else:
                self._notification(c, task, run_id, result.summary, "result", "important", "result:" + run_id)
            self.wakeup.set()
            return {"state": state}

    def _verify_checks(self, c, checks, result, task_id=None):
        # A named check must have a registered verifier; unknown checks never silently succeed.
        for check in checks:
            if check == "artifact" and result.artifacts:
                continue
            if check == "observation" and result.observation_refs:
                from qqbot.storage.task_schema import observations
                found = c.execute(select(func.count()).select_from(observations).where(
                    observations.c.task_id == task_id, observations.c.id.in_(result.observation_refs))).scalar_one()
                if found == len(set(result.observation_refs)):
                    continue
            if check == "effect" and result.effect_refs:
                from qqbot.storage.action_schema import operations
                owner = c.execute(select(tasks).where(tasks.c.id == task_id)).mappings().one()
                rows = c.execute(select(operations).where(operations.c.task_id == self._operation_task_id(c, owner), operations.c.id.in_(result.effect_refs))).mappings().all()
                if len(rows) == len(result.effect_refs) and all(r["state"] == "VERIFIED" for r in rows):
                    continue
            raise ValueError("success check not verified: " + check)

    def append_event(self, run_id, fencing_token, worker_id, event_id, seq, payload):
        validate_public_event(payload)
        encoded = json.dumps(payload, ensure_ascii=False)
        if not isinstance(seq, int) or seq < 1 or len(encoded.encode()) > 2 * 1024 * 1024:
            raise ValueError("invalid event sequence or payload")
        with self.db.transaction() as c:
            run, _task = self._lease(c, run_id, fencing_token, worker_id)
            if payload.get("kind") in PUBLIC_KINDS and not c.execute(select(stream_runs.c.run_id).where(
                stream_runs.c.run_id == run_id)).first():
                raise PermissionError("public answer stream was not enabled for this run")
            old = c.execute(select(events).where(events.c.event_id == event_id)).mappings().first()
            if old:
                if old["run_id"] != run_id or old["seq"] != seq or old["payload"] != encoded:
                    raise ValueError("conflicting event replay")
                return {"acknowledged": seq}
            if seq != run["last_seq"] + 1:
                raise ValueError("event sequence gap")
            c.execute(insert(events).values(event_id=event_id, run_id=run_id, seq=seq, payload=encoded))
            c.execute(update(runs).where(runs.c.id == run_id).values(last_seq=seq))
            return {"acknowledged": seq}

    def _validate_result(self, result):
        total = 0
        if len(result.artifacts) > 20 or len(result.evidence) > 60:
            raise ValueError("result exceeds manifest budget")
        for artifact in result.artifacts:
            artifact_id = artifact.id
            try:
                if str(UUID(artifact_id)) != artifact_id:
                    raise ValueError("invalid archive reference")
            except ValueError:
                raise ValueError("invalid archive reference") from None
            root = os.open(self.archive_root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                fd = os.open(artifact_id, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=root)
                try:
                    info = os.fstat(fd)
                    total += info.st_size
                    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or total > 20 * 1024 * 1024:
                        raise ValueError("unsafe archive file")
                    digest = hashlib.sha256()
                    read_bytes = 0
                    while chunk := os.read(fd, 65536):
                        read_bytes += len(chunk)
                        if read_bytes > info.st_size:
                            raise ValueError("archive changed during verification")
                        digest.update(chunk)
                    if info.st_size != artifact.size or digest.hexdigest() != artifact.sha256:
                        raise ValueError("archive digest mismatch")
                finally:
                    os.close(fd)
            finally:
                os.close(root)
        for evidence in result.evidence:
            from urllib.parse import urlsplit

            if (
                urlsplit(evidence.url).scheme != "https"
                or not evidence.retrieved_at
                or len(evidence.sha256) != 64
            ):
                raise ValueError("invalid retrieval evidence")

    def reconcile(self, public_id, access, run_id, result=None):
        self.permissions.require(access, "task.reconcile", "task:" + public_id)
        with self.db.engine.connect() as c:
            task = self._visible(c, public_id, access)
            task_id = task["id"]
            original = c.execute(select(runs).where(runs.c.id == run_id, runs.c.task_id == task_id)).mappings().first()
            proof_task, proof_process = task_id, run_id
            if original and original["process_id"]:
                from qqbot.storage.task_schema import processes
                process_row = c.execute(select(processes).where(processes.c.id == original["process_id"])).mappings().one()
                proof_process = original["process_id"]
                proof_task = json.loads(process_row["identity_json"]).get("launch_task_id", task_id)
                if task["protocol_version"] == 2 and process_row["state"] != "STOPPED":
                    raise ValueError("gateway requests and host tools require verified worker stop before reconciliation")
        if not self.stop_verifier(proof_task, proof_process):
            raise ValueError("root-owned runner stop proof is required")
        validated = TaskResult.model_validate_json(json.dumps(result)) if result is not None else None
        if validated:
            self._validate_result(validated)
        with self.db.transaction() as c:
            task = self._visible(c, public_id, access)
            run = (
                c.execute(select(runs).where(runs.c.id == run_id, runs.c.task_id == task_id))
                .mappings()
                .first()
            )
            if not run or run["fencing"] != task["fencing"] or not run["slot_reserved"]:
                raise PermissionError("recovery source is not the latest reserved run")
            if task["state"] not in {"NEEDS_RECONCILIATION", "RECOVERING"}:
                raise PermissionError("task is not recoverable")
            if validated and task["stop_reason"]:
                raise PermissionError("cancelled task cannot accept a spool result")
            if validated and validated.status == "completed":
                goal = TaskGoal.model_validate_json(task["goal_json"]) if task["goal_json"] != "{}" else TaskGoal(text=task["goal"])
                if set(goal.requested_outputs) - {item.relative_path for item in validated.artifacts}:
                    raise ValueError("requested outputs missing during reconciliation")
                self._verify_checks(c, goal.success_checks, validated, task_id)
            if validated and self._deadline_expired(task):
                raise PermissionError("expired task cannot accept success during recovery")
            recovery_id, fencing = str(uuid4()), task["fencing"] + 1
            now = self.clock.now().isoformat()
            from qqbot.storage.action_schema import operations
            c.execute(update(operations).where(operations.c.run_id == run_id, operations.c.state == "DISPATCHED")
                .values(state="UNKNOWN", updated_at=now))
            c.execute(update(operations).where(operations.c.run_id == run_id, operations.c.state == "PREPARED")
                .values(state="FAILED", result_json=json.dumps({"reason": "stopped_before_dispatch"}), updated_at=now))
            c.execute(update(runs).where(runs.c.id == run_id).values(slot_reserved=0, runner_stopped_at=now))
            if run["process_id"]:
                from qqbot.storage.task_schema import processes, sessions
                c.execute(update(processes).where(processes.c.id == run["process_id"]).values(state="STOPPED", stopped_at=now))
                c.execute(update(sessions).where(sessions.c.active_run_id == run_id).values(active_run_id=None))
            c.execute(
                insert(runs).values(
                    id=recovery_id,
                    task_id=task_id,
                    worker="recovery:" + access.principal.key,
                    fencing=fencing,
                    lease=now,
                    slot_reserved=0,
                    runner_stopped_at=now,
                    session=run["session"],
                    last_seq=0,
                )
            )
            if validated:
                state = {"completed": "SUCCEEDED", "partial": "PARTIAL", "blocked": "BLOCKED"}.get(validated.status, "FAILED")
            elif self._deadline_expired(task):
                state = "EXPIRED"
            else:
                state = {"deadline": "EXPIRED", "user_cancel": "CANCELLED", "revoked": "CANCELLED"}.get(
                    task["stop_reason"], "FAILED"
                )
            if state == "CANCELLED" and getattr(self, "job_tree_quiescent", None) and not self.job_tree_quiescent(task):
                state = "CANCEL_REQUESTED"
            c.execute(
                update(tasks)
                .where(tasks.c.id == task_id)
                .values(
                    state=state,
                    fencing=fencing,
                    result_json=validated.model_dump_json() if validated else None,
                )
            )
            if task["root_task_id"] and hasattr(self, "graph"):
                settled = {**dict(task), "state": state}
                self.graph.settled(c, settled, recovery_id,
                    (validated or TaskResult(status="failed", agent_authored=False,
                        summary="The interrupted attempt requires review before work can continue.")).model_dump(mode="json"))
            elif validated:
                self._notification(c, task, recovery_id, validated.summary, "result", "important", "result:" + run_id)
            return {"state": state, "recovery_run_id": recovery_id, "fencing_token": fencing}

    def recovery_status(self, run_id, fencing_token, worker_id):
        self.recover()
        with self.db.engine.connect() as c:
            run = (
                c.execute(
                    select(runs).where(
                        runs.c.id == run_id, runs.c.worker == worker_id, runs.c.fencing == fencing_token
                    )
                )
                .mappings()
                .first()
            )
            if not run:
                raise PermissionError("unknown recovery source")
            task = c.execute(select(tasks).where(tasks.c.id == run["task_id"])).mappings().one()
        reconciled = task["state"] in TERMINAL and not run["slot_reserved"]
        return {"state": task["state"], "discard_spool": reconciled}

    def uncertain(self, run_id, fencing_token, worker_id):
        with self.db.transaction() as c:
            _run, task = self._lease(c, run_id, fencing_token, worker_id)
            c.execute(update(tasks).where(tasks.c.id == task["id"]).values(state="NEEDS_RECONCILIATION"))
            return {"state": "NEEDS_RECONCILIATION"}

    def stopped(self, run_id, fencing_token, worker_id):
        with self.db.transaction() as c:
            _run, task = self._lease(c, run_id, fencing_token, worker_id)
            self._require_stopped_process(c, _run)
            reason = task["stop_reason"]
            state = "EXPIRED" if reason == "deadline" else "CANCELLED" if reason in {"user_cancel", "revoked"} else "FAILED"
            if state == "CANCELLED" and getattr(self, "job_tree_quiescent", None) and not self.job_tree_quiescent(task):
                state = "CANCEL_REQUESTED"
            if _run["run_reason"] == "notification" and state not in {"CANCELLED", "CANCEL_REQUESTED"}:
                state = json.loads(task["context_json"]).get("failure_state", "FAILED")
            result = TaskResult(status="failed", agent_authored=False,
                                summary="Execution was stopped after " + (reason or "run authority changed") + ".")
            c.execute(
                update(runs)
                .where(runs.c.id == run_id)
                .values(slot_reserved=0, runner_stopped_at=self.clock.now().isoformat(), result_json=result.model_dump_json())
            )
            self._record_host_terminal(c, task, state, result, "stopped:" + run_id, run_id=run_id)
            state = self._queue_failure_notice(c, task, result, state)
            return {"state": state}

    def _notification(self, c, task, run_id, text, purpose, importance, notification_id):
        if not isinstance(text, str) or not text.strip() or len(text) > 10000:
            raise ValueError("notification text must contain 1..10000 characters")
        validate_user_facing_text(text)
        self.permissions.require(self._access_for(task), "notification.send", "conversation:" + task["conversation"])
        old = c.execute(select(notifications).where(notifications.c.id == notification_id)).mappings().first()
        if old:
            if old["task_id"] != task["id"] or old["body"] != text or old["purpose"] != purpose:
                raise ValueError("notification identity conflict")
            return {"accepted": True, "notification_id": notification_id}
        if purpose == "progress":
            duplicate = c.execute(select(notifications.c.id).join(runs, runs.c.id == notifications.c.run_id)
                .join(outbox_table, outbox_table.c.origin_id == notifications.c.id).where(
                    notifications.c.task_id == task["id"], notifications.c.purpose == purpose,
                    notifications.c.body == text, runs.c.basis_goal_revision == task["goal_revision"],
                    outbox_table.c.state.in_(["PENDING", "RETRYABLE", "SENDING", "SENT", "UNKNOWN",
                                              "WAITING_FOR_TRIGGER", "STREAM_PENDING"])).limit(1)).scalar_one_or_none()
            if duplicate:
                return {"accepted": True, "notification_id": duplicate}
        seq = (c.execute(select(func.max(notifications.c.sequence)).where(
            notifications.c.task_id == task["id"])).scalar_one() or 0) + 1
        stale = select(notifications.c.id).where(notifications.c.task_id == task["id"],
            notifications.c.purpose == "progress", notifications.c.importance == "normal")
        if purpose in {"progress", "result"}:
            c.execute(update(outbox_table).where(outbox_table.c.origin_id.in_(stale),
                outbox_table.c.state.in_(["PENDING", "RETRYABLE", "WAITING_FOR_TRIGGER"]))
                .values(state="SUPERSEDED"))
        now = self.clock.now()
        c.execute(insert(notifications).values(id=notification_id, task_id=task["id"], run_id=run_id,
            sequence=seq, purpose=purpose, importance=importance, body=text, created_at=now.isoformat(), state="ACCEPTED"))
        event = InboundEvent.model_validate_json(task["event_json"])
        deliveries = self.outbox.enqueue_text(c, target=event.conversation, text=text,
            origin_kind="task", origin_id=notification_id, reply_context=event.reply_context,
            ttl_seconds=86400, requester=event.principal, anchor_message_id=event.platform_message_id,
            parent_task_id=task["id"], notification_sequence=seq, purpose=purpose)
        if purpose == "result" and run_id and c.execute(select(stream_runs.c.run_id).where(
            stream_runs.c.run_id == run_id)).first():
            c.execute(update(outbox_table).where(outbox_table.c.id.in_(deliveries)).values(state="STREAM_PENDING"))
        if purpose == "progress" and importance == "normal":
            last_sent = c.execute(select(func.max(outbox_table.c.sent_at)).where(
                outbox_table.c.origin_id.in_(select(notifications.c.id).where(notifications.c.task_id == task["id"])))) .scalar_one()
            if last_sent:
                from datetime import datetime
                next_at = (datetime.fromisoformat(last_sent) + timedelta(seconds=15)).isoformat()
                c.execute(update(outbox_table).where(outbox_table.c.id.in_(deliveries)).values(next_attempt_at=next_at))
        return {"accepted": True, "notification_id": notification_id}

    def notify(self, run_id, fencing_token, worker_id, payload):
        with self.db.transaction() as c:
            _run, task = self._lease(c, run_id, fencing_token, worker_id)
            if task["state"] != "RUNNING":
                raise PermissionError("task no longer running")
            if task["root_task_id"] and hasattr(self, "graph"):
                if not isinstance(payload["text"], str) or not payload["text"].strip() or len(payload["text"]) > 10000:
                    raise ValueError("notification text must contain 1..10000 characters")
                validate_user_facing_text(payload["text"])
                self.permissions.require(self._access_for(task), "notification.send", "conversation:" + task["conversation"])
                self.graph.event(c, task, "worker_update", {"public_text": payload["text"]},
                    attempt_id=run_id, event_id=payload["notification_id"], wake=True)
                self.wakeup.set()
                return {"accepted": True, "notification_id": payload["notification_id"]}
            return self._notification(c, task, run_id, payload["text"], "progress",
                                      payload.get("importance", "normal"), payload["notification_id"])

    def ask(self, run_id, fencing_token, worker_id, payload):
        with self.db.transaction() as c:
            _run, task = self._lease(c, run_id, fencing_token, worker_id)
            if task["state"] != "RUNNING":
                raise PermissionError("task no longer running")
            question_id = payload["question_id"]
            old = c.execute(select(waits).where(waits.c.id == question_id)).mappings().first()
            if old:
                if old["run_id"] != run_id or old["checkpoint"] != payload["checkpoint"]:
                    raise ValueError("conflicting question replay")
                return {"accepted": True, "question_id": question_id, "waiting": True}
            if c.execute(select(waits.c.id).where(waits.c.run_id == run_id)).first():
                raise ValueError("a run may create only one wait intent")
            kind = payload.get("kind", "input")
            if kind not in {"input", "external"}:
                raise ValueError("unsupported wait kind")
            checkpoint = payload["checkpoint"]
            if not isinstance(checkpoint, str) or not 1 <= len(checkpoint) <= 32000:
                raise ValueError("bounded checkpoint required")
            condition = payload.get("condition", {})
            if kind == "external":
                condition = WaitCondition.model_validate_json(json.dumps(condition)).model_dump(mode="json")
            if not task["root_task_id"] or _run["role"] == "leader":
                self._notification(c, task, run_id, payload["text"], "question" if kind == "input" else "progress",
                                   "important", question_id)
            elif kind == "input":
                self.graph.event(c, task, "needs_input", {"question_id": question_id,
                    "public_text": payload["text"]}, attempt_id=run_id, event_id="question:" + question_id, wake=True)
            now = self.clock.now()
            revision = task["continuation_revision"] + 1
            c.execute(insert(waits).values(id=question_id, task_id=task["id"], run_id=run_id,
                revision=revision, kind=kind, notification_id=question_id, checkpoint=checkpoint,
                condition_json=json.dumps(condition), state="INTENT", created_at=now.isoformat(),
                delivery_deadline=(now + timedelta(hours=24)).isoformat(), expires_at=None))
            c.execute(update(tasks).where(tasks.c.id == task["id"]).values(continuation_revision=revision))
            c.execute(update(inputs).where(inputs.c.task_id == task["id"], inputs.c.question_id.is_(None),
                inputs.c.state == "PENDING").values(question_id=question_id))
            return {"accepted": True, "question_id": question_id, "waiting": True}

    def add_input(self, public_id, event, access, text, question_id=None):
        if isinstance(text, str) and not text.strip() and any(s.type == "image" for s in event.segments):
            text = "[图片]"
        if not isinstance(text, str) or not 1 <= len(text.strip()) <= 16000:
            raise ValueError("input must contain 1..16000 characters")
        if access.principal != event.principal or access.conversation != event.conversation:
            raise PermissionError("input identity mismatch")
        with self.db.transaction() as c:
            task = self._visible(c, public_id, access)
            if task["requester"] != event.principal.key:
                raise PermissionError("task input requires the original authenticated subject")
            if task["root_task_id"]:
                return self.add_graph_input(c, task, event, access, text,
                                            kind="answer" if question_id else "followup", question_id=question_id)
            self.permissions.require(access, "task.input", "task:" + task["id"])
            old = c.execute(select(inputs).where(inputs.c.source_id == event.event_id)).mappings().first()
            if old:
                if old["task_id"] != task["id"] or old["text"] != text:
                    raise ValueError("conflicting input replay")
                return {"accepted": True, "input_id": old["id"]}
            if task["state"] in TERMINAL or task["state"] in {"CANCEL_REQUESTED", "NEEDS_RECONCILIATION"}:
                raise PermissionError("task cannot accept input")
            wait = c.execute(select(waits).where(waits.c.task_id == task["id"],
                waits.c.state.in_(["INTENT", "WAITING"]))).mappings().first()
            if question_id and (not wait or wait["id"] != question_id):
                raise ValueError("question is no longer waiting")
            input_id = str(uuid4())
            c.execute(insert(inputs).values(id=input_id, task_id=task["id"], question_id=wait["id"] if wait else None,
                source_id=event.event_id, text=text, state="PENDING", created_at=self.clock.now().isoformat()))
            from qqbot.storage.schema import inbox as inbox_table
            raw = c.execute(select(inbox_table.c.event_json).where(
                inbox_table.c.id == event.event_id,
                inbox_table.c.conversation_id == access.conversation.key,
                inbox_table.c.principal_id == access.principal.key,
            )).scalar_one_or_none()
            if raw:
                context = json.loads(task["context_json"])
                if context:
                    context.setdefault("source_manifest", []).append({"kind": "inbox", "id": event.event_id,
                        "digest": hashlib.sha256(raw.encode()).hexdigest()})
                    source = InboundEvent.model_validate_json(raw)
                    images = [{"source": source.event_id, "message_id": source.platform_message_id,
                               "url": segment.image_url} for segment in source.segments if segment.type == "image"]
                    if images:
                        context.setdefault("images", []).extend(images)
                    c.execute(update(tasks).where(tasks.c.id == task["id"]).values(context_json=json.dumps(context, ensure_ascii=False)))
            if wait:
                self._wake(c, task["id"], wait["id"])
            return {"accepted": True, "input_id": input_id}

    def question_for(self, event):
        with self.db.engine.connect() as c:
            query = select(waits.c.id, tasks.c.public_id).join(tasks, tasks.c.id == waits.c.task_id).where(
                tasks.c.conversation == event.conversation.key, tasks.c.requester == event.principal.key,
                waits.c.kind == "input",
                waits.c.state.in_(["INTENT", "WAITING"]), tasks.c.state.in_(["RUNNING", "WAITING_INPUT"]))
            if event.reply_to:
                query = query.where(waits.c.notification_id.in_(select(outbox_table.c.origin_id).where(
                    outbox_table.c.conversation_id == event.conversation.key,
                    outbox_table.c.platform_message_id == event.reply_to)))
            rows = c.execute(query).mappings().all()
            return dict(rows[0]) if len(rows) == 1 else None

    def _wake(self, c, task_id, question_id):
        task = c.execute(select(tasks).where(tasks.c.id == task_id)).mappings().one()
        wait = c.execute(select(waits).where(waits.c.id == question_id)).mappings().one()
        if task["state"] not in {"WAITING_INPUT", "WAITING_EXTERNAL"} or wait["state"] != "WAITING":
            return False
        if self._deadline_expired(task):
            return False
        if not c.execute(select(inputs.c.id).where(inputs.c.question_id == question_id,
                                                  inputs.c.state == "PENDING")).first():
            return False
        c.execute(update(inputs).where(inputs.c.question_id == question_id, inputs.c.state == "PENDING").values(state="READY"))
        c.execute(update(waits).where(waits.c.id == question_id).values(state="CONSUMED"))
        state = "WAITING_CHILDREN" if task["kind"] == "goal" else "QUEUED"
        c.execute(update(tasks).where(tasks.c.id == task_id).values(state=state))
        if task["kind"] == "goal":
            answer_ids = list(c.execute(select(inputs.c.id).where(inputs.c.question_id == question_id)).scalars())
            self.graph.event(c, {**dict(task), "state": state}, "user_answer",
                {"question_id": question_id, "input_ids": answer_ids}, event_id="answer:" + question_id, wake=True)
        self.wakeup.set()
        return True

    def authorize(self, run_id, fencing_token, worker_id, payload):
        with self.db.engine.connect() as c:
            _run, task = self._lease(c, run_id, fencing_token, worker_id)
            _budget, deadline = self._run_limits(_run, task)
            if task["state"] != "RUNNING" or deadline is not None and deadline <= self.clock.now().isoformat():
                raise PermissionError("run is no longer authorized")
            action = payload.get("action", "task.execute")
            resources = payload.get("resources") or [payload.get("resource", "task:" + task["id"])]
            context = json.loads(task["context_json"])
            access = self._access_for(task)
            if hasattr(self, "graph"):
                self.graph.check_authority(c, task, payload)
            if self.context_validator and context and not self.context_validator(access, context):
                raise PermissionError("context sources or authorization changed")
            if context.get("control_response") and action not in {"model.request", "task.execute", "notification.send"}:
                raise PermissionError("status explanation cannot execute business tools")
            if (payload.get("effect") == "unknown" or action in {"github.unclassified", "github.graphql.read"}) and self.permissions.has_restrictions(access):
                raise PermissionError("unclassified action cannot prove compliance with configured restrictions")
            waiting = c.execute(select(waits.c.id).where(waits.c.run_id == run_id)).first()
            if waiting and action not in {"model.request", "task.execute", "notification.send"}:
                raise PermissionError("wait intent blocks further business tools")
            for resource in resources:
                decision = self.permissions.require(access, action, resource)
            return {"allowed": True, "policy_version": decision.policy_version, "waiting": bool(waiting)}

    def take_inputs(self, run_id, fencing_token, worker_id):
        with self.db.transaction() as c:
            _run, task = self._lease(c, run_id, fencing_token, worker_id)
            if c.execute(select(waits.c.id).where(waits.c.run_id == run_id)).first():
                return []
            rows = c.execute(select(inputs).where(inputs.c.task_id == task["id"],
                inputs.c.state == "PENDING", inputs.c.question_id.is_(None))).mappings().all()
            images = json.loads(task["context_json"]).get("images", ())
            result = []
            for row in rows:
                item = {"id": row["id"], "text": row["text"]}
                source_images = [image for image in images if image["source"] == row["source_id"]]
                if source_images:
                    item["images"] = source_images
                result.append(item)
            return result

    def reserve_budget(self, run_id, fencing_token, worker_id, payload):
        with self.db.transaction() as c:
            _run, task = self._lease(c, run_id, fencing_token, worker_id)
            if task["state"] != "RUNNING":
                raise PermissionError("task no longer running")
            kind = payload["kind"]
            if kind not in {"business", "completion", "compaction", "notification", "tool"}:
                raise ValueError("unknown budget category")
            notification_run = _run["run_reason"] == "notification"
            if notification_run:
                if kind == "tool":
                    raise PermissionError("failure explanation cannot execute business tools")
                kind = "compaction" if kind == "compaction" else "notification"
            request_id = payload["request_id"]
            old = c.execute(select(model_requests).where(model_requests.c.id == request_id)).mappings().first()
            if old:
                if old["run_id"] != run_id or old["kind"] != kind:
                    raise ValueError("budget identity conflict")
                return {"accepted": True}
            plan = ExecutionPlan.model_validate_json(task["plan_json"])
            used = json.loads(task["budget_json"])
            if notification_run:
                key = "notification_model_calls"
                category = "notification_compaction_calls" if kind == "compaction" else "notification_completion_calls"
                if used.get(key, 0) >= 2 or used.get(category, 0) >= 1:
                    raise ValueError("failure explanation model budget exhausted")
                used[category] = used.get(category, 0) + 1
                limit = 2
            else:
                key = "tool_calls" if kind == "tool" else "model_calls"
                limit = getattr(plan.budget, key)
                if kind in {"business", "compaction", "notification"}:
                    limit -= plan.budget.completion_reserve
                if used.get(key, 0) >= limit:
                    raise ValueError("task " + key + " budget exhausted")
            used[key] = used.get(key, 0) + 1
            if task["root_task_id"] and task["root_task_id"] != task["id"]:
                root = self.graph.root(c, task)
                root_used = json.loads(root["budget_json"])
                root_used[key] = root_used.get(key, 0) + 1
                c.execute(update(tasks).where(tasks.c.id == root["id"]).values(budget_json=json.dumps(root_used)))
            c.execute(update(tasks).where(tasks.c.id == task["id"]).values(budget_json=json.dumps(used)))
            c.execute(insert(model_requests).values(id=request_id, task_id=task["id"], run_id=run_id,
                kind=kind, state="RESERVED", created_at=self.clock.now().isoformat()))
            remaining = limit - used[key]
            return {"accepted": True, "remaining": remaining}

    def record_usage(self, run_id, fencing_token, worker_id, payload):
        with self.db.transaction() as c:
            self._lease(c, run_id, fencing_token, worker_id)
            changed = c.execute(update(model_requests).where(model_requests.c.id == payload["request_id"],
                model_requests.c.run_id == run_id).values(state="COMPLETED", usage_json=json.dumps(payload.get("usage"))))
            if not changed.rowcount:
                raise ValueError("usage request was not reserved")
            return {"recorded": True}

    def bind_process(self, run_id, fencing_token, worker_id, payload):
        from qqbot.storage.task_schema import processes, sessions
        with self.db.transaction() as c:
            _run, task = self._lease(c, run_id, fencing_token, worker_id)
            if task["state"] != "RUNNING":
                raise PermissionError("cannot bind inactive task")
            process_id, session_id = payload["process_id"], payload["session_id"]
            for identity in (process_id, session_id):
                if str(UUID(identity)) != identity:
                    raise ValueError("canonical process and session identities required")
            old = c.execute(select(processes).where(processes.c.id == process_id)).mappings().first()
            if old:
                if old["worker"] != worker_id or old["state"] != "IDLE" or payload["generation"] != old["generation"] + 1:
                    raise PermissionError("process is not exclusively idle")
                if old["session_id"] != session_id or old["epoch"] != payload["epoch"]:
                    raise PermissionError("process identity changed")
                c.execute(update(processes).where(processes.c.id == process_id).values(state="BUSY",
                    generation=payload["generation"], last_used=self.clock.now().isoformat()))
                changed = c.execute(update(sessions).where(sessions.c.id == session_id,
                    sessions.c.active_run_id.is_(None)).values(active_run_id=run_id))
                if changed.rowcount != 1:
                    raise PermissionError("session already active")
            else:
                resident = c.execute(select(func.count()).select_from(processes).where(
                    processes.c.state != "STOPPED")).scalar_one()
                if resident >= self.max_resident:
                    raise ValueError("resident process limit reached")
                c.execute(insert(processes).values(id=process_id, session_id=session_id, worker=worker_id,
                    epoch=payload["epoch"], generation=payload["generation"], state="BUSY",
                    identity_json=json.dumps(payload.get("identity", {})), last_used=self.clock.now().isoformat()))
                previous_session = c.execute(select(sessions).where(sessions.c.id == session_id)).mappings().first()
                if previous_session:
                    if previous_session["active_run_id"] not in {None, run_id} or previous_session["session_file"] != payload["session_file"]:
                        raise PermissionError("persisted session has another writer or file")
                    c.execute(update(sessions).where(sessions.c.id == session_id).values(active_run_id=run_id))
                else:
                    c.execute(insert(sessions).values(id=session_id, scope_key=payload["scope_key"],
                        session_file=payload["session_file"], manifest_json=task["context_json"], active_run_id=run_id, epoch=1))
            c.execute(update(runs).where(runs.c.id == run_id).values(process_id=process_id,
                process_epoch=payload["epoch"], binding_generation=payload["generation"], session=payload["session_file"]))
            return {"bound": True}

    def process_stopped(self, worker_id, payload):
        from qqbot.storage.task_schema import processes, sessions
        with self.db.transaction() as c:
            old = c.execute(select(processes).where(processes.c.id == payload["process_id"],
                processes.c.worker == worker_id, processes.c.epoch == payload["epoch"])).mappings().first()
            if not old:
                raise PermissionError("unknown process owner")
            if payload.get("generation") is not None and payload["generation"] != old["generation"]:
                raise PermissionError("stale process stop report")
            stopped = payload.get("stopped") is True
            c.execute(update(processes).where(processes.c.id == old["id"]).values(
                state="STOPPED" if stopped else "UNKNOWN", stopped_at=self.clock.now().isoformat() if stopped else None))
            if stopped:
                c.execute(update(sessions).where(sessions.c.id == old["session_id"]).values(active_run_id=None))
            return {"recorded": True}

    def _operation_task_id(self, c, task):
        while task["retry_of_task_id"] or task["rework_of_task_id"]:
            task = c.execute(select(tasks).where(tasks.c.id == (
                task["retry_of_task_id"] or task["rework_of_task_id"]))).mappings().one()
        return task["id"]

    async def ledger(self, run_id, fencing_token, worker_id, payload):
        from qqbot.capabilities.ledger import SQLiteActionLedger
        from qqbot.capabilities.types import Action, ToolContext
        from qqbot.storage.action_schema import operations
        with self.db.engine.connect() as c:
            _run, task = self._lease(c, run_id, fencing_token, worker_id)
            decision = _run["role"] == "leader" and task["kind"] == "goal"
            readback = payload["operation"] in {"read", "list"} or (
                payload["operation"] == "transition" and payload.get("expected") == "UNKNOWN")
            if task["state"] not in {"RUNNING", "CANCEL_REQUESTED"} and not (decision and readback):
                raise PermissionError("run cannot record an operation")
            operation_task_id = self._operation_task_id(c, task)
            scope_task_ids = list(c.execute(select(tasks.c.id).where(
                tasks.c.root_task_id == task["id"])).scalars()) if decision else [operation_task_id]
            context = ToolContext(task_id=operation_task_id, run_id=run_id, fence=fencing_token,
                principal=self._access_for(task), workspace="")
            if payload["operation"] in {"read", "list"}:
                query = select(operations).where(operations.c.task_id.in_(scope_task_ids))
                if payload["operation"] == "read":
                    query = query.where(operations.c.id == payload["operation_id"])
                rows = c.execute(query.order_by(operations.c.updated_at).limit(100)).mappings().all()
                return [{**dict(row), "arguments": json.loads(row["arguments_json"]),
                    "action": json.loads(row["action_json"]), "result": json.loads(row["result_json"])} for row in rows]
            if payload["operation"] == "transition":
                old = c.execute(select(operations).where(operations.c.id == payload["operation_id"])).mappings().first()
                if not old or old["task_id"] not in scope_task_ids or (
                    (old["run_id"] != run_id or old["fence"] != fencing_token) and payload["expected"] != "UNKNOWN"
                ):
                    raise PermissionError("operation belongs to another binding")
        ledger = SQLiteActionLedger(self.db)
        if payload["operation"] == "prepare":
            action = Action(**{**payload["action"], "resources": tuple(payload["action"]["resources"])})
            self.authorize(run_id, fencing_token, worker_id, {"action": action.name,
                "resources": action.resources, "effect": action.effect})
            return await ledger.prepare(payload["operation_id"], context, action,
                payload["arguments_hash"], payload["arguments"], scope_task_ids=scope_task_ids)
        if payload["operation"] == "transition":
            return {"changed": await ledger.transition(payload["operation_id"], payload["expected"], payload["state"], payload["result"])}
        raise ValueError("unknown ledger operation")

    def control(self, run_id, fencing_token, worker_id):
        with self.db.engine.connect() as c:
            _run, task = self._lease(c, run_id, fencing_token, worker_id)
            resolved_budget, deadline = self._run_limits(_run, task)
            budget = resolved_budget.model_dump()
            used = json.loads(task["budget_json"])
            elapsed = max(0, (self.clock.now() - datetime.fromisoformat(_run["accounted_at"])).total_seconds()) if _run["accounted_at"] else 0
            return {"state": "CANCEL_REQUESTED" if deadline is not None and deadline <= self.clock.now().isoformat() else task["state"],
                    "delivery": task["delivery"], "budget": budget,
                    "notification_due": self._notification_due(c, task),
                    "active_seconds_remaining": None if deadline is None else max(0, budget.get("active_seconds", 1800) - used.get("notification_active_seconds" if _run["run_reason"] == "notification" else "active_seconds", 0) - elapsed)}

    def _account_active(self, c, run, task):
        used = json.loads(task["budget_json"])
        now = self.clock.now()
        if run["accounted_at"]:
            key = "notification_active_seconds" if run["run_reason"] == "notification" else "active_seconds"
            used[key] = used.get(key, 0) + max(0, (now - datetime.fromisoformat(run["accounted_at"])).total_seconds())
        c.execute(update(tasks).where(tasks.c.id == task["id"]).values(budget_json=json.dumps(used)))
        c.execute(update(runs).where(runs.c.id == run["id"]).values(accounted_at=now.isoformat()))
        return used

    def observation(self, run_id, fencing_token, worker_id, payload):
        from qqbot.storage.task_schema import observations
        with self.db.transaction() as c:
            _run, task = self._lease(c, run_id, fencing_token, worker_id)
            if task["state"] != "RUNNING":
                raise PermissionError("task cannot accept observations")
            encoded = json.dumps(payload["data"], ensure_ascii=False)
            if len(encoded.encode()) > 65536 or len(payload["sha256"]) != 64:
                raise ValueError("invalid observation")
            old = c.execute(select(observations).where(observations.c.id == payload["id"])).mappings().first()
            if old:
                if old["task_id"] != task["id"] or old["sha256"] != payload["sha256"]:
                    raise ValueError("observation identity conflict")
            else:
                c.execute(insert(observations).values(id=payload["id"], task_id=task["id"], run_id=run_id,
                    retrieved_at=payload["retrieved_at"], sha256=payload["sha256"], data_json=encoded))
            return {"accepted": True}

    def external_waiters(self):
        self.recover()
        with self.db.engine.connect() as c:
            rows = c.execute(select(waits, tasks.c.permission_json, tasks.c.goal_json).join(
                tasks, tasks.c.id == waits.c.task_id).where(waits.c.kind == "external",
                waits.c.state == "WAITING", tasks.c.state == "WAITING_EXTERNAL")).mappings().all()
            result = []
            for row in rows:
                target = next((ref.split(":", 1)[1].split("#", 1)[0].split("/") for ref in
                    json.loads(row["goal_json"]).get("target_refs", []) if ref.startswith(("pr:", "issue:", "repo:"))), [])
                result.append({"id": row["id"], "task_id": row["task_id"], "revision": row["revision"],
                    "condition": json.loads(row["condition_json"]), "host": target[0] if len(target) == 3 else "github.com",
                    "repository": "/".join(target[1:]) if len(target) == 3 else None,
                    "principal": json.loads(row["permission_json"])["principal"]})
            return result

    def authorize_waiter(self, payload):
        with self.db.engine.connect() as c:
            wait = c.execute(select(waits).where(waits.c.id == payload["wait_id"],
                waits.c.revision == payload["revision"], waits.c.state == "WAITING")).mappings().first()
            if not wait:
                raise PermissionError("wait is no longer active")
            task = c.execute(select(tasks).where(tasks.c.id == wait["task_id"])).mappings().one()
            if task["state"] != "WAITING_EXTERNAL" or self._deadline_expired(task):
                raise PermissionError("waiting task is no longer active")
            if task["root_task_id"] and hasattr(self, "graph"):
                root = self.graph.root(c, task)
                if root["state"] in TERMINAL | {"CANCEL_REQUESTED"} or self._deadline_expired(root):
                    raise PermissionError("waiting root is no longer active")
            access, context = self._access_for(task), json.loads(task["context_json"])
            if self.context_validator and context and not self.context_validator(access, context):
                raise PermissionError("waiting context was revoked")
            action = payload["action"]
            job_read = action == "workspace.job.inspect" and json.loads(wait["condition_json"]).get("capability") == "job.inspect"
            if not job_read and (not action.startswith(("github.", "web.")) or not action.endswith((".read", ".fetch", ".search"))):
                raise PermissionError("waiter action is not a remote observation")
            if action == "github.graphql.read" and self.permissions.has_restrictions(access):
                raise PermissionError("GraphQL observation cannot prove restricted resource scope")
            for resource in payload["resources"]:
                self.permissions.require(access, action, resource)
            return {"allowed": True}

    def observe_waiter(self, payload):
        if not isinstance(payload["observation"], str) or len(payload["observation"].encode()) > 65536:
            raise ValueError("invalid external observation")
        with self.db.transaction() as c:
            wait = c.execute(select(waits).where(waits.c.id == payload["wait_id"],
                waits.c.revision == payload["revision"])).mappings().first()
            if not wait:
                raise PermissionError("unknown waiter")
            old = c.execute(select(inputs).where(inputs.c.source_id == payload["source_id"])).mappings().first()
            if old:
                return {"accepted": old["task_id"] == wait["task_id"]}
            task = c.execute(select(tasks).where(tasks.c.id == wait["task_id"])).mappings().one()
            if task["state"] != "WAITING_EXTERNAL" or wait["state"] != "WAITING" or self._deadline_expired(task):
                raise PermissionError("external wait no longer active")
            c.execute(insert(inputs).values(id=str(uuid4()), task_id=task["id"], question_id=wait["id"],
                source_id=payload["source_id"], text=payload["observation"], state="PENDING",
                created_at=self.clock.now().isoformat()))
            return {"accepted": True, "woken": self._wake(c, task["id"], wait["id"])}

    def extend_budget(self, run_id, fencing_token, worker_id, payload):
        from qqbot.storage.schema import audit_events
        reason = payload.get("reason")
        if not isinstance(reason, str) or not 1 <= len(reason) <= 1000:
            raise ValueError("budget extension requires a reason for the same goal")
        with self.db.transaction() as c:
            _run, task = self._lease(c, run_id, fencing_token, worker_id)
            if task["state"] != "RUNNING":
                raise PermissionError("inactive task budget cannot be extended")
            self.permissions.require(self._access_for(task), "task.budget.extend", "task:" + task["id"])
            plan = ExecutionPlan.model_validate_json(task["plan_json"])
            values, ceiling = plan.budget.model_dump(), self.budget_ceiling
            for key in ("model_calls", "tool_calls", "active_seconds"):
                increment = payload.get(key, 0)
                if type(increment) is not int or increment < 0:
                    raise ValueError("budget increment must be a nonnegative integer")
                values[key] += increment
                if values[key] > getattr(ceiling, key):
                    raise ValueError("task total budget ceiling exceeded")
            new_plan = plan.model_copy(update={"budget": RunBudget(**values)})
            c.execute(update(tasks).where(tasks.c.id == task["id"]).values(plan_json=new_plan.model_dump_json()))
            c.execute(insert(audit_events).values(id=str(uuid4()), kind="task.budget.extend", target=task["id"],
                actor=task["requester"], created_at=self.clock.now().isoformat(), detail=json.dumps({"reason": reason, "before": plan.budget.model_dump(), "after": values}, ensure_ascii=False)))
            return {"accepted": True, "budget": values}

    def _notification_due(self, c, task):
        latest = c.execute(select(func.max(outbox_table.c.sent_at)).where(outbox_table.c.origin_id.in_(
            select(notifications.c.id).where(notifications.c.task_id == task["id"])))).scalar_one()
        return (self.clock.now() - datetime.fromisoformat(latest or task["created_at"])).total_seconds() >= 90

    def acknowledge_input(self, run_id, fencing_token, worker_id, payload):
        with self.db.transaction() as c:
            _run, task = self._lease(c, run_id, fencing_token, worker_id)
            if c.execute(select(waits.c.id).where(waits.c.run_id == run_id)).first():
                return {"accepted": False, "pending_continuation": True}
            changed = c.execute(update(inputs).where(inputs.c.id == payload["input_id"],
                inputs.c.task_id == task["id"], inputs.c.question_id.is_(None), inputs.c.state == "PENDING").values(state="STEERED"))
            return {"accepted": bool(changed.rowcount)}

    def add_graph_input(self, conn, task, event, access, text, *, kind="followup", question_id=None, source_id=None):
        from qqbot.storage.agent_schema import leader_inbox
        if kind not in {"followup", "answer", "notify"}:
            raise ValueError("invalid task input kind")
        if (task["requester"] != event.principal.key or access.principal != event.principal
                or task["conversation"] != event.conversation.key or access.conversation != event.conversation):
            raise PermissionError("task input requires its authenticated subject")
        self.permissions.require(access, "task.input", "task:" + task["id"])
        if task["state"] in TERMINAL | {"CANCEL_REQUESTED", "NEEDS_RECONCILIATION"}:
            raise PermissionError("task is closed")
        source_id = source_id or event.event_id
        old = conn.execute(select(inputs).where(inputs.c.source_id == source_id)).mappings().first()
        if old:
            if old["task_id"] != task["id"] or old["text"] != text or old["kind"] != kind:
                raise ValueError("conflicting task input replay")
            return {"accepted": True, "input_id": old["id"], "state": old["state"]}
        wait = None
        if kind == "answer":
            if not question_id:
                raise ValueError("answer requires an open question_id")
            wait = conn.execute(select(waits).where(waits.c.id == question_id, waits.c.task_id == task["id"],
                waits.c.state.in_(["INTENT", "WAITING"]))).mappings().first()
            if not wait:
                raise PermissionError("question is no longer open for this task")
        root = self.graph.root(conn, task)
        revision = root["goal_revision"] + (kind != "notify")
        now = self.clock.now().isoformat()
        input_id = str(uuid4())
        conn.execute(insert(inputs).values(id=input_id, task_id=task["id"], question_id=question_id,
            source_id=source_id, text=text, state="READY" if task["state"] == "QUEUED" else "PENDING", created_at=now,
            kind=kind, actor=event.principal.key, goal_revision=revision))
        affected = {task["id"]}
        if kind != "notify":
            conn.execute(update(tasks).where(tasks.c.id == root["id"]).values(goal_revision=revision))
        if kind == "followup":
            children = conn.execute(select(tasks).where(tasks.c.root_task_id == root["id"],
                tasks.c.kind != "goal", tasks.c.id != task["id"], tasks.c.state.not_in(TERMINAL | {"CANCEL_REQUESTED"}))).mappings().all()
            for child in children:
                affected.add(child["id"])
                conn.execute(insert(inputs).values(id=str(uuid4()), task_id=child["id"], question_id=None,
                    source_id=source_id + ":" + child["id"], text=text, state="READY" if child["state"] == "QUEUED" else "PENDING",
                    created_at=now, kind=kind, actor=event.principal.key, goal_revision=revision))
            if not children:
                event_id = self.graph.event(conn, root, "followup", {"input_id": input_id, "text": text})
                conn.execute(insert(leader_inbox).values(id=str(uuid4()), target_session_id=root["coordinator_session_id"],
                    root_task_id=root["id"], event_id=event_id, resume_reason="followup", source_revision=revision, created_at=now))
        if kind != "notify":
            from qqbot.storage.job_schema import jobs
            conn.execute(update(jobs).where(jobs.c.task_id.in_(affected),
                jobs.c.state.not_in({"EXITED", "FAILED", "CANCELLED"})).values(
                    state="CANCEL_REQUESTED", cancel_requested_at=func.coalesce(jobs.c.cancel_requested_at, now)))
            suspended = conn.execute(select(waits).join(tasks, tasks.c.id == waits.c.task_id).where(
                waits.c.task_id.in_(affected), waits.c.kind == "external", waits.c.state == "WAITING",
                tasks.c.state == "WAITING_EXTERNAL")).mappings().all()
            for waiting in suspended:
                if json.loads(waiting["condition_json"]).get("capability") != "job.inspect":
                    continue
                if conn.execute(select(runs.c.id).where(runs.c.task_id == waiting["task_id"], runs.c.slot_reserved == 1)).first():
                    continue
                conn.execute(update(waits).where(waits.c.id == waiting["id"]).values(state="SUPERSEDED"))
                conn.execute(update(tasks).where(tasks.c.id == waiting["task_id"]).values(state="QUEUED"))
                conn.execute(update(inputs).where(inputs.c.task_id == waiting["task_id"],
                    inputs.c.consumed_at.is_(None)).values(state="READY"))
        if wait:
            self._wake(conn, task["id"], question_id)
        self.wakeup.set()
        return {"accepted": True, "input_id": input_id, "state": "PENDING", "goal_revision": revision}

    def _session_receipt(self, conn, run, task, payload):
        if (payload.get("session_id") != run["session_id"] or payload.get("run_id") != run["id"]
                or not isinstance(payload.get("session_revision"), str)):
            raise PermissionError("receipt does not match the worker session")
        ids = payload.get("input_ids")
        if not isinstance(ids, list) or any(not isinstance(value, str) for value in ids):
            raise ValueError("input receipt needs explicit IDs")
        known = set(conn.execute(select(inputs.c.id).where(inputs.c.task_id == task["id"])).scalars())
        if set(ids) - known - {"task:" + task["id"]}:
            raise PermissionError("receipt names another task's input")
        now = self.clock.now().isoformat()
        kind = payload.get("kind")
        if kind not in {"input_persisted", "input_consumed", "turn_finished", "run_yielded"}:
            raise ValueError("invalid receipt kind")
        values = {"persisted_at": now}
        if kind in {"input_consumed", "turn_finished", "run_yielded"}:
            values.update(consumed_at=now, state="CONSUMED")
        conn.execute(update(inputs).where(inputs.c.id.in_(ids), inputs.c.task_id == task["id"]).values(**values))
        if kind in {"turn_finished", "run_yielded"}:
            if payload.get("inflight") != 0 or payload.get("tools_paired") is not True:
                raise PermissionError("Harness still has activity")
            conn.execute(update(runs).where(runs.c.id == run["id"]).values(receipt_json=json.dumps(payload)))
        if task["root_task_id"] and kind != "input_persisted":
            root = self.graph.root(conn, task)
            unconsumed = conn.execute(select(inputs.c.id).where(inputs.c.task_id == task["id"],
                inputs.c.kind != "notify", inputs.c.consumed_at.is_(None))).first()
            if not unconsumed:
                conn.execute(update(tasks).where(tasks.c.id == task["id"]).values(goal_revision=root["goal_revision"]))
                conn.execute(update(runs).where(runs.c.id == run["id"]).values(basis_goal_revision=root["goal_revision"]))
            remaining = conn.execute(select(inputs.c.id).where(inputs.c.task_id.in_(select(tasks.c.id).where(
                tasks.c.root_task_id == root["id"], tasks.c.kind != "goal")), inputs.c.kind != "notify",
                inputs.c.consumed_at.is_(None))).first()
            if not remaining:
                conn.execute(update(inputs).where(inputs.c.task_id == root["id"], inputs.c.kind != "notify").values(
                    persisted_at=now, consumed_at=now, state="CONSUMED"))
        return {"accepted": True}

    def session_receipt(self, run_id, fencing_token, worker_id, payload):
        with self.db.transaction() as conn:
            run, task = self._lease(conn, run_id, fencing_token, worker_id)
            return self._session_receipt(conn, run, task, payload)

    def task_operation(self, run_id, fencing_token, worker_id, payload):
        from qqbot.domain.agents import TaskInputSpec
        with self.db.transaction() as conn:
            _run, task = self._lease(conn, run_id, fencing_token, worker_id)
            self.graph.check_authority(conn, task, {"action": "task.execute"})
            access = self._access_for(task)
            operation, arguments = payload["operation"], payload.get("arguments", {})
            if operation == "tasks.submit":
                raise PermissionError("Worker cannot create new root goals")
            if operation == "tasks.spawn":
                return self.graph.spawn(conn, task, arguments, access)
            if operation == "tasks.wait":
                return self.graph.wait(conn, task, arguments, run_id)
            if operation == "tasks.report_progress":
                return self.graph.progress(conn, task, run_id, arguments, payload["call_id"])
            rows = conn.execute(select(tasks).where(tasks.c.root_task_id == task["root_task_id"])).mappings().all()
            if operation == "tasks.list":
                return [self.graph.status(conn, row) for row in rows]
            ref = arguments.get("task_ref")
            target = next((row for row in rows if ref in {row["id"], row["public_id"]}), None)
            if not target:
                raise PermissionError("task reference is outside this tree")
            if operation == "tasks.status":
                return self.graph.status(conn, target)
            if operation == "tasks.send_input":
                spec = TaskInputSpec.model_validate_json(json.dumps(arguments))
                if spec.kind != "notify":
                    raise PermissionError("Worker messages cannot impersonate user revisions or answers")
                event = InboundEvent.model_validate_json(task["event_json"])
                return self.add_graph_input(conn, target, event, access, spec.text, kind="notify",
                    source_id="notify:" + payload["call_id"])
            if operation == "tasks.cancel" and target["parent_task_id"] == task["id"]:
                self.graph.cancel_tree(conn, target)
                return {"accepted": True}
            raise PermissionError("task operation is not allowed for this Worker")
