"""Recover stopped attempts from native receipts without replaying uncertain writes."""
import asyncio
import hashlib
import json
import logging
import os
import re
import stat
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

from sqlalchemy import select, update

from qqbot.adapters.execution.world import execution_workspace
from qqbot.adapters.pi.sessions import recovery_snapshot
from qqbot.capabilities.types import Action, ToolContext
from qqbot.domain import InboundEvent, stable_id
from qqbot.domain.tasks import TaskResult
from qqbot.harness.artifacts import DEFAULT_ARTIFACT_MIME_TYPES, export_artifacts
from qqbot.harness.launcher import trusted_json
from qqbot.storage.action_schema import operations
from qqbot.storage.agent_schema import leader_inbox
from qqbot.storage.schema import outbox
from qqbot.storage.task_schema import (
    inputs,
    model_requests,
    observations,
    processes,
    runs,
    sessions,
    tasks,
    waits,
)

from .task_graph import TERMINAL

logger = logging.getLogger(__name__)
STOP_DIRECTORY = Path("/var/lib/qqbot-daemon-stops")
EPOCH = re.compile(r"^[a-f0-9]{32}$")
UNCERTAIN = frozenset({"UNKNOWN", "DISPATCHED", "PARTIAL", "RECONCILIATION"})


class RuntimeRecovery:
    def __init__(self, core, *, directory=STOP_DIRECTORY, verifier=None):
        self.core, self.service, self.directory = core, core.tasks, Path(directory)
        self.epoch = os.environ.get("INVOCATION_ID", "")
        self.data_root = Path(core.db.path).resolve().parent
        self.verifier = verifier
        self._github = self._icloud = None

    def stopped(self, task_id, process_id):
        with self.core.db.engine.connect() as conn:
            process = conn.execute(select(processes).where(processes.c.id == process_id)).mappings().first()
            run = conn.execute(select(runs).where(runs.c.id == process_id)).mappings().first() if not process else None
        identity = json.loads(process["identity_json"]) if process else json.loads(run["request_json"]) if run else {}
        if not isinstance(identity, dict):
            return False
        if process and identity.get("launch_task_id") == task_id and process["state"] == "STOPPED":
            return True
        epoch = identity.get("host_epoch")
        if not isinstance(epoch, str) or not EPOCH.fullmatch(epoch) or epoch == self.epoch:
            return False
        if process and identity.get("launch_task_id") != task_id:
            return False
        if run and identity.get("task_id") != task_id:
            return False
        try:
            receipt = trusted_json(self.directory / (epoch + ".json"))
            cgroup, boot_id = receipt.get("cgroup"), receipt.get("boot_id")
            return (receipt.get("host_epoch") == epoch and receipt.get("stopped") is True
                and receipt.get("cgroup_empty") is True and isinstance(cgroup, str) and cgroup.startswith("/")
                and ".." not in Path(cgroup).parts and isinstance(boot_id, str) and str(UUID(boot_id)) == boot_id)
        except (OSError, ValueError, TypeError, AttributeError):
            return False

    def _records(self, run_id):
        with self.core.db.engine.connect() as conn:
            run = conn.execute(select(runs).where(runs.c.id == run_id, runs.c.role == "worker")).mappings().first()
            if run is None:
                raise KeyError("unknown Worker recovery attempt")
            task = conn.execute(select(tasks).where(tasks.c.id == run["task_id"])).mappings().one()
            process = conn.execute(select(processes).where(processes.c.id == run["process_id"])).mappings().first()
        return run, task, process

    def recover_processes(self):
        """Retire proved old-daemon residents without settling their active runs."""
        with self.core.db.engine.connect() as conn:
            residents = conn.execute(select(processes).where(processes.c.state != "STOPPED")).mappings().all()
        recovered = []
        for process in residents:
            identity = json.loads(process["identity_json"])
            task_id = identity.get("launch_task_id") if isinstance(identity, dict) else None
            if not isinstance(task_id, str) or not task_id or not self.stopped(task_id, process["id"]):
                continue
            with self.core.db.transaction() as conn:
                changed = conn.execute(update(processes).where(processes.c.id == process["id"],
                    processes.c.epoch == process["epoch"], processes.c.generation == process["generation"],
                    processes.c.identity_json == process["identity_json"], processes.c.state != "STOPPED")
                    .values(state="STOPPED", stopped_at=self.core.clock.now().isoformat()))
                if not changed.rowcount:
                    continue
                inactive = select(runs.c.id).where(runs.c.session_id == process["session_id"], runs.c.slot_reserved == 0)
                conn.execute(update(sessions).where(sessions.c.id == process["session_id"],
                    sessions.c.active_run_id.in_(inactive)).values(active_run_id=None))
                recovered.append({"process_id": process["id"], "state": "STOPPED"})
        return recovered

    def _stop_proved(self, run, task, process):
        if process and (process["epoch"] != run["process_epoch"] or process["generation"] != run["binding_generation"]):
            return False
        identity = json.loads(process["identity_json"]) if process else {}
        if not isinstance(identity, dict):
            return False
        if process and process["state"] == "STOPPED":
            return True
        return self.stopped(identity.get("launch_task_id", task["id"]), run["process_id"] or run["id"])

    def _path(self, run, process):
        identity = json.loads(process["identity_json"]) if process else {}
        path = Path(identity.get("actual_session_file") or run["session"])
        return path if path.is_absolute() else self.data_root / "pi" / path

    def _charge_stopped_activity(self, run, task, process):
        identity = json.loads(process["identity_json"] if process else run["request_json"])
        stopped_at = min(self.core.clock.now(), datetime.fromisoformat(run["lease"]))
        try:
            receipt = trusted_json(self.directory / (identity["host_epoch"] + ".json"))
            timestamp = datetime.fromisoformat(receipt["stopped_at"])
            if timestamp.tzinfo is not None:
                stopped_at = min(self.core.clock.now(), timestamp)
        except (OSError, ValueError, TypeError, KeyError):
            pass
        with self.core.db.transaction() as conn:
            current = conn.execute(select(runs).where(runs.c.id == run["id"])).mappings().one()
            if not current["accounted_at"]:
                return
            elapsed = max(0, (stopped_at - datetime.fromisoformat(current["accounted_at"])).total_seconds())
            if not elapsed:
                return
            budget = json.loads(conn.execute(select(tasks.c.budget_json).where(tasks.c.id == task["id"])).scalar_one())
            key = "notification_active_seconds" if run["run_reason"] == "notification" else "active_seconds"
            budget[key] = budget.get(key, 0) + elapsed
            conn.execute(update(tasks).where(tasks.c.id == task["id"]).values(budget_json=json.dumps(budget)))
            conn.execute(update(runs).where(runs.c.id == run["id"]).values(accounted_at=stopped_at.isoformat()))

    def _release(self, conn, run, *, state):
        now = self.core.clock.now().isoformat()
        conn.execute(update(runs).where(runs.c.id == run["id"]).values(state=state, slot_reserved=0,
            lease=now, runner_stopped_at=now, quiescent_at=now))
        if run["process_id"]:
            conn.execute(update(processes).where(processes.c.id == run["process_id"],
                processes.c.epoch == run["process_epoch"], processes.c.generation == run["binding_generation"])
                .values(state="STOPPED", stopped_at=now))
        conn.execute(update(sessions).where(sessions.c.active_run_id == run["id"]).values(active_run_id=None))

    def _pause(self, run, task, reason):
        with self.core.db.transaction() as conn:
            current = conn.execute(select(tasks).where(tasks.c.id == task["id"])).mappings().one()
            if current["fencing"] != run["fencing"]:
                return {"task_id": task["id"], "run_id": run["id"], "state": current["state"]}
            self._release(conn, run, state="EVIDENCE_REQUIRED" if reason == "invalid_recovery_evidence" else "NEEDS_RECONCILIATION")
            conn.execute(update(tasks).where(tasks.c.id == task["id"]).values(state="NEEDS_RECONCILIATION"))
            graph = getattr(self.service, "graph", None)
            if graph and task["root_task_id"]:
                graph.event(conn, current, "needs_attention", {"reason": reason},
                    attempt_id=run["id"], event_id=stable_id("recovery-paused", run["id"], reason), wake=True)
        self.service.wakeup.set()
        return {"task_id": task["id"], "run_id": run["id"], "state": "NEEDS_RECONCILIATION", "reason": reason}

    def _uncertain(self, conn, task):
        task_id = self.service._operation_task_id(conn, task)
        return bool(conn.execute(select(operations.c.id).where(operations.c.task_id == task_id,
            operations.c.state.in_(UNCERTAIN))).first())

    def _terminal_reason(self, task):
        if task["state"] in TERMINAL:
            return task["state"], task["stop_reason"] or "already_settled"
        if task["stop_reason"] in {"user_cancel", "revoked"}:
            return "CANCELLED", task["stop_reason"]
        if self.service._deadline_expired(task) or (
                self.service._time_limited(task) and task["stop_reason"] == "deadline"):
            return "EXPIRED", "deadline"
        old_time_stop = not self.service._time_limited(task) and task["stop_reason"] in {"deadline", "active_budget"}
        if task["state"] == "CANCEL_REQUESTED" and not old_time_stop:
            return ("FAILED", task["stop_reason"]) if task["stop_reason"] else ("CANCELLED", "user_cancel")
        event = InboundEvent.model_validate_json(task["event_json"])
        access = self.service._access_for(task)
        if not self.core.allowed(event) or not self.service.permissions.check(access, "task.execute", "task:" + task["id"]).allowed:
            return "CANCELLED", "revoked"
        context = json.loads(task["context_json"])
        if self.service.context_validator and not self.service.context_validator(access, context):
            return "BLOCKED", "context_revoked"
        if task["root_task_id"]:
            with self.core.db.engine.connect() as conn:
                root = conn.execute(select(tasks).where(tasks.c.id == task["root_task_id"])).mappings().one()
            if root["state"] in TERMINAL | {"CANCEL_REQUESTED"}:
                return "CANCELLED", "revoked"
        return None

    def _settle_terminal(self, run, task, state, reason):
        with self.core.db.transaction() as conn:
            current = conn.execute(select(tasks).where(tasks.c.id == task["id"])).mappings().one()
            if current["fencing"] != run["fencing"]:
                return {"task_id": task["id"], "run_id": run["id"], "state": current["state"]}
            if current["state"] in TERMINAL:
                self._release(conn, run, state="INTERRUPTED")
                return {"task_id": task["id"], "run_id": run["id"], "state": current["state"]}
            if state == "CANCELLED" and not getattr(self.service, "job_tree_quiescent", lambda _: True)(current):
                state = "CANCEL_REQUESTED"
            self._release(conn, run, state="INTERRUPTED")
            result = TaskResult(status="failed", agent_authored=False,
                summary="The stopped attempt was settled after " + reason + ".")
            conn.execute(update(tasks).where(tasks.c.id == task["id"]).values(state=state, stop_reason=reason,
                result_json=result.model_dump_json()))
            if state in TERMINAL and task["root_task_id"]:
                settled = {**dict(current), "state": state}
                self.service.graph.settled(conn, settled, run["id"], result.model_dump(mode="json"))
        self.service.wakeup.set()
        return {"task_id": task["id"], "run_id": run["id"], "state": state}

    def _spooled_result(self, run):
        for path in sorted((self.data_root / "spool").glob("*.json")):
            try:
                with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)) as source:
                    info = os.fstat(source.fileno())
                    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > 32 * 1024 * 1024:
                        continue
                    encoded = source.read(32 * 1024 * 1024 + 1)
                    if len(encoded.encode()) > 32 * 1024 * 1024:
                        continue
                    payload = json.loads(encoded)
                if isinstance(payload, dict) and payload.get("run_id") == run["id"] and payload.get("fencing_token") == run["fencing"]:
                    return payload
            except (OSError, ValueError, TypeError):
                continue
        return None

    def _proof(self, run, snapshot, spool):
        proof = json.loads(run["receipt_json"]) if run["receipt_json"] else None
        if spool and spool.get("receipt"):
            proof = {**(proof or {}), **spool["receipt"]}
        if snapshot["boundaries"]:
            proof = {**snapshot["boundaries"][-1], **(proof or {})}
        if proof and (proof.get("run_id") != run["id"] or proof.get("session_id") != run["session_id"]
                or proof.get("kind") not in {"turn_finished", "run_yielded"}
                or proof.get("inflight") != 0 or proof.get("tools_paired") is not True):
            raise ValueError("native boundary does not match this stopped attempt")
        return proof

    def _workspace(self, task, *, strict=True):
        binding = json.loads(task["binding_json"])
        return execution_workspace(self.data_root, task["id"], json.loads(task["plan_json"]).get("workspace_ref"),
            binding.get("workspace_bindings", {}), strict=strict)

    def _result(self, run, task, proof, spool):
        if spool:
            return TaskResult.model_validate_json(json.dumps(spool["result"]))
        if proof.get("recovery_result"):
            return TaskResult.model_validate_json(json.dumps(proof["recovery_result"]))
        outcome = proof.get("outcome") or {}
        result = dict(proof.get("result") or {"status": outcome.get("status", "completed"),
            "summary": proof.get("summary") or "Waiting checkpoint persisted.", "agent_authored": True,
            "limitations": outcome.get("unresolved_items", [])})
        with self.core.db.engine.connect() as conn:
            if "observation_refs" not in result:
                result["observation_refs"] = list(conn.execute(select(observations.c.id).where(observations.c.run_id == run["id"])).scalars())
            if "effect_refs" not in result:
                result["effect_refs"] = list(conn.execute(select(operations.c.id).where(operations.c.run_id == run["id"])).scalars())
            generated = conn.execute(select(operations).where(operations.c.run_id == run["id"],
                operations.c.state == "VERIFIED")).mappings().all()
        paths = list(proof.get("attachments") or outcome.get("attachments") or [])
        requested = json.loads(task["goal_json"]).get("requested_outputs", [])
        for operation in generated:
            data = json.loads(operation["result_json"])
            path = data.get("path") if isinstance(data, dict) else None
            if isinstance(path, str) and (path.startswith("outputs/") and Path(path).suffix.lower() in DEFAULT_ARTIFACT_MIME_TYPES
                                          or path in requested):
                paths.append(path)
        if result["status"] == "completed":
            paths.extend(requested)
        if paths:
            result["artifacts"] = export_artifacts(self._workspace(task), self.service.archive_root, list(dict.fromkeys(paths)))
        return TaskResult.model_validate_json(json.dumps(result))

    def _commit(self, run, task, proof, result):
        self.service._validate_result(result)
        with self.core.db.transaction() as conn:
            current = conn.execute(select(tasks).where(tasks.c.id == task["id"])).mappings().one()
            attempt = conn.execute(select(runs).where(runs.c.id == run["id"])).mappings().one()
            if current["fencing"] != run["fencing"] or attempt["state"] == "FINISHED":
                return {"task_id": task["id"], "run_id": run["id"], "state": current["state"]}
            now = self.core.clock.now()
            proof = {**proof, "recovery_result": result.model_dump(mode="json")}
            conn.execute(update(runs).where(runs.c.id == run["id"]).values(slot_reserved=1, state="RECOVERING",
                receipt_json=json.dumps(proof, ensure_ascii=False), lease=(now + timedelta(seconds=30)).isoformat(),
                accounted_at=now.isoformat(), runner_stopped_at=now.isoformat()))
            if run["process_id"]:
                conn.execute(update(processes).where(processes.c.id == run["process_id"],
                    processes.c.generation == run["binding_generation"], processes.c.epoch == run["process_epoch"])
                    .values(state="STOPPED", stopped_at=now.isoformat()))
            conn.execute(update(tasks).where(tasks.c.id == task["id"]).values(state="RUNNING"))
        settled = self.service.submit(run["id"], run["fencing"], run["worker"], result.model_dump(mode="json"),
            stopped=True, receipt=proof)
        with self.core.db.transaction() as conn:
            resolved = ["lease-lost:" + run["id"], *[stable_id("recovery-paused", run["id"], reason)
                for reason in ("unpaired_tools", "uncertain_operations", "wait_boundary_missing",
                    "native_session_missing", "invalid_recovery_evidence")]]
            conn.execute(update(leader_inbox).where(leader_inbox.c.event_id.in_(resolved),
                leader_inbox.c.claimed_attempt_id.is_(None), leader_inbox.c.handled_at.is_(None)).values(
                    handled_at=self.core.clock.now().isoformat(), output_ref="RECOVERED"))
        observer = getattr(self.core, "leader", None)
        if observer:
            observer.observe_operation((run["id"], run["fencing"], run["worker"]), "result", {})
        return {"task_id": task["id"], "run_id": run["id"], **settled}

    def recover_workers(self):
        with self.core.db.engine.connect() as conn:
            candidates = conn.execute(select(runs.c.id).where(runs.c.role == "worker",
                (runs.c.slot_reserved == 1) | (runs.c.state == "NEEDS_RECONCILIATION"))).scalars().all()
        return [self.recover_worker(run_id) for run_id in candidates]

    def recover_worker(self, run_id):
        run, task, process = self._records(run_id)
        if run["fencing"] != task["fencing"] or run["state"] in {"FINISHED", "INTERRUPTED"}:
            return {"task_id": task["id"], "run_id": run_id, "state": task["state"]}
        if run["state"] == "EVIDENCE_REQUIRED":
            return {"task_id": task["id"], "run_id": run_id, "state": "NEEDS_RECONCILIATION", "reason": "invalid_recovery_evidence"}
        proved = self._stop_proved(run, task, process)
        # A live supervisor retains result ownership until its lease expires.
        if process and process["state"] == "STOPPED" and run["lease"] > self.core.clock.now().isoformat():
            return {"task_id": task["id"], "run_id": run_id, "state": task["state"]}
        if run["lease"] > self.core.clock.now().isoformat() and not proved:
            return {"task_id": task["id"], "run_id": run_id, "state": task["state"]}
        if not proved:
            with self.core.db.transaction() as conn:
                conn.execute(update(tasks).where(tasks.c.id == task["id"], tasks.c.state.not_in(TERMINAL))
                    .values(state="NEEDS_RECONCILIATION"))
                conn.execute(update(runs).where(runs.c.id == run_id, runs.c.slot_reserved == 1)
                    .values(state="NEEDS_RECONCILIATION"))
                graph = getattr(self.service, "graph", None)
                if graph and task["root_task_id"]:
                    graph.event(conn, task, "needs_attention", {"reason": "worker_lease_lost"},
                        attempt_id=run_id, event_id="lease-lost:" + run_id, wake=True)
            self.service.wakeup.set()
            return {"task_id": task["id"], "run_id": run_id, "state": "NEEDS_RECONCILIATION"}
        self._charge_stopped_activity(run, task, process)
        now = self.core.clock.now().isoformat()
        with self.core.db.transaction() as conn:
            conn.execute(update(operations).where(operations.c.run_id == run_id, operations.c.state == "DISPATCHED")
                .values(state="UNKNOWN", updated_at=now))
            conn.execute(update(operations).where(operations.c.run_id == run_id, operations.c.state == "PREPARED")
                .values(state="FAILED", result_json=json.dumps({"reason": "daemon_stopped_before_dispatch"}), updated_at=now))
        terminal = self._terminal_reason(task)
        if terminal:
            return self._settle_terminal(run, task, *terminal)
        try:
            snapshot = recovery_snapshot(self._path(run, process), run_id=run_id)
            if snapshot["pending_tools"]:
                return self._pause(run, task, "unpaired_tools")
            with self.core.db.engine.connect() as conn:
                if self._uncertain(conn, task):
                    return self._pause(run, task, "uncertain_operations")
                wait = conn.execute(select(waits).where(waits.c.run_id == run_id, waits.c.state.in_(["INTENT", "WAITING"]))).mappings().first()
                activity = bool(conn.execute(select(model_requests.c.id).where(model_requests.c.run_id == run_id)).first()
                    or conn.execute(select(operations.c.id).where(operations.c.run_id == run_id)).first()
                    or conn.execute(select(observations.c.id).where(observations.c.run_id == run_id)).first())
            spool = self._spooled_result(run)
            proof = self._proof(run, snapshot, spool)
            child_wait = bool(task["wait_json"] and json.loads(task["wait_json"]).get("attempt_id") == run_id)
            if not proof and snapshot["yielded"] and snapshot["session_revision"] and (wait or child_wait):
                proof = {"run_id": run_id, "session_id": run["session_id"], "kind": "run_yielded",
                    "session_revision": snapshot["session_revision"], "input_ids": snapshot["consumed_input_ids"],
                    "inflight": 0, "tools_paired": True, "result": {"status": "waiting_children" if child_wait else
                        "waiting_input" if wait["kind"] == "input" else "waiting_external", "summary": "Waiting checkpoint persisted."}}
            if proof:
                result = self._result(run, task, proof, spool)
                if (wait or child_wait) and not proof.get("result") and result.status == "completed":
                    result = result.model_copy(update={"status": "waiting_children" if child_wait else
                        "waiting_input" if wait["kind"] == "input" else "waiting_external"})
                return self._commit(run, task, proof, result)
            if spool and not spool.get("result", {}).get("agent_authored", True):
                return self._commit(run, task, {}, self._result(run, task, {}, spool))
            if wait or child_wait:
                return self._pause(run, task, "wait_boundary_missing")
            if not snapshot["exists"] and activity:
                return self._pause(run, task, "native_session_missing")
        except (OSError, ValueError, KeyError, TypeError, PermissionError) as error:
            logger.warning("Worker recovery paused run_id=%s error_type=%s", run_id, type(error).__name__)
            return self._pause(run, task, "invalid_recovery_evidence")
        with self.core.db.transaction() as conn:
            current = conn.execute(select(tasks).where(tasks.c.id == task["id"])).mappings().one()
            if current["fencing"] != run["fencing"]:
                return {"task_id": task["id"], "run_id": run_id, "state": current["state"]}
            self._release(conn, run, state="INTERRUPTED")
            conn.execute(update(tasks).where(tasks.c.id == task["id"]).values(state="QUEUED"))
            conn.execute(update(inputs).where(inputs.c.task_id == task["id"], inputs.c.consumed_at.is_(None)).values(state="READY"))
        self.service.wakeup.set()
        return {"task_id": task["id"], "run_id": run_id, "state": "QUEUED"}

    def _task(self, public_id, access, *, action="task.reconcile"):
        with self.core.db.engine.connect() as conn:
            task = self.service._visible(conn, public_id, access)
            self.service.permissions.require(access, action, "task:" + task["id"])
            return dict(task)

    def list_operations(self, public_id, access):
        task = self._task(public_id, access, action="task.read")
        with self.core.db.engine.connect() as conn:
            task_id = self.service._operation_task_id(conn, task)
            rows = conn.execute(select(operations).where(operations.c.task_id == task_id).order_by(operations.c.updated_at)).mappings().all()
        return [{"operation_id": row["id"], "task_id": row["task_id"], "run_id": row["run_id"], "state": row["state"],
            "action": json.loads(row["action_json"]), "arguments": json.loads(row["arguments_json"]),
            "result": json.loads(row["result_json"]), "updated_at": row["updated_at"]} for row in rows]

    def _read_action(self, action):
        if action.name.startswith("github."):
            return ".".join(action.name.split(".")[:2]) + ".read"
        if action.name.startswith("icloud."):
            return "icloud.calendar.get"
        if action.name.startswith("workspace.job."):
            return "workspace.job.inspect"
        if action.name == "workspace.file.write":
            return "workspace.file.read"
        return "task.operations.read"

    async def _verify(self, context, action, arguments, data):
        if self.verifier:
            return await self.verifier(context, action, arguments, data)
        if action.name.startswith("github."):
            if self._github is None:
                from qqbot.capabilities.gateway import standard_registry
                from qqbot.jobs.worker import configured_github
                self._github = standard_registry(github=configured_github(SimpleNamespace(
                    github_host=self.core.settings.orchestration.github_host))).verify
            return await self._github(context, action, arguments, data)
        if action.name.startswith("icloud.") and os.environ.get("ICLOUD_CALENDAR_CONFIG"):
            if self._icloud is None:
                from qqbot.capabilities.icloud import ICloudCalendarService
                self._icloud = ICloudCalendarService.from_file(os.environ["ICLOUD_CALENDAR_CONFIG"])
            return await self._icloud.verify(context, action, arguments, data)
        if action.name in {"workspace.job.start", "workspace.job.cancel"} and getattr(self.core, "job_runtime", None):
            with self.core.db.engine.connect() as conn:
                run = conn.execute(select(runs).where(runs.c.id == context.run_id)).mappings().one()
            identity = (run["id"], run["fencing"], run["worker"])
            payload = {"operation": "lookup", "arguments": {"job_key": arguments["job_key"]}} if action.name.endswith("start") else {
                "operation": "inspect", "arguments": {"job_id": arguments["job_id"]}}
            receipt = await self.core.job_runtime.invoke(identity, payload)
            if action.name.endswith("start") and receipt.get("invocation_id"):
                return "VERIFIED", {**receipt, "verified_effect": "supervised_job_dispatch",
                    "execution_succeeded": receipt.get("state") == "EXITED" and receipt.get("exit_code") == 0}
            if action.name.endswith("cancel") and receipt.get("state") in {"EXITED", "FAILED", "CANCELLED"}:
                return "VERIFIED", {**receipt, "verified_effect": "job_stopped"}
            return "UNKNOWN", receipt
        if action.name == "notification.send":
            notification_id = stable_id("qq-rich", context.task_id, arguments.get("request_id", ""))
            with self.core.db.engine.connect() as conn:
                delivery = conn.execute(select(outbox).where(outbox.c.task_id == context.task_id,
                    outbox.c.origin_id == notification_id)).mappings().first()
            if delivery:
                return "VERIFIED", {"delivery_id": delivery["id"], "state": delivery["state"],
                    "delivered": delivery["state"] == "SENT", "verified_effect": "durable_enqueue"}
        if action.name == "workspace.file.write":
            from qqbot.capabilities.gateway import workspace_file
            def read_file():
                with os.fdopen(workspace_file(context, arguments["path"], os.O_RDONLY), "rb") as source:
                    return source.read(1048577)
            content = await asyncio.to_thread(read_file)
            if content == arguments["text"].encode():
                return "VERIFIED", {"path": arguments["path"], "sha256": hashlib.sha256(content).hexdigest(),
                    "verified_effect": "file_content_matches"}
        return "UNKNOWN", data

    async def readback_operation(self, public_id, access, operation_id):
        self._task(public_id, access)
        record = next((row for row in self.list_operations(public_id, access) if row["operation_id"] == operation_id), None)
        if record is None:
            raise KeyError("operation does not belong to this task")
        if record["state"] not in {"UNKNOWN", "PARTIAL"}:
            return {"operation_id": operation_id, "state": record["state"], "result": record["result"], "changed": False}
        action = Action(**{**record["action"], "resources": tuple(record["action"]["resources"])})
        for resource in action.resources:
            self.service.permissions.require(access, self._read_action(action), resource)
        with self.core.db.engine.connect() as conn:
            run = conn.execute(select(runs).where(runs.c.id == record["run_id"])).mappings().one()
            claim = json.loads(run["request_json"])
            execution_task = conn.execute(select(tasks).where(tasks.c.id == run["task_id"])).mappings().one()
            if self.service._operation_task_id(conn, execution_task) != record["task_id"]:
                raise PermissionError("operation execution does not match its ledger scope")
        host = claim.get("host") or self.core.settings.orchestration.github_host
        if host != self.core.settings.orchestration.github_host:
            raise PermissionError("readback credential host differs from the task binding")
        context = ToolContext(task_id=record["task_id"], run_id=record["run_id"], fence=run["fencing"],
            principal=access.principal.model_dump(mode="json"), workspace=str(self._workspace(execution_task, strict=False)),
            repository=claim.get("repository"), host=host)
        verification = await self._verify(context, action, record["arguments"], record["result"])
        state, result = verification or ("UNKNOWN", record["result"])
        if state not in {"VERIFIED", "FAILED", "PARTIAL", "UNKNOWN"}:
            raise ValueError("readback returned an invalid operation state")
        if len(json.dumps(result).encode()) > 1048576:
            raise ValueError("readback result exceeds the operation evidence limit")
        self._task(public_id, access)
        for resource in action.resources:
            self.service.permissions.require(access, self._read_action(action), resource)
        with self.core.db.transaction() as conn:
            changed = conn.execute(update(operations).where(operations.c.id == operation_id, operations.c.state == record["state"])
                .values(state=state, result_json=json.dumps(result, ensure_ascii=False), updated_at=self.core.clock.now().isoformat())).rowcount
            current = conn.execute(select(operations).where(operations.c.id == operation_id)).mappings().one()
        return {"operation_id": operation_id, "state": current["state"],
            "result": json.loads(current["result_json"]), "changed": bool(changed)}

    def reconcile_task(self, public_id, access, run_id, *, result=None, resume=False):
        task = self._task(public_id, access)
        run, original, process = self._records(run_id)
        if original["id"] != task["id"] or run["fencing"] != task["fencing"]:
            raise PermissionError("recovery source is not this task's latest attempt")
        if run["state"] == "FINISHED" and not run["slot_reserved"]:
            return {"task_id": task["id"], "run_id": run_id, "state": task["state"]}
        if not self._stop_proved(run, task, process):
            raise PermissionError("the old daemon stop is not verified")
        self._charge_stopped_activity(run, task, process)
        if resume and result is not None:
            raise ValueError("choose either continuation or an explicit result")
        if result is None:
            if not resume:
                raise ValueError("reconciliation needs continuation or an explicit result")
            return self.recover_worker(run_id)
        validated = TaskResult.model_validate_json(json.dumps(result))
        with self.core.db.engine.connect() as conn:
            if validated.status == "completed" and self._uncertain(conn, task):
                raise PermissionError("uncertain operations prevent successful completion")
        terminal = self._terminal_reason(task)
        if terminal:
            return self._settle_terminal(run, task, *terminal)
        snapshot = recovery_snapshot(self._path(run, process), run_id=run_id)
        proof = self._proof(run, snapshot, None)
        if validated.agent_authored and not proof:
            raise PermissionError("an agent-authored result requires its durable boundary")
        return self._commit(run, task, proof or {}, validated)
