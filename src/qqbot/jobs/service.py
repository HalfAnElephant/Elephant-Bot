import hashlib
import json
import os
import stat
from datetime import timedelta
from pathlib import Path
from uuid import UUID, uuid4

from sqlalchemy import func, insert, select, update

from qqbot.domain.messages import InboundEvent
from qqbot.domain.tasks import TaskResult
from qqbot.harness.launcher import verify_stop_marker
from qqbot.storage.task_schema import events, runs, tasks

TERMINAL = {"SUCCEEDED", "FAILED", "CANCELLED", "EXPIRED"}


class TaskService:
    def __init__(self, db, clock, outbox, archive_root=None, stop_verifier=None):
        self.db, self.clock, self.outbox = db, clock, outbox
        self.stop_verifier = stop_verifier or verify_stop_marker
        self.archive_root = Path(archive_root) if archive_root else Path(db.path).parent / "artifacts"

    def accept(self, event, access, goal, profile="pi_research", *, retry_of_task_id=None):
        if access.principal != event.principal or access.conversation != event.conversation:
            raise PermissionError("identity mismatch")
        if access.role not in {"owner", "admin"}:
            raise PermissionError("task submission requires a trusted administrator")
        if profile != "pi_research" or not goal.strip() or len(goal) > 16000:
            raise ValueError("unsupported profile or goal")
        with self.db.transaction() as c:
            old = c.execute(select(tasks).where(tasks.c.source == event.event_id)).mappings().first()
            if old:
                if (
                    old["goal"] != goal
                    or old["profile"] != profile
                    or old["retry_of_task_id"] != retry_of_task_id
                ):
                    raise ValueError("source event already accepted with a different goal or profile")
                return dict(old)
            queued = tasks.c.state.in_(["QUEUED", "WAITING_INPUT"])
            for condition, limit in (
                (tasks.c.requester == access.principal.key, 2),
                (tasks.c.conversation == access.conversation.key, 5),
            ):
                count = c.execute(
                    select(func.count()).select_from(tasks).where(queued, condition)
                ).scalar_one()
                if count >= limit:
                    raise ValueError("task queue full")
            task_id = str(uuid4())
            public_id = "T-" + task_id[:12]
            row = {
                "id": task_id,
                "public_id": public_id,
                "source": event.event_id,
                "conversation": event.conversation.key,
                "requester": event.principal.key,
                "event_json": event.model_dump_json(),
                "goal": goal,
                "profile": profile,
                "state": "QUEUED",
                "created_at": self.clock.now().isoformat(),
                "deadline": (self.clock.now() + timedelta(seconds=1800)).isoformat(),
                "fencing": 0,
                "retry_of_task_id": retry_of_task_id,
                "permission_json": json.dumps(
                    {
                        "principal": access.principal.model_dump(),
                        "identity_source": "trusted_access_context",
                        "role": access.role,
                        "scope": access.conversation.key,
                        "tools": ["research_search", "research_fetch", "research_write"],
                        "budget": {
                            "wall_time_s": 1800,
                            "model_calls": 30,
                            "tool_calls": 60,
                            "output_tokens": 4096,
                        },
                        "policy_version": access.policy_version,
                        "expires_at": (self.clock.now() + timedelta(seconds=1800)).isoformat(),
                    }
                ),
            }
            c.execute(insert(tasks).values(**row))
            self.outbox.enqueue_text(
                c,
                target=event.conversation,
                text=f"已接单 {public_id}。可查看或取消。",
                origin_kind="command",
                origin_id=task_id,
                reply_context=event.reply_context,
            )
            return row

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
            if row["requester"] != access.principal.key and access.role not in {"admin", "owner"}:
                raise PermissionError("task belongs to another requester")
            state = row["state"]
            if state not in TERMINAL:
                state = "CANCELLED" if state in {"QUEUED", "WAITING_INPUT"} else "CANCEL_REQUESTED"
                c.execute(
                    update(tasks)
                    .where(tasks.c.id == row["id"])
                    .values(state=state, stop_reason="user_cancel")
                )
            return {"public_id": public_id, "state": state}

    def retry(self, public_id, event, access):
        with self.db.engine.connect() as c:
            old = self._visible(c, public_id, access)
            if old["state"] not in TERMINAL:
                raise ValueError("only a terminal task can be retried")
            if old["requester"] != access.principal.key and access.role not in {"admin", "owner"}:
                raise PermissionError("task belongs to another requester")
            goal, profile, task_id = old["goal"], old["profile"], old["id"]
        return self.accept(event, access, goal, profile, retry_of_task_id=task_id)

    def add_input(self, public_id, event, access, text):
        self.get(public_id, access)
        raise ValueError("this harness does not support additional input or automatic session resume")

    def recover(self):
        now = self.clock.now().isoformat()
        with self.db.transaction() as c:
            expired = select(runs.c.task_id).where(runs.c.slot_reserved == 1, runs.c.lease <= now)
            c.execute(
                update(tasks)
                .where(tasks.c.id.in_(expired), tasks.c.state.not_in(TERMINAL))
                .values(state="NEEDS_RECONCILIATION")
            )
            c.execute(
                update(tasks)
                .where(tasks.c.state == "QUEUED", tasks.c.deadline <= now)
                .values(state="EXPIRED", stop_reason="deadline")
            )

    def claim(self, worker_id):
        self.recover()
        with self.db.transaction() as c:
            if c.execute(
                select(func.count()).select_from(runs).where(runs.c.slot_reserved == 1)
            ).scalar_one():
                return None
            history = tasks.alias("history")
            last_run = (
                select(func.max(runs.c.lease))
                .where(
                    runs.c.task_id.in_(
                        select(history.c.id).where(history.c.conversation == tasks.c.conversation)
                    )
                )
                .scalar_subquery()
            )
            row = (
                c.execute(
                    select(tasks)
                    .where(tasks.c.state == "QUEUED")
                    .order_by(last_run.asc().nulls_first(), tasks.c.created_at, tasks.c.id)
                )
                .mappings()
                .first()
            )
            if not row:
                return None
            run_id = str(uuid4())
            fencing = row["fencing"] + 1
            lease = (self.clock.now() + timedelta(seconds=30)).isoformat()
            c.execute(
                update(tasks)
                .where(tasks.c.id == row["id"], tasks.c.state == "QUEUED")
                .values(state="RUNNING", fencing=fencing)
            )
            c.execute(
                insert(runs).values(
                    id=run_id,
                    task_id=row["id"],
                    worker=worker_id,
                    fencing=fencing,
                    lease=lease,
                    slot_reserved=1,
                    session=f"{run_id}/sessions/{run_id}.jsonl",
                    last_seq=0,
                )
            )
            return {
                "task_id": row["id"],
                "run_id": run_id,
                "fencing_token": fencing,
                "goal": row["goal"],
                "profile": row["profile"],
                "lease_expires_at": lease,
                "deadline": row["deadline"],
                "permission_snapshot": json.loads(row["permission_json"]),
                "session_id": run_id,
                "session_relative_path": f"{run_id}/sessions/{run_id}.jsonl",
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
            state = task["state"]
            if task["deadline"] <= self.clock.now().isoformat() and state == "RUNNING":
                state = "CANCEL_REQUESTED"
                c.execute(
                    update(tasks).where(tasks.c.id == task["id"]).values(state=state, stop_reason="deadline")
                )
            lease = (self.clock.now() + timedelta(seconds=30)).isoformat()
            c.execute(update(runs).where(runs.c.id == run["id"]).values(lease=lease))
            return {"state": state, "lease_expires_at": lease}

    def submit(self, run_id, fencing_token, worker_id, result, stopped=True):
        result = TaskResult.model_validate_json(json.dumps(result))
        if not stopped:
            raise ValueError("runner must be stopped")
        self._validate_result(result)
        with self.db.transaction() as c:
            previous = (
                c.execute(
                    select(runs).where(
                        runs.c.id == run_id,
                        runs.c.worker == worker_id,
                        runs.c.fencing == fencing_token,
                        runs.c.slot_reserved == 0,
                    )
                )
                .mappings()
                .first()
            )
            if previous:
                prior_task = (
                    c.execute(select(tasks).where(tasks.c.id == previous["task_id"])).mappings().one()
                )
                if prior_task["result_json"] == result.model_dump_json():
                    return {"state": prior_task["state"]}
                raise PermissionError("result conflicts with committed run")
            _run, task = self._lease(c, run_id, fencing_token, worker_id)
            if task["state"] != "RUNNING" or task["deadline"] <= self.clock.now().isoformat():
                raise PermissionError("task no longer running")
            state = "SUCCEEDED" if result.status == "completed" else "FAILED"
            c.execute(
                update(tasks)
                .where(tasks.c.id == task["id"])
                .values(state=state, result_json=result.model_dump_json())
            )
            c.execute(
                update(runs)
                .where(runs.c.id == run_id)
                .values(slot_reserved=0, runner_stopped_at=self.clock.now().isoformat())
            )
            event = InboundEvent.model_validate_json(task["event_json"])
            self.outbox.enqueue_text(
                c,
                target=event.conversation,
                text=f"{task['public_id']} {state}\n{result.summary}",
                origin_kind="task",
                origin_id=task["id"],
                reply_context=event.reply_context,
                ttl_seconds=86400,
            )
            return {"state": state}

    def append_event(self, run_id, fencing_token, worker_id, event_id, seq, payload):
        encoded = json.dumps(payload, ensure_ascii=False)
        if not isinstance(seq, int) or seq < 1 or len(encoded.encode()) > 2 * 1024 * 1024:
            raise ValueError("invalid event sequence or payload")
        with self.db.transaction() as c:
            run, _task = self._lease(c, run_id, fencing_token, worker_id)
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
        if result.status == "completed" and (not result.artifacts or not result.evidence):
            raise ValueError("research completion requires exported artifacts and retrieved evidence")
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
        if access.role not in {"owner", "admin"}:
            raise PermissionError("reconciliation requires a trusted administrator")
        with self.db.engine.connect() as c:
            task = self._visible(c, public_id, access)
            task_id = task["id"]
        if not self.stop_verifier(task_id, run_id):
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
            if validated and task["deadline"] <= self.clock.now().isoformat():
                raise PermissionError("expired task cannot accept success during recovery")
            recovery_id, fencing = str(uuid4()), task["fencing"] + 1
            now = self.clock.now().isoformat()
            c.execute(update(runs).where(runs.c.id == run_id).values(slot_reserved=0, runner_stopped_at=now))
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
                state = "SUCCEEDED" if validated.status == "completed" else "FAILED"
            elif task["deadline"] <= now:
                state = "EXPIRED"
            else:
                state = {"deadline": "EXPIRED", "user_cancel": "CANCELLED", "revoked": "CANCELLED"}.get(
                    task["stop_reason"], "FAILED"
                )
            c.execute(
                update(tasks)
                .where(tasks.c.id == task_id)
                .values(
                    state=state,
                    fencing=fencing,
                    result_json=validated.model_dump_json() if validated else None,
                )
            )
            if validated:
                event = InboundEvent.model_validate_json(task["event_json"])
                self.outbox.enqueue_text(
                    c,
                    target=event.conversation,
                    text=f"{task['public_id']} {state}\n{validated.summary}",
                    origin_kind="task",
                    origin_id=task_id,
                    reply_context=event.reply_context,
                    ttl_seconds=86400,
                )
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
            reason = task["stop_reason"]
            state = "EXPIRED" if reason == "deadline" else "CANCELLED" if reason else "FAILED"
            c.execute(
                update(runs)
                .where(runs.c.id == run_id)
                .values(slot_reserved=0, runner_stopped_at=self.clock.now().isoformat())
            )
            c.execute(update(tasks).where(tasks.c.id == task["id"]).values(state=state))
            return {"state": state}
