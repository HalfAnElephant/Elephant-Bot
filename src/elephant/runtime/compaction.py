"""Durable context maintenance, using the existing Leader writer and lease."""
import json
from datetime import timedelta
from uuid import uuid4

from sqlalchemy import case, func, insert, select, update

from elephant.domain import InboundEvent
from elephant.storage.agent_schema import dialogue_inputs, dialogues, leader_inbox
from elephant.storage.schema import inbox
from elephant.storage.task_schema import runs, sessions, tasks


class ContextMaintenance:
    def __init__(self, leader):
        self.leader = leader
        self.db, self.clock = leader.db, leader.clock

    def foreground(self, conn):
        if conn.scalar(select(inbox.c.id).where(inbox.c.state == "PENDING").limit(1)):
            return True
        scheduler = self.leader.sessions
        now, pending = self.clock.now(), scheduler._pending_sources(conn)
        return any(scheduler._next_user(conn, dialogue, pending, now)
                   or scheduler._next_inbox(conn, dialogue, pending, now, settle_invalid=False)
                   for dialogue in conn.execute(scheduler._pending_dialogues()).mappings())

    def claim(self, worker_id):
        capacity = getattr(self.leader, "maintenance_capacity", None)
        if capacity is not None and not capacity.maintenance_available:
            return None
        now = self.clock.now()
        with self.db.engine.connect() as conn:
            if self.foreground(conn):
                return None
            candidates = conn.execute(select(sessions).where(sessions.c.active_run_id.is_(None))).mappings().all()
        for session in candidates:
            state = json.loads(session["manifest_json"]).get("context_compaction", {})
            if not state.get("elastic_due") or (state.get("retry_at") or "") > now.isoformat():
                continue
            with self.db.transaction() as conn:
                if self.foreground(conn) or conn.scalar(select(runs.c.id).where(
                        runs.c.session_id == session["id"], runs.c.slot_reserved == 1).limit(1)):
                    return None
                dialogue = conn.execute(select(dialogues).where(dialogues.c.session_id == session["id"],
                    dialogues.c.state == "ACTIVE")).mappings().first()
                source = conn.execute(select(runs).where(runs.c.session_id == session["id"],
                    runs.c.role == "leader", runs.c.purpose != "compaction",
                    runs.c.slot_reserved == 0).order_by(runs.c.started_at.desc()).limit(1)).mappings().first()
                current = conn.execute(select(sessions).where(sessions.c.id == session["id"])).mappings().one()
                if not dialogue or not source or current["manifest_json"] != session["manifest_json"]:
                    continue
                claim = json.loads(source["request_json"])
                event = InboundEvent.model_validate_json(claim["event_json"])
                manifest = json.loads(current["manifest_json"])
                validator = self.leader.service.context_validator
                if not self.leader.core.allowed(event) or (validator and not validator(
                        self.leader.sessions.access_for_claim(claim), manifest)):
                    manifest["context_compaction"].update(elastic_due=False)
                    conn.execute(update(sessions).where(sessions.c.id == current["id"]).values(
                        manifest_json=json.dumps(manifest)))
                    continue
                run_id = str(uuid4())
                generation = (conn.scalar(select(func.max(runs.c.session_generation)).where(
                    runs.c.session_id == session["id"])) or 0) + 1
                claim.update(run_id=run_id, task_id=run_id, fencing_token=generation, purpose="compaction",
                    target_root_task_id=None, source_ids=[], task_input_ids=[], public_answer_events=False,
                    deadline=(now + timedelta(seconds=60)).isoformat(), active_seconds_remaining=60,
                    goal="", goal_spec={"text": ""})
                claim.pop("steer_until", None)
                claim.pop("memory_mutation", None)
                claim["context"] = {**claim["context"], "purpose": "compaction", "maintenance_compaction": True,
                    "source_input_ids": [], "now": now.isoformat(),
                    "published_run_ids": self.leader.sessions.published_runs(conn, dialogue["id"])}
                claim["context"]["context_failed_input_ids"] = list(conn.execute(select(dialogue_inputs.c.id).where(
                    dialogue_inputs.c.dialogue_id == dialogue["id"], dialogue_inputs.c.state == "CONTEXT_FAILED")).scalars())
                conn.execute(insert(runs).values(id=run_id, worker=worker_id, fencing=generation,
                    lease=(now + timedelta(seconds=30)).isoformat(), slot_reserved=1,
                    session=source["session"], session_id=session["id"], last_seq=0, role="leader",
                    purpose="compaction", session_generation=generation, basis_goal_revision=0,
                    request_json=json.dumps(claim), budget_json="{}", state="RUNNING",
                    started_at=now.isoformat(), accounted_at=now.isoformat()))
                conn.execute(update(sessions).where(sessions.c.id == session["id"]).values(active_run_id=run_id))
                return claim
        return None

    def operation(self, conn, run, claim, payload):
        self.leader.authorize(conn, run, claim, {"action": "task.execute"},
            compaction_saved=payload.get("phase") in {"saved", "failure", "failed"},
            reporting_failure=payload.get("phase") in {"failure", "failed"})
        session = conn.execute(select(sessions).where(sessions.c.id == claim["session_id"])).mappings().first()
        if not session:
            raise PermissionError("context maintenance needs a bound session")
        if session["active_run_id"] not in {None, run["id"]}:
            raise PermissionError("context session writer changed")
        manifest = json.loads(session["manifest_json"])
        state = manifest.setdefault("context_compaction", {})
        phase = payload.get("phase", "failure")
        if phase == "failed":
            phase = "failure"
        now = self.clock.now()
        if phase in {"begin", "commit", "saved", "request"}:
            if phase != "saved" and claim["purpose"] == "compaction" and self.foreground(conn):
                raise PermissionError("maintenance yielded to foreground input")
            if phase == "begin":
                attempt_id = str(uuid4())
                state.update({key: value for key, value in payload.items() if key != "summary"})
                state.update(attempt_id=attempt_id, run_id=run["id"], fencing=run["fencing"],
                    generation=run["session_generation"], branch_id=payload.get("branch_id"), phase="begin")
            else:
                attempt_id = payload.get("attempt_id")
                if (attempt_id != state.get("attempt_id") or state.get("run_id") != run["id"]
                        or state.get("fencing") != run["fencing"] or state.get("generation") != run["session_generation"]
                        or payload.get("branch_id") != state.get("branch_id")):
                    raise PermissionError("stale compaction attempt or branch")
                previous = state.get("phase")
                entry_id = payload.get("entry_id", payload.get("record_id"))
                if phase == "saved":
                    if not entry_id:
                        raise ValueError("native compaction record is required")
                    if previous == "saved" and state.get("entry_id") == entry_id:
                        return {"accepted": True, "attempt_id": attempt_id}
                    if previous != "commit":
                        raise PermissionError("native compaction was not authorized to commit")
                elif previous not in {"begin", "request"}:
                    raise PermissionError("compaction candidate is already closing")
                state.update({key: value for key, value in payload.items() if key != "summary"})
                state["phase"] = phase
                if phase == "saved":
                    self.saved(state, entry_id, payload.get("after_tokens"))
            result = {"accepted": True, "attempt_id": attempt_id, "session_generation": run["session_generation"],
                      "fence": run["fencing"], "run_id": run["id"], "session_id": session["id"]}
        elif phase == "recover":
            source = conn.execute(select(dialogue_inputs).where(dialogue_inputs.c.id == payload.get("input_id"),
                dialogue_inputs.c.dialogue_id == claim["context"]["dialogue_id"],
                dialogue_inputs.c.state == "CONTEXT_FAILED")).mappings().one()
            self.reset_input(conn, source)
            result = {"accepted": True}
        elif phase in {"sources", "status"}:
            tokens = payload.get("tokens", payload.get("after_tokens", 0))
            state["tokens"] = tokens
            threshold = claim["context"].get("limits", {}).get("elastic_compaction_tokens")
            state["elastic_due"] = bool(threshold and tokens >= threshold)
            result = {"accepted": True}
        elif phase == "failure":
            failure = {**payload, "recorded_at": now.isoformat(), "record_id": state.get("entry_id"), "run_id": run["id"]}
            retryable = payload.get("retryable", not payload.get("deterministic", False))
            state.update(failure=failure, retry_at=(now + timedelta(seconds=30)).isoformat())
            claim["context_failure"] = failure
            conn.execute(update(runs).where(runs.c.id == run["id"]).values(request_json=json.dumps(claim)))
            if claim["purpose"] == "user":
                for source in conn.execute(select(dialogue_inputs).where(dialogue_inputs.c.id.in_(claim["source_ids"]))).mappings():
                    context = json.loads(source["context_json"])
                    if context.get("context_failure", {}).get("run_id") == run["id"]:
                        continue
                    context["context_failure"] = failure
                    values = {"context_json": json.dumps(context)}
                    if not retryable:
                        values.update(state="CONTEXT_FAILED", handled_at=now.isoformat())
                    else:
                        count = context.get("context_retry_count", 0) + 1
                        context["context_retry_count"] = count
                        context["context_retry_at"] = (now + timedelta(seconds=min(300, 5 * 2 ** min(count, 6)))).isoformat()
                        values["context_json"] = json.dumps(context)
                        if count >= 3:
                            values.update(state="CONTEXT_FAILED", handled_at=now.isoformat())
                    conn.execute(update(dialogue_inputs).where(dialogue_inputs.c.id == source["id"]).values(**values))
                    if values.get("state") == "CONTEXT_FAILED":
                        event = InboundEvent.model_validate_json(source["event_json"])
                        if not self.leader.service.permissions.check(self.leader.sessions.access_for_claim(claim),
                                "notification.send", "conversation:" + event.conversation.key).allowed:
                            continue
                        self.leader.core.outbox.enqueue_text(conn, target=event.conversation,
                            text="这条请求的上下文暂时无法安全处理，原始输入已保留。请缩小输入后重试，或显式恢复这条请求。",
                            origin_kind="chat", origin_id="context-failure:" + source["id"],
                            reply_context=event.reply_context, requester=event.principal, ttl_seconds=86400,
                            anchor_message_id=event.platform_message_id, purpose="result")
            elif claim["purpose"] != "compaction":
                for source in conn.execute(select(leader_inbox).where(leader_inbox.c.id.in_(claim["source_ids"]))).mappings():
                    try:
                        previous = json.loads(source["output_ref"] or "{}")
                    except (ValueError, TypeError):
                        previous = {}
                    if not isinstance(previous, dict):
                        previous = {}
                    count = previous.get("retry_count", 0) + (previous.get("context_failure", {}).get("run_id") != run["id"])
                    failed = not retryable or count >= 3
                    status = {"status": "CONTEXT_FAILED" if failed else "CONTEXT_RETRY",
                        "context_failure": failure, "retry_count": count,
                        "retry_at": (now + timedelta(seconds=min(300, 5 * 2 ** min(count, 6)))).isoformat()}
                    conn.execute(update(leader_inbox).where(leader_inbox.c.id == source["id"]).values(
                        output_ref=json.dumps(status), handled_at=now.isoformat() if failed else None))
                    if failed:
                        root = conn.execute(select(tasks).where(tasks.c.id == source["root_task_id"])).mappings().one()
                        if self.leader.service.permissions.check(self.leader.service._access_for(root),
                                "notification.send", "conversation:" + root["conversation"]).allowed:
                            self.leader.service._notification(conn, root, run["id"],
                                "任务的协调上下文暂时无法安全处理，原始任务事件已保留。请缩小相关资料或补充新的处理指示。",
                                "result", "important", "context-failure:" + source["id"])
            result = {"accepted": True, "retryable": retryable}
        else:
            raise ValueError("unknown compaction phase")
        conn.execute(update(sessions).where(sessions.c.id == session["id"]).values(manifest_json=json.dumps(manifest)))
        return result

    @staticmethod
    def saved(state, entry_id, after_tokens):
        state.update(phase="saved", entry_id=entry_id, record_id=entry_id, after_tokens=after_tokens,
                     elastic_due=False, retry_at=None)
        state["saved_record"] = {key: state.get(key) for key in ("entry_id", "before_tokens", "after_tokens")}

    def recover(self, conn, run, claim, adapter, path, *, verify_saved=True):
        """Reconcile a native record only after the caller proves the writer stopped."""
        session = conn.execute(select(sessions).where(sessions.c.id == claim["session_id"])).mappings().first()
        if not session:
            return
        manifest = json.loads(session["manifest_json"])
        state = manifest.get("context_compaction", {})
        if state.get("run_id") != run["id"] or state.get("phase") not in {"begin", "commit", "saved", "request"}:
            return
        if not verify_saved and state.get("phase") != "commit":
            return
        if state.get("fencing") != run["fencing"] or state.get("generation") != run["session_generation"]:
            raise ValueError("compaction recovery fence changed")
        records = adapter.compaction_records(path)
        matching = [record for record in records if
                    ((record.get("details") or {}).get("elephant_compaction") or {}).get("attempt_id")
                    == state.get("attempt_id")]
        if len(matching) > 1:
            raise ValueError("ambiguous native compaction records")
        if matching:
            record = matching[0]
            provenance = record["details"]["elephant_compaction"]
            if (state.get("phase") not in {"commit", "saved"}
                    or provenance.get("branch_id") != state.get("branch_id")
                    or provenance.get("session_generation") != run["session_generation"]
                    or provenance.get("fence") != run["fencing"]
                    or provenance.get("run_id") != run["id"]
                    or provenance.get("session_id") != session["id"]
                    or record.get("parentId") != state.get("branch_id")
                    or record.get("firstKeptEntryId") != state.get("first_kept_entry_id")):
                raise ValueError("native compaction branch or retention boundary changed")
            event = InboundEvent.model_validate_json(claim["event_json"])
            validator = getattr(self.leader.core, "validate_context", None)
            access = self.leader.sessions.access_for_claim(claim)
            if not self.leader.core.allowed(event) or (validator and not validator(access, manifest)):
                state.update(phase="invalid", elastic_due=False, failure={"reason": "context_sources_revoked"})
                conn.execute(update(sessions).where(sessions.c.id == session["id"]).values(manifest_json=json.dumps(manifest)))
                return
            self.saved(state, record["id"], provenance.get("after_tokens"))
        elif state.get("phase") == "saved":
            raise ValueError("saved native compaction record is missing")
        else:
            state.update(phase="failed", retry_at=(self.clock.now() + timedelta(seconds=30)).isoformat())
        conn.execute(update(sessions).where(sessions.c.id == session["id"]).values(manifest_json=json.dumps(manifest)))

    def settle(self, conn, run, claim):
        session = conn.execute(select(sessions).where(sessions.c.id == claim["session_id"])).mappings().first()
        if not session:
            return
        manifest = json.loads(session["manifest_json"])
        state = manifest.setdefault("context_compaction", {})
        if state.get("phase") != "saved":
            state.update(phase="failed", retry_at=(self.clock.now() + timedelta(seconds=30)).isoformat())
            conn.execute(update(sessions).where(sessions.c.id == session["id"]).values(manifest_json=json.dumps(manifest)))

    @staticmethod
    def _require_stopped(conn, source):
        if source["claimed_attempt_id"] and conn.scalar(select(runs.c.slot_reserved).where(
                runs.c.id == source["claimed_attempt_id"])):
            raise PermissionError("original input writer has not stopped")

    def reset_input(self, conn, source):
        self._require_stopped(conn, source)
        context = json.loads(source["context_json"])
        for key in ("context_failure", "context_retry_count", "context_retry_at"):
            context.pop(key, None)
        turn = context.get("leader_turn", {})
        now = self.clock.now()
        turn["deadline"] = (now + timedelta(seconds=turn["plan"]["budget"]["total_wall_seconds"])).isoformat()
        if self.leader.sessions.turn_exhausted(turn, now):
            raise PermissionError("original input execution budget exhausted")
        conn.execute(update(dialogue_inputs).where(dialogue_inputs.c.id == source["id"]).values(
            state="PENDING", handled_at=None, claimed_attempt_id=None, context_json=json.dumps(context)))

    @staticmethod
    def inbox_failure(source):
        try:
            status = json.loads(source["output_ref"] or "{}")
        except (ValueError, TypeError):
            return None
        if isinstance(status, dict) and status.get("status") == "CONTEXT_FAILED":
            return status.get("context_failure")

    def reset_inbox(self, conn, source):
        self._require_stopped(conn, source)
        if not self.inbox_failure(source):
            raise PermissionError("coordination event is not awaiting recovery")
        target = conn.execute(select(tasks.c.state, dialogues.c.session_id, dialogues.c.state.label("dialogue_state"))
            .join(dialogues, tasks.c.origin_dialogue_id == dialogues.c.id)
            .where(tasks.c.id == source["root_task_id"])).mappings().one_or_none()
        from .task_graph import TERMINAL
        if not target or target["dialogue_state"] != "ACTIVE" or target["state"] in TERMINAL | {"CANCEL_REQUESTED"}:
            raise PermissionError("coordination event no longer has an active task")
        conn.execute(update(leader_inbox).where(leader_inbox.c.id == source["id"]).values(
            target_session_id=target["session_id"], handled_at=None, claimed_attempt_id=None, output_ref=None))

    def refresh_failed(self):
        """Only a demonstrably larger budget or a newer shrinking summary permits replay."""
        settings = self.leader.sessions.settings
        limits = settings.context.limits(settings.model.context_window,
            settings.execution.interactive_budget.output_tokens_per_call)
        with self.db.engine.connect() as conn:
            failed_inputs = conn.execute(select(dialogue_inputs, dialogues.c.session_id).join(
                dialogues, dialogues.c.id == dialogue_inputs.c.dialogue_id).where(
                dialogue_inputs.c.state == "CONTEXT_FAILED")).mappings().all()
            output = case((func.json_valid(leader_inbox.c.output_ref), leader_inbox.c.output_ref), else_="{}")
            failed_events = conn.execute(select(leader_inbox, dialogues.c.session_id)
                .join(tasks, tasks.c.id == leader_inbox.c.root_task_id)
                .join(dialogues, dialogues.c.id == tasks.c.origin_dialogue_id)
                .where(func.json_extract(output, "$.status") == "CONTEXT_FAILED")).mappings().all()
        for source in (*failed_inputs, *failed_events):
            is_user = "context_json" in source
            table = dialogue_inputs if is_user else leader_inbox
            field = "context_json" if is_user else "output_ref"
            failure = (json.loads(source[field]).get("context_failure") if is_user else self.inbox_failure(source)) or {}
            try:
                model, window, force, byte_limit = failure["limits_fingerprint"].rsplit(":", 3)
                window, force, byte_limit = int(window), int(force), int(byte_limit)
                tokens, size = int(failure["tokens"]), int(failure["request_bytes"])
            except (KeyError, ValueError, TypeError):
                continue
            new_force = limits["force_compaction_tokens"]
            enlarged = (model == settings.model.model and settings.model.context_window == window
                and new_force >= force and limits["max_bytes"] >= byte_limit
                and (new_force > force or limits["max_bytes"] > byte_limit)
                and tokens < new_force and size <= limits["max_bytes"])
            with self.db.transaction() as conn:
                current = conn.execute(select(table).where(table.c.id == source["id"])).mappings().one_or_none()
                if not current or current[field] != source[field] or (is_user and current["state"] != "CONTEXT_FAILED"):
                    continue
                raw = conn.scalar(select(sessions.c.manifest_json).where(sessions.c.id == source["session_id"]))
                state = json.loads(raw).get("context_compaction", {}) if raw else {}
                saved_record = state.get("saved_record") or (state if state.get("phase") == "saved" else {})
                reduction = (saved_record.get("before_tokens") or 0) - (saved_record.get("after_tokens") or 0)
                shrunk = (saved_record.get("entry_id") and saved_record["entry_id"] != failure.get("record_id")
                    and reduction > 0 and tokens - reduction < new_force and size <= limits["max_bytes"])
                if enlarged or shrunk:
                    try:
                        (self.reset_input if is_user else self.reset_inbox)(conn, current)
                    except PermissionError:
                        continue
