"""Core-owned, persisted bridge from public Pi answers to QQ native streams.

Workers never receive QQ credentials. Only ordered public answer events opt in at
claim time. Each write has a committed SENDING intent; an interrupted write is
UNKNOWN and cannot cause an automatic full-answer resend. Final task output is
held until the transport confirms the final public answer was delivered.
"""
import asyncio
import json
from datetime import datetime, timedelta
from uuid import uuid4

from sqlalchemy import func, insert, select, update

from qqbot.adapters.qq_official.streaming import OfficialStream, send_typing
from qqbot.domain import InboundEvent, OutboundEnvelope, Surface, stable_id
from qqbot.domain.tasks import validate_user_facing_text
from qqbot.storage.schema import outbox, reply_counters
from qqbot.storage.stream_schema import stream_messages, stream_runs
from qqbot.storage.task_schema import events, notifications, runs, tasks

PUBLIC_KINDS = frozenset({"public_answer_start", "public_answer_delta", "public_answer_end", "public_answer_final"})


def validate_public_event(payload):
    if payload.get("kind") not in PUBLIC_KINDS:
        return
    if set(payload) - {"kind", "pi_type", "message_id", "text", "error"}:
        raise ValueError("unexpected public answer fields")
    if not isinstance(payload.get("message_id"), str) or not 1 <= len(payload["message_id"]) <= 256:
        raise ValueError("public answer requires bounded message identity")
    if payload.get("pi_type", payload["kind"]) != payload["kind"]:
        raise ValueError("public answer type mismatch")
    text = payload.get("text", "")
    if not isinstance(text, str) or len(text) > 10000:
        raise ValueError("public answer exceeds text budget")
    if not isinstance(payload.get("error", False), bool):
        raise ValueError("invalid public answer error flag")  # noqa: TRY004 - API reports malformed events as 422.
    validate_user_facing_text(text)


class StreamDelivery:
    def __init__(self, core):
        self.core, self.db, self.clock = core, core.db, core.clock
        self.lock = asyncio.Lock()
        self.live = {}

    def eligible(self, event):
        adapter = self.core.adapters.get(event.conversation.adapter_instance_id)
        config = getattr(adapter, "config", None)
        return bool(config and config.enabled and getattr(config, "kind", None) == "official"
                    and (getattr(config, "streaming_enabled", False) or getattr(config, "typing_enabled", False))
                    and event.conversation.surface == Surface.PRIVATE
                    and event.reply_context and event.reply_context.expires_at
                    and event.reply_context.expires_at > self.clock.now())

    def recover(self):
        """Only writes in flight are uncertain; acknowledged prefixes can resume."""
        with self.db.transaction() as c:
            uncertain = select(stream_messages.c.run_id).where(stream_messages.c.state == "SENDING",
                stream_messages.c.kind == "answer")
            c.execute(update(stream_runs).where(stream_runs.c.run_id.in_(uncertain)).values(
                state="UNKNOWN", error="INTERRUPTED_SEND"))
            c.execute(update(stream_messages).where(stream_messages.c.state == "SENDING").values(
                state="UNKNOWN", error="INTERRUPTED_SEND"))

    def _disable(self, run_id, reason, *, unknown=False):
        with self.db.transaction() as c:
            c.execute(update(stream_runs).where(stream_runs.c.run_id == run_id).values(
                state="UNKNOWN" if unknown else "DISABLED", error=reason))

    def hold_result(self, conn, run_id, origin_kind, origin_id):
        gate = conn.execute(update(stream_runs).where(stream_runs.c.run_id == run_id).values(
            result_origin_kind=origin_kind, result_origin_id=origin_id))
        if gate.rowcount:
            conn.execute(update(outbox).where(outbox.c.origin_kind == origin_kind,
                outbox.c.origin_id == origin_id, outbox.c.state == "PENDING").values(state="STREAM_PENDING"))

    def _source(self, conn, run):
        if run["role"] == "leader":
            claim = json.loads(run["request_json"])
            task_id = claim.get("target_root_task_id")
            event_json = claim["event_json"]
        else:
            task_id = run["task_id"]
            event_json = None
        task = conn.execute(select(tasks).where(tasks.c.id == task_id)).mappings().one() if task_id else None
        return InboundEvent.model_validate_json(event_json or task["event_json"]), task

    def _create(self, run_id, message_key, *, kind="answer"):
        with self.db.transaction() as c:
            old = c.execute(select(stream_messages).where(stream_messages.c.run_id == run_id,
                stream_messages.c.message_key == message_key, stream_messages.c.kind == kind)).mappings().first()
            if old:
                return dict(old)
            run = c.execute(select(runs).where(runs.c.id == run_id)).mappings().one()
            event, task = self._source(c, run)
            reply = event.reply_context
            if not self.eligible(event) or (task and task["state"] in {"CANCELLED", "CANCEL_REQUESTED", "NEEDS_RECONCILIATION"}):
                raise ValueError("stream is no longer eligible")
            counter_key = stable_id(event.conversation.key, reply.platform_message_id)
            seq = c.execute(select(reply_counters.c.seq).where(reply_counters.c.key == counter_key)).scalar_one_or_none()
            next_seq = (seq or 0) + 1
            limit = reply.max_replies - 1 if kind == "typing" else reply.max_replies
            if next_seq > limit:
                raise ValueError("stream reply budget exhausted")
            if seq is None:
                c.execute(insert(reply_counters).values(key=counter_key, seq=next_seq))
            else:
                c.execute(update(reply_counters).where(reply_counters.c.key == counter_key).values(seq=next_seq))
            ident = stable_id("qq-stream", run_id, message_key, kind)
            notice_seq = 0
            if task:
                notice_seq = (c.execute(select(func.max(notifications.c.sequence)).where(
                    notifications.c.task_id == task["id"])).scalar_one() or 0) + 1
                c.execute(insert(notifications).values(id=ident, task_id=task["id"], run_id=run_id,
                    sequence=notice_seq, purpose="progress", importance="normal", body="",
                    created_at=self.clock.now().isoformat(), state="STREAMING"))
            envelope = OutboundEnvelope(delivery_id=str(uuid4()), target=event.conversation, text="stream",
                reply_context=reply.model_copy(update={"msg_seq": next_seq}),
                origin_kind="task" if task else "chat", origin_id=ident if task else "qq-stream:" + ident,
                expires_at=min(reply.expires_at, self.clock.now() + timedelta(hours=1)),
                requester=event.principal, anchor_message_id=event.platform_message_id,
                parent_task_id=task["id"] if task else None,
                notification_sequence=notice_seq, purpose="progress")
            row = {"id": ident, "run_id": run_id, "message_key": message_key, "kind": kind, "state": "NEW", "text": "", "sent_text": "",
                       "stream_id": None, "next_index": 0, "final_marker": 0, "envelope_json": envelope.model_dump_json(),
                       "error": None, "created_at": self.clock.now().isoformat()}
            c.execute(insert(stream_messages).values(**row))
            return row

    def _transport(self, row):
        envelope = OutboundEnvelope.model_validate_json(row["envelope_json"])
        adapter = self.core.adapters.get(envelope.target.adapter_instance_id)
        if adapter is None or not adapter.config.enabled:
            self.live.pop(row["id"], None)
            raise ValueError("adapter unavailable")
        if row["id"] in self.live:
            cached = self.live[row["id"]]
            if cached.adapter is adapter:
                return cached
            self.live.pop(row["id"], None)
        transport = OfficialStream(adapter, envelope,
            content_type="markdown" if adapter.config.markdown_enabled else "text",
            authorize=lambda: (self.core.adapters.get(envelope.target.adapter_instance_id) is adapter
                               and adapter.config.enabled and self.core.pre_send(envelope)))
        transport.text, transport.sent_text = row["text"], row["sent_text"]
        transport.stream_id, transport.index = row["stream_id"], row["next_index"]
        transport.remain_msg_len = row.get("remain_msg_len")
        transport.state = row["state"]
        self.live[row["id"]] = transport
        return transport

    async def _send(self, row, text, *, final=False, cancelled=False):
        envelope = OutboundEnvelope.model_validate_json(row["envelope_json"])
        if not self.core.pre_send(envelope):
            self._disable(row["run_id"], "AUTHORIZATION_CHANGED")
            return
        transport = self._transport(row)
        if transport.state in {"UNKNOWN", "FAILED", "WAITING_FOR_TRIGGER", "CANCELLED"}:
            self._disable(row["run_id"], transport.state, unknown=transport.state == "UNKNOWN")
            return
        if not text.startswith(transport.text):
            self._disable(row["run_id"], "PUBLIC_ANSWER_REPLACED")
            return
        with self.db.transaction() as c:
            c.execute(update(stream_messages).where(stream_messages.c.id == row["id"]).values(
                state="SENDING", text=text))
        try:
            # Keep the intended cumulative payload intact without double-appending
            # after a coalesced event or a recovered acknowledged prefix.
            transport.text = text
            receipt = await (transport.cancel() if cancelled else transport.finish() if final else transport.flush())
        except asyncio.CancelledError:
            self._disable(row["run_id"], "SEND_CANCELLED", unknown=True)
            raise
        except Exception:  # noqa: BLE001 - A transport exception can follow an external side effect.
            self._disable(row["run_id"], "SEND_EXCEPTION", unknown=True)
            return
        with self.db.transaction() as c:
            c.execute(update(stream_messages).where(stream_messages.c.id == row["id"]).values(
                state=transport.state, sent_text=transport.sent_text, text=transport.text,
                stream_id=transport.stream_id, next_index=transport.index, remain_msg_len=transport.remain_msg_len,
                error=receipt.error_code if receipt else None))
            c.execute(update(notifications).where(notifications.c.id == row["id"]).values(
                body=transport.sent_text, state="DELIVERED" if transport.state == "FINISHED" else "STREAMING"))
        if receipt and receipt.state != "SENT":
            self._disable(row["run_id"], receipt.error_code or receipt.state, unknown=receipt.state == "UNKNOWN")

    async def _event(self, gate, item):
        payload = json.loads(item["payload"])
        kind = payload.get("kind")
        if kind not in PUBLIC_KINDS:
            return
        validate_public_event(payload)
        message_key = payload["message_id"]
        text = payload.get("text", "")
        if kind == "public_answer_start":
            return  # Do not allocate scarce reply budget for an empty/tool-only message.
        with self.db.engine.connect() as c:
            run = c.execute(select(runs).where(runs.c.id == gate["run_id"])).mappings().one()
            event, _task = self._source(c, run)
        config = self.core.adapters[event.conversation.adapter_instance_id].config
        if not config.streaming_enabled:
            return
        if not text:
            if payload.get("error"):
                self._disable(gate["run_id"], "PUBLIC_ANSWER_ERROR")
            return
        row = self._create(gate["run_id"], message_key)
        if kind == "public_answer_final":
            # Only agent_settled identifies which ended message is the final answer.
            if text != row["text"]:
                raise ValueError("final marker does not match public answer")
            with self.db.transaction() as c:
                c.execute(update(stream_messages).where(stream_messages.c.id == row["id"]).values(final_marker=1))
            return
        if row["state"] == "FINISHED":
            if text != row["text"]:
                raise ValueError("ended answer changed")
            return
        if kind == "public_answer_end" and not text:
            return
        await self._send(row, text, final=kind == "public_answer_end", cancelled=payload.get("error", False))

    def _resolve_result(self, run_id):
        with self.db.transaction() as c:
            gate = c.execute(select(stream_runs).where(stream_runs.c.run_id == run_id)).mappings().one()
            run = c.execute(select(runs).where(runs.c.id == run_id)).mappings().one()
            if not run["result_json"]:
                if run["state"] == "RECOVERED" and not run["slot_reserved"]:
                    c.execute(update(stream_runs).where(stream_runs.c.run_id == run_id).values(
                        state="RECONCILIATION" if gate["state"] == "UNKNOWN" else "DONE"))
                    self._drop_live(run_id)
                return
            # Never resolve before all spooled events acknowledged before submit have been processed.
            if gate["state"] == "ACTIVE" and gate["cursor_seq"] < run["last_seq"]:
                return
            result = json.loads(run["result_json"])
            origin_id = gate["result_origin_id"]
            origin_kind = gate["result_origin_kind"]
            if origin_id is None:
                # Read streams created before the final-delivery binding was added.
                target = c.execute(select(notifications.c.id).where(
                    notifications.c.id == "result:" + run_id)).scalar_one_or_none()
                origin_id, origin_kind = target, "task"
            if origin_id is None:
                if gate["state"] in {"UNKNOWN", "RECONCILIATION"}:
                    c.execute(update(stream_runs).where(stream_runs.c.run_id == run_id).values(state="RECONCILIATION"))
                    self._drop_live(run_id)
                    return
                # Wait/cancel/failure-control runs may legitimately have no final notice.
                # Their public streams must not remain active indefinitely.
                c.execute(update(stream_runs).where(stream_runs.c.run_id == run_id).values(state="DONE"))
                self._drop_live(run_id)
                return
            pending = (outbox.c.origin_id == origin_id) & (outbox.c.origin_kind == origin_kind)
            if gate["state"] in {"UNKNOWN", "RECONCILIATION"}:
                c.execute(update(outbox).where(pending, outbox.c.state == "STREAM_PENDING").values(
                    state="UNKNOWN", receipt_json=json.dumps({"state": "UNKNOWN", "error_code": "STREAM_RECEIPT_UNCERTAIN"})))
                c.execute(update(stream_runs).where(stream_runs.c.run_id == run_id).values(state="RECONCILIATION"))
                self._drop_live(run_id)
                return
            final = c.execute(select(stream_messages).where(stream_messages.c.run_id == run_id,
                stream_messages.c.final_marker == 1, stream_messages.c.state == "FINISHED",
                stream_messages.c.sent_text == result["summary"])).mappings().first()
            if final:
                c.execute(update(outbox).where(pending, outbox.c.state == "STREAM_PENDING").values(
                    state="SENT", platform_message_id=final["stream_id"], sent_at=self.clock.now().isoformat()))
                if origin_kind == "task":
                    c.execute(update(notifications).where(notifications.c.id == origin_id).values(state="DELIVERED"))
            else:
                unused = c.execute(select(stream_messages.c.envelope_json).where(
                    stream_messages.c.run_id == run_id, stream_messages.c.kind == "answer",
                    stream_messages.c.stream_id.is_(None), stream_messages.c.state == "CANCELLED")
                    .order_by(stream_messages.c.created_at).limit(1)).scalar_one_or_none()
                first = c.execute(select(outbox).where(pending, outbox.c.state == "STREAM_PENDING")
                    .order_by(outbox.c.part_no).limit(1)).mappings().first()
                if unused and first:
                    reserved = OutboundEnvelope.model_validate_json(unused).reply_context
                    envelope = OutboundEnvelope.model_validate_json(first["envelope_json"])
                    if (reserved and envelope.reply_context and not envelope.reply_context.msg_seq
                            and reserved.platform_message_id == envelope.reply_context.platform_message_id):
                        # A rejected or unsent stream leaves its sequence available for the fallback.
                        envelope = envelope.model_copy(update={"reply_context": reserved})
                        c.execute(update(outbox).where(outbox.c.id == first["id"]).values(
                            envelope_json=envelope.model_dump_json()))
                c.execute(update(outbox).where(pending, outbox.c.state == "STREAM_PENDING").values(state="PENDING"))
            c.execute(update(stream_runs).where(stream_runs.c.run_id == run_id).values(state="DONE"))
            self._drop_live(run_id)

    def _drop_live(self, run_id):
        with self.db.engine.connect() as conn:
            ids = conn.execute(select(stream_messages.c.id).where(stream_messages.c.run_id == run_id)).scalars().all()
        for ident in ids:
            self.live.pop(ident, None)

    async def _close_if_settled(self, run_id):
        """Close only acknowledged prefixes; never claim local teardown closed QQ."""
        with self.db.engine.connect() as c:
            gate = c.execute(select(stream_runs).where(stream_runs.c.run_id == run_id)).mappings().one()
            run = c.execute(select(runs).where(runs.c.id == run_id)).mappings().one()
            _event, task = self._source(c, run)
            settled = bool(run["result_json"]) and gate["cursor_seq"] >= run["last_seq"]
            stop = ((task and task["state"] in {"CANCELLED", "CANCEL_REQUESTED", "EXPIRED", "NEEDS_RECONCILIATION", "FAILED"})
                    or run["state"] in {"CANCELLED", "NEEDS_RECONCILIATION", "FAILED", "RECOVERED"})
            if not (settled or stop or gate["state"] != "ACTIVE"):
                return
            rows = [dict(row) for row in c.execute(select(stream_messages).where(
                stream_messages.c.run_id == run_id, stream_messages.c.kind == "answer",
                stream_messages.c.state.not_in(["FINISHED", "CANCELLED", "ABANDONED", "UNKNOWN"]))).mappings()]
        for row in rows:
            if row["state"] == "SENDING":
                self._disable(run_id, "INTERRUPTED_SEND", unknown=True)
                continue
            envelope = OutboundEnvelope.model_validate_json(row["envelope_json"])
            if not row["stream_id"]:
                with self.db.transaction() as c:
                    c.execute(update(stream_messages).where(stream_messages.c.id == row["id"]).values(state="CANCELLED"))
                continue
            try:
                transport = self._transport(row)
            except ValueError:
                # A removed/disabled instance cannot close its persisted remote stream.
                with self.db.transaction() as c:
                    c.execute(update(stream_messages).where(stream_messages.c.id == row["id"]).values(
                        state="UNKNOWN", error="CLOSE_ADAPTER_UNAVAILABLE"))
                    c.execute(update(notifications).where(notifications.c.id == row["id"]).values(state="UNKNOWN"))
                self._disable(run_id, "CLOSE_ADAPTER_UNAVAILABLE", unknown=True)
                continue
            transport.authorize = lambda e=envelope, a=transport.adapter: (
                self.core.adapters.get(e.target.adapter_instance_id) is a and a.config.enabled
                and self.core.pre_send(e, stream_cleanup=True))
            # A known rejection is safe to replace with one close request, not a replay.
            transport.state = "OPEN"
            transport.text = transport.sent_text = row["sent_text"]
            with self.db.transaction() as c:
                c.execute(update(stream_messages).where(stream_messages.c.id == row["id"]).values(state="SENDING"))
            try:
                receipt = await transport.cancel()
            except asyncio.CancelledError:
                self._disable(run_id, "CLOSE_INTERRUPTED", unknown=True)
                raise
            except Exception:  # noqa: BLE001 - A close may have reached QQ.
                receipt = None
            closed = bool(receipt and receipt.state == "SENT")
            with self.db.transaction() as c:
                c.execute(update(stream_messages).where(stream_messages.c.id == row["id"]).values(
                    state="CANCELLED" if closed else "UNKNOWN", text=transport.sent_text,
                    sent_text=transport.sent_text, next_index=transport.index,
                    error=None if closed else "STREAM_CLOSE_UNCONFIRMED"))
                c.execute(update(notifications).where(notifications.c.id == row["id"]).values(
                    state="DELIVERED" if closed else "UNKNOWN", body=transport.sent_text))
            if not closed:
                self._disable(run_id, "STREAM_CLOSE_UNCONFIRMED", unknown=True)
        if stop:
            with self.db.transaction() as c:
                state = c.execute(select(stream_runs.c.state).where(stream_runs.c.run_id == run_id)).scalar_one()
                c.execute(update(stream_runs).where(stream_runs.c.run_id == run_id).values(
                    state="UNKNOWN" if state == "UNKNOWN" else "DISABLED"))
            self._drop_live(run_id)

    async def _typing(self, gate):
        with self.db.engine.connect() as c:
            run = c.execute(select(runs).where(runs.c.id == gate["run_id"])).mappings().one()
            event, task = self._source(c, run)
            old = c.execute(select(stream_messages).where(stream_messages.c.run_id == gate["run_id"],
                stream_messages.c.kind == "typing").order_by(stream_messages.c.created_at.desc())).mappings().first()
            count = c.execute(select(func.count()).select_from(stream_messages).where(
                stream_messages.c.run_id == gate["run_id"], stream_messages.c.kind == "typing")).scalar_one()
            answer = c.execute(select(stream_messages.c.id).where(
                stream_messages.c.run_id == gate["run_id"], stream_messages.c.kind == "answer")).first()
        adapter = self.core.adapters.get(event.conversation.adapter_instance_id)
        if (not adapter or not adapter.config.typing_enabled or run["state"] != "RUNNING"
                or (task and task["state"] in {"CANCELLED", "CANCEL_REQUESTED", "EXPIRED", "NEEDS_RECONCILIATION", "FAILED"})
                or answer or not self.eligible(event)):
            return
        # Reserve at least one passive reply for the answer. Never retry uncertain status.
        if count >= event.reply_context.max_replies - 1 or (old and (
            old["state"] != "SENT" or datetime.fromisoformat(old["created_at"]) + timedelta(seconds=55) > self.clock.now()
        )):
            return
        try:
            row = self._create(gate["run_id"], f"typing:{count}", kind="typing")
        except ValueError:
            return
        envelope = OutboundEnvelope.model_validate_json(row["envelope_json"])
        if not self.core.pre_send(envelope):
            return
        with self.db.transaction() as c:
            c.execute(update(stream_messages).where(stream_messages.c.id == row["id"]).values(state="SENDING"))
        # This status message is never retried. A lost typing receipt does not
        # make a later, separately sequenced answer uncertain.
        try:
            receipt = await send_typing(adapter, envelope, seconds=60,
                                        authorize=lambda: self.core.pre_send(envelope))
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - Persist uncertain outcome of a status write.
            receipt = None
        with self.db.transaction() as c:
            c.execute(update(stream_messages).where(stream_messages.c.id == row["id"]).values(
                state=receipt.state if receipt else "UNKNOWN", error=receipt.error_code if receipt else "SEND_EXCEPTION"))

    async def dispatch_once(self):
        async with self.lock:
            with self.db.engine.connect() as c:
                gates = [dict(row) for row in c.execute(select(stream_runs).where(
                    stream_runs.c.state.in_(["ACTIVE", "UNKNOWN", "DISABLED"]))).mappings()]
            for gate in gates:
                with self.db.engine.connect() as c:
                    run = c.execute(select(runs).where(runs.c.id == gate["run_id"])).mappings().one()
                    _event, task = self._source(c, run)
                    terminal = run["state"] != "RUNNING"
                    stopping = ((task and task["state"] in {"CANCELLED", "CANCEL_REQUESTED", "EXPIRED", "NEEDS_RECONCILIATION"})
                                or (task and task["state"] == "FAILED" and not run["result_json"])
                                or run["state"] in {"CANCELLED", "NEEDS_RECONCILIATION", "FAILED", "RECOVERED"})
                if gate["state"] == "ACTIVE" and not stopping:
                    with self.db.engine.connect() as c:
                        items = [dict(row) for row in c.execute(select(events).where(events.c.run_id == gate["run_id"],
                            events.c.seq > gate["cursor_seq"]).order_by(events.c.seq).limit(200)).mappings()]
                    for item in items:
                        try:
                            await self._event(gate, item)
                        except (ValueError, KeyError, TypeError):
                            self._disable(gate["run_id"], "INVALID_PUBLIC_ANSWER")
                        with self.db.transaction() as c:
                            c.execute(update(stream_runs).where(stream_runs.c.run_id == gate["run_id"]).values(
                                cursor_seq=item["seq"]))
                            state = c.execute(select(stream_runs.c.state).where(
                                stream_runs.c.run_id == gate["run_id"])).scalar_one()
                        if state != "ACTIVE":
                            break
                    with self.db.engine.connect() as c:
                        state = c.execute(select(stream_runs.c.state).where(
                            stream_runs.c.run_id == gate["run_id"])).scalar_one()
                        buffered = [dict(row) for row in c.execute(select(stream_messages).where(
                            stream_messages.c.run_id == gate["run_id"], stream_messages.c.kind == "answer",
                            stream_messages.c.state.in_(["NEW", "OPEN"]),
                            stream_messages.c.text != stream_messages.c.sent_text)).mappings()]
                    if state == "ACTIVE":
                        for row in buffered:
                            await self._send(row, row["text"])
                        if not terminal:
                            await self._typing(gate)
                await self._close_if_settled(gate["run_id"])
                self._resolve_result(gate["run_id"])
