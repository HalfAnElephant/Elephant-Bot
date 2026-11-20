import asyncio
from contextlib import nullcontext
from datetime import datetime, timedelta
from random import Random
from uuid import uuid4

from sqlalchemy import and_, func, insert, or_, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from elephant.adapters.qq_official.streaming import OfficialStream
from elephant.domain import OutboundEnvelope, SendReceipt, Surface, stable_id
from elephant.storage.agent_schema import dialogue_inputs, leader_events
from elephant.storage.schema import outbox, reply_counters
from elephant.storage.stream_schema import next_delivery_sequence, stream_messages, stream_runs

from .presentation import chat_bubbles, plain_text, split_text

MEDIA_UPLOAD_MAX_ATTEMPTS = 3


class Outbox:
    def __init__(self, db, clock, *, markdown_adapters=()):
        self.db, self.clock = db, clock
        self.markdown_adapters = frozenset(markdown_adapters)

    def enqueue(self, c, envelope: OutboundEnvelope, *, defer_reply=False) -> str:
        # Rich payloads are atomic. Never duplicate a keyboard or media by text splitting.
        if envelope.content_format == "markdown" or envelope.official_content is not None:
            return self._enqueue_one(c, envelope, defer_reply=defer_reply)
        text = envelope.text if envelope.plain_text else plain_text(envelope.text)
        deliveries = []
        for offset, part in enumerate(split_text(text)):
            deliveries.append(self._enqueue_one(c, envelope.model_copy(update={
                "text": part, "plain_text": True, "part_no": envelope.part_no + offset,
                "delivery_id": envelope.delivery_id if offset == 0 else str(uuid4()),
            }), defer_reply=defer_reply))
        return deliveries[0]

    def _enqueue_one(self, c, envelope, *, defer_reply=False):
        key = stable_id(
            envelope.origin_kind,
            envelope.origin_id,
            envelope.target.key,
            str(envelope.part_no),
            str(envelope.content_version),
        )
        old = c.execute(select(outbox.c.id).where(outbox.c.dedupe_key == key)).scalar()
        if old:
            return old
        if (envelope.origin_kind == "chat" and envelope.purpose == "result" and not envelope.fixed_publication
                and envelope.origin_id.startswith("turn:")):
            prefix = ":".join(envelope.origin_id.split(":", 2)[:2]) + ":"
            c.execute(update(outbox).where(outbox.c.conversation_id == envelope.target.key,
                outbox.c.origin_kind == "chat", outbox.c.origin_id.startswith(prefix),
                outbox.c.origin_id != envelope.origin_id,
                func.json_extract(outbox.c.envelope_json, "$.purpose") == "result",
                func.json_extract(outbox.c.envelope_json, "$.text") == envelope.text,
                func.json_extract(outbox.c.envelope_json, "$.fixed_publication").is_(None),
                outbox.c.state.in_(["PENDING", "RETRYABLE", "WAITING_FOR_TRIGGER"]))
                .values(state="SUPERSEDED"))
        if defer_reply and envelope.reply_context:
            envelope = envelope.model_copy(update={
                "reply_context": envelope.reply_context.model_copy(update={"msg_seq": None})})
        else:
            envelope = self._reserve_reply(c, envelope)
        c.execute(
            insert(outbox).values(
                id=envelope.delivery_id,
                dedupe_key=key,
                conversation_id=envelope.target.key,
                envelope_json=envelope.model_dump_json(),
                state="PENDING",
                attempts=0,
                created_at=self.clock.now().isoformat(),
                enqueue_seq=next_delivery_sequence(),
                origin_id=envelope.origin_id,
                origin_kind=envelope.origin_kind,
                part_no=envelope.part_no,
                task_id=envelope.parent_task_id,
                notification_sequence=envelope.notification_sequence,
            )
        )
        return envelope.delivery_id

    def _reserve_reply(self, c, envelope):
        reply = envelope.reply_context
        if reply and reply.expires_at and reply.expires_at > self.clock.now():
            counter_key = stable_id(envelope.target.key, reply.platform_message_id)
            seq = c.execute(select(reply_counters.c.seq).where(reply_counters.c.key == counter_key)).scalar()
            next_seq = (seq or 0) + 1
            limit = reply.max_replies - int(envelope.purpose in {"progress", "status"})
            if next_seq > limit:
                return envelope.model_copy(update={"reply_context": reply.model_copy(update={"msg_seq": None})})
            if seq is None:
                c.execute(insert(reply_counters).values(key=counter_key, seq=next_seq))
            else:
                c.execute(
                    update(reply_counters).where(reply_counters.c.key == counter_key).values(seq=next_seq)
                )
            envelope = envelope.model_copy(
                update={"reply_context": reply.model_copy(update={"msg_seq": next_seq})}
            )
        elif reply:
            envelope = envelope.model_copy(update={"reply_context": reply.model_copy(update={"msg_seq": None})})
        return envelope

    def enqueue_text(
        self,
        c,
        *,
        target,
        text,
        origin_kind,
        origin_id,
        reply_context=None,
        ttl_seconds=300,
        allow_proactive=False,
        context_version=None,
        persona_version=None,
        policy_version=None,
        anchor_message_id=None,
        requester=None,
        parent_task_id=None,
        notification_sequence=0,
        purpose=None,
        defer_reply=False,
        fixed_publication=False,
    ):
        ids = []
        if (target.adapter_instance_id in self.markdown_adapters and target.surface in {"private", "group", "guild_channel", "guild_private"}
                and text.strip() and len(text) <= 10000):
            # Preserve the complete Markdown unit. Oversize output is explicitly rendered
            # as text below rather than silently truncating or breaking a code fence.
            return [self.enqueue(c, OutboundEnvelope(
                    delivery_id=str(uuid4()), target=target, text=text, content_format="markdown",
                    origin_kind=origin_kind, origin_id=origin_id, reply_context=reply_context,
                    expires_at=self.clock.now() + timedelta(seconds=ttl_seconds),
                    allow_proactive=allow_proactive, context_version=context_version,
                    persona_version=persona_version, policy_version=policy_version,
                    anchor_message_id=anchor_message_id, requester=requester,
                    parent_task_id=parent_task_id, notification_sequence=notification_sequence, purpose=purpose,
                    fixed_publication=fixed_publication,
                ), defer_reply=defer_reply)]
        bubbles = [text] if defer_reply or origin_kind == "command" or purpose == "question" else chat_bubbles(text)
        parts = [part for bubble in bubbles for part in split_text(plain_text(bubble))]
        for part_no, part in enumerate(parts):
            ids.append(
                self.enqueue(
                    c,
                    OutboundEnvelope(
                        delivery_id=str(uuid4()),
                        target=target,
                        text=part,
                        plain_text=True,
                        origin_kind=origin_kind,
                        origin_id=origin_id,
                        part_no=part_no,
                        reply_context=reply_context,
                        expires_at=self.clock.now() + timedelta(seconds=ttl_seconds),
                        allow_proactive=allow_proactive,
                        context_version=context_version,
                        persona_version=persona_version,
                        policy_version=policy_version,
                        anchor_message_id=anchor_message_id,
                        requester=requester,
                        parent_task_id=parent_task_id,
                        notification_sequence=notification_sequence,
                        purpose=purpose,
                        fixed_publication=fixed_publication,
                    ), defer_reply=defer_reply,
                )
            )
        return ids

    def _prepare_pending(self, c, row):
        envelope = OutboundEnvelope.model_validate_json(row["envelope_json"])
        if envelope.reply_context and not envelope.reply_context.msg_seq:
            envelope = self._reserve_reply(c, envelope)
            c.execute(update(outbox).where(outbox.c.id == row["id"]).values(envelope_json=envelope.model_dump_json()))
        if envelope.plain_text or envelope.content_format == "markdown" or envelope.official_content is not None:
            return envelope
        parts = split_text(plain_text(envelope.text))
        envelope = envelope.model_copy(update={"text": parts[0], "plain_text": True})
        c.execute(update(outbox).where(outbox.c.id == row["id"]).values(envelope_json=envelope.model_dump_json()))
        for offset, part in enumerate(parts[1:], 1):
            self._enqueue_one(c, envelope.model_copy(update={"text": part, "delivery_id": str(uuid4()),
                                                          "part_no": envelope.part_no + offset}))
        return envelope

    @staticmethod
    def _spacing(delivery_id):
        # The persisted UUID gives each send a stable random gap across retries and restarts.
        return timedelta(seconds=Random(delivery_id).uniform(1.2, 2.8))

    def _defer(self, c, delivery_id, due):
        c.execute(update(outbox).where(outbox.c.id == delivery_id,
            or_(outbox.c.next_attempt_at.is_(None), outbox.c.next_attempt_at < due.isoformat()))
            .values(next_attempt_at=due.isoformat()))

    def retire_adapters(self, conn, adapters):
        adapter_id = func.json_extract(outbox.c.envelope_json, "$.target.adapter_instance_id")
        deliveries = conn.execute(select(outbox.c.id).where(
            outbox.c.state.in_(["PENDING", "RETRYABLE", "WAITING_FOR_TRIGGER"]),
            adapter_id.not_in(tuple(adapters)),
        )).scalars().all()
        receipt = SendReceipt(state="FAILED", error_code="ADAPTER_REMOVED")
        for delivery_id in deliveries:
            self._receipt(delivery_id, receipt, connection=conn)

    def retire_chat_for_finished_task(self, conn, event):
        # The task result replaces queued acknowledgments for this same input.
        deliveries = conn.execute(select(outbox.c.id).where(
            outbox.c.conversation_id == event.conversation.key, outbox.c.origin_kind == "chat",
            or_(outbox.c.origin_id.startswith("turn:"), outbox.c.origin_id.startswith("progress:")),
            outbox.c.origin_id.endswith(":" + event.event_id, autoescape=True),
            func.json_extract(outbox.c.envelope_json, "$.anchor_message_id") == event.platform_message_id,
            outbox.c.state.in_(["PENDING", "RETRYABLE", "WAITING_FOR_TRIGGER"]),
        )).scalars().all()
        receipt = SendReceipt(state="FAILED", error_code="STALE_CONTEXT")
        for delivery_id in deliveries:
            self._receipt(delivery_id, receipt, connection=conn)

    def recover(self):
        with self.db.transaction() as c:
            interrupted = c.execute(select(outbox.c.id).where(outbox.c.state == "SENDING")).scalars().all()
            for delivery_id in interrupted:
                self._receipt(delivery_id, SendReceipt(state="UNKNOWN", error_code="INTERRUPTED_SEND"), connection=c)
            candidates = c.execute(select(outbox).where(
                outbox.c.state.in_(["PENDING", "RETRYABLE", "WAITING_FOR_TRIGGER"]),
                func.json_extract(outbox.c.envelope_json, "$.purpose") == "result",
                outbox.c.created_at > (self.clock.now() - timedelta(days=1)).isoformat())).mappings().all()
            for row in candidates:
                envelope = OutboundEnvelope.model_validate_json(row["envelope_json"])
                if envelope.origin_kind == "chat":
                    if not envelope.origin_id.startswith("turn:"):
                        continue
                    prefix = ":".join(envelope.origin_id.split(":", 2)[:2]) + ":"
                    newer = c.execute(select(outbox.c.envelope_json).where(
                        outbox.c.conversation_id == envelope.target.key, outbox.c.origin_kind == "chat",
                        outbox.c.origin_id.startswith(prefix), outbox.c.origin_id != envelope.origin_id,
                        outbox.c.enqueue_seq > row["enqueue_seq"], outbox.c.state == "SENT",
                        func.json_extract(outbox.c.envelope_json, "$.purpose") == "result")
                        .order_by(outbox.c.enqueue_seq.desc()).limit(1)).scalar_one_or_none()
                    if newer and not envelope.fixed_publication:
                        if OutboundEnvelope.model_validate_json(newer).text == envelope.text:
                            c.execute(update(outbox).where(outbox.c.id == row["id"]).values(state="SUPERSEDED"))
                        continue
                    envelope = envelope.model_copy(update={"expires_at": max(envelope.expires_at,
                        datetime.fromisoformat(row["created_at"]) + timedelta(days=1))})
                if envelope.origin_kind not in {"chat", "task"}:
                    continue
                envelope = envelope.model_copy(update={"allow_proactive": True})
                c.execute(update(outbox).where(outbox.c.id == row["id"]).values(
                    envelope_json=envelope.model_dump_json(),
                    state="PENDING" if row["state"] == "WAITING_FOR_TRIGGER" else row["state"]))

    def retrigger(self, c, event):
        # The triggering user retains one reply slot for their current request.
        reply = event.reply_context
        if (not reply or not reply.expires_at or reply.expires_at <= self.clock.now()
                or not (event.mentions_bot or event.conversation.surface == "private")):
            return []
        renewed = []
        rows = (
            c.execute(
                select(outbox).where(
                    outbox.c.conversation_id == event.conversation.key,
                    outbox.c.state == "WAITING_FOR_TRIGGER",
                ).order_by(outbox.c.enqueue_seq)
            )
            .mappings()
            .all()
        )
        for row in rows:
            envelope = OutboundEnvelope.model_validate_json(row["envelope_json"])
            if envelope.requester is not None and envelope.requester != event.principal:
                continue
            if envelope.origin_kind == "chat":
                if (envelope.purpose != "result" or not envelope.origin_id.startswith("turn:")
                        or envelope.requester is None or envelope.expires_at <= self.clock.now()):
                    continue
                if (not envelope.fixed_publication and not envelope.allow_proactive
                        and not (event.reply_to and event.reply_to == envelope.anchor_message_id)):
                    prefix = ":".join(envelope.origin_id.split(":", 2)[:2]) + ":"
                    if c.execute(select(outbox.c.id).where(outbox.c.conversation_id == envelope.target.key,
                            outbox.c.origin_kind == "chat", outbox.c.origin_id.startswith(prefix),
                            outbox.c.enqueue_seq > row["enqueue_seq"], outbox.c.state == "SENT",
                            func.json_extract(outbox.c.envelope_json, "$.purpose") == "result")).first():
                        continue
            elif envelope.origin_kind != "task":
                continue
            counter_key = stable_id(envelope.target.key, reply.platform_message_id)
            seq = c.execute(select(reply_counters.c.seq).where(reply_counters.c.key == counter_key)).scalar()
            if (seq or 0) >= reply.max_replies - 1:
                break
            next_seq = (seq or 0) + 1
            if seq is None:
                c.execute(insert(reply_counters).values(key=counter_key, seq=next_seq))
            else:
                c.execute(
                    update(reply_counters).where(reply_counters.c.key == counter_key).values(seq=next_seq)
                )
            envelope = envelope.model_copy(
                update={
                    "reply_context": reply.model_copy(update={"msg_seq": next_seq}),
                    "expires_at": max(envelope.expires_at, self.clock.now() + timedelta(seconds=300)),
                }
            )
            c.execute(
                update(outbox)
                .where(outbox.c.id == row["id"])
                .values(state="PENDING", envelope_json=envelope.model_dump_json())
            )
            renewed.append(row["id"])
        return renewed

    def pending_rows(self, adapters, *, only_target=None, exclude_targets=(), before_seq=None, through=None,
                     include_deferred=False):
        proactive = [name for name, adapter in adapters.items()
                     if getattr(getattr(adapter, "config", None), "proactive_verified", False)]
        # A blocked tail must not consume the bounded batch ahead of healthy
        # conversations. Recheck the dependency below before claiming a send.
        earlier = outbox.alias("earlier_part")
        blocked_part = select(earlier.c.id).where(
            earlier.c.conversation_id == outbox.c.conversation_id,
            earlier.c.origin_kind == outbox.c.origin_kind,
            earlier.c.origin_id == outbox.c.origin_id,
            earlier.c.part_no < outbox.c.part_no,
            earlier.c.state != "SENT",
        ).correlate(outbox).exists()
        candidates = (
            select(outbox.c.id, func.row_number().over(
                partition_by=outbox.c.conversation_id, order_by=outbox.c.enqueue_seq,
            ).label("target_rank"))
            .where(or_(outbox.c.state.in_(["PENDING", "RETRYABLE"]),
                       and_(outbox.c.state == "WAITING_FOR_TRIGGER",
                            func.json_extract(outbox.c.envelope_json, "$.allow_proactive") == 1,
                            func.json_extract(outbox.c.envelope_json, "$.target.adapter_instance_id").in_(proactive))),
                   ~blocked_part, outbox.c.conversation_id.not_in(exclude_targets))
        )
        if only_target is not None:
            candidates = candidates.where(outbox.c.conversation_id == only_target)
        if before_seq is not None:
            candidates = candidates.where(outbox.c.enqueue_seq < before_seq)
        if through is not None:
            candidates = candidates.where(outbox.c.created_at <= through)
        candidates = candidates.subquery()
        heads = select(outbox).join(candidates, candidates.c.id == outbox.c.id).where(candidates.c.target_rank == 1)
        # Defer the target as a whole so a newer delivery cannot overtake its head.
        if not include_deferred:
            heads = heads.where(or_(outbox.c.next_attempt_at.is_(None),
                outbox.c.next_attempt_at <= self.clock.now().isoformat()))
        with self.db.engine.connect() as c:
            return c.execute(heads.order_by(outbox.c.enqueue_seq).limit(50)).mappings().all()

    async def dispatch_once(self, adapters, budget=0, pre_send=None):
        rows = self.pending_rows(adapters)
        slots = asyncio.Semaphore(8)

        async def dispatch(row):
            async with slots:
                await self._dispatch_row(row, adapters, budget, pre_send)

        async with asyncio.TaskGroup() as group:
            for row in rows:
                group.create_task(dispatch(row))

    async def dispatch_target(self, target, adapters, budget, pre_send, *, before_seq=None, through=None):
        rows = self.pending_rows(adapters, only_target=target, before_seq=before_seq, through=through,
                                 include_deferred=before_seq is not None)
        if rows and (not rows[0]["next_attempt_at"] or rows[0]["next_attempt_at"] <= self.clock.now().isoformat()):
            await self._dispatch_row(rows[0], adapters, budget, pre_send)
        return bool(rows)

    async def _dispatch_row(self, row, adapters, budget, pre_send):
        with self.db.transaction() as c:
            current = c.execute(select(outbox).where(outbox.c.id == row["id"],
                outbox.c.state.in_(["PENDING", "RETRYABLE", "WAITING_FOR_TRIGGER"]))).mappings().first()
            if not current:
                return
            if current["state"] == "WAITING_FOR_TRIGGER":
                c.execute(update(outbox).where(outbox.c.id == row["id"]).values(state="PENDING"))
            try:
                envelope = self._prepare_pending(c, current)
            except ValueError:
                self._receipt(row["id"], SendReceipt(state="FAILED", error_code="EMPTY_TEXT"), connection=c)
                return
        now = self.clock.now()
        if row["next_attempt_at"] and row["next_attempt_at"] > now.isoformat():
            return
        if envelope.expires_at <= now and envelope.origin_kind != "task":
            self._receipt(envelope.delivery_id, SendReceipt(state="FAILED", error_code="EXPIRED"))
            return
        adapter = adapters.get(envelope.target.adapter_instance_id)
        if adapter is None:
            with self.db.transaction() as c:
                retired = c.execute(select(outbox.c.id).where(
                    outbox.c.conversation_id == envelope.target.key,
                    outbox.c.state.in_(["PENDING", "RETRYABLE", "WAITING_FOR_TRIGGER"]))).scalars().all()
                receipt = SendReceipt(state="FAILED", error_code="ADAPTER_REMOVED")
                for delivery_id in retired:
                    self._receipt(delivery_id, receipt, connection=c)
            return
        if pre_send and not pre_send(envelope):
            self._receipt(envelope.delivery_id, SendReceipt(state="FAILED", error_code="STALE_CONTEXT"))
            return
        check = await adapter.validate_delivery(envelope)
        if not check.allowed:
            if check.wait_for_trigger:
                self._receipt(
                    envelope.delivery_id,
                    SendReceipt(state="WAITING_FOR_TRIGGER", error_code=check.reason),
                )
            elif check.reason in {"TARGET_DENIED", "IDENTITY_MISMATCH", "UNSUPPORTED_SURFACE",
                                 "RICH_MESSAGES_DISABLED", "MARKDOWN_DISABLED", "INCOMPATIBLE_CONTENT",
                                 "UNSUPPORTED_RICH_CONTENT", "RELATIONSHIP_REMOVED", "PROACTIVE_REJECTED", "LOCAL_MEDIA_LIMIT_UNVERIFIED"}:
                self._receipt(envelope.delivery_id, SendReceipt(state="FAILED", error_code=check.reason))
            return
        with self.db.transaction() as c:
            current = c.execute(select(outbox.c.state, outbox.c.next_attempt_at).where(
                outbox.c.id == row["id"])).mappings().one()
            if current["state"] not in {"PENDING", "RETRYABLE"} or (
                current["next_attempt_at"] and current["next_attempt_at"] > now.isoformat()
            ):
                return
            if c.execute(select(outbox.c.id).where(outbox.c.conversation_id == envelope.target.key,
                outbox.c.state == "SENDING")).first():
                return
            last = c.execute(select(outbox.c.id, outbox.c.sent_at, outbox.c.attempted_at).where(
                outbox.c.conversation_id == envelope.target.key, outbox.c.attempted_at.is_not(None))
                .order_by(func.coalesce(outbox.c.sent_at, outbox.c.attempted_at).desc()).limit(1)).mappings().first()
            if last:
                due = datetime.fromisoformat(last["sent_at"] or last["attempted_at"]) + self._spacing(last["id"])
                if due > now:
                    self._defer(c, row["id"], due)
                    return
            predecessor = c.execute(
                select(outbox.c.id).where(
                    outbox.c.conversation_id == envelope.target.key,
                    outbox.c.origin_kind == envelope.origin_kind,
                    outbox.c.origin_id == envelope.origin_id,
                    outbox.c.part_no < envelope.part_no,
                    outbox.c.state != "SENT",
                )
            ).first()
            if predecessor:
                return
            if envelope.parent_task_id and c.execute(select(outbox.c.id).where(
                outbox.c.task_id == envelope.parent_task_id,
                outbox.c.notification_sequence < envelope.notification_sequence,
                outbox.c.state.in_(["PENDING", "RETRYABLE", "SENDING"]))).first():
                return
            config = getattr(adapter, "config", None)
            effective_budget = budget
            if getattr(config, "kind", None) == "official":
                effective_budget = min(budget or config.official_messages_per_minute,
                                       config.official_messages_per_minute)
                # Count every attempted delivery conservatively, including UNKNOWN.
                # Platform rate limits still remain authoritative.
                daily = c.execute(select(func.count()).select_from(outbox).where(
                    outbox.c.conversation_id == envelope.target.key,
                    outbox.c.attempted_at >= (now - timedelta(days=1)).isoformat(),
                )).scalar_one()
                global_minute = c.execute(select(func.count()).select_from(outbox).where(
                    func.json_extract(outbox.c.envelope_json, "$.target.adapter_instance_id")
                    == envelope.target.adapter_instance_id,
                    outbox.c.attempted_at >= (now - timedelta(seconds=60)).isoformat(),
                )).scalar_one()
                if daily >= config.official_messages_per_day or global_minute >= config.official_bot_messages_per_minute:
                    self._defer(c, envelope.delivery_id, now + timedelta(seconds=60))
                    return
            if effective_budget:
                rate_time = func.coalesce(outbox.c.sent_at, outbox.c.attempted_at)
                used = c.execute(
                    select(rate_time)
                    .order_by(rate_time.desc())
                    .offset(effective_budget - 1).limit(1)
                    .select_from(outbox)
                    .where(
                        outbox.c.conversation_id == envelope.target.key,
                        or_(
                            outbox.c.state == "SENDING",
                            and_(outbox.c.state == "UNKNOWN",
                                outbox.c.attempted_at >= (now - timedelta(seconds=60)).isoformat()),
                            and_(
                                outbox.c.state == "SENT",
                                outbox.c.sent_at >= (now - timedelta(seconds=60)).isoformat(),
                            ),
                        ),
                    )
                ).scalar_one_or_none()
                if used:
                    self._defer(c, envelope.delivery_id, datetime.fromisoformat(used) + timedelta(seconds=60, microseconds=1))
                    return
            claimed = c.execute(
                update(outbox)
                .where(outbox.c.id == row["id"], outbox.c.state.in_(["PENDING", "RETRYABLE"]))
                .values(state="SENDING", attempts=outbox.c.attempts + 1, attempted_at=now.isoformat())
            )
            if claimed.rowcount != 1:
                return
            use_final_stream = (
                getattr(config, "kind", None) == "official"
                and config.streaming_enabled
                and envelope.target.surface == Surface.PRIVATE
                and check.reason == "VALID_REPLY"
                and envelope.official_content is None
                and 0 < len(envelope.text) <= 10000
                and bool(envelope.text.strip())
                # A settled Pi stream already had its chance to publish this origin.
                and not c.execute(select(stream_messages.c.id).join(
                    stream_runs, stream_runs.c.run_id == stream_messages.c.run_id
                ).where(
                    stream_runs.c.result_origin_kind == envelope.origin_kind,
                    stream_runs.c.result_origin_id == envelope.origin_id,
                    stream_messages.c.kind == "answer",
                ).limit(1)).first()
            )
        try:
            if getattr(config, "kind", None) == "official":
                def authorize(envelope=envelope, adapter=adapter):
                    return adapters.get(envelope.target.adapter_instance_id) is adapter and (
                        not pre_send or pre_send(envelope))

                receipt = None
                if use_final_stream:
                    stream = OfficialStream(adapter, envelope, content_type=envelope.content_format,
                                            authorize=authorize)
                    stream.text = envelope.text
                    receipt = await stream.finish()
                if receipt is None or receipt.error_code in {
                    "HTTP_400", "HTTP_403", "HTTP_404", "HTTP_405", "HTTP_415", "HTTP_422",
                }:
                    receipt = await adapter.send(envelope, authorize=authorize)
            else:
                receipt = await adapter.send(envelope)
        except asyncio.CancelledError:
            self._receipt(envelope.delivery_id, SendReceipt(state="UNKNOWN", error_code="SEND_CANCELLED"))
            raise
        except Exception:  # noqa: BLE001 - A send exception may follow an external side effect.
            receipt = SendReceipt(state="UNKNOWN", error_code="SEND_EXCEPTION")
        self._receipt(envelope.delivery_id, receipt)

    def _receipt(self, delivery_id, receipt, *, connection=None):
        with self.db.transaction() if connection is None else nullcontext(connection) as c:
            previous = c.execute(select(outbox.c.state, outbox.c.conversation_id, outbox.c.envelope_json,
                outbox.c.enqueue_seq, outbox.c.attempts, outbox.c.origin_kind, outbox.c.origin_id,
                outbox.c.task_id).where(
                outbox.c.id == delivery_id)).mappings().one()
            error = receipt.error_code or ""
            if (receipt.state == "RETRYABLE" and previous["attempts"] >= MEDIA_UPLOAD_MAX_ATTEMPTS
                    and (error == "MEDIA_UPLOAD_UNAVAILABLE" or error.startswith("LOCAL_UPLOAD_"))):
                # Upload retries occur before the QQ message POST and can otherwise hold the target indefinitely.
                receipt = receipt.model_copy(update={"state": "FAILED"})
            c.execute(
                update(outbox)
                .where(outbox.c.id == delivery_id)
                .values(
                    state=receipt.state,
                    receipt_json=receipt.model_dump_json(),
                    platform_message_id=receipt.platform_message_id,
                    sent_at=self.clock.now().isoformat() if receipt.state == "SENT" else None,
                    next_attempt_at=(self.clock.now() + timedelta(seconds=10)).isoformat()
                    if receipt.state == "RETRYABLE"
                    else None,
                )
            )

            dialogue_id = None
            origin = previous["origin_id"] or ""
            if previous["origin_kind"] == "chat" and origin.startswith(("turn:", "progress:")):
                turn = origin if origin.startswith("turn:") else origin.split(":", 2)[-1]
                _, _, source = turn.removeprefix("turn:").partition(":")
                dialogue_id = c.execute(select(dialogue_inputs.c.dialogue_id).where(
                    dialogue_inputs.c.id == source)).scalar_one_or_none()
            elif previous["origin_kind"] == "task" and previous["task_id"]:
                from elephant.storage.task_schema import tasks
                dialogue_id = c.execute(select(tasks.c.origin_dialogue_id).where(
                    tasks.c.id == previous["task_id"])).scalar_one_or_none()
            if dialogue_id:
                c.execute(sqlite_insert(leader_events).values(dialogue_id=dialogue_id, source_kind="delivery",
                    source_id=stable_id(delivery_id, str(previous["attempts"]), receipt.state),
                    payload_json=receipt.model_dump_json()).on_conflict_do_nothing(
                    index_elements=[leader_events.c.source_kind, leader_events.c.source_id]))

            if previous["state"] == "SENDING":
                due = self.clock.now() + self._spacing(delivery_id)
                pending = c.execute(select(outbox.c.id).where(
                    outbox.c.conversation_id == previous["conversation_id"],
                    outbox.c.state.in_(["PENDING", "RETRYABLE"]))).scalars().all()
                for pending_id in pending:
                    self._defer(c, pending_id, due)

            if receipt.state == "SENT":
                from elephant.storage.task_schema import notification_question_ids, notifications, waits
                origin = c.execute(select(outbox.c.origin_id).where(outbox.c.id == delivery_id)).scalar_one()
                remaining = c.execute(select(outbox.c.id).where(outbox.c.origin_id == origin,
                    outbox.c.state != "SENT")).first()
                if not remaining:
                    now = self.clock.now()
                    envelope = OutboundEnvelope.model_validate_json(previous["envelope_json"])
                    if (envelope.origin_kind == "chat" and envelope.purpose == "result" and not envelope.fixed_publication
                            and envelope.origin_id.startswith("turn:")):
                        prefix = ":".join(envelope.origin_id.split(":", 2)[:2]) + ":"
                        c.execute(update(outbox).where(outbox.c.conversation_id == envelope.target.key,
                            outbox.c.origin_kind == "chat", outbox.c.origin_id.startswith(prefix),
                            outbox.c.origin_id != envelope.origin_id,
                            outbox.c.enqueue_seq < previous["enqueue_seq"],
                            func.json_extract(outbox.c.envelope_json, "$.purpose") == "result",
                            func.json_extract(outbox.c.envelope_json, "$.text") == envelope.text,
                            func.json_extract(outbox.c.envelope_json, "$.fixed_publication").is_(None),
                            outbox.c.state.in_(["PENDING", "RETRYABLE", "WAITING_FOR_TRIGGER"]))
                            .values(state="SUPERSEDED"))
                    c.execute(update(notifications).where(notifications.c.id == origin).values(state="SENT"))
                    linked = (waits.c.id.in_(notification_question_ids(c, origin))
                              if envelope.purpose == "question" else waits.c.notification_id == origin)
                    c.execute(update(waits).where(linked, waits.c.delivered_at.is_(None)).values(delivered_at=now.isoformat(),
                            expires_at=(now + timedelta(hours=1)).isoformat()))
