from contextlib import nullcontext
from datetime import datetime, timedelta
from random import Random
from uuid import uuid4

from sqlalchemy import and_, func, insert, or_, select, update

from qqbot.domain import OutboundEnvelope, SendReceipt, stable_id
from qqbot.storage.schema import outbox, reply_counters

from .presentation import chat_bubbles, plain_text, split_text


class Outbox:
    def __init__(self, db, clock, *, markdown_adapters=()):
        self.db, self.clock = db, clock
        self.markdown_adapters = frozenset(markdown_adapters)

    def enqueue(self, c, envelope: OutboundEnvelope) -> str:
        # Rich payloads are atomic. Never duplicate a keyboard or media by text splitting.
        if envelope.content_format == "markdown" or envelope.official_content is not None:
            return self._enqueue_one(c, envelope)
        text = envelope.text if envelope.plain_text else plain_text(envelope.text)
        deliveries = []
        for offset, part in enumerate(split_text(text)):
            deliveries.append(self._enqueue_one(c, envelope.model_copy(update={
                "text": part, "plain_text": True, "part_no": envelope.part_no + offset,
                "delivery_id": envelope.delivery_id if offset == 0 else str(uuid4()),
            })))
        return deliveries[0]

    def _enqueue_one(self, c, envelope):
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
        reply = envelope.reply_context
        if reply and reply.expires_at:
            counter_key = stable_id(envelope.target.key, reply.platform_message_id)
            seq = c.execute(select(reply_counters.c.seq).where(reply_counters.c.key == counter_key)).scalar()
            next_seq = (seq or 0) + 1
            if seq is None:
                c.execute(insert(reply_counters).values(key=counter_key, seq=next_seq))
            else:
                c.execute(
                    update(reply_counters).where(reply_counters.c.key == counter_key).values(seq=next_seq)
                )
            envelope = envelope.model_copy(
                update={"reply_context": reply.model_copy(update={"msg_seq": next_seq})}
            )
        c.execute(
            insert(outbox).values(
                id=envelope.delivery_id,
                dedupe_key=key,
                conversation_id=envelope.target.key,
                envelope_json=envelope.model_dump_json(),
                state="PENDING",
                attempts=0,
                created_at=self.clock.now().isoformat(),
                origin_id=envelope.origin_id,
                origin_kind=envelope.origin_kind,
                part_no=envelope.part_no,
                task_id=envelope.parent_task_id,
                notification_sequence=envelope.notification_sequence,
            )
        )
        return envelope.delivery_id

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
                ))]
        bubbles = [text] if origin_kind == "command" or purpose == "question" else chat_bubbles(text)
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
                    ),
                )
            )
        return ids

    def _prepare_pending(self, c, row):
        envelope = OutboundEnvelope.model_validate_json(row["envelope_json"])
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

    def recover(self):
        with self.db.transaction() as c:
            c.execute(update(outbox).where(outbox.c.state == "SENDING").values(state="UNKNOWN"))

    def retrigger(self, c, event):
        # Only an addressed trigger in the same audience may renew a task reply credential.
        if not event.reply_context or not (event.mentions_bot or event.conversation.surface == "private"):
            return
        rows = (
            c.execute(
                select(outbox).where(
                    outbox.c.conversation_id == event.conversation.key,
                    outbox.c.state == "WAITING_FOR_TRIGGER",
                )
            )
            .mappings()
            .all()
        )
        for row in rows:
            envelope = OutboundEnvelope.model_validate_json(row["envelope_json"])
            if envelope.origin_kind != "task":
                continue
            reply = event.reply_context
            counter_key = stable_id(envelope.target.key, reply.platform_message_id)
            seq = c.execute(select(reply_counters.c.seq).where(reply_counters.c.key == counter_key)).scalar()
            if (seq or 0) >= reply.max_replies:
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
                    "expires_at": self.clock.now() + timedelta(seconds=300),
                }
            )
            c.execute(
                update(outbox)
                .where(outbox.c.id == row["id"])
                .values(state="PENDING", envelope_json=envelope.model_dump_json())
            )

    async def dispatch_once(self, adapters, budget=0, pre_send=None):
        with self.db.engine.connect() as c:
            rows = (
                c.execute(
                    select(outbox)
                    .where(outbox.c.state.in_(["PENDING", "RETRYABLE"]),
                           or_(outbox.c.next_attempt_at.is_(None), outbox.c.next_attempt_at <= self.clock.now().isoformat()))
                    .order_by(outbox.c.created_at, outbox.c.conversation_id, outbox.c.origin_id,
                              outbox.c.part_no, outbox.c.id)
                    .limit(50)
                )
                .mappings()
                .all()
            )
        for row in rows:
            with self.db.transaction() as c:
                current = c.execute(select(outbox).where(outbox.c.id == row["id"],
                    outbox.c.state.in_(["PENDING", "RETRYABLE"]))).mappings().first()
                if not current:
                    continue
                try:
                    envelope = self._prepare_pending(c, current)
                except ValueError:
                    self._receipt(row["id"], SendReceipt(state="FAILED", error_code="EMPTY_TEXT"), connection=c)
                    continue
            now = self.clock.now()
            if row["next_attempt_at"] and row["next_attempt_at"] > now.isoformat():
                continue
            if envelope.expires_at <= now and envelope.origin_kind != "task":
                self._receipt(envelope.delivery_id, SendReceipt(state="FAILED", error_code="EXPIRED"))
                continue
            adapter = adapters.get(envelope.target.adapter_instance_id)
            if adapter is None:
                continue
            if pre_send and not pre_send(envelope):
                self._receipt(envelope.delivery_id, SendReceipt(state="FAILED", error_code="STALE_CONTEXT"))
                continue
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
                continue
            with self.db.transaction() as c:
                current = c.execute(select(outbox.c.state, outbox.c.next_attempt_at).where(
                    outbox.c.id == row["id"])).mappings().one()
                if current["state"] not in {"PENDING", "RETRYABLE"} or (
                    current["next_attempt_at"] and current["next_attempt_at"] > now.isoformat()
                ):
                    continue
                if c.execute(select(outbox.c.id).where(outbox.c.conversation_id == envelope.target.key,
                    outbox.c.state == "SENDING")).first():
                    continue
                last = c.execute(select(outbox.c.id, outbox.c.sent_at, outbox.c.attempted_at).where(
                    outbox.c.conversation_id == envelope.target.key, outbox.c.attempted_at.is_not(None))
                    .order_by(func.coalesce(outbox.c.sent_at, outbox.c.attempted_at).desc()).limit(1)).mappings().first()
                if last:
                    due = datetime.fromisoformat(last["sent_at"] or last["attempted_at"]) + self._spacing(last["id"])
                    if due > now:
                        self._defer(c, row["id"], due)
                        continue
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
                    continue
                if envelope.parent_task_id and c.execute(select(outbox.c.id).where(
                    outbox.c.task_id == envelope.parent_task_id,
                    outbox.c.notification_sequence < envelope.notification_sequence,
                    outbox.c.state.in_(["PENDING", "RETRYABLE", "SENDING"]))).first():
                    continue
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
                        continue
                if effective_budget:
                    used = c.execute(
                        select(func.count())
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
                    ).scalar_one()
                    if used >= effective_budget:
                        continue
                claimed = c.execute(
                    update(outbox)
                    .where(outbox.c.id == row["id"], outbox.c.state.in_(["PENDING", "RETRYABLE"]))
                    .values(state="SENDING", attempts=outbox.c.attempts + 1, attempted_at=now.isoformat())
                )
                if claimed.rowcount != 1:
                    continue
            try:
                if getattr(getattr(adapter, "config", None), "kind", None) == "official":
                    receipt = await adapter.send(envelope, authorize=lambda envelope=envelope: not pre_send or pre_send(envelope))
                else:
                    receipt = await adapter.send(envelope)
            except Exception:  # noqa: BLE001 - A send exception may follow an external side effect.
                receipt = SendReceipt(state="UNKNOWN", error_code="SEND_EXCEPTION")
            self._receipt(envelope.delivery_id, receipt)

    def _receipt(self, delivery_id, receipt, *, connection=None):
        with self.db.transaction() if connection is None else nullcontext(connection) as c:
            previous = c.execute(select(outbox.c.state, outbox.c.conversation_id).where(
                outbox.c.id == delivery_id)).mappings().one()
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

            if previous["state"] == "SENDING":
                due = self.clock.now() + self._spacing(delivery_id)
                pending = c.execute(select(outbox.c.id).where(
                    outbox.c.conversation_id == previous["conversation_id"],
                    outbox.c.state.in_(["PENDING", "RETRYABLE"]))).scalars().all()
                for pending_id in pending:
                    self._defer(c, pending_id, due)

            if receipt.state == "SENT":
                from qqbot.storage.task_schema import notifications, waits
                origin = c.execute(select(outbox.c.origin_id).where(outbox.c.id == delivery_id)).scalar_one()
                remaining = c.execute(select(outbox.c.id).where(outbox.c.origin_id == origin,
                    outbox.c.state != "SENT")).first()
                if not remaining:
                    now = self.clock.now()
                    c.execute(update(notifications).where(notifications.c.id == origin).values(state="SENT"))
                    c.execute(update(waits).where(waits.c.notification_id == origin,
                        waits.c.delivered_at.is_(None)).values(delivered_at=now.isoformat(),
                            expires_at=(now + timedelta(hours=1)).isoformat()))
