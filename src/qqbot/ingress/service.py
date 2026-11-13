from contextlib import nullcontext

from sqlalchemy import func, insert, select, update

from qqbot.domain import InboundEvent
from qqbot.storage.schema import conversations, inbox


class Inbox:
    def __init__(self, db):
        self.db = db

    def accept(self, event: InboundEvent, *, conn=None) -> tuple[InboundEvent, bool]:
        with (nullcontext(conn) if conn is not None else self.db.transaction()) as c:
            existing = c.execute(select(inbox.c.event_json).where(inbox.c.id == event.event_id)).scalar()
            old_event = InboundEvent.model_validate_json(existing) if existing else None
            if old_event and (old_event.mentions_bot or not event.mentions_bot):
                return old_event, False
            key = event.conversation.key
            seq = c.execute(select(conversations.c.seq).where(conversations.c.id == key)).scalar()
            if seq is None:
                c.execute(
                    insert(conversations).values(
                        id=key, ref_json=event.conversation.model_dump_json(), seq=0, revocation=0
                    )
                )
                seq = 0
            event = event.model_copy(update={"ingest_seq": seq + 1})
            c.execute(update(conversations).where(conversations.c.id == key).values(seq=seq + 1))
            if old_event:
                # Some platforms deliver both all-message and @ notifications for one message.
                event = old_event.model_copy(
                    update={
                        "mentions_bot": True,
                        "ingest_seq": seq + 1,
                        "observation_mode": event.observation_mode,
                        "received_at": event.received_at,
                    }
                )
                c.execute(
                    update(inbox)
                    .where(inbox.c.id == event.event_id)
                    .values(seq=seq + 1, event_json=event.model_dump_json(), state="PENDING")
                )
                return event, False
            c.execute(
                insert(inbox).values(
                    id=event.event_id,
                    conversation_id=key,
                    seq=seq + 1,
                    event_json=event.model_dump_json(),
                    state="PENDING",
                    priority=int(event.text.strip().startswith("/") or event.event_type != "message.created"),
                    event_type=event.event_type,
                    message_id=event.platform_message_id,
                    principal_id=event.principal.key,
                    received_at=event.received_at.isoformat(),
                )
            )
            return event, True

    def pending(self, limit: int = 100, exclude=()) -> list[InboundEvent]:
        with self.db.engine.connect() as c:
            ranked = (
                select(
                    inbox.c.event_json,
                    inbox.c.seq,
                    func.row_number()
                    .over(
                        partition_by=inbox.c.conversation_id, order_by=(inbox.c.priority.desc(), inbox.c.seq)
                    )
                    .label("rank"),
                )
                .where(inbox.c.state == "PENDING", inbox.c.id.not_in(exclude))
                .subquery()
            )
            return [
                InboundEvent.model_validate_json(row[0])
                for row in c.execute(
                    select(ranked.c.event_json).where(ranked.c.rank == 1).order_by(ranked.c.seq).limit(limit)
                )
            ]

    def finish(self, event_id: str, error: str | None = None, expected_seq: int | None = None):
        with self.db.transaction() as c:
            c.execute(
                update(inbox)
                .where(inbox.c.id == event_id, True if expected_seq is None else inbox.c.seq == expected_seq)
                .values(state="FAILED" if error else "DONE", error=error)
            )
