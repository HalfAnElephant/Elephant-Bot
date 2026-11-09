"""Run only inside Core. Network providers receive bounded snapshots and never a Database."""

import json
from datetime import timedelta

from sqlalchemy import delete, insert, or_, select, update

from qqbot.domain.messages import DTO, InboundEvent, TextSegment, stable_id
from qqbot.memory import MemoryRecord, _safe
from qqbot.storage.schema import inbox
from qqbot.storage.social_schema import (
    conversation_summaries,
    memory_checkpoints,
    memory_source_retention,
    social_index_jobs,
    social_records,
    social_tombstones,
)


class MaintenanceReport(DTO):
    state: str
    summary_seq: int = 0
    extraction_state: str = "SKIPPED"
    candidates: int = 0
    rejected: int = 0
    indexed: int = 0
    skipped_indexes: int = 0
    failed_indexes: int = 0
    purged_records: int = 0
    expired_sources: int = 0


class MemoryMaintenance:
    def __init__(
        self,
        service,
        extractor=None,
        embedding=None,
        *,
        enabled=False,
        raw_retention_days=30,
        max_messages=40,
    ):
        if raw_retention_days < 1 or not 1 <= max_messages <= 100:
            raise ValueError("invalid maintenance bounds")
        self.service, self.db, self.clock = service, service.db, service.clock
        self.extractor, self.embedding, self.enabled = extractor, embedding, enabled
        self.raw_retention_days, self.max_messages = raw_retention_days, max_messages

    def _events(self, access, after_seq, conn):
        revoked = select(social_tombstones.c.source_id)
        rows = conn.execute(
            select(inbox.c.seq, inbox.c.event_json)
            .where(
                inbox.c.conversation_id == access.conversation.key,
                inbox.c.seq > after_seq,
                inbox.c.id.not_in(revoked),
            )
            .order_by(inbox.c.seq)
            .limit(self.max_messages)
        ).all()
        events = []
        last_seq = after_seq
        for seq, raw in rows:
            last_seq = seq
            event = InboundEvent.model_validate_json(raw)
            if event.event_type != "message.created" or event.sender_is_bot:
                continue
            try:
                _safe(event.text)
            except ValueError:
                continue
            events.append(event)
        return last_seq, events

    def summary(self, access):
        with self.db.engine.connect() as conn:
            row = (
                conn.execute(
                    select(conversation_summaries).where(
                        conversation_summaries.c.conversation_id == access.conversation.key,
                        conversation_summaries.c.status == "VALID",
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                return ""
            revoked = conn.execute(
                select(social_tombstones.c.source_id).where(
                    social_tombstones.c.source_id.in_(json.loads(row["source_ids_json"]))
                )
            ).first()
            return "" if revoked else json.dumps(json.loads(row["summary_json"]), ensure_ascii=False)

    def _summarize(self, access):
        cid = access.conversation.key
        with self.db.transaction() as conn:
            checkpoint = (
                conn.execute(select(memory_checkpoints).where(memory_checkpoints.c.conversation_id == cid))
                .mappings()
                .first()
            )
            if checkpoint is None:
                conn.execute(
                    insert(memory_checkpoints).values(
                        conversation_id=cid, summary_seq=0, extraction_seq=0, extraction_state="SKIPPED"
                    )
                )
                checkpoint = {"summary_seq": 0, "extraction_seq": 0, "extraction_state": "SKIPPED"}
            seq, events = self._events(access, checkpoint["summary_seq"], conn)
            if seq == checkpoint["summary_seq"]:
                return seq
            previous = (
                conn.execute(
                    select(conversation_summaries).where(conversation_summaries.c.conversation_id == cid)
                )
                .mappings()
                .first()
            )
            snippets = (
                json.loads(previous["summary_json"]) if previous and previous["status"] == "VALID" else []
            )
            snippets += [
                {
                    "event_id": e.event_id,
                    "speaker": e.principal.key,
                    "at": e.occurred_at.isoformat(),
                    "text": e.text[:300],
                }
                for e in events
            ]
            snippets = snippets[-self.max_messages :]
            # Extractive snippets keep provenance and avoid model-generated facts becoming evidence.
            values = {
                "summary_json": json.dumps(snippets, ensure_ascii=False),
                "source_ids_json": json.dumps([s["event_id"] for s in snippets]),
                "status": "VALID",
                "updated_at": self.clock.now().isoformat(),
            }
            if previous:
                conn.execute(
                    update(conversation_summaries)
                    .where(conversation_summaries.c.conversation_id == cid)
                    .values(**values)
                )
            else:
                conn.execute(insert(conversation_summaries).values(conversation_id=cid, **values))
            conn.execute(
                update(memory_checkpoints)
                .where(memory_checkpoints.c.conversation_id == cid)
                .values(summary_seq=seq)
            )
            return seq

    async def _extract(self, access, auto_extract):
        cid = access.conversation.key
        with self.db.engine.connect() as conn:
            checkpoint = (
                conn.execute(select(memory_checkpoints).where(memory_checkpoints.c.conversation_id == cid))
                .mappings()
                .one()
            )
            seq, events = self._events(access, checkpoint["extraction_seq"], conn)
        state, count, rejected = "SKIPPED", 0, 0
        if seq == checkpoint["extraction_seq"]:
            return checkpoint["extraction_state"], count, rejected
        if auto_extract and self.extractor is not None:
            result = await self.extractor.extract(events)
            state = result.state
            source_ids = {e.event_id for e in events}
            for suggestion in result.suggestions:
                if not suggestion.should_persist:
                    continue
                try:
                    if not set(suggestion.source_event_ids) <= source_ids:
                        raise ValueError("model widened evidence scope")
                    if (
                        suggestion.scope_type == "principal_in_conversation"
                        and suggestion.subject_ref != access.principal.key
                    ):
                        raise PermissionError("model widened subject scope")
                    now = self.clock.now()
                    record = MemoryRecord(
                        memory_id=stable_id(
                            "extracted-memory", cid, self.extractor.model, suggestion.model_dump_json()
                        ),
                        scope_type=suggestion.scope_type,
                        scope_id=cid,
                        subject_ref=suggestion.subject_ref,
                        predicate=suggestion.predicate,
                        value_json=suggestion.value_json,
                        text=suggestion.text,
                        source_event_ids=suggestion.source_event_ids,
                        source_kind="inference",
                        confidence=suggestion.confidence,
                        valid_from=now,
                        valid_until=now + timedelta(days=suggestion.valid_for_days),
                        created_at=now,
                        last_verified_at=now,
                        extraction_model=self.extractor.model,
                    )
                    self.service.candidate(access, record, auto_enabled=True)
                    count += 1
                except (ValueError, PermissionError, LookupError):
                    rejected += 1
        with self.db.transaction() as conn:
            values = {"extraction_state": state}
            if state != "FAILED":
                values["extraction_seq"] = seq
            conn.execute(
                update(memory_checkpoints)
                .where(
                    memory_checkpoints.c.conversation_id == cid,
                    memory_checkpoints.c.extraction_seq == checkpoint["extraction_seq"],
                )
                .values(**values)
            )
        return state, count, rejected

    async def _index(self, access):
        with self.db.engine.connect() as conn:
            jobs = conn.execute(
                select(social_index_jobs.c.id, social_records.c.record_json)
                .join(social_records, social_records.c.id == social_index_jobs.c.memory_id)
                .where(social_index_jobs.c.state == "PENDING", self.service._management_access(access))
                .limit(8)
            ).all()
        indexed = skipped = failed = 0
        for job_id, raw in jobs:
            record = MemoryRecord.model_validate_json(raw)
            state = "SKIPPED"
            if (
                record.status == "active"
                and self.service._manageable(access, record)
                and self.embedding is not None
                and self.embedding.api_key
            ):
                vector = await self.embedding.embed(record.text)
                if vector is None:
                    state = "FAILED"
                    failed += 1
                else:
                    try:
                        self.service.store_embedding(
                            access, record.memory_id, vector, self.embedding.model, self.embedding.version
                        )
                        indexed += 1
                        state = "DONE"
                    except (PermissionError, ValueError, LookupError):
                        state = "SKIPPED"
            if state == "SKIPPED":
                skipped += 1
            with self.db.transaction() as conn:
                conn.execute(
                    update(social_index_jobs).where(social_index_jobs.c.id == job_id).values(state=state)
                )
        return indexed, skipped, failed

    def requeue_embeddings(self, access):
        with self.db.transaction() as conn:
            ids = select(social_records.c.id).where(
                self.service._management_access(access), social_records.c.status == "active"
            )
            conn.execute(
                update(social_index_jobs)
                .where(
                    social_index_jobs.c.memory_id.in_(ids),
                    social_index_jobs.c.state.in_(("SKIPPED", "FAILED")),
                )
                .values(state="PENDING")
            )

    def _purge(self, access):
        cid = access.conversation.key
        cutoff = self.clock.now() - timedelta(days=self.raw_retention_days)
        expired_sources = []
        with self.db.engine.connect() as conn:
            sources = conn.execute(
                select(inbox.c.id, inbox.c.event_json)
                .where(
                    inbox.c.conversation_id == cid,
                    inbox.c.id.not_in(select(memory_source_retention.c.source_id)),
                    or_(inbox.c.received_at < cutoff.isoformat(), inbox.c.received_at.is_(None)),
                )
                .limit(100)
            ).all()
            active = (
                conn.execute(
                    select(social_records.c.record_json).where(
                        social_records.c.conversation_id == cid,
                        social_records.c.status.in_(("active", "candidate")),
                    )
                )
                .scalars()
                .all()
            )
            retained_sources = {
                source for raw in active for source in MemoryRecord.model_validate_json(raw).source_event_ids
            }
        for source_id, raw in sources:
            event = InboundEvent.model_validate_json(raw)
            if (
                event.received_at < cutoff
                and event.segments
                and (event.principal.key == access.principal.key or access.role in {"admin", "owner"})
            ):
                if source_id not in retained_sources:
                    self.service.revoke_source(access, source_id)
                segments = (TextSegment(text=event.text[:300]),) if source_id in retained_sources else ()
                with self.db.transaction() as conn:
                    conn.execute(
                        update(inbox)
                        .where(inbox.c.id == source_id)
                        .values(event_json=event.model_copy(update={"segments": segments}).model_dump_json())
                    )
                    conn.execute(
                        insert(memory_source_retention).values(
                            source_id=source_id, conversation_id=cid, redacted_at=self.clock.now().isoformat()
                        )
                    )
                expired_sources.append(source_id)
        with self.db.transaction() as conn:
            rows = (
                conn.execute(
                    select(social_records.c.record_json).where(
                        social_records.c.conversation_id == cid, social_records.c.status == "deleted"
                    )
                )
                .scalars()
                .all()
            )
            purged = 0
            for raw in rows:
                record = MemoryRecord.model_validate_json(raw)
                if record.text == "[deleted]" and record.value_json == "null" and record.embedding is None:
                    continue
                for source_id in record.source_event_ids:
                    if (
                        conn.execute(
                            select(social_tombstones.c.source_id).where(
                                social_tombstones.c.source_id == source_id
                            )
                        ).first()
                        is None
                    ):
                        conn.execute(
                            insert(social_tombstones).values(source_id=source_id, conversation_id=cid)
                        )
                self.service._save(conn, record)
                conn.execute(
                    delete(social_index_jobs).where(social_index_jobs.c.memory_id == record.memory_id)
                )
                purged += 1
            if purged:
                self.service._bump(conn, cid)
        return purged, len(expired_sources)

    async def maintenance_once(self, access, *, auto_extract=False):
        if not self.enabled:
            with self.db.transaction() as conn:
                ids = select(social_records.c.id).where(self.service._management_access(access))
                result = conn.execute(
                    update(social_index_jobs)
                    .where(social_index_jobs.c.memory_id.in_(ids), social_index_jobs.c.state == "PENDING")
                    .values(state="SKIPPED")
                )
            return MaintenanceReport(state="SKIPPED", skipped_indexes=result.rowcount)
        purged, expired = self._purge(access)
        seq = self._summarize(access)
        extraction, count, rejected = await self._extract(access, auto_extract)
        indexed, skipped, failed = await self._index(access)
        return MaintenanceReport(
            state="DONE",
            summary_seq=seq,
            extraction_state=extraction,
            candidates=count,
            rejected=rejected,
            indexed=indexed,
            skipped_indexes=skipped,
            failed_indexes=failed,
            purged_records=purged,
            expired_sources=expired,
        )
