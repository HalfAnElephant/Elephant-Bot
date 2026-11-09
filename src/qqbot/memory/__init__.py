import json
import re
from datetime import datetime
from typing import Literal
from uuid import uuid4

from pydantic import Field, field_validator
from sqlalchemy import and_, delete, insert, or_, select, update

from qqbot.domain.messages import DTO, AccessContext, InboundEvent
from qqbot.storage.schema import inbox
from qqbot.storage.social_schema import (
    conversation_summaries,
    memory_deletions,
    social_index_jobs,
    social_records,
    social_tombstones,
    social_versions,
)


class MemoryRecord(DTO):
    memory_id: str = Field(default_factory=lambda: str(uuid4()))
    scope_type: Literal["conversation", "principal_in_conversation", "owner_private", "global_curated"]
    scope_id: str
    subject_ref: str
    predicate: str
    value_json: str
    text: str = Field(min_length=1, max_length=4000)
    source_event_ids: tuple[str, ...] = Field(min_length=1)
    source_kind: Literal["user_explicit", "observed_event", "task_verified", "inference"]
    confidence: float = Field(ge=0, le=1, allow_inf_nan=False)
    status: Literal["candidate", "active", "superseded", "expired", "deleted", "disputed"] = "candidate"
    valid_from: datetime
    valid_until: datetime | None = None
    created_at: datetime
    last_verified_at: datetime
    supersedes_id: str | None = None
    sensitivity: Literal["normal", "private"] = "normal"
    embedding_model_id: str | None = None
    embedding_version: str | None = None
    embedding: tuple[float, ...] | None = None
    extraction_model: str = "explicit-command"
    prompt_version: str = "memory-v1"

    @field_validator("valid_from", "valid_until", "created_at", "last_verified_at")
    @classmethod
    def aware_times(cls, value):
        if value is not None and value.tzinfo is None:
            raise ValueError("timezone required")
        return value


class JargonEntry(DTO):
    scope_id: str
    term: str
    sense_id: str
    definition: str
    example_event_ids: tuple[str, ...]
    speaker_count: int
    occurrence_count: int
    confidence: float = Field(ge=0, le=1)
    status: Literal["candidate", "active", "disputed", "expired"]
    last_seen_at: datetime
    expires_at: datetime | None = None
    forbidden_for_output: bool = True


def keywords(text: str) -> set[str]:
    parts = re.findall(r"[a-zA-Z0-9_]+|[\u3400-\u9fff]+", text.lower())
    return {part[i : i + n] for part in parts for n in (1, 2, 3) for i in range(len(part) - n + 1)} | set(
        parts
    )


def _safe(text):
    if re.search(
        r"(?i)(sk-[a-z0-9_-]{12,}|-----BEGIN .*PRIVATE KEY|(?:api[_-]?key|password|密码)\s*[:=]\s*\S+)", text
    ):
        raise ValueError("secret persistence prohibited")


class SocialService:
    """Core-owned service. All mutations and evidence checks share a SQLite transaction."""

    def __init__(self, db, clock):
        self.db, self.clock = db, clock

    def _access(self, access):
        cid, pid = access.conversation.key, access.principal.key
        permitted = [
            and_(social_records.c.scope_type == "conversation", social_records.c.scope_id == cid),
            and_(
                social_records.c.scope_type == "principal_in_conversation",
                social_records.c.conversation_id == cid,
                social_records.c.subject_ref == pid,
            ),
            social_records.c.scope_type == "global_curated",
        ]
        if access.role == "owner" and access.conversation.surface.value == "private":
            permitted.append(
                and_(social_records.c.scope_type == "owner_private", social_records.c.scope_id == pid)
            )
        return or_(*permitted)

    def _management_access(self, access):
        cid, pid = access.conversation.key, access.principal.key
        permitted = [
            and_(
                social_records.c.scope_type == "principal_in_conversation",
                social_records.c.scope_id == cid,
                social_records.c.subject_ref == pid,
            )
        ]
        if access.role in {"admin", "owner"}:
            permitted.append(
                and_(social_records.c.scope_type == "conversation", social_records.c.scope_id == cid)
            )
        if access.role == "owner":
            permitted.append(social_records.c.scope_type == "global_curated")
            if access.conversation.surface.value == "private":
                permitted.append(
                    and_(social_records.c.scope_type == "owner_private", social_records.c.scope_id == pid)
                )
        return or_(*permitted)

    def _manageable(self, access, record):
        if record.scope_type == "principal_in_conversation":
            return record.scope_id == access.conversation.key and record.subject_ref == access.principal.key
        if record.scope_type == "conversation":
            return record.scope_id == access.conversation.key and access.role in {"admin", "owner"}
        if record.scope_type == "owner_private":
            return (
                record.scope_id == access.principal.key
                and access.role == "owner"
                and access.conversation.surface.value == "private"
            )
        return access.role == "owner"

    def _evidence(self, conn, access, record):
        rows = conn.execute(
            select(inbox.c.event_json).where(
                inbox.c.id.in_(record.source_event_ids), inbox.c.conversation_id == access.conversation.key
            )
        ).scalars()
        events = [InboundEvent.model_validate_json(row) for row in rows]
        if len(events) != len(set(record.source_event_ids)):
            raise ValueError("missing or inaccessible evidence")
        revoked = conn.execute(
            select(social_tombstones.c.source_id).where(
                social_tombstones.c.source_id.in_(record.source_event_ids)
            )
        ).first()
        if revoked or any(e.sender_is_bot or e.event_type != "message.created" for e in events):
            raise ValueError("invalid or revoked evidence")
        if record.scope_type in {"principal_in_conversation", "owner_private"} and any(
            e.principal.key != record.subject_ref for e in events
        ):
            raise ValueError("subject mismatch")
        if record.source_kind == "task_verified":
            raise ValueError("task evidence requires a separately verified task-result ingress")
        return events

    def _load(self, conn, access, memory_id):
        row = conn.execute(
            select(social_records.c.record_json).where(social_records.c.id == memory_id, self._access(access))
        ).scalar_one_or_none()
        if row is None:
            raise LookupError("memory unavailable")
        return MemoryRecord.model_validate_json(row)

    def _invalidate_summaries(self, conn, cid, source_ids):
        row = conn.execute(
            select(conversation_summaries.c.source_ids_json).where(
                conversation_summaries.c.conversation_id == cid
            )
        ).scalar_one_or_none()
        if row and set(json.loads(row)) & set(source_ids):
            conn.execute(
                update(conversation_summaries)
                .where(conversation_summaries.c.conversation_id == cid)
                .values(status="INVALID", summary_json="[]", source_ids_json="[]")
            )

    def _save(self, conn, record):
        if record.status == "deleted":
            cid = conn.execute(
                select(social_records.c.conversation_id).where(social_records.c.id == record.memory_id)
            ).scalar_one()
            record = record.model_copy(update={"text": "[deleted]", "value_json": "null", "embedding": None})
            if (
                conn.execute(
                    select(memory_deletions.c.memory_id).where(
                        memory_deletions.c.memory_id == record.memory_id
                    )
                ).first()
                is None
            ):
                conn.execute(
                    insert(memory_deletions).values(
                        memory_id=record.memory_id,
                        conversation_id=cid,
                        deleted_at=self.clock.now().isoformat(),
                        source_ids_json=json.dumps(record.source_event_ids),
                    )
                )
            self._invalidate_summaries(conn, cid, record.source_event_ids)
        conn.execute(
            update(social_records)
            .where(social_records.c.id == record.memory_id)
            .values(status=record.status, record_json=record.model_dump_json())
        )

    def _bump(self, conn, cid):
        # A global epoch conservatively invalidates every assembled draft after a memory mutation.
        cid = "__global_memory_epoch__"
        value = conn.execute(
            select(social_versions.c.version).where(social_versions.c.conversation_id == cid)
        ).scalar_one_or_none()
        if value is None:
            conn.execute(insert(social_versions).values(conversation_id=cid, version=1))
        else:
            conn.execute(
                update(social_versions)
                .where(social_versions.c.conversation_id == cid)
                .values(version=value + 1)
            )

    def version(self, access):
        with self.db.engine.connect() as conn:
            return (
                conn.execute(
                    select(social_versions.c.version).where(
                        social_versions.c.conversation_id == "__global_memory_epoch__"
                    )
                ).scalar_one_or_none()
                or 0
            )

    @staticmethod
    def _same_content(left, right):
        fields = (
            "scope_type",
            "scope_id",
            "subject_ref",
            "predicate",
            "value_json",
            "text",
            "source_event_ids",
            "source_kind",
            "sensitivity",
        )
        return all(getattr(left, field) == getattr(right, field) for field in fields)

    def candidate(
        self, access: AccessContext, record: MemoryRecord, *, auto_enabled=False, explicit=False
    ) -> MemoryRecord:
        if not explicit and not auto_enabled:
            raise PermissionError("automatic extraction disabled")
        if record.status != "candidate" or not self._manageable(access, record):
            raise PermissionError("candidate scope not manageable")
        if record.scope_type == "global_curated" and not explicit:
            raise PermissionError("global facts require curation")
        if record.valid_until is not None and record.valid_until <= record.valid_from:
            raise ValueError("invalid validity interval")
        _safe(record.text + record.value_json)
        json.loads(record.value_json)
        with self.db.transaction() as conn:
            self._evidence(conn, access, record)
            previous = conn.execute(
                select(social_records.c.record_json).where(
                    social_records.c.id == record.memory_id,
                    social_records.c.conversation_id == access.conversation.key,
                )
            ).scalar_one_or_none()
            if previous is not None:
                existing = MemoryRecord.model_validate_json(previous)
                if not self._same_content(existing, record):
                    raise ValueError("memory id conflicts with existing content")
                return existing
            conn.execute(
                insert(social_records).values(
                    id=record.memory_id,
                    conversation_id=access.conversation.key,
                    scope_type=record.scope_type,
                    scope_id=record.scope_id,
                    subject_ref=record.subject_ref,
                    status=record.status,
                    record_json=record.model_dump_json(),
                )
            )
        return record

    def commit(self, access, memory_id, *, authorized=False):
        if not authorized:
            raise PermissionError("explicit commit authorization required")
        with self.db.transaction() as conn:
            record = self._load(conn, access, memory_id)
            if not self._manageable(access, record):
                raise PermissionError("memory not manageable")
            if record.status != "candidate":
                return record
            self._evidence(conn, access, record)
            existing = conn.execute(
                select(social_records.c.record_json).where(
                    self._access(access),
                    social_records.c.status == "active",
                    social_records.c.scope_type == record.scope_type,
                    social_records.c.scope_id == record.scope_id,
                    social_records.c.subject_ref == record.subject_ref,
                )
            ).scalars()
            status = "active"
            for raw in existing:
                old = MemoryRecord.model_validate_json(raw)
                if old.predicate != record.predicate:
                    continue
                if old.value_json == record.value_json:
                    record = record.model_copy(
                        update={"status": "superseded", "supersedes_id": old.memory_id}
                    )
                    self._save(conn, record)
                    return old
                if record.source_kind == "user_explicit" and old.source_kind == "inference":
                    self._save(conn, old.model_copy(update={"status": "superseded", "embedding": None}))
                    record = record.model_copy(update={"supersedes_id": old.memory_id})
                else:
                    status = "disputed"
                    self._save(conn, old.model_copy(update={"status": "disputed", "embedding": None}))
            record = record.model_copy(update={"status": status, "last_verified_at": self.clock.now()})
            self._save(conn, record)
            conn.execute(
                insert(social_index_jobs).values(id=str(uuid4()), memory_id=record.memory_id, state="PENDING")
            )
            self._bump(conn, access.conversation.key)
            return record

    def retrieve(self, access, query: str = "", *, limit=8, embedding=None, model_id=None, version=None):
        now = self.clock.now()
        with self.db.engine.connect() as conn:
            raw = conn.execute(
                select(social_records.c.record_json).where(
                    self._access(access), social_records.c.status == "active"
                )
            ).scalars()
            records = [MemoryRecord.model_validate_json(row) for row in raw]
            source_ids = {source for record in records for source in record.source_event_ids}
            revoked = set(
                conn.execute(
                    select(social_tombstones.c.source_id).where(social_tombstones.c.source_id.in_(source_ids))
                ).scalars()
            )
            records = [record for record in records if not revoked.intersection(record.source_event_ids)]
        records = [
            r for r in records if r.valid_from <= now and (r.valid_until is None or r.valid_until > now)
        ]
        tokens = keywords(query)
        lexical = sorted(
            ((len(tokens & keywords(r.text)), r) for r in records), key=lambda item: item[0], reverse=True
        )
        lexical = [r for score, r in lexical if score > 0 or not query][:20]
        vectors = []
        if embedding is not None:
            from qqbot.memory.embedding import cosine

            vectors = sorted(
                [
                    r
                    for r in records
                    if r.embedding
                    and r.embedding_model_id == model_id
                    and r.embedding_version == version
                    and len(r.embedding) == len(embedding)
                ],
                key=lambda r: cosine(embedding, r.embedding),
                reverse=True,
            )[:20]
        ranks = {}
        by_id = {}
        for ranking in (lexical, vectors):
            for rank, record in enumerate(ranking):
                ranks[record.memory_id] = ranks.get(record.memory_id, 0) + 1 / (60 + rank + 1)
                by_id[record.memory_id] = record
        return [by_id[key] for key in sorted(ranks, key=ranks.get, reverse=True)[: max(0, min(limit, 8))]]

    def forget(self, access, memory_id):
        with self.db.transaction() as conn:
            record = self._load(conn, access, memory_id)
            if not self._manageable(access, record):
                raise PermissionError("memory not manageable")
            self._save(conn, record.model_copy(update={"status": "deleted", "embedding": None}))
            conn.execute(delete(social_index_jobs).where(social_index_jobs.c.memory_id == memory_id))
            # A deletion tombstones evidence so an old summary cannot reintroduce it.
            for source in record.source_event_ids:
                if (
                    conn.execute(
                        select(social_tombstones.c.source_id).where(social_tombstones.c.source_id == source)
                    ).first()
                    is None
                ):
                    conn.execute(
                        insert(social_tombstones).values(
                            source_id=source, conversation_id=access.conversation.key
                        )
                    )
            dependent_rows = (
                conn.execute(
                    select(social_records.c.record_json).where(
                        social_records.c.conversation_id == access.conversation.key
                    )
                )
                .scalars()
                .all()
            )
            for raw in dependent_rows:
                dependent = MemoryRecord.model_validate_json(raw)
                if set(dependent.source_event_ids) & set(record.source_event_ids):
                    self._save(conn, dependent.model_copy(update={"status": "deleted", "embedding": None}))
                    conn.execute(
                        delete(social_index_jobs).where(social_index_jobs.c.memory_id == dependent.memory_id)
                    )
            self._bump(conn, access.conversation.key)

    def correct(self, access, memory_id, replacement: MemoryRecord):
        _safe(replacement.text + replacement.value_json)
        json.loads(replacement.value_json)
        with self.db.transaction() as conn:
            old = self._load(conn, access, memory_id)
            if not self._manageable(access, old) or not self._manageable(access, replacement):
                raise PermissionError("memory not manageable")
            if (old.scope_type, old.scope_id, old.subject_ref, old.predicate) != (
                replacement.scope_type,
                replacement.scope_id,
                replacement.subject_ref,
                replacement.predicate,
            ):
                raise ValueError("correction cannot widen scope or change subject")
            if replacement.source_kind != "user_explicit":
                raise ValueError("correction must be explicit")
            self._evidence(conn, access, replacement)
            previous = conn.execute(
                select(social_records.c.record_json).where(
                    social_records.c.id == replacement.memory_id, self._access(access)
                )
            ).scalar_one_or_none()
            if previous is not None:
                existing = MemoryRecord.model_validate_json(previous)
                if existing.supersedes_id != old.memory_id or not self._same_content(existing, replacement):
                    raise ValueError("correction id conflicts with existing content")
                return existing
            if old.status == "deleted":
                raise ValueError("deleted memory cannot be corrected")
            self._save(conn, old.model_copy(update={"status": "superseded", "embedding": None}))
            new = replacement.model_copy(
                update={
                    "status": "active",
                    "supersedes_id": old.memory_id,
                    "embedding": None,
                    "last_verified_at": self.clock.now(),
                }
            )
            conn.execute(
                insert(social_records).values(
                    id=new.memory_id,
                    conversation_id=access.conversation.key,
                    scope_type=new.scope_type,
                    scope_id=new.scope_id,
                    subject_ref=new.subject_ref,
                    status=new.status,
                    record_json=new.model_dump_json(),
                )
            )
            conn.execute(
                insert(social_index_jobs).values(id=str(uuid4()), memory_id=new.memory_id, state="PENDING")
            )
            self._bump(conn, access.conversation.key)
            return new

    def revoke_source(self, access, source_id):
        with self.db.transaction() as conn:
            event = conn.execute(
                select(inbox.c.event_json).where(
                    inbox.c.id == source_id, inbox.c.conversation_id == access.conversation.key
                )
            ).scalar_one_or_none()
            if event is None:
                raise LookupError("source unavailable")
            source = InboundEvent.model_validate_json(event)
            if source.principal.key != access.principal.key and access.role not in {"admin", "owner"}:
                raise PermissionError("source not manageable")
            if (
                conn.execute(
                    select(social_tombstones.c.source_id).where(social_tombstones.c.source_id == source_id)
                ).first()
                is None
            ):
                conn.execute(
                    insert(social_tombstones).values(
                        source_id=source_id, conversation_id=access.conversation.key
                    )
                )
            self._invalidate_summaries(conn, access.conversation.key, (source_id,))
            rows = (
                conn.execute(
                    select(social_records.c.record_json).where(
                        social_records.c.conversation_id == access.conversation.key
                    )
                )
                .scalars()
                .all()
            )
            for raw in rows:
                record = MemoryRecord.model_validate_json(raw)
                if source_id in record.source_event_ids:
                    self._save(conn, record.model_copy(update={"status": "deleted", "embedding": None}))
                    conn.execute(
                        delete(social_index_jobs).where(social_index_jobs.c.memory_id == record.memory_id)
                    )
            self._bump(conn, access.conversation.key)

    def jargon(self, access, term, sense_id, definition, evidence_ids, *, allow_output=False):
        now = self.clock.now()
        candidate = MemoryRecord(
            scope_type="conversation",
            scope_id=access.conversation.key,
            subject_ref="",
            predicate="jargon:" + term + ":" + sense_id,
            value_json=json.dumps(definition),
            text=term + " " + definition,
            source_event_ids=tuple(evidence_ids),
            source_kind="observed_event",
            confidence=0.5,
            valid_from=now,
            created_at=now,
            last_verified_at=now,
        )
        _safe(candidate.text)
        with self.db.engine.connect() as conn:
            events = self._evidence(conn, access, candidate)
        # Repeated identical text does not count as a distinct context.
        contexts = {e.text.strip() for e in events}
        speakers = {e.principal.key for e in events}
        active = len(contexts) >= 3 and len(speakers) >= 2
        return JargonEntry(
            scope_id=access.conversation.key,
            term=term,
            sense_id=sense_id,
            definition=definition,
            example_event_ids=tuple(evidence_ids),
            speaker_count=len(speakers),
            occurrence_count=len(contexts),
            confidence=0.8 if active else 0.3,
            status="active" if active else "candidate",
            last_seen_at=now,
            forbidden_for_output=not (active and allow_output),
        )

    def store_embedding(self, access, memory_id, vector, model_id, version):
        from qqbot.memory.embedding import cosine

        cosine(vector, vector)
        with self.db.transaction() as conn:
            record = self._load(conn, access, memory_id)
            if not self._manageable(access, record) or record.status != "active":
                raise PermissionError("embedding target unavailable")
            self._evidence(conn, access, record)
            self._save(
                conn,
                record.model_copy(
                    update={
                        "embedding": tuple(vector),
                        "embedding_model_id": model_id,
                        "embedding_version": version,
                    }
                ),
            )
            conn.execute(
                update(social_index_jobs)
                .where(social_index_jobs.c.memory_id == memory_id)
                .values(state="DONE")
            )

    def learn_jargon(
        self, access, term, sense_id, definition, evidence_ids, *, authorized=False, allow_output=False
    ):
        if not authorized:
            raise PermissionError("jargon publication requires authorization")
        entry = self.jargon(access, term, sense_id, definition, evidence_ids, allow_output=allow_output)
        now = self.clock.now()
        record = MemoryRecord(
            scope_type="conversation",
            scope_id=access.conversation.key,
            subject_ref="",
            predicate="jargon:" + term + ":" + sense_id,
            value_json=entry.model_dump_json(),
            text=term + " " + definition,
            source_event_ids=tuple(evidence_ids),
            source_kind="observed_event",
            confidence=entry.confidence,
            valid_from=now,
            created_at=now,
            last_verified_at=now,
        )
        self.candidate(access, record, explicit=True)
        if entry.status == "active":
            self.commit(access, record.memory_id, authorized=True)
        return entry, record.memory_id

    def jargon_entries(self, access, term, *, for_output=False):
        result = []
        for record in self.retrieve(access, term):
            if not record.predicate.startswith("jargon:" + term + ":"):
                continue
            entry = JargonEntry.model_validate_json(record.value_json)
            if entry.expires_at and entry.expires_at <= self.clock.now():
                continue
            if not for_output or not entry.forbidden_for_output:
                result.append(entry)
        return result[:1] if for_output else result[:5]
