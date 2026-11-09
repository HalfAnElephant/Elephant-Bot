"""Core-owned adaptation of MaiBot's default expression learning and fast selection.

Selection and expression formatting derive from MaisakaExpressionSelector, MaiBot
e5391507531706a8bfce011d55ed94628039f2de (GPL-3.0-only). See THIRD_PARTY_NOTICES.md.
"""

import json
import logging
from datetime import UTC, datetime, timedelta
from hashlib import sha256

from sqlalchemy import insert, select, update

from qqbot.domain import InboundEvent, stable_id
from qqbot.expressions.config import ExpressionConfig
from qqbot.expressions.expression_style_utils import (
    is_prompt_example_expression_style,
    normalize_expression_style_for_learning,
)
from qqbot.expressions.learner import HTTPLearner, LearnedExpression, LearningResult
from qqbot.expressions.weighted_sampling import weighted_sample
from qqbot.memory import _safe
from qqbot.permissions import PermissionService
from qqbot.storage.expression_schema import (
    expression_checkpoints,
    expression_tombstones,
    expression_versions,
    expressions,
)
from qqbot.storage.schema import inbox
from qqbot.storage.social_schema import memory_source_retention, social_tombstones

__all__ = [
    "ExpressionConfig", "ExpressionService", "ExpressionVersionConflict", "HTTPLearner",
    "LearnedExpression", "LearningResult", "build_habits", "invalidate_source",
]
logger = logging.getLogger(__name__)


class ExpressionVersionConflict(ValueError):
    pass


def _fingerprint(conversation_id, situation, style):
    return stable_id("maibot-expression", conversation_id, situation, style)


def _bump(conn, conversation_id):
    existing = conn.execute(select(expression_versions.c.version).where(
        expression_versions.c.conversation_id == conversation_id,
    )).scalar_one_or_none()
    if existing is None:
        conn.execute(insert(expression_versions).values(conversation_id=conversation_id, version=1))
    else:
        conn.execute(update(expression_versions).where(
            expression_versions.c.conversation_id == conversation_id,
        ).values(version=existing + 1))


def _tombstone(conn, identity, conversation_id, now):
    if not conn.execute(select(expression_tombstones.c.expression_id).where(
        expression_tombstones.c.expression_id == identity,
    )).first():
        conn.execute(insert(expression_tombstones).values(
            expression_id=identity, conversation_id=conversation_id, deleted_at=now.isoformat(),
        ))


def _delete_record(conn, record, now):
    for identity in {record["id"], _fingerprint(record["conversation_id"], record["situation"], record["style"])}:
        _tombstone(conn, identity, record["conversation_id"], now)
    conn.execute(update(expressions).where(expressions.c.id == record["id"]).values(
        status="deleted", situation="", style="", sources_json="[]", checked=False,
        revision=record["revision"] + 1, updated_at=now.isoformat(),
    ))
    _bump(conn, record["conversation_id"])


def invalidate_source(conn, source_id, *, now=None):
    """Called in the same Core transaction that revokes or redacts source evidence."""
    now = now or datetime.now(UTC)
    records = conn.execute(select(expressions).where(expressions.c.status == "active")).mappings().all()
    for record in records:
        if source_id in {source["id"] for source in json.loads(record["sources_json"])}:
            _delete_record(conn, record, now)


def build_habits(records):
    if not records:
        return ""
    lines = [f'- 当"{item["situation"]}"时，可以用"{item["style"]}"来表达。' for item in records]
    return "你可以视情况自由使用以下句式：\n" + "\n".join(lines)


class ExpressionService:
    def __init__(self, db, clock, config=None, *, permissions=None):
        self.db, self.clock = db, clock
        self.config = config or ExpressionConfig()
        self.permissions = permissions or PermissionService()
        self._active = set()

    def _require(self, access, action, conversation_id=None):
        self.permissions.require(access, action, "conversation:" + (conversation_id or access.conversation.key))

    def _scopes(self, access):
        cid = access.conversation.key
        scopes = {cid}
        if access.conversation.surface != "private":
            for group in self.config.expression_groups:
                if cid in group:
                    scopes.update(group)
        return scopes

    def _eligible(self, event, access):
        if event.sender_is_bot or event.event_type != "message.created" or self.config.blacklisted(event.principal):
            return False
        text = event.text.strip()
        if not text or text.startswith("/") or len(text.encode()) > 8192:
            return False
        try:
            _safe(text)
        except ValueError:
            return False
        author_access = access.model_copy(update={"principal": event.principal, "conversation": event.conversation})
        if not self.permissions.check(
            author_access, "conversation.receive", "conversation:" + event.conversation.key,
        ).allowed:
            return False
        return all(self.permissions.check(access, action, resource).allowed for action, resource in (
            ("expression.source.read", "event:" + event.event_id),
            ("history.source.read", "event:" + event.event_id),
            ("conversation.receive", "conversation:" + event.conversation.key),
        ))

    def _source(self, conn, source_id, access, *, cache=None):
        if cache is not None and source_id in cache:
            return cache[source_id]
        raw = conn.execute(select(inbox.c.event_json).where(inbox.c.id == source_id)).scalar_one_or_none()
        blocked = conn.execute(select(social_tombstones.c.source_id).where(
            social_tombstones.c.source_id == source_id,
        )).first() or conn.execute(select(memory_source_retention.c.source_id).where(
            memory_source_retention.c.source_id == source_id,
        )).first()
        value = None
        if raw is not None and not blocked:
            event = InboundEvent.model_validate_json(raw)
            recalled = conn.execute(select(inbox.c.id).where(
                inbox.c.conversation_id == event.conversation.key,
                inbox.c.event_type == "message.recalled", inbox.c.message_id == event.platform_message_id,
            )).first()
            if not recalled and self._eligible(event, access):
                value = (event, sha256(raw.encode()).hexdigest())
        if cache is not None:
            cache[source_id] = value
        return value

    def _valid_record(self, conn, access, record, *, cache=None, for_use=True):
        if record["status"] != "active" or record["conversation_id"] not in self._scopes(access):
            return False
        if record["conversation_id"] != access.conversation.key and (
            record["surface"] == "private" or access.conversation.surface == "private"
        ):
            return False
        if for_use and self.config.expression_checked_only and not (record["checked"] and record["modified_by"] == "user"):
            return False
        if not self.permissions.check(access, "expression.read", "conversation:" + record["conversation_id"]).allowed:
            return False
        if conn.execute(select(expression_tombstones.c.expression_id).where(
            expression_tombstones.c.expression_id == record["id"],
        )).first():
            return False
        sources = json.loads(record["sources_json"])
        if not sources or is_prompt_example_expression_style(record["style"]):
            return False
        for source in sources:
            current = self._source(conn, source["id"], access, cache=cache)
            if current is None or current[1] != source["digest"] or current[0].conversation.key != record["conversation_id"]:
                return False
        return True

    @staticmethod
    def _public(record):
        values = {key: value for key, value in record.items() if key != "sources_json"}
        values["source_event_ids"] = [source["id"] for source in json.loads(record["sources_json"])]
        return values

    def select(self, access):
        if not self.config.enabled or not self.config.rule(access.conversation).use:
            return []
        if not self.permissions.check(access, "expression.use", "conversation:" + access.conversation.key).allowed:
            return []
        with self.db.engine.connect() as conn:
            rows = conn.execute(select(expressions).where(
                expressions.c.conversation_id.in_(self._scopes(access)), expressions.c.status == "active",
            )).mappings().all()
            cache = {}
            candidates = [self._public(row) for row in rows if self._valid_record(conn, access, row, cache=cache)]
        # MaiBot's fast mode waits for ten candidates before its first sample.
        if len(candidates) < 10:
            return []
        high_count = [item for item in candidates if item["count"] > 1]
        selected_high = weighted_sample(high_count, 5) if len(high_count) >= 10 else []
        selected_random = weighted_sample(candidates, 5)
        selected, seen = [], set()
        for candidate in [*selected_high, *selected_random]:
            if candidate["id"] not in seen:
                selected.append(candidate)
                seen.add(candidate["id"])
        if selected:
            with self.db.transaction() as conn:
                conn.execute(update(expressions).where(expressions.c.id.in_(seen)).values(
                    last_active_time=self.clock.now().isoformat(),
                ))
        return selected

    def validate_record(self, access, expression_id, revision):
        if not self.config.enabled or not self.config.rule(access.conversation).use:
            return False
        if not self.permissions.check(access, "expression.use", "conversation:" + access.conversation.key).allowed:
            return False
        with self.db.engine.connect() as conn:
            record = conn.execute(select(expressions).where(expressions.c.id == expression_id)).mappings().first()
            return bool(record and record["revision"] == revision and self._valid_record(conn, access, record))

    def list(self, access, *, include_deleted=False, limit=200):
        self._require(access, "expression.read")
        with self.db.engine.connect() as conn:
            query = select(expressions).where(expressions.c.conversation_id == access.conversation.key)
            if not include_deleted:
                query = query.where(expressions.c.status == "active")
            rows = conn.execute(query.order_by(expressions.c.updated_at.desc(), expressions.c.id).limit(
                max(1, min(limit, 500)),
            )).mappings().all()
            # Deleted-source text is never exposed even before background cleanup runs.
            cache = {}
            return [self._public(row) for row in rows if row["status"] == "deleted"
                    or self._valid_record(conn, access, row, cache=cache, for_use=False)]

    def _managed(self, conn, access, expression_id, expected_revision):
        self._require(access, "expression.manage")
        record = conn.execute(select(expressions).where(
            expressions.c.id == expression_id, expressions.c.conversation_id == access.conversation.key,
        )).mappings().first()
        if record is None:
            raise LookupError("expression unavailable")
        if type(expected_revision) is not int or record["revision"] != expected_revision:
            raise ExpressionVersionConflict("expression revision changed")
        return record

    def review(self, access, expression_id, *, checked, expected_revision, situation=None, style=None):
        if type(checked) is not bool:
            raise ValueError("checked must be a boolean")
        now = self.clock.now()
        with self.db.transaction() as conn:
            record = self._managed(conn, access, expression_id, expected_revision)
            if record["status"] != "active":
                raise ValueError("deleted expression cannot be reviewed")
            if not self._valid_record(conn, access, record, for_use=False):
                raise ValueError("expression source is unavailable")
            situation = record["situation"] if situation is None else situation
            style = record["style"] if style is None else style
            if not isinstance(situation, str) or not isinstance(style, str):
                raise TypeError("expression content must be text")
            checked_value = LearnedExpression(
                situation=situation.strip(), style=normalize_expression_style_for_learning(style),
                source_event_id="manual-review",
            )
            _safe(checked_value.situation + " " + checked_value.style)
            if is_prompt_example_expression_style(checked_value.style):
                raise ValueError("prompt example cannot be used as expression material")
            new_identity = _fingerprint(record["conversation_id"], checked_value.situation, checked_value.style)
            if new_identity != record["id"] and conn.execute(select(expression_tombstones.c.expression_id).where(
                expression_tombstones.c.expression_id == new_identity,
            )).first():
                raise ValueError("deleted expression cannot be recreated by review")
            duplicate = conn.execute(select(expressions.c.id).where(
                expressions.c.conversation_id == record["conversation_id"],
                expressions.c.id != record["id"], expressions.c.status == "active",
                expressions.c.situation == checked_value.situation, expressions.c.style == checked_value.style,
            )).first()
            if duplicate:
                raise ValueError("review conflicts with an existing expression")
            # Keep the original stable ID so existing references fail by revision, not by identity reuse.
            conn.execute(update(expressions).where(expressions.c.id == expression_id).values(
                checked=checked, situation=checked_value.situation, style=checked_value.style,
                modified_by="user", revision=record["revision"] + 1, updated_at=now.isoformat(),
            ))
            _bump(conn, record["conversation_id"])
            updated = conn.execute(select(expressions).where(expressions.c.id == expression_id)).mappings().one()
            return self._public(updated)

    def delete(self, access, expression_id, *, expected_revision):
        with self.db.transaction() as conn:
            record = self._managed(conn, access, expression_id, expected_revision)
            if record["status"] != "deleted":
                _delete_record(conn, record, self.clock.now())

    def status(self, access):
        self._require(access, "expression.read")
        rule = self.config.rule(access.conversation)
        with self.db.engine.connect() as conn:
            row = conn.execute(select(expression_checkpoints).where(
                expression_checkpoints.c.conversation_id == access.conversation.key,
            )).mappings().first()
        return {
            "enabled": self.config.enabled, "use": self.config.enabled and rule.use,
            "learn": self.config.enabled and rule.learn,
            "state": row["state"] if row else "PENDING", "reason": row["reason"] if row else "",
            "next_attempt_at": row["next_attempt_at"] if row else None,
            "pending_count": len(json.loads(row["pending_json"])) if row else 0,
            "cursor": row["scan_seq"] if row else 0, "failures": row["failures"] if row else 0,
        }

    def _checkpoint(self, conn, cid, **values):
        if conn.execute(select(expression_checkpoints.c.conversation_id).where(
            expression_checkpoints.c.conversation_id == cid,
        )).first():
            conn.execute(update(expression_checkpoints).where(expression_checkpoints.c.conversation_id == cid).values(**values))
        else:
            conn.execute(insert(expression_checkpoints).values(conversation_id=cid, **values))

    async def maintenance_once(self, access, learner):
        cid, now = access.conversation.key, self.clock.now()
        if not self.config.enabled or not self.config.rule(access.conversation).learn:
            return {"state": "SKIPPED", "reason": "learning_disabled"}
        if not self.permissions.check(access, "expression.learn", "conversation:" + cid).allowed:
            return {"state": "SKIPPED", "reason": "learning_not_permitted"}
        if cid in self._active or len(self._active) >= self.config.max_expression_learner:
            return {"state": "SKIPPED", "reason": "learner_capacity"}
        self._active.add(cid)
        try:
            return await self._maintain(access, learner, now)
        finally:
            self._active.discard(cid)

    async def _maintain(self, access, learner, now):
        cid = access.conversation.key
        with self.db.transaction() as conn:
            checkpoint = conn.execute(select(expression_checkpoints).where(
                expression_checkpoints.c.conversation_id == cid,
            )).mappings().first()
            if checkpoint and datetime.fromisoformat(checkpoint["next_attempt_at"]) > now:
                return {"state": "SKIPPED", "reason": "cooldown"}
            cursor = checkpoint["scan_seq"] if checkpoint else 0
            failures = checkpoint["failures"] if checkpoint else 0
            pending_ids = json.loads(checkpoint["pending_json"]) if checkpoint else []
            source_cache, events, sources = {}, [], {}

            def collect(source_id):
                source = self._source(conn, source_id, access, cache=source_cache)
                if source is None or source[0].conversation.key != cid or source_id in sources:
                    return
                event, digest = source
                author_access = access.model_copy(update={"principal": event.principal})
                if not self.permissions.check(author_access, "expression.learn", "conversation:" + cid).allowed:
                    return
                if sum(len(item.text.encode()) for item in events) + len(event.text.encode()) > 65536:
                    return
                events.append(event)
                sources[source_id] = digest

            for source_id in pending_ids:
                collect(source_id)
            if len(events) < self.config.max_messages:
                rows = conn.execute(select(inbox.c.id, inbox.c.seq).where(
                    inbox.c.conversation_id == cid, inbox.c.seq > cursor,
                ).order_by(inbox.c.seq).limit(self.config.max_messages * 10)).all()
                for source_id, seq in rows:
                    if len(events) >= self.config.max_messages:
                        break
                    cursor = seq
                    collect(source_id)
            pending = [event.event_id for event in events]
            base = {"scan_seq": cursor, "pending_json": json.dumps(pending), "failures": failures,
                    "updated_at": now.isoformat(), "next_attempt_at": (now + timedelta(
                        seconds=self.config.learn_interval_seconds,
                    )).isoformat()}
            if learner is None or not getattr(learner, "configured", True):
                self._checkpoint(conn, cid, **base, state="SKIPPED", reason="missing_model_configuration")
                return {"state": "SKIPPED", "reason": "missing_model_configuration"}
            if len(events) < self.config.min_messages:
                self._checkpoint(conn, cid, **base, state="PENDING", reason="waiting_for_messages")
                return {"state": "PENDING", "pending": len(events)}
            self._checkpoint(conn, cid, **base, state="RUNNING", reason="")
        try:
            result = await learner.learn(events, self_reflect=self.config.expression_self_reflect)
        except Exception:
            logger.exception("expression learning failed")
            result = LearningResult(state="FAILED", reason="learner_error")
        now = self.clock.now()
        with self.db.transaction() as conn:
            if not self.config.enabled or not self.config.rule(access.conversation).learn:
                self._checkpoint(conn, cid, **base, state="SKIPPED", reason="learning_disabled")
                return {"state": "SKIPPED", "reason": "learning_disabled"}
            if result.state != "DONE":
                failures += result.state == "FAILED"
                delay = min(3600, self.config.learn_interval_seconds * 2 ** min(failures, 5))
                self._checkpoint(conn, cid, **{**base, "failures": failures,
                    "next_attempt_at": (now + timedelta(seconds=delay)).isoformat(),
                    "updated_at": now.isoformat()}, state=result.state, reason=result.reason)
                return {"state": result.state, "reason": result.reason}
            written, rejected = 0, result.rejected
            for candidate in result.candidates:
                source = self._source(conn, candidate.source_event_id, access)
                if (candidate.source_event_id not in sources or source is None
                        or source[1] != sources[candidate.source_event_id] or source[0].conversation.key != cid):
                    rejected += 1
                    continue
                if not self.permissions.check(access, "expression.learn", "conversation:" + cid).allowed:
                    rejected += 1
                    continue
                author_access = access.model_copy(update={"principal": source[0].principal})
                if not self.permissions.check(author_access, "expression.learn", "conversation:" + cid).allowed:
                    rejected += 1
                    continue
                if self._upsert(conn, access, candidate, sources[candidate.source_event_id], now):
                    written += 1
            self._checkpoint(conn, cid, **{**base, "pending_json": "[]", "failures": 0,
                "next_attempt_at": (now + timedelta(seconds=self.config.learn_interval_seconds)).isoformat(),
                "updated_at": now.isoformat()}, state="DONE", reason="")
        return {"state": "DONE", "written": written, "rejected": rejected}

    def _upsert(self, conn, access, candidate, source_digest, now):
        candidate = LearnedExpression.model_validate(candidate)
        _safe(candidate.situation + " " + candidate.style)
        if is_prompt_example_expression_style(candidate.style):
            return False
        cid = access.conversation.key
        identity = _fingerprint(cid, candidate.situation, candidate.style)
        if conn.execute(select(expression_tombstones.c.expression_id).where(
            expression_tombstones.c.expression_id == identity,
        )).first():
            return False
        old = conn.execute(select(expressions).where(
            expressions.c.conversation_id == cid, expressions.c.situation == candidate.situation,
            expressions.c.style == candidate.style, expressions.c.status == "active",
        )).mappings().first()
        source = {"id": candidate.source_event_id, "digest": source_digest}
        if old:
            sources = json.loads(old["sources_json"])
            if any(item["id"] == source["id"] for item in sources):
                return False
            # shortcut: retain at most 100 sources; use a source table before learning more occurrences.
            if len(sources) >= 100:
                return False
            sources = [*sources, source]
            conn.execute(update(expressions).where(expressions.c.id == old["id"]).values(
                count=old["count"] + 1, sources_json=json.dumps(sources), updated_at=now.isoformat(),
            ))
            return True
        existing = conn.execute(select(expressions.c.id).where(expressions.c.id == identity)).first()
        if existing:
            return False
        conn.execute(insert(expressions).values(
            id=identity, conversation_id=cid, surface=access.conversation.surface.value,
            situation=candidate.situation, style=candidate.style, count=1, checked=False,
            modified_by="ai", status="active", revision=1, sources_json=json.dumps([source]),
            created_at=now.isoformat(), updated_at=now.isoformat(), last_active_time=None,
        ))
        return True
