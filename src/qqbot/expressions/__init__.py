"""Delete legacy learned expressions when their original sources are revoked."""
import json
from datetime import UTC, datetime

from sqlalchemy import insert, select, update

from qqbot.domain import stable_id
from qqbot.storage.expression_schema import expression_tombstones, expression_versions, expressions

__all__ = ["invalidate_source"]


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
