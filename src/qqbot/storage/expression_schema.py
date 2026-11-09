from sqlalchemy import Boolean, Column, Integer, String, Table, Text

from qqbot.storage.schema import metadata

expressions = Table(
    "expressions", metadata,
    Column("id", String, primary_key=True),
    Column("conversation_id", String, nullable=False, index=True),
    Column("surface", String, nullable=False),
    Column("situation", Text, nullable=False),
    Column("style", Text, nullable=False),
    Column("count", Integer, nullable=False),
    Column("checked", Boolean, nullable=False),
    Column("modified_by", String, nullable=False),
    Column("status", String, nullable=False),
    Column("revision", Integer, nullable=False),
    Column("sources_json", Text, nullable=False),
    Column("created_at", String, nullable=False),
    Column("updated_at", String, nullable=False),
    Column("last_active_time", String, nullable=True),
)
expression_checkpoints = Table(
    "expression_checkpoints", metadata,
    Column("conversation_id", String, primary_key=True),
    Column("scan_seq", Integer, nullable=False),
    Column("pending_json", Text, nullable=False),
    Column("state", String, nullable=False),
    Column("failures", Integer, nullable=False),
    Column("next_attempt_at", String, nullable=False),
    Column("updated_at", String, nullable=False),
    Column("reason", String, nullable=False),
)
expression_tombstones = Table(
    "expression_tombstones", metadata,
    Column("expression_id", String, primary_key=True),
    Column("conversation_id", String, nullable=False),
    Column("deleted_at", String, nullable=False),
)
expression_versions = Table(
    "expression_versions", metadata,
    Column("conversation_id", String, primary_key=True),
    Column("version", Integer, nullable=False),
)
EXPRESSION_TABLES = (expressions, expression_checkpoints, expression_tombstones, expression_versions)
