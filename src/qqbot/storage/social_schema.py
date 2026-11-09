from sqlalchemy import Column, Integer, String, Table, Text

from qqbot.storage.schema import metadata

social_records = Table(
    "social_records",
    metadata,
    Column("id", String, primary_key=True),
    Column("conversation_id", String, nullable=False),
    Column("scope_type", String, nullable=False),
    Column("scope_id", String, nullable=False),
    Column("subject_ref", String, nullable=False),
    Column("status", String, nullable=False),
    Column("record_json", Text, nullable=False),
)
social_tombstones = Table(
    "social_tombstones",
    metadata,
    Column("source_id", String, primary_key=True),
    Column("conversation_id", String, nullable=False),
)
social_versions = Table(
    "social_versions",
    metadata,
    Column("conversation_id", String, primary_key=True),
    Column("version", Integer, nullable=False),
)
social_index_jobs = Table(
    "social_index_jobs",
    metadata,
    Column("id", String, primary_key=True),
    Column("memory_id", String, nullable=False),
    Column("state", String, nullable=False),
)
SOCIAL_TABLES = (social_records, social_tombstones, social_versions, social_index_jobs)

memory_checkpoints = Table(
    "memory_checkpoints",
    metadata,
    Column("conversation_id", String, primary_key=True),
    Column("summary_seq", Integer, nullable=False),
    Column("extraction_seq", Integer, nullable=False),
    Column("extraction_state", String, nullable=False),
)
conversation_summaries = Table(
    "conversation_summaries",
    metadata,
    Column("conversation_id", String, primary_key=True),
    Column("summary_json", Text, nullable=False),
    Column("source_ids_json", Text, nullable=False),
    Column("status", String, nullable=False),
    Column("updated_at", String, nullable=False),
)
memory_deletions = Table(
    "memory_deletions",
    metadata,
    Column("memory_id", String, primary_key=True),
    Column("conversation_id", String, nullable=False),
    Column("deleted_at", String, nullable=False),
    Column("source_ids_json", Text, nullable=False),
)
MAINTENANCE_TABLES = (memory_checkpoints, conversation_summaries, memory_deletions)

memory_source_retention = Table(
    "memory_source_retention",
    metadata,
    Column("source_id", String, primary_key=True),
    Column("conversation_id", String, nullable=False),
    Column("redacted_at", String, nullable=False),
)
MAINTENANCE_TABLES = (*MAINTENANCE_TABLES, memory_source_retention)
