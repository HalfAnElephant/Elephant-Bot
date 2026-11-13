"""Memory synchronization receipts and deletion barriers; no fact text lives here."""
from sqlalchemy import Column, Integer, String, Table, Text, UniqueConstraint

from qqbot.storage.schema import metadata

memory_owners = Table(
    "memory_owners", metadata,
    Column("id", String, primary_key=True), Column("scope_json", Text, nullable=False),
    Column("binding_json", Text, nullable=False), Column("binding_hash", String, nullable=False),
    Column("remote_identity", String, nullable=False, unique=True),
    Column("epoch", Integer, nullable=False), Column("state", String, nullable=False),
    Column("last_error", String), Column("updated_at", String, nullable=False),
)
memory_streams = Table(
    "memory_streams", metadata,
    Column("id", String, primary_key=True), Column("scope_id", String, nullable=False),
    Column("session_id", String, nullable=False), Column("branch", String, nullable=False),
    Column("projection_version", String, nullable=False), Column("epoch", Integer, nullable=False),
    Column("exported_seq", Integer, nullable=False), Column("extracted_seq", Integer, nullable=False),
    Column("indexed_seq", Integer, nullable=False),
)
memory_operations = Table(
    "memory_operations", metadata,
    Column("seq", Integer, primary_key=True, autoincrement=True),
    Column("id", String, nullable=False, unique=True), Column("scope_id", String, nullable=False),
    Column("stream_id", String), Column("kind", String, nullable=False),
    Column("epoch", Integer, nullable=False), Column("state", String, nullable=False),
    Column("refs_json", Text, nullable=False), Column("payload_hash", String, nullable=False),
    Column("remote_session_id", String), Column("remote_task_id", String), Column("archive_uri", String),
    Column("result_json", Text, nullable=False), Column("error_type", String),
    Column("created_at", String, nullable=False), Column("updated_at", String, nullable=False),
)
memory_sources = Table(
    "memory_sources", metadata,
    Column("id", String, primary_key=True), Column("scope_id", String, nullable=False),
    Column("source_id", String, nullable=False), Column("revision", String, nullable=False),
    Column("event_id", String, nullable=False), Column("operation_id", String, nullable=False),
    Column("content_hash", String, nullable=False), Column("state", String, nullable=False),
    UniqueConstraint("scope_id", "source_id", "revision"),
)
memory_documents = Table(
    "memory_documents", metadata,
    Column("id", String, primary_key=True), Column("scope_id", String, nullable=False),
    Column("uri", String, nullable=False), Column("content_hash", String, nullable=False),
    Column("epoch", Integer, nullable=False), Column("state", String, nullable=False),
    Column("index_state", String, nullable=False), Column("index_task_id", String),
    Column("updated_at", String, nullable=False),
    UniqueConstraint("scope_id", "uri"),
)
memory_derivations = Table(
    "memory_derivations", metadata,
    Column("id", String, primary_key=True), Column("scope_id", String, nullable=False),
    Column("uri", String, nullable=False), Column("source_id", String, nullable=False),
    Column("operation_id", String, nullable=False), Column("active", Integer, nullable=False, server_default="1"),
    UniqueConstraint("scope_id", "uri", "source_id", "operation_id"),
)
memory_barriers = Table(
    "memory_barriers", metadata,
    Column("id", String, primary_key=True), Column("scope_id", String, nullable=False),
    Column("epoch", Integer, nullable=False), Column("operation_id", String, nullable=False),
    Column("source_id", String), Column("uri", String), Column("allowed_hash", String),
    Column("created_at", String, nullable=False),
)
memory_changes = Table(
    "memory_changes", metadata,
    Column("seq", Integer, primary_key=True, autoincrement=True), Column("id", String, nullable=False, unique=True),
    Column("scope_id", String, nullable=False), Column("operation_id", String, nullable=False),
    Column("epoch", Integer, nullable=False), Column("kind", String, nullable=False),
    Column("refs_json", Text, nullable=False), Column("created_at", String, nullable=False),
)
MEMORY_SYNC_TABLES = (
    memory_owners, memory_streams, memory_operations, memory_sources, memory_documents,
    memory_derivations, memory_barriers, memory_changes,
)
