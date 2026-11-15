"""Durable dialogue routing and source receipts; Pi alone owns model history."""
from sqlalchemy import Column, Integer, String, Table, Text, UniqueConstraint

from qqbot.storage.schema import metadata

dialogues = Table(
    "dialogues", metadata,
    Column("id", String, primary_key=True),
    Column("owner_id", String, nullable=False),
    Column("conversation_id", String, nullable=False),
    Column("title", String, nullable=False),
    Column("workspace_id", String, nullable=False, server_default="personal"),
    Column("session_id", String, unique=True, nullable=False),
    Column("binding_json", Text, nullable=False),
    Column("state", String, nullable=False),
    Column("created_at", String, nullable=False),
    Column("last_admitted_at", String),
)
focus = Table(
    "dialogue_focus", metadata,
    Column("key", String, primary_key=True),
    Column("dialogue_id", String, nullable=False),
)
dialogue_inputs = Table(
    "dialogue_inputs", metadata,
    Column("id", String, primary_key=True),
    Column("dialogue_id", String, nullable=False),
    Column("owner_id", String, nullable=False),
    Column("event_json", Text, nullable=False),
    Column("context_json", Text, nullable=False),
    Column("state", String, nullable=False),
    Column("created_at", String, nullable=False),
    Column("claimed_attempt_id", String),
    Column("persisted_at", String),
    Column("consumed_at", String),
    Column("handled_at", String),
)
leader_inbox = Table(
    "leader_inbox", metadata,
    Column("id", String, primary_key=True),
    Column("target_session_id", String, nullable=False),
    Column("root_task_id", String, nullable=False),
    Column("event_id", String, nullable=False),
    Column("resume_reason", String, nullable=False),
    Column("source_revision", Integer, nullable=False),
    Column("created_at", String, nullable=False),
    Column("claimed_attempt_id", String),
    Column("persisted_at", String),
    Column("handled_at", String),
    Column("output_ref", String),
    UniqueConstraint("target_session_id", "root_task_id", "event_id"),
)
agent_events = Table(
    "agent_events", metadata,
    Column("id", String, primary_key=True),
    Column("task_id", String, nullable=False),
    Column("attempt_id", String),
    Column("seq", Integer, nullable=False),
    Column("generation", Integer, nullable=False),
    Column("kind", String, nullable=False),
    Column("source_revision", Integer, nullable=False),
    Column("payload_json", Text, nullable=False),
    Column("created_at", String, nullable=False),
    UniqueConstraint("task_id", "seq"),
)
AGENT_TABLES = (dialogues, focus, dialogue_inputs, leader_inbox, agent_events)
