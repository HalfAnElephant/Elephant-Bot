"""Per-Agent Hermes learning counters and one durable conditional review intent."""
from sqlalchemy import Column, String, Table, Text

from qqbot.storage.schema import metadata

learning_sessions = Table(
    "agent_learning_sessions", metadata,
    Column("session_id", String, primary_key=True),
    Column("owner_id", String, nullable=False),
    Column("state_json", Text, nullable=False),
    Column("pending_json", Text),
    Column("claimed_attempt_id", String),
    Column("updated_at", String, nullable=False),
)
learning_events = Table(
    "agent_learning_events", metadata,
    Column("id", String, primary_key=True),
    Column("session_id", String, nullable=False),
    Column("run_id", String, nullable=False),
    Column("kind", String, nullable=False),
    Column("created_at", String, nullable=False),
)
LEARNING_TABLES = (learning_sessions, learning_events)
