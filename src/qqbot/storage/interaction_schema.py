from sqlalchemy import Column, Integer, String, Table, Text

from .schema import metadata

# Button tokens are random, scoped, short-lived capabilities, stored only as hashes.
qq_interaction_tokens = Table(
    "qq_interaction_tokens", metadata,
    Column("token_hash", String, primary_key=True),
    Column("conversation_key", String, nullable=False),
    Column("principal_key", String, nullable=False),
    Column("action", String, nullable=False),
    Column("payload_json", Text, nullable=False, server_default="{}"),
    Column("group_key", String, nullable=True),
    Column("expires_at", String, nullable=False),
    Column("consumed_by", String, nullable=True),
)
qq_interaction_events = Table(
    "qq_interaction_events", metadata,
    Column("event_key", String, primary_key=True),
    Column("interaction_id", String, nullable=False),
    Column("conversation_key", String, nullable=False),
    Column("principal_key", String, nullable=False),
    Column("action", String, nullable=True),
    Column("ack_code", Integer, nullable=False),
    Column("state", String, nullable=False),
    Column("created_at", String, nullable=False),
    Column("details", Text, nullable=True),
)
