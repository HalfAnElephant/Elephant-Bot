from sqlalchemy import Column, Integer, MetaData, String, Table, Text, UniqueConstraint

metadata = MetaData()
conversations = Table(
    "conversations",
    metadata,
    Column("id", String, primary_key=True),
    Column("ref_json", Text, nullable=False),
    Column("seq", Integer, nullable=False, default=0),
    Column("revocation", Integer, nullable=False, default=0),
)
inbox = Table(
    "inbox",
    metadata,
    Column("id", String, primary_key=True),
    Column("conversation_id", String, nullable=False),
    Column("seq", Integer, nullable=False),
    Column("event_json", Text, nullable=False),
    Column("state", String, nullable=False, default="PENDING"),
    Column("error", String),
    UniqueConstraint("conversation_id", "seq"),
)
outbox = Table(
    "outbox",
    metadata,
    Column("id", String, primary_key=True),
    Column("dedupe_key", String, nullable=False, unique=True),
    Column("conversation_id", String, nullable=False),
    Column("envelope_json", Text, nullable=False),
    Column("state", String, nullable=False),
    Column("receipt_json", Text),
    Column("attempts", Integer, nullable=False, default=0),
    Column("next_attempt_at", String),
    Column("created_at", String, nullable=False),
    Column("sent_at", String),
)
reply_counters = Table(
    "reply_counters",
    metadata,
    Column("key", String, primary_key=True),
    Column("seq", Integer, nullable=False),
)

inbox.append_column(Column("priority", Integer, nullable=False, default=0))
inbox.append_column(Column("event_type", String))
inbox.append_column(Column("message_id", String))
inbox.append_column(Column("principal_id", String))
inbox.append_column(Column("received_at", String))
outbox.append_column(Column("origin_id", String))
outbox.append_column(Column("origin_kind", String))
outbox.append_column(Column("part_no", Integer, default=0))
outbox.append_column(Column("platform_message_id", String))
outbox.append_column(Column("attempted_at", String))
runtime_state = Table(
    "runtime_state",
    metadata,
    Column("conversation_id", String, primary_key=True),
    Column("mode", String, nullable=False, default="addressed_only"),
    Column("policy_version", Integer, nullable=False, default=1),
    Column("state_json", Text, nullable=False, default="{}"),
)
decisions = Table(
    "speech_decisions",
    metadata,
    Column("event_id", String, primary_key=True),
    Column("conversation_id", String, nullable=False),
    Column("decision_json", Text, nullable=False),
    Column("created_at", String, nullable=False),
)
audit_events = Table(
    "audit_events",
    metadata,
    Column("id", String, primary_key=True),
    Column("kind", String, nullable=False),
    Column("target", String, nullable=False),
    Column("actor", String, nullable=False),
    Column("created_at", String, nullable=False),
    Column("detail", Text, nullable=False),
)
