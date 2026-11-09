from sqlalchemy import Column, Integer, String, Table, Text, UniqueConstraint

from qqbot.storage.schema import metadata

tasks = Table(
    "tasks",
    metadata,
    Column("id", String, primary_key=True),
    Column("public_id", String, unique=True, nullable=False),
    Column("source", String, unique=True, nullable=False),
    Column("conversation", String, nullable=False),
    Column("requester", String, nullable=False),
    Column("event_json", Text, nullable=False),
    Column("goal", Text, nullable=False),
    Column("profile", String, nullable=False),
    Column("state", String, nullable=False),
    Column("created_at", String, nullable=False),
    Column("deadline", String, nullable=False),
    Column("fencing", Integer, nullable=False),
    Column("permission_json", Text, nullable=False),
    Column("retry_of_task_id", String),
    Column("stop_reason", String),
    Column("result_json", Text),
)
runs = Table(
    "task_runs",
    metadata,
    Column("id", String, primary_key=True),
    Column("task_id", String, nullable=False),
    Column("worker", String, nullable=False),
    Column("fencing", Integer, nullable=False),
    Column("lease", String, nullable=False),
    Column("slot_reserved", Integer, nullable=False),
    Column("runner_stopped_at", String),
    Column("session", String, nullable=False),
    Column("last_seq", Integer, nullable=False),
)
events = Table(
    "task_events",
    metadata,
    Column("event_id", String, primary_key=True),
    Column("run_id", String, nullable=False),
    Column("seq", Integer, nullable=False),
    Column("payload", Text, nullable=False),
    UniqueConstraint("run_id", "seq"),
)
