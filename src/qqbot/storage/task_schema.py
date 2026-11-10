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

# Defaults preserve imported tasks; new claims explicitly negotiate protocol 2.
for column in (
    Column("protocol_version", Integer, nullable=False, server_default="1"),
    Column("goal_json", Text, nullable=False, server_default="{}"),
    Column("failure_notice_json", Text),
    Column("plan_json", Text, nullable=False, server_default="{}"),
    Column("context_json", Text, nullable=False, server_default="{}"),
    Column("delivery", String, nullable=False, server_default="DETACHED"),
    Column("execution_priority", String, nullable=False, server_default="BACKGROUND"),
    Column("detach_at", String),
    Column("continuation_revision", Integer, nullable=False, server_default="0"),
    Column("budget_json", Text, nullable=False, server_default="{}"),
):
    tasks.append_column(column)
for column in (
    Column("started_at", String),
    Column("accounted_at", String),
    Column("quiescent_at", String),
    Column("process_id", String),
    Column("process_epoch", Integer),
    Column("binding_generation", Integer),
    Column("parent_run_id", String),
    Column("run_reason", String, nullable=False, server_default="initial"),
    Column("result_json", Text),
):
    runs.append_column(column)

notifications = Table(
    "task_notifications", metadata,
    Column("id", String, primary_key=True),
    Column("task_id", String, nullable=False),
    Column("run_id", String),
    Column("sequence", Integer, nullable=False),
    Column("purpose", String, nullable=False),
    Column("importance", String, nullable=False),
    Column("body", Text, nullable=False),
    Column("created_at", String, nullable=False),
    Column("state", String, nullable=False),
    UniqueConstraint("task_id", "sequence"),
)
waits = Table(
    "task_waits", metadata,
    Column("id", String, primary_key=True),
    Column("task_id", String, nullable=False),
    Column("run_id", String, nullable=False),
    Column("revision", Integer, nullable=False),
    Column("kind", String, nullable=False),
    Column("notification_id", String),
    Column("checkpoint", Text, nullable=False),
    Column("condition_json", Text, nullable=False, server_default="{}"),
    Column("state", String, nullable=False),
    Column("created_at", String, nullable=False),
    Column("delivery_deadline", String),
    Column("delivered_at", String),
    Column("expires_at", String),
    UniqueConstraint("task_id", "revision"),
)
inputs = Table(
    "task_inputs", metadata,
    Column("id", String, primary_key=True),
    Column("task_id", String, nullable=False),
    Column("question_id", String),
    Column("source_id", String, nullable=False, unique=True),
    Column("text", Text, nullable=False),
    Column("state", String, nullable=False),
    Column("created_at", String, nullable=False),
)
sessions = Table(
    "pi_sessions", metadata,
    Column("id", String, primary_key=True),
    Column("scope_key", String, nullable=False),
    Column("session_file", Text, nullable=False),
    Column("manifest_json", Text, nullable=False),
    Column("active_run_id", String),
    Column("epoch", Integer, nullable=False),
)
processes = Table(
    "session_processes", metadata,
    Column("id", String, primary_key=True),
    Column("session_id", String, nullable=False),
    Column("worker", String, nullable=False),
    Column("epoch", Integer, nullable=False),
    Column("generation", Integer, nullable=False),
    Column("state", String, nullable=False),
    Column("identity_json", Text, nullable=False),
    Column("last_used", String, nullable=False),
    Column("stopped_at", String),
)
model_requests = Table(
    "model_requests", metadata,
    Column("id", String, primary_key=True),
    Column("task_id", String, nullable=False),
    Column("run_id", String, nullable=False),
    Column("kind", String, nullable=False),
    Column("state", String, nullable=False),
    Column("usage_json", Text),
    Column("created_at", String, nullable=False),
)
EXECUTION_TABLES = (notifications, waits, inputs, sessions, processes, model_requests)

observations = Table(
    "task_observations", metadata,
    Column("id", String, primary_key=True),
    Column("task_id", String, nullable=False),
    Column("run_id", String, nullable=False),
    Column("retrieved_at", String, nullable=False),
    Column("sha256", String, nullable=False),
    Column("data_json", Text, nullable=False),
)
EXECUTION_TABLES = (*EXECUTION_TABLES, observations)
