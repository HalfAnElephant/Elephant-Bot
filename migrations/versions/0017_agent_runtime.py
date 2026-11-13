"""Leader attempts, task trees and pinned sessions, including experimental 0010."""
import sqlalchemy as sa
from alembic import op

revision = "0017"
down_revision = "0016"
branch_labels = None
depends_on = None


def upgrade():
    from qqbot.storage.agent_schema import AGENT_TABLES
    bind = op.get_bind()
    for table in AGENT_TABLES:
        table.create(bind, checkfirst=True)
    additions = {
        "tasks": [
            sa.Column("kind", sa.String, nullable=False, server_default="work"),
            sa.Column("owner_id", sa.String), sa.Column("root_task_id", sa.String),
            sa.Column("parent_task_id", sa.String), sa.Column("origin_dialogue_id", sa.String),
            sa.Column("coordinator_session_id", sa.String), sa.Column("session_id", sa.String),
            sa.Column("goal_revision", sa.Integer, nullable=False, server_default="0"),
            sa.Column("dispatch_key", sa.String), sa.Column("delegation_key", sa.String),
            sa.Column("rework_of_task_id", sa.String),
            sa.Column("binding_json", sa.Text, nullable=False, server_default="{}"),
            sa.Column("wait_json", sa.Text),
        ],
        "task_runs": [
            sa.Column("session_id", sa.String),
            sa.Column("role", sa.String, nullable=False, server_default="worker"),
            sa.Column("purpose", sa.String, nullable=False, server_default="work"),
            sa.Column("session_generation", sa.Integer, nullable=False, server_default="1"),
            sa.Column("basis_goal_revision", sa.Integer, nullable=False, server_default="0"),
            sa.Column("request_json", sa.Text, nullable=False, server_default="{}"),
            sa.Column("budget_json", sa.Text, nullable=False, server_default="{}"),
            sa.Column("state", sa.String, nullable=False, server_default="RUNNING"),
            sa.Column("receipt_json", sa.Text),
        ],
        "task_inputs": [
            sa.Column("kind", sa.String, nullable=False, server_default="followup"),
            sa.Column("actor", sa.String),
            sa.Column("goal_revision", sa.Integer, nullable=False, server_default="0"),
            sa.Column("persisted_at", sa.String), sa.Column("consumed_at", sa.String),
        ],
    }
    for name, columns in additions.items():
        existing = {column["name"] for column in sa.inspect(bind).get_columns(name)}
        for column in columns:
            if column.name not in existing:
                op.add_column(name, column)
    with op.batch_alter_table("task_runs") as batch:
        batch.alter_column("task_id", existing_type=sa.String(), nullable=True)
    op.create_index("uq_tasks_dispatch", "tasks", ["owner_id", "dispatch_key"], unique=True)
    op.create_index("uq_tasks_delegation", "tasks", ["parent_task_id", "delegation_key"], unique=True)
    op.create_index("ix_agent_task_tree", "tasks", ["root_task_id", "state"])


def downgrade():
    raise RuntimeError("Agent migration preserves task audit; restore through the documented export protocol")
