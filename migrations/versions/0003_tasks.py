"""Durable task control plane."""

from alembic import op

from qqbot.storage.task_schema import events, runs, tasks

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade():
    for table in (tasks, runs, events):
        table.create(op.get_bind())


def downgrade():
    for table in (events, runs, tasks):
        table.drop(op.get_bind())
