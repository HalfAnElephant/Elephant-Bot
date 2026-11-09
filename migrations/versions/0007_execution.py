"""General tasks, durable continuations, notifications and process ownership."""
from alembic import op
from sqlalchemy import inspect

from qqbot.storage.action_schema import operations
from qqbot.storage.schema import outbox
from qqbot.storage.task_schema import EXECUTION_TABLES, runs, tasks

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    for table in (tasks, runs, outbox):
        existing = {column["name"] for column in inspect(bind).get_columns(table.name)}
        for column in table.columns:
            if column.name not in existing:
                op.add_column(table.name, column._copy())
    for table in (*EXECUTION_TABLES, operations):
        table.create(bind, checkfirst=True)


def downgrade():
    raise RuntimeError("Restore the pre-upgrade backup; execution history cannot be losslessly downgraded")
