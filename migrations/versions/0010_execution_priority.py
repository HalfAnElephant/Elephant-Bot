"""Separate model scheduling priority from user-facing delivery mode."""

import sqlalchemy as sa
from alembic import op

revision = "0010"
down_revision = "0009"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    if "execution_priority" not in {column["name"] for column in sa.inspect(bind).get_columns("tasks")}:
        op.add_column(
            "tasks", sa.Column("execution_priority", sa.String(), nullable=False, server_default="BACKGROUND")
        )
    bind.execute(
        sa.text(
            "UPDATE tasks SET execution_priority = CASE WHEN delivery = 'ATTACHED' "
            "THEN 'INTERACTIVE' ELSE 'BACKGROUND' END"
        )
    )


def downgrade():
    op.drop_column("tasks", "execution_priority")
