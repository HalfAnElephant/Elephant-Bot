"""Allow public Leader streams without a task and persist their final delivery."""
from alembic import op
from sqlalchemy import Column, String, inspect

revision = "0021"
down_revision = "0020"
branch_labels = None
depends_on = None


def upgrade():
    columns = {column["name"]: column for column in inspect(op.get_bind()).get_columns("qq_stream_runs")}
    with op.batch_alter_table("qq_stream_runs") as batch:
        if not columns["task_id"]["nullable"]:
            batch.alter_column("task_id", existing_type=String(), nullable=True)
        for name in ("result_origin_kind", "result_origin_id"):
            if name not in columns:
                batch.add_column(Column(name, String()))


def downgrade():
    raise RuntimeError("Leader stream receipts must survive code rollback")
