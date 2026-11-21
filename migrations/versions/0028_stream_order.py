"""Give the first persisted QQ answer frame a place in the outbox order."""
import sqlalchemy as sa
from alembic import op

revision = "0028"
down_revision = "0027"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    if "enqueue_seq" not in {column["name"] for column in sa.inspect(bind).get_columns("qq_stream_runs")}:
        op.add_column("qq_stream_runs", sa.Column("enqueue_seq", sa.Integer))
    op.create_index("ix_qq_stream_runs_enqueue_seq", "qq_stream_runs", ["enqueue_seq"], if_not_exists=True)


def downgrade():
    op.drop_index("ix_qq_stream_runs_enqueue_seq", table_name="qq_stream_runs")
    op.drop_column("qq_stream_runs", "enqueue_seq")
