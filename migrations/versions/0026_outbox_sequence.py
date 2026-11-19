"""Persist Outbox insertion order across restarts."""
import sqlalchemy as sa
from alembic import op

revision = "0026"
down_revision = "0025"
branch_labels = None
depends_on = None


def upgrade():
    # Revision 0007 fills columns from live metadata on fresh databases.
    if "enqueue_seq" not in {column["name"] for column in sa.inspect(op.get_bind()).get_columns("outbox")}:
        op.add_column("outbox", sa.Column("enqueue_seq", sa.Integer, nullable=False, server_default="0"))
    op.execute("""
        WITH ordered AS (
            SELECT id, row_number() OVER (ORDER BY created_at, rowid) AS seq FROM outbox
        )
        UPDATE outbox SET enqueue_seq = (SELECT seq FROM ordered WHERE ordered.id = outbox.id)
    """)
    op.create_index("ix_outbox_enqueue_seq", "outbox", ["enqueue_seq"], unique=True)


def downgrade():
    op.drop_index("ix_outbox_enqueue_seq", table_name="outbox")
    op.drop_column("outbox", "enqueue_seq")
