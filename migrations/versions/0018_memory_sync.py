"""Incremental OpenViking sources, mutation epochs and durable deletion barriers."""
from alembic import op

revision = "0018"
down_revision = "0017"
branch_labels = None
depends_on = None


def upgrade():
    from qqbot.storage.memory_sync_schema import MEMORY_SYNC_TABLES
    for table in MEMORY_SYNC_TABLES:
        table.create(op.get_bind(), checkfirst=True)


def downgrade():
    raise RuntimeError("Memory deletion barriers must survive rollback; use the documented restore protocol")
