"""Persist Hermes conditional review counters with each actual Agent Session."""
from alembic import op

revision = "0020"
down_revision = "0019"
branch_labels = None
depends_on = None


def upgrade():
    from qqbot.storage.learning_schema import LEARNING_TABLES
    for table in LEARNING_TABLES:
        table.create(op.get_bind(), checkfirst=True)


def downgrade():
    raise RuntimeError("Learning receipts and pending reviews must survive code rollback")
