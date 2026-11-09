"""Incremental memory maintenance and content-free deletion ledger."""

from alembic import op

from qqbot.storage.social_schema import MAINTENANCE_TABLES

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade():
    for table in MAINTENANCE_TABLES:
        table.create(op.get_bind())


def downgrade():
    for table in reversed(MAINTENANCE_TABLES):
        table.drop(op.get_bind())
