"""Scoped memories and revocation records."""

from alembic import op

from qqbot.storage.social_schema import SOCIAL_TABLES

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade():
    for table in SOCIAL_TABLES:
        table.create(op.get_bind())


def downgrade():
    for table in reversed(SOCIAL_TABLES):
        table.drop(op.get_bind())
