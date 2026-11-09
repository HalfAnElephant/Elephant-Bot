"""Versioned application permission policy and mutation audit."""
from alembic import op

from qqbot.storage.permission_schema import PERMISSION_TABLES

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade():
    for table in PERMISSION_TABLES:
        table.create(op.get_bind())


def downgrade():
    for table in reversed(PERMISSION_TABLES):
        table.drop(op.get_bind())
