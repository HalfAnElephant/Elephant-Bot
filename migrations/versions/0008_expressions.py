"""MaiBot expression material, source evidence, durable learning and deletion ledger."""
from alembic import op

from qqbot.storage.expression_schema import EXPRESSION_TABLES

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade():
    for table in EXPRESSION_TABLES:
        table.create(op.get_bind(), checkfirst=True)


def downgrade():
    raise RuntimeError("Restore the pre-upgrade backup; learned expressions cannot be losslessly downgraded")
