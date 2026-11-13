"""Persist authenticated schedules and exactly-once goal firing receipts."""
from alembic import op

revision = "0022"
down_revision = "0021"
branch_labels = None
depends_on = None


def upgrade():
    from qqbot.storage.schedule_schema import schedule_fires, schedules
    schedules.create(op.get_bind(), checkfirst=True)
    schedule_fires.create(op.get_bind(), checkfirst=True)


def downgrade():
    raise RuntimeError("Schedule firing receipts must survive code rollback")
