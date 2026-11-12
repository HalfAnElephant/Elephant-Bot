"""Persist native QQ C2C stream delivery intent and reconciliation state."""
from alembic import op

from qqbot.storage.stream_schema import stream_messages, stream_runs

revision = "0011"
down_revision = "0010"
branch_labels = None
depends_on = None


def upgrade():
    stream_runs.create(op.get_bind())
    stream_messages.create(op.get_bind())


def downgrade():
    stream_messages.drop(op.get_bind())
    stream_runs.drop(op.get_bind())
