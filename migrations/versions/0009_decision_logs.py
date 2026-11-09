"""Keep each speech evaluation while preserving the latest-decision contract."""

import sqlalchemy as sa
from alembic import op

from qqbot.storage.schema import decision_logs, decisions

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    decision_logs.create(bind)
    bind.execute(sa.insert(decision_logs).from_select(
        ["event_id", "conversation_id", "created_at", "decision_json", "context_json"],
        sa.select(
            decisions.c.event_id, decisions.c.conversation_id, decisions.c.created_at,
            decisions.c.decision_json, sa.literal('{"legacy":true}'),
        ).order_by(decisions.c.created_at, decisions.c.event_id),
    ))


def downgrade():
    decision_logs.drop(op.get_bind())
