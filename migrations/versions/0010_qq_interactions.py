"""Durable scoped official QQ button capabilities and interaction receipts."""
from alembic import op

from qqbot.storage.interaction_schema import qq_interaction_events, qq_interaction_tokens

revision = "0010"
down_revision = "0009"
branch_labels = None
depends_on = None


def upgrade():
    qq_interaction_tokens.create(op.get_bind())
    qq_interaction_events.create(op.get_bind())


def downgrade():
    qq_interaction_events.drop(op.get_bind())
    qq_interaction_tokens.drop(op.get_bind())
