"""Persist QQ relationship removals and proactive opt-out events."""
from alembic import op

from qqbot.storage.relationship_schema import qq_relationship_events, qq_relationships

revision = "0013"
down_revision = "0012"
branch_labels = None
depends_on = None


def upgrade():
    qq_relationships.create(op.get_bind())
    qq_relationship_events.create(op.get_bind())


def downgrade():
    qq_relationship_events.drop(op.get_bind())
    qq_relationships.drop(op.get_bind())
