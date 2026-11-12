"""Immutable scoped QQ local media staging."""
from alembic import op

from qqbot.storage.local_media_schema import qq_local_media

revision = "0014"
down_revision = "0013"
branch_labels = None
depends_on = None


def upgrade():
    qq_local_media.create(op.get_bind())


def downgrade():
    qq_local_media.drop(op.get_bind())
