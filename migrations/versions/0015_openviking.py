"""Track scoped OpenViking snapshots and uncertain message writes."""
from alembic import op

from qqbot.storage.openviking_schema import openviking_scopes

revision = "0015"
down_revision = "0014"
branch_labels = None
depends_on = None


def upgrade():
    openviking_scopes.create(op.get_bind())


def downgrade():
    openviking_scopes.drop(op.get_bind())
