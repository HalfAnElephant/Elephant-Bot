"""Durable systemd Job intent and process identity."""
from alembic import op

revision = '0019'
down_revision = '0018'
branch_labels = None
depends_on = None


def upgrade():
    from qqbot.storage.job_schema import JOB_TABLES
    for table in JOB_TABLES:
        table.create(op.get_bind(), checkfirst=True)


def downgrade():
    raise RuntimeError('Job dispatch and process identities must survive code rollback')
