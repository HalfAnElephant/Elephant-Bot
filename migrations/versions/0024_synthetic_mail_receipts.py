"""Persist synthetic mail acceptance receipts without changing timer receipt semantics."""
from alembic import op
from sqlalchemy import Column, Integer, inspect

revision = '0024'
down_revision = '0023'
branch_labels = None
depends_on = None


def upgrade():
    from qqbot.storage.schedule_schema import synthetic_mail_receipts

    bind = op.get_bind()
    columns = {column['name'] for column in inspect(bind).get_columns('schedules')}
    for name in ('synthetic_mail_last_uid', 'synthetic_mail_activation_uid'):
        if name not in columns:
            op.add_column('schedules', Column(name, Integer()))
    synthetic_mail_receipts.create(bind, checkfirst=True)


def downgrade():
    raise RuntimeError('Synthetic mail receipts must survive code rollback')
