"""Freeze project identity with each persistent dialogue."""
from alembic import op
from sqlalchemy import Column, String, inspect

revision = "0023"
down_revision = "0022"
branch_labels = None
depends_on = None


def upgrade():
    columns = {column['name'] for column in inspect(op.get_bind()).get_columns('dialogues')}
    if 'workspace_id' not in columns:
        op.add_column('dialogues', Column('workspace_id', String(), nullable=False, server_default='personal'))
    inputs = {column['name'] for column in inspect(op.get_bind()).get_columns('task_inputs')}
    if 'source_event_id' not in inputs:
        op.add_column('task_inputs', Column('source_event_id', String()))


def downgrade():
    raise RuntimeError('Persistent dialogue scopes must survive code rollback')
