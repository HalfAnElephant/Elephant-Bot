"""Owner-scoped mailbox configuration, containing credential references only."""
from sqlalchemy import Column, Integer, String, Table, Text, UniqueConstraint

from elephant.storage.schema import metadata

mail_accounts = Table(
    'mail_accounts', metadata,
    Column('id', String, primary_key=True),
    Column('account_id', String, nullable=False),
    Column('owner_id', String, nullable=False),
    Column('principal', String, nullable=False),
    Column('conversation', String, nullable=False),
    Column('workspace_id', String, nullable=False),
    Column('event_json', Text, nullable=False),
    Column('spec_json', Text, nullable=False),
    Column('revision', Integer, nullable=False),
    Column('mailbox_generation', Integer, nullable=False, server_default='1'),
    Column('enabled', Integer, nullable=False),
    Column('credential_ref', String),
    Column('tested_revision', Integer),
    Column('last_test_at', String),
    Column('last_error', String),
    Column('created_at', String, nullable=False),
    Column('updated_at', String, nullable=False),
    UniqueConstraint('owner_id', 'principal', 'conversation', 'workspace_id', 'account_id'),
)
