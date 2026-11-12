"""Job intent and systemd receipt, independent of a Harness Attempt lifecycle."""
from sqlalchemy import Column, Integer, String, Table, Text, UniqueConstraint

from qqbot.storage.schema import metadata

jobs = Table(
    'execution_jobs', metadata,
    Column('id', String, primary_key=True),
    Column('job_key', String, nullable=False),
    Column('task_id', String, nullable=False),
    Column('root_task_id', String, nullable=False),
    Column('owner_id', String, nullable=False),
    Column('workspace', String, nullable=False),
    Column('world_id', String, nullable=False),
    Column('arguments_hash', String, nullable=False),
    Column('request_json', Text, nullable=False),
    Column('state', String, nullable=False),
    Column('unit', String, unique=True, nullable=False),
    Column('description', String, nullable=False),
    Column('identity_json', Text),
    Column('deadline', String, nullable=False),
    Column('created_at', String, nullable=False),
    Column('updated_at', String, nullable=False),
    Column('exit_code', Integer),
    Column('cancel_requested_at', String),
    Column('stopped_at', String),
    Column('log_ref', String, nullable=False),
    Column('error', String),
    UniqueConstraint('task_id', 'job_key'),
)

JOB_TABLES = (jobs,)
