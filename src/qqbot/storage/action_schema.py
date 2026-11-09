from sqlalchemy import Column, Integer, String, Table, Text

from qqbot.storage.schema import metadata

operations = Table(
    "action_operations", metadata,
    Column("id", String, primary_key=True),
    Column("task_id", String, nullable=False),
    Column("run_id", String, nullable=False),
    Column("fence", Integer, nullable=False),
    Column("arguments_hash", String, nullable=False),
    Column("action_json", Text, nullable=False),
    Column("arguments_json", Text, nullable=False),
    Column("state", String, nullable=False),
    Column("result_json", Text, nullable=False),
    Column("updated_at", String, nullable=False),
)
