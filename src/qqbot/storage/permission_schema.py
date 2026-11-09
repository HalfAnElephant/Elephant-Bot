from sqlalchemy import Column, Integer, String, Table, Text

from qqbot.storage.schema import metadata

permission_policy = Table(
    "permission_policy", metadata,
    Column("id", Integer, primary_key=True),
    Column("version", Integer, nullable=False),
    Column("policy_json", Text, nullable=False),
)
permission_audit = Table(
    "permission_audit", metadata,
    Column("version", Integer, primary_key=True),
    Column("actor", String, nullable=False),
    Column("operation", String, nullable=False),
    Column("rule_id", String, nullable=True),
    Column("before_json", Text, nullable=False),
    Column("after_json", Text, nullable=False),
    Column("created_at", String, nullable=False),
)
PERMISSION_TABLES = (permission_policy, permission_audit)
