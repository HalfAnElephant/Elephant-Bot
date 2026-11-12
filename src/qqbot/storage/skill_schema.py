from sqlalchemy import Column, Index, Integer, String, Table, Text, UniqueConstraint

from qqbot.storage.schema import metadata

learned_skills = Table(
    "learned_skills", metadata,
    Column("id", String, primary_key=True),
    Column("scope", String, nullable=False),
    Column("name", String, nullable=False),
    Column("version", Integer, nullable=False),
    Column("task_id", String, nullable=False),
    Column("run_id", String, nullable=False),
    Column("policy_version", Integer, nullable=False),
    Column("document_json", Text, nullable=False),
    Column("fingerprint", String, nullable=False),
    Column("state", String, nullable=False),
    Column("created_at", String, nullable=False),
    UniqueConstraint("scope", "name", "version"),
    UniqueConstraint("scope", "task_id", "name"),
)
Index("ix_learned_skills_scope_name", learned_skills.c.scope, learned_skills.c.name)
