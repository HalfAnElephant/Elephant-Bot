"""Schedules retain their authenticated source and immutable firing receipts."""
from sqlalchemy import Column, Index, Integer, String, Table, Text, UniqueConstraint

from qqbot.storage.schema import metadata

schedules = Table(
    "schedules", metadata,
    Column("id", String, primary_key=True),
    Column("owner_id", String, nullable=False),
    Column("principal", String, nullable=False),
    Column("conversation", String, nullable=False),
    Column("creation_key", String, nullable=False),
    Column("creation_json", Text, nullable=False),
    Column("event_json", Text, nullable=False),
    Column("dialogue_id", String, nullable=False),
    Column("spec_json", Text, nullable=False),
    Column("source_context_json", Text, nullable=False, server_default="{}"),
    Column("revision", Integer, nullable=False),
    Column("enabled", Integer, nullable=False),
    Column("next_fire_at", String),
    Column("last_fire_at", String),
    Column("last_task_id", String),
    Column("last_error", String),
    Column("created_at", String, nullable=False),
    Column("updated_at", String, nullable=False),
    Column("deleted_at", String),
    UniqueConstraint("owner_id", "principal", "conversation", "creation_key"),
)
Index("ix_schedules_due", schedules.c.enabled, schedules.c.deleted_at, schedules.c.next_fire_at)

schedule_fires = Table(
    "schedule_fires", metadata,
    Column("id", String, primary_key=True),
    Column("schedule_id", String, nullable=False),
    Column("scheduled_fire_at", String, nullable=False),
    Column("schedule_revision", Integer, nullable=False),
    Column("task_id", String, nullable=False),
    Column("created_at", String, nullable=False),
    UniqueConstraint("schedule_id", "scheduled_fire_at"),
)
