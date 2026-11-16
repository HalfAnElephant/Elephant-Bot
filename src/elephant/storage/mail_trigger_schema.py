"""Mailbox epochs, immutable inputs and per-rule transactional firing receipts."""
from sqlalchemy import Column, Integer, String, Table, Text, UniqueConstraint

from elephant.storage.schema import metadata

mail_rule_state = Table(
    "mail_rule_state", metadata,
    Column("schedule_id", String, primary_key=True),
    Column("account_id", String, nullable=False),
    Column("account_revision", Integer, nullable=False),
    Column("mailbox_generation", Integer, nullable=False),
    Column("uidvalidity", Integer, nullable=False),
    Column("last_uid", Integer, nullable=False),
    Column("activation_uid", Integer, nullable=False),
    Column("generation", Integer, nullable=False),
)

mail_received = Table(
    "mail_received", metadata,
    Column("id", String, primary_key=True),
    Column("account_id", String, nullable=False),
    Column("folder", String, nullable=False),
    Column("mailbox_generation", Integer, nullable=False),
    Column("uidvalidity", Integer, nullable=False),
    Column("uid", Integer, nullable=False),
    Column("message_json", Text, nullable=False),
    Column("digest", String, nullable=False),
    Column("created_at", String, nullable=False),
    UniqueConstraint("account_id", "mailbox_generation", "folder", "uidvalidity", "uid"),
)

mail_rule_receipts = Table(
    "mail_rule_receipts", metadata,
    Column("id", String, primary_key=True),
    Column("schedule_id", String, nullable=False),
    Column("message_id", String, nullable=False),
    Column("schedule_revision", Integer, nullable=False),
    Column("generation", Integer, nullable=False),
    Column("permissions_json", Text, nullable=False),
    Column("task_id", String, nullable=False),
    Column("created_at", String, nullable=False),
    UniqueConstraint("schedule_id", "message_id"),
)
