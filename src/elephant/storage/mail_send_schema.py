"""Frozen outgoing mail and durable SMTP outcome receipts."""
from sqlalchemy import Column, Index, Integer, LargeBinary, String, Table, Text, UniqueConstraint

from elephant.storage.schema import metadata

mail_outgoing = Table(
    "mail_outgoing", metadata,
    Column("id", String, primary_key=True),
    Column("owner_id", String, nullable=False),
    Column("principal", String, nullable=False),
    Column("conversation", String, nullable=False),
    Column("workspace_id", String, nullable=False),
    Column("account_id", String, nullable=False),
    Column("account_revision", Integer, nullable=False),
    Column("transport_snapshot", Text, nullable=False),
    Column("credential_ref", String, nullable=False),
    Column("scope_id", String, nullable=False),
    Column("idempotency_key", String, nullable=False),
    Column("request_hash", String, nullable=False),
    Column("content_hash", String, nullable=False),
    Column("content_json", Text, nullable=False),
    Column("wire", LargeBinary, nullable=False),
    Column("message_id", String, nullable=False),
    Column("state", String, nullable=False),
    Column("result_json", Text, nullable=False),
    Column("created_at", String, nullable=False),
    Column("updated_at", String, nullable=False),
    UniqueConstraint("owner_id", "principal", "conversation", "workspace_id", "scope_id", "idempotency_key"),
)
Index("ix_mail_outgoing_content_scope", mail_outgoing.c.owner_id, mail_outgoing.c.scope_id,
      mail_outgoing.c.content_hash)
