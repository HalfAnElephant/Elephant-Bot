from sqlalchemy import Column, Integer, String, Table, UniqueConstraint

from .schema import metadata

qq_local_media = Table("qq_local_media", metadata,
    Column("id", String, primary_key=True),
    Column("task_id", String, nullable=False),
    Column("conversation_id", String, nullable=False),
    Column("principal_key", String, nullable=False),
    Column("request_id", String, nullable=False),
    Column("sha256", String, nullable=False),
    Column("mime_type", String, nullable=False),
    Column("filename", String, nullable=False),
    Column("media_type", String, nullable=False),
    Column("size", Integer, nullable=False),
    Column("expires_at", String, nullable=False),
    UniqueConstraint("task_id", "conversation_id", "principal_key", "request_id"))
