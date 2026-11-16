from sqlalchemy import Column, Integer, String, Table

from .schema import metadata

qq_relationships = Table("qq_relationships", metadata,
    Column("conversation_id", String, primary_key=True),
    Column("dimension", String, primary_key=True),
    Column("allowed", Integer, nullable=False),
    Column("occurred_at", String, nullable=False),
    Column("event_id", String, nullable=False))
qq_relationship_events = Table("qq_relationship_events", metadata,
    Column("key", String, primary_key=True), Column("received_at", String, nullable=False))
