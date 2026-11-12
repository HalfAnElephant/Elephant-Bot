"""Persist bounded application-owned interactive workflow payloads."""
from alembic import op
from sqlalchemy import Column, Text, inspect

revision = "0012"
down_revision = "0011"
branch_labels = None
depends_on = None


def upgrade():
    if "payload_json" not in {c["name"] for c in inspect(op.get_bind()).get_columns("qq_interaction_tokens")}:
        op.add_column("qq_interaction_tokens", Column("payload_json", Text, nullable=False, server_default="{}"))


def downgrade():
    op.drop_column("qq_interaction_tokens", "payload_json")
