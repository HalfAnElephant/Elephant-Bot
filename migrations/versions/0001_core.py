"""Core inbox/outbox schema."""
from alembic import op
import sqlalchemy as sa
revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table("conversations", sa.Column("id", sa.String, primary_key=True), sa.Column("ref_json", sa.Text, nullable=False), sa.Column("seq", sa.Integer, nullable=False), sa.Column("revocation", sa.Integer, nullable=False))
    op.create_table("inbox", sa.Column("id", sa.String, primary_key=True), sa.Column("conversation_id", sa.String, nullable=False), sa.Column("seq", sa.Integer, nullable=False), sa.Column("event_json", sa.Text, nullable=False), sa.Column("state", sa.String, nullable=False), sa.Column("error", sa.String), sa.UniqueConstraint("conversation_id", "seq"))
    op.create_table("outbox", sa.Column("id", sa.String, primary_key=True), sa.Column("dedupe_key", sa.String, nullable=False, unique=True), sa.Column("conversation_id", sa.String, nullable=False), sa.Column("envelope_json", sa.Text, nullable=False), sa.Column("state", sa.String, nullable=False), sa.Column("receipt_json", sa.Text), sa.Column("attempts", sa.Integer, nullable=False), sa.Column("next_attempt_at", sa.String), sa.Column("created_at", sa.String, nullable=False), sa.Column("sent_at", sa.String))
    op.create_table("reply_counters", sa.Column("key", sa.String, primary_key=True), sa.Column("seq", sa.Integer, nullable=False))


def downgrade():
    for name in ["reply_counters", "outbox", "inbox", "conversations"]:
        op.drop_table(name)
