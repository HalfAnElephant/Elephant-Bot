"""Indexed runtime queues, policy state and audit metadata."""
from alembic import op
import sqlalchemy as sa
revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("inbox", sa.Column("priority", sa.Integer, nullable=False, server_default="0"))
    for name in ("event_type", "message_id", "principal_id", "received_at"):
        op.add_column("inbox", sa.Column(name, sa.String))
    for name in ("origin_id", "origin_kind", "platform_message_id", "attempted_at"):
        op.add_column("outbox", sa.Column(name, sa.String))
    op.add_column("outbox", sa.Column("part_no", sa.Integer))
    op.execute("UPDATE inbox SET event_type=json_extract(event_json,'$.event_type'),message_id=json_extract(event_json,'$.platform_message_id'),received_at=json_extract(event_json,'$.received_at')")
    op.execute("UPDATE outbox SET origin_id=json_extract(envelope_json,'$.origin_id'),origin_kind=json_extract(envelope_json,'$.origin_kind'),part_no=json_extract(envelope_json,'$.part_no'),platform_message_id=json_extract(receipt_json,'$.platform_message_id')")
    op.create_index("ix_inbox_pending", "inbox", ["state", "conversation_id", "seq"])
    op.create_index("ix_inbox_message", "inbox", ["conversation_id", "message_id", "event_type"])
    op.create_index("ix_outbox_pending", "outbox", ["state", "created_at"])
    op.create_index("ix_outbox_origin", "outbox", ["conversation_id", "origin_id", "origin_kind", "part_no"])
    op.create_index("ix_outbox_receipt", "outbox", ["conversation_id", "platform_message_id"])
    op.create_table("runtime_state", sa.Column("conversation_id", sa.String, primary_key=True), sa.Column("mode", sa.String, nullable=False), sa.Column("policy_version", sa.Integer, nullable=False), sa.Column("state_json", sa.Text, nullable=False))
    op.create_table("speech_decisions", sa.Column("event_id", sa.String, primary_key=True), sa.Column("conversation_id", sa.String, nullable=False), sa.Column("decision_json", sa.Text, nullable=False), sa.Column("created_at", sa.String, nullable=False))
    op.create_table("audit_events", sa.Column("id", sa.String, primary_key=True), sa.Column("kind", sa.String, nullable=False), sa.Column("target", sa.String, nullable=False), sa.Column("actor", sa.String, nullable=False), sa.Column("created_at", sa.String, nullable=False), sa.Column("detail", sa.Text, nullable=False))


def downgrade():
    for name in ("audit_events", "speech_decisions", "runtime_state"):
        op.drop_table(name)
    for table, names in (("outbox", ("ix_outbox_receipt", "ix_outbox_origin", "ix_outbox_pending")), ("inbox", ("ix_inbox_message", "ix_inbox_pending"))):
        for name in names:
            op.drop_index(name, table_name=table)
    for table, names in (("outbox", ("origin_id", "origin_kind", "platform_message_id", "attempted_at", "part_no")), ("inbox", ("priority", "event_type", "message_id", "principal_id", "received_at"))):
        with op.batch_alter_table(table) as batch:
            for name in names:
                batch.drop_column(name)
