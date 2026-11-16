"""Persist mailbox credentials references, receive cursors and frozen send attempts."""
from alembic import op

revision = "0025"
down_revision = "0024"
branch_labels = None
depends_on = None


def upgrade():
    from elephant.storage.mail_schema import mail_accounts
    from elephant.storage.mail_send_schema import mail_outgoing
    from elephant.storage.mail_trigger_schema import mail_received, mail_rule_receipts, mail_rule_state

    for table in (mail_accounts, mail_outgoing, mail_rule_state, mail_received, mail_rule_receipts):
        table.create(op.get_bind(), checkfirst=True)


def downgrade():
    raise RuntimeError("Mail receive and send receipts must survive code rollback")
