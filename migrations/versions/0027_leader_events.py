"""Persist a common arrival sequence for the two Leader source stores."""
from alembic import op

from elephant.storage.agent_schema import leader_events

revision = "0027"
down_revision = "0026"
branch_labels = None
depends_on = None


def upgrade():
    leader_events.create(op.get_bind(), checkfirst=True)
    op.execute("""
        INSERT OR IGNORE INTO leader_events (dialogue_id, source_kind, source_id)
        SELECT dialogue_id, source_kind, source_id FROM (
            SELECT dialogue_id, 'user' AS source_kind, id AS source_id, created_at FROM dialogue_inputs
            UNION ALL
            SELECT coalesce(tasks.origin_dialogue_id, dialogues.id), 'worker', leader_inbox.id, leader_inbox.created_at
            FROM leader_inbox JOIN tasks ON tasks.id = leader_inbox.root_task_id
            LEFT JOIN dialogues ON dialogues.session_id = leader_inbox.target_session_id
        ) WHERE dialogue_id IS NOT NULL ORDER BY created_at, source_id
    """)


def downgrade():
    leader_events.drop(op.get_bind())
