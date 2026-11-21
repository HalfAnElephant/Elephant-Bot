"""Index idle sessions due for context compaction."""
from alembic import op

revision = "0029"
down_revision = "0028"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_pi_sessions_compaction_due ON pi_sessions (
            coalesce(json_extract(manifest_json, '$.context_compaction.retry_at'), ''), id
        ) WHERE active_run_id IS NULL AND json_extract(manifest_json, '$.context_compaction.elastic_due') = 1
    """)


def downgrade():
    op.drop_index("ix_pi_sessions_compaction_due", table_name="pi_sessions")
