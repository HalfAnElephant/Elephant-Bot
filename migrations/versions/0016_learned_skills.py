"""Scoped, versioned procedural skills; activation follows source task success."""
from alembic import op

from qqbot.storage.skill_schema import learned_skills

revision = "0016"
down_revision = "0015"
branch_labels = None
depends_on = None


def upgrade():
    learned_skills.create(op.get_bind())


def downgrade():
    learned_skills.drop(op.get_bind())
