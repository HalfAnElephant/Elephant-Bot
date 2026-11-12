"""Both published and experimental 0010 upgrade without replacing history."""
import sqlite3

import pytest
from alembic import command
from alembic.config import Config

from qqbot.cli import migrate


@pytest.mark.parametrize("experimental", [False, True])
def test_upgrade_0010_preserves_history(tmp_path, experimental):
    path = tmp_path / "legacy.db"
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", f"sqlite:///{path}")
    command.upgrade(cfg, "0010")
    with sqlite3.connect(path) as conn:
        conn.execute("INSERT INTO audit_events VALUES ('existing','test','task','user','2026-11-13','preserve')")
        if experimental:
            conn.execute("DROP TABLE qq_interaction_events")
            conn.execute("DROP TABLE qq_interaction_tokens")
            conn.execute("ALTER TABLE tasks ADD COLUMN execution_priority INTEGER NOT NULL DEFAULT 0")
    migrate(str(path), backup=False)
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT version_num FROM alembic_version").fetchone() == ("0020",)
        assert conn.execute("SELECT detail FROM audit_events WHERE id='existing'").fetchone() == ("preserve",)
        assert conn.execute("SELECT count(*) FROM qq_interaction_tokens").fetchone() == (0,)
        assert conn.execute("PRAGMA integrity_check").fetchone() == ("ok",)
    assert not list(tmp_path.glob("*.before-*"))
