"""Published and experimental schemas upgrade without replacing history."""
import sqlite3

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory

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
        assert conn.execute("SELECT version_num FROM alembic_version").fetchone() == (
            ScriptDirectory.from_config(cfg).get_current_head(),)
        assert conn.execute("SELECT detail FROM audit_events WHERE id='existing'").fetchone() == ("preserve",)
        assert conn.execute("SELECT count(*) FROM qq_interaction_tokens").fetchone() == (0,)
        assert conn.execute("PRAGMA integrity_check").fetchone() == ("ok",)
    assert not list(tmp_path.glob("*.before-*"))


def test_leader_stream_upgrade_preserves_existing_delivery_receipts(tmp_path):
    path = tmp_path / "streams.db"
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", f"sqlite:///{path}")
    command.upgrade(cfg, "0020")
    with sqlite3.connect(path) as conn:
        # Historical migrations import current metadata, so recreate the deployed 0020 table.
        conn.execute("DROP TABLE qq_stream_runs")
        conn.execute("CREATE TABLE qq_stream_runs (run_id VARCHAR PRIMARY KEY, task_id VARCHAR NOT NULL, "
                     "state VARCHAR NOT NULL, cursor_seq INTEGER NOT NULL DEFAULT 0, error VARCHAR)")
        conn.execute("INSERT INTO qq_stream_runs VALUES ('old-run','old-task','UNKNOWN',8,'INTERRUPTED_SEND')")
        conn.execute("INSERT INTO qq_stream_messages "
                     "(id,run_id,message_key,kind,state,envelope_json,created_at) "
                     "VALUES ('old-message','old-run','answer','answer','UNKNOWN','{}','2026-11-13')")
    migrate(str(path), backup=False)
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT * FROM qq_stream_runs").fetchone() == (
            "old-run", "old-task", "UNKNOWN", 8, "INTERRUPTED_SEND", None, None)
        assert conn.execute("SELECT state FROM qq_stream_messages").fetchone() == ("UNKNOWN",)
        conn.execute("INSERT INTO qq_stream_runs (run_id,task_id,state) VALUES ('leader',NULL,'ACTIVE')")
        assert conn.execute("PRAGMA integrity_check").fetchone() == ("ok",)
