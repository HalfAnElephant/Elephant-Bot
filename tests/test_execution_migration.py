"""Upgrade preserves reserved legacy work and writes a restorable private backup."""
import sqlite3
import stat

from alembic import command
from alembic.config import Config

from qqbot.cli import migrate
from qqbot.storage.task_schema import runs, tasks


def test_execution_upgrade_backs_up_and_preserves_legacy_reservations(tmp_path):
    path = tmp_path / "legacy.sqlite"
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", "sqlite:///" + str(path))
    command.upgrade(config, "0005")
    original = {
        "tasks": {"id", "public_id", "source", "conversation", "requester", "event_json", "goal", "profile",
                  "state", "created_at", "deadline", "fencing", "permission_json", "retry_of_task_id", "stop_reason", "result_json"},
        "task_runs": {"id", "task_id", "worker", "fencing", "lease", "slot_reserved", "runner_stopped_at", "session", "last_seq"},
    }
    with sqlite3.connect(path) as db:
        for table in (tasks, runs):
            for column in table.columns:
                if column.name not in original[table.name]:
                    db.execute(f'ALTER TABLE "{table.name}" DROP COLUMN "{column.name}"')
        db.execute("INSERT INTO tasks (id,public_id,source,conversation,requester,event_json,goal,profile,state,"
            "created_at,deadline,fencing,permission_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("legacy", "T-legacy", "event", "group", "user", "{}", "original goal", "pi_research", "NEEDS_RECONCILIATION",
             "2026-01-01", "2026-01-02", 7, "{}"))
        db.execute("INSERT INTO task_runs (id,task_id,worker,fencing,lease,slot_reserved,session,last_seq) VALUES (?,?,?,?,?,?,?,?)",
            ("legacy-run", "legacy", "old-worker", 7, "2026-01-01", 1, "old-session", 2))
    migrate(str(path))
    backups = list(tmp_path.glob("legacy.sqlite.before-0008-*.db"))
    assert len(backups) == 1 and stat.S_IMODE(backups[0].stat().st_mode) == 0o600
    with sqlite3.connect(backups[0]) as old:
        assert old.execute("SELECT version_num FROM alembic_version").fetchone() == ("0005",)
        assert "protocol_version" not in {row[1] for row in old.execute("PRAGMA table_info(tasks)")}
        assert old.execute("PRAGMA integrity_check").fetchone() == ("ok",)
    with sqlite3.connect(path) as upgraded:
        assert upgraded.execute("SELECT version_num FROM alembic_version").fetchone() == ("0008",)
        assert upgraded.execute("SELECT protocol_version,goal,state,fencing FROM tasks").fetchone() == (
            1, "original goal", "NEEDS_RECONCILIATION", 7)
        assert upgraded.execute("SELECT slot_reserved,last_seq,session FROM task_runs").fetchone() == (1, 2, "old-session")
        assert upgraded.execute("PRAGMA integrity_check").fetchone() == ("ok",)
    migrate(str(path))
    assert list(tmp_path.glob("legacy.sqlite.before-0008-*.db")) == backups
