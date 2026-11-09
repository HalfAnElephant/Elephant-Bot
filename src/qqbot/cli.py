import argparse
import fcntl
import json
import os
import socket
import sqlite3
import stat
import tempfile
from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory

from qqbot.config import Settings, load_runtime_settings, load_settings


def migrate(path: str):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", f"sqlite:///{path}")
    target_revision = ScriptDirectory.from_config(cfg).get_current_head()
    if Path(path).exists() and Path(path).stat().st_size:
        from datetime import UTC, datetime
        with sqlite3.connect(path) as source:
            try:
                revision = source.execute("SELECT version_num FROM alembic_version").fetchone()
            except sqlite3.OperationalError:
                revision = None
            if not revision or revision[0] != target_revision:
                backup_path = str(path) + f".before-{target_revision}-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S%f") + ".db"
                descriptor = os.open(backup_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                os.close(descriptor)
                with sqlite3.connect(backup_path) as destination:
                    source.backup(destination)
    command.upgrade(cfg, "head")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=["doctor", "validate", "migrate", "serve", "schema", "backup", "restore", "export-deletions"],
    )
    parser.add_argument("--config", default="configs/example.yaml")
    parser.add_argument("--destination")
    parser.add_argument("--snapshot")
    parser.add_argument("--deletion-ledger")
    args = parser.parse_args()
    startup_settings = load_settings(args.config)
    settings = load_runtime_settings(startup_settings)
    if args.command in {"backup", "restore", "export-deletions"}:
        from qqbot.storage.operations import backup, export_deletions, restore

        if not args.destination:
            parser.error("--destination required")
        if args.command == "backup":
            backup(settings.database, args.destination)
        elif args.command == "export-deletions":
            export_deletions(settings.database, args.destination)
        else:
            if not args.snapshot or not args.deletion_ledger:
                parser.error("restore requires --snapshot and a current --deletion-ledger")
            restore(args.snapshot, args.destination, args.deletion_ledger)
        print(args.command + " complete")
    elif args.command == "schema":
        print(json.dumps(Settings.model_json_schema(), ensure_ascii=False, indent=2))
    elif args.command == "validate":
        print("configuration valid")
    elif args.command == "migrate":
        migrate(settings.database)
        print("migration complete")
    elif args.command == "doctor":
        with tempfile.TemporaryDirectory() as folder:
            db = str(Path(folder) / "doctor.db")
            migrate(db)
            with sqlite3.connect(db) as connection:
                assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
                connection.execute("CREATE VIRTUAL TABLE probe USING fts5(content)")
        print(
            json.dumps(
                {
                    "offline": "passed",
                    "adapters_configured": len(settings.adapters),
                    "qq_live": "not_tested",
                    "pi_model_run": "not_tested",
                }
            )
        )
    else:
        import uvicorn

        from qqbot.app import create_app

        Path(settings.database).parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        lock = os.open(settings.database + ".core.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if settings.tasks_enabled or settings.interactive_execution_enabled:
                clean_stale_socket(settings.internal_socket)
            uvicorn.run(create_app(startup_settings), host="127.0.0.1", port=8080)
        finally:
            os.close(lock)


def clean_stale_socket(path):
    """Called only while the database's exclusive Core process lock is held."""
    path = Path(path)
    try:
        info = path.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid():
        raise RuntimeError("internal socket path has unexpected ownership or type")
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as probe:
        probe.settimeout(1)
        try:
            probe.connect(str(path))
        except ConnectionRefusedError:
            path.unlink()
        else:
            raise RuntimeError("another internal listener is still active")
