"""Offline backup/restore; restoration replays later tombstones before serving."""

import json
import os
import sqlite3
from pathlib import Path


def backup(source, destination):
    target = Path(destination)
    if target.exists():
        raise FileExistsError(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    if temporary.exists():
        raise FileExistsError(temporary)
    with (
        sqlite3.connect(f"file:{Path(source).resolve()}?mode=ro", uri=True) as src,
        sqlite3.connect(temporary) as dst,
    ):
        src.backup(dst)
        if dst.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise RuntimeError("backup integrity check failed")
    os.chmod(temporary, 0o600)
    temporary.replace(target)


def export_deletions(source, destination):
    with sqlite3.connect(f"file:{Path(source).resolve()}?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        result = {
            table: [dict(row) for row in db.execute(f"SELECT * FROM {table}")]
            for table in ("social_tombstones", "memory_deletions")
        }
    path = Path(destination)
    with path.open("x") as file:
        os.chmod(path, 0o600)
        json.dump(result, file)


def restore(snapshot, destination, deletion_ledger):
    # Restore only to a new file, never replace a live WAL database.
    ledger = json.loads(Path(deletion_ledger).read_text())
    if set(ledger) != {"social_tombstones", "memory_deletions"}:
        raise ValueError("invalid deletion ledger")
    backup(snapshot, destination)
    try:
        with sqlite3.connect(destination) as db:
            for table, columns in (
                ("social_tombstones", ("source_id", "conversation_id")),
                ("memory_deletions", ("memory_id", "conversation_id", "deleted_at", "source_ids_json")),
            ):
                for record in ledger[table]:
                    if set(record) != set(columns):
                        raise ValueError("invalid ledger row")
                    db.execute(
                        f"INSERT OR IGNORE INTO {table} ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
                        [record[column] for column in columns],
                    )
            sources = {row[0] for row in db.execute("SELECT source_id FROM social_tombstones")}
            for record_id, raw in db.execute("SELECT id, record_json FROM social_records").fetchall():
                record = json.loads(raw)
                if sources.intersection(record["source_event_ids"]):
                    record.update(status="deleted", text="[deleted]", value_json="null", embedding=None)
                    db.execute(
                        "UPDATE social_records SET status='deleted',record_json=? WHERE id=?",
                        (json.dumps(record), record_id),
                    )
                    db.execute("DELETE FROM social_index_jobs WHERE memory_id=?", (record_id,))
            db.execute(
                "UPDATE conversation_summaries SET status='INVALID',summary_json='[]',source_ids_json='[]'"
            )
            db.execute("UPDATE social_versions SET version=version+1")
            db.execute("UPDATE outbox SET state='UNKNOWN' WHERE state='SENDING'")
            db.execute(
                "UPDATE tasks SET state='NEEDS_RECONCILIATION' WHERE state IN ('RUNNING','RECOVERING','CANCEL_REQUESTED')"
            )
            assert db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    except BaseException:
        Path(destination).unlink(missing_ok=True)
        raise
