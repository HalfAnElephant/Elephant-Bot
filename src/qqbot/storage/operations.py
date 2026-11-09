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
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='expression_tombstones'").fetchone():
            result["expression_tombstones"] = [dict(row) for row in db.execute("SELECT * FROM expression_tombstones")]
    path = Path(destination)
    with path.open("x") as file:
        os.chmod(path, 0o600)
        json.dump(result, file)


def restore(snapshot, destination, deletion_ledger):
    # Restore only to a new file, never replace a live WAL database.
    ledger = json.loads(Path(deletion_ledger).read_text())
    required = {"social_tombstones", "memory_deletions"}
    if not required.issubset(ledger) or set(ledger) - required - {"expression_tombstones"}:
        raise ValueError("invalid deletion ledger")
    backup(snapshot, destination)
    try:
        with sqlite3.connect(destination) as db:
            if "expression_tombstones" in ledger:
                db.execute("CREATE TABLE IF NOT EXISTS expression_tombstones ("
                           "expression_id VARCHAR PRIMARY KEY, conversation_id VARCHAR NOT NULL, deleted_at VARCHAR NOT NULL)")
                for record in ledger["expression_tombstones"]:
                    columns = ("expression_id", "conversation_id", "deleted_at")
                    if set(record) != set(columns):
                        raise ValueError("invalid expression deletion ledger row")
                    db.execute("INSERT OR IGNORE INTO expression_tombstones VALUES (?,?,?)",
                               [record[column] for column in columns])
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
            if db.execute("SELECT 1 FROM sqlite_master WHERE name='expressions'").fetchone():
                removed = {row[0] for row in db.execute("SELECT expression_id FROM expression_tombstones")}
                retained = {row[0] for row in db.execute("SELECT source_id FROM memory_source_retention")}
                for expression_id, raw in db.execute("SELECT id,sources_json FROM expressions").fetchall():
                    evidence = {item["id"] for item in json.loads(raw)}
                    if expression_id in removed or evidence.intersection(sources | retained):
                        db.execute("UPDATE expressions SET situation='',style='',sources_json='[]',"
                                   "status='deleted',revision=revision+1 WHERE id=?", (expression_id,))
                db.execute("UPDATE expression_versions SET version=version+1")
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
