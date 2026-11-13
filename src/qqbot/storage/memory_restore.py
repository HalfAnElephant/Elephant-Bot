"""Carry deletion epochs across old database restores; never export memory bodies."""
import json
from uuid import uuid4

from qqbot.contracts.memory import MemoryScope
from qqbot.storage.memory_sync_schema import (
    memory_barriers,
    memory_derivations,
    memory_documents,
    memory_owners,
    memory_sources,
)

LEDGER_TABLES = {"owners": memory_owners, "barriers": memory_barriers, "documents": memory_documents,
                 "derivations": memory_derivations, "sources": memory_sources}


def export_memory_ledger(db):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name='memory_owners'").fetchone():
        return None
    return {key: [dict(row) for row in db.execute(f"SELECT * FROM {table.name}")]
            for key, table in LEDGER_TABLES.items()}


def apply_memory_ledger(db, ledger):
    if not isinstance(ledger, dict) or set(ledger) != set(LEDGER_TABLES):
        raise ValueError("invalid OpenViking deletion ledger")
    owners = {}
    for item in ledger["owners"]:
        if set(item) != set(memory_owners.c.keys()) or type(item["epoch"]) is not int or item["epoch"] < 0:
            raise ValueError("invalid memory owner deletion epoch")
        scope = MemoryScope.model_validate_json(item["scope_json"])
        if scope.key != item["id"]:
            raise ValueError("memory deletion scope identity mismatch")
        owners[item["id"]] = item
    for item in ledger["barriers"]:
        if (set(item) != set(memory_barriers.c.keys()) or item["scope_id"] not in owners
                or type(item["epoch"]) is not int or not 0 <= item["epoch"] <= owners[item["scope_id"]]["epoch"]):
            raise ValueError("invalid memory deletion barrier")
    for key in ("documents", "derivations", "sources"):
        for item in ledger[key]:
            if set(item) != set(LEDGER_TABLES[key].c.keys()) or item["scope_id"] not in owners:
                raise ValueError("invalid memory receipt metadata")
    for scope_id, item in owners.items():
        old = db.execute("SELECT epoch FROM memory_owners WHERE id=?", (scope_id,)).fetchone()
        if old and old[0] > item["epoch"]:
            raise ValueError("memory deletion ledger is older than restored database")
        values = {**item, "state": "RESTORE_REQUIRED"}
        columns = list(values)
        db.execute(f"INSERT OR REPLACE INTO memory_owners ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
                   [values[key] for key in columns])
        # Native Pi files can outlive SQLite snapshots; never reopen their deleted context.
        owner_id = json.loads(item["scope_json"])["owner_id"]
        for dialogue_id, old_session in db.execute("SELECT id,session_id FROM dialogues WHERE owner_id=?", (owner_id,)).fetchall():
            replacement = str(uuid4())
            db.execute("UPDATE dialogues SET session_id=? WHERE id=?", (replacement, dialogue_id))
            db.execute("UPDATE tasks SET coordinator_session_id=? WHERE origin_dialogue_id=? AND kind='goal'", (replacement, dialogue_id))
            db.execute("UPDATE leader_inbox SET target_session_id=? WHERE target_session_id=? AND handled_at IS NULL", (replacement, old_session))
    for item in ledger["barriers"]:
        columns = list(item)
        db.execute(f"INSERT OR REPLACE INTO memory_barriers ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
                   [item[key] for key in columns])
    # Accepted receipt metadata restores corrected provenance without retaining fact text.
    for key in ("documents", "derivations", "sources"):
        for item in ledger[key]:
            columns = list(item)
            db.execute(f"INSERT OR REPLACE INTO {LEDGER_TABLES[key].name} ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
                       [item[column] for column in columns])
