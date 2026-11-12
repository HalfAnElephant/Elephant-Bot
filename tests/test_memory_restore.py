"""A database snapshot cannot reopen Pi history or recalls predating a forget."""
from sqlalchemy import insert, select

from qqbot.contracts.memory import MemoryScope
from qqbot.storage.agent_schema import dialogues
from qqbot.storage.db import Database
from qqbot.storage.memory_sync_schema import memory_barriers, memory_documents, memory_owners
from qqbot.storage.operations import backup, export_deletions, restore


def test_restore_replays_latest_memory_epoch_and_rotates_native_session(db, tmp_path, clock):
    scope = MemoryScope(owner_id="personal-owner")
    with db.transaction() as conn:
        conn.execute(insert(memory_owners).values(id=scope.key, scope_json=scope.model_dump_json(), binding_json="{}",
            binding_hash="binding", remote_identity="owner", epoch=0, state="READY", updated_at=clock.now().isoformat()))
        conn.execute(insert(dialogues).values(id="dialogue",owner_id=scope.owner_id,conversation_id="private",
            title="default",session_id="old-pi-session",binding_json="{}",state="ACTIVE",created_at=clock.now().isoformat()))
    snapshot=tmp_path/"snapshot.db"
    backup(db.path,snapshot)
    with db.transaction() as conn:
        conn.execute(memory_owners.update().values(epoch=1))
        conn.execute(insert(memory_documents).values(id="corrected",scope_id=scope.key,
            uri="viking://user/owner/memories/profile/corrected.md",content_hash="new-corrected-hash",
            epoch=1,state="ACTIVE",index_state="INDEXED",updated_at=clock.now().isoformat()))
        conn.execute(insert(memory_barriers).values(id="forget",scope_id=scope.key,epoch=1,operation_id="mutation",
            source_id="original-user-fact",uri="viking://user/owner/memories/profile/fact.md",created_at=clock.now().isoformat()))
    ledger=tmp_path/"deletions.json"
    export_deletions(db.path,ledger)
    target=tmp_path/"restored.db"
    restore(snapshot,target,ledger)
    restored=Database(str(target))
    try:
        with restored.engine.connect() as conn:
            owner=conn.execute(select(memory_owners)).mappings().one()
            assert owner["epoch"]==1 and owner["state"]=="RESTORE_REQUIRED"
            assert conn.execute(select(memory_barriers.c.source_id)).scalar_one()=="original-user-fact"
            assert conn.execute(select(memory_documents.c.content_hash)).scalar_one()=="new-corrected-hash"
            assert conn.execute(select(dialogues.c.session_id)).scalar_one()!="old-pi-session"
    finally:
        restored.close()
