import pytest
from sqlalchemy import insert, select

from qqbot.storage.db import Database
from qqbot.storage.operations import backup, export_deletions, restore
from qqbot.storage.social_schema import social_tombstones


def test_online_backup_restore_ledger(db, tmp_path):
    snapshot = tmp_path/"backup.sqlite"
    backup(db.path, snapshot)
    with db.transaction() as c:
        c.execute(insert(social_tombstones).values(source_id="deleted-after-backup", conversation_id="c"))
    ledger = tmp_path/"deletions.json"
    export_deletions(db.path, ledger)
    destination = tmp_path/"restored.sqlite"
    restore(snapshot, destination, ledger)
    restored = Database(str(destination))
    with restored.engine.connect() as c:
        assert c.execute(select(social_tombstones.c.source_id)).scalar_one() == "deleted-after-backup"
    restored.close()
    with pytest.raises(FileExistsError):
        restore(snapshot, destination, ledger)


def test_restore_old_snapshot_cannot_resurrect_deleted_memory(db, tmp_path, official_config, official_event, clock, incoming):
    import json

    from qqbot.adapters.qq_official.adapter import OfficialAdapter
    from qqbot.domain import AccessContext
    from qqbot.memory import MemoryRecord, SocialService
    from qqbot.storage.social_schema import social_records

    event = incoming.accept(OfficialAdapter(official_config, 'restore-test', incoming, clock).normalize(official_event))[0]
    access = AccessContext(principal=event.principal, conversation=event.conversation, role='admin')
    service = SocialService(db, clock)
    record = MemoryRecord(scope_type='principal_in_conversation', scope_id=event.conversation.key,
        subject_ref=event.principal.key, predicate='preference',value_json=json.dumps('测试偏好'),text='测试偏好',
        source_event_ids=(event.event_id,),source_kind='user_explicit',confidence=1.0,
        valid_from=clock.now(),created_at=clock.now(),last_verified_at=clock.now())
    candidate = service.candidate(access, record, explicit=True)
    service.commit(access,candidate.memory_id,authorized=True)
    snapshot=tmp_path/'before-forget.sqlite'
    backup(db.path,snapshot)
    service.forget(access,candidate.memory_id)
    ledger=tmp_path/'later-deletions.json'
    export_deletions(db.path,ledger)
    destination=tmp_path/'after-restore.sqlite'
    restore(snapshot,destination,ledger)
    restored=Database(str(destination))
    try:
        recovered=SocialService(restored,clock)
        assert recovered.retrieve(access,'测试偏好') == []
        with restored.engine.connect() as c:
            row=json.loads(c.execute(select(social_records.c.record_json).where(social_records.c.id==candidate.memory_id)).scalar_one())
            assert row['status']=='deleted' and row['text']=='[deleted]' and row['embedding'] is None
        with pytest.raises(ValueError,match='revoked'):
            recovered.candidate(access,record,explicit=True)
    finally:
        restored.close()
