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


def test_restore_old_snapshot_cannot_resurrect_deleted_memory(db, tmp_path, nap_config, nap_event, clock, incoming):
    import json

    from qqbot.adapters.onebot_v11.adapter import OneBotAdapter
    from qqbot.domain import AccessContext
    from qqbot.memory import MemoryRecord, SocialService
    from qqbot.storage.social_schema import social_records

    event = incoming.accept(OneBotAdapter(nap_config, 'restore-test', incoming, clock).normalize(nap_event))[0]
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


def test_restore_preserves_completed_effects_and_fences_active_work(db, tmp_path, clock, nap_config, nap_event):
    import json

    from qqbot.adapters.onebot_v11.adapter import OneBotAdapter
    from qqbot.domain import AccessContext
    from qqbot.ingress.service import Inbox
    from qqbot.jobs.service import TaskService
    from qqbot.outbox.service import Outbox
    from qqbot.storage.action_schema import operations
    from qqbot.storage.task_schema import runs, tasks

    incoming = Inbox(db)
    adapter = OneBotAdapter(nap_config, 'restore-test', incoming, clock)
    event = incoming.accept(adapter.normalize(nap_event))[0]
    access = AccessContext(principal=event.principal, conversation=event.conversation, role='owner')
    service = TaskService(db, clock, Outbox(db, clock))
    done = service.accept(event, access, 'finished work')
    claim = service.claim('worker')
    service.submit(claim['run_id'], claim['fencing_token'], 'worker', {'status': 'completed', 'summary': 'done'})
    running_event = event.model_copy(update={'event_id': 'running', 'platform_message_id': 'running'})
    running = service.accept(running_event, access, 'uncertain work')
    active = service.claim('worker')
    queued_event = event.model_copy(update={'event_id': 'queued', 'platform_message_id': 'queued'})
    queued = service.accept(queued_event, access, 'queued work')
    with db.transaction() as c:
        for identity, state in [('verified-effect', 'VERIFIED'), ('unknown-effect', 'UNKNOWN')]:
            c.execute(insert(operations).values(id=identity, task_id=running['id'], run_id=active['run_id'],
                fence=active['fencing_token'], arguments_hash='a' * 64, action_json='{}', arguments_json='{}',
                state=state, result_json='{}', updated_at=clock.now().isoformat()))
    snapshot, ledger, target = (tmp_path / name for name in ('snapshot.sqlite', 'deletions.json', 'restored.sqlite'))
    backup(db.path, snapshot)
    export_deletions(db.path, ledger)
    restore(snapshot, target, ledger)
    restored = Database(str(target))
    try:
        recovered = TaskService(restored, clock, Outbox(restored, clock), stop_verifier=lambda *_: True,
                                max_active=1, sustained_limit=1)
        assert recovered.get(done['public_id'], access)['state'] == 'SUCCEEDED'
        assert recovered.get(running['public_id'], access)['state'] == 'NEEDS_RECONCILIATION'
        assert recovered.get(queued['public_id'], access)['state'] == 'QUEUED'
        assert recovered.claim('new-worker') is None
        with restored.engine.connect() as c:
            assert dict(c.execute(select(operations.c.id, operations.c.state)).all()) == {
                'verified-effect': 'VERIFIED', 'unknown-effect': 'UNKNOWN'}
            assert c.execute(select(runs.c.slot_reserved).where(runs.c.id == active['run_id'])).scalar_one() == 1
            assert json.loads(c.execute(select(tasks.c.result_json).where(tasks.c.id == done['id'])).scalar_one())['summary'] == 'done'
        recovered.reconcile(running['public_id'], access, active['run_id'])
        next_claim = recovered.claim('new-worker')
        assert next_claim['task_id'] == queued['id']
    finally:
        restored.close()
