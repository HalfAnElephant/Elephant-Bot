"""Incoming files and images retain their authenticated source across graph updates."""
import json

import pytest
from sqlalchemy import select
from test_agent_runtime import root_goal
from test_agent_runtime import runtime as runtime_fixture

from qqbot.domain import MediaSegment
from qqbot.ingress.service import Inbox
from qqbot.storage.agent_schema import agent_events, leader_inbox
from qqbot.storage.schema import inbox
from qqbot.storage.task_schema import inputs, tasks

runtime = runtime_fixture


def media_event(runtime, identifier='media-followup'):
    return runtime.event.model_copy(update={'event_id': identifier, 'platform_message_id': identifier,
        'segments': (MediaSegment(type='file', attachment_ref='file-ref', source_url='https://cdn.example/file',
            filename='original.pptx', size=12), MediaSegment(type='image', attachment_ref='image-ref',
            image_url='https://cdn.example/image.png'))})


def test_followup_media_maps_original_event_through_opaque_dispatch_keys(runtime):
    root = root_goal(runtime)
    claim = runtime.service.claim('worker')
    event, _ = Inbox(runtime.db).accept(media_event(runtime))
    with runtime.db.transaction() as conn:
        task = conn.execute(select(tasks).where(tasks.c.id == root['task_id'])).mappings().one()
        runtime.service.add_graph_input(conn, task, event, runtime.access, '', source_id='opaque-idempotency-key')
    extra = runtime.service.take_inputs(claim['run_id'], claim['fencing_token'], 'worker')
    assert len(extra) == 1 and '原始附件' in extra[0]['text']
    assert extra[0]['files'][0]['source'] == event.event_id
    assert extra[0]['images'][0]['source'] == event.event_id
    assert extra[0]['source_manifest'][0]['id'] == event.event_id
    with runtime.db.engine.connect() as conn:
        rows = conn.execute(select(inputs).where(inputs.c.source_event_id == event.event_id)).mappings().all()
        assert len(rows) == 2
        for raw in conn.execute(select(tasks.c.context_json).where(tasks.c.root_task_id == root['task_id'])).scalars():
            context = json.loads(raw)
            assert context['files'][0]['filename'] == 'original.pptx'
            assert context['images'][0]['attachment_ref'] == 'image-ref'


def test_unpersisted_and_cross_identity_media_cannot_become_source_receipts(runtime):
    root = root_goal(runtime)
    event = media_event(runtime)
    with pytest.raises(PermissionError, match='durably accepted'):
        runtime.service.add_input(root['public_id'], event, runtime.access, '')
    forged = event.model_copy(update={'principal': event.principal.model_copy(update={'remote_user_id': 'other'})})
    with pytest.raises(PermissionError):
        runtime.service.add_input(root['public_id'], forged, runtime.access, '')
    with runtime.db.engine.connect() as conn:
        assert not conn.execute(select(inputs)).first()
        assert not conn.execute(select(inbox)).first()


def test_worker_plain_notification_records_fact_without_leader_wakeup(runtime):
    root_goal(runtime)
    claim = runtime.service.claim('worker')
    runtime.service.notify(claim['run_id'], claim['fencing_token'], 'worker',
        {'text': '正在核对资料', 'notification_id': 'plain-update'})
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(agent_events.c.id).where(agent_events.c.kind == 'worker_update')).first()
        assert not conn.execute(select(leader_inbox)).first()


@pytest.mark.parametrize('verified,outcome', [(False, 'WAITING_FOR_TRIGGER'), (True, 'SENT'), (True, 'UNKNOWN')])
async def test_scheduled_result_uses_existing_proactive_authority_and_unknown_fence(
    db, clock, incoming, official_config, official_event, verified, outcome
):
    import httpx

    from qqbot.adapters.qq_official.adapter import OfficialAdapter
    from qqbot.domain import AccessContext
    from qqbot.jobs.service import TaskService
    from qqbot.outbox.service import Outbox
    from qqbot.storage.schema import outbox

    calls = []
    def transport(request):
        if request.url.path.endswith('getAppAccessToken'):
            return httpx.Response(200, json={'access_token': 'fixture', 'expires_in': 7200})
        calls.append(request)
        if outcome == 'UNKNOWN':
            raise httpx.ReadTimeout('receipt lost')
        return httpx.Response(200, json={'id': 'scheduled-result'})
    adapter = OfficialAdapter(official_config.model_copy(update={'proactive_verified': verified}), 'd', incoming, clock,
        httpx.AsyncClient(transport=httpx.MockTransport(transport)))
    original = adapter.normalize(official_event)
    event = original.model_copy(update={'reply_context': None})
    outgoing = Outbox(db, clock)
    service = TaskService(db, clock, outgoing)
    access = AccessContext(principal=event.principal, conversation=event.conversation)
    task = service.accept(event, access, 'Scheduled report', context={'schedule_fire_id': 'trusted-fire'})
    with db.transaction() as conn:
        row = conn.execute(select(tasks).where(tasks.c.id == task['id'])).mappings().one()
        service._notification(conn, row, None, '预约报告已完成', 'result', 'important', 'scheduled-result')
    await outgoing.dispatch_once({'qq': adapter})
    with db.engine.connect() as conn:
        assert conn.execute(select(outbox.c.state)).scalar_one() == outcome
        envelope = json.loads(conn.execute(select(outbox.c.envelope_json)).scalar_one())
        assert envelope['allow_proactive']
    if outcome == 'UNKNOWN':
        with db.transaction() as conn:
            outgoing.retrigger(conn, original)
            service._notification(conn, row, None, '预约报告已完成', 'result', 'important', 'scheduled-result')
        await outgoing.dispatch_once({'qq': adapter})
        assert len(calls) == 1
        with db.engine.connect() as conn:
            assert conn.execute(select(outbox.c.state)).scalar_one() == 'UNKNOWN'
    await adapter.stop()
