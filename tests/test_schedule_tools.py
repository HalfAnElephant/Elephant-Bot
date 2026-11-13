"""Schedule tools follow the same authenticated Leader entry as ordinary user turns."""
from datetime import timedelta
from unittest.mock import AsyncMock

import httpx
import pytest
from test_leader_core import make_core

from qqbot.harness.gateway import RunGateway
from qqbot.runtime.tools import native_tools
from qqbot.storage.task_schema import runs


def test_schedule_schemas_are_only_available_to_user_leader():
    assert {'schedules.create', 'schedules.list', 'schedules.get', 'schedules.update', 'schedules.cancel'} <= set(
        native_tools('leader', 'user'))
    for role, purpose in [('worker', 'work'), ('leader', 'progress'), ('leader', 'finalize'), ('skill_review', 'skill_review')]:
        assert not any(name.startswith('schedules.') for name in native_tools(role, purpose))


async def test_schedule_native_tool_reaches_host_operation(tmp_path):
    call = AsyncMock(return_value={'allowed': True, 'id': 'saved-schedule'})
    gateway = RunGateway(tmp_path, 'model', '', 'https://model.invalid', call=call)
    try:
        gateway.bind({'task_id': 'turn', 'run_id': 'run', 'fencing_token': 1, 'role': 'leader', 'purpose': 'user',
            'goal': '明天检查一次', 'permission_snapshot': {'principal': {}}, 'context': {}})
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url='http://test') as client:
            response = await client.post('/tools/schedules_create', headers={'Authorization': 'Bearer ' + gateway.tool_token},
                json={'call_id': 'create-once', 'arguments': {'goal': '检查一次', 'run_at': '2030-01-01T09:00:00+08:00'}})
        assert response.status_code == 200, response.text
        assert response.json()['id'] == 'saved-schedule'
        arguments = next(item.args[1] for item in call.await_args_list if item.args[0] == 'schedules')
        assert arguments['operation'] == 'create' and arguments['call_id'] == 'create-once'
    finally:
        gateway.revoke()


async def test_live_leader_schedule_controls_persist_and_worker_role_is_rejected(
        tmp_path, official_config, official_event, clock):
    from conftest import official_payload
    core = make_core(tmp_path, official_config, clock)
    try:
        event = core.adapters['qq'].normalize(official_payload(official_event, private=True,
            text='明天检查一次仓库', message_id='schedule-user'))
        event, _ = core.inbox.accept(event)
        await core.process(event)
        claim = core.sessions.claim('schedule-leader')
        identity = (claim['run_id'], claim['fencing_token'], 'schedule-leader')
        payload = {'operation': 'create', 'call_id': 'schedule-call', 'arguments': {'goal': '检查一次仓库',
            'run_at': (clock.now() + timedelta(days=1)).isoformat()}}
        first = await core.leader.invoke('schedules', identity, payload)
        assert await core.leader.invoke('schedules', identity, payload) == first
        assert len(core.schedules.list(event)) == 1
        with core.db.transaction() as conn:
            conn.execute(runs.update().where(runs.c.id == claim['run_id']).values(role='skill_review'))
        with pytest.raises(PermissionError, match='real Leader user turn'):
            await core.leader.invoke('schedules', identity, payload)
    finally:
        await core.stop()
        core.db.close()
