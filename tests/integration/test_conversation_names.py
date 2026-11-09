
from fastapi.testclient import TestClient

from qqbot.adapters.onebot_v11.adapter import OneBotAdapter
from qqbot.app import create_app
from qqbot.config import Settings


async def test_onebot_conversation_names_and_disconnected_fallback(nap_config, incoming, clock, nap_event):
    adapter = OneBotAdapter(nap_config, 'demo', incoming, clock)
    group = adapter.normalize(nap_event).conversation
    private = group.model_copy(update={'surface': 'private', 'remote_conversation_id': '123456'})
    adapter.websocket = object()

    async def request(action, params):
        if action == 'get_group_info':
            assert params == {'group_id': group.remote_conversation_id}
            return {'status': 'ok', 'data': {'group_name': '测试群'}}
        assert action == 'get_stranger_info' and params == {'user_id': '123456'}
        return {'status': 'ok', 'data': {'nickname': 'QQ名'}}

    adapter._request = request
    names = await adapter.resolve_conversation_names([group, private])
    assert names[(group.surface, group.remote_conversation_id)] == '测试群'
    assert names[('private', '123456')] == 'QQ名'
    adapter.websocket = None
    assert await adapter.resolve_conversation_names([group, private]) == names


async def test_onebot_name_lookup_failure_preserves_cached_names(nap_config, incoming, clock, nap_event):
    adapter = OneBotAdapter(nap_config, 'demo', incoming, clock)
    ref = adapter.normalize(nap_event).conversation
    adapter.websocket = object()
    adapter.conversation_names[(ref.surface, ref.remote_conversation_id)] = '已知群名'

    async def request(*args):
        raise ConnectionError('offline')

    adapter._request = request
    assert (await adapter.resolve_conversation_names([ref]))[(ref.surface, ref.remote_conversation_id)] == '已知群名'


def test_conversation_api_labels_existing_rows_without_changing_ids(tmp_path, monkeypatch, nap_config, nap_event, clock):
    monkeypatch.setenv('QQBOT_ADMIN_TOKEN', 'test-admin')
    settings = Settings(database=str(tmp_path / 'core.db'), adapters=(nap_config,), interactive_execution_enabled=False)
    app = create_app(settings, clock=clock, run_background=False)
    adapter = app.state.adapters[nap_config.instance_id]
    event = adapter.normalize(nap_event)
    app.state.core.inbox.accept(event)
    adapter.conversation_names[(event.conversation.surface, event.conversation.remote_conversation_id)] = '测试群'
    with TestClient(app) as client:
        response = client.get('/api/v1/conversations', headers={'Authorization': 'Bearer test-admin'})
        assert response.status_code == 200
        row = response.json()[0]
        assert row['id'] == event.conversation.key
        assert row['display_label'] == f'测试群（{event.conversation.remote_conversation_id}）'
        adapter.conversation_names.clear()
        row = client.get('/api/v1/conversations', headers={'Authorization': 'Bearer test-admin'}).json()[0]
        assert row['display_label'] == f'群聊（{event.conversation.remote_conversation_id}）'
