
from fastapi.testclient import TestClient

from qqbot.app import create_app
from qqbot.config import Settings


def test_conversation_api_labels_existing_rows_without_changing_ids(tmp_path, monkeypatch, official_config, official_event, clock):
    monkeypatch.setenv('QQBOT_ADMIN_TOKEN', 'test-admin')
    settings = Settings(database=str(tmp_path / 'core.db'), adapters=(official_config,), interactive_execution_enabled=False)
    app = create_app(settings, clock=clock, run_background=False)
    adapter = app.state.adapters[official_config.instance_id]
    event = adapter.normalize(official_event)
    app.state.core.inbox.accept(event)
    with TestClient(app) as client:
        assert client.get('/onebot/qq').status_code == 404
        response = client.get('/api/v1/conversations', headers={'Authorization': 'Bearer test-admin'})
        assert response.status_code == 200
        row = response.json()[0]
        assert row['id'] == event.conversation.key
        assert row['display_label'] == f'群聊（{event.conversation.remote_conversation_id}）'
        status = client.get('/api/v1/status', headers={'Authorization': 'Bearer test-admin'})
        assert status.status_code == 200
        assert not {'connected', 'context_gap'} & status.json()['adapters'][0].keys()
        adapter_status = client.get('/api/v1/adapters', headers={'Authorization': 'Bearer test-admin'})
        assert adapter_status.status_code == 200
        assert not {'online', 'context_gap'} & adapter_status.json()[0].keys()
