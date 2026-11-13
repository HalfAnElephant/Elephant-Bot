"""A single owner-authorized scan installs a usable adapter and survives restart."""
import asyncio
import base64
import json
import os
from typing import ClassVar

import httpx
import pytest
from test_qq_onboarding import encrypted

from qqbot.app import create_app
from qqbot.config import Settings
from qqbot.permissions import PermissionSubject


class ConnectedGateway:
    lifecycle: ClassVar[list] = []

    def __init__(self, adapter):
        self.adapter = adapter
        self.connected = False

    async def start(self):
        self.connected = True
        self.lifecycle.append(('start', self.adapter.config.credential_ref))

    async def stop(self):
        self.connected = False
        self.lifecycle.append(('stop', self.adapter.config.credential_ref))

    def snapshot(self):
        return {'state': 'CONNECTED' if self.connected else 'RECONNECTING', 'connected': self.connected,
                'healthy': self.connected, 'error': None, 'retry_count': 0, 'intents': self.adapter.config.intents}


@pytest.fixture
async def automatic(tmp_path, monkeypatch):
    monkeypatch.setenv('QQBOT_CREDENTIAL_KEY', base64.b64encode(os.urandom(32)).decode())
    monkeypatch.setenv('QQBOT_ADMIN_TOKEN', 'owner-administration-token')
    monkeypatch.setattr('qqbot.adapters.qq_official.gateway.QQGateway', ConnectedGateway)
    ConnectedGateway.lifecycle = []
    keys, scanner = {}, {'openid': 'verified-scanner-openid'}

    def upstream(request):
        body = json.loads(request.content) if request.content else {}
        if request.url.path.endswith('create_bind_task'):
            task_id = str(len(keys) + 1)
            keys[task_id] = base64.b64decode(body['key'])
            return httpx.Response(200, json={'retcode': 0, 'data': {'task_id': task_id}})
        if request.url.path.endswith('poll_bind_result'):
            return httpx.Response(200, json={'retcode': 0, 'data': {'status': 2, 'bot_appid': 'verified-app',
                'user_openid': scanner['openid'], 'bot_encrypt_secret': encrypted(keys[body['task_id']])}})
        if request.url.path.endswith('getAppAccessToken'):
            return httpx.Response(200, json={'access_token': 'private-access-token', 'expires_in': 3600})
        raise AssertionError('Unexpected official request')

    settings = Settings(database=str(tmp_path / 'core.db'))
    official = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
    app = create_app(settings, run_background=False, official_client=official)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test',
            headers={'Authorization': 'Bearer owner-administration-token'}) as client,
    ):
        yield app, client, settings, official, scanner
    await official.aclose()


async def scan(app, client):
    response = await client.post('/api/v1/qq/onboarding/sessions', json={'instance_id': 'qq-official', 'automatic': True})
    assert response.status_code == 200, response.text
    session = app.state.qq_onboarding.sessions[response.json()['session_id']]
    await asyncio.wait_for(session.task, 2)
    response = await client.get('/api/v1/qq/onboarding/sessions/' + session.id)
    assert response.status_code == 200
    return session, response.json()


async def test_automatic_scan_connects_binds_owner_and_restricts_other_subjects(automatic):
    app, client, _settings, _official, _scanner = automatic
    session, public = await scan(app, client)
    assert public['state'] == 'connected', public
    assert public['automatic'] and public['provisioned'] and public['owner_bound']
    assert public['activated'] and not public['restart_required']
    assert session.secret is None and session.scanner_openid is None
    assert 'private-client-secret' not in json.dumps(public)
    assert 'verified-scanner-openid' not in json.dumps(public)
    assert 'private-access-token' not in json.dumps(public)
    core = app.state.core
    saved = core.config_store.read()
    cfg = saved.adapters[0]
    assert cfg.enabled and cfg.transport == 'websocket' and cfg.intents == 1 << 25
    assert cfg.secret() == 'private-client-secret'
    assert saved.owner.qq_bindings[0].user_openid == 'verified-scanner-openid'
    assert core.settings == saved and core.sessions.settings == saved
    assert core.sessions.learning.settings == saved and core.tasks.graph.settings == saved
    assert core.adapters is core.tasks.qq_adapters is app.state.adapters
    assert cfg.instance_id in core.outbox.markdown_adapters
    allowed = PermissionSubject(adapter_instance_id=cfg.instance_id, identity_namespace='qq_app:verified-app',
        user_id='verified-scanner-openid')
    denied = allowed.model_copy(update={'user_id': 'other-person'})
    assert core.permissions.check(allowed, 'task.execute', '*').allowed
    assert not core.permissions.check(denied, 'task.execute', '*').allowed
    assert core.permissions.check(PermissionSubject(adapter_instance_id='admin_api', identity_namespace='admin_api',
        user_id='owner'), 'admin.get', 'http:/api/v1/config').allowed
    status = (await client.get('/api/v1/qq/onboarding/status')).json()
    assert status['adapters'][0]['owner_bound'] and status['adapters'][0]['connection']['connected']
    assert (await client.get('/health/ready')).status_code == 200
    app.state.adapters[cfg.instance_id].gateway.connected = False
    readiness = await client.get('/health/ready')
    assert readiness.status_code == 503 and readiness.json()['disconnected'] == [cfg.instance_id]
    app.state.adapters[cfg.instance_id].gateway.connected = True
    assert (await client.post('/webhooks/qq/qq-official', json={})).status_code == 409


async def test_rescan_rotates_same_application_without_a_second_live_receiver(automatic):
    app, client, _settings, _official, _scanner = automatic
    _first, result = await scan(app, client)
    assert result['state'] == 'connected'
    original = app.state.core.settings.adapters[0]
    _second, result = await scan(app, client)
    assert result['state'] == 'connected', result
    rotated = app.state.core.settings.adapters[0]
    assert rotated.credential_ref != original.credential_ref
    assert len(app.state.adapters) == 1 and len(app.state.core.settings.owner.qq_bindings) == 1
    assert ConnectedGateway.lifecycle[:3] == [('start', original.credential_ref), ('stop', original.credential_ref),
                                           ('start', rotated.credential_ref)]
    assert original.secret() == rotated.secret() == 'private-client-secret'


async def test_different_scanner_cannot_take_over_existing_owner(automatic):
    app, client, _settings, _official, scanner = automatic
    _first, result = await scan(app, client)
    assert result['state'] == 'connected'
    original = app.state.core.config_store.read()
    policy = app.state.core.permissions.list()
    scanner['openid'] = 'different-scanner'
    _second, result = await scan(app, client)
    assert result['state'] == 'failed', result
    assert app.state.core.config_store.read() == original
    assert app.state.core.permissions.list() == policy
    assert len(ConnectedGateway.lifecycle) == 1


async def test_enabled_automatic_configuration_reconnects_after_restart(automatic):
    app, client, settings, official, _scanner = automatic
    _session, result = await scan(app, client)
    assert result['state'] == 'connected'
    # Stop the first receiver before constructing the independent restarted host.
    await app.state.adapters['qq-official'].stop()
    restarted = create_app(settings, run_background=False, official_client=official)
    async with (
        restarted.router.lifespan_context(restarted),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=restarted), base_url='http://test',
            headers={'Authorization': 'Bearer owner-administration-token'}) as after,
    ):
        status = (await after.get('/api/v1/qq/onboarding/status')).json()
        assert status['adapters'][0]['connection']['connected'] and status['adapters'][0]['owner_bound']
        assert (await after.get('/health/ready')).status_code == 200
        assert restarted.state.core.sessions.settings.owner.qq_bindings[0].user_openid == 'verified-scanner-openid'


async def test_authenticated_socket_interactions_use_the_existing_verified_dispatch(automatic, monkeypatch):
    app, client, _settings, _official, _scanner = automatic
    called = []
    async def verified(adapter, core, store, payload):
        called.append((adapter, core, store, payload))
        return {'op': 12}
    monkeypatch.setattr('qqbot.app.handle_verified_interaction', verified)
    _session, result = await scan(app, client)
    assert result['state'] == 'connected'
    adapter = app.state.adapters['qq-official']
    payload = {'op': 0, 't': 'INTERACTION_CREATE', 'id': 'official-interaction', 'd': {}}
    await adapter.accept_payload(payload)
    assert called == [(adapter, app.state.core, app.state.qq_interactions, payload)]


async def test_closing_the_request_client_does_not_cancel_server_side_setup(automatic):
    app, _client, _settings, _official, _scanner = automatic
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test',
            headers={'Authorization': 'Bearer owner-administration-token'}) as browser:
        response = await browser.post('/api/v1/qq/onboarding/sessions', json={'instance_id': 'qq-official', 'automatic': True})
        session_id = response.json()['session_id']
    session = app.state.qq_onboarding.sessions[session_id]
    await asyncio.wait_for(session.task, 2)
    assert session.provisioned and session.state == 'connected'
    assert app.state.adapters['qq-official'].gateway.snapshot()['connected']


async def test_saved_automatic_connection_survives_qr_expiration_and_cannot_be_cancelled(automatic):
    app, client, _settings, _official, _scanner = automatic
    session, result = await scan(app, client)
    assert result['state'] == 'connected'
    previous = app.state.core.config_store.read()
    session.expires = 0
    route = '/api/v1/qq/onboarding/sessions/' + session.id
    assert (await client.get(route)).json()['state'] == 'connected'
    assert (await client.delete(route)).status_code == 422
    assert app.state.core.config_store.read() == previous
    assert app.state.adapters['qq-official'].gateway.snapshot()['connected']


async def test_automatic_onboarding_requires_the_official_scanner_identity(automatic):
    app, client, _settings, _official, scanner = automatic
    scanner['openid'] = None
    session, result = await scan(app, client)
    assert result['state'] == 'failed'
    assert session.secret is None and session.scanner_openid is None
    assert not app.state.core.config_store.read().adapters
    assert not app.state.core.config_store.read().owner.qq_bindings
    assert not app.state.adapters
