import base64
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from test_host_controls import control_runtime as control_runtime  # noqa: PLC0414
from test_project_scopes import host_event
from test_project_scopes import project_core as project_core  # noqa: PLC0414

from elephant.admin.mail import build_mail_router
from elephant.mail.accounts import MailAccounts
from elephant.mail.models import MailAccountSpec
from elephant.storage.mail_schema import mail_accounts


@pytest.fixture
def accounts(control_runtime, monkeypatch):
    monkeypatch.setenv('ELEPHANT_CREDENTIAL_KEY', base64.b64encode(b'r' * 32).decode())
    core = control_runtime.core
    mail_accounts.create(core.db.engine, checkfirst=True)
    transport = SimpleNamespace(test=lambda spec, secret: {'imap': 'authenticated', 'smtp': 'authenticated', 'sent': False})
    service = core.mail.accounts
    service.transport = transport
    return service, control_runtime.event('configure mailbox')


def create(service, event, **overrides):
    return service.invoke(event, 'personal', 'create', {'account_id': 'weekly',
        'spec': {'provider': 'qq', 'address': 'reader@qq.com'}} | overrides)


@pytest.mark.parametrize(('provider', 'imap', 'smtp', 'port', 'security'), [
    ('icloud', 'imap.mail.me.com', 'smtp.mail.me.com', 587, 'starttls'),
    ('qq', 'imap.qq.com', 'smtp.qq.com', 465, 'ssl'),
    ('163', 'imap.163.com', 'smtp.163.com', 465, 'ssl'),
])
def test_provider_defaults_and_custom_servers(provider, imap, smtp, port, security):
    spec = MailAccountSpec(provider=provider, address='reader@example.org')
    assert (spec.imap_host, spec.smtp_host, spec.smtp_port, spec.smtp_security) == (imap, smtp, port, security)
    assert spec.username == 'reader@example.org'
    custom = MailAccountSpec(provider='custom', address='reader@example.org', username='reader',
        imap_host='imap.example.org', smtp_host='smtp.example.org', imap_security='starttls', imap_port=143)
    assert custom.imap_security == 'starttls'


@pytest.mark.parametrize('changes', [{'address': 'a@example.org\r\nBcc: other@example.org'},
    {'imap_host': 'https://imap.example.org'}, {'smtp_security': 'none'}, {'username': 'x\r\ny'},
    {'smtp_host': 'smtp.example.org\nsecret'}, {'imap_port': 0}])
def test_account_parameters_reject_injection_and_plaintext_transport(changes):
    with pytest.raises(ValueError):
        MailAccountSpec.model_validate({'provider': 'qq', 'address': 'reader@qq.com'} | changes)


def test_account_alias_is_idempotent_and_scoped(accounts):
    service, event = accounts
    item = create(service, event)
    assert not item['ready'] and item['credential_ref'] is None
    assert create(service, event)['id'] == item['id']
    assert service.resolve(event, 'personal', 'weekly')['id'] == item['id']
    assert service.resolve(event, 'personal', item['id'])['id'] == item['id']
    other = event.model_copy(update={'conversation': event.conversation.model_copy(update={'remote_conversation_id': 'other'})})
    assert service.invoke(other, 'personal', 'accounts.list', {}) == {'accounts': []}
    with pytest.raises(LookupError):
        service.resolve(other, 'personal', item['id'])
    with pytest.raises(ValueError):
        create(service, event, account_id=item['id'])
    with pytest.raises(ValueError):
        create(service, event, secret='chat-secret')


def test_credentials_are_encrypted_bound_and_invalidated_on_connection_change(accounts):
    service, event = accounts
    item = create(service, event)
    with pytest.raises(PermissionError):
        service.resolve(event, 'personal', item['id'], require_ready=True)
    ready = service.set_credential(item['id'], 'protected-app-password', expected_revision=1)
    assert ready['ready'] and ready['revision'] == 2
    path = service.credential_dir / (ready['credential_ref'] + '.json')
    assert b'protected-app-password' not in path.read_bytes()
    assert path.stat().st_mode & 0o077 == 0
    row = service.resolve(event, 'personal', item['id'], require_ready=True)
    assert service.credential(row) == 'protected-app-password'
    assert 'protected-app-password' not in str(ready)
    assert service.invoke(event, 'personal', 'test', {'account_id': item['id']})['status']['sent'] is False
    assert service.invoke(event, 'personal', 'get', {'account_id': item['id']})['tested']
    updated = service.invoke(event, 'personal', 'update', {'account_id': item['id'], 'expected_revision': 2,
        'changes': {'spec': {'imap_host': 'new.example.org'}}})
    assert not updated['ready'] and updated['credential_ref'] is None and not path.exists()
    with pytest.raises(ValueError, match='revision'):
        service.set_credential(item['id'], 'stale-password', expected_revision=2)


def test_disable_and_delete_block_network_and_remove_credentials(accounts):
    service, event = accounts
    item = create(service, event)
    ready = service.set_credential(item['id'], 'protected-password', expected_revision=1)
    result = service.invoke(event, 'personal', 'disable', {'account_id': item['id'], 'expected_revision': 2})
    assert not result['enabled'] and not result['ready']
    with pytest.raises(PermissionError):
        service.invoke(event, 'personal', 'test', {'account_id': item['id']})
    with pytest.raises(ValueError, match='revision'):
        service.invoke(event, 'personal', 'delete', {'account_id': item['id'], 'expected_revision': 2})
    service.invoke(event, 'personal', 'delete', {'account_id': item['id'], 'expected_revision': 3})
    assert not (service.credential_dir / (ready['credential_ref'] + '.json')).exists()
    with pytest.raises(LookupError):
        service.resolve(event, 'personal', item['id'])


def test_accounts_are_isolated_by_project_and_principal(project_core):
    core = project_core
    mail_accounts.create(core.db.engine, checkfirst=True)
    service = MailAccounts(core)
    event = host_event(core, 'mail-scope')
    item = service.invoke(event, 'alpha', 'create', {'account_id': 'weekly',
        'spec': {'provider': 'qq', 'address': 'reader@qq.com'}})
    assert service.invoke(event, 'beta', 'list', {}) == {'accounts': []}
    with pytest.raises(LookupError):
        service.resolve(event, 'beta', item['id'])
    stranger = event.model_copy(update={'principal': event.principal.model_copy(update={'remote_user_id': 'stranger'})})
    with pytest.raises(PermissionError):
        service.resolve(stranger, 'alpha', item['id'])


def test_invalid_model_arguments_do_not_echo_pasted_secrets(accounts):
    service, event = accounts
    with pytest.raises(ValueError) as error:
        create(service, event, spec={'provider': 'qq', 'address': 'reader@qq.com', 'password': 'pasted-secret'})
    assert 'pasted-secret' not in str(error.value)


def test_mailbox_generation_changes_only_with_imap_identity(accounts):
    service, event = accounts
    item = create(service, event)
    assert item['mailbox_generation'] == 1
    item = service.set_credential(item['id'], 'protected-password', expected_revision=item['revision'])
    assert item['mailbox_generation'] == 1
    for changes in ({'enabled': False}, {'enabled': True}, {'spec': {'smtp_host': 'smtp.other.example'}}):
        item = service.invoke(event, 'personal', 'update', {'account_id': item['id'],
            'expected_revision': item['revision'], 'changes': changes})
        assert item['mailbox_generation'] == 1
    item = service.invoke(event, 'personal', 'update', {'account_id': item['id'],
        'expected_revision': item['revision'], 'changes': {'spec': {'username': 'second-mailbox'}}})
    assert item['mailbox_generation'] == 2 and item['credential_ref'] is None
    item = service.set_credential(item['id'], 'replacement-password', expected_revision=item['revision'])
    assert item['mailbox_generation'] == 2


async def test_setup_shell_and_credential_api_protect_credentials(accounts):
    service, event = accounts
    item = create(service, event)
    app = FastAPI()

    def authenticate(authorization: str | None = Header(default=None)):
        if authorization != 'Bearer protected-admin':
            raise HTTPException(401)

    @app.exception_handler(ValueError)
    async def invalid(request, error):
        return JSONResponse({'detail': str(error)}, status_code=422)

    app.include_router(build_mail_router(service.core, authenticate))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
        shell = await client.get('/api/v1/mail')
        assert shell.status_code == 200 and 'type="password"' in shell.text
        assert 'reader@qq.com' not in shell.text
        path = '/api/v1/mail/accounts/' + item['id'] + '/credential'
        body = {'expected_revision': 1, 'secret': 'protected-secret'}
        assert (await client.post(path, json=body)).status_code == 401
        invalid = await client.post(path, json={'secret': 'protected-secret'},
            headers={'Authorization': 'Bearer protected-admin'})
        assert invalid.status_code == 422 and 'protected-secret' not in invalid.text
        response = await client.post(path, json=body, headers={'Authorization': 'Bearer protected-admin'})
        assert response.status_code == 200, response.text
        assert response.json()['ready'] and 'protected-secret' not in response.text
        assert 'protected-secret' not in (await client.get('/api/v1/mail/accounts',
            headers={'Authorization': 'Bearer protected-admin'})).text


async def test_manual_setup_can_bind_only_to_an_active_verified_owner_dialogue(accounts):
    from elephant.permissions import PermissionPolicy, PermissionRule, SubjectSelector

    service, event = accounts
    core = service.core
    source, _ = core.inbox.accept(event)
    core.sessions.enqueue(source, core.access(source), context=core.context_for(source))
    app = FastAPI()

    def authenticate(authorization: str | None = Header(default=None)):
        if authorization != 'Bearer protected-admin':
            raise HTTPException(401)

    app.include_router(build_mail_router(core, authenticate))
    auth = {'Authorization': 'Bearer protected-admin'}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
        response = await client.get('/api/v1/mail/accounts/scopes', headers=auth)
        assert response.status_code == 200, response.text
        assert response.json()['scopes'][0]['scope_event_id'] == source.event_id
        body = {'account_id': 'manual-qq', 'workspace_id': 'personal', 'scope_event_id': source.event_id,
            'spec': {'provider': 'qq', 'address': 'reader@qq.com'}}
        created = await client.post('/api/v1/mail/accounts', headers=auth, json=body)
        assert created.status_code == 200, created.text
        assert service.resolve(source, 'personal', created.json()['id'])['principal'] == source.principal.key
        assert (await client.post('/api/v1/mail/accounts', headers=auth,
            json=body | {'scope_event_id': 'unverified-source'})).status_code == 403
        core.permissions.initial_policy = PermissionPolicy(preset='custom', rules=(PermissionRule(
            id='deny-current-owner', effect='deny', subject=SubjectSelector(kind='person',
                adapter_instance_id=source.principal.adapter_instance_id,
                identity_namespace=source.principal.identity_namespace, user_id=source.principal.remote_user_id),
            actions=('conversation.receive',)),))
        assert (await client.get('/api/v1/mail/accounts/scopes', headers=auth)).json() == {'scopes': []}
        assert (await client.post('/api/v1/mail/accounts', headers=auth, json=body)).status_code == 403
