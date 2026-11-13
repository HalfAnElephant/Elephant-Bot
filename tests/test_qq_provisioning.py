"""Automatic provisioning commits credentials, owner scope and policy before activation."""
import asyncio
import base64
import hashlib
import json
import time
from dataclasses import replace
from types import SimpleNamespace

import httpx
import pytest

from qqbot.adapters.qq_official.credential_store import load_credential, save_credential
from qqbot.adapters.qq_official.onboarding import Session
from qqbot.admin.qq_provisioning import TOKEN_URL, ProvisioningError, provision_owner
from qqbot.config import AdapterConfig, QQOwnerBinding, Settings
from qqbot.configuration import ConfigurationStore, version
from qqbot.permissions import PermissionPolicy, PermissionRule, PermissionService, PermissionSubject


@pytest.fixture
async def provisioning(db, clock, monkeypatch):
    monkeypatch.setenv('QQBOT_CREDENTIAL_KEY', base64.b64encode(b'K' * 32).decode())
    monkeypatch.setenv('QQBOT_ADMIN_TOKEN', 'administrator-token')
    policy = PermissionPolicy(rules=(PermissionRule(id='existing-global-denial', priority=30,
        effect='deny', actions=('workspace.delete',)),))
    settings = Settings(database=db.path, permissions=policy)
    core = SimpleNamespace(settings=settings, clock=clock, config_store=ConfigurationStore(settings),
        permissions=PermissionService(db, clock, policy), adapters={})
    now = time.monotonic()
    session = Session('scan-session', hashlib.sha256(b'Bearer administrator-token').hexdigest(),
        'qq-personal', now, now + 300, state='validating', automatic=True,
        app_id='12345', secret='private-scanned-secret', scanner_openid='private-scanner-openid')
    remote = SimpleNamespace(calls=[], before=None, status=200,
        body={'access_token': 'private-access-token', 'expires_in': 7200}, offline=False)
    activated = []
    fixture = SimpleNamespace(core=core, session=session, remote=remote, activated=activated,
        directory=core.config_store.path.parent / 'qq-credentials')
    async def request(message):
        remote.calls.append((str(message.url), json.loads(message.content)))
        if remote.before:
            await remote.before()
        if remote.offline:
            raise httpx.ConnectError('sensitive platform body: ' + session.secret)
        return httpx.Response(remote.status, json=remote.body)
    async def activate(candidate, adapter):
        assert core.config_store.read() == candidate
        assert core.permissions.list()['rules'] == candidate.permissions.model_dump(mode='json')['rules']
        assert load_credential(adapter.credential_store_dir, adapter.credential_ref, adapter.app_id) == 'private-scanned-secret'
        assert fixture.session.provisioned and fixture.session.state == 'connecting' and fixture.session.secret is None
        activated.append((candidate, adapter))
        return {'connected': True, 'state': 'CONNECTED', 'error': None, 'retry': False}
    async with httpx.AsyncClient(transport=httpx.MockTransport(request)) as client:
        fixture.client, fixture.activate = client, activate
        yield fixture


def stored_files(fixture):
    return list(fixture.directory.glob('*.json'))


async def test_first_automatic_scan_persists_verified_owner_and_exact_scoped_policy_before_activation(provisioning):
    fixture = provisioning
    before_other = fixture.core.permissions.check(PermissionSubject(adapter_instance_id='other'),
        'workspace.delete', 'workspace:example').allowed
    result = await provision_owner(fixture.core, fixture.session, client=fixture.client, activate=fixture.activate)
    saved = fixture.core.config_store.read()
    adapter = saved.adapters[0]
    assert adapter.enabled and adapter.transport == 'websocket' and adapter.intents == 1 << 25
    assert adapter.admin_users == ('private-scanner-openid',) and adapter.token_url == TOKEN_URL
    assert not adapter.secret_env and adapter.credential_ref and adapter.secret() == 'private-scanned-secret'
    assert len(saved.owner.qq_bindings) == 1
    owner = saved.owner.qq_bindings[0]
    assert owner.app_id == '12345' and owner.user_openid == 'private-scanner-openid'
    assert owner.verified_at == fixture.core.clock.now().isoformat()
    exact_owner = PermissionSubject(adapter_instance_id='qq-personal', identity_namespace='qq_app:12345',
        user_id='private-scanner-openid')
    foreign_owner = exact_owner.model_copy(update={'user_id': 'another-user'})
    foreign_app = exact_owner.model_copy(update={'identity_namespace': 'qq_app:other'})
    assert fixture.core.permissions.check(exact_owner, 'workspace.delete', 'workspace:example').allowed
    assert not fixture.core.permissions.check(foreign_owner, 'task.execute', '*').allowed
    assert not fixture.core.permissions.check(foreign_app, 'task.execute', '*').allowed
    assert fixture.core.permissions.check(PermissionSubject(adapter_instance_id='other'),
        'workspace.delete', 'workspace:example').allowed == before_other
    assert saved.permissions.rules[0].id == 'existing-global-denial'
    assert result['saved'] and result['activated'] and result['owner_bound']
    assert result['saved_version'] == version(saved) and fixture.session.state == 'connected'
    assert fixture.remote.calls == [(TOKEN_URL, {'appId': '12345', 'clientSecret': 'private-scanned-secret'})]
    public = json.dumps(fixture.session.public())
    assert all(private not in public for private in ('private-scanned-secret', 'private-scanner-openid', 'private-access-token'))
    assert len(fixture.activated) == 1


async def test_automatic_rotation_enables_websocket_preserves_tuning_and_keeps_working_credential(provisioning):
    fixture = provisioning
    reference = save_credential(fixture.directory, '12345', 'working-old-secret')
    existing = AdapterConfig(instance_id='qq-personal', kind='official', app_id='12345', bot_id='existing-bot',
        credential_ref=reference, credential_store_dir=str(fixture.directory), enabled=False, transport='webhook',
        intents=(1 << 25) | (1 << 26), admin_users=('existing-admin',), markdown_enabled=False,
        streaming_enabled=True, official_messages_per_day=321, allowed_groups=('existing-group',))
    binding = QQOwnerBinding(app_id='12345', user_openid='private-scanner-openid', verified_at='2025-01-01T00:00:00+00:00')
    previous = fixture.core.settings.model_copy(update={'adapters': (existing,),
        'owner': fixture.core.settings.owner.model_copy(update={'qq_bindings': (binding,), 'preferences': 'Keep private preferences.'})})
    fixture.core.config_store.save(fixture.core.settings, previous)
    fixture.core.settings = previous
    fixture.session.replace_app_id = '12345'
    result = await provision_owner(fixture.core, fixture.session, client=fixture.client, activate=fixture.activate)
    saved = fixture.core.config_store.read()
    rotated = saved.adapters[0]
    assert result['activated'] and rotated.enabled and rotated.transport == 'websocket'
    changed = {key for key in AdapterConfig.model_fields if getattr(existing, key) != getattr(rotated, key)}
    assert changed == {'credential_ref', 'enabled', 'transport', 'admin_users'}
    assert rotated.admin_users == ('existing-admin', 'private-scanner-openid')
    assert load_credential(fixture.directory, reference, '12345') == 'working-old-secret'
    assert saved.owner == previous.owner and len(stored_files(fixture)) == 2
    assert len(fixture.activated) == 1


async def test_existing_app_cannot_rebind_another_scanner_or_change_private_owner_data(provisioning):
    fixture = provisioning
    previous = fixture.core.settings.model_copy(update={'owner': fixture.core.settings.owner.model_copy(update={
        'qq_bindings': (QQOwnerBinding(app_id='12345', user_openid='original-owner', verified_at='2025-01-01'),),
        'preferences': 'Original private owner preferences.'})})
    fixture.core.config_store.save(fixture.core.settings, previous)
    with pytest.raises(ProvisioningError, match='owner_binding_conflict'):
        await provision_owner(fixture.core, fixture.session, client=fixture.client, activate=fixture.activate)
    assert fixture.core.config_store.read() == previous
    assert fixture.core.permissions.policy_version == 0
    assert not fixture.remote.calls and not fixture.activated and not stored_files(fixture)


@pytest.mark.parametrize('invalid', ['manual', 'cancelled', 'expired'])
async def test_unapproved_or_inactive_session_never_validates_or_persists(provisioning, invalid):
    fixture = provisioning
    if invalid == 'manual':
        fixture.session.automatic = False
    elif invalid == 'cancelled':
        fixture.session.state = 'cancelled'
    else:
        fixture.session.expires = time.monotonic() - 1
    with pytest.raises(ProvisioningError):
        await provision_owner(fixture.core, fixture.session, client=fixture.client, activate=fixture.activate)
    assert not fixture.remote.calls and not stored_files(fixture) and not fixture.activated


@pytest.mark.parametrize('invalid', ['status', 'redirect', 'missing_token', 'oversized', 'offline'])
async def test_bad_token_validation_leaves_no_credentials_policy_or_config(provisioning, invalid):
    fixture = provisioning
    if invalid == 'status':
        fixture.remote.status = 401
    elif invalid == 'redirect':
        fixture.remote.status = 302
    elif invalid == 'missing_token':
        fixture.remote.body = {'detail': 'private-scanned-secret'}
    elif invalid == 'oversized':
        fixture.remote.body = {'access_token': 'x' * 65536}
    else:
        fixture.remote.offline = True
    with pytest.raises(ProvisioningError) as error:
        await provision_owner(fixture.core, fixture.session, client=fixture.client, activate=fixture.activate)
    assert error.value.code == 'credential_validation_failed'
    assert 'private-' not in str(error.value)
    assert not fixture.core.config_store.path.exists() and not stored_files(fixture)
    assert fixture.core.permissions.policy_version == 0 and not fixture.activated


@pytest.mark.parametrize('change', ['cancel', 'permission', 'administrator', 'identity'])
async def test_validation_await_rechecks_session_current_authority_and_scanned_identity(provisioning, change):
    fixture = provisioning
    async def changed():
        if change == 'cancel':
            fixture.session.state = 'cancelled'
        elif change == 'permission':
            fixture.core.permissions.set_policy(PermissionPolicy(rules=(PermissionRule(id='revoke-onboarding',
                priority=1000, effect='deny', actions=('permission.set_policy',)),)), expected_version=0, actor='test')
        elif change == 'administrator':
            fixture.session.owner = hashlib.sha256(b'Bearer replaced-token').hexdigest()
        else:
            fixture.session.scanner_openid = 'changed-after-validation'
    fixture.remote.before = changed
    with pytest.raises(ProvisioningError):
        await provision_owner(fixture.core, fixture.session, client=fixture.client, activate=fixture.activate)
    assert not fixture.core.config_store.path.exists() and not stored_files(fixture) and not fixture.activated


async def test_configuration_rebases_latest_unrelated_saved_settings_without_hot_applying_them(provisioning):
    fixture = provisioning
    async def changed():
        previous = fixture.core.config_store.read()
        fixture.core.config_store.save(previous, previous.model_copy(update={'send_budget_per_minute': 7}))
    fixture.remote.before = changed
    await provision_owner(fixture.core, fixture.session, client=fixture.client, activate=fixture.activate)
    assert fixture.core.config_store.read().send_budget_per_minute == 7
    assert fixture.core.settings.send_budget_per_minute == 0


async def test_configuration_failure_restores_policy_and_keeps_old_working_config(provisioning, monkeypatch):
    fixture = provisioning
    def failed(previous, candidate):
        raise OSError('private-scanned-secret must never be returned')
    monkeypatch.setattr(fixture.core.config_store, 'save', failed)
    with pytest.raises(ProvisioningError) as error:
        await provision_owner(fixture.core, fixture.session, client=fixture.client, activate=fixture.activate)
    assert error.value.code == 'configuration_save_failed'
    assert not fixture.core.config_store.path.exists() and not stored_files(fixture)
    assert fixture.core.permissions.list()['rules'] == fixture.core.settings.permissions.model_dump(mode='json')['rules']
    assert fixture.core.permissions.policy_version == 2
    assert not fixture.session.provisioned and not fixture.activated


async def test_staged_credential_readback_failure_is_cleaned_before_policy_or_config_commit(provisioning, monkeypatch):
    fixture = provisioning
    def corrupted(directory, reference, app_id):
        raise ValueError('credential failed authenticated decryption')
    monkeypatch.setattr('qqbot.admin.qq_provisioning.load_credential', corrupted)
    with pytest.raises(ProvisioningError, match='credential_save_failed'):
        await provision_owner(fixture.core, fixture.session, client=fixture.client, activate=fixture.activate)
    assert not fixture.core.config_store.path.exists() and not stored_files(fixture)
    assert fixture.core.permissions.policy_version == 0 and not fixture.activated


async def test_authenticated_credential_readback_preserves_supported_utf8_secret(provisioning):
    fixture = provisioning
    secret = '令牌秘密✓'
    fixture.session.secret = secret
    async def activate(candidate, adapter):
        assert adapter.secret() == secret
        return {'connected': True, 'state': 'CONNECTED'}
    result = await provision_owner(fixture.core, fixture.session, client=fixture.client, activate=activate)
    assert result['saved'] and result['activated']
    assert secret not in json.dumps(fixture.session.public(), ensure_ascii=False)


async def test_policy_cas_conflict_preserves_concurrent_policy_and_removes_unreferenced_key(provisioning, monkeypatch):
    fixture = provisioning
    mutate = fixture.core.permissions.set_policy
    def raced(policy, *, expected_version, actor):
        mutate(PermissionPolicy(rules=(PermissionRule(id='concurrent-rule', effect='deny', actions=('web.fetch',)),)),
            expected_version=expected_version, actor='another-admin')
        return mutate(policy, expected_version=expected_version, actor=actor)
    monkeypatch.setattr(fixture.core.permissions, 'set_policy', raced)
    with pytest.raises(ProvisioningError, match='permission_changed'):
        await provision_owner(fixture.core, fixture.session, client=fixture.client, activate=fixture.activate)
    assert fixture.core.permissions.get('concurrent-rule')
    assert not fixture.core.config_store.path.exists() and not stored_files(fixture) and not fixture.activated


async def test_saved_config_ack_loss_activates_the_verified_durable_candidate_once(provisioning, monkeypatch):
    fixture = provisioning
    save = fixture.core.config_store.save
    def lost_ack(previous, candidate):
        save(previous, candidate)
        raise OSError('save receipt lost')
    monkeypatch.setattr(fixture.core.config_store, 'save', lost_ack)
    result = await provision_owner(fixture.core, fixture.session, client=fixture.client, activate=fixture.activate)
    assert result['saved'] and result['activated'] and len(fixture.activated) == 1
    assert fixture.core.config_store.read().adapters[0].secret() == 'private-scanned-secret'


async def test_connecting_receiver_is_saved_but_not_reported_as_connected(provisioning):
    fixture = provisioning
    async def connecting(candidate, adapter):
        assert fixture.session.provisioned and fixture.core.config_store.read() == candidate
        return {'connected': False, 'state': 'CONNECTING', 'error': None, 'retry': True}
    result = await provision_owner(fixture.core, fixture.session, client=fixture.client, activate=connecting)
    assert result['saved'] and not result['activated'] and not result['connection']['connected']
    assert fixture.session.state == 'connecting'


async def test_same_owner_rescan_preserves_subsequent_high_priority_permission_restrictions(provisioning):
    fixture = provisioning
    await provision_owner(fixture.core, fixture.session, client=fixture.client, activate=fixture.activate)
    owner = PermissionSubject(adapter_instance_id='qq-personal', identity_namespace='qq_app:12345',
        user_id='private-scanner-openid')
    assert fixture.core.permissions.check(owner, 'workspace.delete', 'workspace:restricted').allowed
    fixture.core.permissions.upsert(PermissionRule(id='subsequent-owner-restriction', priority=1000,
        effect='deny', actions=('workspace.delete',), resources=('workspace:restricted',)),
        expected_version=fixture.core.permissions.policy_version, actor='administrator')
    before = fixture.core.permissions.list()
    other = PermissionSubject(adapter_instance_id='another-adapter')
    before_other = fixture.core.permissions.check(other, 'workspace.delete', 'workspace:restricted')
    fixture.session = replace(fixture.session, id='rescan-session', state='validating', provisioned=False,
        secret='private-scanned-secret', scanner_openid='private-scanner-openid', result={},
        expires=time.monotonic() + 300)
    result = await provision_owner(fixture.core, fixture.session, client=fixture.client, activate=fixture.activate)
    assert result['saved'] and result['activated']
    after = fixture.core.permissions.list()
    assert {key: value for key, value in before.items() if key != 'policy_version'} == {
        key: value for key, value in after.items() if key != 'policy_version'}
    assert not fixture.core.permissions.check(owner, 'workspace.delete', 'workspace:restricted').allowed
    after_other = fixture.core.permissions.check(other, 'workspace.delete', 'workspace:restricted')
    assert (before_other.effect, before_other.matched_rule) == (after_other.effect, after_other.matched_rule)


async def test_reserved_permission_rule_identity_mismatch_is_not_overwritten(provisioning):
    fixture = provisioning
    fixture.core.permissions.upsert(PermissionRule(id='qq-owner:qq-personal:allow', effect='allow'),
        expected_version=0, actor='administrator')
    before = fixture.core.permissions.list()
    with pytest.raises(ProvisioningError, match='permission_rule_conflict'):
        await provision_owner(fixture.core, fixture.session, client=fixture.client, activate=fixture.activate)
    assert fixture.core.permissions.list() == before
    assert not fixture.core.config_store.path.exists() and not stored_files(fixture) and not fixture.activated


async def test_activation_failure_preserves_restartable_config_and_only_safe_diagnostics(provisioning):
    fixture = provisioning
    async def failed(candidate, adapter):
        raise RuntimeError('private-scanned-secret private-scanner-openid private-access-token')
    result = await provision_owner(fixture.core, fixture.session, client=fixture.client, activate=failed)
    assert result['saved'] and not result['activated'] and fixture.session.provisioned
    assert result['connection']['state'] == 'FAILED' and result['connection']['error'] == 'ACTIVATION_FAILED'
    assert fixture.core.config_store.read().adapters[0].secret() == 'private-scanned-secret'
    assert all(private not in json.dumps(fixture.session.public())
        for private in ('private-scanned-secret', 'private-scanner-openid', 'private-access-token'))


async def test_shutdown_cancellation_awaits_activation_cleanup_and_preserves_saved_config(provisioning):
    fixture = provisioning
    entered, cleaned = asyncio.Event(), asyncio.Event()
    async def pending(candidate, adapter):
        try:
            entered.set()
            await asyncio.Event().wait()
        finally:
            cleaned.set()
    task = asyncio.create_task(provision_owner(fixture.core, fixture.session, client=fixture.client, activate=pending))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cleaned.is_set() and fixture.session.provisioned and fixture.session.state == 'connecting'
    assert fixture.core.config_store.read().adapters[0].secret() == 'private-scanned-secret'
