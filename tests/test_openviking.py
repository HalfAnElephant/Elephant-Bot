import json

import httpx
import pytest
from pydantic import ValidationError

from qqbot.app import create_app
from qqbot.config import Settings
from qqbot.memory import MemoryRecord
from qqbot.memory.openviking_config import OpenVikingConfig


@pytest.fixture
async def ov(tmp_path, official_config, official_event, clock, monkeypatch):
    for key in ('ALL_PROXY', 'all_proxy', 'http_proxy', 'HTTP_PROXY', 'HTTPS_PROXY', 'https_proxy'):
        monkeypatch.delenv(key, raising=False)
    core = create_app(Settings(database=str(tmp_path / 'ov.db'), adapters=(official_config,),
        memory={'openviking': {'endpoint': 'http://127.0.0.1:1933'}}),
        clock=clock, run_background=False).state.core
    event = core.adapters['qq'].normalize({**official_event, 't': 'C2C_MESSAGE_CREATE', 'd': {
        **official_event['d'], 'author': {'user_openid': '300'}, 'content': 'hello'}})
    event = core.inbox.accept(event)[0]
    access = core.access(event)
    calls = []
    state = {'resources': [], 'fail': False}
    async def handler(request):
        body = json.loads(request.content) if request.content else None
        calls.append((request.method, request.url.path, body))
        if state['fail']:
            raise httpx.ReadTimeout('fixture timeout')
        result = {'resources': state['resources']} if request.url.path.endswith('/find') else {}
        return httpx.Response(200, json={'status': 'ok', 'result': result})
    await core.openviking.client.aclose()
    core.openviking.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    yield core, event, access, calls, state
    await core.stop()
    core.db.close()


def record(core, event, access):
    value = core.social.candidate(access, MemoryRecord(scope_type='principal_in_conversation',
        scope_id=access.conversation.key, subject_ref=access.principal.key, predicate='preference',
        value_json='"山峰"', text='山峰', source_event_ids=(event.event_id,), source_kind='user_explicit',
        confidence=1, valid_from=core.clock.now(), created_at=core.clock.now(),
        last_verified_at=core.clock.now()), explicit=True)
    return core.social.commit(access, value.memory_id, authorized=True)


async def prepare(core, event, access):
    turn = core.contexts.turn(event)
    context = core.contexts.build(access, turn)
    await core.openviking.prepare(access, turn, context)


async def test_private_default_snapshots_idempotency_and_cleanup(ov):
    core, event, access, calls, _ = ov
    assert core.openviking.enabled(access)
    await prepare(core, event, access)
    assert core.openviking.status == 'ready'
    creation = next(body for _, path, body in calls if path == '/api/v1/sessions')
    assert creation['auto_commit_policy'] is None
    assert all(not value['enabled'] for value in creation['memory_policy'].values())
    await core.openviking.reconcile()  # Valid stored validation must not crash or delete.
    assert not any(method == 'DELETE' for method, _, _ in calls)
    await prepare(core, event, access)
    assert sum(path.endswith('/messages/batch') for _, path, _ in calls) == 1
    core.social.revoke_source(access, event.event_id)
    await core.openviking.reconcile()
    assert sum(method == 'DELETE' for method, _, _ in calls) == 2


async def test_only_known_local_memories_are_returned_and_forget_cleans(ov):
    from qqbot.memory.openviking import digest
    core, event, access, calls, state = ov
    value = record(core, event, access)
    root = core.openviking.prefix(core.openviking.scope_id(access))
    state['resources'] = [{'uri': root + '/' + digest(value.memory_id) + '.md', 'abstract': 'evil'},
                          {'uri': root + '/foreign.md', 'abstract': 'injected'}]
    await prepare(core, event, access)
    assert core.social.retrieve(access, 'hello')[0].text == '山峰'
    core.social.forget(access, value.memory_id)
    assert core.social.retrieve(access, 'hello') == []
    await core.openviking.reconcile()
    assert any(method == 'DELETE' for method, _, _ in calls)


async def test_timeout_leaves_cleanup_ledger_and_retry_deletes_before_rewrite(ov):
    core, event, access, calls, state = ov
    state['fail'] = True
    await prepare(core, event, access)
    assert core.openviking.status == 'unavailable_local_fallback'
    assert core.openviking._saved(core.openviking.scope_id(access))['state'] == 'PENDING'
    state['fail'] = False
    calls.clear()
    await prepare(core, event, access)
    assert calls[0][0] == calls[1][0] == 'DELETE'
    assert core.openviking.status == 'ready'


async def test_unconfigured_and_group_do_not_export(ov):
    from qqbot.domain import Surface
    core, event, access, calls, _ = ov
    group = access.model_copy(update={'conversation': access.conversation.model_copy(
        update={'surface': Surface.GROUP})})
    assert not core.openviking.enabled(group)
    core.openviking.settings = core.settings.model_copy(update={'memory': core.settings.memory.model_copy(
        update={'openviking': OpenVikingConfig()})})
    await prepare(core, event, access)
    assert calls == []


@pytest.mark.parametrize('endpoint', ['http://example.com', 'https://u:p@example.com',
    'https://example.com/path', 'https://example.com?secret=x'])
def test_insecure_endpoints_rejected(endpoint):
    with pytest.raises(ValidationError):
        OpenVikingConfig(endpoint=endpoint)


async def test_permission_change_removes_unselected_exported_records(ov):
    from qqbot.permissions import PermissionPolicy, PermissionRule
    core, event, access, calls, _ = ov
    record(core, event, access)  # Does not lexically match hello; absent from selected memory manifest.
    await prepare(core, event, access)
    core.permissions.set_policy(PermissionPolicy(rules=(PermissionRule(
        id='revoke-source', actions=('memory.source.read',), effect='deny'),)),
        expected_version=core.permissions.policy_version, actor='test')
    await core.openviking.reconcile()
    assert any(method == 'DELETE' for method, _, _ in calls)
    assert core.openviking._saved(core.openviking.scope_id(access)) is None


async def test_expired_export_is_removed_without_memory_version_change(ov):
    from datetime import timedelta

    from sqlalchemy import update

    from qqbot.storage.social_schema import social_records
    core, event, access, calls, _ = ov
    value = record(core, event, access)
    expiring = value.model_copy(update={'valid_until': core.clock.now() + timedelta(seconds=1)})
    with core.db.transaction() as conn:
        conn.execute(update(social_records).where(social_records.c.id == value.memory_id).values(
            record_json=expiring.model_dump_json()))
    await prepare(core, event, access)
    core.clock.advance(seconds=2)
    await core.openviking.reconcile()
    assert any(method == 'DELETE' for method, _, _ in calls)


async def test_disabled_service_cleans_then_endpoint_removal_reports_pending(ov):
    core, event, access, calls, _ = ov
    await prepare(core, event, access)
    settings = core.settings
    core.openviking.settings = settings.model_copy(update={'memory': settings.memory.model_copy(
        update={'openviking': OpenVikingConfig()})})
    await core.openviking.reconcile()
    assert core.openviking.status == 'endpoint_removed_cleanup_required'
    core.openviking.settings = settings.model_copy(update={'memory': settings.memory.model_copy(
        update={'openviking': settings.memory.openviking.model_copy(update={'private_enabled': False})})})
    await core.openviking.reconcile()
    assert core.openviking._saved(core.openviking.scope_id(access)) is None
    assert any(method == 'DELETE' for method, _, _ in calls)


async def test_cleanup_rotates_past_first_page(ov):
    from sqlalchemy import insert

    from qqbot.storage.openviking_schema import openviking_scopes
    core, event, access, _, _ = ov
    await prepare(core, event, access)
    saved = dict(core.openviking._saved(core.openviking.scope_id(access)))
    with core.db.transaction() as conn:
        for index in range(130):
            conn.execute(insert(openviking_scopes).values({**saved, 'id': f'z{index:04d}'}))
    await core.openviking.reconcile()
    assert core.openviking._cleanup_cursor
    await core.openviking.reconcile()
    assert core.openviking._cleanup_cursor == ''


async def test_private_principals_and_conversations_have_distinct_remote_scopes(ov):
    core, _, access, _, _ = ov
    other_user = access.model_copy(update={'principal': access.principal.model_copy(
        update={'remote_user_id': 'different-user'})})
    other_chat = access.model_copy(update={'conversation': access.conversation.model_copy(
        update={'remote_conversation_id': 'different-chat'})})
    prefixes = {core.openviking.prefix(core.openviking.scope_id(item))
                for item in (access, other_user, other_chat)}
    assert len(prefixes) == 3


async def test_group_export_requires_explicit_enable(ov):
    from qqbot.domain import Surface

    core, _, access, _, _ = ov
    group = access.model_copy(update={'conversation': access.conversation.model_copy(
        update={'surface': Surface.GROUP})})
    assert not core.openviking.enabled(group)
    settings = core.settings
    core.openviking.settings = settings.model_copy(update={'memory': settings.memory.model_copy(
        update={'openviking': settings.memory.openviking.model_copy(update={'group_enabled': True})})})
    assert core.openviking.enabled(group)
    assert core.openviking.scope_id(group) != core.openviking.scope_id(access)
