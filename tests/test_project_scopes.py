"""Project identity stays frozen across focus changes, memory changes and HOST replay."""
import hashlib
import json
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select, update

from qqbot.app import create_app
from qqbot.config import Settings
from qqbot.contracts.memory import MemorySearchResult
from qqbot.domain import ConversationRef, InboundEvent, MediaSegment, PrincipalRef, Surface, TextSegment
from qqbot.storage.agent_schema import dialogue_inputs
from qqbot.storage.memory_sync_schema import memory_owners
from qqbot.storage.schema import inbox
from qqbot.storage.task_schema import tasks


@pytest.fixture
async def project_core(tmp_path, clock, official_config):
    settings = Settings(database=str(tmp_path / 'projects.db'), adapters=(official_config,),
        owner={'id': 'owner', 'qq_bindings': [{'app_id': 'app', 'user_openid': '300', 'verified_at': clock.now().isoformat()}]},
        orchestration={'workspace_bindings': {'alpha': str(tmp_path / 'alpha'), 'beta': str(tmp_path / 'beta')}},
        memory={'openviking': {'endpoint': 'http://127.0.0.1:1933', 'bindings': [
            {'owner_id': 'owner', 'workspace_id': name, 'account_id': 'elephant',
             'user_id': name, 'api_key_env': 'PROJECT_MEMORY_TEST_KEY'} for name in ('personal', 'alpha', 'beta')]}})
    core = create_app(settings, clock=clock, run_background=False).state.core
    yield core
    await core.stop()
    core.db.close()


def host_event(core, name, text='检查项目'):
    return InboundEvent(event_id=name, event_type='message.created', platform_message_id=name,
        conversation=ConversationRef(deployment_id=core.settings.deployment_id, adapter_instance_id='admin_api',
            bot_identity_id='host', surface=Surface.PRIVATE, remote_conversation_id=core.settings.owner.id),
        principal=PrincipalRef(adapter_instance_id='admin_api', identity_namespace='admin_api', remote_user_id=core.settings.owner.id),
        occurred_at=core.clock.now(), received_at=core.clock.now(), trace_id=name, segments=(TextSegment(text=text),))


def test_project_dialogues_are_distinct_and_original_input_stays_frozen(project_core):
    core = project_core
    original = host_event(core, 'alpha-input')
    original, _ = core.inbox.accept(original)
    context = core.context_for(original, workspace_id='alpha')
    accepted = core.sessions.enqueue(original, core.access(original), context=context)
    with core.db.transaction() as conn:
        beta = core.sessions.dialogue(conn, host_event(core, 'beta-select'), workspace_id='beta')
        assert beta['id'] != accepted['dialogue_id']
        with pytest.raises(ValueError, match='cannot change'):
            core.sessions.dialogue(conn, original, select_ref=accepted['dialogue_id'], workspace_id='beta')
    assert core.sessions.workspace_for(original) == 'alpha'
    assert core.sessions.workspace_for(host_event(core, 'fresh')) == 'beta'
    assert core.knowledge.scope(original).workspace_id == 'alpha'
    assert core.knowledge.scope(original).audience == 'project'
    claim = core.sessions.claim('leader')
    assert claim['context']['workspace_id'] == 'alpha'
    _event, binding, _turns = core.knowledge._session_context(claim['session_id'])
    assert binding['workspace_id'] == 'alpha'


def test_project_scope_rejects_unconfigured_or_unbound_identity(project_core):
    core = project_core
    event = host_event(core, 'selection')
    for workspace in ('../alpha', '/tmp', 'unknown'):
        with pytest.raises(ValueError):
            core.context_for(event, workspace_id=workspace)
    impostor = event.model_copy(update={'principal': event.principal.model_copy(update={'remote_user_id': 'other'})})
    with pytest.raises(PermissionError):
        core.sessions.validate_workspace(impostor, 'alpha')


def test_project_skills_share_within_project_and_hide_other_projects(project_core):
    core = project_core
    access = core.access(host_event(core, 'skills'))
    alpha = core.skills_for(access, workspace_id='alpha')
    result = alpha.skill_manage({'operations': [{'action': 'create', 'name': 'alpha-only',
        'content': '---\nname: alpha-only\ndescription: Alpha procedure\n---\nRead and verify alpha inputs.'}]})
    assert result['success']
    assert 'alpha-only' in str(core.skills_for(access, workspace_id='alpha').skills_list())
    assert 'alpha-only' not in str(core.skills_for(access, workspace_id='beta').skills_list())
    assert 'alpha-only' not in str(core.skills_for(access).skills_list())


def test_host_structured_source_is_atomic_idempotent_and_memory_scoped(project_core):
    core = project_core
    receipt = core.enqueue_admin_task('完成任务', idempotency_key='structured', workspace_id='alpha',
        success_checks=('artifact',), requested_outputs=('outputs/report.txt',))
    with core.db.engine.connect() as conn:
        root = conn.execute(select(tasks).where(tasks.c.id == receipt['task_id'])).mappings().one()
        context = json.loads(root['context_json'])
        source = conn.execute(select(inbox).where(inbox.c.id == json.loads(root['event_json'])['event_id'])).mappings().one()
        assert source['state'] == 'DONE'
        assert not conn.execute(select(dialogue_inputs)).first()
        assert context['memory_scope']['workspace_id'] == 'alpha'
        assert context['memory_scope']['audience'] == 'project'
        assert context['workspace_id'] == 'alpha'
        structured = context['structured_request']
        encoded = json.dumps(structured['request'], sort_keys=True, separators=(',', ':'), ensure_ascii=False)
        assert structured['sha256'] == hashlib.sha256(encoded.encode()).hexdigest()
        assert core.validate_context(core.access(InboundEvent.model_validate_json(root['event_json'])), context)
    assert core.enqueue_admin_task('完成任务', idempotency_key='structured', workspace_id='alpha',
        success_checks=('artifact',), requested_outputs=('outputs/report.txt',))['task_id'] == receipt['task_id']
    with pytest.raises(ValueError, match='different content'):
        core.enqueue_admin_task('完成任务', idempotency_key='structured', workspace_id='alpha',
            requested_outputs=('outputs/other.txt',))


async def test_host_memory_replan_preserves_structured_checks_and_real_followups(project_core):
    core = project_core
    receipt = core.enqueue_admin_task('处理资料', idempotency_key='replan', workspace_id='alpha',
        success_checks=('artifact',), requested_outputs=('outputs/final.txt',))
    followup = host_event(core, 'real-followup', '仅使用公开资料')
    followup = followup.model_copy(update={'segments': (*followup.segments, MediaSegment(type='file',
        attachment_ref='new-file', source_url='https://cdn.example/new', filename='followup.docx'))})
    followup, _ = core.inbox.accept(followup)
    with core.db.transaction() as conn:
        root = conn.execute(select(tasks).where(tasks.c.id == receipt['task_id'])).mappings().one()
        event = InboundEvent.model_validate_json(root['event_json'])
        scope = core.knowledge.scope(event, workspace_id='alpha')
        core.tasks.add_graph_input(conn, root, followup, core.access(event), followup.text)
        conn.execute(update(memory_owners).where(memory_owners.c.id == scope.key).values(epoch=1))
        core.knowledge.invalidate_root(conn, root, scope, 'corrected', ['unrelated-memory-source'])
    core.openviking.status = AsyncMock(return_value={'state': 'READY', 'memory_epoch': 1})
    core.openviking.search = AsyncMock(return_value=MemorySearchResult(status='ready', memory_epoch=1))
    await core.knowledge.replan()
    with core.db.engine.connect() as conn:
        root = conn.execute(select(tasks).where(tasks.c.id == receipt['task_id'])).mappings().one()
        goal = json.loads(root['goal_json'])
        assert root['goal_revision'] == 2
        assert goal['success_checks'] == ['artifact']
        assert goal['requested_outputs'] == ['outputs/final.txt']
        assert '仅使用公开资料' in root['goal']
        context = json.loads(root['context_json'])
        assert context['workspace_id'] == 'alpha'
        assert context['files'][0]['source'] == followup.event_id
        assert followup.event_id in {source['id'] for source in context['source_manifest']}


def test_attachment_followup_reuses_only_same_project_authorized_sources(project_core):
    core = project_core
    original = host_event(core, 'file-source', '请读取附件')
    original = original.model_copy(update={'segments': (*original.segments, MediaSegment(type='file',
        attachment_ref='file', source_url='https://cdn.example/data.csv', filename='data.csv', size=12))})
    original, _ = core.inbox.accept(original)
    core.sessions.enqueue(original, core.access(original), context=core.context_for(original, workspace_id='alpha'))
    followup = host_event(core, 'file-followup', '解释第二列')
    context = core.context_for(followup, workspace_id='alpha')
    assert context['files'][0]['source'] == original.event_id
    assert context['files'][0]['filename'] == 'data.csv'
    assert not core.context_for(followup, workspace_id='beta')['files']
    recalled = original.model_copy(update={'event_id': 'recall', 'event_type': 'message.recalled'})
    core.inbox.accept(recalled)
    assert not core.context_for(followup, workspace_id='alpha')['files']


def test_maximum_chinese_host_goal_keeps_verifiable_structured_source(project_core):
    core = project_core
    goal = '中' * 16000
    receipt = core.enqueue_admin_task(goal, idempotency_key='large-chinese', workspace_id='alpha',
        success_checks=('artifact',), requested_outputs=('outputs/report.txt',))
    with core.db.engine.connect() as conn:
        raw = conn.execute(select(tasks.c.event_json).where(tasks.c.id == receipt['task_id'])).scalar_one()
    event = InboundEvent.model_validate_json(raw)
    assert event.text == goal
    payload = next(segment.payload for segment in event.segments if segment.type == 'structured')
    assert payload['goal_sha256'] == hashlib.sha256(goal.encode()).hexdigest()
    assert 'goal' not in payload


@pytest.mark.parametrize("barrier_state", ["barrier", "FROZEN", "REVOKED"])
def test_forgotten_attachment_cannot_reenter_fresh_epoch_or_delivery_context(project_core, barrier_state):
    from qqbot.storage.memory_sync_schema import memory_barriers, memory_sources
    core = project_core
    original = host_event(core, 'forgotten-file')
    original = original.model_copy(update={'segments': (*original.segments, MediaSegment(type='file',
        attachment_ref='file', source_url='https://cdn.example/file', filename='private.docx'))})
    original, _ = core.inbox.accept(original)
    context = core.context_for(original, workspace_id='alpha')
    core.sessions.enqueue(original, core.access(original), context=context)
    scope = core.knowledge.scope(original, workspace_id='alpha')
    owner = core.openviking._owner(scope)
    with core.db.transaction() as conn:
        conn.execute(update(memory_owners).where(memory_owners.c.id == scope.key).values(epoch=1))
        if barrier_state == 'barrier':
            conn.execute(memory_barriers.insert().values(id='forgotten-attachment', scope_id=scope.key,
                epoch=1, operation_id='forget', source_id=original.event_id, created_at=core.clock.now().isoformat()))
        else:
            conn.execute(memory_sources.insert().values(id='revoked-original', scope_id=scope.key,
                source_id=original.event_id, revision='v1', event_id='native-source', operation_id='forget',
                content_hash='hash', state=barrier_state))
    assert not core.validate_context(core.access(original), context)
    assert not core.validate_context(core.access(original), {**context, 'memory_scope': scope.model_dump(), 'memory_epoch': 1})
    fresh = host_event(core, 'fresh-after-forget')
    new_context = core.context_for(fresh, workspace_id='alpha')
    assert not new_context['files']
    resent = original.model_copy(update={'event_id': 'explicitly-resent', 'platform_message_id': 'new-message'})
    resent, _ = core.inbox.accept(resent)
    resent_context = core.context_for(resent, workspace_id='alpha')
    assert resent_context['files'][0]['source'] == 'explicitly-resent'
    assert core.validate_context(core.access(resent), resent_context)
    assert owner['epoch'] == 0


def test_group_project_requires_explicit_group_grant(project_core, official_event):
    from qqbot.permissions import PermissionRule
    core = project_core
    event = core.adapters['qq'].normalize(official_event)
    assert event.conversation.surface != Surface.PRIVATE
    with pytest.raises(PermissionError, match='explicit scoped grant'):
        core.sessions.validate_workspace(event, 'alpha')
    rule = PermissionRule(id='group-alpha', effect='allow', subject={'kind': 'group',
        'adapter_instance_id': event.conversation.adapter_instance_id,
        'group_id': event.conversation.remote_conversation_id}, actions=('workspace.use',), resources=('project:alpha',))
    core.permissions.upsert(rule, expected_version=core.permissions.policy_version, actor='fixture')
    assert core.sessions.validate_workspace(event, 'alpha') == 'alpha'
    with pytest.raises(PermissionError):
        core.sessions.validate_workspace(event, 'beta')


def test_legacy_group_memory_binding_does_not_become_private_project(project_core):
    from qqbot.contracts.memory import MemoryBinding
    core = project_core
    group = 'a' * 64
    binding = MemoryBinding(owner_id=core.settings.owner.id, workspace_id=group,
        account_id='elephant', user_id='legacy-group', api_key_env='PROJECT_MEMORY_TEST_KEY')
    config = core.settings.memory.openviking.model_copy(update={'bindings': (*core.settings.memory.openviking.bindings, binding)})
    settings = core.settings.model_copy(update={'memory': core.settings.memory.model_copy(update={'openviking': config})})
    core.sessions.settings = settings
    assert group not in core.sessions.configured_workspaces()
    orchestration = settings.orchestration.model_copy(update={'workspace_bindings': {group: '/configured/project'}})
    core.sessions.settings = settings.model_copy(update={'orchestration': orchestration})
    assert group in core.sessions.configured_workspaces()


def test_skill_write_failures_are_durable_deduplicated_and_redacted(project_core):
    from qqbot.skills import record_skill_result
    from qqbot.storage.schema import audit_events
    core = project_core
    record_skill_result(core.tasks, 'run', 'call', {'success': False, 'error': 'private body and token'})
    record_skill_result(core.tasks, 'run', 'call', {'success': False, 'error': 'private body and token'})
    record_skill_result(core.tasks, 'run', 'ok', {'success': True})
    with core.db.engine.connect() as conn:
        rows = conn.execute(select(audit_events).where(audit_events.c.kind == 'skills.write_failed')).mappings().all()
    assert len(rows) == 1
    assert json.loads(rows[0]['detail']) == {'run_id': 'run', 'error_type': 'SkillManageRejected'}
    assert 'private body' not in rows[0]['detail']
