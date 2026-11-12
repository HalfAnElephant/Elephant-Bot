import asyncio
import json
from dataclasses import asdict

import pytest

from qqbot.adapters.hermes_skills import TOOL_SCHEMAS, AgentLearningState, HermesSkills
from qqbot.adapters.hermes_skills._vendor import skill_ledger, skill_usage

CONTENT = '---\nname: sample\ndescription: Use when testing a reusable workflow.\n---\nRead the source.\nVerify the result.\n'


def create(adapter, name='sample'):
    return adapter.skill_manage({'operations': [{'action': 'create', 'name': name,
                                                'content': CONTENT.replace('name: sample', 'name: ' + name)}]})


def test_native_schema_and_immediate_file_reuse(tmp_path):
    assert set(TOOL_SCHEMAS) == {'skills_list', 'skill_view', 'skill_manage'}
    assert TOOL_SCHEMAS['skill_manage']['parameters']['required'] == ['operations']
    adapter = HermesSkills(tmp_path / 'owner')
    assert create(adapter)['success']
    assert (adapter.root / 'skills/sample/SKILL.md').read_text() == CONTENT
    fresh = HermesSkills(adapter.root)
    assert fresh.skills_list()['skills'][0]['name'] == 'sample'
    assert fresh.skill_view({'name': 'sample'})['content'] == CONTENT
    assert all('content' not in row for row in fresh.skills_list()['skills'])


def test_owner_scope_including_cache_and_concurrent_calls(tmp_path):
    first, second = (HermesSkills(tmp_path / name) for name in ('one', 'two'))
    assert create(first)['success']
    assert first.skills_list()['count'] == 1
    assert second.skills_list()['skills'] == []
    assert not second.skill_view({'name': 'sample'})['success']
    async def run():
        return await asyncio.gather(asyncio.to_thread(create, first, 'first'),
                                    asyncio.to_thread(create, second, 'second'))
    assert all(result['success'] for result in asyncio.run(run()))
    assert {row['name'] for row in first.skills_list()['skills']} == {'sample', 'first'}
    assert {row['name'] for row in second.skills_list()['skills']} == {'second'}


def test_patch_and_ledger_rollback_restore_actual_previous_files(tmp_path):
    adapter = HermesSkills(tmp_path)
    assert create(adapter)['success']
    assert adapter.skill_manage({'operations': [
        {'action': 'patch', 'name': 'sample', 'old_string': 'Read the source.',
         'new_string': 'Read and verify the source.'},
        {'action': 'write_file', 'name': 'sample', 'file_path': 'references/check.md',
         'file_content': 'Check every source.'}]})['success']
    entries = adapter.ledger()
    assert {row['action'] for row in entries} >= {'create', 'patch', 'write_file'}
    patch = next(row for row in entries if row['action'] == 'patch')
    assert patch['before'] and patch['after']
    ok, message = adapter.rollback(patch['id'])
    assert ok, message
    assert adapter.skill_view({'name': 'sample'})['content'] == CONTENT
    assert (adapter.root / 'skills/sample/references/check.md').read_text() == 'Check every source.'


def test_batch_failure_restores_all_touched_skills(tmp_path):
    adapter = HermesSkills(tmp_path)
    assert create(adapter)['success']
    result = adapter.skill_manage({'operations': [
        {'action': 'patch', 'name': 'sample', 'old_string': 'Read the source.', 'new_string': 'Changed.'},
        {'action': 'create', 'name': 'new-skill', 'content': CONTENT.replace('sample', 'new-skill')},
        {'action': 'patch', 'name': 'sample', 'old_string': 'No such unique sentence.', 'new_string': 'Stop.'}]})
    assert not result['success'] and result['failed_index'] == 2
    assert 'rolled back' in result['error']
    assert adapter.skill_view({'name': 'sample'})['content'] == CONTENT
    assert not (adapter.root / 'skills/new-skill').exists()


@pytest.mark.parametrize('path', ['../outside', '/tmp/outside', 'references/../../outside', 'scripts/../outside'])
def test_write_path_boundaries(tmp_path, path):
    adapter = HermesSkills(tmp_path)
    assert create(adapter)['success']
    result = adapter.skill_manage({'operations': [{'action': 'write_file', 'name': 'sample',
                                                 'file_path': path, 'file_content': 'Forbidden.'}]})
    assert not result['success']


def test_poisoned_tree_and_untrusted_identity_fields_fail_closed(tmp_path):
    adapter = HermesSkills(tmp_path / 'owner')
    assert create(adapter)['success']
    with pytest.raises(ValueError, match='unexpected'):
        adapter.skills_list({'owner_id': 'another'})
    (adapter.root / 'skills/sample/references').symlink_to(tmp_path)
    with pytest.raises(PermissionError, match='links'):
        adapter.skill_view({'name': 'sample'})


def test_review_provenance_read_before_write_and_no_delete(tmp_path):
    adapter = HermesSkills(tmp_path)
    assert create(adapter)['success']
    patch = {'operations': [{'action': 'patch', 'name': 'sample', 'old_string': 'Read the source.',
                             'new_string': 'Read current source.'}]}
    with adapter.review_context():
        refused = adapter.skill_manage(patch)
        assert not refused['success'] and 'loaded' in refused['error']
        assert adapter.skill_view({'name': 'sample'})['success']
        assert adapter.skill_manage(patch)['success']
        assert not adapter.skill_manage({'operations': [{'action': 'delete', 'name': 'sample'}]})['success']
        assert create(adapter, 'review-created')['success']
    with adapter.context():
        assert skill_usage.get_record('sample')['created_by'] == 'learn'
        assert skill_usage.get_record('review-created')['created_by'] == 'agent'
    assert adapter.ledger(skill='review-created')[0]['actor'] == 'curator'
    assert adapter.skill_manage({'operations': [{'action': 'delete', 'name': 'sample'}]})['success']


def test_review_read_marks_can_be_carried_between_dispatch_tasks(tmp_path):
    adapter = HermesSkills(tmp_path)
    assert create(adapter)['success']
    with adapter.review_context() as marks:
        assert adapter.skill_view({'name': 'sample'})['success']
    with adapter.review_context(marks):
        assert adapter.skill_manage({'operations': [{'action': 'patch', 'name': 'sample',
            'old_string': 'Read the source.', 'new_string': 'Read first.'}]})['success']


def test_unreadable_rollback_safety_capture_refuses_without_mutation(tmp_path, monkeypatch):
    adapter = HermesSkills(tmp_path)
    assert create(adapter)['success']
    entry = adapter.ledger()[0]
    def fail(*args, **kwargs):
        raise OSError('disk full')
    monkeypatch.setattr(skill_ledger, '_store_blob', fail)
    ok, message = adapter.rollback(entry['id'])
    assert not ok and 'safety' in message.lower()
    assert adapter.skill_view({'name': 'sample'})['content'] == CONTENT


def test_agent_local_learning_trigger_persistence_and_recursion_guard():
    leader, worker = AgentLearningState(), AgentLearningState()
    for _ in range(10):
        worker.note_iteration()
    restored = AgentLearningState(**json.loads(json.dumps(asdict(worker))))
    assert leader.finish_turn(final_response='done') is None
    request = restored.finish_turn(final_response='done')
    assert request and request['allowed_tools'] == ['skill_manage', 'skill_view', 'skills_list']
    assert request['write_origin'] == 'background_review'
    for _ in range(10):
        restored.note_iteration()
    assert restored.finish_turn(final_response='done', is_review=True) is None
    restored.finish_review()
    for _ in range(10):
        worker.note_iteration()
    worker.note_tool('skill_manage')
    assert worker.finish_turn(final_response='done') is None
    for _ in range(10):
        worker.note_iteration()
    assert worker.finish_turn(final_response='done', interrupted=True) is None


def test_same_owner_concurrent_patches_share_one_lock(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    adapter = HermesSkills(tmp_path)
    assert create(adapter)['success']
    barrier = Barrier(2)
    def patch(old, new):
        barrier.wait()
        return HermesSkills(tmp_path).skill_manage({'operations': [
            {'action': 'patch', 'name': 'sample', 'old_string': old, 'new_string': new}]})
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(patch, 'Read the source.', 'Read current source.')
        second = pool.submit(patch, 'Verify the result.', 'Verify every result.')
        assert first.result()['success'] and second.result()['success']
    content = adapter.skill_view({'name': 'sample'})['content']
    assert 'Read current source.' in content and 'Verify every result.' in content


def test_config_explains_candidate_migration_and_rejects_inline_shell():
    from pydantic import ValidationError

    from qqbot.skills.config import SkillsConfig
    with pytest.raises(ValidationError, match='retired'):
        SkillsConfig(max_per_scope=10)
    with pytest.raises(ValidationError):
        SkillsConfig(hermes={'inline_shell': True})


def test_user_content_lock_is_preserved_for_normal_and_review_agents(tmp_path):
    adapter = HermesSkills(tmp_path)
    assert create(adapter)['success']
    with adapter.context():
        assert skill_usage.set_locked('sample', True)
    patch = {'operations': [{'action': 'patch', 'name': 'sample',
                             'old_string': 'Read the source.', 'new_string': 'Changed.'}]}
    assert not adapter.skill_manage(patch)['success']
    with adapter.review_context():
        adapter.skill_view({'name': 'sample'})
        assert not adapter.skill_manage(patch)['success']
    assert adapter.skill_view({'name': 'sample'})['content'] == CONTENT


async def test_capability_contract_uses_native_schema_and_host_identity():
    from qqbot.capabilities.gateway import CapabilityRegistry
    from qqbot.capabilities.skills import register_skills
    calls = []
    async def host_call(operation, body):
        calls.append((operation, body))
        return {'success': True}
    registry = CapabilityRegistry()
    register_skills(registry, host_call)
    assert set(registry.entries) == set(TOOL_SCHEMAS)
    native = {'operations': [{'action': 'delete', 'name': 'obsolete'}]}
    assert (await registry.entries['skill_manage'][1](native, None))['success']
    assert calls == [('skills', {'operation': 'skill_manage', 'arguments': native})]
