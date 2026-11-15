"""Persistent Job modes and direct Leader scopes survive their initiating turn."""
import json

import pytest
from sqlalchemy import select
from test_jobs import job_runtime as job_runtime  # noqa: PLC0414

from qqbot.adapters.execution.runtime import JobRuntime
from qqbot.adapters.execution.world import execution_workspace
from qqbot.contracts.execution import ExecutionCapabilities
from qqbot.domain import TextSegment
from qqbot.storage.action_schema import operations
from qqbot.storage.task_schema import runs, tasks


async def both_modes():
    return ExecutionCapabilities(persistent_jobs=True, daemon_restart_survival=True,
        process_tree_cancel=True, environments=('sandbox', 'host'))


def direct_turn(fixture, event_id='direct-job'):
    fixture.core.outbox = fixture.service.outbox
    event = fixture.event.model_copy(update={'event_id': event_id, 'segments': (TextSegment(text='运行后台任务'),)})
    fixture.core.sessions.enqueue(event, fixture.access)
    claim = fixture.core.sessions.claim('leader')
    execution_workspace(fixture.runtime.data_root, claim['task_id'], claim['plan'].get('workspace_ref'),
        claim['binding'].get('workspace_bindings', {}), create=True)
    return claim, (claim['run_id'], claim['fencing_token'], 'leader')


def finish_turn(fixture, claim):
    return fixture.core.leader.complete(claim['run_id'], claim['fencing_token'], 'leader',
        {'status': 'completed', 'summary': '后台任务已启动。'}, stopped=True, quiescent=False,
        receipt={'kind': 'turn_finished', 'session_id': claim['session_id'], 'run_id': claim['run_id'],
            'session_revision': 'finished', 'input_ids': claim['context']['source_input_ids'],
            'inflight': 0, 'tools_paired': True})


@pytest.mark.parametrize('environment', ['sandbox', 'host'])
async def test_worker_mode_and_prepare_hash_follow_persisted_plan(job_runtime, environment):
    f = job_runtime
    f.provider.describe = both_modes
    with f.service.db.transaction() as conn:
        plan = dict(f.claim['plan'], environment=environment)
        conn.execute(tasks.update().where(tasks.c.id == f.claim['task_id']).values(plan_json=json.dumps(plan)))
    arguments = {'job_key': 'mode-job', 'argv': ['true']}
    prepared = await f.runtime.invoke(f.identity, {'operation': 'prepare', 'arguments': arguments})
    started = await f.runtime.invoke(f.identity, {'operation': 'start', 'arguments': arguments})
    assert prepared['environment'] == environment and started['environment'] == environment
    assert f.provider.launches[0]['environment'] == environment
    assert f.runtime.jobs._row(started['job_id'])['arguments_hash'] == prepared['arguments_hash']


async def test_prepare_rejects_unsupported_host_mode(job_runtime):
    f = job_runtime
    async def sandbox_only():
        return ExecutionCapabilities(persistent_jobs=True, daemon_restart_survival=True)
    f.provider.describe = sandbox_only
    _, identity = direct_turn(f)
    with pytest.raises(ValueError, match='execution environment'):
        await f.runtime.invoke(identity, {'operation': 'prepare',
            'arguments': {'job_key': 'unsupported', 'argv': ['true']}})
    assert not f.provider.launches


async def test_direct_job_survives_turn_and_runtime_recreation_and_new_turn_can_cancel(job_runtime):
    f = job_runtime
    f.provider.describe = both_modes
    claim, identity = direct_turn(f)
    arguments = {'job_key': 'persistent-direct', 'argv': ['true']}
    prepared = await f.runtime.invoke(identity, {'operation': 'prepare', 'arguments': arguments})
    started = await f.runtime.invoke(identity, {'operation': 'start', 'arguments': arguments})
    assert prepared['environment'] == started['environment'] == 'host'
    assert started['task_id'] == claim['task_id'] == started['root_task_id']
    finish_turn(f, claim)
    fresh = JobRuntime(f.core, f.runtime.data_root, provider=f.provider)
    f.core.job_runtime = fresh
    await fresh.maintain()
    assert not f.provider.cancels and len(f.provider.launches) == 1
    late = await fresh.invoke(identity, {'operation': 'lookup', 'arguments': {'job_key': arguments['job_key']}})
    assert late['job_id'] == started['job_id'] and late['state'] == 'RUNNING'
    assert (await fresh.invoke(identity, {'operation': 'inspect',
        'arguments': {'job_id': started['job_id']}}))['job_id'] == started['job_id']
    with pytest.raises(PermissionError, match='stale Leader'):
        await fresh.invoke(identity, {'operation': 'start', 'arguments': arguments})
    new_claim, current = direct_turn(f, 'cancel-direct')
    assert new_claim['task_id'] != claim['task_id']
    assert (await fresh.invoke(current, {'operation': 'inspect',
        'arguments': {'job_id': started['job_id']}}))['state'] == 'RUNNING'
    assert (await fresh.invoke(current, {'operation': 'cancel',
        'arguments': {'job_id': started['job_id']}}))['state'] == 'CANCELLED'
    assert len(f.provider.launches) == 1 and f.provider.cancels == [started['unit']]


async def test_direct_job_late_receipt_settles_journal_and_owner_revocation_stops_job(job_runtime):
    f = job_runtime
    f.provider.describe = both_modes
    claim, identity = direct_turn(f)
    arguments = {'job_key': 'journal-direct', 'argv': ['true']}
    prepared = await f.runtime.invoke(identity, {'operation': 'prepare', 'arguments': arguments})
    await f.core.leader.invoke('ledger', identity, {'operation': 'prepare', 'operation_id': 'direct-job-effect',
        'arguments_hash': 'journal-request', 'arguments': {**arguments, '_preconditions': prepared},
        'action': {'name': 'workspace.job.start', 'effect': 'write', 'resources': ['workspace:' + prepared['workspace']]}})
    await f.core.leader.invoke('ledger', identity, {'operation': 'transition', 'operation_id': 'direct-job-effect',
        'expected': 'PREPARED', 'state': 'DISPATCHED', 'result': {}})
    started = await f.runtime.invoke(identity, {'operation': 'start', 'arguments': arguments})
    finish_turn(f, claim)
    f.runtime.reconcile_operations()
    with f.service.db.engine.connect() as conn:
        assert conn.execute(select(operations.c.state).where(operations.c.id == 'direct-job-effect')).scalar_one() == 'VERIFIED'
        assert conn.execute(select(runs.c.task_id).where(runs.c.id == claim['run_id'])).scalar_one() is None
    f.core.sessions.owner = lambda _: None
    await f.runtime.maintain()
    assert f.provider.cancels == [started['unit']]
    assert len(f.provider.launches) == 1
