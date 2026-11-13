import asyncio
import json
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError
from sqlalchemy import select

from qqbot.adapters.execution.jobs import JobService, SystemdJobProvider, UnitReceipt
from qqbot.contracts.execution import ExecutionCapabilities, JobScope, JobStart
from qqbot.storage.job_schema import jobs
from qqbot.storage.task_schema import inputs, runs, tasks, waits


class FakeSystemd:
    def __init__(self):
        self.units = {}
        self.launches = []
        self.cancels = []
        self.launch_hook = None
        self.cancel_unknown = False

    async def describe(self):
        return ExecutionCapabilities(persistent_jobs=True, daemon_restart_survival=True, process_tree_cancel=True)

    async def launch(self, **request):
        if self.launch_hook:
            await self.launch_hook(request)
        self.launches.append(request)
        unit = request['unit']
        self.units[unit] = UnitReceipt(True, unit, request['description'], '1' * 32,
            '/user.slice/' + unit, 1234, '100000', 'active', 'running', 'success', None,
            cgroup_empty=False)

    async def inspect(self, unit):
        return self.units.get(unit, UnitReceipt(False, unit))

    async def cancel(self, unit, *, cgroup):
        assert cgroup == self.units[unit].cgroup
        self.cancels.append(unit)
        if self.cancel_unknown:
            raise TimeoutError('stop receipt lost')
        self.units[unit] = replace(self.units[unit], active_state='inactive', sub_state='dead',
                                   cgroup_empty=True, pid=0)
        return {'unit': unit, 'stopped': True}

    async def logs(self, unit, *, invocation_id):
        assert invocation_id == self.units[unit].invocation_id
        return 'Observed test output.'


@pytest.fixture
def setup(db, clock, tmp_path):
    workspace = tmp_path / 'workspace'
    workspace.mkdir()
    scope = JobScope(owner_id='owner', task_id='task', root_task_id='root',
                     workspace=str(workspace), expires_at=clock.now() + timedelta(hours=2))
    provider = FakeSystemd()
    service = JobService(db, provider, clock=clock, context_resolver=lambda context: scope)
    return service, provider, scope


def request(**updates):
    return JobStart(job_key='build', argv=('/usr/bin/true',), **updates)


async def test_job_survives_service_recreation_without_attempt_or_respawn(setup):
    service, provider, scope = setup
    handle = await service.start_job(request(), scope)
    assert handle.state == 'RUNNING' and handle.pid == 1234 and handle.invocation_id
    assert handle.cgroup and handle.daemon_restart_survival
    fresh = JobService(service.db, provider, clock=service.clock)
    recovered = await fresh.recover()
    assert recovered[0].job_id == handle.job_id and recovered[0].state == 'RUNNING'
    assert len(provider.launches) == 1
    provider.units[handle.unit] = replace(provider.units[handle.unit], pid=0, active_state='active',
        sub_state='exited', cgroup_empty=True, exit_code=0)
    finished = await fresh.inspect_job(handle.job_id, scope)
    assert finished.state == 'EXITED' and finished.exit_code == 0
    assert (await service.start_job(request(), scope)).job_id == handle.job_id
    assert len(provider.launches) == 1


async def test_dispatch_intent_commits_before_systemd_and_key_is_immutable(setup):
    service, provider, scope = setup
    async def intent_must_exist(arguments):
        with service.db.engine.connect() as conn:
            row = conn.execute(select(jobs)).mappings().one()
        assert row['state'] == 'DISPATCHING' and row['unit'] == arguments['unit']
    provider.launch_hook = intent_must_exist
    handle = await service.start_job(request(), scope)
    assert (await service.start_job(request(), scope)).job_id == handle.job_id
    with pytest.raises(ValueError, match='different arguments'):
        await service.start_job(JobStart(job_key='build', argv=('/usr/bin/false',)), scope)
    assert len(provider.launches) == 1


async def test_crash_before_dispatch_does_not_replay_ambiguous_intent(setup):
    service, provider, scope = setup
    async def crash(arguments):
        raise asyncio.CancelledError
    provider.launch_hook = crash
    with pytest.raises(asyncio.CancelledError):
        await service.start_job(request(), scope)
    fresh = JobService(service.db, provider, clock=service.clock)
    recovered = await fresh.recover()
    assert len(recovered) == 1 and recovered[0].state == 'UNKNOWN'
    assert recovered[0].error == 'unit_missing_receipt_unknown'
    assert not provider.launches
    handle = await fresh.start_job(request(), scope)
    assert handle.job_id == recovered[0].job_id and not provider.launches


async def test_dispatch_receipt_lost_is_recovered_by_unit_not_relaunched(setup):
    service, provider, scope = setup
    original = provider.launch
    async def lost(**arguments):
        await original(**arguments)
        raise TimeoutError('reply lost')
    provider.launch = lost
    handle = await service.start_job(request(), scope)
    assert handle.state == 'RUNNING'
    fresh = JobService(service.db, provider, clock=service.clock)
    assert (await fresh.recover())[0].job_id == handle.job_id
    assert len(provider.launches) == 1


@pytest.mark.parametrize('field,value', [('owner_id', 'someone-else'), ('task_id', 'other-task'),
                                         ('root_task_id', 'other-root')])
async def test_scope_enforced_on_inspection_cancellation_and_logs(setup, field, value):
    service, provider, scope = setup
    handle = await service.start_job(request(), scope)
    other = scope.model_copy(update={field: value})
    for method in (service.inspect_job, service.cancel_job):
        with pytest.raises(PermissionError):
            await method(handle.job_id, other)
    service.context_resolver = lambda context: other
    with pytest.raises(PermissionError):
        await service.inspect({'job_id': handle.job_id, 'include_logs': True}, None)
    assert not provider.cancels


async def test_process_identity_mismatch_cannot_kill_reused_unit_or_pid(setup):
    service, provider, scope = setup
    handle = await service.start_job(request(), scope)
    provider.units[handle.unit] = replace(provider.units[handle.unit], invocation_id='2' * 32)
    result = await service.cancel_job(handle.job_id, scope)
    assert result.state == 'CANCEL_REQUESTED' and result.error == 'unit_identity_mismatch'
    assert not provider.cancels
    fresh = JobService(service.db, provider, clock=service.clock)
    assert (await fresh.recover())[0].state == 'CANCEL_REQUESTED'
    assert not provider.cancels


async def test_cancel_root_only_stops_owned_tree_and_pending_cancel_recovers(setup):
    service, provider, scope = setup
    first = await service.start_job(request(), scope)
    unrelated_scope = scope.model_copy(update={'task_id': 'unrelated', 'root_task_id': 'unrelated-root'})
    second = await service.start_job(request(), unrelated_scope)
    provider.cancel_unknown = True
    results = await service.cancel_root('root', 'owner')
    assert len(results) == 1 and results[0].state == 'CANCEL_REQUESTED'
    provider.cancel_unknown = False
    fresh = JobService(service.db, provider, clock=service.clock)
    recovered = await fresh.recover()
    assert {item.job_id: item.state for item in recovered} == {first.job_id: 'CANCELLED', second.job_id: 'RUNNING'}
    assert provider.cancels == [first.unit, first.unit]
    assert await fresh.cancel_root('unrelated-root', 'someone-else') == []


async def test_deadline_is_bounded_by_task_and_recovery_stops_expired_jobs(setup):
    service, provider, scope = setup
    scope = scope.model_copy(update={'expires_at': service.clock.now() + timedelta(seconds=10)})
    handle = await service.start_job(request(timeout_seconds=1000), scope)
    assert provider.launches[0]['deadline_seconds'] == 10
    assert handle.deadline == scope.expires_at
    service.clock.advance(11)
    assert (await service.recover())[0].state == 'CANCELLED'
    with pytest.raises(PermissionError, match='expired'):
        await service.start_job(JobStart(job_key='new', argv=('true',)), scope)


async def test_failed_main_with_live_cgroup_is_not_reported_finished(setup):
    service, provider, scope = setup
    handle = await service.start_job(request(), scope)
    provider.units[handle.unit] = replace(provider.units[handle.unit], active_state='failed',
        sub_state='failed', pid=0, exit_code=1, result='exit-code', cgroup_empty=False)
    current = await service.inspect_job(handle.job_id, scope)
    assert current.state == 'WAITING_RECONCILIATION' and current.error == 'unit_termination_unconfirmed'
    assert (await service.cancel_job(handle.job_id, scope)).state == 'CANCELLED'


async def test_unknown_job_keeps_its_identity_without_blocking_independent_jobs(setup):
    service, provider, scope = setup
    handle = await service.start_job(request(), scope)
    del provider.units[handle.unit]
    assert (await service.inspect_job(handle.job_id, scope)).state == 'UNKNOWN'
    second = await service.start_job(JobStart(job_key='second', argv=('true',)), scope)
    assert second.job_id != handle.job_id and second.state == 'RUNNING'
    assert (await service.inspect_job(handle.job_id, scope)).state == 'UNKNOWN'
    assert len(provider.launches) == 2


@pytest.mark.parametrize('updates', [{'cwd': '../escape'}, {'cwd': '/tmp'}, {'env': {'HOME': '/owner'}},
                                     {'env': {'LD_PRELOAD': 'evil'}}, {'env': {'TZ': 'Asia/Shanghai\ninvalid'}}])
async def test_workspace_and_environment_boundaries(setup, updates):
    service, provider, scope = setup
    with pytest.raises((ValueError, ValidationError)):
        await service.start_job(request(**updates), scope)
    assert not provider.launches


async def test_same_execution_world_cwd_and_file_boundary(setup, tmp_path):
    service, provider, scope = setup
    workspace = Path(scope.workspace)
    (workspace / 'sub').mkdir()
    (workspace / 'outside').symlink_to(tmp_path.parent)
    with pytest.raises(ValueError, match='escapes'):
        await service.start_job(request(cwd='outside'), scope)
    handle = await service.start_job(request(cwd='sub', env={'TZ': 'Asia/Shanghai'}), scope)
    launched = provider.launches[0]
    assert launched['workspace'] == workspace and launched['request'].cwd == 'sub'
    other = tmp_path / 'different-world'
    other.mkdir()
    with pytest.raises(PermissionError):
        await service.inspect_job(handle.job_id, scope.model_copy(update={'workspace': str(other)}))


async def test_tools_register_native_schema_and_do_not_bind_to_run_lifecycle(setup):
    from qqbot.capabilities.gateway import CapabilityRegistry
    service, provider, scope = setup
    registry = CapabilityRegistry()
    service.register(registry)
    assert set(registry.entries) == {'job.start', 'job.inspect', 'job.cancel'}
    context = SimpleNamespace(task_id=scope.task_id, workspace=scope.workspace, run_id='old-run')
    started = await registry.entries['job.start'][1]({'job_key': 'build', 'argv': ['true']}, context)
    context.run_id = 'new-run'
    result = await registry.entries['job.inspect'][1]({'job_id': started['job_id'], 'include_logs': True}, context)
    assert result['state'] == 'RUNNING' and result['logs'] == 'Observed test output.'
    assert len(provider.launches) == 1


async def test_real_provider_declares_capability_only_with_user_manager():
    async def unavailable(argv):
        return 1, 'Failed to connect to bus'
    provider = SystemdJobProvider(command=unavailable, platform='linux', available=lambda name: '/usr/bin/' + name)
    assert not (await provider.describe()).daemon_restart_survival
    provider.platform = 'darwin'
    assert not (await provider.describe()).persistent_jobs


async def test_real_provider_builds_controlled_unit_and_bwrap_command(tmp_path):
    commands = []
    async def capture(argv):
        commands.append(argv)
        return 0, ''
    provider = SystemdJobProvider(command=capture, platform='linux')
    unit = 'elephant-job-' + 'a' * 32 + '.service'
    await provider.launch(unit=unit, description='host-bound description', request=request(cwd='sub'),
                          workspace=tmp_path, deadline_seconds=60)
    command = commands[0]
    assert command[:2] == ('systemd-run', '--user')
    assert '--property=ExitType=cgroup' in command and '--property=KillMode=control-group' in command
    assert '--property=RuntimeMaxSec=60' in command and '--clearenv' in command
    assert '--die-with-parent' not in command
    bind = command.index('--bind')
    assert command[bind + 1:bind + 3] == (str(tmp_path), '/workspace')
    assert command[command.index('--chdir') + 1] == '/workspace/sub'
    assert command[-2:] == ('--', '/usr/bin/true')
    assert not any('QQBOT_' in item or 'TOKEN=' in item for item in command)


async def test_real_provider_parses_exit_and_filters_journal_by_invocation():
    unit = 'elephant-job-' + 'b' * 32 + '.service'
    commands = []
    async def command(argv):
        commands.append(argv)
        if argv[0] == 'journalctl':
            return 0, 'final output'
        return 0, '\n'.join(['Id=' + unit, 'Description=known', 'LoadState=loaded',
            'ActiveState=active', 'SubState=exited', 'MainPID=0', 'ControlGroup=',
            'InvocationID=' + 'c' * 32, 'ExecMainStatus=0', 'ExecMainCode=1', 'Result=success', 'Transient=yes'])
    provider = SystemdJobProvider(command=command, platform='linux')
    receipt = await provider.inspect(unit)
    assert receipt.stopped and receipt.exit_code == 0 and receipt.invocation_id == 'c' * 32
    assert await provider.logs(unit, invocation_id=receipt.invocation_id) == 'final output'
    assert '_SYSTEMD_INVOCATION_ID=' + 'c' * 32 in commands[-1]


async def test_capability_handshake_checks_systemd_version_and_cgroup_v2(tmp_path):
    version = 249
    async def command(argv):
        return (0, 'systemd ' + str(version)) if '--version' in argv else (0, 'running')
    provider = SystemdJobProvider(command=command, platform='linux', available=lambda name: name,
                                  cgroup_root=tmp_path)
    assert not (await provider.describe()).daemon_restart_survival
    (tmp_path / 'cgroup.controllers').write_text('cpu memory pids')
    assert not (await provider.describe()).daemon_restart_survival
    version = 255
    assert (await provider.describe()).daemon_restart_survival


async def test_successful_stop_cli_cannot_mask_remaining_descendants(tmp_path):
    (tmp_path / 'cgroup.controllers').write_text('cpu')
    group = tmp_path / 'job-cgroup'
    group.mkdir()
    (group / 'cgroup.events').write_text('populated 1\nfrozen 0\n')
    async def command(argv):
        return 0, ''
    provider = SystemdJobProvider(command=command, cgroup_root=tmp_path)
    with pytest.raises(RuntimeError, match='cgroup is empty'):
        await provider.cancel('elephant-job-' + 'a' * 32 + '.service', cgroup='/job-cgroup')
    (group / 'cgroup.events').write_text('populated 0\nfrozen 0\n')
    assert (await provider.cancel('elephant-job-' + 'a' * 32 + '.service', cgroup='/job-cgroup'))['stopped']


@pytest.fixture
def job_runtime(db, clock, tmp_path, monkeypatch):
    from qqbot.adapters.execution.runtime import JobRuntime
    from qqbot.adapters.execution.world import execution_workspace
    from qqbot.config import OwnerConfig, QQOwnerBinding, Settings
    from qqbot.domain import AccessContext, ConversationRef, InboundEvent, PrincipalRef, Surface
    from qqbot.harness.gateway import RunGateway
    from qqbot.jobs.service import TaskService
    from qqbot.outbox.service import Outbox
    from qqbot.runtime.leader import LeaderRuntime
    from qqbot.runtime.scheduler import RuntimeClient
    from qqbot.runtime.sessions import SessionService
    from qqbot.runtime.task_graph import TaskGraph
    monkeypatch.delenv('ICLOUD_CALENDAR_CONFIG', raising=False)
    settings = Settings(owner=OwnerConfig(id='owner', qq_bindings=(
        QQOwnerBinding(app_id='app', user_openid='person', verified_at=clock.now().isoformat()),)))
    service = TaskService(db, clock, Outbox(db, clock))
    service.graph = TaskGraph(service, settings)
    sessions = SessionService(service, settings)
    core = SimpleNamespace(tasks=service, db=db, clock=clock, settings=settings, sessions=sessions,
                           allowed=lambda event: True)
    provider = FakeSystemd()
    runtime = JobRuntime(core, tmp_path, provider=provider)
    core.job_runtime = runtime
    event = InboundEvent(event_id='job-origin', event_type='message.created',
        conversation=ConversationRef(deployment_id='d', adapter_instance_id='official', bot_identity_id='app',
                                     surface=Surface.PRIVATE, remote_conversation_id='person'),
        principal=PrincipalRef(adapter_instance_id='official', identity_namespace='qq_app:app', remote_user_id='person'),
        platform_message_id='message', occurred_at=clock.now(), received_at=clock.now(), trace_id='trace')
    access = AccessContext(principal=event.principal, conversation=event.conversation, role='owner')
    with db.transaction() as conn:
        dialogue = sessions.dialogue(conn, event)
        root = service.graph.submit(conn, dialogue, event, access, {
            'goal': 'Run a supervised build.', 'children': [{'mode': 'work', 'goal': 'Build in workspace.',
                                                          'delegation_key': 'build'}]},
            'dispatch-build', sessions.binding(), {})
    claim = service.claim('daemon')
    workspace = execution_workspace(tmp_path, claim['task_id'], None, {}, create=True)
    core.leader = LeaderRuntime(core, sessions)
    client = RuntimeClient(core, core.leader)
    async def call(operation, payload):
        response = await client.post('/internal/tasks/' + operation, json={
            'worker_id': 'daemon', 'run_id': claim['run_id'], 'fencing_token': claim['fencing_token'], 'payload': payload})
        response.raise_for_status()
        return response.json()
    gateway = RunGateway(workspace, 'model', 'fixture-key', 'https://model.invalid/v1/chat/completions', call=call)
    gateway.bind(claim)
    return SimpleNamespace(runtime=runtime, provider=provider, scope=runtime._authorized(
        (claim['run_id'], claim['fencing_token'], 'daemon'), 'inspect'), service=service, core=core,
        client=client, gateway=gateway, claim=claim, root=root, event=event, access=access,
        identity=(claim['run_id'], claim['fencing_token'], 'daemon'), workspace=workspace)


async def test_runtime_gateway_starts_verifies_and_reads_independent_job(job_runtime):
    from qqbot.storage.action_schema import operations
    fixture = job_runtime
    result = await fixture.gateway.tools.invoke('job.start', {'job_key': 'build', 'argv': ['true']},
                                                fixture.gateway.context, operation_id='job-start')
    assert result.status == 'verified' and result.data['verified_effect'] == 'supervised_job_dispatch'
    assert result.data['state'] == 'RUNNING' and not result.data['execution_succeeded']
    with fixture.service.db.engine.connect() as conn:
        operation = conn.execute(select(operations).where(operations.c.id == 'job-start')).mappings().one()
    assert operation['state'] == 'VERIFIED'
    data = await fixture.runtime.invoke(fixture.identity, {'operation': 'inspect',
        'arguments': {'job_id': result.data['job_id'], 'include_logs': True}})
    assert data['logs'] == 'Observed test output.'
    assert fixture.provider.launches[0]['workspace'] == fixture.workspace
    assert fixture.gateway.registry.search('后台进程')[0]['name'].startswith('job.')


async def test_runtime_cancelled_run_can_readback_and_host_stops_job_without_lease(job_runtime):
    from sqlalchemy import update
    fixture = job_runtime
    started = await fixture.runtime.invoke(fixture.identity, {'operation': 'start',
        'arguments': {'job_key': 'build', 'argv': ['true']}})
    with fixture.service.db.transaction() as conn:
        root = conn.execute(select(tasks).where(tasks.c.id == fixture.root['task_id'])).mappings().one()
        fixture.service.graph.cancel_tree(conn, root)
        conn.execute(update(runs).where(runs.c.id == fixture.claim['run_id']).values(slot_reserved=0))
    inspected = await fixture.runtime.invoke(fixture.identity, {'operation': 'inspect',
        'arguments': {'job_id': started['job_id']}})
    assert inspected['state'] == 'RUNNING'
    with pytest.raises(PermissionError):
        await fixture.runtime.invoke(fixture.identity, {'operation': 'start',
            'arguments': {'job_key': 'forbidden', 'argv': ['true']}})
    await fixture.runtime.maintain()
    stopped = await fixture.runtime.invoke(fixture.identity, {'operation': 'inspect',
        'arguments': {'job_id': started['job_id']}})
    assert stopped['state'] == 'CANCELLED' and fixture.provider.cancels == [started['unit']]
    assert len(fixture.provider.launches) == 1


async def test_runtime_late_start_receipt_settles_original_operation_after_slot_release(job_runtime):
    from sqlalchemy import update

    from qqbot.storage.action_schema import operations
    fixture = job_runtime
    result = await fixture.gateway.tools.invoke('job.start', {'job_key': 'build', 'argv': ['true']},
                                                fixture.gateway.context, operation_id='lost-start')
    with fixture.service.db.transaction() as conn:
        conn.execute(update(operations).where(operations.c.id == 'lost-start').values(state='UNKNOWN', result_json='{}'))
        conn.execute(update(runs).where(runs.c.id == fixture.claim['run_id']).values(slot_reserved=0))
    await fixture.runtime.maintain()
    with fixture.service.db.engine.connect() as conn:
        operation = conn.execute(select(operations).where(operations.c.id == 'lost-start')).mappings().one()
    assert operation['state'] == 'VERIFIED'
    assert json.loads(operation['result_json'])['job_id'] == result.data['job_id']
    assert len(fixture.provider.launches) == 1


async def test_runtime_late_readback_survives_workspace_deletion(job_runtime):
    import shutil
    fixture = job_runtime
    started = await fixture.runtime.invoke(fixture.identity, {'operation': 'start',
        'arguments': {'job_key': 'build', 'argv': ['true']}})
    shutil.rmtree(fixture.workspace)
    assert (await fixture.runtime.invoke(fixture.identity, {'operation': 'inspect',
        'arguments': {'job_id': started['job_id']}}))['state'] == 'RUNNING'
    with fixture.service.db.transaction() as conn:
        conn.execute(tasks.update().where(tasks.c.id == fixture.claim['task_id']).values(state='CANCELLED'))
    await fixture.runtime.maintain()
    assert fixture.provider.cancels == [started['unit']]


async def test_runtime_prepare_rechecks_authority_after_provider_await(job_runtime):
    from sqlalchemy import update
    fixture = job_runtime
    original = fixture.provider.describe
    async def revoke_during_handshake():
        with fixture.service.db.transaction() as conn:
            conn.execute(update(tasks).where(tasks.c.id == fixture.claim['task_id']).values(state='CANCEL_REQUESTED'))
        return await original()
    fixture.provider.describe = revoke_during_handshake
    with pytest.raises(PermissionError):
        await fixture.runtime.invoke(fixture.identity, {'operation': 'start',
            'arguments': {'job_key': 'build', 'argv': ['true']}})
    assert not fixture.provider.launches
    with fixture.service.db.engine.connect() as conn:
        assert not conn.execute(select(jobs)).first()


async def test_runtime_consult_and_review_cannot_start_business_job(job_runtime):
    from sqlalchemy import update
    fixture = job_runtime
    with fixture.service.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.id == fixture.claim['task_id']).values(kind='consult'))
    with pytest.raises(PermissionError, match='read-only'):
        await fixture.runtime.invoke(fixture.identity, {'operation': 'start',
            'arguments': {'job_key': 'build', 'argv': ['true']}})
    with fixture.service.db.transaction() as conn:
        conn.execute(update(runs).where(runs.c.id == fixture.claim['run_id']).values(role='skill_review'))
    with pytest.raises(PermissionError, match='Worker'):
        await fixture.runtime.invoke(fixture.identity, {'operation': 'inspect', 'arguments': {'job_id': '0' * 32}})
    assert not fixture.provider.launches


async def test_runtime_revision_and_permission_revocation_stop_existing_job(job_runtime):
    from sqlalchemy import insert

    from qqbot.permissions import PermissionPolicy, PermissionRule
    fixture = job_runtime
    started = await fixture.runtime.invoke(fixture.identity, {'operation': 'start',
        'arguments': {'job_key': 'build', 'argv': ['true']}})
    with fixture.service.db.transaction() as conn:
        conn.execute(insert(inputs).values(id='revision', task_id=fixture.claim['task_id'], kind='followup',
            source_id='new-user-input', text='Stop modifying files.', state='PENDING',
            created_at=fixture.core.clock.now().isoformat()))
    await fixture.runtime.maintain()
    assert fixture.provider.cancels == [started['unit']]
    assert fixture.runtime.jobs._row(started['job_id'])['state'] == 'CANCELLED'
    with fixture.service.db.transaction() as conn:
        conn.execute(inputs.update().where(inputs.c.id == 'revision').values(consumed_at=fixture.core.clock.now().isoformat()))
    next_job = await fixture.runtime.invoke(fixture.identity, {'operation': 'start',
        'arguments': {'job_key': 'next', 'argv': ['true']}})
    fixture.service.permissions.initial_policy = PermissionPolicy(rules=(PermissionRule(
        id='deny-job-write', effect='deny', actions=('workspace.job.start',)),))
    await fixture.runtime.maintain()
    assert next_job['unit'] in fixture.provider.cancels


@pytest.fixture
async def sibling_jobs(job_runtime):
    from qqbot.adapters.execution.world import execution_workspace
    fixture = job_runtime
    with fixture.service.db.transaction() as conn:
        root = conn.execute(select(tasks).where(tasks.c.id == fixture.root['task_id'])).mappings().one()
        sibling = fixture.service.graph.spawn(conn, root, {'mode': 'work', 'goal': 'Independent background task.',
            'delegation_key': 'sibling-job'}, fixture.access)
    second = fixture.service.claim('daemon')
    assert second['task_id'] == sibling['task_id']
    execution_workspace(fixture.runtime.data_root, second['task_id'], None, {}, create=True)
    claims, started = [fixture.claim, second], []
    for index, claim in enumerate(claims):
        identity = (claim['run_id'], claim['fencing_token'], 'daemon')
        job = await fixture.runtime.invoke(identity, {'operation': 'start',
            'arguments': {'job_key': 'sibling-' + str(index), 'argv': ['true']}})
        started.append(job)
        question = {'question_id': 'sibling-wait-' + str(index), 'kind': 'input' if index == 0 else 'external',
            'text': '请提供新配置。' if index == 0 else '等待原有进程。', 'checkpoint': '保留原有 Job 身份。'}
        if index == 1:
            question['condition'] = {'capability': 'job.inspect', 'arguments': {'job_id': job['job_id']},
                'path': 'state', 'equals': 'EXITED', 'interval_seconds': 15}
        fixture.service.ask(*identity, question)
        fixture.service.submit(*identity, {'status': 'waiting_input' if index == 0 else 'waiting_external',
            'summary': '等待输入或后台进程。'}, receipt={'kind': 'run_yielded', 'session_id': claim['context']['session_id'],
                'run_id': claim['run_id'], 'session_revision': 'sibling-yield-' + str(index),
                'input_ids': claim['context']['source_input_ids'], 'inflight': 0, 'tools_paired': True})
    return fixture, claims, started


@pytest.mark.parametrize('kind', ['answer', 'followup'])
@pytest.mark.parametrize('consume_before_maintenance', [False, True])
async def test_job_revision_stop_intent_preserves_unaffected_sibling_and_survives_input_consumption(
    sibling_jobs, kind, consume_before_maintenance
):
    from qqbot.domain import TextSegment
    fixture, claims, started = sibling_jobs
    with fixture.service.db.engine.connect() as conn:
        second_wait = dict(conn.execute(select(waits).where(waits.c.run_id == claims[1]['run_id'])).mappings().one())
        second_inputs = [dict(row) for row in conn.execute(select(inputs).where(inputs.c.task_id == claims[1]['task_id'])).mappings()]
        target = conn.execute(select(tasks).where(tasks.c.id == (
            claims[0]['task_id'] if kind == 'answer' else fixture.root['task_id']))).mappings().one()
    event = fixture.event.model_copy(update={'event_id': 'sibling-revision-' + kind,
        'segments': (TextSegment(text='使用更新后的配置完成任务。'),)})
    fixture.service.add_input(target['public_id'], event, fixture.access, event.text,
        question_id='sibling-wait-0' if kind == 'answer' else None)
    assert fixture.runtime.jobs._row(started[0]['job_id'])['cancel_requested_at']
    if consume_before_maintenance:
        with fixture.service.db.transaction() as conn:
            conn.execute(inputs.update().where(inputs.c.source_id.like(event.event_id + '%')).values(
                consumed_at=fixture.core.clock.now().isoformat(), state='CONSUMED'))
    await fixture.runtime.maintain()
    assert fixture.runtime.jobs._row(started[0]['job_id'])['state'] == 'CANCELLED'
    if kind == 'answer':
        assert fixture.runtime.jobs._row(started[1]['job_id'])['state'] == 'RUNNING'
        assert fixture.provider.cancels == [started[0]['unit']]
        with fixture.service.db.engine.connect() as conn:
            assert dict(conn.execute(select(waits).where(waits.c.id == second_wait['id'])).mappings().one()) == second_wait
            assert [dict(row) for row in conn.execute(select(inputs).where(inputs.c.task_id == claims[1]['task_id'])).mappings()] == second_inputs
            assert conn.execute(select(tasks.c.state).where(tasks.c.id == claims[1]['task_id'])).scalar_one() == 'WAITING_EXTERNAL'
    else:
        assert fixture.runtime.jobs._row(started[1]['job_id'])['state'] == 'CANCELLED'
        assert set(fixture.provider.cancels) == {job['unit'] for job in started}
    assert len(fixture.provider.launches) == 2


async def test_job_dispatched_after_revision_transaction_is_marked_for_stop_without_replay(job_runtime):
    from qqbot.domain import TextSegment
    fixture = job_runtime
    guard = fixture.runtime.jobs.dispatch_guard
    def revise_after_authorization(scope):
        guard(scope)
        event = fixture.event.model_copy(update={'event_id': 'revision-before-job-intent',
            'segments': (TextSegment(text='停止旧操作，改用更新后的配置。'),)})
        fixture.service.add_input(fixture.root['public_id'], event, fixture.access, event.text)
        with fixture.service.db.engine.connect() as conn:
            assert not conn.execute(select(jobs)).first()
    fixture.runtime.jobs.dispatch_guard = revise_after_authorization
    result = await fixture.runtime.invoke(fixture.identity, {'operation': 'start',
        'arguments': {'job_key': 'inflight-revision', 'argv': ['true']}})
    assert result['state'] == 'CANCEL_REQUESTED'
    assert result['invocation_id'] == '1' * 32 and len(fixture.provider.launches) == 1
    with fixture.service.db.transaction() as conn:
        assert conn.execute(select(jobs.c.cancel_requested_at)).scalar_one()
        conn.execute(inputs.update().where(inputs.c.kind == 'followup').values(
            consumed_at=fixture.core.clock.now().isoformat(), state='CONSUMED'))
    await fixture.runtime.maintain()
    assert fixture.runtime.jobs._row(result['job_id'])['state'] == 'CANCELLED'
    assert fixture.provider.cancels == [result['unit']] and len(fixture.provider.launches) == 1


async def test_runtime_claim_keeps_shared_workspace_locked_by_live_job(job_runtime, tmp_path):
    from sqlalchemy import insert, update
    fixture = job_runtime
    shared = tmp_path / 'shared'
    shared.mkdir()
    binding = fixture.core.sessions.binding()
    binding['workspace_bindings'] = {'shared': str(shared)}
    with fixture.service.db.transaction() as conn:
        task = dict(conn.execute(select(tasks).where(tasks.c.id == fixture.claim['task_id'])).mappings().one())
        plan = json.loads(task['plan_json'])
        plan['workspace_ref'] = 'shared'
        conn.execute(update(tasks).where(tasks.c.id == task['id']).values(plan_json=json.dumps(plan), binding_json=json.dumps(binding)))
    started = await fixture.runtime.invoke(fixture.identity, {'operation': 'start',
        'arguments': {'job_key': 'build', 'argv': ['true']}})
    with fixture.service.db.transaction() as conn:
        conn.execute(update(runs).where(runs.c.id == fixture.claim['run_id']).values(slot_reserved=0))
        second = {**task, 'id': 'second-task', 'public_id': 'T-second', 'source': 'second-origin',
                  'state': 'QUEUED', 'plan_json': json.dumps(plan), 'binding_json': json.dumps(binding),
                  'session_id': 'second-session', 'delegation_key': 'second-build'}
        conn.execute(insert(tasks).values(**second))
    assert fixture.service.claim('other-worker') is None
    await fixture.runtime.jobs.cancel_task(task['id'], 'owner')
    assert fixture.service.claim('other-worker')['task_id'] == 'second-task'
    assert fixture.provider.cancels == [started['unit']]


async def test_runtime_tasks_list_native_bridge_accepts_list_result(job_runtime):
    import httpx
    fixture = job_runtime
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=fixture.gateway.app), base_url='http://gateway') as client:
        response = await client.post('/tools/tasks_list', headers={'Authorization': 'Bearer ' + fixture.gateway.tool_token},
                                     json={'call_id': 'list-tasks', 'arguments': {}})
    assert response.status_code == 200, response.text
    assert isinstance(response.json(), list) and len(response.json()) == 2


async def test_runtime_cancel_waits_for_job_stop_proof_after_worker_finished(job_runtime):
    fixture = job_runtime
    started = await fixture.runtime.invoke(fixture.identity, {'operation': 'start',
        'arguments': {'job_key': 'build', 'argv': ['true']}})
    fixture.provider.cancel_unknown = True
    with fixture.service.db.transaction() as conn:
        root = conn.execute(select(tasks).where(tasks.c.id == fixture.root['task_id'])).mappings().one()
        fixture.service.graph.cancel_tree(conn, root)
    result = fixture.service.submit(*fixture.identity, {'status': 'failed', 'agent_authored': False,
                                                       'summary': 'Worker stopped.'}, stopped=True)
    assert result['state'] == 'CANCEL_REQUESTED'
    await fixture.runtime.maintain()
    with fixture.service.db.engine.connect() as conn:
        states = dict(conn.execute(select(tasks.c.id, tasks.c.state)).all())
    assert states[fixture.root['task_id']] == states[fixture.claim['task_id']] == 'CANCEL_REQUESTED'
    fixture.provider.cancel_unknown = False
    await fixture.runtime.maintain()
    with fixture.service.db.engine.connect() as conn:
        states = dict(conn.execute(select(tasks.c.id, tasks.c.state)).all())
    assert states[fixture.root['task_id']] == states[fixture.claim['task_id']] == 'CANCELLED'
    assert fixture.runtime.jobs._row(started['job_id'])['state'] == 'CANCELLED'


async def test_runtime_wrong_worker_fence_cannot_use_late_job_readback(job_runtime):
    fixture = job_runtime
    started = await fixture.runtime.invoke(fixture.identity, {'operation': 'start',
        'arguments': {'job_key': 'build', 'argv': ['true']}})
    run_id, fence, worker = fixture.identity
    for identity in [(run_id, fence + 1, worker), (run_id, fence, 'another-worker')]:
        with pytest.raises(PermissionError):
            await fixture.runtime.invoke(identity, {'operation': 'inspect', 'arguments': {'job_id': started['job_id']}})
    assert not fixture.provider.cancels


@pytest.fixture
async def job_waiter(job_runtime):
    fixture = job_runtime
    started = await fixture.runtime.invoke(fixture.identity, {'operation': 'start',
        'arguments': {'job_key': 'waited-job', 'argv': ['true']}})
    fixture.service.ask(*fixture.identity, {'question_id': 'wait-job', 'kind': 'external',
        'text': '等待原有后台进程。', 'checkpoint': '保持原有 Job 身份。',
        'condition': {'capability': 'job.inspect', 'arguments': {'job_id': started['job_id']},
            'path': 'state', 'equals': 'EXITED', 'interval_seconds': 15}})
    fixture.service.submit(*fixture.identity, {'status': 'waiting_external', 'summary': '等待后台进程。'},
        receipt={'kind': 'run_yielded', 'session_id': fixture.claim['context']['session_id'],
            'run_id': fixture.claim['run_id'], 'session_revision': 'yielded-job',
            'input_ids': fixture.claim['context']['source_input_ids'], 'inflight': 0, 'tools_paired': True})
    fixture.core.clock.advance(31)
    return fixture, started, {'wait_id': 'wait-job', 'revision': 1}


async def test_job_waiter_reads_original_job_after_paired_yield_and_expired_attempt_lease(job_waiter):
    fixture, started, payload = job_waiter
    calls = []
    authorize = fixture.service.authorize_waiter
    def observed_authorize(arguments):
        calls.append(arguments)
        return authorize(arguments)
    fixture.service.authorize_waiter = observed_authorize
    result = await fixture.runtime.inspect_waiter(payload)
    assert result['job_id'] == started['job_id'] and result['state'] == 'RUNNING'
    expected = {**payload, 'action': 'workspace.job.inspect',
        'resources': ['workspace:' + str(fixture.workspace), 'task:' + fixture.claim['task_id']]}
    assert calls == [expected, expected]
    with fixture.service.db.engine.connect() as conn:
        run = conn.execute(select(runs).where(runs.c.id == fixture.claim['run_id'])).mappings().one()
        assert run['state'] == 'YIELDED' and not run['slot_reserved']
        assert run['lease'] < fixture.core.clock.now().isoformat()
    assert len(fixture.provider.launches) == 1 and not fixture.provider.cancels


@pytest.mark.parametrize('change', ['cancelled', 'cancelled_root', 'context', 'policy', 'foreign_job', 'unyielded'])
async def test_job_waiter_rejects_revoked_or_unrelated_observations_before_provider_read(job_waiter, change):
    fixture, started, payload = job_waiter
    with fixture.service.db.transaction() as conn:
        if change == 'cancelled':
            conn.execute(tasks.update().where(tasks.c.id == fixture.claim['task_id']).values(state='CANCELLED'))
        elif change == 'cancelled_root':
            conn.execute(tasks.update().where(tasks.c.id == fixture.root['task_id']).values(state='CANCEL_REQUESTED'))
        elif change == 'context':
            fixture.service.context_validator = lambda access, context: False
        elif change == 'policy':
            def revoked_policy(access, action, resource):
                raise PermissionError('Job read policy revoked')
            fixture.service.permissions.require = revoked_policy
        elif change == 'foreign_job':
            conn.execute(jobs.update().where(jobs.c.id == started['job_id']).values(owner_id='another-owner'))
        else:
            conn.execute(runs.update().where(runs.c.id == fixture.claim['run_id']).values(state='RUNNING'))
    inspected = []
    async def unexpected_inspect(unit):
        inspected.append(unit)
        return fixture.provider.units[unit]
    fixture.provider.inspect = unexpected_inspect
    with pytest.raises(PermissionError):
        await fixture.runtime.inspect_waiter(payload)
    assert not inspected and len(fixture.provider.launches) == 1 and not fixture.provider.cancels


@pytest.mark.parametrize('change', ['cancelled', 'context', 'policy', 'condition'])
async def test_job_waiter_discards_read_if_authority_changes_during_provider_await(job_waiter, change):
    fixture, _started, payload = job_waiter
    inspect = fixture.provider.inspect
    async def changed_during_read(unit):
        with fixture.service.db.transaction() as conn:
            if change == 'cancelled':
                conn.execute(tasks.update().where(tasks.c.id == fixture.claim['task_id']).values(state='CANCELLED'))
            elif change == 'context':
                fixture.service.context_validator = lambda access, context: False
            elif change == 'policy':
                def revoked_policy(access, action, resource):
                    raise PermissionError('Job read policy revoked')
                fixture.service.permissions.require = revoked_policy
            else:
                condition = {'capability': 'job.inspect', 'arguments': {'job_id': 'replacement'},
                    'path': 'state', 'equals': 'EXITED'}
                conn.execute(waits.update().where(waits.c.id == payload['wait_id']).values(condition_json=json.dumps(condition)))
        return await inspect(unit)
    fixture.provider.inspect = changed_during_read
    with pytest.raises(PermissionError):
        await fixture.runtime.inspect_waiter(payload)
    assert len(fixture.provider.launches) == 1 and not fixture.provider.cancels


async def test_job_waiter_only_accepts_stored_wait_identity_and_job_inspection_condition(job_waiter):
    fixture, _started, payload = job_waiter
    with pytest.raises(ValueError):
        await fixture.runtime.inspect_waiter({**payload, 'arguments': {'job_id': 'foreign-job'}})
    with pytest.raises(PermissionError):
        await fixture.runtime.inspect_waiter({**payload, 'revision': 2})
    with fixture.service.db.transaction() as conn:
        conn.execute(waits.update().where(waits.c.id == payload['wait_id']).values(
            condition_json=json.dumps({'capability': 'job.cancel', 'arguments': {'job_id': 'foreign-job'}})))
    with pytest.raises(PermissionError):
        await fixture.runtime.inspect_waiter(payload)
    assert len(fixture.provider.launches) == 1 and not fixture.provider.cancels


async def test_scheduler_recovers_jobs_before_claiming_and_shutdown_leaves_units_alone(job_runtime, monkeypatch):
    from qqbot.runtime.scheduler import Scheduler
    fixture = job_runtime
    scheduler = Scheduler(fixture.core, SimpleNamespace(sessions=fixture.core.sessions))
    recovery_gate = asyncio.Event()
    claims = []
    async def maintain():
        await recovery_gate.wait()
        return []
    pending = [{'run_id': 'claimed-after-recovery'}]
    async def call(*args, **kwargs):
        assert args == ('claim',)
        return pending.pop(0) if pending else None
    async def once(claim):
        assert claim['run_id'] == 'claimed-after-recovery'
        claims.append('claim')
        return False
    async def sweep():
        return None
    monkeypatch.setattr(fixture.core.job_runtime, 'maintain', maintain)
    monkeypatch.setattr(scheduler.worker, 'call', call)
    monkeypatch.setattr(scheduler.worker, 'once', once)
    monkeypatch.setattr(scheduler.pool, 'sweep', sweep)
    await scheduler.step()
    await asyncio.sleep(0)
    assert claims == []
    recovery_gate.set()
    await scheduler.job_maintenance
    await scheduler.step()
    await asyncio.sleep(0)
    assert claims == ['claim']
    await scheduler.close()
    assert fixture.provider.cancels == []


def _finish_job_worker(fixture, claim=None):
    claim = claim or fixture.claim
    fixture.service.submit(claim['run_id'], claim['fencing_token'], 'daemon',
        {'status': 'completed', 'summary': '后台 Job 已交回句柄。'}, stopped=True,
        receipt={'kind': 'turn_finished', 'session_id': claim['context']['session_id'], 'run_id': claim['run_id'],
                 'session_revision': 'job-worker-finished', 'input_ids': claim['context']['source_input_ids'],
                 'inflight': 0, 'tools_paired': True})


def _job_leader(fixture, *, user=False):
    _finish_job_worker(fixture)
    if user:
        from qqbot.domain import TextSegment
        event = fixture.event.model_copy(update={'event_id': 'explicit-job-read', 'segments': (TextSegment(text='查看指定 Job'),)})
        fixture.core.sessions.enqueue(event, fixture.access)
    claim = fixture.core.sessions.claim('leader-reader')
    assert claim['purpose'] == ('user' if user else 'finalize')
    return claim, (claim['run_id'], claim['fencing_token'], 'leader-reader')


async def test_leader_finalizer_inspects_actual_child_job_and_logs_without_worker_scope(job_runtime):
    from qqbot.jobs.api import WorkerRequest
    fixture = job_runtime
    started = await fixture.runtime.invoke(fixture.identity, {'operation': 'start',
        'arguments': {'job_key': 'leader-read', 'argv': ['true']}})
    claim, identity = _job_leader(fixture)
    result = await fixture.client.invoke('jobs', WorkerRequest(
        run_id=identity[0], fencing_token=identity[1], worker_id=identity[2],
        payload={'operation': 'inspect', 'arguments': {'job_id': started['job_id'], 'include_logs': True}}))
    assert claim['target_root_task_id'] == result['root_task_id'] == fixture.root['task_id']
    assert result['task_id'] == fixture.claim['task_id'] and result['state'] == 'RUNNING'
    assert result['logs'] == 'Observed test output.' and len(fixture.provider.launches) == 1
    assert not fixture.provider.cancels


async def test_unbound_user_leader_can_read_explicit_job_only_in_own_conversation(job_runtime):
    fixture = job_runtime
    started = await fixture.runtime.invoke(fixture.identity, {'operation': 'start',
        'arguments': {'job_key': 'user-read', 'argv': ['true']}})
    claim, identity = _job_leader(fixture, user=True)
    assert claim['target_root_task_id'] is None
    result = await fixture.runtime.invoke(identity, {'operation': 'inspect', 'arguments': {'job_id': started['job_id']}})
    assert result['task_id'] == fixture.claim['task_id']
    foreign = json.loads(claim['event_json'])
    foreign['conversation']['remote_conversation_id'] = 'another-conversation'
    claim['event_json'] = json.dumps(foreign)
    with fixture.service.db.transaction() as conn:
        conn.execute(runs.update().where(runs.c.id == identity[0]).values(request_json=json.dumps(claim)))
    with pytest.raises(PermissionError, match='conversation'):
        await fixture.runtime.invoke(identity, {'operation': 'inspect', 'arguments': {'job_id': started['job_id']}})


@pytest.mark.parametrize('operation', ['start', 'prepare', 'lookup', 'cancel'])
async def test_leader_job_inspection_does_not_enable_other_job_operations(job_runtime, operation):
    fixture = job_runtime
    _, identity = _job_leader(fixture)
    with pytest.raises(PermissionError):
        await fixture.runtime.invoke(identity, {'operation': operation, 'arguments': {}})
    assert not fixture.provider.launches and not fixture.provider.cancels


@pytest.mark.parametrize('mutation', ['job_owner', 'task_owner', 'task_conversation', 'task_requester', 'root_owner'])
async def test_leader_job_read_rejects_foreign_scope_before_returning_logs(job_runtime, mutation):
    fixture = job_runtime
    started = await fixture.runtime.invoke(fixture.identity, {'operation': 'start',
        'arguments': {'job_key': 'scope-read', 'argv': ['true']}})
    _, identity = _job_leader(fixture)
    with fixture.service.db.transaction() as conn:
        if mutation == 'job_owner':
            conn.execute(jobs.update().where(jobs.c.id == started['job_id']).values(owner_id='foreign-owner'))
        else:
            column = {'task_owner': 'owner_id', 'task_conversation': 'conversation',
                      'task_requester': 'requester', 'root_owner': 'owner_id'}[mutation]
            task_id = fixture.root['task_id'] if mutation == 'root_owner' else fixture.claim['task_id']
            conn.execute(tasks.update().where(tasks.c.id == task_id).values(**{column: 'foreign-scope'}))
    with pytest.raises(PermissionError, match='same owner'):
        await fixture.runtime.invoke(identity, {'operation': 'inspect',
            'arguments': {'job_id': started['job_id'], 'include_logs': True}})
    assert not fixture.provider.cancels


async def test_bound_leader_cannot_inspect_another_actual_root(job_runtime):
    fixture = job_runtime
    started = await fixture.runtime.invoke(fixture.identity, {'operation': 'start',
        'arguments': {'job_key': 'first-root', 'argv': ['true']}})
    _finish_job_worker(fixture)
    first_leader = fixture.core.sessions.claim('first-leader')
    assert first_leader['target_root_task_id'] == fixture.root['task_id']
    source = fixture.event.model_copy(update={'event_id': 'second-root-source'})
    with fixture.service.db.transaction() as conn:
        dialogue = fixture.core.sessions.dialogue(conn, source, new_title='另一个根目标')
        other = fixture.service.graph.submit(conn, dialogue, source, fixture.access,
            {'goal': 'Second independent goal', 'children': [{'goal': 'Second work', 'delegation_key': 'second'}]},
            'second-root', fixture.core.sessions.binding(), {})
    second_worker = fixture.service.claim('daemon')
    _finish_job_worker(fixture, second_worker)
    second_leader = fixture.core.sessions.claim('second-leader')
    assert second_leader['target_root_task_id'] == other['task_id']
    with pytest.raises(PermissionError, match='another root'):
        await fixture.runtime.invoke((second_leader['run_id'], second_leader['fencing_token'], 'second-leader'),
            {'operation': 'inspect', 'arguments': {'job_id': started['job_id'], 'include_logs': True}})


@pytest.mark.parametrize('action', ['task.read', 'workspace.job.inspect'])
async def test_leader_job_read_requires_current_actual_child_permissions(job_runtime, action):
    from qqbot.permissions import PermissionRule
    fixture = job_runtime
    started = await fixture.runtime.invoke(fixture.identity, {'operation': 'start',
        'arguments': {'job_key': 'permission-read', 'argv': ['true']}})
    _, identity = _job_leader(fixture)
    fixture.service.permissions.upsert(PermissionRule(id='deny-job-read', effect='deny', actions=(action,),
        resources=('task:' + fixture.claim['task_id'],)), expected_version=0, actor='test')
    with pytest.raises(PermissionError):
        await fixture.runtime.invoke(identity, {'operation': 'inspect', 'arguments': {'job_id': started['job_id']}})


async def test_leader_job_read_rechecks_authority_after_asynchronous_provider_read(job_runtime):
    from qqbot.permissions import PermissionRule
    fixture = job_runtime
    started = await fixture.runtime.invoke(fixture.identity, {'operation': 'start',
        'arguments': {'job_key': 'late-read-revoke', 'argv': ['true']}})
    _, identity = _job_leader(fixture)
    inspect = fixture.provider.inspect
    async def revoke(unit):
        fixture.service.permissions.upsert(PermissionRule(id='revoke-job-read', effect='deny',
            actions=('workspace.job.inspect',), resources=('workspace:' + str(fixture.workspace),)),
            expected_version=0, actor='test')
        return await inspect(unit)
    fixture.provider.inspect = revoke
    with pytest.raises(PermissionError):
        await fixture.runtime.invoke(identity, {'operation': 'inspect', 'arguments': {'job_id': started['job_id']}})


@pytest.mark.parametrize('revocation', ['expired_lease', 'wrong_fence', 'skill_review'])
async def test_job_inspection_requires_live_leader_and_excludes_review(job_runtime, revocation):
    fixture = job_runtime
    started = await fixture.runtime.invoke(fixture.identity, {'operation': 'start',
        'arguments': {'job_key': 'leader-lease', 'argv': ['true']}})
    _, identity = _job_leader(fixture)
    with fixture.service.db.transaction() as conn:
        if revocation == 'expired_lease':
            conn.execute(runs.update().where(runs.c.id == identity[0]).values(lease=fixture.core.clock.now().isoformat()))
        elif revocation == 'skill_review':
            conn.execute(runs.update().where(runs.c.id == identity[0]).values(role='skill_review'))
        else:
            identity = (identity[0], identity[1] + 1, identity[2])
    with pytest.raises(PermissionError):
        await fixture.runtime.invoke(identity, {'operation': 'inspect', 'arguments': {'job_id': started['job_id']}})


async def test_leader_missing_job_is_bounded_runtime_error_without_mutation(job_runtime):
    fixture = job_runtime
    _, identity = _job_leader(fixture)
    response = await fixture.client.post('/internal/tasks/jobs', json={
        'worker_id': identity[2], 'run_id': identity[0], 'fencing_token': identity[1],
        'payload': {'operation': 'inspect', 'arguments': {'job_id': '0' * 32}}})
    assert response.status_code == 422 and 'unavailable' in response.json()['detail'].lower()
    assert not fixture.provider.launches and not fixture.provider.cancels
    with fixture.service.db.engine.connect() as conn:
        assert not conn.execute(select(jobs)).first()


@pytest.mark.parametrize('path', ['state', 'data.state'])
@pytest.mark.parametrize(('target', 'exit_code'), [('EXITED', 0), ('FAILED', 7)])
async def test_host_job_condition_wakes_original_task_once_without_model_polling(job_waiter, path, target, exit_code):
    from qqbot.capabilities.gateway import CapabilityRegistry
    from qqbot.jobs.waiters import ExternalWaiters
    from qqbot.storage.task_schema import model_requests
    fixture, started, payload = job_waiter
    with fixture.service.db.transaction() as conn:
        condition = json.loads(conn.execute(select(waits.c.condition_json).where(waits.c.id == payload['wait_id'])).scalar_one())
        condition.update(path=path, equals=target)
        conn.execute(waits.update().where(waits.c.id == payload['wait_id']).values(condition_json=json.dumps(condition)))
    called = []
    async def call(operation, arguments):
        called.append(operation)
        response = await fixture.client.post('/internal/tasks/' + operation, json={'worker_id': 'daemon', 'payload': arguments})
        response.raise_for_status()
        return response.json()
    observer = ExternalWaiters(call, CapabilityRegistry())
    await observer.once()
    with fixture.service.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == fixture.claim['task_id'])).scalar_one() == 'WAITING_EXTERNAL'
    unit = started['unit']
    fixture.provider.units[unit] = replace(fixture.provider.units[unit], active_state='inactive', sub_state='exited',
        result='success' if exit_code == 0 else 'exit-code', exit_code=exit_code, cgroup_empty=True, pid=0)
    observer.next_poll.clear()
    await observer.once()
    observer.next_poll.clear()
    await observer.once()
    with fixture.service.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == fixture.claim['task_id'])).scalar_one() == 'QUEUED'
        assert conn.execute(select(waits.c.state).where(waits.c.id == payload['wait_id'])).scalar_one() == 'CONSUMED'
        assert len(conn.execute(select(inputs).where(inputs.c.task_id == fixture.claim['task_id'])).all()) == 1
        assert not conn.execute(select(model_requests)).first()
        original = conn.execute(select(runs).where(runs.c.id == fixture.claim['run_id'])).mappings().one()
        assert original['state'] == 'YIELDED' and original['slot_reserved'] == 0
    assert called.count('wait-job-inspect') == 2 and called.count('wait-observed') == 1
    resumed = fixture.service.claim('resumed')
    assert resumed['task_id'] == fixture.claim['task_id'] and resumed['session_id'] == fixture.claim['session_id']
    observation = json.loads(resumed['continuation']['inputs'][0]['text'])
    assert observation['job_id'] == started['job_id'] and observation['state'] == target
    assert len(fixture.provider.launches) == 1 and not fixture.provider.cancels


async def test_revision_supersedes_job_wait_but_writes_and_finalization_wait_for_stop(job_waiter):
    from qqbot.domain import TextSegment
    fixture, started, payload = job_waiter
    with fixture.service.db.engine.connect() as conn:
        public_id = conn.execute(select(tasks.c.public_id).where(tasks.c.id == fixture.claim['task_id'])).scalar_one()
    event = fixture.event.model_copy(update={'event_id': 'read-only-job-revision', 'segments': (TextSegment(text='只检查，不修改。'),)})
    fixture.service.add_input(public_id, event, fixture.access, event.text)
    with fixture.service.db.engine.connect() as conn:
        assert conn.execute(select(waits.c.state).where(waits.c.id == payload['wait_id'])).scalar_one() == 'SUPERSEDED'
        job = conn.execute(select(jobs).where(jobs.c.id == started['job_id'])).mappings().one()
        assert job['state'] == 'CANCEL_REQUESTED' and job['cancel_requested_at']
    resumed = fixture.service.claim('revised-worker')
    assert resumed['task_id'] == fixture.claim['task_id'] and resumed['session_id'] == fixture.claim['session_id']
    assert resumed['continuation']['inputs'][0]['text'] == event.text
    identity = resumed['run_id'], resumed['fencing_token'], 'revised-worker'
    proof = {'kind': 'input_consumed', 'session_id': resumed['session_id'], 'run_id': resumed['run_id'],
        'session_revision': 'new-instruction', 'input_ids': resumed['context']['source_input_ids']}
    fixture.service.session_receipt(*identity, proof)
    write = {'action': 'workspace.file.write', 'effect': 'write', 'resources': ['workspace:' + str(fixture.workspace)]}
    with pytest.raises(PermissionError, match='proven stopped'):
        fixture.service.authorize(*identity, write)
    assert fixture.service.authorize(*identity, {**write, 'action': 'workspace.file.read', 'effect': 'read'})['allowed']
    assert fixture.service.authorize(*identity, {**write, 'action': 'workspace.job.cancel'})['allowed']
    fixture.service.submit(*identity, {'status': 'completed', 'summary': '按新指令完成只读核验。'},
        receipt={**proof, 'kind': 'turn_finished', 'inflight': 0, 'tools_paired': True})
    assert fixture.core.sessions.claim('premature-finalizer') is None
    await fixture.runtime.maintain()
    assert (await fixture.runtime.invoke(fixture.identity, {'operation': 'inspect', 'arguments': {'job_id': started['job_id']}}))['state'] == 'CANCELLED'
    finalizer = fixture.core.sessions.claim('finalizer')
    assert finalizer['purpose'] == 'finalize' and finalizer['target_root_task_id'] == fixture.root['task_id']
    assert len(fixture.provider.launches) == 1 and fixture.provider.cancels == [started['unit']]


async def test_revision_before_job_wait_yield_does_not_leave_an_obsolete_condition(job_runtime):
    from qqbot.domain import TextSegment
    fixture = job_runtime
    started = await fixture.runtime.invoke(fixture.identity, {'operation': 'start',
        'arguments': {'job_key': 'before-yield', 'argv': ['true']}})
    fixture.service.ask(*fixture.identity, {'question_id': 'early-job-wait', 'kind': 'external',
        'text': '等待原进程', 'checkpoint': '保留 Job', 'condition': {'capability': 'job.inspect',
            'arguments': {'job_id': started['job_id']}, 'path': 'state', 'equals': 'EXITED'}})
    event = fixture.event.model_copy(update={'event_id': 'before-yield-revision', 'segments': (TextSegment(text='只检查，不修改。'),)})
    fixture.service.add_input(fixture.root['public_id'], event, fixture.access, event.text)
    result = fixture.service.submit(*fixture.identity, {'status': 'waiting_external', 'summary': '旧等待已持久化'},
        receipt={'kind': 'run_yielded', 'session_id': fixture.claim['session_id'], 'run_id': fixture.claim['run_id'],
            'session_revision': 'old-wait', 'input_ids': fixture.claim['context']['source_input_ids'], 'inflight': 0, 'tools_paired': True})
    assert result['state'] == 'QUEUED' and fixture.service.external_waiters() == []
    with fixture.service.db.engine.connect() as conn:
        assert conn.execute(select(waits.c.state).where(waits.c.id == 'early-job-wait')).scalar_one() == 'SUPERSEDED'
        assert conn.execute(select(jobs.c.cancel_requested_at).where(jobs.c.id == started['job_id'])).scalar_one()
    resumed = fixture.service.claim('new-worker')
    assert resumed['task_id'] == fixture.claim['task_id'] and resumed['session_id'] == fixture.claim['session_id']
    assert any(item['text'] == event.text for item in resumed['continuation']['inputs'])


async def test_unbounded_task_scope_keeps_explicit_job_timeout(setup):
    service, provider, scope = setup
    scope = scope.model_copy(update={'expires_at': None})
    handle = await service.start_job(request(timeout_seconds=120), scope)
    assert provider.launches[0]['deadline_seconds'] == 120
    assert handle.deadline == service.clock.now() + timedelta(seconds=120)


async def test_independent_jobs_can_exceed_old_global_capacity(setup):
    service, provider, scope = setup
    handles = [await service.start_job(JobStart(job_key='job-' + str(index), argv=('true',)), scope)
               for index in range(10)]
    assert len({handle.job_id for handle in handles}) == 10 and len(provider.launches) == 10


@pytest.mark.parametrize('old_state', ['RUNNING', 'CANCEL_REQUESTED'])
async def test_background_job_start_and_readback_ignore_old_task_deadline(job_runtime, old_state):
    fixture = job_runtime
    with fixture.service.db.transaction() as conn:
        conn.execute(tasks.update().values(deadline=(fixture.core.clock.now() - timedelta(days=1)).isoformat(),
            stop_reason='active_budget'))
    started = await fixture.runtime.invoke(fixture.identity, {'operation': 'start',
        'arguments': {'job_key': 'past-task-time', 'argv': ['true'], 'timeout_seconds': 120}})
    assert fixture.provider.launches[0]['deadline_seconds'] == 120
    with fixture.service.db.transaction() as conn:
        conn.execute(tasks.update().values(state=old_state))
    await fixture.runtime.maintain()
    assert not fixture.provider.cancels
    assert (await fixture.runtime.invoke(fixture.identity, {'operation': 'inspect',
        'arguments': {'job_id': started['job_id']}}))['state'] == 'RUNNING'


async def test_job_waiter_reads_after_old_root_deadline(job_waiter):
    fixture, started, payload = job_waiter
    with fixture.service.db.transaction() as conn:
        conn.execute(tasks.update().values(deadline=(fixture.core.clock.now() - timedelta(seconds=1)).isoformat()))
    assert (await fixture.runtime.inspect_waiter(payload))['job_id'] == started['job_id']


async def test_takeover_leader_can_start_jobs_but_uncommitted_takeover_cannot(job_runtime):
    fixture = job_runtime
    worker = fixture.claim
    fixture.service.submit(*fixture.identity, {'status': 'failed', 'summary': '需要接手处理', 'agent_authored': False})
    leader = fixture.core.sessions.claim('leader')
    identity = (leader['run_id'], leader['fencing_token'], 'leader')
    arguments = {'operation': 'start', 'arguments': {'job_key': 'takeover-job', 'argv': ['true']}}
    with pytest.raises(PermissionError, match='bound task takeover'):
        await fixture.runtime.invoke(identity, arguments)
    await fixture.core.leader.invoke('tasks', identity, {'operation': 'tasks.take_over', 'call_id': 'takeover', 'arguments': {}})
    with fixture.service.db.engine.connect() as conn:
        root = conn.execute(select(tasks).where(tasks.c.id == fixture.root['task_id'])).mappings().one()
    fixture.runtime.workspace(root, strict=False).mkdir(parents=True)
    started = await fixture.runtime.invoke(identity, arguments)
    assert started['task_id'] == fixture.root['task_id']
    assert started['root_task_id'] == fixture.root['task_id']
    assert (await fixture.runtime.invoke(identity, arguments))['job_id'] == started['job_id']
    assert len(fixture.provider.launches) == 1
    assert worker['task_id'] != started['task_id']


@pytest.mark.parametrize('failure', ['failed', 'unknown', 'timeout'])
async def test_persistent_job_problem_wakes_leader_without_relaunch(job_runtime, failure):
    fixture = job_runtime
    started = await fixture.runtime.invoke(fixture.identity, {'operation': 'start',
        'arguments': {'job_key': 'decision-job', 'argv': ['true'], 'timeout_seconds': 120}})
    if failure == 'unknown':
        del fixture.provider.units[started['unit']]
    elif failure == 'failed':
        fixture.provider.units[started['unit']] = replace(fixture.provider.units[started['unit']],
            active_state='failed', sub_state='failed', cgroup_empty=True, pid=0, exit_code=1, result='exit-code')
    else:
        fixture.core.clock.advance(121)
    await fixture.runtime.maintain()
    attention = fixture.core.sessions.claim('leader')
    assert attention['purpose'] == 'attention'
    assert attention['target_root_task_id'] == fixture.root['task_id']
    assert len(fixture.provider.launches) == 1
