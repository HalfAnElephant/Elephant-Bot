"""Host-owned Job authorization, late readback and recovery over the shared task repository."""
import contextvars
import hashlib
import json
from datetime import datetime
from pathlib import Path

from sqlalchemy import select, update

from qqbot.contracts.execution import JobScope, JobStart
from qqbot.domain import InboundEvent
from qqbot.storage.action_schema import operations
from qqbot.storage.job_schema import jobs
from qqbot.storage.task_schema import inputs, runs, tasks

from .jobs import JobService, SystemdJobProvider
from .world import execution_workspace

_JOB_TERMINAL = frozenset({'EXITED', 'FAILED', 'CANCELLED'})
_STOP_TASK = frozenset({'CANCEL_REQUESTED', 'CANCELLED', 'FAILED', 'BLOCKED', 'EXPIRED'})
_call_identity = contextvars.ContextVar('job_runtime_identity', default=None)


class JobRuntime:
    def __init__(self, core, data_root, *, provider=None):
        self.core, self.task_service, self.db = core, core.tasks, core.db
        self.data_root = Path(data_root)
        self.jobs = JobService(self.db, provider or SystemdJobProvider(), clock=core.clock,
            context_resolver=lambda scope: scope, max_active=core.settings.execution.max_active,
            dispatch_guard=self._guard_dispatch)
        self.task_service.job_workspace_available = self.workspace_available
        self.task_service.job_tree_quiescent = self.tree_quiescent

    def workspace(self, task, *, strict=True):
        plan, binding = json.loads(task['plan_json']), json.loads(task['binding_json'])
        binding = binding or self.core.sessions.binding()
        if binding.get('execution_world', 'local') != 'local':
            raise PermissionError('Persistent Jobs require the task local execution world')
        return execution_workspace(self.data_root, task['id'], plan.get('workspace_ref'),
                                   binding.get('workspace_bindings', {}), strict=strict)

    def _authorized(self, identity, operation):
        if operation not in {'start', 'prepare', 'inspect', 'lookup', 'cancel'}:
            raise ValueError('Unknown host Job operation')
        run_id, fence, worker = identity
        with self.db.engine.connect() as conn:
            run = conn.execute(select(runs).where(runs.c.id == run_id)).mappings().first()
            if (not run or run['worker'] != worker or run['fencing'] != fence
                    or run['role'] != 'worker' or not run['task_id']):
                raise PermissionError('Job operation belongs to another Worker attempt')
            task = conn.execute(select(tasks).where(tasks.c.id == run['task_id'])).mappings().one()
            root = conn.execute(select(tasks).where(tasks.c.id == (task['root_task_id'] or task['id']))).mappings().one()
        event = InboundEvent.model_validate_json(task['event_json'])
        owner = self.core.sessions.owner(event)
        if (not owner or task['owner_id'] != owner or root['owner_id'] != owner
                or not self.core.allowed(event)):
            raise PermissionError('Persistent Job requires the verified task owner')
        access = self.task_service._access_for(task)
        self.task_service.permissions.require(access, 'task.read', 'task:' + task['id'])
        workspace = self.workspace(task, strict=operation in {"start", "prepare"})
        action = 'workspace.job.' + ('start' if operation == 'prepare' else
                                     'inspect' if operation == 'lookup' else operation)
        resources = ('workspace:' + str(workspace), 'task:' + task['id'])
        if operation in {'start', 'prepare', 'cancel'}:
            self.task_service.authorize(*identity, {'action': action, 'resources': resources, 'effect': 'write'})
        else:
            # Readback remains available after the initiating run is cancelled or finishes.
            for resource in resources:
                self.task_service.permissions.require(access, action, resource)
        return JobScope(owner_id=owner, task_id=task['id'], root_task_id=root['id'], workspace=str(workspace),
                        expires_at=min(datetime.fromisoformat(task['deadline']), datetime.fromisoformat(root['deadline'])))

    def _guard_dispatch(self, scope):
        identity = _call_identity.get()
        if identity is None or self._authorized(identity, 'start') != scope:
            raise PermissionError('Job dispatch authority changed')

    async def invoke(self, identity, payload):
        if not isinstance(payload, dict) or set(payload) != {'operation', 'arguments'}:
            raise ValueError('Job operation and arguments are required')
        operation, arguments = payload['operation'], payload['arguments']
        scope = self._authorized(identity, operation)
        if operation == 'lookup':
            if (not isinstance(arguments, dict) or set(arguments) != {'job_key'}
                    or not isinstance(arguments['job_key'], str) or not 1 <= len(arguments['job_key']) <= 128):
                raise ValueError('Job lookup requires its original key')
            with self.db.engine.connect() as conn:
                job_id = conn.execute(select(jobs.c.id).where(jobs.c.task_id == scope.task_id,
                    jobs.c.job_key == arguments['job_key'])).scalar_one_or_none()
            return {'found': False} if not job_id else (await self.jobs.inspect_job(job_id, scope)).model_dump(mode='json')
        if operation == 'prepare':
            request = JobStart.model_validate_json(json.dumps(arguments))
            _, cwd = self.jobs._workspace(scope, request.cwd)
            request = request.model_copy(update={'cwd': cwd})
            digest = hashlib.sha256(json.dumps(request.model_dump(mode='json'), sort_keys=True,
                                               separators=(',', ':')).encode()).hexdigest()
            capabilities = await self.jobs.describe()
            if not capabilities.persistent_jobs or not capabilities.daemon_restart_survival:
                raise ValueError(capabilities.reason or 'Persistent Job provider is unavailable')
            return {'world_id': capabilities.world_id, 'job_key': request.job_key,
                    'arguments_hash': digest, 'workspace': scope.workspace,
                    'daemon_restart_survival': True}
        token = _call_identity.set(identity)
        try:
            return await getattr(self.jobs, operation)(arguments, scope)
        finally:
            _call_identity.reset(token)

    def workspace_available(self, task):
        workspace = str(self.workspace(task, strict=False))
        with self.db.engine.connect() as conn:
            occupied = conn.execute(select(jobs.c.id).where(jobs.c.workspace == workspace,
                jobs.c.task_id != task['id'], jobs.c.state.not_in(_JOB_TERMINAL))).first()
        return occupied is None

    def tree_quiescent(self, task):
        with self.db.engine.connect() as conn:
            descendants = self._descendants(conn, task['id'])
            return conn.execute(select(jobs.c.id).where(jobs.c.task_id.in_(descendants),
                jobs.c.state.not_in(_JOB_TERMINAL))).first() is None

    @staticmethod
    def _descendants(conn, task_id):
        ids, frontier = {task_id}, {task_id}
        while frontier:
            frontier = set(conn.execute(select(tasks.c.id).where(tasks.c.parent_task_id.in_(frontier))).scalars()) - ids
            ids |= frontier
        return ids

    def _finish_cancellation(self):
        with self.db.transaction() as conn:
            pending = conn.execute(select(tasks).where(tasks.c.state == 'CANCEL_REQUESTED',
                tasks.c.stop_reason.in_({'user_cancel', 'revoked'}))).mappings().all()
            for task in pending:
                descendants = self._descendants(conn, task['id'])
                active = conn.execute(select(runs.c.id).where(runs.c.task_id.in_(descendants), runs.c.slot_reserved == 1)).first()
                unsettled = conn.execute(select(jobs.c.id).where(jobs.c.task_id.in_(descendants),
                    jobs.c.state.not_in(_JOB_TERMINAL))).first()
                if active or unsettled:
                    continue
                conn.execute(update(tasks).where(tasks.c.id == task['id'], tasks.c.state == 'CANCEL_REQUESTED')
                    .values(state='CANCELLED'))
                graph = getattr(self.task_service, 'graph', None)
                if graph:
                    graph.event(conn, task, 'finished', {'state': 'CANCELLED', 'public_summary': '任务进程已确认停止。'},
                        event_id='job-cancel-settled:' + task['id'])

    async def maintain(self):
        with self.db.engine.connect() as conn:
            rows = conn.execute(select(jobs.c.id, jobs.c.task_id, jobs.c.root_task_id,
                jobs.c.owner_id, jobs.c.workspace, jobs.c.state).where(jobs.c.state.not_in(_JOB_TERMINAL))).mappings().all()
            task_rows = {row['id']: dict(row) for row in conn.execute(select(tasks).where(tasks.c.id.in_(
                {item['task_id'] for item in rows} | {item['root_task_id'] for item in rows}))).mappings()}
            revised_roots = set(conn.execute(select(tasks.c.root_task_id).join(inputs, inputs.c.task_id == tasks.c.id)
                .where(tasks.c.root_task_id.in_({item['root_task_id'] for item in rows}),
                       inputs.c.kind.in_({'followup', 'answer'}), inputs.c.consumed_at.is_(None))).scalars())
        cancelled = set()
        for row in rows:
            task, root = task_rows.get(row['task_id']), task_rows.get(row['root_task_id'])
            stop = (not task or not root or task['state'] in _STOP_TASK or root['state'] in _STOP_TASK
                    or task['stop_reason'] in {'user_cancel', 'revoked', 'deadline', 'active_budget'}
                    or root['stop_reason'] in {'user_cancel', 'revoked', 'deadline', 'active_budget'})
            if not stop:
                event = InboundEvent.model_validate_json(task['event_json'])
                stop = self.core.sessions.owner(event) != row['owner_id'] or not self.core.allowed(event)
                if not stop:
                    access = self.task_service._access_for(task)
                    stop = not self.task_service.permissions.check(access, 'workspace.job.start',
                                                                  'workspace:' + row['workspace']).allowed
            stop = stop or row['root_task_id'] in revised_roots
            key = (row['task_id'], row['owner_id'])
            if stop and key not in cancelled:
                await self.jobs.cancel_task(*key)
                cancelled.add(key)
        recovered = await self.jobs.recover()
        by_id = {item['id']: item for item in rows}
        changed = [handle for handle in recovered if handle.state != by_id[handle.job_id]['state']]
        # cancel_task may already have settled a Job, so include those terminal receipts as well.
        if cancelled:
            with self.db.engine.connect() as conn:
                settled = conn.execute(select(jobs).where(jobs.c.id.in_(by_id),
                    jobs.c.state.in_(_JOB_TERMINAL))).mappings().all()
            changed.extend(self.jobs._handle(row) for row in settled if row['state'] != by_id[row['id']]['state'])
        graph = getattr(self.task_service, 'graph', None)
        if graph:
            with self.db.transaction() as conn:
                for handle in changed:
                    task = task_rows.get(handle.task_id)
                    if task:
                        graph.event(conn, task, 'job_updated', {'job_id': handle.job_id, 'state': handle.state,
                            'exit_code': handle.exit_code, 'evidence_refs': ['job:' + handle.job_id],
                            'public_summary': '后台进程状态已更新为 ' + handle.state},
                            event_id='job:' + handle.job_id + ':' + handle.state, wake=True)
        self.reconcile_operations()
        self._finish_cancellation()
        return recovered

    def reconcile_operations(self):
        """Trusted stored unit receipts can settle a late effect after its run lost its lease."""
        with self.db.transaction() as conn:
            pending = conn.execute(select(operations, runs.c.task_id.label('source_task_id'))
                .join(runs, runs.c.id == operations.c.run_id)
                .where(operations.c.state.in_({'DISPATCHED', 'UNKNOWN'}))).mappings().all()
            for operation in pending:
                action = json.loads(operation['action_json'])
                if action.get('name') not in {'workspace.job.start', 'workspace.job.cancel'}:
                    continue
                arguments = json.loads(operation['arguments_json'])
                expected = arguments.get('_preconditions', {})
                if action['name'] == 'workspace.job.start':
                    query = select(jobs).where(jobs.c.task_id == operation['source_task_id'],
                        jobs.c.job_key == arguments.get('job_key'),
                        jobs.c.arguments_hash == expected.get('arguments_hash'),
                        jobs.c.workspace == expected.get('workspace'))
                else:
                    query = select(jobs).where(jobs.c.task_id == operation['source_task_id'],
                                               jobs.c.id == arguments.get('job_id'))
                job = conn.execute(query).mappings().first()
                if not job or not job['identity_json']:
                    continue
                identity = json.loads(job['identity_json'])
                if not identity.get('invocation_id') or job['error'] == 'unit_identity_mismatch':
                    continue
                if action['name'] == 'workspace.job.cancel' and (
                        job['state'] not in _JOB_TERMINAL
                        or expected.get('invocation_id') != identity['invocation_id']):
                    continue
                result = self.jobs._handle(job).model_dump(mode='json')
                result['verified_effect'] = ('supervised_job_dispatch' if action['name'].endswith('start') else 'job_stopped')
                result['execution_succeeded'] = job['state'] == 'EXITED' and job['exit_code'] == 0
                conn.execute(update(operations).where(operations.c.id == operation['id'],
                    operations.c.state == operation['state']).values(state='VERIFIED',
                    result_json=json.dumps(result, ensure_ascii=False), updated_at=self.core.clock.now().isoformat()))
