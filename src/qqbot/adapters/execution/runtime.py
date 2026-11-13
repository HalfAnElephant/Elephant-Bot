"""Host-owned Job authorization, late readback and recovery over the shared task repository."""
import contextvars
import hashlib
import json
from datetime import datetime
from pathlib import Path

from sqlalchemy import func, select, update

from qqbot.contracts.execution import JobScope, JobStart
from qqbot.domain import InboundEvent
from qqbot.domain.tasks import TaskResult
from qqbot.storage.action_schema import operations
from qqbot.storage.job_schema import jobs
from qqbot.storage.task_schema import inputs, runs, tasks, waits

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
            context_resolver=lambda scope: scope,
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
                    or run['role'] not in {'worker', 'leader'} or not run['task_id']):
                raise PermissionError('Job operation belongs to another Worker attempt')
            task = conn.execute(select(tasks).where(tasks.c.id == run['task_id'])).mappings().one()
            root = conn.execute(select(tasks).where(tasks.c.id == (task['root_task_id'] or task['id']))).mappings().one()
            if run['role'] == 'leader':
                run, claim = self.core.leader.lease(conn, *identity)
                if (not claim.get('takeover') or claim.get('target_root_task_id') != root['id']
                        or task['id'] != root['id'] or claim['purpose'] not in {'attention', 'finalize'}):
                    raise PermissionError('Leader Job execution requires its bound task takeover')
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
            authorization = {'action': action, 'resources': resources, 'effect': 'write'}
            if run['role'] == 'leader':
                with self.db.engine.connect() as conn:
                    current, claim = self.core.leader.lease(conn, *identity)
                    self.core.leader.authorize(conn, current, claim, authorization)
            else:
                self.task_service.authorize(*identity, authorization)
        else:
            # Readback remains available after the initiating run is cancelled or finishes.
            for resource in resources:
                self.task_service.permissions.require(access, action, resource)
        return JobScope(owner_id=owner, task_id=task['id'], root_task_id=root['id'], workspace=str(workspace),
                        expires_at=min(datetime.fromisoformat(task['deadline']), datetime.fromisoformat(root['deadline']))
                            if self.task_service._time_limited(task) and self.task_service._time_limited(root) else None)

    def _guard_dispatch(self, scope):
        identity = _call_identity.get()
        if identity is None or self._authorized(identity, 'start') != scope:
            raise PermissionError('Job dispatch authority changed')

    def _leader_inspection_scope(self, identity, job_id):
        with self.db.engine.connect() as conn:
            run, claim = self.core.leader.lease(conn, *identity)
            if run['role'] != 'leader':
                raise PermissionError('Job inspection requires the current Leader')
            event = InboundEvent.model_validate_json(claim['event_json'])
            owner = self.core.sessions.owner(event)
            job = self.jobs._row(job_id)
            task = conn.execute(select(tasks).where(tasks.c.id == job['task_id'])).mappings().first()
            root = conn.execute(select(tasks).where(tasks.c.id == job['root_task_id'])).mappings().first()
            if (not owner or not task or not root or job['owner_id'] != owner
                    or task['owner_id'] != owner or root['owner_id'] != owner
                    or (task['root_task_id'] or task['id']) != root['id']
                    or any(row['conversation'] != event.conversation.key or row['requester'] != event.principal.key
                           for row in (task, root))):
                raise PermissionError('Job inspection requires the same owner, requester and conversation')
            source = InboundEvent.model_validate_json(task['event_json'])
            if (self.core.sessions.owner(source) != owner or source.principal != event.principal
                    or source.conversation != event.conversation):
                raise PermissionError('Job source identity does not match the current Leader')
            bound_root = claim.get('target_root_task_id')
            if ((bound_root and bound_root != root['id']) or
                    (not bound_root and claim['purpose'] != 'user')):
                raise PermissionError('Leader Job inspection is bound to another root')
            workspace = self.workspace(task, strict=False)
            access = self.core.sessions.access_for_claim(claim)
            self.task_service.permissions.require(access, 'task.read', 'task:' + task['id'])
            self.core.leader.authorize(conn, run, claim, {'action': 'workspace.job.inspect',
                'resources': ['workspace:' + str(workspace), 'task:' + task['id']], 'effect': 'read'})
        return JobScope(owner_id=owner, task_id=task['id'], root_task_id=root['id'], workspace=str(workspace),
                        expires_at=min(datetime.fromisoformat(task['deadline']), datetime.fromisoformat(root['deadline']))
                            if self.task_service._time_limited(task) and self.task_service._time_limited(root) else None)

    async def invoke(self, identity, payload):
        if not isinstance(payload, dict) or set(payload) != {'operation', 'arguments'}:
            raise ValueError('Job operation and arguments are required')
        operation, arguments = payload['operation'], payload['arguments']
        if operation == 'inspect':
            with self.db.engine.connect() as conn:
                role = conn.execute(select(runs.c.role).where(runs.c.id == identity[0])).scalar_one_or_none()
            if role == 'leader':
                if not isinstance(arguments, dict):
                    raise ValueError('Invalid Job inspection arguments')
                scope = self._leader_inspection_scope(identity, arguments.get('job_id'))
                result = await self.jobs.inspect(arguments, scope)
                if self._leader_inspection_scope(identity, arguments.get('job_id')) != scope:
                    raise PermissionError('Leader Job inspection authority changed')
                return result
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
            result = await getattr(self.jobs, operation)(arguments, scope)
            if operation == 'start':
                try:
                    if self._authorized(identity, 'start') != scope:
                        raise PermissionError('Job dispatch authority changed')
                except PermissionError:
                    now = self.core.clock.now().isoformat()
                    with self.db.transaction() as conn:
                        conn.execute(update(jobs).where(jobs.c.id == result['job_id'],
                            jobs.c.state.not_in(_JOB_TERMINAL)).values(state='CANCEL_REQUESTED',
                                cancel_requested_at=func.coalesce(jobs.c.cancel_requested_at, now), updated_at=now))
                    result = self.jobs._handle(self.jobs._row(result['job_id'])).model_dump(mode='json')
            return result
        finally:
            _call_identity.reset(token)

    async def inspect_waiter(self, payload):
        if (not isinstance(payload, dict) or set(payload) != {'wait_id', 'revision'}
                or not isinstance(payload['wait_id'], str) or type(payload['revision']) is not int):
            raise ValueError('Job waiter requires its stored identity and revision')
        with self.db.engine.connect() as conn:
            wait = conn.execute(select(waits).where(waits.c.id == payload['wait_id'],
                waits.c.revision == payload['revision'], waits.c.kind == 'external',
                waits.c.state == 'WAITING')).mappings().first()
            run = conn.execute(select(runs).where(runs.c.id == wait['run_id'])).mappings().first() if wait else None
        if (not run or run['task_id'] != wait['task_id'] or run['role'] != 'worker'
                or run['state'] != 'YIELDED' or run['slot_reserved']):
            raise PermissionError('Job waiter requires its original yielded Worker')
        condition = json.loads(wait['condition_json'])
        if condition.get('capability') != 'job.inspect':
            raise PermissionError('Job waiter only inspects its stored Job condition')
        identity = (run['id'], run['fencing'], run['worker'])
        scope = self._authorized(identity, 'inspect')
        authorization = {**payload, 'action': 'workspace.job.inspect',
            'resources': ['workspace:' + scope.workspace, 'task:' + scope.task_id]}
        self.task_service.authorize_waiter(authorization)
        if scope.expires_at is not None and scope.expires_at <= self.core.clock.now():
            raise PermissionError('Job waiter scope expired')
        result = await self.invoke(identity, {'operation': 'inspect', 'arguments': condition['arguments']})
        self.task_service.authorize_waiter(authorization)
        with self.db.engine.connect() as conn:
            current = conn.execute(select(waits).where(waits.c.id == wait['id'])).mappings().first()
        if (current != wait or self._authorized(identity, 'inspect') != scope
                or scope.expires_at is not None and scope.expires_at <= self.core.clock.now()):
            raise PermissionError('Job waiter authority changed during observation')
        return result

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
                previous = TaskResult.model_validate_json(task['result_json']) if task['result_json'] else TaskResult(
                    status='failed', agent_authored=False, summary='任务进程已确认停止。')
                result = previous.model_copy(update={'status': 'failed', 'agent_authored': False,
                                                     'summary': '任务进程已确认停止。'})
                self.task_service._record_host_terminal(conn, task, 'CANCELLED', result,
                    'job-cancel-result:' + task['id'])
                graph = getattr(self.task_service, 'graph', None)
                if graph:
                    graph.event(conn, task, 'finished', {'state': 'CANCELLED', 'public_summary': '任务进程已确认停止。'},
                        event_id='job-cancel-settled:' + task['id'])

    def _requires_stop(self, task):
        if not task:
            return True
        time_stop = task['stop_reason'] in {'deadline', 'active_budget'}
        if task['state'] == 'CANCEL_REQUESTED' and time_stop and not self.task_service._time_limited(task):
            return False
        return (task['state'] in _STOP_TASK or task['stop_reason'] in {'user_cancel', 'revoked'}
                or time_stop and self.task_service._time_limited(task))

    async def maintain(self):
        with self.db.engine.connect() as conn:
            rows = conn.execute(select(jobs.c.id, jobs.c.task_id, jobs.c.root_task_id,
                jobs.c.owner_id, jobs.c.workspace, jobs.c.state).where(jobs.c.state.not_in(_JOB_TERMINAL))).mappings().all()
            task_rows = {row['id']: dict(row) for row in conn.execute(select(tasks).where(tasks.c.id.in_(
                {item['task_id'] for item in rows} | {item['root_task_id'] for item in rows}))).mappings()}
            revised_tasks = set(conn.execute(select(inputs.c.task_id).where(
                inputs.c.task_id.in_({item['task_id'] for item in rows}),
                inputs.c.kind.in_({'followup', 'answer'}), inputs.c.consumed_at.is_(None))).scalars())
        cancelled = set()
        for row in rows:
            task, root = task_rows.get(row['task_id']), task_rows.get(row['root_task_id'])
            stop = self._requires_stop(task) or self._requires_stop(root)
            if not stop:
                event = InboundEvent.model_validate_json(task['event_json'])
                stop = self.core.sessions.owner(event) != row['owner_id'] or not self.core.allowed(event)
                if not stop:
                    access = self.task_service._access_for(task)
                    stop = not self.task_service.permissions.check(access, 'workspace.job.start',
                                                                  'workspace:' + row['workspace']).allowed
            stop = stop or row['task_id'] in revised_tasks
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
                        attention = (handle.state in {'FAILED', 'UNKNOWN', 'WAITING_RECONCILIATION'}
                                     or handle.state == 'CANCELLED' and handle.deadline <= self.core.clock.now())
                        graph.event(conn, task, 'needs_attention' if attention else 'job_updated',
                            {'job_id': handle.job_id, 'state': handle.state,
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
