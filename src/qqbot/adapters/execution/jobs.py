"""Durable local Jobs supervised by systemd, never by a model run or daemon task."""
import asyncio
import hashlib
import json
import os
import re
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

from sqlalchemy import insert, select, update

from qqbot.capabilities.subprocesses import start_process, stop_process
from qqbot.capabilities.types import Action, Capability
from qqbot.clock import Clock
from qqbot.contracts.execution import ExecutionCapabilities, JobHandle, JobScope, JobStart
from qqbot.storage.job_schema import jobs

_UNIT = re.compile(r'^elephant-job-[a-f0-9]{32}\.service$')
_TERMINAL = {'EXITED', 'FAILED', 'CANCELLED'}
_PROPS = ('Id', 'Description', 'LoadState', 'ActiveState', 'SubState', 'MainPID', 'ControlGroup',
          'InvocationID', 'ExecMainStartTimestampMonotonic', 'ExecMainCode', 'ExecMainStatus', 'Result', 'Transient')


@dataclass(frozen=True)
class UnitReceipt:
    exists: bool
    unit: str
    description: str = ''
    invocation_id: str = ''
    cgroup: str = ''
    pid: int = 0
    started_at: str = ''
    active_state: str = ''
    sub_state: str = ''
    result: str = ''
    exit_code: int | None = None
    transient: bool = True
    cgroup_empty: bool | None = None

    @property
    def stopped(self):
        return (self.cgroup_empty is True
                and (self.active_state in {'inactive', 'failed'} or self.sub_state == 'exited'))


class SystemdJobProvider:
    """User-manager units survive their caller; the user manager must be kept alive with linger."""
    def __init__(self, *, command=None, platform=None, available=None, cancellation_grace_seconds=10, cgroup_root="/sys/fs/cgroup"):
        self.command = command or self._command
        self.platform = platform or sys.platform
        self.available = available or shutil.which
        self.cgroup_root = Path(cgroup_root)
        if type(cancellation_grace_seconds) is not int or not 1 <= cancellation_grace_seconds <= 30:
            raise ValueError('Job cancellation grace must be 1..30 seconds')
        self.cancellation_grace_seconds = cancellation_grace_seconds

    async def _command(self, argv):
        process = await start_process(*argv, env={
            'PATH': '/usr/local/bin:/usr/bin:/bin', 'LANG': 'C.UTF-8',
            'XDG_RUNTIME_DIR': f'/run/user/{os.getuid()}',
            'DBUS_SESSION_BUS_ADDRESS': f'unix:path=/run/user/{os.getuid()}/bus',
        }, stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT, start_new_session=True)
        output = bytearray()
        try:
            async with asyncio.timeout(20):
                while chunk := await process.stdout.read(65536):
                    output.extend(chunk)
                    if len(output) > 262144:
                        raise RuntimeError('systemd command response exceeds transport limit')
                await process.wait()
            return process.returncode, output.decode(errors='replace')
        finally:
            if process.returncode is None:
                await stop_process(process)

    async def describe(self):
        missing = [name for name in ('systemd-run', 'systemctl', 'journalctl', 'bwrap') if not self.available(name)]
        if self.platform != 'linux' or missing:
            return ExecutionCapabilities(reason='Persistent Jobs require Linux systemd user manager and bubblewrap')
        if not (self.cgroup_root / 'cgroup.controllers').is_file():
            return ExecutionCapabilities(reason='Persistent Jobs require the unified cgroup v2 hierarchy')
        try:
            version_code, version = await self.command(('systemctl', '--version'))
            parsed = re.match(r'systemd (\d+)', version)
            if version_code or not parsed or int(parsed[1]) < 250:
                return ExecutionCapabilities(reason='Persistent Jobs require systemd 250+ for cgroup exit tracking')
            code, output = await self.command(('systemctl', '--user', 'is-system-running'))
        except (OSError, RuntimeError, TimeoutError):
            return ExecutionCapabilities(reason='systemd user manager is unavailable')
        ready = code in {0, 1} and output.strip() in {'running', 'degraded'}
        return ExecutionCapabilities(persistent_jobs=ready, daemon_restart_survival=ready,
                                     process_tree_cancel=ready,
                                     reason=None if ready else 'systemd user manager is unavailable; enable linger')

    @staticmethod
    def _unit(unit):
        if not isinstance(unit, str) or not _UNIT.fullmatch(unit):
            raise ValueError('Invalid host-managed Job unit')
        return unit

    async def launch(self, *, unit, description, request, workspace, deadline_seconds):
        unit = self._unit(unit)
        command = ['systemd-run', '--user', '--quiet', '--no-ask-password', '--service-type=exec',
                   '--unit=' + unit, '--description=' + description,
                   '--property=RemainAfterExit=yes', '--property=ExitType=cgroup', '--property=KillMode=control-group',
                   '--property=TimeoutStopSec=' + str(self.cancellation_grace_seconds),
                   '--property=RuntimeMaxSec=' + str(deadline_seconds),
                   '--property=NoNewPrivileges=yes', '--property=StandardInput=null',
                   '--property=StandardOutput=journal', '--property=StandardError=inherit',
                   '--property=UMask=0077', '--',
                   'bwrap', '--unshare-all', '--new-session', '--clearenv',
                   '--setenv', 'PATH', '/usr/local/bin:/usr/bin:/bin',
                   '--setenv', 'HOME', '/tmp', '--setenv', 'LANG', 'C.UTF-8']
        for name, value in sorted(request.env.items()):
            command.extend(['--setenv', name, value])
        for directory in ('/usr', '/bin', '/lib', '/lib64'):
            if Path(directory).exists():
                command.extend(['--ro-bind', directory, directory])
        command.extend(['--proc', '/proc', '--dev', '/dev', '--tmpfs', '/tmp',
                        '--bind', str(workspace), '/workspace', '--chdir',
                        '/workspace' + ('' if request.cwd == '.' else '/' + request.cwd), '--', *request.argv])
        code, _ = await self.command(tuple(command))
        # A failed CLI receipt cannot prove the transient unit was never created.
        if code != 0:
            raise RuntimeError('systemd-run did not confirm Job dispatch; inspect the recorded unit')

    async def inspect(self, unit):
        unit = self._unit(unit)
        code, output = await self.command(('systemctl', '--user', 'show', unit, '--no-pager',
                                           '--property=' + ','.join(_PROPS)))
        values = dict(line.split('=', 1) for line in output.splitlines() if '=' in line)
        if values.get('LoadState') == 'not-found':
            return UnitReceipt(False, unit)
        if code != 0 or values.get('Id') != unit or 'ActiveState' not in values:
            raise RuntimeError('systemd unit state is unavailable')
        status = values.get('ExecMainStatus', '')
        exit_code = int(status) if status.isdigit() and values.get('ExecMainCode') == '1' else None
        # Signal termination remains a failure; encode it separately from a normal exit status.
        if status.isdigit() and values.get('ExecMainCode') in {'2', '3'}:
            exit_code = -int(status)
        return UnitReceipt(True, unit, values.get('Description', ''), values.get('InvocationID', ''),
                           values.get('ControlGroup', ''), int(values.get('MainPID') or 0),
                           values.get('ExecMainStartTimestampMonotonic', ''), values.get('ActiveState', ''),
                           values.get('SubState', ''), values.get('Result', ''), exit_code,
                           values.get('Transient') == 'yes',
                           self._cgroup_empty(values.get('ControlGroup', ''), int(values.get('MainPID') or 0)))

    def _cgroup_empty(self, cgroup, pid):
        if not cgroup:
            return pid == 0
        if not cgroup.startswith('/') or '..' in Path(cgroup).parts:
            raise RuntimeError('systemd returned an invalid Job cgroup')
        if not (self.cgroup_root / 'cgroup.controllers').is_file():
            return None
        directory = self.cgroup_root / cgroup.lstrip('/')
        path = directory / 'cgroup.events'
        try:
            return dict(line.split() for line in path.read_text().splitlines()).get('populated') == '0'
        except FileNotFoundError:
            return True if not directory.exists() else None
        except (OSError, ValueError):
            return None

    async def cancel(self, unit, *, cgroup):
        # systemctl waits for the stop job; KillMode=control-group joins all descendants.
        code, _ = await self.command(('systemctl', '--user', 'stop', self._unit(unit), '--no-ask-password'))
        if code != 0:
            raise RuntimeError('systemd could not confirm Job cancellation')
        if not cgroup or self._cgroup_empty(cgroup, 0) is not True:
            raise RuntimeError('Job stop returned without proof that its cgroup is empty')
        return {'unit': unit, 'stopped': True}

    async def logs(self, unit, *, invocation_id, lines=100):
        if not re.fullmatch(r"[a-f0-9]{32}", invocation_id or ""):
            raise RuntimeError("Job log invocation identity is unavailable")
        code, output = await self.command(('journalctl', '--user-unit=' + self._unit(unit), '--no-pager',
                                           '--output=cat', '--lines=' + str(lines), '--quiet',
                                           '_SYSTEMD_INVOCATION_ID=' + invocation_id))
        if code != 0:
            raise RuntimeError('Job journal is unavailable')
        return output[-65536:]


class JobService:
    def __init__(self, db, provider, *, context_resolver=None, clock=None, max_active=8, dispatch_guard=None):
        self.db, self.provider = db, provider
        self.context_resolver = context_resolver
        self.dispatch_guard = dispatch_guard
        if type(max_active) is not int or not 1 <= max_active <= 64:
            raise ValueError('Job capacity must be 1..64')
        self.clock, self.max_active = clock or Clock(), max_active
        self._locks = {}

    def _lock(self, job_id):
        return self._locks.setdefault(job_id, asyncio.Lock())

    async def describe(self):
        return await self.provider.describe()

    @staticmethod
    def _workspace(scope, cwd='.'):
        root = Path(scope.workspace)
        if root.is_symlink() or not root.is_dir():
            raise ValueError('Job workspace must be an existing real local directory')
        root = root.resolve(strict=True)
        relative = Path(cwd)
        if relative.is_absolute() or '..' in relative.parts or '\x00' in cwd or '\\' in cwd:
            raise ValueError('Job cwd must be a relative path inside its workspace')
        target = (root / relative).resolve(strict=True)
        if not target.is_relative_to(root) or not target.is_dir():
            raise ValueError('Job cwd escapes its workspace')
        return root, target.relative_to(root).as_posix()

    def _row(self, job_id):
        if not isinstance(job_id, str) or not re.fullmatch(r'[a-f0-9]{32}', job_id):
            raise ValueError('Invalid Job identity')
        with self.db.engine.connect() as conn:
            row = conn.execute(select(jobs).where(jobs.c.id == job_id)).mappings().first()
        if row is None:
            raise LookupError('Job is unavailable')
        return dict(row)

    def _owned(self, job_id, scope):
        row = self._row(job_id)
        root = Path(scope.workspace).resolve()
        if (row['owner_id'], row['task_id'], row['root_task_id'], row['workspace'], row['world_id']) != (
                scope.owner_id, scope.task_id, scope.root_task_id, str(root), scope.world_id):
            raise PermissionError('Job belongs to another owner, task or execution world')
        return row

    def _save(self, row, **values):
        values['updated_at'] = self.clock.now().isoformat()
        with self.db.transaction() as conn:
            conn.execute(update(jobs).where(jobs.c.id == row['id']).values(**values))
        return self._row(row['id'])

    @staticmethod
    def _handle(row):
        identity = json.loads(row['identity_json'] or '{}')
        return JobHandle(job_id=row['id'], task_id=row['task_id'], root_task_id=row['root_task_id'],
                         state=row['state'], unit=row['unit'], invocation_id=identity.get('invocation_id'),
                         cgroup=identity.get('cgroup'), pid=identity.get('pid'),
                         process_started_at=identity.get('started_at'), deadline=datetime.fromisoformat(row['deadline']),
                         exit_code=row['exit_code'], log_ref=row['log_ref'], error=row['error'],
                         daemon_restart_survival=True)

    async def start_job(self, request: JobStart, scope: JobScope):
        capabilities = await self.describe()
        if not capabilities.persistent_jobs or (request.restart_survival and not capabilities.daemon_restart_survival):
            raise RuntimeError(capabilities.reason or 'Provider does not support daemon restart survival')
        if capabilities.world_id != scope.world_id or capabilities.filesystem != 'local':
            raise RuntimeError('Job provider and workspace execution worlds do not match')
        if self.dispatch_guard is not None:
            self.dispatch_guard(scope)
        root, cwd = self._workspace(scope, request.cwd)
        request = request.model_copy(update={'cwd': cwd})
        now = self.clock.now()
        deadline = min(scope.expires_at, now + timedelta(seconds=request.timeout_seconds))
        seconds = int((deadline - now).total_seconds())
        if seconds < 1:
            raise PermissionError('Job task deadline has expired')
        arguments = json.dumps(request.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(arguments.encode()).hexdigest()
        with self.db.transaction() as conn:
            previous = conn.execute(select(jobs).where(jobs.c.task_id == scope.task_id,
                                                        jobs.c.job_key == request.job_key)).mappings().first()
            if previous is None:
                active = conn.execute(select(jobs.c.id).where(jobs.c.state.not_in(_TERMINAL))).all()
                if len(active) >= self.max_active:
                    raise ValueError('Persistent Job capacity is full')
                job_id = uuid4().hex
                unit = 'elephant-job-' + job_id + '.service'
                description = 'Elephant Job ' + job_id
                conn.execute(insert(jobs).values(id=job_id, job_key=request.job_key, task_id=scope.task_id,
                    root_task_id=scope.root_task_id, owner_id=scope.owner_id, workspace=str(root),
                    world_id=scope.world_id, arguments_hash=digest, request_json=arguments,
                    state='DISPATCHING', unit=unit, description=description, deadline=deadline.isoformat(),
                    created_at=now.isoformat(), updated_at=now.isoformat(), log_ref='journal:' + unit))
            else:
                job_id = previous['id']
                if previous['arguments_hash'] != digest:
                    raise ValueError('Job idempotency key is already bound to different arguments')
        row = self._owned(job_id, scope)
        # An existing dispatch intent is never launched again, including after an ambiguous receipt.
        if previous is not None:
            return await self.inspect_job(job_id, scope)
        async with self._lock(job_id):
            try:
                await self.provider.launch(unit=row['unit'], description=row['description'], request=request,
                                           workspace=root, deadline_seconds=seconds)
            except (OSError, RuntimeError, TimeoutError):
                row = self._save(row, state='UNKNOWN', error='dispatch_receipt_unknown')
            return self._handle(await self._refresh(row))

    async def _refresh(self, row):
        if row['state'] in _TERMINAL:
            return row
        try:
            receipt = await self.provider.inspect(row['unit'])
        except (OSError, RuntimeError, TimeoutError):
            return self._save(row, state='CANCEL_REQUESTED' if row['cancel_requested_at'] else 'UNKNOWN',
                              error='unit_state_unavailable')
        old_identity = json.loads(row['identity_json'] or '{}')
        if not receipt.exists:
            return self._save(row, state='CANCEL_REQUESTED' if row['cancel_requested_at'] else 'UNKNOWN',
                              error='unit_missing_receipt_unknown')
        if (receipt.unit != row['unit'] or receipt.description != row['description'] or not receipt.transient
                or not receipt.invocation_id
                or (old_identity and old_identity['invocation_id'] != receipt.invocation_id)):
            return self._save(row, state='CANCEL_REQUESTED' if row['cancel_requested_at'] else 'UNKNOWN',
                              error='unit_identity_mismatch')
        identity = {'invocation_id': receipt.invocation_id,
                    'cgroup': old_identity.get('cgroup') or receipt.cgroup,
                    'pid': old_identity.get('pid') or receipt.pid,
                    'started_at': old_identity.get('started_at') or receipt.started_at}
        if receipt.stopped:
            state = ('CANCELLED' if row['cancel_requested_at'] else
                     'EXITED' if receipt.exit_code == 0 and receipt.result == 'success' else 'FAILED')
        else:
            state = ('CANCEL_REQUESTED' if row['cancel_requested_at'] else
                     'WAITING_RECONCILIATION' if receipt.active_state in {'inactive', 'failed'} else
                     'STARTING' if receipt.active_state == 'activating' else 'RUNNING')
        return self._save(row, state=state, identity_json=json.dumps(identity),
                          exit_code=receipt.exit_code if receipt.stopped else None,
                          stopped_at=self.clock.now().isoformat() if receipt.stopped else None,
                          log_ref='journal:' + row['unit'] + ':' + receipt.invocation_id,
                          error=('unit_termination_unconfirmed' if receipt.active_state in {'inactive', 'failed'}
                                 and not receipt.stopped else None))

    async def inspect_job(self, job_id, scope):
        self._owned(job_id, scope)
        async with self._lock(job_id):
            row = self._owned(job_id, scope)
            return self._handle(await self._refresh(row))

    async def _cancel(self, row):
        if row['state'] in _TERMINAL:
            return self._handle(row)
        row = self._save(row, state='CANCEL_REQUESTED',
                         cancel_requested_at=row['cancel_requested_at'] or self.clock.now().isoformat())
        row = await self._refresh(row)
        if row['state'] in _TERMINAL or row['error'] not in {None, 'unit_termination_unconfirmed'}:
            return self._handle(row)
        try:
            identity = json.loads(row['identity_json'] or '{}')
            receipt = await self.provider.cancel(row['unit'], cgroup=identity.get('cgroup'))
            if receipt.get('unit') == row['unit'] and receipt.get('stopped') is True:
                return self._handle(self._save(row, state='CANCELLED', error=None,
                                               stopped_at=self.clock.now().isoformat()))
        except (OSError, RuntimeError, TimeoutError):
            row = self._save(row, error='cancellation_receipt_unknown')
        return self._handle(await self._refresh(row))

    async def cancel_job(self, job_id, scope):
        self._owned(job_id, scope)
        async with self._lock(job_id):
            return await self._cancel(self._owned(job_id, scope))

    async def cancel_root(self, root_task_id, owner_id):
        """Trusted runtime cancellation entry; never a model-selectable owner/tree."""
        with self.db.engine.connect() as conn:
            ids = conn.execute(select(jobs.c.id).where(jobs.c.root_task_id == root_task_id,
                jobs.c.owner_id == owner_id, jobs.c.state.not_in(_TERMINAL))).scalars().all()
        results = []
        for job_id in ids:
            async with self._lock(job_id):
                results.append(await self._cancel(self._row(job_id)))
        return results

    async def cancel_task(self, task_id, owner_id):
        with self.db.engine.connect() as conn:
            ids = conn.execute(select(jobs.c.id).where(jobs.c.task_id == task_id,
                jobs.c.owner_id == owner_id, jobs.c.state.not_in(_TERMINAL))).scalars().all()
        results = []
        for job_id in ids:
            async with self._lock(job_id):
                results.append(await self._cancel(self._row(job_id)))
        return results

    async def recover(self):
        """Query persisted units only. Never respawn Jobs after daemon restart."""
        with self.db.engine.connect() as conn:
            ids = conn.execute(select(jobs.c.id).where(jobs.c.state.not_in(_TERMINAL))).scalars().all()
        results = []
        for job_id in ids:
            async with self._lock(job_id):
                row = self._row(job_id)
                if row['cancel_requested_at'] or datetime.fromisoformat(row['deadline']) <= self.clock.now():
                    results.append(await self._cancel(row))
                else:
                    results.append(self._handle(await self._refresh(row)))
        return results

    def _scope(self, context):
        if self.context_resolver is None:
            raise RuntimeError('Job tools require a host-owned task/owner scope resolver')
        scope = self.context_resolver(context)
        return scope if isinstance(scope, JobScope) else JobScope.model_validate(scope)

    async def start(self, arguments, context):
        return (await self.start_job(JobStart.model_validate_json(json.dumps(arguments)), self._scope(context))).model_dump(mode='json')

    async def inspect(self, arguments, context):
        if (not isinstance(arguments, dict) or set(arguments) - {'job_id', 'include_logs'}
                or type(arguments.get('include_logs', False)) is not bool):
            raise ValueError('Invalid Job inspection arguments')
        scope = self._scope(context)
        handle = await self.inspect_job(arguments['job_id'], scope)
        result = handle.model_dump(mode='json')
        if arguments.get('include_logs', False):
            self._owned(arguments['job_id'], scope)
            result['logs'] = await self.provider.logs(handle.unit, invocation_id=handle.invocation_id)
        return result

    async def cancel(self, arguments, context):
        if not isinstance(arguments, dict) or set(arguments) != {'job_id'}:
            raise ValueError('Job cancellation accepts only job_id')
        return (await self.cancel_job(arguments['job_id'], self._scope(context))).model_dump(mode='json')

    def register(self, registry):
        for name in ('start', 'inspect', 'cancel'):
            properties = JobStart.model_json_schema() if name == 'start' else {
                'type': 'object', 'properties': {'job_id': {'type': 'string'},
                **({'include_logs': {'type': 'boolean'}} if name == 'inspect' else {})},
                'required': ['job_id'], 'additionalProperties': False}
            effect = 'read' if name == 'inspect' else 'write'
            registry.register(Capability('job.' + name, 'Persistent background Job 后台进程 ' + name,
                                         properties, timeout_seconds=60, max_output_bytes=70000),
                              getattr(self, name), lambda args, context, n=name, e=effect: Action(
                                  'workspace.job.' + n, ('workspace:' + context.workspace, 'task:' + context.task_id), e))


def register_remote_jobs(registry, call):
    """Worker tools call the daemon repository; no job state lives in the runner process."""
    for name in ('start', 'inspect', 'cancel'):
        parameters = JobStart.model_json_schema() if name == 'start' else {
            'type': 'object', 'properties': {'job_id': {'type': 'string'},
            **({'include_logs': {'type': 'boolean'}} if name == 'inspect' else {})},
            'required': ['job_id'], 'additionalProperties': False}
        async def execute(arguments, context, operation=name):
            arguments = {key: value for key, value in arguments.items() if key != '_preconditions'}
            return await call('jobs', {'operation': operation, 'arguments': arguments})
        registry.register(Capability('job.' + name, 'Persistent background Job 后台进程 ' + name,
                                     parameters, timeout_seconds=60, max_output_bytes=70000),
                          execute, lambda arguments, context, operation=name: Action(
                              'workspace.job.' + operation,
                              ('workspace:' + context.workspace, 'task:' + context.task_id),
                              'read' if operation == 'inspect' else 'write'))
