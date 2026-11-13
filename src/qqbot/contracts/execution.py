"""Execution world and persistent Job contracts. Identity is supplied only by the host."""
from datetime import datetime
from typing import Literal, Protocol

from pydantic import AwareDatetime, Field, field_validator

from qqbot.domain import DTO


class ExecutionCapabilities(DTO):
    world_id: str = 'local'
    filesystem: Literal['local'] = 'local'
    persistent_jobs: bool = False
    daemon_restart_survival: bool = False
    process_tree_cancel: bool = False
    reason: str | None = None


class JobScope(DTO):
    owner_id: str = Field(min_length=1, max_length=256)
    task_id: str = Field(min_length=1, max_length=256)
    root_task_id: str = Field(min_length=1, max_length=256)
    workspace: str = Field(min_length=1, max_length=4096)
    expires_at: AwareDatetime | None = None
    world_id: Literal['local'] = 'local'


class JobStart(DTO):
    job_key: str = Field(min_length=1, max_length=128)
    argv: tuple[str, ...] = Field(min_length=1, max_length=128)
    cwd: str = Field(default='.', min_length=1, max_length=4096)
    env: dict[str, str] = Field(default_factory=dict, max_length=3)
    timeout_seconds: int = Field(default=3600, ge=1, le=86400)
    restart_survival: bool = True

    @field_validator('argv')
    @classmethod
    def bounded_argv(cls, value):
        if any(not item or '\x00' in item for item in value) or sum(len(item.encode()) for item in value) > 65536:
            raise ValueError('Job argv must contain bounded nonempty strings without NUL')
        return value

    @field_validator('env')
    @classmethod
    def controlled_environment(cls, value):
        if (set(value) - {'LANG', 'LC_ALL', 'TZ'}
                or any('\x00' in item or '\n' in item or len(item.encode()) > 1024 for item in value.values())):
            raise ValueError('Job env permits only bounded LANG, LC_ALL and TZ values')
        return value


class JobHandle(DTO):
    job_id: str
    task_id: str
    root_task_id: str
    state: str
    unit: str
    invocation_id: str | None = None
    cgroup: str | None = None
    pid: int | None = None
    process_started_at: str | None = None
    deadline: datetime
    exit_code: int | None = None
    log_ref: str
    error: str | None = None
    daemon_restart_survival: bool


class ExecutionPort(Protocol):
    async def describe(self) -> ExecutionCapabilities: ...
    async def start_job(self, request: JobStart, scope: JobScope) -> JobHandle: ...
    async def inspect_job(self, job_id: str, scope: JobScope) -> JobHandle: ...
    async def cancel_job(self, job_id: str, scope: JobScope) -> JobHandle: ...
