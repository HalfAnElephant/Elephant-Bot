"""Worker request schema shared by daemon and legacy HTTP transports."""

from pydantic import BaseModel, ConfigDict, Field

from qqbot.domain.tasks import TaskResult


class WorkerRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    worker_id: str = Field(min_length=1, max_length=128)
    run_id: str | None = None
    fencing_token: int | None = Field(default=None, ge=1)
    result: TaskResult | None = None
    stopped: bool = True
    quiescent: bool = False
    process: dict | None = None
    protocol_version: int = 1
    wait_ms: int = Field(default=0, ge=0, le=10000)
    event_id: str | None = Field(default=None, max_length=128)
    seq: int | None = Field(default=None, ge=1)
    payload: dict | None = None
    receipt: dict | None = None
