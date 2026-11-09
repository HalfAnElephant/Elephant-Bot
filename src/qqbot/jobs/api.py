import hmac

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from qqbot.domain.tasks import TaskResult


class WorkerRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    worker_id: str = Field(min_length=1, max_length=128)
    run_id: str | None = None
    fencing_token: int | None = Field(default=None, ge=1)
    result: TaskResult | None = None
    stopped: bool = True
    event_id: str | None = Field(default=None, max_length=128)
    seq: int | None = Field(default=None, ge=1)
    payload: dict | None = None


def internal_router(service, token):
    if len(token) < 32:
        raise ValueError("internal token must contain at least 32 characters")
    router = APIRouter(prefix="/internal/tasks")

    @router.post("/{operation}")
    def invoke(operation: str, body: WorkerRequest, authorization: str = Header(default="")):
        if not hmac.compare_digest(authorization, "Bearer " + token):
            raise HTTPException(401)
        try:
            if operation == "claim":
                return service.claim(body.worker_id)
            if not body.run_id or not body.fencing_token:
                raise ValueError("run identity required")
            args = (body.run_id, body.fencing_token, body.worker_id)
            if operation == "event" and body.event_id and body.seq and body.payload is not None:
                return service.append_event(*args, body.event_id, body.seq, body.payload)
            if operation == "recovery-status":
                return service.recovery_status(*args)
            if operation == "renew":
                return service.renew(*args)
            if operation == "uncertain":
                return service.uncertain(*args)
            if operation == "stopped":
                return service.stopped(*args)
            if operation == "result" and body.result:
                return service.submit(*args, body.result.model_dump(), stopped=body.stopped)
            raise ValueError("unsupported operation")
        except PermissionError as error:
            raise HTTPException(409, str(error)) from error
        except ValueError as error:
            raise HTTPException(422, str(error)) from error

    return router
