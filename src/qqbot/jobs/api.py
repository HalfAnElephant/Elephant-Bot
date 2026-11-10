import asyncio
import hmac

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from qqbot.domain.tasks import TaskResult
from qqbot.observability.diagnostics import report_worker


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


def internal_router(service, token):
    if len(token) < 32:
        raise ValueError("internal token must contain at least 32 characters")
    router = APIRouter(prefix="/internal/tasks")

    @router.post("/{operation}")
    async def invoke(operation: str, body: WorkerRequest, authorization: str = Header(default="")):
        if not hmac.compare_digest(authorization, "Bearer " + token):
            raise HTTPException(401)
        try:
            if operation == "worker-config" and body.payload:
                return report_worker(service, body.worker_id, body.payload)
            if operation == "claim":
                claim = service.claim(body.worker_id, body.protocol_version)
                if claim is None and body.wait_ms:
                    service.wakeup.clear()
                    try:
                        await asyncio.wait_for(service.wakeup.wait(), body.wait_ms / 1000)
                    except TimeoutError:
                        pass
                    claim = service.claim(body.worker_id, body.protocol_version)
                return claim
            if operation == "waiters":
                return service.external_waiters()
            if operation == "wait-authorize" and body.payload:
                return service.authorize_waiter(body.payload)
            if operation == "wait-observed" and body.payload:
                return service.observe_waiter(body.payload)
            if operation == "process-stopped" and body.payload:
                return service.process_stopped(body.worker_id, body.payload)
            if not body.run_id or not body.fencing_token:
                raise ValueError("run identity required")
            args = (body.run_id, body.fencing_token, body.worker_id)
            if operation == "memory-query" and body.payload:
                return await service.memory_query(*args, body.payload)
            if operation == "ledger" and body.payload:
                return await service.ledger(*args, body.payload)
            if operation == "input-ack" and body.payload:
                return service.acknowledge_input(*args, body.payload)
            if operation == "extend-budget" and body.payload:
                return service.extend_budget(*args, body.payload)
            if operation == "observation" and body.payload:
                return service.observation(*args, body.payload)
            if operation == "bind-process" and body.payload:
                return service.bind_process(*args, body.payload)
            if operation == "bind-skills" and body.payload:
                return service.bind_skills(*args, body.payload)
            if operation in {"notify", "ask", "authorize", "reserve-budget", "usage"} and body.payload is not None:
                method = {"notify": service.notify, "ask": service.ask, "authorize": service.authorize,
                          "reserve-budget": service.reserve_budget, "usage": service.record_usage}[operation]
                return method(*args, body.payload)
            if operation == "inputs":
                return service.take_inputs(*args)
            if operation == "event" and body.event_id and body.seq and body.payload is not None:
                return service.append_event(*args, body.event_id, body.seq, body.payload)
            if operation == "recovery-status":
                return service.recovery_status(*args)
            if operation == "control":
                return service.control(*args)
            if operation == "renew":
                return service.renew(*args)
            if operation == "uncertain":
                return service.uncertain(*args)
            if operation == "stopped":
                return service.stopped(*args)
            if operation == "result" and body.result:
                return service.submit(*args, body.result.model_dump(), stopped=body.stopped, quiescent=body.quiescent, process=body.process)
            raise ValueError("unsupported operation")
        except PermissionError as error:
            raise HTTPException(409, str(error)) from error
        except ValueError as error:
            raise HTTPException(422, str(error)) from error

    return router
