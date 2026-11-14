"""One daemon owns admission, Pi processes, event replay and task recovery."""
import asyncio
import base64
import logging
from pathlib import Path
from types import SimpleNamespace

import httpx
from sqlalchemy import select, update

from qqbot.adapters.deepseek import DeepSeekAPI
from qqbot.capabilities.gateway import standard_registry
from qqbot.contracts.harness import resolve_harness
from qqbot.harness.pool import SessionPool
from qqbot.jobs.api import WorkerRequest
from qqbot.jobs.waiters import ExternalWaiters
from qqbot.jobs.worker import Spool, Worker, configured_github
from qqbot.providers import ModelCapacity
from qqbot.storage.task_schema import runs

logger = logging.getLogger(__name__)


class RuntimeClient:
    """Direct-call transport for the existing reliable runner; no Worker HTTP listener."""
    def __init__(self, core, leader):
        self.core, self.service, self.leader = core, core.tasks, leader
        self.last_role = "worker"

    async def post(self, path, *, json=None, content=None, headers=None, timeout=None):
        request = httpx.Request("POST", "http://runtime.local" + path)
        try:
            if path.endswith("/qq-file-upload"):
                metadata = __import__("json").loads(base64.urlsafe_b64decode(headers["X-QQ-Upload"]))
                body = WorkerRequest.model_validate(metadata)
                from qqbot.jobs.rich_delivery import local_file_upload
                result = await local_file_upload(self.service, (body.run_id, body.fencing_token, body.worker_id),
                                                body.payload, content)
            else:
                body = WorkerRequest.model_validate(json)
                result = await self.invoke(path.rsplit("/", 1)[-1], body)
            if result is None:
                return httpx.Response(200, content=b"null", headers={"content-type": "application/json"}, request=request)
            return httpx.Response(200, json=result, request=request)
        except PermissionError as error:
            if path.endswith("/session-receipt"):
                payload = body.payload or {}
                logger.warning("Session receipt rejected run_id=%s kind=%s inflight=%s tools_paired=%s detail=%s",
                    body.run_id, payload.get("kind"), payload.get("inflight"), payload.get("tools_paired"), error)
            return httpx.Response(409, json={"detail": str(error)}, request=request)
        except (ValueError, LookupError, TypeError) as error:
            return httpx.Response(422, json={"detail": str(error)}, request=request)

    async def invoke(self, operation, body):
        result = await self._invoke(operation, body)
        if body.run_id:
            self.leader.observe_operation((body.run_id, body.fencing_token, body.worker_id), operation, body.payload or {}, result)
        return result

    async def _invoke(self, operation, body):
        if operation == "claim":
            self.core.knowledge.recover_mutations()
            functions = ((self.leader.sessions.claim, self.service.claim) if self.last_role == "worker"
                         else (self.service.claim, self.leader.sessions.claim))
            functions = (*functions, self.leader.claim_review)
            for claim in functions:
                result = claim(body.worker_id)
                if result:
                    self.last_role = result.get("role", "worker")
                    result["host_epoch"] = self.core.recovery.epoch
                    with self.service.db.transaction() as conn:
                        conn.execute(update(runs).where(runs.c.id == result["run_id"]).values(request_json=__import__("json").dumps(result)))
                    return result
            return None
        if operation == "process-stopped":
            return self.service.process_stopped(body.worker_id, body.payload)
        if operation == "waiters":
            return self.service.external_waiters()
        if operation == "wait-authorize":
            return self.service.authorize_waiter(body.payload)
        if operation == "wait-job-inspect":
            return await self.core.job_runtime.inspect_waiter(body.payload)
        if operation == "wait-observed":
            return self.service.observe_waiter(body.payload)
        identity = (body.run_id, body.fencing_token, body.worker_id)
        if operation == "bind-process":
            body.payload["identity"] = {**body.payload.get("identity", {}), "host_epoch": self.core.recovery.epoch}
        if operation == "memory":
            return await self.core.knowledge.invoke(identity, body.payload)
        if operation == "schedules":
            return await self.leader.invoke(operation, identity, body.payload)
        if operation == "jobs":
            return await self.core.job_runtime.invoke(identity, body.payload)
        with self.service.db.engine.connect() as conn:
            role = conn.execute(select(runs.c.role).where(runs.c.id == body.run_id)).scalar_one_or_none()
        values = body.model_dump(exclude_none=True)
        for key in ("worker_id", "run_id", "fencing_token", "payload"):
            values.pop(key, None)
        if role in {"leader", "skill_review"}:
            return await self.leader.invoke(operation, identity, body.payload, **values)
        if operation == "result":
            return self.service.submit(*identity, body.result.model_dump(mode="json"), stopped=body.stopped,
                quiescent=body.quiescent, process=body.process, receipt=body.receipt)
        if operation == "event":
            return self.service.append_event(*identity, body.event_id, body.seq, body.payload)
        if operation == "ledger":
            return await self.service.ledger(*identity, body.payload)
        if operation == "skills":
            from qqbot.skills import skill_operation
            return await asyncio.to_thread(skill_operation, self.service, identity, body.payload)
        if operation in {"qq-send", "qq-status", "qq-buttons", "qq-file-send"}:
            from qqbot.jobs.rich_delivery import local_file_send, rich_buttons, rich_send, rich_status
            function = {"qq-send": rich_send, "qq-status": rich_status, "qq-buttons": rich_buttons,
                        "qq-file-send": local_file_send}[operation]
            result = function(self.service, identity, body.payload)
            return await result if hasattr(result, "__await__") else result
        methods = {"session-receipt": "session_receipt", "tasks": "task_operation", "inputs": "take_inputs",
            "input-ack": "acknowledge_input", "extend-budget": "extend_budget", "bind-process": "bind_process",
            "reserve-budget": "reserve_budget", "usage": "record_usage", "recovery-status": "recovery_status"}
        method = getattr(self.service, methods.get(operation, operation.replace("-", "_")))
        if operation in {"control", "renew", "inputs", "stopped", "uncertain", "recovery-status"}:
            return method(*identity) if operation != "inputs" else self.service.take_inputs(*identity)
        return method(*identity, body.payload)


class Scheduler:
    def __init__(self, core, leader):
        self.core, self.leader, self.settings = core, leader, core.settings
        data = Path(core.db.path).resolve().parent
        self.client = RuntimeClient(core, leader)
        model_limit = self.settings.orchestration.model_request_limit
        self.capacity = ModelCapacity(total=model_limit, reserved=int(model_limit > 1))
        from qqbot.adapters.execution.world import LocalExecution
        self.execution = getattr(core, "execution", None) or LocalExecution()
        core.job_runtime = self.execution.create_job_runtime(core, data)
        self.job_maintenance = None
        self.jobs_recovered = False
        self.next_job_maintenance = 0.0
        self.args = SimpleNamespace(pi=self.settings.orchestration.pi_executable,
            workspace=str(data / "pi"), archive=str(data / "artifacts"), spool=str(data / "spool"),
            worker_id="daemon", github_host=self.settings.orchestration.github_host,
            gh="gh", github_token_env="QQBOT_GITHUB_TOKEN", workspace_binding=[])

        async def media_headers(claim, attachment):
            from qqbot.adapters.qq_official.media_download import inbound_media_headers
            return await inbound_media_headers(core, claim, attachment)

        def factory(claim, process_id, session_id):
            binding = claim.get("binding") or leader.sessions.binding()
            adapter = resolve_harness(core, binding)
            if binding.get("execution_world", "local") != self.execution.world_id:
                raise ValueError("persisted task requires an unavailable execution world")
            args = SimpleNamespace(**{**vars(self.args), "model": binding.get("model"),
                "model_endpoint": binding.get("endpoint"), "model_key_env": binding.get("credential_env", "QQBOT_MODEL_KEY"),
                "model_context_window": binding.get("context_window", 32768),
                "workspace_binding": [key + "=" + value for key, value in binding.get("workspace_bindings", {}).items()],
                "execution": self.execution, "media_headers": media_headers})
            if not args.model or not args.model_endpoint:
                raise ValueError("persisted session has no configured model binding")
            runner = adapter.create(args, claim, self.client, process_id, session_id, self.capacity)
            runner.archive = Path(args.archive)
            return runner

        self.pool = SessionPool(factory,
            max_idle=self.settings.execution.warm_idle_max if self.settings.execution.warm_max else 0,
            ttl=self.settings.execution.warm_ttl_seconds)
        self.worker = Worker(self.client, "daemon", Spool(self.args.spool), factory, pool=self.pool)
        async def waiter_call(operation, payload):
            return await self.worker.call(operation, payload=payload)
        import os
        deepseek = DeepSeekAPI.from_model(self.settings.model.endpoint,
            os.environ.get(self.settings.model.api_key_env, ""))
        self.waiters = ExternalWaiters(waiter_call, standard_registry(github=configured_github(self.args),
                                                                    execution=self.execution, deepseek=deepseek))
        self.active = set()

    async def step(self):
        loop = asyncio.get_running_loop()
        if self.job_maintenance is not None and self.job_maintenance.done():
            self.job_maintenance.result()
            self.job_maintenance = None
            self.jobs_recovered = True
            self.next_job_maintenance = loop.time() + 1
        if self.job_maintenance is None and loop.time() >= self.next_job_maintenance:
            self.job_maintenance = asyncio.create_task(self.core.job_runtime.maintain())
        if not self.jobs_recovered:
            return False
        completed = False
        failure = None
        for task in list(self.active):
            if task.done():
                self.active.remove(task)
                # A failed runner leaves its persisted spool for the next bounded replay.
                try:
                    task.result()
                except asyncio.CancelledError:
                    failure = failure or RuntimeError("Worker cycle was cancelled unexpectedly")
                except Exception as error:  # noqa: BLE001 — drain the completed batch, then re-raise its failure.
                    failure = failure or error
                else:
                    completed = True
        if failure is not None:
            raise failure
        async with self.worker.replay_lock:
            await self.worker.replay()
        while claim := await self.worker.call("claim", protocol_version=2, wait_ms=0):
            self.active.add(asyncio.create_task(self.worker.once(claim)))
            await asyncio.sleep(0)
        await self.pool.sweep()
        # A decoded empty claim confirms idle health without starting a Worker cycle.
        return completed or not self.active

    async def close(self):
        self.worker.begin_shutdown()
        if self.job_maintenance is not None:
            self.job_maintenance.cancel()
            await asyncio.gather(self.job_maintenance, return_exceptions=True)
        for task in self.active:
            task.cancel()
        await asyncio.gather(*self.active, return_exceptions=True)
        await self.pool.close()
