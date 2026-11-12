"""Independent HTTP-only supervisor. No database handle is imported here."""

import asyncio
import inspect
import json
import logging
import os
import socket
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

import httpx
import uvicorn

from qqbot.harness.artifacts import export_artifacts
from qqbot.harness.gateway import RunGateway
from qqbot.harness.launcher import (
    LAUNCHER,
    MANIFESTS,
    WORKSPACES,
    production_policy,
    validate_manifest,
    verify_stop_marker,
)
from qqbot.harness.pi import MAX_EVENT, PiHarness, compaction_settings
from qqbot.harness.pool import PoolAcquisitionError, SessionPool

logger = logging.getLogger(__name__)

class CapabilityServer(uvicorn.Server):
    @contextmanager
    def capture_signals(self):
        # The supervisor owns shutdown. An embedded per-run server cannot replace it.
        yield


class Spool:
    def __init__(self, directory, max_bytes=20 * 1024 * 1024):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.max_bytes = max_bytes

    def save(self, payload, *, run_id=None, seq=None):
        data = json.dumps(payload, ensure_ascii=False).encode()
        total = sum(path.stat().st_size for path in self.directory.rglob("*") if path.is_file())
        if total + len(data) > self.max_bytes:
            raise RuntimeError("spool full")
        if run_id is None:
            destination = self.directory / f"{uuid4()}.json"
        else:
            if seq is None or not 1 <= seq <= 5000:
                raise RuntimeError("event spool limit exceeded")
            directory = self.directory / "events" / run_id
            directory.mkdir(parents=True, mode=0o700, exist_ok=True)
            destination = directory / f"{seq:08d}.json"
        temporary = destination.with_suffix(".tmp")
        with temporary.open("xb") as output:
            os.chmod(temporary, 0o600)
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        temporary.replace(destination)
        directory_fd = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        return destination


class Worker:
    def __init__(self, client, worker_id, spool, harness_factory, *, pool=None):
        self.client, self.worker_id, self.spool, self.harness_factory = (
            client,
            worker_id,
            spool,
            harness_factory,
        )
        self.pool = pool
        self.replay_lock = asyncio.Lock()
        self.event_lock = asyncio.Lock()

    async def call(self, operation, **payload):
        response = await self.client.post(
            "/internal/tasks/" + operation, json={"worker_id": self.worker_id, **payload}
        )
        response.raise_for_status()
        return response.json()

    async def save_result(self, payload):
        try:
            return self.spool.save(payload)
        except (OSError, RuntimeError):
            logger.exception("Result persistence failed worker_id=%s run_id=%s", self.worker_id, payload["run_id"])
            try:
                await self.call("uncertain", **{key: payload[key] for key in ("run_id", "fencing_token")})
            except Exception:
                logger.exception("Run uncertainty report failed worker_id=%s run_id=%s", self.worker_id, payload["run_id"])
            raise

    async def deliver_result(self, payload, path):
        if not payload.get("stopped") and not payload.get("quiescent"):
            identity = {key: payload[key] for key in ("run_id", "fencing_token")}
            status = await self.call("recovery-status", **identity)
            if status["discard_spool"]:
                path.unlink(missing_ok=True)
                return True
            await self.call("uncertain", **identity)
            return False
        process = payload.get("process")
        if payload.get("stopped") and process:
            await self.call("process-stopped", payload={"process_id": process["id"], "epoch": process["epoch"],
                "generation": process["generation"], "stopped": True})
        try:
            await self.call("result", **payload)
        except httpx.HTTPStatusError as error:
            if error.response.status_code != 409 or not payload.get("stopped"):
                raise
            identity = {key: payload[key] for key in ("run_id", "fencing_token")}
            lease = await self.call("renew", **identity)
            if lease["state"] != "CANCEL_REQUESTED":
                raise
            await self.call("stopped", **identity)
        path.unlink(missing_ok=True)
        return True

    async def flush_events(self, run_id=None):
        async with self.event_lock:
            await self._flush_events(run_id)

    async def _flush_events(self, run_id=None):
        root = self.spool.directory / "events"
        if run_id:
            root = root / run_id
        async with asyncio.timeout(2):
            for path in sorted(root.rglob("*.json")):
                payload = json.loads(path.read_text())
                try:
                    await self.call("event", **payload)
                except httpx.HTTPStatusError as error:
                    if error.response.status_code != 409:
                        raise
                    identity = {key: payload[key] for key in ("run_id", "fencing_token")}
                    status = await self.call("recovery-status", **identity)
                    if not status["discard_spool"]:
                        continue
                path.unlink(missing_ok=True)

    def event_sink(self, identity):
        seq = 0

        def save(event):
            nonlocal seq
            original = event.get("type")
            kinds = {
                "host_accepted": "accepted",
                "agent_start": "progress",
                "agent_end": "progress",
                "message_start": "progress",
                "message_end": "progress",
                "agent_settled": "result_ready",
                "tool_execution_start": "tool_started",
                "tool_execution_end": "tool_finished",
                "source_retrieved": "source",
                "host_error": "failed",
                "public_answer_start": "public_answer_start",
                "public_answer_delta": "public_answer_delta",
                "public_answer_end": "public_answer_end",
                "public_answer_final": "public_answer_final",
            }
            if original == "response":
                if event.get("command") != "prompt":
                    return
                kind = "accepted" if event.get("success") else "failed"
            else:
                kind = kinds.get(original)
            if not kind:
                return
            payload = {"kind": kind, "pi_type": original}
            if original.startswith("public_answer_"):
                payload.update({key: event[key] for key in ("message_id", "text", "error") if key in event})
            for key in ("toolName", "toolCallId", "isError", "durationMs", "source"):
                if key in event:
                    payload[key] = event[key]
            message = event.get("message", {})
            if original == "message_end":
                payload["role"] = message.get("role")
                if message.get("errorMessage"):
                    payload["error"] = str(message["errorMessage"])[:1000]
                if message.get("usage"):
                    payload["usage"] = message["usage"]
            if original == "host_error":
                payload["error"] = str(event.get("error", ""))[-1000:]
            seq += 1
            self.spool.save(
                {**identity, "event_id": str(uuid4()), "seq": seq, "payload": payload},
                run_id=identity["run_id"],
                seq=seq,
            )

        return save

    def quarantine_result(self, path):
        rejected = self.spool.directory / "rejected"
        rejected.mkdir(mode=0o700, exist_ok=True)
        destination = rejected / path.name
        path.replace(destination)
        logger.error("Core rejected result evidence preserved worker_id=%s spool=%s", self.worker_id, destination)

    async def lease_heartbeat(self, identity):
        # Renewal preserves supervisor ownership only. It never updates runner authority or deadlines.
        while True:
            try:
                await self.call("renew", **identity)
            except httpx.HTTPStatusError as error:
                if error.response.status_code == 409:
                    return
                logger.exception("Lease heartbeat failed run_id=%s", identity["run_id"])
            except httpx.TransportError:
                logger.exception("Lease heartbeat unavailable run_id=%s", identity["run_id"])
            await asyncio.sleep(5)

    async def replay(self):
        await self.flush_events()
        for path in sorted(self.spool.directory.glob("*.json")):
            payload = json.loads(path.read_text())
            # A stale result remains recovery evidence. It never re-launches the model.
            try:
                await self.deliver_result(payload, path)
            except httpx.HTTPStatusError as error:
                if error.response.status_code == 409:
                    identity = {key: payload[key] for key in ("run_id", "fencing_token")}
                    status = await self.call("recovery-status", **identity)
                    if status["discard_spool"]:
                        path.unlink(missing_ok=True)
                    continue
                if error.response.status_code == 422:
                    self.quarantine_result(path)
                    recovery = {**payload, "stopped": False, "quiescent": False,
                        "result": {"status": "failed", "agent_authored": False,
                            "summary": "Core rejected persisted result. Original evidence quarantined for recovery."}}
                    recovery_path = await self.save_result(recovery)
                    try:
                        await self.deliver_result(recovery, recovery_path)
                    except httpx.HTTPStatusError as recovery_error:
                        if recovery_error.response.status_code != 409:
                            raise
                    continue
                raise

    def failure_result(self, claim, error, harness=None):
        summary = f"Runner failed with {type(error).__name__}."
        if harness is not None and hasattr(harness, "failure_summary"):
            try:
                summary = harness.failure_summary(error)
            except Exception:  # noqa: BLE001 -- Diagnostics must preserve the original failure.
                logger.error("Failure diagnostic unavailable run_id=%s", claim["run_id"])
        logger.error("Worker run failed worker_id=%s task_id=%s run_id=%s fence=%s error=%s",
                     self.worker_id, claim["task_id"], claim["run_id"], claim["fencing_token"], summary)
        return {"status": "failed", "agent_authored": False, "summary": summary}

    async def stop_execution(self, harness, execution):
        if execution is not None:
            if not execution.done():
                execution.cancel()
            await asyncio.gather(execution, return_exceptions=True)
        if harness is None:
            return False
        if not harness.stopped_verified:
            try:
                await harness.stop()
            except Exception:
                harness.stopped_verified = False
                logger.exception("Runner stop failed worker_id=%s process_id=%s",
                                 self.worker_id, getattr(harness, "process_id", None))
        return harness.stopped_verified

    async def once(self):
        async with self.replay_lock:
            await self.replay()
        claim = await self.call("claim", protocol_version=2, wait_ms=2000 if self.pool else 0)
        if not claim:
            return False
        identity = {key: claim[key] for key in ("run_id", "fencing_token")}
        asyncio.current_task().set_name("qqbot-run:" + claim["task_id"] + ":" + claim["run_id"])
        use_pool = self.pool is not None and claim.get("protocol_version", 1) >= 2
        harness = execution = path = None
        committed = False
        heartbeat = None
        last_renewed = time.monotonic()
        try:
            harness = await self.pool.acquire(claim) if use_pool else self.harness_factory(claim)
            if inspect.isawaitable(harness):
                harness = await harness
            harness.event_sink = self.event_sink(identity)
            harness.public_event_sink = harness.event_sink if claim.get("public_answer_events") is True else None
            gateway = getattr(harness, "gateway", None)
            if gateway:
                gateway.event_sink = harness.event_sink
            harness.event_sink({"type": "host_accepted"})
            execution = asyncio.create_task(harness.run(claim["goal"], keep_alive=True) if use_pool else harness.run(claim["goal"]))
            while not execution.done():
                done, _ = await asyncio.wait({execution}, timeout=0.15)
                if done:
                    break
                operation = "renew" if time.monotonic() - last_renewed >= 10 else "control"
                lease = await self.call(operation, **identity)
                if operation == "renew":
                    last_renewed = time.monotonic()
                if execution.done():
                    break
                if getattr(harness, "gateway", None):
                    harness.gateway.provider.background = lease.get("delivery") == "DETACHED"
                    if "budget" in lease:
                        harness.gateway.tools.max_calls = lease["budget"].get("tool_calls", 60)
                    deadline = getattr(harness, "run_timeout", None)
                    if "active_seconds_remaining" in lease and deadline is not None:
                        deadline.reschedule(asyncio.get_running_loop().time() + lease["active_seconds_remaining"])
                if hasattr(harness, "renewed"):
                    harness.renewed()
                if lease["state"] != "RUNNING":
                    heartbeat = asyncio.create_task(self.lease_heartbeat(identity))
                    await self.stop_execution(harness, execution)
                    payload = {**identity, "result": {"status": "failed", "agent_authored": False,
                        "summary": "Execution stopped after run authority changed."},
                        "stopped": harness.stopped_verified, "quiescent": False}
                    if hasattr(harness, "process_identity"):
                        payload["process"] = harness.process_identity()
                    path = await self.save_result(payload)
                    await self.flush_events(claim["run_id"])
                    committed = await self.deliver_result(payload, path)
                    return committed
                await self.flush_events(claim["run_id"])
                if getattr(harness, "gateway", None) and not harness.gateway.waiting and getattr(harness, "accepting_input", True):
                    for extra in await self.call("inputs", **identity):
                        try:
                            if extra.get("images"):
                                context = harness.gateway.claim.setdefault("context", {})
                                known = context.setdefault("images", [])
                                for image in extra["images"]:
                                    if image not in known:
                                        known.append(image)
                                        harness.gateway.image_content = None
                            await harness.append_input(extra["id"], extra["text"])
                        except RuntimeError:
                            if not execution.done() and getattr(harness, "accepting_input", True):
                                raise
                            break
            heartbeat = asyncio.create_task(self.lease_heartbeat(identity))
            result = execution.result().model_dump(mode="json")
            if gateway:
                result["evidence"] = gateway.evidence
                paths = list(gateway.artifacts)
                if result["status"] == "completed" and claim.get("role", "worker") == "worker":
                    paths.extend(claim.get("goal_spec", {}).get("requested_outputs", []))
                result["artifacts"] = export_artifacts(gateway.workspace, harness.archive, list(dict.fromkeys(paths)))
            if not harness.stopped_verified and not getattr(harness, "quiescent_verified", False):
                raise RuntimeError("runner returned without verified stop or quiescence")
            payload = {**identity, "result": result, "stopped": harness.stopped_verified,
                       "quiescent": getattr(harness, "quiescent_verified", False),
                       "receipt": getattr(harness, "last_receipt", None)}
            if hasattr(harness, "process_identity"):
                payload["process"] = harness.process_identity()
            path = await self.save_result(payload)
            await self.flush_events(claim["run_id"])
            committed = await self.deliver_result(payload, path)
            return committed
        except asyncio.CancelledError:
            if heartbeat is None:
                heartbeat = asyncio.create_task(self.lease_heartbeat(identity))
            stopped = await self.stop_execution(harness, execution)
            if path is None:
                payload = {**identity, "result": {"status": "failed", "agent_authored": False,
                    "summary": "Supervisor stopped before task completion."}, "stopped": stopped, "quiescent": False}
                if harness is not None and hasattr(harness, "process_identity"):
                    payload["process"] = harness.process_identity()
                try:
                    path = await self.save_result(payload)
                    await self.flush_events(claim["run_id"])
                    committed = await self.deliver_result(payload, path)
                except Exception:
                    logger.exception("Cancelled run needs recovery worker_id=%s run_id=%s spool=%s",
                                     self.worker_id, claim["run_id"], path)
            raise
        except Exception as error:
            if heartbeat is None:
                heartbeat = asyncio.create_task(self.lease_heartbeat(identity))
            if path is not None and isinstance(error, httpx.HTTPStatusError) and error.response.status_code == 422:
                self.quarantine_result(path)
                path = None
            if path is not None:
                logger.error("Result delivery pending worker_id=%s task_id=%s run_id=%s spool=%s error_type=%s",
                             self.worker_id, claim["task_id"], claim["run_id"], path, type(error).__name__)
                raise
            if isinstance(error, PoolAcquisitionError):
                harness = error.runner
            stopped = await self.stop_execution(harness, execution)
            if harness is None and use_pool and isinstance(self.pool, SessionPool):
                stopped = True  # Pool acquisition failed before binding a runner for this run.
            result = self.failure_result(claim, error, harness)
            payload = {**identity, "result": result, "stopped": stopped, "quiescent": False}
            if harness is not None and hasattr(harness, "process_identity"):
                payload["process"] = harness.process_identity()
            path = await self.save_result(payload)
            try:
                if harness is not None and harness.event_sink is not None:
                    harness.event_sink({"type": "host_error", "error": result["summary"]})
                await self.flush_events(claim["run_id"])
            except Exception:
                logger.exception("Failure events pending worker_id=%s run_id=%s spool=%s",
                                 self.worker_id, claim["run_id"], path)
            committed = await self.deliver_result(payload, path)
            return committed
        finally:
            try:
                if harness is not None:
                    if use_pool:
                        await self.pool.release(harness, committed)
                    elif not harness.stopped_verified:
                        await self.stop_execution(harness, execution)
                if execution is not None:
                    if not execution.done():
                        execution.cancel()
                    await asyncio.gather(execution, return_exceptions=True)
            finally:
                if heartbeat is not None:
                    heartbeat.cancel()
                    await asyncio.gather(heartbeat, return_exceptions=True)


def configured_github(args):
    from qqbot.capabilities.github import GitHubCLI
    host = getattr(args, "github_host", "github.com")
    executable = getattr(args, "gh", "gh")
    def credentials(context):
        if context.host != host:
            raise PermissionError("credential host differs from requested host")
        # Ignore gh's environment overrides so the local login really takes precedence.
        environment = {key: value for key, value in os.environ.items() if key not in {
            "GH_TOKEN", "GITHUB_TOKEN", "GH_ENTERPRISE_TOKEN", "GITHUB_ENTERPRISE_TOKEN", "GH_DEBUG"}}
        token = ""
        try:
            result = subprocess.run(
                [executable, "auth", "token", "--hostname", host], env=environment,
                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                text=True, timeout=5, check=False,
            )
            if result.returncode == 0:
                token = result.stdout.strip()
        except (OSError, subprocess.SubprocessError):
            pass
        token = token or os.environ.get(getattr(args, "github_token_env", "QQBOT_GITHUB_TOKEN"), "")
        return {"GH_TOKEN" if host == "github.com" else "GH_ENTERPRISE_TOKEN": token} if token else {}
    return GitHubCLI(executable=executable, credentials=credentials)


class GeneralRun(PiHarness):
    def __init__(self, args, claim, client, process_id, session_id, capacity):
        self.args, self.claim, self.core_client = args, claim, client
        self.worker_id, self.process_id, self.session_id = args.worker_id, process_id, session_id
        self.process_epoch, self.registered = 1, False
        workspace = Path(args.workspace) / session_id
        self.prepare_workspace(workspace)
        self.archive = Path(args.archive)
        self.server = self.server_task = self.listener = None
        github = configured_github(args)
        gateway = RunGateway(workspace, args.model, os.environ.get(getattr(args, "model_key_env", "QQBOT_RUNNER_OPENAI_KEY"), ""),
            args.model_endpoint, call=self.gateway_call, upload=self.gateway_upload, capacity=capacity, github=github,
            github_host=getattr(args, "github_host", "github.com"),
            content_root=Path(getattr(args, "spool", str(Path(args.archive).parent / "spool"))) / "content",
            model_window=getattr(args, "model_context_window", 32768))
        super().__init__(args.pi, workspace, provider="qqbot", model=args.model, gateway=gateway, session_id=session_id)

    def prepare_workspace(self, workspace):
        workspace.mkdir(parents=True, exist_ok=True, mode=0o700)
        (workspace / "outputs").mkdir(exist_ok=True, mode=0o700)

    async def gateway_upload(self, metadata, content):
        import base64
        header = base64.urlsafe_b64encode(json.dumps({
            "worker_id": self.worker_id, "run_id": self.claim["run_id"],
            "fencing_token": self.claim["fencing_token"], "payload": metadata},
            ensure_ascii=True).encode()).decode()
        response = await self.core_client.post("/internal/tasks/qq-file-upload", content=content,
            headers={"Content-Type": "application/octet-stream", "Content-Length": str(metadata["size"]),
                     "X-QQ-Upload": header}, timeout=600)
        response.raise_for_status()
        return response.json()

    async def gateway_call(self, operation, payload):
        response = await self.core_client.post("/internal/tasks/" + operation, json={
            "worker_id": self.worker_id, "run_id": self.claim["run_id"],
            "fencing_token": self.claim["fencing_token"], "payload": payload})
        response.raise_for_status()
        return response.json()

    async def bind(self, claim):
        if self.busy:
            raise RuntimeError("cannot bind active session")
        self.claim = claim
        if not claim.get("repository") and getattr(self.args, "repository", None):
            claim["repository"] = self.args.repository
        bindings = dict(value.split("=", 1) for value in getattr(self.args, "workspace_binding", []))
        from qqbot.adapters.execution.world import execution_workspace as resolve_workspace
        data_root = Path(getattr(self.args, "spool", str(Path(self.args.archive).parent / "spool"))).parent
        execution_workspace = resolve_workspace(data_root, claim["task_id"],
            claim.get("plan", {}).get("workspace_ref"), bindings, create=True)
        (execution_workspace / "outputs").mkdir(exist_ok=True, mode=0o700)
        self.gateway.workspace = execution_workspace
        self.gateway.bind(claim)
        self.timeout = claim.get("active_seconds_remaining", claim.get("plan", {}).get("budget", {}).get("active_seconds", 1800))
        if self.timeout <= 0:
            raise ValueError("task active execution budget exhausted")
        await self.gateway_call("bind-process", {"process_id": self.process_id, "session_id": self.session_id,
            "epoch": self.process_epoch, "generation": self.gateway.generation,
            "scope_key": claim.get("context", {}).get("key", "task:" + claim["task_id"]),
            "session_file": str(self.session_path), "identity": {"pid": self.process.pid if self.process else None,
                "launch_task_id": getattr(self, "launch_task_id", claim["task_id"])}})
        self.registered = True

    def process_identity(self):
        return {"id": self.process_id, "epoch": self.process_epoch, "generation": self.gateway.generation,
                "actual_session_id": getattr(self, "actual_session_id", None),
                "actual_session_file": getattr(self, "actual_session_file", None)}

    async def start(self):
        self.listener = socket.socket()
        self.bind_listener()
        self.listener.listen(16)
        self.gateway_url = f"http://{self.listener.getsockname()[0]}:{self.listener.getsockname()[1]}"
        self.server = CapabilityServer(uvicorn.Config(self.gateway.app, log_level="warning", access_log=False))
        self.server_task = asyncio.create_task(self.server.serve(sockets=[self.listener]))
        for _ in range(100):
            if self.server.started:
                break
            if self.server_task.done():
                await self.server_task
            await asyncio.sleep(0.01)
        if not self.server.started:
            raise RuntimeError("capability gateway failed to start")
        await self.start_runner()

    def bind_listener(self):
        self.listener.bind(("127.0.0.1", 0))

    async def start_runner(self):
        await PiHarness.start(self)

    async def stop_runner(self):
        return await PiHarness.stop(self)

    async def stop(self):
        stopped = False
        try:
            if not self.process and not self.server_task and not getattr(self, "manifest_path", None):
                self.gateway.revoke()
                stopped = True
                return stopped
            drained = await self.gateway.stop_requests()
            runner_stopped = await self.stop_runner()
            if self.server:
                self.server.should_exit = True
            if self.server_task:
                try:
                    await asyncio.wait_for(asyncio.shield(self.server_task), 2)
                except TimeoutError:
                    self.server_task.cancel()
                    await asyncio.gather(self.server_task, return_exceptions=True)
            if self.listener:
                self.listener.close()
            pending = {task for task in self.server.server_state.tasks if not task.done()} if self.server else set()
            for task in pending:
                task.cancel()
            if pending:
                _, pending = await asyncio.wait(pending, timeout=10)
            stopped = runner_stopped and drained and not pending
            return stopped
        finally:
            self.stopped_verified = stopped
            if self.registered:
                try:
                    await self.gateway_call("process-stopped", {"process_id": self.process_id,
                        "epoch": self.process_epoch, "generation": self.gateway.generation, "stopped": stopped})
                except httpx.HTTPError:
                    logger.exception("Process stop report pending worker_id=%s process_id=%s run_id=%s",
                                     self.worker_id, self.process_id, self.claim["run_id"])


class SystemdGeneralRun(GeneralRun):
    def __init__(self, args, claim, client, process_id, session_id, capacity):
        self.policy = production_policy()
        if Path(args.pi).resolve() != Path("/opt/qqbot/node_modules/.bin/pi").resolve(strict=True):
            raise ValueError("production pi runtime path is fixed")
        if Path(args.workspace).resolve() != WORKSPACES:
            raise ValueError("production workspace root is fixed")
        self.launch_task_id = claim["task_id"]
        self.manifest_path = None
        super().__init__(args, claim, client, process_id, session_id, capacity)

    def prepare_workspace(self, workspace):
        # Only the root launcher creates production workspace directories.
        return

    def bind_listener(self):
        for port in range(self.policy["proxy_port_min"], self.policy["proxy_port_max"] + 1):
            try:
                self.listener.bind((self.policy["proxy_host"], port))
                return
            except OSError:
                continue
        raise RuntimeError("no available approved proxy port")

    async def start_runner(self):
        from datetime import UTC, datetime, timedelta
        output = max(self.gateway.provider.output_tokens, self.gateway.provider.compaction_output)
        compaction = compaction_settings(self.gateway.model_window, self.claim.get("context", {}).get("limits", {}), output)
        manifest = {"schema_version": 2, "task_id": self.launch_task_id, "run_id": self.claim["run_id"],
            "session_id": self.session_id, "process_id": self.process_id, "process_epoch": self.process_epoch,
            "profile": "pi", "model": self.model, "proxy_url": self.gateway_url,
            "process_token": self.gateway.process_token, "model_context_window": self.gateway.model_window,
            "output_tokens": output,
            "soft_input_tokens": self.gateway.model_window - compaction["reserveTokens"],
            "keep_recent_tokens": compaction["keepRecentTokens"],
            "expires_at": (datetime.now(UTC) + timedelta(hours=23)).isoformat()}
        validate_manifest(manifest, self.launch_task_id, self.process_id, self.policy)
        MANIFESTS.mkdir(parents=True, mode=0o700, exist_ok=True)
        self.manifest_path = MANIFESTS / (self.process_id + ".json")
        with self.manifest_path.open("x") as output:
            os.chmod(self.manifest_path, 0o600)
            json.dump(manifest, output)
            output.flush()
            os.fsync(output.fileno())
        self.process = await asyncio.create_subprocess_exec("/usr/bin/sudo", "-n", LAUNCHER,
            "start", self.launch_task_id, self.process_id,
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            limit=MAX_EVENT + 1, env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"}, start_new_session=True)
        self.readers = [asyncio.create_task(self._stdout()), asyncio.create_task(self._stderr())]

    async def stop_runner(self):
        self.gateway.revoke()
        await self.gateway.processes.close_run(self.gateway.context)
        if not self.process and not self.manifest_path:
            self.stopped_verified = True
            return True
        if self.process and self.process.returncode is None:
            for operation in ("clear_queue", "abort"):
                try:
                    await asyncio.wait_for(self.command(operation), 1)
                except (OSError, RuntimeError, TimeoutError):
                    pass
        if not verify_stop_marker(self.launch_task_id, self.process_id):
            stop = await asyncio.create_subprocess_exec("/usr/bin/sudo", "-n", LAUNCHER, "stop",
                self.launch_task_id, self.process_id, stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL, env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"})
            try:
                await asyncio.wait_for(stop.wait(), 25)
            except TimeoutError:
                self.stopped_verified = False
                return False
        if self.process:
            try:
                await asyncio.wait_for(self.process.wait(), 5)
            except TimeoutError:
                self.stopped_verified = False
                return False
        for reader in self.readers:
            reader.cancel()
        await asyncio.gather(*self.readers, return_exceptions=True)
        self.stopped_verified = verify_stop_marker(self.launch_task_id, self.process_id)
        return self.stopped_verified
