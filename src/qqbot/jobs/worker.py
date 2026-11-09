"""Independent HTTP-only supervisor. No database handle is imported here."""

import argparse
import asyncio
import json
import os
import signal
import socket
import time
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

import httpx
import uvicorn

from qqbot.harness.artifacts import export_artifacts
from qqbot.harness.launcher import (
    LAUNCHER,
    MANIFESTS,
    WORKSPACES,
    production_policy,
    validate_manifest,
    verify_stop_marker,
)
from qqbot.harness.pi import MAX_EVENT, PiHarness
from qqbot.harness.research import ResearchCapabilities


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
    def __init__(self, client, worker_id, spool, harness_factory):
        self.client, self.worker_id, self.spool, self.harness_factory = (
            client,
            worker_id,
            spool,
            harness_factory,
        )

    async def call(self, operation, **payload):
        response = await self.client.post(
            "/internal/tasks/" + operation, json={"worker_id": self.worker_id, **payload}
        )
        response.raise_for_status()
        return response.json()

    async def deliver_result(self, payload, path):
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
        path.unlink()

    async def flush_events(self, run_id=None):
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
                path.unlink()

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
                        path.unlink()
                    continue
                raise

    async def once(self):
        await self.replay()
        if any(self.spool.directory.glob("*.json")):
            return False
        claim = await self.call("claim")
        if not claim:
            return False
        identity = {key: claim[key] for key in ("run_id", "fencing_token")}
        try:
            harness = self.harness_factory(claim)
        except (ValueError, RuntimeError, OSError):
            payload = {
                **identity,
                "result": {"status": "failed", "summary": "Runner configuration rejected before startup."},
                "stopped": True,
            }
            path = self.spool.save(payload)
            await self.deliver_result(payload, path)
            return True
        harness.event_sink = self.event_sink(identity)
        if getattr(harness, "research", None):
            harness.research.event_sink = harness.event_sink
        harness.event_sink({"type": "host_accepted"})
        execution = asyncio.create_task(harness.run(claim["goal"]))
        try:
            while not execution.done():
                done, _ = await asyncio.wait({execution}, timeout=10)
                if done:
                    break
                try:
                    lease = await self.call("renew", **identity)
                except (httpx.HTTPError, ValueError):
                    await harness.stop()
                    execution.cancel()
                    await asyncio.gather(execution, return_exceptions=True)
                    return False
                if hasattr(harness, "renewed"):
                    harness.renewed()
                if lease["state"] == "RUNNING":
                    await self.flush_events(claim["run_id"])
                if lease["state"] != "RUNNING":
                    await harness.stop()
                    execution.cancel()
                    await asyncio.gather(execution, return_exceptions=True)
                    await self.call("stopped" if harness.stopped_verified else "uncertain", **identity)
                    return True
            try:
                result = execution.result().model_dump(mode="json")
                if getattr(harness, "research", None):
                    result["evidence"] = harness.research.evidence
                    result["artifacts"] = export_artifacts(
                        harness.workspace, harness.archive, harness.research.artifacts
                    )
                    if not result["artifacts"] or not result["evidence"]:
                        result["status"] = "failed"
                        result["limitations"] = [
                            "Research acceptance failed: artifact and retrieval evidence are required."
                        ]
            except Exception as error:  # noqa: BLE001 - Persist a failed result at the execution boundary.
                summary = harness.failure_summary(error)
                result = {"status": "failed", "summary": summary}
                harness.event_sink({"type": "host_error", "error": summary})
            if not harness.stopped_verified:
                await self.flush_events(claim["run_id"])
                await self.call("uncertain", **identity)
                return False
            payload = {**identity, "result": result, "stopped": True}
            path = self.spool.save(payload)
            await self.flush_events(claim["run_id"])
            await self.deliver_result(payload, path)
            return True
        except asyncio.CancelledError:
            await harness.stop()
            execution.cancel()
            await asyncio.gather(execution, return_exceptions=True)
            try:
                if harness.stopped_verified:
                    lease = await self.call("renew", **identity)
                    if lease["state"] == "RUNNING":
                        payload = {
                            **identity,
                            "result": {
                                "status": "failed",
                                "summary": "Supervisor stopped before task completion.",
                            },
                            "stopped": True,
                        }
                        path = self.spool.save(payload)
                        await self.flush_events(claim["run_id"])
                        await self.deliver_result(payload, path)
                    else:
                        await self.flush_events(claim["run_id"])
                        await self.call("stopped", **identity)
                else:
                    await self.call("uncertain", **identity)
            except (httpx.HTTPError, OSError, RuntimeError):
                pass
            raise
        finally:
            await harness.stop()
            if not execution.done():
                execution.cancel()
            await asyncio.gather(execution, return_exceptions=True)


class ResearchRun(PiHarness):
    def __init__(self, args, claim, client):
        self.core_client, self.claim, self.worker_id = client, claim, args.worker_id
        self.lease_until = time.monotonic() + 25
        workspace = Path(args.workspace) / claim["run_id"]
        self.research_capabilities = ResearchCapabilities(
            workspace,
            args.model,
            os.environ.get("QQBOT_RUNNER_OPENAI_KEY", ""),
            upstream=getattr(args, "model_endpoint", "https://api.openai.com/v1/chat/completions"),
            active=lambda: time.monotonic() < self.lease_until,
            authority=self.authority,
        )
        self.server = None
        self.server_task = None
        self.listener = None
        self.archive = Path(args.archive)
        super().__init__(
            args.pi,
            workspace,
            provider="qqbot",
            model=args.model,
            research=self.research_capabilities,
            timeout=1800,
            session_id=claim["session_id"],
        )

    async def authority(self):
        try:
            response = await self.core_client.post(
                "/internal/tasks/renew",
                json={
                    "worker_id": self.worker_id,
                    "run_id": self.claim["run_id"],
                    "fencing_token": self.claim["fencing_token"],
                },
            )
            response.raise_for_status()
            if response.json()["state"] != "RUNNING":
                return False
            self.renewed()
            return True
        except (httpx.HTTPError, ValueError):
            return False

    def renewed(self):
        self.lease_until = time.monotonic() + 25

    async def start(self):
        self.listener = socket.socket()
        self.bind_listener()
        self.listener.listen(16)
        self.research_url = f"http://{self.listener.getsockname()[0]}:{self.listener.getsockname()[1]}"
        self.server = CapabilityServer(
            uvicorn.Config(self.research.app, log_level="warning", access_log=False)
        )
        self.server_task = asyncio.create_task(self.server.serve(sockets=[self.listener]))
        for _ in range(100):
            if self.server.started:
                break
            if self.server_task.done():
                await self.server_task
            await asyncio.sleep(0.01)
        if not self.server.started:
            raise RuntimeError("research capability service failed to start")
        await self.start_runner()

    def bind_listener(self):
        self.listener.bind(("127.0.0.1", 0))

    async def start_runner(self):
        await PiHarness.start(self)

    async def stop_runner(self):
        return await PiHarness.stop(self)

    async def stop(self):
        stopped = await self.stop_runner()
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
        return stopped


class SystemdResearchRun(ResearchRun):
    """Production path is gated on a root-owned, target-verified Linux policy."""

    def __init__(self, args, claim, client):
        self.policy = production_policy()
        if Path(args.pi).resolve() != Path("/opt/qqbot/node_modules/.bin/pi").resolve(strict=True):
            raise ValueError("production Pi runtime path is fixed")
        if Path(args.workspace).resolve() != WORKSPACES:
            raise ValueError("production workspace root is fixed")
        if claim["profile"] != "pi_research" or claim["session_id"] != claim["run_id"]:
            raise ValueError("unsupported production run identity")
        super().__init__(args, claim, client)
        self.manifest_path = None

    def bind_listener(self):
        for port in range(self.policy["proxy_port_min"], self.policy["proxy_port_max"] + 1):
            try:
                self.listener.bind((self.policy["proxy_host"], port))
                return
            except OSError:
                continue
        raise RuntimeError("no available approved proxy port")

    async def start_runner(self):
        manifest = {
            "schema_version": 1,
            "task_id": self.claim["task_id"],
            "run_id": self.claim["run_id"],
            "session_id": self.claim["session_id"],
            "profile": "pi_research",
            "model": self.model,
            "proxy_url": self.research_url,
            "model_token": self.research.model_token,
            "tool_token": self.research.tool_token,
            "expires_at": self.claim["deadline"],
        }
        validate_manifest(manifest, self.claim["task_id"], self.claim["run_id"], self.policy)
        MANIFESTS.mkdir(parents=True, mode=0o700, exist_ok=True)
        self.manifest_path = MANIFESTS / (self.claim["run_id"] + ".json")
        with self.manifest_path.open("x") as output:
            os.chmod(self.manifest_path, 0o600)
            json.dump(manifest, output)
            output.flush()
            os.fsync(output.fileno())
        self.process = await asyncio.create_subprocess_exec(
            "/usr/bin/sudo",
            "-n",
            LAUNCHER,
            "start",
            self.claim["task_id"],
            self.claim["run_id"],
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=MAX_EVENT + 1,
            env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"},
            start_new_session=True,
        )
        self.readers = [asyncio.create_task(self._stdout()), asyncio.create_task(self._stderr())]

    async def stop_runner(self):
        self.research.revoke()
        if not self.process and not self.manifest_path:
            self.stopped_verified = True
            return True
        if self.process and self.process.returncode is None:
            for operation in ("clear_queue", "abort"):
                try:
                    await asyncio.wait_for(self.command(operation), 1)
                except (OSError, RuntimeError, TimeoutError):
                    pass
        if not verify_stop_marker(self.claim["task_id"], self.claim["run_id"]):
            stop = await asyncio.create_subprocess_exec(
                "/usr/bin/sudo",
                "-n",
                LAUNCHER,
                "stop",
                self.claim["task_id"],
                self.claim["run_id"],
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
                env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"},
            )
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
        self.stopped_verified = verify_stop_marker(self.claim["task_id"], self.claim["run_id"])
        return self.stopped_verified


async def main_async(args):
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    loop.add_signal_handler(signal.SIGTERM, task.cancel)
    if args.runner_mode == "systemd":
        production_policy()
    token = Path(args.token_file).read_text().strip()
    transport = httpx.AsyncHTTPTransport(uds=args.socket)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://core", headers={"Authorization": "Bearer " + token}, timeout=5
    ) as client:
        worker = Worker(
            client,
            args.worker_id,
            Spool(args.spool),
            lambda claim: (SystemdResearchRun if args.runner_mode == "systemd" else ResearchRun)(
                args, claim, client
            ),
        )
        while True:
            try:
                worked = await worker.once()
            except (httpx.HTTPError, OSError):
                worked = False
            if not worked:
                await asyncio.sleep(3)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--socket", required=True)
    parser.add_argument("--token-file", required=True)
    parser.add_argument("--pi", required=True)
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--spool", required=True)
    parser.add_argument("--archive", required=True)
    parser.add_argument("--runner-mode", choices=["local", "systemd"], default="local")
    parser.add_argument("--worker-id", default="local-worker")
    parser.add_argument("--model", default="gpt-4o-mini")
    parser.add_argument("--model-endpoint", default="https://api.openai.com/v1/chat/completions")
    try:
        asyncio.run(main_async(parser.parse_args()))
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass


if __name__ == "__main__":
    main()
