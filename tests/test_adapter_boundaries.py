"""Alternate Harness logs and execution worlds retain the host's task rules."""
import asyncio
import io
import json
import os
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import ClassVar

import httpx
import pytest
from sqlalchemy import select, update
from test_pi_bridge import compaction_session as compaction_fixture
from test_runtime_recovery import (
    bound_worker,
    expire,
    prove_stop,
    task_row,
)
from test_runtime_recovery import (
    recovery as recovery_fixture,
)
from test_runtime_recovery import (
    runtime as runtime_fixture,
)

from qqbot.app import create_app
from qqbot.capabilities.types import Action, Capability
from qqbot.config import Settings
from qqbot.contracts.harness import resolve_harness
from qqbot.domain.tasks import HarnessCapabilities
from qqbot.harness.gateway import RunGateway
from qqbot.runtime.scheduler import Scheduler
from qqbot.storage.agent_schema import leader_inbox
from qqbot.storage.task_schema import runs, tasks

runtime = runtime_fixture
recovery = recovery_fixture
compaction_session = compaction_fixture


class AlternateHarness:
    binding: ClassVar[dict] = {"harness": "alternate", "compression": "checkpoint", "contract_version": "test-1"}
    capabilities = HarnessCapabilities(append_input=True, interactive_questions=True)
    projection_version = "alternate-original-v1"

    def create(self, args, claim, client, process_id, session_id, capacity):
        self.created = (args, claim, client, process_id, session_id, capacity)
        return SimpleNamespace()

    def session_path(self, data_root, run, process):
        return run["session"]

    def recovery_snapshot(self, path, *, run_id):
        # This JSON object deliberately uses a different format from Pi JSONL.
        checkpoint = json.loads(Path(path).read_text())
        assert checkpoint["completed"]["run_id"] == run_id
        return {"exists": True, "session_revision": "alternate-checkpoint", "pending_tools": (),
            "consumed_input_ids": checkpoint["completed"]["input_ids"],
            "boundaries": [checkpoint["completed"]], "yielded": False}


class AlternateProcesses:
    def register(self, registry):
        async def start(arguments, context):
            self.started = (arguments, context)
            return {"process_ref": "remote-process"}
        registry.register(Capability("process.start", "Remote test process"), start,
            lambda arguments, context: Action("workspace.process.start", ("workspace:" + context.workspace,), "write"))

    def active_run(self, context):
        return False

    async def close_run(self, context):
        self.closed = context


class AlternateExecution:
    world_id = "remote-test"

    def __init__(self):
        self.files = {}
        self.processes = AlternateProcesses()

    def workspace(self, data_root, task_id, workspace_ref, bindings, *, create=False, strict=True):
        return "/remote-test/" + task_id

    def create_process_service(self):
        return self.processes

    def create_git_service(self, credentials):
        world = self
        class Git:
            def register(self, registry):
                async def execute(arguments, context):
                    world.git_context = context
                    return {"remote": True}
                registry.register(Capability("git.transport", "Remote test git"), execute,
                    lambda arguments, context: Action("git.clone", ("workspace:" + context.workspace,), "write"))
        return Git()

    def create_job_runtime(self, core, data_root):
        self.job_runtime = SimpleNamespace()
        return self.job_runtime

    async def read_file(self, context, path, offset, limit):
        assert context.workspace.startswith("/remote-test/")
        return self.files[path][offset:offset + limit]

    async def write_file(self, context, path, content):
        assert context.workspace.startswith("/remote-test/")
        if path in self.files:
            raise FileExistsError(path)
        self.files[path] = content

    def export_artifacts(self, workspace, archive, paths):
        self.exported = (workspace, archive, paths)
        return []

    def snapshot_file(self, workspace, path, media_type):
        self.snapshot = (workspace, path, media_type)
        content = self.files[path]
        return io.BytesIO(content), len(content), "text/plain"


async def test_custom_adapters_reach_normal_app_and_scheduler_entry_points(tmp_path, clock):
    harness, execution = AlternateHarness(), AlternateExecution()
    settings = Settings(database=str(tmp_path / "adapter.db"), model={"model": "offline", "endpoint": "https://model.invalid"})
    app = create_app(settings, clock=clock, run_background=False,
        harnesses={"alternate": harness}, default_harness="alternate", execution=execution)
    core = app.state.core
    try:
        binding = core.sessions.binding()
        assert binding["harness"] == "alternate" and binding["compression"] == "checkpoint"
        assert binding["execution_world"] == execution.world_id
        scheduler = Scheduler(core, core.leader)
        claim = {"binding": binding, "task_id": "worker"}
        runner = scheduler.pool.factory(claim, "process", "session")
        assert harness.created[0].execution is execution
        assert harness.created[1] is claim
        assert runner.archive == tmp_path / "artifacts"
        assert core.job_runtime is execution.job_runtime
        assert resolve_harness(core, {"harness": "pi", "compression": "native"}).binding["harness"] == "pi"
        with pytest.raises(ValueError, match="unavailable"):
            scheduler.pool.factory({"binding": {**binding, "compression": "other"}}, "p", "s")
        harness.capabilities = harness.capabilities.model_copy(update={"cancel": False})
        with pytest.raises(ValueError, match="cancel"):
            scheduler.pool.factory(claim, "p", "s")
        await scheduler.close()
    finally:
        await core.stop()
        core.db.close()


def test_different_log_format_recovers_same_worker_result_once(recovery):
    claim = bound_worker(recovery)
    harness = AlternateHarness()
    recovery.core.harnesses = {"alternate": harness}
    claim["binding"] = harness.binding
    proof = {"kind": "turn_finished", "run_id": claim["run_id"], "session_id": claim["session_id"],
        "session_revision": "alternate-checkpoint", "input_ids": claim["context"]["source_input_ids"],
        "inflight": 0, "tools_paired": True, "result": {"status": "completed", "summary": "Alternate result"}}
    claim["native_path"].write_text(json.dumps({"completed": proof}))
    with recovery.runtime.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.id == claim["task_id"]).values(binding_json=json.dumps(harness.binding)))
        conn.execute(update(runs).where(runs.c.id == claim["run_id"]).values(request_json="{}"))
    prove_stop(recovery)
    expire(recovery)
    assert recovery.service.recover_workers()[0]["state"] == "SUCCEEDED"
    assert json.loads(task_row(recovery.runtime, claim["task_id"])["result_json"])["summary"] == "Alternate result"
    assert recovery.service.recover_workers() == []
    with recovery.runtime.db.engine.connect() as conn:
        assert len(conn.execute(select(runs)).all()) == 1
        assert len(conn.execute(select(leader_inbox).where(leader_inbox.c.handled_at.is_(None))).all()) == 1


async def test_remote_file_process_git_and_qq_snapshot_use_injected_world(tmp_path):
    world = AlternateExecution()
    calls, uploaded = [], []
    async def host(operation, payload):
        calls.append((operation, payload))
        if operation == "authorize":
            return {"allowed": True}
        if operation == "ledger":
            return {"state": "PREPARED"} if payload["operation"] == "prepare" else {"changed": True}
        raise AssertionError(operation)
    async def upload(metadata, chunks):
        uploaded.append((metadata, b"".join([chunk async for chunk in chunks])))
        return {"delivery_id": "remote-file"}
    gateway = RunGateway("/remote-test/task", "offline", "", "https://model.invalid", call=host,
        content_root=tmp_path / "content", execution=world, upload=upload)
    gateway.bind({"task_id": "task", "run_id": "run", "fencing_token": 1, "goal": "Work remotely",
        "permission_snapshot": {"principal": {}}, "goal_spec": {}, "context": {}})
    result = await gateway.tools.invoke("workspace.write", {"path": "outputs/result.txt", "text": "remote"}, gateway.context)
    assert result.status == "verified" and world.files["outputs/result.txt"] == b"remote"
    assert gateway.artifacts == ["outputs/result.txt"]
    read = await gateway.tools.invoke("workspace.read", {"path": "outputs/result.txt"}, gateway.context)
    assert read.data == {"text": "remote", "next_offset": None}
    await gateway.registry.entries["process.start"][1]({"argv": ["remote-command"]}, gateway.context)
    assert world.processes.started[1] == gateway.context
    await gateway.registry.entries["git.transport"][1]({"operation": "clone"}, gateway.context)
    assert world.git_context == gateway.context
    sent = await gateway.registry.entries["qq.file.send"][1]({"request_id": "send", "path": "outputs/result.txt",
                                                           "media_type": "file"}, gateway.context)
    assert sent["delivery_id"] == "remote-file" and uploaded[0][1] == b"remote"
    assert world.snapshot[0] == Path("/remote-test/task")
    gateway.revoke()
    with pytest.raises(PermissionError, match="revoked"):
        await gateway.tools.invoke("workspace.read", {"path": "outputs/result.txt"}, gateway.context)


async def test_rework_handoff_reaches_locked_pi_input_and_records_turn_purpose(compaction_session):
    state = compaction_session
    state.gateway.claim["context"]["input_refs"] = [{"task_id": "previous-worker", "kind": "rework_of",
        "result": {"summary": "REWORK_INPUT_6a21", "artifacts": [{"relative_path": "outputs/previous.txt"}]}}]
    result = await state.runner.run("Continue the referenced work", keep_alive=True)
    assert result.summary
    messages = state.requests[0]["payload"]["messages"]
    assert any("REWORK_INPUT_6a21" in str(message) and "outputs/previous.txt" in str(message) for message in messages)
    entries = [json.loads(line) for line in state.runner.session_path.read_text().splitlines()]
    turn = next(entry["data"] for entry in entries if entry.get("customType") == "elephant-run")
    assert turn["run_id"] == state.claim["run_id"] and turn["role"] == "worker" and turn["purpose"] == "work"


async def test_cancelled_gateway_write_drains_thread_before_releasing_request(tmp_path, monkeypatch):
    entered, release, finished = asyncio.Event(), threading.Event(), threading.Event()
    loop = asyncio.get_running_loop()
    original_fsync = os.fsync
    def blocked_fsync(fd):
        loop.call_soon_threadsafe(entered.set)
        try:
            assert release.wait(5), "test write was not released"
            original_fsync(fd)
        finally:
            finished.set()
    monkeypatch.setattr(os, "fsync", blocked_fsync)
    transitions = []
    async def host(operation, payload):
        if operation == "authorize":
            return {"allowed": True}
        if operation == "reserve-budget":
            return {"accepted": True}
        if operation == "ledger":
            if payload["operation"] == "prepare":
                return {"state": "PREPARED"}
            transitions.append(payload["state"])
            if payload["state"] == "UNKNOWN":
                assert finished.is_set()
            return {"changed": True}
        raise AssertionError(operation)
    gateway = RunGateway(tmp_path, "offline", "", "https://model.invalid", call=host)
    gateway.bind({"task_id": "task", "run_id": "run", "fencing_token": 1, "goal": "Write a file",
        "permission_snapshot": {"principal": {}}, "goal_spec": {}, "context": {}})
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://gateway") as client:
        request = asyncio.create_task(client.post("/tools/capability_invoke",
            headers={"Authorization": "Bearer " + gateway.tool_token}, json={"call_id": "write",
                "arguments": {"name": "workspace.write", "arguments": {"path": "result.txt", "text": "complete"}}}))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            for _ in range(2):
                request.cancel()
                done, _ = await asyncio.wait({request}, timeout=0.05)
                assert not done and gateway.inflight == 1
                assert "UNKNOWN" not in transitions
        finally:
            release.set()
            outcome = await asyncio.gather(request, return_exceptions=True)
    assert isinstance(outcome[0], asyncio.CancelledError)
    assert finished.is_set() and gateway.inflight == 0
    assert (tmp_path / "result.txt").read_bytes() == b"complete"
    assert transitions == ["DISPATCHED", "UNKNOWN"]
