"""Committed tasks.submit receipts survive a lost response at the native tool boundary."""
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import func, select, update
from test_agent_runtime import runtime as agent_runtime
from test_pi_bridge import completion_sse

from qqbot.adapters.pi.sessions import branch_entries, load_entries, unfinished_tools
from qqbot.jobs.worker import GeneralRun
from qqbot.providers import ModelCapacity
from qqbot.runtime.scheduler import RuntimeClient
from qqbot.storage.task_schema import runs, tasks

runtime = agent_runtime


def native_pending_submit(runtime, claim, arguments, *, tool_name="tasks_submit"):
    path = runtime.path / "leader-session.jsonl"
    executable = Path("node_modules/.bin/pi").resolve(strict=True)
    module = executable.parents[1] / "index.js"
    script = """
import {readFileSync} from 'node:fs';
const body = JSON.parse(readFileSync(0, 'utf8'));
const {SessionManager} = await import(body.module);
const session = SessionManager.open(body.path);
session.appendCustomEntry('elephant-source', {id: body.source, kind: 'user'});
session.appendMessage({role: 'user', content: [{type: 'text', text: '[[elephant-input:' + body.source + ']]\\n检查项目'}], timestamp: Date.now()});
session.appendCustomEntry('elephant-consumed', {input_ids: [body.source], run_id: body.run});
session.appendMessage({role: 'assistant', content: [{type: 'toolCall', id: 'native-submit-call', name: body.tool_name, arguments: body.arguments}],
  api: 'openai-completions', provider: 'qqbot', model: 'offline', stopReason: 'toolUse', timestamp: Date.now(),
  usage: {input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2, cost: {input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0}}});
"""
    subprocess.run(["node", "--input-type=module", "-e", script], input=json.dumps({"module": module.as_uri(),
        "path": str(path), "source": claim["source_ids"][0], "run": claim["run_id"],
        "arguments": arguments, "tool_name": tool_name}),
        capture_output=True, text=True, timeout=20, check=True)
    with runtime.db.transaction() as conn:
        conn.execute(update(runs).where(runs.c.id == claim["run_id"]).values(session=str(path)))
    assert unfinished_tools(path)[0]["id"] == "native-submit-call"
    return path


async def submit_tool(runtime, claim, arguments):
    # The bridge passes execute(id)'s native tool ID as call_id, unchanged.
    return await RuntimeClient(runtime.leader.core, runtime.leader).post("/internal/tasks/tasks", json={
        "worker_id": "leader", "run_id": claim["run_id"], "fencing_token": claim["fencing_token"],
        "payload": {"operation": "tasks.submit", "call_id": "native-submit-call", "arguments": arguments}})


@pytest.mark.parametrize("legacy_intent", [False, True])
async def test_committed_submit_lost_response_restores_native_receipt_before_releasing_writer(runtime, legacy_intent):
    runtime.sessions.enqueue(runtime.event, runtime.access)
    old = runtime.sessions.claim("leader")
    arguments = {"goal": "检查项目", "children": [{"goal": "独立检查", "delegation_key": "primary"}]}
    with pytest.raises(httpx.ReadTimeout):
        response = await submit_tool(runtime, old, arguments)
        assert response.status_code == 200
        original = response.json()
        raise httpx.ReadTimeout("Host committed before the response reached Pi")
    if legacy_intent:
        with runtime.db.transaction() as conn:
            context = json.loads(conn.execute(select(tasks.c.context_json).where(
                tasks.c.id == original["task_id"])).scalar_one())
            for child in context["dispatch_spec"]["children"]:
                child.pop("environment", None)
            conn.execute(update(tasks).where(tasks.c.id == original["task_id"]).values(context_json=json.dumps(context)))
    path = native_pending_submit(runtime, old, arguments)
    before = path.read_bytes()
    runtime.clock.advance(31)
    runtime.service.stop_verifier = lambda *_: False
    assert runtime.leader.recover_attempt(old["run_id"])["state"] == "NEEDS_RECONCILIATION"
    assert path.read_bytes() == before
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(runs.c.slot_reserved).where(runs.c.id == old["run_id"])).scalar_one() == 1
    runtime.service.stop_verifier = lambda *_: True
    assert runtime.leader.recover_attempt(old["run_id"])["state"] == "RECOVERED"
    assert unfinished_tools(path) == ()
    native = load_entries(path)
    assert path.read_bytes().startswith(before)
    assert native[0] == json.loads(before.splitlines()[0])
    results = [entry["message"] for entry in branch_entries(native) if entry.get("type") == "message"
               and entry["message"]["role"] == "toolResult"]
    assert len(results) == 1
    assert results[0]["toolCallId"] == "native-submit-call" and results[0]["isError"] is False
    assert json.loads(results[0]["content"][0]["text"])["task_id"] == original["task_id"]
    # Repeated recovery and an explicit identical replay keep the original dispatch.
    assert runtime.leader.recover_attempt(old["run_id"])["state"] == "RECOVERED"
    retry = runtime.sessions.claim("leader")
    assert retry["session_id"] == old["session_id"]
    assert retry["source_ids"] == old["source_ids"]
    replay = await submit_tool(runtime, retry, arguments)
    assert replay.status_code == 200 and replay.json()["task_id"] == original["task_id"]
    # A real locked Pi process can resume that same native file and settle normally.
    requests = []
    def upstream(request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, headers={"content-type": "text/event-stream"},
            text=completion_sse({"role": "assistant", "content": "已接受，任务正在独立执行。"}))
    runtime.leader.core.recovery = SimpleNamespace(epoch="fixture-epoch")
    args = SimpleNamespace(workspace=str(runtime.path / "pi"), archive=str(runtime.path / "archive"),
        pi=str(Path("node_modules/.bin/pi").resolve()), model="offline",
        model_endpoint="https://model.invalid/v1/chat/completions", worker_id="leader")
    runner = GeneralRun(args, retry, RuntimeClient(runtime.leader.core, runtime.leader), str(uuid4()),
                        retry["session_id"], ModelCapacity())
    runner.session_path = path
    runner.gateway.transport = httpx.MockTransport(upstream)
    try:
        await runner.bind(retry)
        result = await runner.run(retry["goal"], keep_alive=True)
        assert result.summary == "已接受，任务正在独立执行。"
        assert runner.quiescent_verified and runner.last_receipt["tools_paired"] is True
        assert len(requests) == 1
        tool_results = [message for message in requests[0]["messages"] if message.get("role") == "tool"]
        assert len(tool_results) == 1 and original["task_id"] in tool_results[0]["content"]
    finally:
        await runner.stop()
    worker = runtime.service.claim("only-worker")
    assert worker is not None
    assert runtime.service.claim("duplicate-worker") is None
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(tasks).where(tasks.c.kind == "goal")).scalar_one() == 1
        assert conn.execute(select(func.count()).select_from(runs).where(runs.c.role == "worker")).scalar_one() == 1
    assert len([entry for entry in load_entries(path) if entry.get("type") == "message"
                and entry["message"]["role"] == "toolResult"]) == 1


@pytest.mark.parametrize("committed", [False, True])
async def test_uncommitted_or_changed_submit_intent_retains_writer_without_native_success(runtime, committed):
    runtime.sessions.enqueue(runtime.event, runtime.access)
    old = runtime.sessions.claim("leader")
    arguments = {"goal": "检查项目", "children": [{"goal": "独立检查", "delegation_key": "primary"}]}
    if committed:
        assert (await submit_tool(runtime, old, arguments)).status_code == 200
    different = {**arguments, "goal": "另一个目标"} if committed else arguments
    path = native_pending_submit(runtime, old, different)
    before = path.read_bytes()
    runtime.clock.advance(31)
    assert runtime.leader.recover_attempt(old["run_id"])["state"] == "NEEDS_RECONCILIATION"
    assert path.read_bytes() == before and len(unfinished_tools(path)) == 1
    assert runtime.sessions.claim("replacement-leader") is None
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(runs.c.slot_reserved).where(runs.c.id == old["run_id"])).scalar_one() == 1
        assert conn.execute(select(func.count()).select_from(tasks).where(tasks.c.kind == "goal")).scalar_one() == int(committed)


def test_interrupted_direct_tool_releases_writer_with_recovery_receipt(runtime):
    runtime.sessions.enqueue(runtime.event, runtime.access)
    old = runtime.sessions.claim("leader")
    path = native_pending_submit(runtime, old, {"name": "github.issue.create", "arguments": {}},
                                 tool_name="capability_invoke")
    runtime.clock.advance(31)
    assert runtime.leader.recover_attempt(old["run_id"])["state"] == "RECOVERED"
    assert unfinished_tools(path) == ()
    replacement = runtime.sessions.claim("replacement-leader")
    assert replacement["task_id"] == old["task_id"]
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(runs.c.slot_reserved).where(runs.c.id == old["run_id"])).scalar_one() == 0
        assert conn.execute(select(func.count()).select_from(tasks)).scalar_one() == 0
