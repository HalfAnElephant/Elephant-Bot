"""Committed tasks.submit receipts survive a lost response at the native tool boundary."""
import asyncio
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import func, insert, select, update
from test_agent_runtime import runtime as agent_runtime
from test_pi_bridge import completion_sse

from elephant.adapters.pi.sessions import branch_entries, load_entries, unfinished_tools
from elephant.jobs.worker import GeneralRun
from elephant.providers import ModelCapacity
from elephant.runtime.scheduler import RuntimeClient
from elephant.storage.agent_schema import dialogue_inputs
from elephant.storage.task_schema import processes, runs, tasks

runtime = agent_runtime


def native_pending_submit(runtime, claim, arguments, *, tool_name="tasks_submit", stop_reason="toolUse",
                          run_sources=None):
    path = runtime.path / "leader-session.jsonl"
    executable = Path("node_modules/.bin/pi").resolve(strict=True)
    module = executable.parents[1] / "index.js"
    script = """
import {readFileSync} from 'node:fs';
const body = JSON.parse(readFileSync(0, 'utf8'));
const {SessionManager} = await import(body.module);
const session = SessionManager.open(body.path);
session.appendCustomEntry('elephant-run', {run_id: body.run, role: 'leader', purpose: 'user', source_event_ids: body.run_sources});
session.appendCustomEntry('elephant-source', {id: body.source, kind: 'user'});
session.appendMessage({role: 'user', content: [{type: 'text', text: '[[elephant-input:' + body.source + ']]\\n检查项目'}], timestamp: Date.now()});
session.appendCustomEntry('elephant-consumed', {input_ids: [body.source], run_id: body.run});
session.appendMessage({role: 'assistant', content: [{type: 'toolCall', id: 'native-submit-call', name: body.tool_name, arguments: body.arguments}],
  api: 'openai-completions', provider: 'qqbot', model: 'offline', stopReason: body.stop_reason, timestamp: Date.now(),
  usage: {input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2, cost: {input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0}}});
"""
    subprocess.run(["node", "--input-type=module", "-e", script], input=json.dumps({"module": module.as_uri(),
        "path": str(path), "source": claim["source_ids"][0], "run": claim["run_id"],
        "arguments": arguments, "tool_name": tool_name, "stop_reason": stop_reason,
        "run_sources": claim["source_ids"] if run_sources is None else run_sources}),
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


@pytest.mark.parametrize("crash_after_edit", [False, True])
def test_aborted_partial_submit_releases_and_requeues_without_faking_tool_result(runtime, monkeypatch, crash_after_edit):
    runtime.sessions.enqueue(runtime.event, runtime.access)
    old = runtime.sessions.claim("leader")
    path = native_pending_submit(runtime, old, {}, stop_reason="aborted")
    original = path.read_bytes()
    runtime.clock.advance(32)
    if crash_after_edit:
        with monkeypatch.context() as patch:
            def interrupted(*_args, **_kwargs):
                raise RuntimeError("interrupted before database commit")
            patch.setattr(runtime.leader, "_account_turn", interrupted)
            with pytest.raises(RuntimeError, match="before database commit"):
                runtime.leader.recover_attempt(old["run_id"])
        assert runtime.sessions.claim("replacement") is None
    assert runtime.leader.recover_attempt(old["run_id"])["state"] == "RECOVERED"
    assert path.read_bytes().startswith(original)
    entries = load_entries(path)
    edits = [entry for entry in entries if entry["type"] == "context_edit"]
    assert len(edits) == 1 and edits[0]["replacement"] is None
    assert not any(entry.get("message", {}).get("role") == "toolResult" for entry in entries)
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(tasks)).scalar_one() == 0
        context = json.loads(conn.execute(select(dialogue_inputs.c.context_json)).scalar_one())
        assert context["leader_turn"]["used"]["active_seconds"] == 30
        assert conn.execute(select(runs.c.slot_reserved)).scalar_one() == 0
    retry = runtime.sessions.claim("replacement")
    assert retry["source_ids"] == old["source_ids"]
    # Once recorded by the host, the tombstone does not obstruct later recovery of this session.
    with runtime.db.transaction() as conn:
        conn.execute(update(runs).where(runs.c.id == retry["run_id"]).values(session=str(path)))
    runtime.clock.advance(31)
    assert runtime.leader.recover_attempt(retry["run_id"])["state"] == "RECOVERED"
    assert load_entries(path) == entries


@pytest.mark.parametrize("blocker", ["tool_use", "committed", "wrong_sources"])
async def test_partial_submit_without_abort_proof_or_with_receipt_retains_slot(runtime, blocker):
    runtime.sessions.enqueue(runtime.event, runtime.access)
    old = runtime.sessions.claim("leader")
    if blocker == "committed":
        response = await submit_tool(runtime, old, {"goal": "检查项目", "children": [
            {"goal": "独立检查", "delegation_key": "primary"}]})
        assert response.status_code == 200
    path = native_pending_submit(runtime, old, {}, stop_reason="toolUse" if blocker == "tool_use" else "aborted",
                                 run_sources=["wrong-source"] if blocker == "wrong_sources" else None)
    before = path.read_bytes()
    runtime.clock.advance(31)
    assert runtime.leader.recover_attempt(old["run_id"])["state"] == "NEEDS_RECONCILIATION"
    assert runtime.sessions.claim("replacement") is None
    assert path.read_bytes() == before


async def test_leader_runtime_reads_do_not_wait_for_sqlite_writer(runtime):
    runtime.sessions.enqueue(runtime.event, runtime.access)
    claim = runtime.sessions.claim("leader")
    core = runtime.leader.core
    client = RuntimeClient(core, runtime.leader)
    request = {"worker_id": "leader", "run_id": claim["run_id"], "fencing_token": claim["fencing_token"]}
    with runtime.db.transaction():
        allowed = await client.post("/internal/tasks/authorize", json={**request, "payload": {"action": "model.request"}})
        control = await client.post("/internal/tasks/control", json=request)
        inputs = await client.post("/internal/tasks/inputs", json=request)
        assert allowed.status_code == 200 and allowed.json()["allowed"]
        assert control.status_code == 200 and control.json()["state"] == "RUNNING"
        assert inputs.status_code == 200 and inputs.json() == []
        core.allowed = lambda _: False
        denied = await client.post("/internal/tasks/authorize", json={**request, "payload": {"action": "model.request"}})
        control = await client.post("/internal/tasks/control", json=request)
        assert denied.status_code == 409 and control.json()["state"] == "CANCEL_REQUESTED"
        runtime.clock.advance(31)
        expired = await client.post("/internal/tasks/inputs", json=request)
        assert expired.status_code == 409


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


def failed_stop(runtime, claim):
    return runtime.leader.complete(claim["run_id"], claim["fencing_token"], "leader",
        {"status": "failed", "summary": "进程已停止", "agent_authored": False}, stopped=True, quiescent=False)


@pytest.mark.parametrize("crash_after_edit", [False, True])
def test_active_failed_stop_cleans_partial_submit_before_retry_without_new_pi_records(runtime, monkeypatch, crash_after_edit):
    runtime.sessions.enqueue(runtime.event, runtime.access)
    old = runtime.sessions.claim("leader")
    path = native_pending_submit(runtime, old, {}, stop_reason="aborted")
    original = path.read_bytes()
    if crash_after_edit:
        with monkeypatch.context() as patch:
            def interrupted(*args, **kwargs):
                raise RuntimeError("interrupted before database commit")
            patch.setattr(runtime.leader, "_account_turn", interrupted)
            with pytest.raises(RuntimeError, match="before database commit"):
                failed_stop(runtime, old)
        assert runtime.sessions.claim("replacement") is None
    assert failed_stop(runtime, old)["state"] == "FINISHED"
    entries = load_entries(path)
    assert path.read_bytes().startswith(original)
    assert len([entry for entry in entries if entry["type"] == "context_edit"]) == 1
    assert not any(entry.get("message", {}).get("role") == "toolResult" for entry in entries)
    retry = runtime.sessions.claim("leader")
    assert retry["session_id"] == old["session_id"] and retry["source_ids"] == old["source_ids"]
    with runtime.db.transaction() as conn:
        conn.execute(update(runs).where(runs.c.id == retry["run_id"]).values(session=str(path)))
    runtime.clock.advance(31)
    assert runtime.leader.recover_attempt(retry["run_id"])["state"] == "RECOVERED"
    assert load_entries(path) == entries


@pytest.mark.parametrize("blocker", ["tool_use", "committed_partial", "changed_committed", "wrong_sources"])
async def test_active_failed_stop_keeps_uncertain_submit_and_writer(runtime, blocker):
    runtime.sessions.enqueue(runtime.event, runtime.access)
    old = runtime.sessions.claim("leader")
    arguments = {}
    if blocker in {"committed_partial", "changed_committed"}:
        assert (await submit_tool(runtime, old, {"goal": "检查项目", "children": [
            {"goal": "独立检查", "delegation_key": "primary"}]})).status_code == 200
        if blocker == "changed_committed":
            arguments = {"goal": "另一个目标", "children": [{"goal": "独立检查", "delegation_key": "primary"}]}
    path = native_pending_submit(runtime, old, arguments,
        stop_reason="toolUse" if blocker == "tool_use" else "aborted",
        run_sources=["wrong"] if blocker == "wrong_sources" else None)
    before = path.read_bytes()
    assert failed_stop(runtime, old)["state"] == "NEEDS_RECONCILIATION"
    assert path.read_bytes() == before
    assert runtime.sessions.claim("replacement") is None
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(runs.c.slot_reserved).where(runs.c.id == old["run_id"])).scalar_one() == 1


async def test_active_failed_stop_restores_only_matching_committed_submit_receipt(runtime):
    runtime.sessions.enqueue(runtime.event, runtime.access)
    old = runtime.sessions.claim("leader")
    arguments = {"goal": "检查项目", "children": [{"goal": "独立检查", "delegation_key": "primary"}]}
    response = await submit_tool(runtime, old, arguments)
    path = native_pending_submit(runtime, old, arguments)
    assert failed_stop(runtime, old)["state"] == "FINISHED"
    entries = load_entries(path)
    assert not any(entry["type"] == "context_edit" for entry in entries)
    results = [entry["message"] for entry in entries if entry.get("message", {}).get("role") == "toolResult"]
    assert len(results) == 1 and not results[0]["isError"]
    assert json.loads(results[0]["content"][0]["text"])["task_id"] == response.json()["task_id"]


async def test_leader_renew_does_not_block_loop_under_real_sqlite_writer(runtime):
    runtime.sessions.enqueue(runtime.event, runtime.access)
    claim = runtime.sessions.claim("leader")
    client = RuntimeClient(runtime.leader.core, runtime.leader)
    request = {"worker_id": "leader", **{key: claim[key] for key in ("run_id", "fencing_token")}}
    loop = asyncio.get_running_loop()
    with runtime.db.transaction():
        started = loop.time()
        renewing = asyncio.create_task(client.post("/internal/tasks/renew", json=request))
        await asyncio.sleep(0.025)
        assert loop.time() - started < 0.5
        assert not renewing.done()
    assert (await renewing).status_code == 200
    stale = {**request, "fencing_token": claim["fencing_token"] + 1}
    assert (await client.post("/internal/tasks/renew", json=stale)).status_code == 409


async def test_model_authorization_uses_a_fresh_off_loop_check(runtime, monkeypatch):
    from threading import get_ident

    runtime.sessions.enqueue(runtime.event, runtime.access)
    claim = runtime.sessions.claim("leader")
    client = RuntimeClient(runtime.leader.core, runtime.leader)
    payload = {"worker_id": "leader", "run_id": claim["run_id"], "fencing_token": claim["fencing_token"],
        "payload": {"action": "model.request"}}
    loop_thread = get_ident()
    calls = []
    original = runtime.leader.authorize

    def authorize(conn, run, binding, action):
        assert get_ident() != loop_thread
        calls.append(action["action"])
        if len(calls) > 1:
            raise PermissionError("context revoked")
        return original(conn, run, binding, action)

    monkeypatch.setattr(runtime.leader, "authorize", authorize)
    assert (await client.post("/internal/tasks/authorize", json=payload)).json()["allowed"]
    assert (await client.post("/internal/tasks/authorize", json=payload)).status_code == 409
    assert calls == ["model.request", "model.request"]


async def test_native_failure_settlement_keeps_timer_and_sqlite_writer_available(runtime, monkeypatch):
    from threading import Event

    from elephant.adapters.pi.adapter import PiAdapter

    runtime.sessions.enqueue(runtime.event, runtime.access)
    claim = runtime.sessions.claim("leader")
    path = native_pending_submit(runtime, claim, {}, stop_reason="aborted")
    entered, release = Event(), Event()
    discard = PiAdapter.discard_aborted_submit

    def slow_discard(adapter, *args):
        entered.set()
        assert release.wait(3)
        return discard(adapter, *args)

    monkeypatch.setattr(PiAdapter, "discard_aborted_submit", slow_discard)
    client = RuntimeClient(runtime.leader.core, runtime.leader)
    payload = {"worker_id": "leader", **{key: claim[key] for key in ("run_id", "fencing_token")},
        "result": {"status": "failed", "summary": "进程已停止", "agent_authored": False},
        "stopped": True, "quiescent": False}
    start = asyncio.get_running_loop().time()
    settling = asyncio.create_task(client.post("/internal/tasks/result", json=payload))
    try:
        await asyncio.sleep(0.025)
        assert asyncio.get_running_loop().time() - start < 0.5
        assert await asyncio.to_thread(entered.wait, 2)

        def unrelated_writer():
            with runtime.db.transaction() as conn:
                assert conn.execute(select(runs.c.state).where(runs.c.id == claim["run_id"])).scalar_one() == "SETTLING"
        await asyncio.wait_for(asyncio.to_thread(unrelated_writer), 1)
        # Freeze also fences old tool requests and process rebinding during I/O.
        assert (await submit_tool(runtime, claim, {"goal": "检查项目", "children": [
            {"goal": "独立检查", "delegation_key": "primary"}]})).status_code == 409
        runtime.leader.core.recovery = SimpleNamespace(epoch="fixture")
        rebound = await client.post("/internal/tasks/bind-process", json={
            "worker_id": "leader", "run_id": claim["run_id"], "fencing_token": claim["fencing_token"],
            "payload": {"identity": {}}})
        assert rebound.status_code == 409
        assert runtime.sessions.claim("replacement") is None
        settling.cancel()
        await asyncio.sleep(0.025)
        assert not settling.done()
    finally:
        release.set()
        await asyncio.gather(settling, return_exceptions=True)
    assert settling.cancelled()
    assert len([entry for entry in load_entries(path) if entry["type"] == "context_edit"]) == 1
    # Cancellation drains native work; the durable result is idempotently replayable.
    assert (await client.post("/internal/tasks/result", json=payload)).json()["state"] == "FINISHED"


@pytest.mark.parametrize("rival", ["result", "recovery"])
async def test_native_settlement_serializes_other_runtime_after_lease_expiry(runtime, monkeypatch, rival):
    from threading import Event

    from elephant.runtime.leader import LeaderRuntime

    runtime.sessions.enqueue(runtime.event, runtime.access)
    claim = runtime.sessions.claim("leader")
    path = native_pending_submit(runtime, claim, {}, stop_reason="aborted")
    entered, release = Event(), Event()
    reconcile = runtime.leader._reconcile_stopped_tools

    def slow_reconcile(*args):
        entered.set()
        assert release.wait(3)
        return reconcile(*args)

    monkeypatch.setattr(runtime.leader, "_reconcile_stopped_tools", slow_reconcile)
    client = RuntimeClient(runtime.leader.core, runtime.leader)
    payload = {"worker_id": "leader", **{key: claim[key] for key in ("run_id", "fencing_token")},
        "result": {"status": "failed", "summary": "进程已停止", "agent_authored": False},
        "stopped": True, "quiescent": False}
    settling = asyncio.create_task(client.post("/internal/tasks/result", json=payload))
    other = LeaderRuntime(runtime.leader.core, runtime.sessions)
    waiting = None
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        if rival == "recovery":
            runtime.clock.advance(31)
            waiting = asyncio.create_task(asyncio.to_thread(other.recover_attempt, claim["run_id"]))
        else:
            waiting = asyncio.create_task(RuntimeClient(other.core, other).post("/internal/tasks/result", json=payload))
        await asyncio.sleep(0.05)
        assert not waiting.done()
        assert runtime.sessions.claim("replacement") is None
    finally:
        release.set()
        results = await asyncio.gather(settling, *([waiting] if waiting else []), return_exceptions=True)
    assert len([entry for entry in load_entries(path) if entry["type"] == "context_edit"]) == 1
    if rival == "recovery":
        assert results[0].status_code == 409
        assert results[1]["state"] == "RECOVERED"
    else:
        assert all(result.status_code == 200 and result.json()["state"] == "FINISHED" for result in results)
    assert runtime.sessions.claim("replacement") is not None


async def test_worker_preserves_spooled_failure_on_http_success_needing_reconciliation(runtime):
    from elephant.jobs.worker import Spool, Worker

    runtime.sessions.enqueue(runtime.event, runtime.access)
    claim = runtime.sessions.claim("leader")
    path = native_pending_submit(runtime, claim, {}, stop_reason="toolUse")
    native = path.read_bytes()
    client = RuntimeClient(runtime.leader.core, runtime.leader)
    worker = Worker(client, "leader", Spool(runtime.path / "spool"), lambda _: None)
    payload = {"worker_id": "leader", **{key: claim[key] for key in ("run_id", "fencing_token")},
        "result": {"status": "failed", "summary": "进程已停止", "agent_authored": False},
        "stopped": True, "quiescent": False}
    evidence = await worker.save_result(payload)
    original = evidence.read_bytes()
    assert await worker.deliver_result(payload, evidence) is False
    assert evidence.read_bytes() == original
    await worker.replay()
    assert evidence.read_bytes() == original and path.read_bytes() == native
    assert runtime.sessions.claim("replacement") is None
    with runtime.db.engine.connect() as conn:
        row = conn.execute(select(runs).where(runs.c.id == claim["run_id"])).mappings().one()
        assert row["slot_reserved"] == 1 and row["state"] == "NEEDS_RECONCILIATION"


@pytest.mark.parametrize("blocker", ["fence", "running_process", "epoch", "generation"])
async def test_native_settlement_rechecks_fencing_and_stop_before_edit(runtime, blocker):
    runtime.sessions.enqueue(runtime.event, runtime.access)
    claim = runtime.sessions.claim("leader")
    path = native_pending_submit(runtime, claim, {}, stop_reason="aborted")
    before = path.read_bytes()
    if blocker != "fence":
        with runtime.db.transaction() as conn:
            conn.execute(insert(processes).values(id="native-process", session_id=claim["session_id"],
                worker="leader", epoch=2 if blocker == "epoch" else 1,
                generation=2 if blocker == "generation" else 1,
                state="BUSY" if blocker == "running_process" else "STOPPED", identity_json="{}",
                last_used=runtime.clock.now().isoformat()))
            conn.execute(update(runs).where(runs.c.id == claim["run_id"]).values(
                process_id="native-process", process_epoch=1, binding_generation=1))
    payload = {"worker_id": "leader", "run_id": claim["run_id"],
        "fencing_token": claim["fencing_token"] + int(blocker == "fence"),
        "result": {"status": "failed", "summary": "进程已停止", "agent_authored": False},
        "stopped": True, "quiescent": False}
    response = await RuntimeClient(runtime.leader.core, runtime.leader).post("/internal/tasks/result", json=payload)
    assert response.status_code == 409 and path.read_bytes() == before
    with runtime.db.engine.connect() as conn:
        row = conn.execute(select(runs).where(runs.c.id == claim["run_id"])).mappings().one()
        assert row["slot_reserved"] == 1 and row["state"] == "RUNNING"
