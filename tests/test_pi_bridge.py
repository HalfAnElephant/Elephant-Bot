"""Exercise the locked real pi extension with deterministic model SSE and no remote writes."""
import asyncio
import json
import socket
from pathlib import Path

import httpx
import pytest
import uvicorn

from elephant.harness.gateway import RunGateway
from elephant.harness.pi import PiRunnerAdapter
from elephant.jobs.worker import CapabilityServer


def completion_sse(message, usage=None):
    calls = message.get("tool_calls")
    if calls:
        message = {**message, "tool_calls": [{"index": index, **call} for index, call in enumerate(calls)]}
    event = {"id": "fixture", "object": "chat.completion.chunk", "model": "offline", "created": 1,
             "choices": [{"index": 0, "delta": message, "finish_reason": None}]}
    finish = {**event, "choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls" if calls else "stop"}],
              "usage": usage if usage is not None else {"prompt_tokens": 250, "completion_tokens": 15}}
    return "data: " + json.dumps(event) + "\n\ndata: " + json.dumps(finish) + "\n\ndata: [DONE]\n\n"


@pytest.mark.parametrize("wait_wrapped", [False, True])
@pytest.mark.parametrize("wait_first", [False, True])
async def test_locked_pi_blocks_every_member_of_mixed_wait_write_batch(tmp_path, wait_wrapped, wait_first):
    executable = Path(__file__).resolve().parents[1] / "node_modules/.bin/pi"
    if not executable.exists():
        pytest.skip("locked pi 1.5.0 package is not installed")
    requests, host_calls = [], []
    ask_name = "capability_invoke" if wait_wrapped else "task_ask_user"
    ask_arguments = {"text": "Which repository?", "checkpoint": "No external writes made"}
    if wait_wrapped:
        ask_arguments = {"name": "task.ask_user", "arguments": ask_arguments}
    wait = {"id": "fixture_wait", "type": "function", "function": {
        "name": ask_name, "arguments": json.dumps(ask_arguments)}}
    write = {"id": "fixture_write", "type": "function", "function": {
        "name": "capability_invoke", "arguments": json.dumps({"name": "workspace.write",
        "arguments": {"path": "forbidden.txt", "text": "must never execute"}})}}
    async def core(operation, payload):
        host_calls.append((operation, payload))
        if operation == "authorize":
            return {"allowed": True}
        if operation == "reserve-budget":
            return {"accepted": True, "remaining": 20}
        if operation == "usage":
            return {"recorded": True}
        raise AssertionError("Mixed wait/write reached Core operation " + operation)
    def upstream(request):
        requests.append(json.loads(request.content))
        if len(requests) == 1:
            message = {"role": "assistant", "content": None, "tool_calls": [wait, write] if wait_first else [write, wait]}
        else:
            message = {"role": "assistant", "content": "The mixed batch was blocked before any action."}
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, text=completion_sse(message))
    gateway = RunGateway(tmp_path, "offline", "", "https://model.invalid/v1/chat/completions",
                         call=core, transport=httpx.MockTransport(upstream))
    gateway.bind({"task_id": "task", "run_id": "run", "fencing_token": 1,
        "goal": "Ask a question and write a file", "permission_snapshot": {"principal": {"user_id": "user"}},
        "context": {"seed": ""}})
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(8)
    server = CapabilityServer(uvicorn.Config(gateway.app, log_level="warning", access_log=False))
    server_task = asyncio.create_task(server.serve(sockets=[listener]))
    runner = PiRunnerAdapter(executable, tmp_path, provider="qqbot", model="offline", timeout=20,
        gateway=gateway, gateway_url="http://127.0.0.1:" + str(listener.getsockname()[1]))
    try:
        async with asyncio.timeout(3):
            while not server.started:
                if server_task.done():
                    await server_task
                await asyncio.sleep(0.01)
        result = await runner.run("trusted goal")
        assert runner.quiescent_verified
        assert result.summary == "The mixed batch was blocked before any action."
        assert len(requests) == 2
        results = [message for message in requests[1]["messages"] if message.get("role") == "tool"]
        assert len(results) == 2
        assert all("Waiting tools must be the only call" in str(message["content"]) for message in results)
        assert not (tmp_path / "forbidden.txt").exists()
        assert not gateway.waiting and not gateway.effects
        assert all(operation not in {"ask", "ledger", "notify"} for operation, _ in host_calls)
        assert len([operation for operation, _ in host_calls if operation == "usage"]) == 2
    finally:
        await runner.stop()
        server.should_exit = True
        await asyncio.wait_for(server_task, 3)
        listener.close()


@pytest.fixture
async def compaction_session(tmp_path, request):
    from types import SimpleNamespace
    executable = Path(__file__).resolve().parents[1] / "node_modules/.bin/pi"
    if not executable.exists():
        pytest.skip("locked pi 1.5.0 package is not installed")
    options = getattr(request, "param", {})
    window = options.get("window", 32768)
    state = SimpleNamespace(requests=[], host_calls=[], events=[], http=[], first_prompt_tokens=100,
                            reject_next=False, request_gate=None, request_started=asyncio.Event())
    async def core(operation, payload):
        state.host_calls.append((gateway.context.run_id, operation, payload))
        if operation == "authorize":
            return {"allowed": True}
        if operation == "reserve-budget":
            return {"accepted": True, "remaining": 20}
        if operation in {"usage", "observation", "session-receipt"}:
            return {"recorded": True}
        raise AssertionError("unexpected host operation " + operation)
    async def upstream(request):
        payload = json.loads(request.content)
        reservation = next(call[2] for call in reversed(state.host_calls) if call[1] == "reserve-budget")
        state.requests.append({"run_id": gateway.context.run_id, "kind": reservation["kind"], "payload": payload})
        state.request_started.set()
        if state.request_gate:
            await state.request_gate.wait()
        if options.get("interrupted_tool") and len(state.requests) == 1:
            class InterruptedToolStream(httpx.AsyncByteStream):
                async def __aiter__(self):
                    event = {"id": "fixture", "object": "chat.completion.chunk", "model": "offline", "created": 1,
                        "choices": [{"index": 0, "delta": {"tool_calls": [{"index": 0,
                            "id": "interrupted-call", "type": "function", "function": {
                                "name": "capability_invoke", "arguments": "{"}}]}, "finish_reason": None}]}
                    yield ("data: " + json.dumps(event) + "\n\n").encode()
                    raise httpx.ReadError("fixture stream interrupted")
            return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=InterruptedToolStream())
        if state.reject_next and reservation["kind"] == "business":
            state.reject_next = False
            return httpx.Response(400, json={"error": {"code": "context_length_exceeded"}})
        if reservation["kind"] == "compaction":
            content = "Verified checkpoint. Earlier requested work is documented. No external effects occurred."
        elif len(state.requests) == 1:
            content = "Known verified previous result. " + "details " * 140
        else:
            content = "Continued after pi compaction."
        content = options.get("response_text", content)
        # Synthetic usage drives pi's soft threshold. It is not a measured provider token count.
        tokens = state.first_prompt_tokens if len(state.requests) == 1 else 100
        return httpx.Response(200, headers={"content-type": "text/event-stream"},
            text=completion_sse({"role": "assistant", "content": content},
                                {"prompt_tokens": tokens, "completion_tokens": 20}))
    gateway = RunGateway(tmp_path, "offline", "", "https://model.invalid/v1/chat/completions", call=core,
                         transport=httpx.MockTransport(upstream))
    from elephant.config import ContextConfig
    limits = ContextConfig().limits(window, 4096) if options.get("automatic") else {
        "soft_compaction_input_tokens": 2000, "keep_recent_tokens": 128, "compaction_output_tokens": 1000}
    claim = {"task_id": "compaction-task", "run_id": "first-run", "fencing_token": 1,
        "goal": "Retain this checked history for the next turn. " + "known history " * 80,
        "permission_snapshot": {"principal": {"user_id": "user"}},
        "context": {"key": "compaction-topic", "seed": "", "model_context_window": window, "limits": limits}}
    gateway.bind(claim)
    @gateway.app.middleware("http")
    async def capture_binding(request, call_next):
        packet = await request.json()
        state.http.append({"run_id": gateway.context.run_id, "path": request.url.path,
                           "authorization": request.headers.get("authorization"), "body": packet})
        return await call_next(request)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(8)
    server = CapabilityServer(uvicorn.Config(gateway.app, log_level="warning", access_log=False))
    server_task = asyncio.create_task(server.serve(sockets=[listener]))
    runner = PiRunnerAdapter(executable, tmp_path, provider="qqbot", model="offline", timeout=25,
        gateway=gateway, gateway_url="http://127.0.0.1:" + str(listener.getsockname()[1]))
    runner.event_sink = lambda event: state.events.append((gateway.context.run_id, event))
    state.runner, state.gateway, state.claim = runner, gateway, claim
    try:
        async with asyncio.timeout(3):
            while not server.started:
                if server_task.done():
                    await server_task
                await asyncio.sleep(0.01)
        await runner.start()
        yield state
    finally:
        if state.request_gate:
            state.request_gate.set()
        await runner.stop()
        server.should_exit = True
        await asyncio.wait_for(server_task, 3)
        listener.close()


async def test_locked_pi_rebind_precedes_auto_compaction_and_post_request_passes_gate(compaction_session):
    state = compaction_session
    runner, gateway = state.runner, state.gateway
    settings = json.loads((runner.workspace / "profile" / "settings.json").read_text())
    assert settings["compaction"]["reserveTokens"] == gateway.model_window - 2000
    assert settings["compaction"]["keepRecentTokens"] == 128
    # Save a high synthetic usage without post-run compaction, then trigger it on next-run preparation.
    state.first_prompt_tokens = 3500
    await runner.rpc("set_auto_compaction", enabled=False)
    await runner.run("initial goal", keep_alive=True)
    assert len(state.requests) == 1 and state.requests[0]["kind"] == "business"
    assert gateway.revoked and runner.quiescent_verified
    old_model, old_tool = gateway.model_token, gateway.tool_token
    await runner.rpc("set_auto_compaction", enabled=True)
    gateway.bind({**state.claim, "run_id": "second-run", "goal": "Continue the checked work."})
    result = await runner.run("next goal", keep_alive=True)
    assert result.summary == "Continued after pi compaction."
    assert runner.quiescent_verified
    second = [request for request in state.requests if request["run_id"] == "second-run"]
    assert second and second[0]["kind"] == "compaction"
    assert second[-1]["kind"] == "business"
    assert all(request["kind"] == "compaction" for request in second[:-1])
    assert 1 <= len(second[:-1]) <= 2
    assert all(request["payload"]["max_tokens"] == 1000 for request in second[:-1])
    assert "max_tokens" not in second[-1]["payload"]
    assert gateway.provider.max_bytes == 16 * 1024 * 1024
    assert len(json.dumps(second[-1]["payload"]).encode()) < gateway.provider.max_bytes
    assert any("Verified checkpoint" in str(message) for message in second[-1]["payload"]["messages"])
    http = [request for request in state.http if request["run_id"] == "second-run"]
    binding_index = next(index for index, request in enumerate(http) if request["path"] == "/binding")
    mode_index = next(index for index, request in enumerate(http)
                      if request["path"] == "/mode" and request["body"].get("kind") == "compaction")
    model_index = next(index for index, request in enumerate(http) if request["path"] == "/v1/chat/completions")
    assert binding_index < mode_index < model_index
    assert http[mode_index]["authorization"] == "Bearer " + gateway.tool_token
    assert all(request["authorization"] == "Bearer " + gateway.model_token for request in http
               if request["path"] == "/v1/chat/completions")
    assert all(request["authorization"] not in {"Bearer " + old_tool, "Bearer " + old_model} for request in http)
    assert any(run == "second-run" and event.get("type") == "compaction_end" and not event.get("aborted")
               and event.get("result") for run, event in state.events)
    assert gateway.provider.compaction_count == len(second[:-1])


@pytest.mark.parametrize("compaction_session", [{"interrupted_tool": True}], indirect=True)
async def test_locked_pi_retry_discards_partial_tool_before_durable_receipt(compaction_session):
    state = compaction_session
    state.gateway.claim["session_id"] = "retry-session"
    result = await state.runner.run("Finish after a transient interrupted tool stream", keep_alive=True)
    assert result.summary == "Continued after pi compaction."
    assert len(state.requests) == 2
    entries = [json.loads(line) for line in state.runner.session_path.read_text().splitlines()]
    partial = next(entry for entry in entries if entry.get("type") == "message"
        and any(part.get("id") == "interrupted-call" for part in entry["message"].get("content", [])
                if isinstance(part, dict)))
    assert any(entry.get("type") == "context_edit" and entry["targetId"] == partial["id"]
               and entry["replacement"] is None for entry in entries)
    assert state.gateway.session_receipt["kind"] == "turn_finished"
    assert state.runner.quiescent_verified
    assert not any(event.get("type") == "host_error" for _, event in state.events)


async def test_locked_pi_busy_compact_rejected_and_idle_unauthorized_compact_explicit(compaction_session):
    state = compaction_session
    runner, gateway = state.runner, state.gateway
    state.request_gate = asyncio.Event()
    execution = asyncio.create_task(runner.run("busy goal", keep_alive=True))
    try:
        await asyncio.wait_for(state.request_started.wait(), 3)
        assert runner.busy and gateway.provider.active == 1
        with pytest.raises(RuntimeError, match="idle session"):
            await runner.rpc("compact")
        assert runner.busy and not execution.done() and runner.process.returncode is None
        assert not any(event.get("type") == "compaction_start" for _, event in state.events)
        state.request_gate.set()
        result = await asyncio.wait_for(execution, 10)
        assert result.summary.startswith("Known verified previous result.")
        assert not runner.busy and runner.quiescent_verified and gateway.revoked
        before = len(state.requests)
        with pytest.raises(RuntimeError, match="403|rejected|expired"):
            await runner.rpc("compact")
        assert len(state.requests) == before
        assert gateway.provider.active == 0
    finally:
        state.request_gate.set()
        if not execution.done():
            execution.cancel()
        await asyncio.gather(execution, return_exceptions=True)


@pytest.mark.parametrize("compaction_session", [{"window": 32768}, {"window": 1000000}], indirect=True)
async def test_locked_pi_recovers_once_from_real_upstream_overflow(compaction_session):
    state = compaction_session
    await state.runner.run("Create checked history", keep_alive=True)
    state.gateway.bind({**state.claim, "run_id": "overflow-run", "goal": "Continue the checked work."})
    state.reject_next = True
    result = await state.runner.run("continue", keep_alive=True)
    assert result.summary == "Continued after pi compaction."
    requests = [r for r in state.requests if r["run_id"] == "overflow-run"]
    assert requests[0]["kind"] == requests[-1]["kind"] == "business"
    assert len([r for r in requests if r["kind"] == "business"]) == 2
    assert 1 <= len([r for r in requests if r["kind"] == "compaction"]) <= 2
    assert state.runner.quiescent_verified


@pytest.mark.parametrize("compaction_session", [{"window": 1000000, "automatic": True}], indirect=True)
async def test_locked_pi_uses_one_million_window_for_large_normal_context(compaction_session):
    state = compaction_session
    state.gateway.revoke()
    state.gateway.bind({**state.claim, "run_id": "large-context", "goal": "Read the relevant context and respond.\n" + "相关上下文。" * 10000})
    result = await state.runner.run("Read the relevant context", keep_alive=True)
    assert result.status == "completed"
    assert len(state.requests) == 1 and state.requests[0]["kind"] == "business"
    assert len(json.dumps(state.requests[0]["payload"], ensure_ascii=False).encode()) > 16000
    settings = json.loads((state.runner.workspace / "profile/settings.json").read_text())
    assert settings["compaction"] == {"enabled": True, "reserveTokens": 100000, "keepRecentTokens": 200000}
    assert not any(e.get("type") == "compaction_start" for _, e in state.events)


@pytest.mark.parametrize("compaction_session", [{"response_text":
    '<｜｜DSML｜｜ calls>\n<｜｜DSML｜｜ invoke name="capability_invoke">unexecuted call</｜｜DSML｜｜ invoke></｜｜DSML｜｜ calls>'}], indirect=True)
async def test_locked_pi_reports_raw_tool_protocol_after_one_correction(compaction_session):
    state = compaction_session
    state.gateway.claim["session_id"] = state.runner.session_path.stem
    result = await state.runner.run("List PRs", keep_alive=True)
    assert result.status == "failed" and not result.agent_authored
    assert result.limitations == ("model_tool_protocol_recovery_failed",)
    assert "DSML" not in result.summary
    assert len(state.requests) == 2
    assert all(request["kind"] == "business" for request in state.requests)
    assert state.runner.quiescent_verified
    assert state.gateway.revoked
    receipts = [call[2] for call in state.host_calls if call[1] == "session-receipt"
        and call[2]["kind"] == "turn_finished"]
    assert len(receipts) == 2 and all(receipt["input_ids"] == [] for receipt in receipts)


async def test_locked_pi_protocol_correction_keeps_real_tool_results_and_same_session(compaction_session):
    from elephant.capabilities.types import Action, Capability

    state = compaction_session
    state.gateway.claim["session_id"] = state.runner.session_path.stem
    read_calls, requests, upstream_sessions = [], [], []
    state.gateway.registry.register(Capability("fixture.read", "Read fixture"),
        lambda arguments, context: read_calls.append(context.run_id) or {"verified_value": "ACTUAL_TOOL_RECEIPT"},
        lambda arguments, context: Action("fixture.read", ()))
    def upstream(request):
        requests.append(json.loads(request.content))
        upstream_sessions.append(request.headers["x-opencode-session"])
        message = {"role": "assistant", "content": "已读取 ACTUAL_TOOL_RECEIPT，处理完成。"}
        if len(requests) == 1:
            message = {"role": "assistant", "content": None, "tool_calls": [{"id": "actual-read", "type": "function",
                "function": {"name": "capability_invoke", "arguments": json.dumps({"name": "fixture.read", "arguments": {}})}}]}
        elif len(requests) == 2:
            message["content"] = '<｜｜DSML｜｜ calls><｜｜DSML｜｜ invoke name="capability_invoke">unexecuted call</｜｜DSML｜｜ invoke></｜｜DSML｜｜ calls>'
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, text=completion_sse(message))
    state.gateway.provider.transport = httpx.MockTransport(upstream)
    public = []
    state.runner.public_event_sink = public.append
    result = await state.runner.run("Read and report the verified fixture", keep_alive=True)
    assert result.status == "completed" and result.agent_authored
    assert result.summary == "已读取 ACTUAL_TOOL_RECEIPT，处理完成。"
    assert read_calls == [state.claim["run_id"]]
    assert len(requests) == 3 and len(set(upstream_sessions)) == 1
    assert any(message["role"] == "tool" and "ACTUAL_TOOL_RECEIPT" in str(message["content"])
        for message in requests[2]["messages"])
    assert any(message["role"] == "user" and "原生结构化工具接口" in str(message["content"])
        for message in requests[2]["messages"])
    assert result.observation_refs and state.runner.quiescent_verified
    assert state.gateway.session_receipt["summary"] == result.summary
    assert not state.runner.protocol_recovery_attempted
    assert not any("DSML" in event.get("text", "") for event in public)


async def test_failed_protocol_correction_preserves_refs_for_worker_failure(compaction_session):
    from elephant.jobs.worker import Worker

    state = compaction_session
    requests = []
    def upstream(request):
        requests.append(json.loads(request.content))
        if len(requests) == 1:
            return httpx.Response(200, headers={"content-type": "text/event-stream"}, text=completion_sse({
                "role": "assistant", "content": '<｜｜DSML｜｜ calls><｜｜DSML｜｜ invoke name="capability_invoke">unexecuted call</｜｜DSML｜｜ invoke></｜｜DSML｜｜ calls>'}))
        return httpx.Response(400, json={"error": {"message": "fixture correction rejected"}})
    state.gateway.provider.transport = httpx.MockTransport(upstream)
    state.gateway.observations["observation:verified-read"] = "{}"
    state.gateway.effects["verified-operation"] = "verified"
    with pytest.raises(RuntimeError) as failure:
        await state.runner.run("Continue completed work", keep_alive=True)
    assert state.runner.stopped_verified and state.runner.protocol_recovery_attempted
    worker = Worker(None, "fixture", None, None)
    result = worker.failure_result(state.claim, failure.value, state.runner)
    assert result["status"] == "failed" and result["agent_authored"] is False
    assert result["limitations"] == ["model_tool_protocol_recovery_failed"]
    assert result["observation_refs"] == ["observation:verified-read"]
    assert result["effect_refs"] == ["verified-operation"]
    assert "1 条操作已核验" in result["summary"]


async def test_locked_pi_corrects_rejected_outcome_without_repeating_work(compaction_session):
    state = compaction_session
    gateway = state.gateway
    gateway.claim["session_id"] = state.runner.session_path.stem
    gateway.observations["known-observation"] = {}
    gateway.effects["verified-operation"] = "verified"
    original_call = gateway.call
    validations, requests = [], []

    async def host(operation, payload):
        if operation == "validate-result-refs":
            validations.append(payload)
            if "missing-observation" in payload["observation_refs"]:
                httpx.Response(422, json={"detail": "success check not verified: observation"},
                    request=httpx.Request("POST", "http://runtime.local/validate-result-refs")).raise_for_status()
            return {"accepted": True}
        return await original_call(operation, payload)

    def upstream(request):
        requests.append(json.loads(request.content))
        if len(requests) < 3:
            reference = "missing-observation" if len(requests) == 1 else "known-observation"
            message = {"role": "assistant", "content": None, "tool_calls": [{
                "id": "outcome-" + str(len(requests)), "type": "function", "function": {
                    "name": "task_outcome", "arguments": json.dumps({"status": "completed",
                        "observation_refs": [reference], "effect_refs": []})}}]}
        else:
            message = {"role": "assistant", "content": "结算引用已修正，原操作已保留。"}
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, text=completion_sse(message))

    gateway.call = host
    gateway.provider.transport = httpx.MockTransport(upstream)
    result = await state.runner.run("Report the completed work", keep_alive=True)
    assert len(requests) == 3 and len(validations) == 2
    assert "success check not verified: observation" in json.dumps(requests[1]["messages"])
    assert result.status == "completed" and result.observation_refs == ("known-observation",)
    assert result.effect_refs == ()
    assert gateway.session_receipt["result"] == result.model_dump(mode="json")
    assert not any(operation == "ledger" for _run, operation, _payload in state.host_calls)


async def test_locked_pi_raw_protocol_preserves_committed_wait_without_correction(compaction_session):
    state = compaction_session
    gateway = state.gateway
    original_call = gateway.call
    async def host(operation, payload):
        if operation == "ask":
            return {"accepted": True, "waiting": True}
        return await original_call(operation, payload)
    gateway.call = host
    gateway.revoke()
    gateway.bind({**state.claim, "session_id": state.runner.session_path.stem,
        "context": {**state.claim["context"], "source_input_ids": ["fixture-input"]}})
    requests = []
    def upstream(request):
        requests.append(json.loads(request.content))
        message = {"role": "assistant", "content": '<｜｜DSML｜｜ calls><｜｜DSML｜｜ invoke name="capability_invoke">unexecuted call</｜｜DSML｜｜ invoke></｜｜DSML｜｜ calls>',
            "tool_calls": [{"id": "committed-wait", "type": "function", "function": {"name": "task_ask_user",
                "arguments": json.dumps({"text": "请提供仓库全名。", "checkpoint": "等待仓库身份"})}}]}
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, text=completion_sse(message))
    gateway.provider.transport = httpx.MockTransport(upstream)
    public = []
    state.runner.public_event_sink = public.append
    result = await state.runner.run("Ask for the repository", keep_alive=True)
    assert result.status == "waiting_input"
    assert "model_tool_protocol_recovery_failed" not in result.limitations
    assert "DSML" not in result.summary
    assert not any("DSML" in event.get("text", "") for event in public)
    assert len(requests) == 1
    assert gateway.waiting and gateway.wait_kind == "waiting_input"
    assert gateway.session_receipt["kind"] == "run_yielded"
    assert gateway.session_receipt["input_ids"] == ["fixture-input"]
    assert state.runner.quiescent_verified


async def test_runner_malformed_settled_text_preserves_existing_wait_boundary(compaction_session, monkeypatch):
    state = compaction_session
    gateway = state.gateway
    gateway.claim["session_id"] = state.runner.session_path.stem
    gateway.waiting, gateway.wait_kind = True, "waiting_input"
    gateway.session_receipt = {"kind": "run_yielded", "tools_paired": True, "inflight": 0}
    prompts = []
    original_command = state.runner.command
    async def command(kind, **values):
        if kind == "prompt":
            prompts.append(values)
            await state.runner.queue.put({"type": "message_end", "message": {"role": "assistant", "content": [
                {"type": "text", "text": '<｜｜DSML｜｜ calls><｜｜DSML｜｜ invoke name="capability_invoke">unexecuted call</｜｜DSML｜｜ invoke></｜｜DSML｜｜ calls>'}]}})
            await state.runner.queue.put({"type": "agent_settled"})
            return "wait-prompt"
        if kind == "get_state":
            await state.runner.queue.put({"type": "response", "id": "wait-state", "success": True,
                "data": {"sessionId": state.runner.session_path.stem, "sessionFile": str(state.runner.session_path)}})
            return "wait-state"
        return await original_command(kind, **values)
    monkeypatch.setattr(state.runner, "command", command)
    result = await state.runner.run("Resume the existing wait", keep_alive=True)
    assert result.status == "failed" and not result.agent_authored
    assert result.limitations == ("model_tool_protocol_recovery_failed",)
    assert "等待条件已保存" in result.summary
    assert len(prompts) == 1 and "protocol_recovery" not in json.loads(prompts[0]["message"])
    assert gateway.waiting and gateway.wait_kind == "waiting_input"
    assert gateway.session_receipt["kind"] == "run_yielded"
    assert state.runner.quiescent_verified


@pytest.mark.parametrize("prior_status", [None, "completed", "failed"])
async def test_locked_pi_protocol_correction_budget_exhaustion_is_durable_partial(compaction_session, prior_status):
    state = compaction_session
    gateway = state.gateway
    gateway.claim["session_id"] = state.runner.session_path.stem
    if prior_status:
        gateway.outcome = {"status": prior_status, "unresolved_items": ["previous-limitation"]}
    original_call = gateway.call
    business_calls = 0
    async def host(operation, payload):
        nonlocal business_calls
        if operation == "reserve-budget" and payload["kind"] == "business":
            business_calls += 1
            if business_calls == 2:
                raise ValueError("model budget exhausted")
        return await original_call(operation, payload)
    gateway.call = host
    requests = []
    def upstream(request):
        requests.append(json.loads(request.content))
        content = ("已有执行记录已保留，任务完成状态尚未确认。" if len(requests) > 1 else
            '<｜｜DSML｜｜ calls><｜｜DSML｜｜ invoke name="capability_invoke">unexecuted call</｜｜DSML｜｜ invoke></｜｜DSML｜｜ calls>')
        return httpx.Response(200, headers={"content-type": "text/event-stream"},
            text=completion_sse({"role": "assistant", "content": content}))
    gateway.provider.transport = httpx.MockTransport(upstream)
    result = await state.runner.run("Continue with the remaining execution budget", keep_alive=True)
    expected_status = "failed" if prior_status == "failed" else "partial"
    assert result.status == expected_status
    assert gateway.session_receipt["result"]["status"] == expected_status
    assert result.summary == gateway.session_receipt["result"]["summary"]
    assert ("model_tool_protocol_recovery_budget_exhausted" in result.limitations) == (expected_status == "partial")
    if prior_status:
        assert "previous-limitation" in result.limitations
    assert len(requests) == 2 and business_calls == 2
    assert requests[1]["tool_choice"] == "none"
    assert state.runner.quiescent_verified and gateway.revoked


@pytest.mark.parametrize("notification", [False, True])
async def test_locked_pi_receives_preferences_without_legacy_history_seed(compaction_session, notification):
    state = compaction_session
    state.gateway.revoke()
    state.gateway.bind({**state.claim, "run_id": "preference-run",
        "notification_status": "failed" if notification else None,
        "context": {**state.claim["context"], "preferences": "TRUSTED_PREFERENCE_SENTINEL",
            "seed": {"history": [{"role": "user", "text": "RETIRED_HISTORY_SEED"}]}}})
    await state.runner.run("Say hello" if not notification else "Explain the verified failure", keep_alive=True)
    system = "\n".join(message["content"] for message in state.requests[0]["payload"]["messages"] if message["role"] == "system")
    assert "个人助手大象" in system and "TRUSTED_PREFERENCE_SENTINEL" in system
    assert "RETIRED_HISTORY_SEED" not in json.dumps(state.requests[0]["payload"])
    assert state.requests[0]["kind"] == ("completion" if notification else "business")


async def test_locked_pi_refreshes_preferences_without_reseeding_history(compaction_session):
    state = compaction_session
    await state.runner.rpc("set_auto_compaction", enabled=False)
    state.gateway.revoke()
    for index, preference in enumerate(("PREFERENCE_FIRST_SENTINEL", "PREFERENCE_SECOND_SENTINEL")):
        state.gateway.bind({**state.claim, "run_id": "preference-run-" + str(index),
            "context": {**state.claim["context"], "preferences": preference}})
        await state.runner.run("Respond naturally", keep_alive=True)
        messages = state.requests[-1]["payload"]["messages"]
        system = "\n".join(str(message["content"]) for message in messages if message["role"] == "system")
        assert preference in system
        assert all(preference not in str(message["content"]) for message in messages if message["role"] == "user")
    assert state.runner.quiescent_verified


async def test_locked_pi_review_exposes_only_native_hermes_tools(compaction_session):
    state = compaction_session
    state.gateway.revoke()
    state.gateway.bind({**state.claim, "run_id": "review-run", "role": "skill_review", "purpose": "skill_review"})
    await state.runner.run("Review the completed reference work", keep_alive=True)
    assert {tool["function"]["name"] for tool in state.requests[0]["payload"]["tools"]} == {"skills_list", "skill_view", "skill_manage"}


@pytest.mark.parametrize("use_tools", [True, False])
async def test_locked_pi_reports_before_operation_and_answers_chat_directly(compaction_session, use_tools):
    from elephant.capabilities.types import Action, Capability

    state = compaction_session
    gateway = state.gateway
    gateway.revoke()
    gateway.bind({**state.claim, "context": {**state.claim["context"], "notify_before_tools": True}})
    order, requests = [], []
    original_call = gateway.call
    async def host(operation, payload):
        if operation == "notify":
            order.append("notice")
            assert payload["importance"] == "important"
            return {"accepted": True}
        return await original_call(operation, payload)
    gateway.call = host
    gateway.registry.register(Capability("fixture.read", "Read fixture"),
        lambda a, c: order.append("read") or {"verified": True}, lambda a, c: Action("fixture.read", ()))
    read = {"name": "capability_invoke", "arguments": {"name": "fixture.read", "arguments": {}}}
    notice = {"name": "task_notify", "arguments": {"text": "我查一下，稍等。"}}
    def upstream(request):
        requests.append(json.loads(request.content))
        index = len(requests) - 1
        message = {"role": "assistant", "content": "查好了。" if use_tools else "你好。"}
        if use_tools and index < 3:
            tool = [read, notice, read][index]
            message = {"role": "assistant", "content": None, "tool_calls": [{"id": "call_" + str(index),
                "type": "function", "function": {"name": tool["name"],
                    "arguments": json.dumps(tool["arguments"])}}]}
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, text=completion_sse(message))
    gateway.provider.transport = httpx.MockTransport(upstream)
    result = await state.runner.run("Query current data" if use_tools else "Hello", keep_alive=True)
    assert result.summary == ("查好了。" if use_tools else "你好。")
    assert order == (["notice", "read"] if use_tools else [])
    assert len(requests) == (4 if use_tools else 1)
    if use_tools:
        assert any("先单独调用 task_notify" in str(message) for message in requests[1]["messages"])


async def test_locked_pi_image_reaches_upstream_without_entering_session(compaction_session, monkeypatch):
    state = compaction_session
    data = "data:image/png;base64,iVBORw0KGgo="
    monkeypatch.setattr("elephant.harness.gateway.fetch_image", lambda url, limit: data)
    state.gateway.claim["context"]["images"] = [
        {"source": "image-event", "message_id": "42", "url": "https://cdn.example/signed-image"}]
    result = await state.runner.run("看看图片", keep_alive=True)
    assert result.summary
    messages = state.requests[0]["payload"]["messages"]
    assert any(part.get("type") == "image_url" and part["image_url"]["url"] == data
               for message in messages if isinstance(message.get("content"), list) for part in message["content"])
    session = state.runner.session_path.read_text()
    assert data not in session and "https://cdn.example/signed-image" not in session


@pytest.mark.parametrize(("tool_name", "arguments", "expected_status"), [
    ("tasks_wait", {"children": ["child"]}, "waiting_children"),
    ("task_ask_user", {"text": "请提供仓库全名。", "checkpoint": "等待仓库身份"}, "waiting_input"),
])
async def test_locked_pi_committed_wait_abort_yields_without_retry(compaction_session, tool_name, arguments, expected_status):
    state = compaction_session
    gateway = state.gateway
    original_call = gateway.call
    async def host(operation, payload):
        if operation == "session-receipt":
            return {"accepted": True}
        if operation == "tasks":
            assert payload["operation"] == "tasks.wait"
            return {"waiting": True}
        if operation == "ask":
            return {"accepted": True, "waiting": True}
        return await original_call(operation, payload)
    gateway.call = host
    gateway.revoke()
    gateway.bind({**state.claim, "session_id": state.runner.session_path.stem,
        "context": {**state.claim["context"], "source_input_ids": ["fixture-input"]}})
    requests = []
    def upstream(request):
        requests.append(json.loads(request.content))
        message = {"role": "assistant", "content": None, "tool_calls": [{"id": "wait_call", "type": "function",
            "function": {"name": tool_name, "arguments": json.dumps(arguments)}}]}
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, text=completion_sse(message))
    gateway.provider.transport = httpx.MockTransport(upstream)
    result = await state.runner.run("Wait for the committed condition", keep_alive=True)
    assert result.status == expected_status
    assert gateway.session_receipt["kind"] == "run_yielded"
    assert gateway.session_receipt["tools_paired"] is True
    assert state.runner.quiescent_verified
    messages = (await state.runner.rpc("get_messages"))["messages"]
    assert any(message.get("errorMessage") == "This operation was aborted" for message in messages)
    entries = [json.loads(line) for line in state.runner.session_path.read_text().splitlines()]
    yields = [entry for entry in entries if entry.get("customType") == "elephant-yield-boundary"]
    assert len(yields) == 1


async def test_locked_leader_sees_elapsed_time_and_notification_acceptance(compaction_session):
    state = compaction_session
    gateway = state.gateway
    original_call = gateway.call
    async def host(operation, payload):
        if operation == "notify":
            return {"accepted": True}
        return await original_call(operation, payload)
    gateway.call = host
    gateway.revoke()
    gateway.bind({**state.claim, "role": "leader", "purpose": "user"})
    calls = [
        ("capability_search", {"query": "github", "user_message": None}),
        ("task_notify", {"text": "收到，我正在确认仓库名称～"}),
        ("capability_search", {"query": "web", "user_message": None}),
    ]
    requests = []
    def upstream(request):
        requests.append(json.loads(request.content))
        index = len(requests) - 1
        message = {"role": "assistant", "content": "查好了。"}
        if index < len(calls):
            name, arguments = calls[index]
            message = {"role": "assistant", "content": None, "tool_calls": [{"id": f"feedback-{index}",
                "type": "function", "function": {"name": name, "arguments": json.dumps(arguments)}}]}
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, text=completion_sse(message))
    gateway.provider.transport = httpx.MockTransport(upstream)
    result = await state.runner.run("查一下仓库", keep_alive=True)
    assert result.summary == "查好了。"
    messages = (await state.runner.rpc("get_messages"))["messages"]
    feedback = [json.loads(part["text"])["host_interaction"] for message in messages
        if message.get("role") == "toolResult" for part in message["content"][1:]]
    assert [item["progress_notice_accepted"] for item in feedback] == [False, True, True]
    assert all(item["elapsed_turn_seconds"] >= 0 for item in feedback)
    assert all("max_tokens" not in request for request in requests)
    assert all(any("elapsed_turn_seconds" in json.dumps(message.get("content"))
        for message in request["messages"] if message["role"] == "tool") for request in requests[1:])


@pytest.mark.parametrize("scenario", ["message", "quiet", "notification_denied", "business_failure", "worker", "stale_notice", "string_null"])
async def test_locked_pi_business_call_keeps_explicit_communication_separate(compaction_session, scenario):
    from elephant.capabilities.types import Action, Capability

    state = compaction_session
    gateway = state.gateway
    original_call = gateway.call
    order = []
    revoked = {"binding": False}
    async def host(operation, payload):
        if operation == "authorize" and payload["action"] == "task.execute" and revoked["binding"]:
            revoked["binding"] = False
            return {"allowed": False}
        if operation == "notify":
            order.append("notice")
            if scenario in {"notification_denied", "stale_notice"}:
                revoked["binding"] = scenario == "stale_notice"
                raise PermissionError("notification.send denied")
            return {"accepted": True, "reply_messages_remaining": 3}
        return await original_call(operation, payload)
    gateway.call = host
    def read(arguments, context):
        order.append("read")
        assert arguments == {"value": "original"}
        if scenario == "business_failure":
            raise OSError("fixture query failed")
        return {"checked": True}
    gateway.registry.register(Capability("fixture.read", "Read fixture"), read, lambda a, c: Action("fixture.read", ()))
    gateway.revoke()
    gateway.bind({**state.claim, "role": "worker" if scenario == "worker" else "leader", "purpose": "user",
        "event_json": json.dumps({"reply_context": {"max_replies": 4}})})
    arguments = {"name": "fixture.read", "arguments": {"value": "original"}}
    if scenario != "worker":
        arguments["user_message"] = None if scenario == "quiet" else "收到，我来核实一下～"
        if scenario == "string_null":
            arguments["user_message"] = "null"
    requests = []
    def upstream(request):
        requests.append(json.loads(request.content))
        message = {"role": "assistant", "content": "查询受阻。" if scenario in {"business_failure", "stale_notice"} else "核实好了。"}
        if len(requests) == 1:
            message = {"role": "assistant", "content": None, "tool_calls": [{"id": "explicit-choice", "type": "function",
                "function": {"name": "capability_invoke", "arguments": json.dumps(arguments)}}]}
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, text=completion_sse(message))
    gateway.provider.transport = httpx.MockTransport(upstream)
    result = await state.runner.run("核实这个数据", keep_alive=True)
    assert result.summary == ("查询受阻。" if scenario in {"business_failure", "stale_notice"} else "核实好了。")
    schema = next(tool["function"]["parameters"] for tool in requests[0]["tools"] if tool["function"]["name"] == "capability_invoke")
    if scenario == "worker":
        assert "user_message" not in schema["properties"]
    else:
        assert "user_message" in schema["required"]
    expected_order = ["read"] if scenario in {"quiet", "worker", "string_null"} else ["notice", "read"]
    assert order == (["notice"] if scenario == "stale_notice" else expected_order)
    messages = (await state.runner.rpc("get_messages"))["messages"]
    tool_result = next(message for message in messages if message.get("role") == "toolResult")
    assert tool_result["isError"] is (scenario in {"business_failure", "stale_notice"})
    if scenario == "business_failure":
        assert '"accepted":true' in "".join(part.get("text", "") for part in tool_result["content"])
    elif scenario not in {"worker", "stale_notice"}:
        feedback = json.loads(tool_result["content"][1]["text"])["host_interaction"]
        assert feedback["progress_notice_accepted"] is (scenario == "message")
        assert feedback["reply_messages_remaining"] == (3 if scenario == "message" else 4)


async def test_locked_leader_requires_new_communication_choice_with_legacy_history(compaction_session):
    from elephant.capabilities.types import Action, Capability

    state = compaction_session
    gateway = state.gateway
    original_call = gateway.call
    order, requests = [], []
    async def host(operation, payload):
        if operation == "notify":
            order.append("notice")
            return {"accepted": True}
        return await original_call(operation, payload)
    gateway.call = host
    gateway.registry.register(Capability("fixture.read", "Read fixture"),
        lambda a, c: order.append("read") or {"checked": True}, lambda a, c: Action("fixture.read", ()))
    arguments = {"name": "fixture.read", "arguments": {}}
    def upstream(request):
        requests.append(json.loads(request.content))
        index = len(requests)
        if index in {1, 3, 4}:
            args = {**arguments, "user_message": "收到，我来核实一下～"} if index == 4 else arguments
            message = {"role": "assistant", "content": None, "tool_calls": [{"id": f"history-{index}",
                "type": "function", "function": {"name": "capability_invoke", "arguments": json.dumps(args)}}]}
        else:
            message = {"role": "assistant", "content": "核实好了。"}
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, text=completion_sse(message))
    gateway.provider.transport = httpx.MockTransport(upstream)
    await state.runner.run("之前的查询", keep_alive=True)
    assert order == ["read"]
    gateway.bind({**state.claim, "run_id": "updated-leader", "role": "leader", "purpose": "user"})
    gateway.provider.transport = httpx.MockTransport(upstream)
    await state.runner.run("继续核实", keep_alive=True)
    schema = next(tool["function"]["parameters"] for tool in requests[2]["tools"]
        if tool["function"]["name"] == "capability_invoke")
    assert "user_message" in schema["required"]
    assert any(message.get("tool_call_id") == "history-3" and "user_message" in str(message["content"])
        for message in requests[3]["messages"] if message.get("role") == "tool")
    messages = (await state.runner.rpc("get_messages"))["messages"]
    rejected = next(message for message in messages if message.get("toolCallId") == "history-3")
    assert rejected["isError"] is True
    assert order == ["read", "notice", "read"]


async def test_locked_pi_replaces_runtime_snapshots_without_erasing_log(compaction_session):
    state = compaction_session
    await state.runner.rpc("set_auto_compaction", enabled=False)
    for index in range(3):
        state.gateway.revoke()
        state.gateway.bind({**state.claim, "run_id": f"snapshot-run-{index}",
            "context": {**state.claim["context"], "active_tasks": [
                {"id": "task", "status": "running", "goal": "LARGE_RETIRED_GOAL_" + "x" * 5000}]}})
        await state.runner.run("Continue the verified history", keep_alive=True)
        payload = json.dumps(state.requests[-1]["payload"], ensure_ascii=False)
        assert payload.count('\\"active_tasks\\"') == 1
        assert "x" * 1000 not in payload
    entries = [json.loads(line) for line in state.runner.session_path.read_text().splitlines()]
    assert sum(entry.get("customType") == "elephant-runtime" for entry in entries) == 3
    assert sum(entry.get("type") == "context_edit" and entry.get("replacement") is None for entry in entries) >= 2


def test_native_compactor_rolls_complete_interactions_and_bounds_requests():
    import subprocess

    root = Path(__file__).resolve().parents[1]
    if not (root / "node_modules/.bin/pi").exists():
        pytest.skip("locked pi package is not installed")
    script = r'''
import assert from "node:assert/strict";
import {createJiti} from "jiti";
const jiti = createJiti(import.meta.url, {alias: {
  "@earendil-works/pi-ai": process.cwd() + "/node_modules/@earendil-works/pi-coding-agent/dist/core/extensions/pi-ai-extension-view.js"}});
const {nativeContextCompactor, interactionGroups, createBridge} = await jiti.import("./src/elephant/harness/pi_bridge.ts");
const messages = Array.from({length: 6}, (_, i) => ({role: "user", content: "history-" + i + "-" + "x".repeat(2200), timestamp: i}));
const assistant = {role: "assistant", content: [{type: "toolCall", id: "call", name: "read", arguments: {}}]};
const tool = {role: "toolResult", toolCallId: "call", content: [{type: "text", text: "checked"}]};
assert.deepEqual(interactionGroups([messages[0], assistant, tool]).map(group => group.length), [1, 2]);
const preparation = {messagesToSummarize: messages, turnPrefixMessages: [], isSplitTurn: false,
  firstKeptEntryId: "kept", tokensBefore: 9000, fileOps: {read: new Set(), edited: new Set(), written: new Set()}, settings: {keepRecentTokens: 100}};
let calls = 0;
const input = {preparation, reason: "force", signal: new AbortController().signal,
  model: {id: "fixture", provider: "qqbot", api: "openai-completions"},
  request: async (path, body) => {
    const text = JSON.stringify(body);
    if (path === "/context/budget") return {fits: text.length < 9500};
    assert(text.length < 9500);
    calls++;
    return {summary: "Verified rolling checkpoint " + calls};
  }};
const result = await nativeContextCompactor(input);
assert(calls >= 2 && calls <= 6);
assert.equal(result.firstKeptEntryId, "kept");
assert(result.summary.includes("Verified rolling checkpoint"));
await assert.rejects(nativeContextCompactor({...input, maxRequests: 1}), /request limit/);
await assert.rejects(nativeContextCompactor({...input, request: async () => ({fits: false})}), /complete message\/tool interaction/);
const abort = new AbortController(); abort.abort();
await assert.rejects(nativeContextCompactor({...input, signal: abort.signal}), /abort/i);
const hooks = new Map(), observed = [], packets = [];
let rejectCompactor = false;
const pi = {on: (event, handler) => hooks.set(event, handler), appendEntry: () => {},
  registerProvider: () => {}, registerTool: () => {}, setActiveTools: () => {}};
createBridge({compactor: async args => {
  observed.push(args.reason);
  assert.equal(args.preparation.firstKeptEntryId, "kept");
  if (rejectCompactor) throw new Error("injected failure");
  return {summary: "injected checkpoint", firstKeptEntryId: "kept", tokensBefore: 9000};
}})(pi);
globalThis.fetch = async (url, options) => {
  packets.push(JSON.parse(options.body));
  return new Response(JSON.stringify(String(url).endsWith("/binding") ? {generation: 1, goal: "continue",
    force_compaction_tokens: 10000, model_window: 32768, native_tools: [], model: "fixture"} :
    String(url).endsWith("/context/compaction") && packets.at(-1).phase === "begin" ? {attempt_id: "attempt"} : {}));
};
const ctx = {model: input.model, sessionManager: {getBranch: () => [], getLeafId: () => "branch",
  buildSessionProjection: () => ({entries: [{sourceEntry: {id: "kept"}, messages: [messages[0]]}]})}};
await hooks.get("input")({text: JSON.stringify({binding_id: "binding"})}, ctx);
for (const reason of ["manual", "threshold", "overflow"]) {
  const result = await hooks.get("session_before_compact")({preparation, reason, signal: input.signal}, ctx);
  assert.equal(result.compaction.summary, "injected checkpoint");
}
assert.deepEqual(observed, ["manual", "force", "overflow"]);
rejectCompactor = true;
const failure = await hooks.get("session_before_compact")({preparation, reason: "overflow", signal: input.signal}, ctx);
assert.equal(failure.cancel, true);
assert.equal(packets.at(-1).phase, "failed");
const {createHash} = await import("node:crypto");
const failedText = "[[elephant-input:failed]]\noriginal oversized input";
const entries = [{type: "custom", customType: "elephant-input-persisting", data: {
  sha256: createHash("sha256").update(failedText).digest("hex"), input_ids: ["failed"]}},
  {type: "message", id: "failed-user", message: {role: "user", content: failedText}}];
const edits = [], failedHooks = new Map();
createBridge()({...pi, on: (event, handler) => failedHooks.set(event, handler)});
globalThis.fetch = async () => new Response(JSON.stringify({generation: 2, goal: "independent",
  context_failed_input_ids: ["failed"], input_ids: ["fresh"], model_window: 32768, native_tools: [], model: "fixture"}));
await failedHooks.get("input")({text: JSON.stringify({binding_id: "new-binding"})}, {
  ...ctx, sessionManager: {...ctx.sessionManager, getBranch: () => entries,
    appendContextEdit: (id, replacement) => edits.push({id, replacement})}});
assert.deepEqual(edits, [{id: "failed-user", replacement: null}]);
const retryHooks = new Map(), branch = [], retryEdits = [];
let nextBinding;
createBridge()({...pi, on: (event, handler) => retryHooks.set(event, handler),
  appendEntry: (customType, data) => branch.push({type: "custom", customType, data})});
globalThis.fetch = async () => new Response(JSON.stringify(nextBinding));
const retryCtx = {...ctx, sessionManager: {...ctx.sessionManager, getBranch: () => branch,
  appendContextEdit: (id, replacement) => retryEdits.push({id, replacement})}};
async function bindUser(generation, inputId, failed = []) {
  nextBinding = {generation, session_id: "session", source_kind: "user", input_ids: [inputId],
    context_failed_input_ids: failed, goal: inputId === "failed" ? "OVERSIZED_SOURCE" : "independent",
    model_window: 32768, native_tools: [], model: "fixture"};
  return retryHooks.get("input")({text: JSON.stringify({binding_id: "binding-" + generation})}, retryCtx);
}
for (const generation of [1, 2]) {
  const {text} = await bindUser(generation, "failed");
  assert.deepEqual(branch.at(-1).data.input_ids, ["failed"]);
  // Include a retry written by the earlier bridge with an empty source manifest.
  if (generation === 2) branch.at(-1).data.input_ids = [];
  branch.push({type: "message", id: "attempt-" + generation, message: {role: "user", content: text}});
}
await bindUser(3, "new-user", ["failed"]);
assert.deepEqual(retryEdits.map(edit => edit.id), ["attempt-1", "attempt-2"]);
assert.deepEqual(retryHooks.get("context")({messages: branch.filter(entry => entry.message)
  .map(entry => entry.message)}).messages, []);

'''
    result = subprocess.run(["node", "--input-type=module", "-"], check=False, input=script, text=True,
                            cwd=root, capture_output=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("scenario", [
    "force", "overflow", "summary_failure", "overflow_failure", "summary_timeout", "maintenance", "tool_force", "rolling",
])
async def test_locked_leader_uses_native_compaction_commit_and_final_gate(compaction_session, scenario):
    state = compaction_session
    runner, gateway = state.runner, state.gateway
    await runner.rpc("set_auto_compaction", enabled=False)
    # Seed real native history while the previous Worker binding has no Leader ceiling.
    async def seed(request):
        repetitions = {"force": 3500, "summary_failure": 3500, "summary_timeout": 3500, "rolling": 3200, "tool_force": 800}
        return httpx.Response(200, headers={"content-type": "text/event-stream"},
            text=completion_sse({"role": "assistant", "content": "Earlier verified work. " * repetitions.get(scenario, 60)}))
    for index in range(3 if scenario == "rolling" else 1):
        if index:
            gateway.bind({**state.claim, "run_id": "seed-" + str(index)})
        gateway.provider.transport = httpx.MockTransport(seed)
        await runner.run("Preserve these original messages", keep_alive=True)
    await runner.rpc("set_auto_compaction", enabled=True)
    original_call = gateway.call
    phases, upstream, attempts = [], [], []
    async def core(operation, payload):
        if operation == "notify" and scenario == "tool_force":
            return {"accepted": True, "verified_tool_result": "result " * 3000}
        if operation == "context-compaction":
            phases.append(payload)
            if payload["phase"] == "begin":
                attempts.append("attempt-" + str(len(attempts)))
                return {"attempt_id": attempts[-1]}
            return {"accepted": True}
        return await original_call(operation, payload)
    gateway.call = core
    gateway.bind({**state.claim, "run_id": "enhanced-run", "role": "leader",
        "purpose": "compaction" if scenario == "maintenance" else "user",
        "context": {**state.claim["context"], "limits": {**state.claim["context"]["limits"],
            "force_compaction_tokens": 15000 if scenario == "tool_force" else 10000,
            "elastic_compaction_tokens": 14000 if scenario == "tool_force" else 9000}}})
    async def model(request):
        payload = json.loads(request.content)
        upstream.append(payload)
        if payload.get("stream") is False:
            if scenario in {"summary_failure", "overflow_failure"}:
                return httpx.Response(500, json={"error": {"message": "fixture summary unavailable"}})
            if scenario == "summary_timeout":
                raise httpx.ReadTimeout("fixture summary timeout", request=request)
            return httpx.Response(200, json={"choices": [{"message": {"role": "assistant",
                "content": "Verified native checkpoint. Original source history remains on disk."}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 100, "completion_tokens": 20}})
        if scenario == "tool_force" and sum(bool(item.get("stream")) for item in upstream) == 1:
            return httpx.Response(200, headers={"content-type": "text/event-stream"},
                text=completion_sse({"role": "assistant", "content": None, "tool_calls": [{"id": "large-result",
                    "type": "function", "function": {"name": "task_notify", "arguments": json.dumps({"text": "Checking."})}}]}))
        if scenario in {"overflow", "overflow_failure"} and sum(bool(item.get("stream")) for item in upstream) == 1:
            return httpx.Response(400, json={"error": {"code": "context_length_exceeded",
                "message": "maximum context length exceeded"}})
        return httpx.Response(200, headers={"content-type": "text/event-stream"},
            text=completion_sse({"role": "assistant", "content": "Continued with the committed checkpoint."}))
    gateway.provider.transport = httpx.MockTransport(model)
    failed = scenario in {"summary_failure", "overflow_failure", "summary_timeout"}
    if failed:
        with pytest.raises(RuntimeError):
            await runner.run("Continue with the authorized history", keep_alive=True)
    else:
        result = await runner.run("Continue with the authorized history", keep_alive=True)
        assert result.status == "completed"
    summaries = [payload for payload in upstream if payload.get("stream") is False]
    business = [payload for payload in upstream if payload.get("stream") is not False]
    assert summaries and len(attempts) == 1
    if failed:
        assert len(business) == int(scenario == "overflow_failure")
        assert not any(packet["phase"] == "saved" for packet in phases)
        assert next(packet for packet in phases if packet["phase"] == "failure")["retryable"]
    else:
        assert [packet["phase"] for packet in phases if packet["phase"] in {"begin", "commit", "saved"}] == ["begin", "commit", "saved"]
        assert len(business) == (0 if scenario == "maintenance" else 2 if scenario in {"overflow", "tool_force"} else 1)
        entries = [json.loads(line) for line in runner.session_path.read_text().splitlines()]
        saved = next(packet for packet in phases if packet["phase"] == "saved")
        native = next(entry for entry in entries if entry.get("id") == saved["entry_id"])
        assert native["type"] == "compaction"
        assert native["details"]["elephant_compaction"]["attempt_id"] == attempts[0]
        assert any("verified history" in json.dumps(entry) or "Earlier verified work" in json.dumps(entry) for entry in entries)
        if scenario == "rolling":
            assert len(summaries) >= 2
            assert sum(entry["type"] == "compaction" for entry in entries) == 1
        if scenario in {"maintenance", "rolling"}:
            await runner.stop()
            gateway.revoke()
            gateway.bind({**state.claim, "run_id": "restart-after-compaction", "role": "leader", "purpose": "user",
                "context": {**state.claim["context"], "limits": {**state.claim["context"]["limits"],
                    "force_compaction_tokens": 10000, "elastic_compaction_tokens": 9000}}})
            gateway.provider.transport = httpx.MockTransport(model)
            restarted = PiRunnerAdapter(runner.executable, runner.workspace, provider="qqbot", model="offline",
                timeout=25, session_id=runner.session_path.stem, gateway=gateway, gateway_url=runner.gateway_url)
            try:
                result = await restarted.run("Resume the native checkpoint after restart")
            finally:
                await restarted.stop()
            assert result.summary == "Continued with the committed checkpoint."
            assert len(attempts) == 1
            assert "Verified native checkpoint" in json.dumps(upstream[-1]["messages"])


@pytest.mark.parametrize("stage", ["model_capacity", "in_flight", "before_commit"])
async def test_locked_pi_maintenance_yields_without_a_second_writer(compaction_session, stage, record_property):
    from contextlib import AsyncExitStack

    from elephant.providers import ModelCapacity

    state = compaction_session
    runner, gateway = state.runner, state.gateway
    await runner.run("Preserve a previous authorized turn", keep_alive=True)
    phases = []
    previous = gateway.call
    started = asyncio.Event()
    pending = asyncio.Event()
    body_arrived = asyncio.Event()
    timing = {}

    async def core(operation, payload):
        if operation != "context-compaction":
            return await previous(operation, payload)
        phases.append(payload["phase"])
        if payload["phase"] == "commit" and stage == "before_commit":
            timing["arrival"] = asyncio.get_running_loop().time()
            response = httpx.Response(409, json={"detail": "maintenance yielded to foreground"},
                request=httpx.Request("POST", "http://core/context-compaction"))
            response.raise_for_status()
        return {"accepted": True, "attempt_id": "preemptible"}

    async def upstream(request):
        body = json.loads(request.content)
        if body["stream"]:
            timing["model"] = asyncio.get_running_loop().time()
            body_arrived.set()
            return httpx.Response(200, headers={"content-type": "text/event-stream"},
                text=completion_sse({"role": "assistant", "content": "The foreground turn completed."}))
        started.set()
        if stage == "in_flight":
            await pending.wait()
        return httpx.Response(200, json={"choices": [{"message": {"content": "Summary only."}, "finish_reason": "stop"}]})

    gateway.call = core
    gateway.capacity = ModelCapacity(total=1)
    limits = {**state.claim["context"]["limits"], "force_compaction_tokens": 10000, "elastic_compaction_tokens": 9000}
    claim = {**state.claim, "role": "leader", "purpose": "compaction",
             "context": {**state.claim["context"], "limits": limits}}
    gateway.bind({**claim, "run_id": "maintenance-preempted"})
    gateway.provider.transport = httpx.MockTransport(upstream)
    loop = asyncio.get_running_loop()
    async with AsyncExitStack() as stack:
        if stage == "model_capacity":
            await stack.enter_async_context(gateway.capacity.slot(False))
            timing["arrival"] = loop.time()
        task = asyncio.create_task(runner.run("", keep_alive=True))
        if stage == "in_flight":
            await asyncio.wait_for(started.wait(), 5)
            timing["arrival"] = loop.time()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 5)
        else:
            with pytest.raises(RuntimeError, match="Compaction cancelled"):
                await asyncio.wait_for(task, 5)
    writer_released = loop.time()
    assert runner.stopped_verified and gateway.provider.active == 0
    assert "saved" not in phases
    assert started.is_set() == (stage != "model_capacity")
    assert not any(json.loads(line)["type"] == "compaction" for line in runner.session_path.read_text().splitlines())

    gateway.bind({**claim, "purpose": "user", "run_id": "independent-foreground"})
    gateway.provider.transport = httpx.MockTransport(upstream)
    resumed = PiRunnerAdapter(runner.executable, runner.workspace, provider="qqbot", model="offline",
        timeout=10, session_id=runner.session_path.stem, gateway=gateway, gateway_url=runner.gateway_url)
    try:
        result = await resumed.run("Process the newly arrived input")
    finally:
        await resumed.stop()
    assert result.summary == "The foreground turn completed."
    assert body_arrived.is_set()
    record_property("session_yield_ms", round((writer_released - timing["arrival"]) * 1000, 1))
    record_property("foreground_model_start_after_yield_ms", round((timing["model"] - writer_released) * 1000, 1))
    assert timing["model"] >= writer_released


async def test_locked_pi_rejects_unsummarizable_first_input_and_admits_independent_input(compaction_session):
    state = compaction_session
    runner, gateway = state.runner, state.gateway
    previous = gateway.call
    phases, upstream = [], []

    async def core(operation, payload):
        if operation == "context-compaction":
            phases.append(payload)
            return {"accepted": True, "attempt_id": "first-input-attempt"}
        return await previous(operation, payload)

    async def model(request):
        payload = json.loads(request.content)
        upstream.append(payload)
        if not payload["stream"]:
            return httpx.Response(200, json={"choices": [{"message": {"content": "Only a partial checkpoint."}}]})
        return httpx.Response(200, headers={"content-type": "text/event-stream"},
            text=completion_sse({"role": "assistant", "content": "Independent input processed."}))

    gateway.call = core
    gateway.revoke()
    large = "oversized-first-input " * 4000
    claim = {**state.claim, "role": "leader", "purpose": "user", "session_id": runner.session_path.stem,
        "context": {**state.claim["context"], "source_input_ids": ["oversized"],
            "limits": {**state.claim["context"]["limits"],
                       "force_compaction_tokens": 12000, "elastic_compaction_tokens": 10800}}}
    gateway.bind({**claim, "run_id": "unrecoverable-first-turn", "goal": large})
    gateway.provider.transport = httpx.MockTransport(model)
    with pytest.raises(RuntimeError):
        await runner.run(large)
    assert not [payload for payload in upstream if payload["stream"]]
    assert not [packet for packet in phases if packet["phase"] == "saved"]
    failure = next(packet for packet in phases if packet["phase"] == "failure")
    assert failure["retryable"] is False and failure["tokens"] >= 12000
    assert large in runner.session_path.read_text()

    claim["context"].update(source_input_ids=["independent"], context_failed_input_ids=["oversized"])
    gateway.bind({**claim, "run_id": "independent-next-turn", "goal": "Independent next turn."})
    gateway.provider.transport = httpx.MockTransport(model)
    resumed = PiRunnerAdapter(runner.executable, runner.workspace, provider="qqbot", model="offline",
        timeout=10, session_id=runner.session_path.stem, gateway=gateway, gateway_url=runner.gateway_url)
    try:
        result = await resumed.run("Independent next turn.")
    finally:
        await resumed.stop()
    assert result.summary == "Independent input processed."
    body, = [payload for payload in upstream if payload["stream"]]
    assert "oversized-first-input" not in json.dumps(body)
    assert large in runner.session_path.read_text()
