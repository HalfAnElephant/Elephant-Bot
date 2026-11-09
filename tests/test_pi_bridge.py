"""Exercise the locked real pi extension with deterministic model SSE and no remote writes."""
import asyncio
import json
import socket
from pathlib import Path

import httpx
import pytest
import uvicorn

from qqbot.harness.gateway import RunGateway
from qqbot.harness.pi import PiRunnerAdapter
from qqbot.jobs.worker import CapabilityServer


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
        if operation in {"usage", "observation"}:
            return {"recorded": True}
        raise AssertionError("unexpected host operation " + operation)
    async def upstream(request):
        payload = json.loads(request.content)
        reservation = next(call[2] for call in reversed(state.host_calls) if call[1] == "reserve-budget")
        state.requests.append({"run_id": gateway.context.run_id, "kind": reservation["kind"], "payload": payload})
        state.request_started.set()
        if state.request_gate:
            await state.request_gate.wait()
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
    from qqbot.config import ContextConfig
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
    assert second[-1]["payload"]["max_tokens"] == gateway.provider.output_tokens
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
    state.gateway.bind({**state.claim, "run_id": "large-context", "goal": "Read the relevant context and respond.",
        "context": {**state.claim["context"], "seed": "相关上下文。" * 10000}})
    result = await state.runner.run("Read the relevant context", keep_alive=True)
    assert result.status == "completed"
    assert len(state.requests) == 1 and state.requests[0]["kind"] == "business"
    assert len(json.dumps(state.requests[0]["payload"], ensure_ascii=False).encode()) > 16000
    settings = json.loads((state.runner.workspace / "profile/settings.json").read_text())
    assert settings["compaction"] == {"enabled": True, "reserveTokens": 100000, "keepRecentTokens": 200000}
    assert not any(e.get("type") == "compaction_start" for _, e in state.events)


@pytest.mark.parametrize("compaction_session", [{"response_text":
    '<｜｜DSML｜｜ calls>\n<｜｜DSML｜｜ invoke name="capability_invoke">unexecuted call</｜｜DSML｜｜ invoke></｜｜DSML｜｜ calls>'}], indirect=True)
async def test_locked_pi_rejects_raw_tool_protocol_before_result(compaction_session):
    state = compaction_session
    with pytest.raises(ValueError, match="model emitted tool-call protocol"):
        await state.runner.run("List PRs", keep_alive=True)
    assert state.runner.stopped_verified
    assert not state.runner.quiescent_verified
    assert state.gateway.revoked


@pytest.mark.parametrize("notification", [False, True])
async def test_locked_pi_receives_trusted_elephant_persona_for_every_speech_path(compaction_session, notification):
    state = compaction_session
    state.gateway.revoke()
    state.gateway.bind({**state.claim, "run_id": "persona-run",
        "notification_status": "failed" if notification else None,
        "context": {**state.claim["context"], "seed": {
            "persona": {"core": {"name": "大象", "style": {"tone": "TRUSTED_PERSONA_SENTINEL"}}, "group": {}},
            "history": [{"role": "user", "text": "UNTRUSTED_HISTORY_SENTINEL"}]}}})
    await state.runner.run("Say hello" if not notification else "Explain the verified failure", keep_alive=True)
    system = "\n".join(message["content"] for message in state.requests[0]["payload"]["messages"] if message["role"] == "system")
    assert "你是大象" in system and "QQ 文字只用纯文本" in system
    assert "TRUSTED_PERSONA_SENTINEL" in system
    assert "UNTRUSTED_HISTORY_SENTINEL" not in system
    assert state.requests[0]["kind"] == ("completion" if notification else "business")
