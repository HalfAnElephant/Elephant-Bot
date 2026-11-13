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
async def test_locked_pi_rejects_raw_tool_protocol_before_result(compaction_session):
    state = compaction_session
    with pytest.raises(ValueError, match="model emitted tool-call protocol"):
        await state.runner.run("List PRs", keep_alive=True)
    assert state.runner.stopped_verified
    assert not state.runner.quiescent_verified
    assert state.gateway.revoked


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
    from qqbot.capabilities.types import Action, Capability

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
    monkeypatch.setattr("qqbot.harness.gateway.fetch_image", lambda url, limit: data)
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
        ("capability_search", {"query": "github"}),
        ("task_notify", {"text": "收到，我正在确认仓库名称～"}),
        ("capability_search", {"query": "web"}),
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


@pytest.mark.parametrize("scenario", ["message", "quiet", "old_call", "notification_denied", "business_failure", "worker", "stale_notice", "string_null"])
async def test_locked_pi_business_call_keeps_explicit_communication_separate(compaction_session, scenario):
    from qqbot.capabilities.types import Action, Capability

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
    if scenario not in {"old_call", "worker"}:
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
    expected_order = ["read"] if scenario in {"quiet", "old_call", "worker", "string_null"} else ["notice", "read"]
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
