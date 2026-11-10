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
    users = [str(message["content"]) for message in state.requests[0]["payload"]["messages"]
             if message["role"] == "user"]
    assert all("TRUSTED_PERSONA_SENTINEL" not in message for message in users)
    assert any("UNTRUSTED_HISTORY_SENTINEL" in message for message in users)
    assert state.requests[0]["kind"] == ("completion" if notification else "business")


async def test_locked_pi_refreshes_expression_material_without_reseeding_topic(compaction_session):
    state = compaction_session
    await state.runner.rpc("set_auto_compaction", enabled=False)
    state.gateway.revoke()
    for index, material in enumerate(("EXPRESSION_FIRST_SENTINEL", "EXPRESSION_SECOND_SENTINEL")):
        state.gateway.bind({**state.claim, "run_id": "expression-run-" + str(index),
            "context": {**state.claim["context"], "expression_habits": material}})
        await state.runner.run("Respond naturally", keep_alive=True)
        messages = state.requests[-1]["payload"]["messages"]
        system = "\n".join(str(message["content"]) for message in messages if message["role"] == "system")
        users = [str(message["content"]) for message in messages if message["role"] == "user"]
        assert material not in system
        assert any(material in message for message in users)
    assert state.runner.quiescent_verified


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


async def test_locked_pi_context_delta_persists_source_cursor_across_rebind_and_restart(compaction_session):
    state = compaction_session
    await state.runner.rpc("set_auto_compaction", enabled=False)
    state.gateway.revoke()
    first = {"role": "user", "speaker": "member_a", "source": "source-a", "text": "FIRST_CONTEXT_SENTINEL"}
    second = {"role": "user", "speaker": "member_b", "source": "source-b", "text": "NEW_GROUP_CONTEXT_SENTINEL"}
    delivered = {"role": "assistant", "source": "delivery-a", "text": "DELIVERED_CONTEXT_SENTINEL"}
    seed = {"history": [first], "memories": [], "topic": {"kind": "general"}}
    for index, history in enumerate(([first], [first, second, delivered], [second, delivered])):
        state.gateway.bind({**state.claim, "run_id": "delta-run-" + str(index),
            "context": {**state.claim["context"], "seed": {**seed, "history": history}}})
        await state.runner.run("Respond with the current context", keep_alive=True)
        users = [str(message["content"]) for message in state.requests[-1]["payload"]["messages"]
                 if message["role"] == "user"]
        combined = "\n".join(users)
        assert combined.count("FIRST_CONTEXT_SENTINEL") == 1
        if index:
            assert combined.count("NEW_GROUP_CONTEXT_SENTINEL") == 1
            assert combined.count("DELIVERED_CONTEXT_SENTINEL") == 1
    entries = [json.loads(line) for line in state.runner.session_path.read_text().splitlines()]
    contexts = [entry for entry in entries if entry.get("customType") == "qqbot-context"]
    assert len(contexts) == 2
    assert json.loads(contexts[1]["content"])["context_delta"]["history"] == [second, delivered]
    assert set(contexts[1]["details"]["source_cursor"]) == {"user:source-a", "user:source-b", "assistant:delivery-a"}

    # A new process loads ACKs from the same session file, without relying on in-memory sets.
    old_runner = state.runner
    await old_runner.stop()
    state.gateway.bind({**state.claim, "run_id": "delta-recovery-run",
        "context": {**state.claim["context"], "seed": {**seed, "history": [first, second, delivered]}}})
    state.runner = PiRunnerAdapter(old_runner.executable, old_runner.workspace, provider="qqbot", model="offline",
        timeout=25, gateway=state.gateway, gateway_url=old_runner.gateway_url,
        session_id=old_runner.session_path.stem)
    try:
        await state.runner.run("Continue after process recovery", keep_alive=True)
        messages = str(state.requests[-1]["payload"]["messages"])
        assert messages.count("FIRST_CONTEXT_SENTINEL") == 1
        assert messages.count("NEW_GROUP_CONTEXT_SENTINEL") == 1
        assert messages.count("DELIVERED_CONTEXT_SENTINEL") == 1
        entries = [json.loads(line) for line in state.runner.session_path.read_text().splitlines()]
        assert len([entry for entry in entries if entry.get("customType") == "qqbot-context"]) == 2
    finally:
        await state.runner.stop()


async def test_locked_pi_context_revision_reaches_model_once(compaction_session):
    state = compaction_session
    await state.runner.rpc("set_auto_compaction", enabled=False)
    state.gateway.revoke()
    for index, text in enumerate(("OLD_CONTEXT_SENTINEL", "CORRECTED_CONTEXT_SENTINEL", "CORRECTED_CONTEXT_SENTINEL")):
        state.gateway.bind({**state.claim, "run_id": "revision-run-" + str(index),
            "context": {**state.claim["context"], "seed": {"history": [
                {"source": "stable-source", "role": "user", "text": text}]}}})
        await state.runner.run("Use the current source revision", keep_alive=True)
    messages = str(state.requests[-1]["payload"]["messages"])
    assert messages.count("OLD_CONTEXT_SENTINEL") == 1
    assert messages.count("CORRECTED_CONTEXT_SENTINEL") == 1
    entries = [json.loads(line) for line in state.runner.session_path.read_text().splitlines()]
    contexts = [entry for entry in entries if entry.get("customType") == "qqbot-context"]
    assert len(contexts) == 2
    assert json.loads(contexts[-1]["content"])["context_delta"]["history"][0]["text"] == "CORRECTED_CONTEXT_SENTINEL"


async def test_locked_pi_discovers_unknown_skill_reads_and_executes_its_script(compaction_session, tmp_path, monkeypatch):
    import sys

    from qqbot.capabilities import process as process_module
    from qqbot.harness.skills import SkillStore

    state = compaction_session
    source = tmp_path / 'maintained-skills'
    skill = source / 'camera-advisor'
    (skill / 'scripts').mkdir(parents=True)
    (skill / 'SKILL.md').write_text('---\nname: camera-advisor\ndescription: Prepare a camera recommendation.\n---\n'
        'CAMERA_SKILL_BODY_SENTINEL\nRun /skills/camera-advisor/scripts/create.py and export outputs/camera.txt.\n')
    (skill / 'scripts' / 'create.py').write_text(
        'from pathlib import Path\nPath("outputs").mkdir(exist_ok=True)\n'
        'Path("outputs/camera.txt").write_text("camera report from unknown skill")\n')
    snapshot = SkillStore(source, tmp_path / 'versions').select()
    gateway = state.gateway
    old_runner = state.runner
    await old_runner.stop()
    gateway.revoke()
    workspace = tmp_path / 'task-workspace'
    workspace.mkdir()
    gateway.workspace = workspace
    gateway.skills = snapshot
    gateway.bind({**state.claim, 'run_id': 'unknown-skill', 'context': {
        **state.claim['context'], 'skill_version': snapshot.version}})
    original_call = gateway.call

    async def host(operation, payload):
        if operation == 'ledger':
            return {'state': 'PREPARED', 'result': {}} if payload['operation'] == 'prepare' else {'changed': True}
        return await original_call(operation, payload)

    gateway.call = host
    gateway.tools.ledger.call = host
    # Exercise the production command/mount construction, with a local subprocess replacing bwrap.
    mounts = []

    async def start_process(*command, **options):
        mounts.append(command)
        assert ('--ro-bind', str(snapshot.directory), '/skills') == command[
            command.index(str(snapshot.directory)) - 1:command.index(str(snapshot.directory)) + 2]
        argv = list(command[command.index('--') + 1:])
        argv[0] = sys.executable
        argv = [arg.replace('/skills/', str(snapshot.directory) + '/') for arg in argv]
        return await asyncio.create_subprocess_exec(*argv, cwd=workspace, **options)

    monkeypatch.setattr(process_module.sys, 'platform', 'linux')
    monkeypatch.setattr(process_module.shutil, 'which', lambda _name: '/usr/bin/bwrap')
    monkeypatch.setattr(process_module, 'start_process', start_process)
    requests = []

    def upstream(request):
        payload = json.loads(request.content)
        requests.append(payload)
        if len(requests) == 1:
            system = '\n'.join(str(message['content']) for message in payload['messages'] if message['role'] == 'system')
            assert 'camera-advisor' in system and '/skills/camera-advisor/SKILL.md' in system
            assert 'CAMERA_SKILL_BODY_SENTINEL' not in system
            assert '已安装能力目录' not in system
            assert 'capability_search' not in [tool['function']['name'] for tool in payload['tools']]
            tool = {'name': 'read', 'arguments': {'path': '/skills/camera-advisor/SKILL.md'}}
        elif len(requests) == 2:
            assert 'CAMERA_SKILL_BODY_SENTINEL' in str(payload['messages'])
            tool = {'name': 'capability_invoke', 'arguments': {'name': 'process.start',
                'arguments': {'argv': ['python3', '/skills/camera-advisor/scripts/create.py']}}}
        elif len(requests) == 3:
            reference = next(iter(gateway.processes.processes))
            tool = {'name': 'capability_invoke', 'arguments': {'name': 'process.wait',
                'arguments': {'process_ref': reference, 'wait_seconds': 5}}}
        elif len(requests) == 4:
            tool = {'name': 'task_outcome', 'arguments': {'status': 'completed', 'attachments': ['outputs/camera.txt']}}
        else:
            return httpx.Response(200, headers={'content-type': 'text/event-stream'},
                text=completion_sse({'role': 'assistant', 'content': '相机建议文件已生成。'}))
        message = {'role': 'assistant', 'content': None, 'tool_calls': [{'id': 'skill-call-' + str(len(requests)),
            'type': 'function', 'function': {'name': tool['name'], 'arguments': json.dumps(tool['arguments'])}}]}
        return httpx.Response(200, headers={'content-type': 'text/event-stream'}, text=completion_sse(message))

    gateway.provider.transport = httpx.MockTransport(upstream)
    state.runner = PiRunnerAdapter(old_runner.executable, old_runner.workspace, provider='qqbot', model='offline',
        timeout=25, gateway=gateway, gateway_url=old_runner.gateway_url)
    try:
        result = await state.runner.run('Generate a camera recommendation', keep_alive=True)
    finally:
        await state.runner.stop()
    assert result.status == 'completed'
    assert (workspace / 'outputs' / 'camera.txt').read_text() == 'camera report from unknown skill'
    assert gateway.artifacts == ['outputs/camera.txt'] and len(mounts) == 1
    assert len(requests) == 5
    skill_reads = [call[2] for call in state.host_calls if call[1] == 'observation'
                   and call[2]['data'].get('skill_version')]
    assert skill_reads[0]['data']['skill_version'] == snapshot.version


@pytest.mark.parametrize('valid', [True, False])
async def test_locked_pi_explicit_skill_is_loaded_or_stops_before_model(compaction_session, tmp_path, valid):
    from qqbot.harness.skills import SkillStore

    state = compaction_session
    old_runner = state.runner
    await old_runner.stop()
    source = tmp_path / 'explicit-skills'
    skill = source / 'explicit-fixture'
    skill.mkdir(parents=True)
    description = 'description: Explicit test workflow.\n' if valid else ''
    (skill / 'SKILL.md').write_text('---\nname: explicit-fixture\n' + description +
        'disable-model-invocation: true\n---\nEXPLICIT_SKILL_BODY_SENTINEL\n')
    snapshot = SkillStore(source, tmp_path / 'versions').select()
    state.gateway.skills = snapshot
    state.gateway.bind({**state.claim, 'run_id': 'explicit-skill',
        'goal': '/skill:explicit-fixture Say hello', 'context': {
            **state.claim['context'], 'skill_version': snapshot.version, 'notify_before_tools': True}})
    runner = PiRunnerAdapter(old_runner.executable, old_runner.workspace, provider='qqbot', model='offline',
        timeout=25, gateway=state.gateway, gateway_url=old_runner.gateway_url)
    try:
        if valid:
            result = await runner.run('/skill:explicit-fixture Say hello', keep_alive=True)
            assert result.status == 'completed'
            assert len(state.requests) == 1
            users = [message for message in state.requests[0]['payload']['messages'] if message['role'] == 'user']
            assert 'EXPLICIT_SKILL_BODY_SENTINEL' in str(users)
            reads = [call for call in state.host_calls if call[1] == 'observation']
            assert reads[0][2]['data']['skill_version'] == snapshot.version
        else:
            with pytest.raises(RuntimeError, match='Requested skill is unavailable'):
                await runner.run('/skill:explicit-fixture Say hello', keep_alive=True)
            assert not state.requests
    finally:
        await runner.stop()


@pytest.mark.parametrize("cold", [False, True])
async def test_locked_pi_continuation_and_restored_updates_are_injected_once(compaction_session, cold):
    state = compaction_session
    runner, gateway = state.runner, state.gateway
    await runner.run("initial goal", keep_alive=True)
    decision = {"id": "continued-input", "text": "CONTINUATION_DECISION_SENTINEL"}
    old_decision = {"id": "older-input", "text": "RESTORED_DECISION_SENTINEL"}
    queued = []
    if cold:
        await runner.stop()
        queued = [{"id": "queued-first", "text": "QUEUED_FIRST_SENTINEL"},
                  {"id": "queued-second", "text": "QUEUED_SECOND_SENTINEL"}]
        entries = [{"type": "custom", "customType": "qqbot-input", "data": {"id": item["id"]}}
                   for item in queued]
        entries += [{"type": "message", "message": {"role": "user", "timestamp": 0,
            "content": [{"type": "text", "text": f"用户补充[{item['id']}]：\n{item['text']}"}]}}
                    for item in queued]
        entries.append({"type": "custom", "customType": "qqbot-input", "data": {"id": decision["id"]}})
        previous = json.loads(runner.session_path.read_text().splitlines()[-1])
        with runner.session_path.open("a") as session:
            for index, entry in enumerate(entries):
                entry.update(id=f"cafe{index:04x}", parentId=previous["id"], timestamp=previous["timestamp"])
                session.write(json.dumps(entry) + "\n")
                previous = entry
        runner = PiRunnerAdapter(runner.executable, runner.workspace, provider="qqbot", model="offline",
            timeout=25, session_id=runner.session_path.stem, gateway=gateway, gateway_url=runner.gateway_url)
    observation = {"id": "external-input", "text": "EXTERNAL_OBSERVATION_SENTINEL"}
    old_observation = {"id": "external-old", "text": "RESTORED_OBSERVATION_SENTINEL"}
    task_state = {"goal": {}, "checkpoint": None, "user_updates": queued + [old_decision, decision],
                  "external_observations": [old_observation, observation]}
    continuation = {"checkpoint": "Awaited user input for the old target", "inputs": [decision, observation]}
    try:
        gateway.bind({**state.claim, "run_id": "second-run", "task_state": task_state, "continuation": continuation})
        await runner.run("continue with the user's input", keep_alive=True)
        second = state.requests[-1]["payload"]["messages"]
        latest_user = next(message for message in reversed(second) if message["role"] == "user")
        text = "".join(part["text"] for part in latest_user["content"] if part["type"] == "text")
        assert text.startswith("原始用户目标：")
        materials, supplement = text.split("本轮已核验的用户补充", 1)
        assert decision["text"] not in materials and old_decision["text"] not in materials
        assert f"用户补充[{decision['id']}]：\n{decision['text']}" in supplement
        assert supplement.index(old_decision["text"]) < supplement.index(decision["text"])
        assert text.count(observation["text"]) == 1 and observation["text"] in materials
        assert text.count(old_observation["text"]) == 1 and old_observation["text"] in materials
        assert "用户补充[external-input]" not in text
        assert text.count(decision["text"]) == 1 and text.count(old_decision["text"]) == 1
        for item in queued:
            assert item["text"] not in text
            assert json.dumps(second).count(item["text"]) == 1
        gateway.bind({**state.claim, "run_id": "third-run", "task_state": task_state, "continuation": continuation})
        await runner.run("restore the same durable input state", keep_alive=True)
        third = state.requests[-1]["payload"]["messages"]
        latest_user = next(message for message in reversed(third) if message["role"] == "user")
        for item in queued + [old_decision, decision, old_observation, observation]:
            assert item["text"] not in json.dumps(latest_user)
            assert json.dumps(third).count(item["text"]) == 1
    finally:
        if cold:
            await runner.stop()


async def test_locked_pi_recovers_a_cleared_steer_on_the_next_binding(compaction_session):
    state = compaction_session
    runner, gateway = state.runner, state.gateway
    state.request_gate = asyncio.Event()
    decision = {"id": "cleared-input", "text": "CLEARED_INPUT_SENTINEL"}
    execution = asyncio.create_task(runner.run("initial goal", keep_alive=True))
    try:
        await asyncio.wait_for(state.request_started.wait(), 3)
        await runner.append_input(decision["id"], decision["text"])
        await runner.command("clear_queue", wait=True)
        state.request_gate.set()
        await asyncio.wait_for(execution, 10)
        assert decision["text"] not in json.dumps(state.requests[-1]["payload"]["messages"])
        gateway.bind({**state.claim, "run_id": "recovered-run",
            "task_state": {"user_updates": [decision]}, "continuation": {"inputs": [decision]}})
        await runner.run("resume the user's input", keep_alive=True)
        messages = state.requests[-1]["payload"]["messages"]
        latest_user = next(message for message in reversed(messages) if message["role"] == "user")
        assert json.dumps(latest_user).count(decision["text"]) == 1
        assert json.dumps(messages).count(decision["text"]) == 1
    finally:
        state.request_gate.set()
        if not execution.done():
            execution.cancel()
        await asyncio.gather(execution, return_exceptions=True)
