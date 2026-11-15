"""Deterministic provider and host-binding checks without external service credentials."""
import asyncio
import json
from contextlib import asynccontextmanager

import httpx
import pytest
from fastapi import HTTPException

from qqbot.capabilities import Action, Capability
from qqbot.harness.gateway import RunGateway
from qqbot.providers import ProviderAdapter, normalize_usage, validate_transactions


class ProviderFixture:
    def __init__(self, responder=None, **settings):
        self.requests, self.reservations, self.usage, self.authorizations = [], [], [], []
        self.allowed = True
        self.responder = responder or (lambda request: httpx.Response(200, json={
            "choices": [{"message": {"role": "assistant", "content": "done"}}]}))
        self.adapter = ProviderAdapter("offline", "https://model.invalid/v1/chat/completions", "fixture-key",
            authorize=self.authorize, reserve=self.reserve, record=self.record,
            transport=httpx.MockTransport(self.upstream), **settings)

    async def authorize(self, action):
        self.authorizations.append(action)
        if not self.allowed:
            raise PermissionError("source authorization changed")

    async def reserve(self, request_id, kind):
        self.reservations.append((request_id, kind))
        return {"accepted": True, "remaining": 10}

    async def record(self, request_id, usage):
        self.usage.append((request_id, usage))

    async def upstream(self, request):
        self.requests.append(json.loads(request.content))
        result = self.responder(request)
        return await result if hasattr(result, "__await__") else result


@pytest.fixture
def body():
    return {"model": "offline", "messages": [{"role": "user", "content": "hello"}]}


@pytest.mark.parametrize("messages", [
    [{"role": "tool", "tool_call_id": "orphan", "content": "x"}],
    [{"role": "assistant", "tool_calls": [{"id": "missing"}]}],
    [{"role": "assistant", "tool_calls": [{"id": "a"}]}, {"role": "user", "content": "interrupt"}],
    [{"role": "assistant", "tool_calls": [{"id": "a"}]}, {"role": "tool", "tool_call_id": "a"},
     {"role": "tool", "tool_call_id": "a"}],
    [{"role": "user", "tool_calls": [{"id": "a"}]}],
    [{"role": "assistant", "tool_calls": [{"id": "a"}, {"id": "a"}]}],
    [{"role": "assistant", "tool_calls": [{"id": {"forged": True}}]}],
    ["malformed"],
    [{"role": ["forged"]}],
])
async def test_transaction_rejection_precedes_reservation(messages, body):
    fixture = ProviderFixture()
    body["messages"] = messages
    with pytest.raises(HTTPException) as error:
        await fixture.adapter.complete(body)
    assert error.value.status_code == 422
    assert not fixture.requests and not fixture.reservations


def test_complete_parallel_transaction_is_accepted():
    validate_transactions([
        {"role": "user", "content": "do it"},
        {"role": "assistant", "tool_calls": [{"id": "a"}, {"id": "b"}]},
        {"role": "tool", "tool_call_id": "b", "content": "second"},
        {"role": "tool", "tool_call_id": "a", "content": "first"},
    ])


async def test_tool_schema_included_in_request_byte_gate(body):
    fixture = ProviderFixture(max_bytes=200)
    body["tools"] = [{"type": "function", "function": {"name": "large", "description": "中" * 300}}]
    with pytest.raises(HTTPException) as error:
        await fixture.adapter.complete(body)
    assert error.value.status_code == 413
    assert not fixture.requests and not fixture.reservations


@pytest.mark.parametrize("raw,expected", [
    (None, {"coverage": "unknown", "input_tokens": None, "output_tokens": None, "cost": None}),
    ({"future_metric": 7}, {"coverage": "unknown", "input_tokens": None}),
    ({"prompt_tokens": 100, "completion_tokens": 5, "prompt_tokens_details": {"cached_tokens": 80}},
     {"coverage": "reported", "input_tokens": 100, "cached_read_tokens": 80, "uncached_input_tokens": 20}),
    ({"input_tokens": 10, "cache_creation_input_tokens": 20, "cache_read_input_tokens": 70, "output_tokens": 4},
     {"input_tokens": 100, "uncached_input_tokens": 10, "cached_write_tokens": 20, "cached_read_tokens": 70}),
    ({"prompt_cache_miss_tokens": 20, "prompt_cache_hit_tokens": 80, "completion_tokens": 5},
     {"input_tokens": 100, "uncached_input_tokens": 20, "cached_read_tokens": 80}),
    ({"prompt_tokens": "invalid", "completion_tokens": -1, "prompt_tokens_details": "malformed"},
     {"coverage": "unknown", "input_tokens": None, "output_tokens": None}),
])
def test_usage_raw_and_cache_accounting(raw, expected):
    usage = normalize_usage(raw)
    assert usage["raw"] is raw
    assert all(usage[key] == value for key, value in expected.items())
    assert usage["cost"] is None


class ChunkStream(httpx.AsyncByteStream):
    def __init__(self, chunks, gate=None):
        self.chunks, self.gate = chunks, gate
        self.finished = False

    async def __aiter__(self):
        for index, chunk in enumerate(self.chunks):
            if index and self.gate:
                await self.gate.wait()
            yield chunk
        self.finished = True


async def test_true_stream_first_chunk_precedes_upstream_completion(body):
    gate = asyncio.Event()
    first = b'event: message\r\ndata:{"choices":[{"delta":{"content":"hi"}}]}\r\n\r\n'
    usage = {"prompt_tokens": 100, "completion_tokens": 4, "prompt_tokens_details": {"cached_tokens": 60},
             "future_provider_field": {"billing": "unknown"}}
    last = ('data: ' + json.dumps({"choices": [], "usage": usage}) + '\n\ndata: [DONE]\n\n').encode()
    stream = ChunkStream([first, last], gate)
    fixture = ProviderFixture(lambda request: httpx.Response(200, headers={"content-type": "text/event-stream"},
                                                             stream=stream))
    response = await fixture.adapter.complete({**body, "stream": True})
    iterator = response.body_iterator.__aiter__()
    assert await asyncio.wait_for(anext(iterator), 1) == first
    assert not stream.finished and not fixture.usage and fixture.adapter.active == 1
    gate.set()
    remaining = b"".join([chunk async for chunk in iterator])
    assert remaining == last and stream.finished and fixture.adapter.active == 0
    assert len(fixture.requests) == 1 and len(fixture.usage) == 1
    assert fixture.usage[0][1]["raw"] == usage
    assert fixture.usage[0][1]["cached_read_tokens"] == 60
    assert fixture.usage[0][1]["ttft_ms"] is not None
    assert fixture.requests[0]["stream_options"] == {"include_usage": True}


async def test_stream_utf8_split_and_missing_usage_stay_unknown(body):
    text = 'data: {"choices":[{"delta":{"content":"中文"}}]}\n\ndata: [DONE]\n\n'.encode()
    offset = text.index("中".encode()) + 1
    stream = ChunkStream([text[:offset], text[offset:offset + 1], text[offset + 1:]])
    fixture = ProviderFixture(lambda request: httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=stream))
    response = await fixture.adapter.complete({**body, "stream": True})
    assert b"".join([chunk async for chunk in response.body_iterator]) == text
    assert fixture.usage[0][1]["coverage"] == "unknown"
    assert fixture.usage[0][1]["input_tokens"] is None


async def test_source_revalidation_after_waiting_for_model_slot(body):
    fixture = ProviderFixture()
    class DelayedCapacity:
        @asynccontextmanager
        async def slot(self, background):
            fixture.allowed = False
            yield
    fixture.adapter.capacity = DelayedCapacity()
    with pytest.raises(PermissionError, match="source authorization"):
        await fixture.adapter.complete(body)
    assert not fixture.requests
    assert fixture.adapter.active == 0
    assert fixture.usage[0][1]["coverage"] == "unknown"


async def test_stream_revalidates_each_chunk_and_never_retries(body):
    gate = asyncio.Event()
    first = b'data: {"choices":[]}\n\n'
    stream = ChunkStream([first, b'data: [DONE]\n\n'], gate)
    fixture = ProviderFixture(lambda request: httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=stream))
    response = await fixture.adapter.complete({**body, "stream": True})
    iterator = response.body_iterator.__aiter__()
    assert await anext(iterator) == first
    fixture.allowed = False
    gate.set()
    with pytest.raises(PermissionError):
        await anext(iterator)
    assert len(fixture.requests) == 1 and len(fixture.usage) == 1
    assert fixture.adapter.active == 0


async def test_upstream_error_is_once_and_unknown_usage_is_recorded(body):
    fixture = ProviderFixture(lambda request: httpx.Response(503))
    with pytest.raises(HTTPException) as error:
        await fixture.adapter.complete(body)
    assert error.value.status_code == 503
    assert len(fixture.requests) == 1 and fixture.adapter.request_count == 1
    assert fixture.usage[0][1]["coverage"] == "unknown"


async def test_business_budget_disables_calls_without_removing_protocol_schema(body):
    fixture = ProviderFixture()
    exhausted = []
    async def reserve(request_id, kind):
        fixture.reservations.append((request_id, kind))
        if kind == "business":
            raise ValueError("task model_calls budget exhausted")
        return {"remaining": 0}
    fixture.adapter.reserve = reserve
    fixture.adapter.on_exhausted = lambda: exhausted.append(True)
    body["tools"] = [{"type": "function", "function": {"name": "write"}}]
    body["tool_choice"] = "required"
    await fixture.adapter.complete(body)
    assert exhausted == [True]
    assert [kind for _, kind in fixture.reservations] == ["business", "completion"]
    assert len(fixture.requests) == 1
    payload = fixture.requests[0]
    assert payload["tools"] == body["tools"] and payload["tool_choice"] == "none"
    assert payload["messages"][0]["role"] == "system"
    assert "未验证" in payload["messages"][0]["content"]
    with pytest.raises(HTTPException) as error:
        await fixture.adapter.complete(body)
    assert error.value.status_code == 429 and len(fixture.requests) == 1


async def test_last_business_reservation_revokes_future_business_tools(body):
    fixture = ProviderFixture()
    exhausted = []
    async def reserve(request_id, kind):
        fixture.reservations.append((request_id, kind))
        return {"remaining": 0}
    fixture.adapter.reserve = reserve
    fixture.adapter.on_exhausted = lambda: exhausted.append(True)
    await fixture.adapter.complete({**body, "tools": [{"function": {"name": "write"}}]})
    assert exhausted == [True] and fixture.requests[0]["tool_choice"] == "none"
    assert fixture.requests[0]["tools"] == [{"function": {"name": "write"}}]
    await fixture.adapter.complete(body)
    assert [kind for _, kind in fixture.reservations] == ["business", "completion"]


async def test_compaction_shares_byte_limit_and_reserves_each_request(body):
    fixture = ProviderFixture(max_bytes=2000, output_tokens=40, compaction_output=100)
    large = {**body, "messages": [{"role": "user", "content": "x" * 800}]}
    await fixture.adapter.complete(large)
    fixture.adapter.request_kind = "compaction"
    for _ in range(3):
        await fixture.adapter.complete(large)
    assert [request["max_tokens"] for request in fixture.requests] == [40, 100, 100, 100]
    assert [kind for _, kind in fixture.reservations] == ["business", "compaction", "compaction", "compaction"]
    with pytest.raises(HTTPException) as error:
        await fixture.adapter.complete({**large, "messages": [{"role": "user", "content": "x" * 3000}]})
    assert error.value.status_code == 413 and len(fixture.requests) == 4


class FakeCore:
    def __init__(self):
        self.calls, self.ledger = [], {}
        self.allowed = True
        self.control_only = False

    async def __call__(self, operation, payload):
        self.calls.append((operation, payload))
        if operation == "authorize":
            if not self.allowed or (self.control_only and payload["action"] not in {"task.execute", "model.request"}):
                raise PermissionError("current source or goal scope denies business tools")
            return {"allowed": True}
        if operation == "reserve-budget":
            return {"accepted": True, "remaining": 10}
        if operation in {"usage", "observation"}:
            return {"recorded": True}
        if operation == "ledger":
            identity = payload["operation_id"]
            if payload["operation"] == "prepare":
                return self.ledger.setdefault(identity, {"state": "PREPARED", "result": {}}).copy()
            entry = self.ledger[identity]
            if entry["state"] != payload["expected"]:
                return {"changed": False}
            entry.update(state=payload["state"], result=payload["result"])
            return {"changed": True}
        raise AssertionError(operation)


@pytest.fixture
def gateway(tmp_path):
    core = FakeCore()
    gateway = RunGateway(tmp_path, "offline", "", "https://model.invalid/v1/chat/completions", call=core)
    claim = {"task_id": "task-real", "run_id": "run-real", "fencing_token": 7, "goal": "trusted goal",
             "permission_snapshot": {"principal": {"user_id": "real-user"}}, "context": {"seed": "bounded"}}
    gateway.bind(claim)
    return gateway, core, claim


def auth(token):
    return {"Authorization": "Bearer " + token}


async def test_old_binding_and_tokens_are_revoked(gateway):
    instance, _core, claim = gateway
    old_binding, old_model, old_tool = instance.binding, instance.model_token, instance.tool_token
    instance.revoke()
    instance.bind({**claim, "run_id": "next-run"})
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=instance.app), base_url="http://gateway") as client:
        assert (await client.post("/binding", json={"binding_id": old_binding}, headers=auth(instance.process_token))).status_code == 403
        assert (await client.post("/tools/capability_search", json={"call_id": "old", "arguments": {}}, headers=auth(old_tool))).status_code == 403
        assert (await client.post("/v1/chat/completions", json={"model": "offline", "messages": []}, headers=auth(old_model))).status_code == 403
        current = await client.post("/binding", json={"binding_id": instance.binding}, headers=auth(instance.process_token))
        assert current.status_code == 200 and current.json()["generation"] == 2


@pytest.mark.parametrize("enabled", [True, False])
async def test_operation_notice_gates_tools_and_resets_on_rebind(gateway, enabled):
    instance, core, claim = gateway
    instance.revoke()
    claim = {**claim, "context": {"notify_before_tools": enabled}}
    instance.bind(claim)
    invoked = []
    instance.registry.register(Capability("fixture.read", "read"), lambda a, c: invoked.append(1) or {},
                               lambda a, c: Action("fixture.read", ()))
    packet = {"call_id": "read", "arguments": {"name": "fixture.read", "arguments": {}}}
    original_call = instance.call
    accepted = False
    async def notify(operation, payload):
        if operation == "notify":
            core.calls.append((operation, payload))
            return {"accepted": accepted}
        return await original_call(operation, payload)
    instance.call = notify
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=instance.app), base_url="http://gateway") as client:
        first = await client.post("/tools/capability_invoke", json=packet, headers=auth(instance.tool_token))
        if enabled:
            assert first.status_code == 422 and "task_notify" in first.json()["detail"]
            assert not invoked
            assert "必须先单独调用 task_notify" in instance.system_prompt
        else:
            assert first.status_code == 200 and invoked == [1]
            assert "必须先单独调用 task_notify" not in instance.system_prompt
        notice = {"call_id": "notice", "arguments": {"text": "我查一下现在的 Issue。"}}
        await client.post("/tools/task_notify", json=notice, headers=auth(instance.tool_token))
        assert not instance.progress_announced
        accepted = True
        response = await client.post("/tools/task_notify", json=notice, headers=auth(instance.tool_token))
        assert response.json()["accepted"] and instance.progress_announced
        if enabled:
            assert core.calls[-1][1]["importance"] == "important"
        assert (await client.post("/tools/capability_invoke", json=packet,
                                  headers=auth(instance.tool_token))).status_code == 200
        instance.revoke()
        instance.bind({**claim, "run_id": "next"})
        assert not instance.progress_announced
        response = await client.post("/tools/capability_search", json={"call_id": "search", "arguments": {}},
                                     headers=auth(instance.tool_token))
        assert response.status_code == (422 if enabled else 200)


@pytest.mark.parametrize("packet", [[], None, {"call_id": "", "arguments": {}},
                                     {"call_id": 9, "arguments": {}}, {"call_id": "x", "arguments": []}])
async def test_malformed_bridge_packet_rejected(gateway, packet):
    instance, core, _claim = gateway
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=instance.app), base_url="http://gateway") as client:
        response = await client.post("/tools/capability_invoke", content=json.dumps(packet),
                                     headers={**auth(instance.tool_token), "content-type": "application/json"})
        assert response.status_code == 422
    assert not any(operation == "ledger" for operation, _ in core.calls)


async def test_forged_tool_identity_is_ignored_by_host(gateway):
    instance, core, _claim = gateway
    seen = []
    def execute(arguments, context):
        seen.append(context)
        return {"observed": True}
    instance.registry.register(Capability("fixture.read", "read"), execute,
                               lambda a, c: Action("fixture.read", ("task:" + c.task_id,)))
    packet = {"call_id": "same-call", "task_id": "forged-task", "run_id": "forged-run",
              "arguments": {"name": "fixture.read", "arguments": {"principal": {"user_id": "attacker"}, "fence": 999}}}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=instance.app), base_url="http://gateway") as client:
        response = await client.post("/tools/capability_invoke", json=packet, headers=auth(instance.tool_token))
    assert response.status_code == 200
    assert (seen[0].task_id, seen[0].run_id, seen[0].fence) == ("task-real", "run-real", 7)
    assert seen[0].principal == {"user_id": "real-user"}
    assert response.json()["observation_refs"]
    assert response.json()["observation_refs"][0] in instance.observations
    permission = next(payload for operation, payload in core.calls
                      if operation == "authorize" and payload["action"] == "fixture.read")
    assert permission["resources"] == ("task:task-real",)


async def test_control_response_cannot_execute_business_tools(gateway):
    instance, core, _claim = gateway
    core.control_only = True
    called = []
    instance.registry.register(Capability("fixture.write", "write"), lambda a, c: called.append(1),
                               lambda a, c: Action("fixture.write", (), "write"))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=instance.app), base_url="http://gateway") as client:
        response = await client.post("/tools/capability_invoke", headers=auth(instance.tool_token),
            json={"call_id": "write", "arguments": {"name": "fixture.write", "arguments": {}}})
    assert response.status_code == 422 and not called and not core.ledger


async def test_unknown_write_is_observed_once_and_never_reported_verified(gateway):
    instance, _core, _claim = gateway
    called = []
    instance.registry.register(Capability("fixture.unknown", "unknown"), lambda a, c: called.append(1) or {"exit_code": 0},
                               lambda a, c: Action("fixture.unknown", ("external:target",), "unknown"))
    packet = {"call_id": "external-write", "arguments": {"name": "fixture.unknown", "arguments": {}}}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=instance.app), base_url="http://gateway") as client:
        first = await client.post("/tools/capability_invoke", json=packet, headers=auth(instance.tool_token))
        replay = await client.post("/tools/capability_invoke", json=packet, headers=auth(instance.tool_token))
    assert first.status_code == replay.status_code == 200
    assert first.json()["status"] == replay.json()["status"] == "unknown"
    assert first.json()["effect_refs"] == []
    assert first.json()["observation_refs"] and (instance.task_content / first.json()["content_ref"][8:]).is_file()
    assert len(called) == 1


async def test_keepalive_is_streamed_without_claiming_first_token(body):
    gate = asyncio.Event()
    comment = b": keepalive\n\n"
    token = b'data: {"choices":[{"delta":{"content":"token"}}]}\n\ndata: [DONE]\n\n'
    fixture = ProviderFixture(lambda request: httpx.Response(200, headers={"content-type": "text/event-stream"},
        stream=ChunkStream([comment, token], gate)))
    response = await fixture.adapter.complete({**body, "stream": True})
    iterator = response.body_iterator.__aiter__()
    assert await anext(iterator) == comment
    assert fixture.adapter.ttft is None
    gate.set()
    assert b"".join([chunk async for chunk in iterator]) == token
    assert fixture.adapter.ttft is not None


async def test_nonstream_malformed_json_is_upstream_failure(body):
    fixture = ProviderFixture(lambda request: httpx.Response(200, content=b"malformed upstream"))
    with pytest.raises(HTTPException) as error:
        await fixture.adapter.complete(body)
    assert error.value.status_code == 502 and len(fixture.requests) == 1
    assert fixture.usage[0][1]["coverage"] == "unknown"


async def test_binding_authorization_pins_generation(gateway):
    instance, _core, claim = gateway
    entered, release = asyncio.Event(), asyncio.Event()
    async def authority():
        entered.set()
        await release.wait()
        return True
    instance.authority = authority
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=instance.app), base_url="http://host") as client:
        pending = asyncio.create_task(client.post("/binding", headers=auth(instance.process_token),
            json={"binding_id": instance.binding}))
        await entered.wait()
        assert not await instance.drain()
        instance.revoke()
        with pytest.raises(RuntimeError, match="active binding"):
            instance.bind({**claim, "run_id": "new"})
        release.set()
        response = await pending
        assert response.status_code == 403
        assert instance.inflight == 0


@pytest.mark.parametrize("stage,action", [
    ("eof", "finish"), ("usage", "finish"), ("eof", "tool"),
    ("eof", "unpaired"), ("eof", "revoke"), ("eof", "deny"), ("eof", "stop"),
])
async def test_final_receipt_waits_for_model_cleanup(gateway, body, stage, action):
    instance, core, _claim = gateway
    blocked, release, receipt_started = asyncio.Event(), asyncio.Event(), asyncio.Event()
    tool_started, tool_release = asyncio.Event(), asyncio.Event()
    receipts, pending = [], []
    receipt_task = None

    class DelayedEOF(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'data: {"choices":[],"usage":{"prompt_tokens":1}}\n\ndata: [DONE]\n\n'
            if stage == "eof":
                blocked.set()
                await release.wait()

    async def host(operation, payload):
        if operation == "authorize" and asyncio.current_task() is receipt_task:
            receipt_started.set()
        if operation == "usage" and stage == "usage":
            blocked.set()
            await release.wait()
        if operation == "session-receipt":
            receipts.append(payload.copy())
            if payload["inflight"] or payload["tools_paired"] is not True:
                httpx.Response(409, json={"detail": "Harness still has activity"},
                    request=httpx.Request("POST", "http://runtime.local/session-receipt")).raise_for_status()
            return {"accepted": True}
        return await core(operation, payload)

    async def tool(arguments, context):
        tool_started.set()
        await tool_release.wait()
        return {}

    instance.call = host
    instance.provider.transport = httpx.MockTransport(lambda request: httpx.Response(
        200, headers={"content-type": "text/event-stream"}, stream=DelayedEOF()))
    instance.registry.register(Capability("fixture.block", "blocked"), tool,
        lambda a, c: Action("fixture.read", ("fixture:x",)))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=instance.app), base_url="http://host") as client:
        try:
            if action == "tool":
                pending.append(asyncio.create_task(client.post("/tools/capability_invoke", headers=auth(instance.tool_token),
                    json={"call_id": "block", "arguments": {"name": "fixture.block", "arguments": {}}})))
                await asyncio.wait_for(tool_started.wait(), 1)
            pending.append(asyncio.create_task(client.post("/v1/chat/completions", headers=auth(instance.model_token),
                json={**body, "stream": True})))
            await asyncio.wait_for(blocked.wait(), 1)
            receipt_task = asyncio.create_task(client.post("/session-receipt", headers=auth(instance.tool_token),
                json={"kind": "turn_finished", "tools_paired": action != "unpaired", "inflight": 0}))
            pending.append(receipt_task)
            await asyncio.wait_for(receipt_started.wait(), 1)
            assert not receipt_task.done() and not receipts
            if action == "stop":
                assert await asyncio.wait_for(instance.stop_requests(), 2)
                assert not receipts and instance.inflight == 0 and not instance.model_requests
                return
            if action == "revoke":
                instance.revoke()
            elif action == "deny":
                core.allowed = False
            release.set()
            response = await asyncio.wait_for(receipt_task, 1)
            if action in {"revoke", "deny"}:
                assert response.status_code == 403 and not receipts
            elif action in {"tool", "unpaired"}:
                assert response.status_code == 409
                assert receipts[0]["inflight"] == (1 if action == "tool" else 0)
                assert receipts[0]["tools_paired"] is (action != "unpaired")
                assert instance.session_receipt is None
            else:
                assert response.status_code == 200 and response.json() == {"accepted": True}
                assert receipts[0]["inflight"] == 0 and instance.provider.active == 0
                assert instance.session_receipt == receipts[0]
            assert not instance.model_requests
        finally:
            release.set()
            tool_release.set()
            await asyncio.gather(*pending, return_exceptions=True)


@pytest.mark.parametrize("status", [409, 422])
async def test_receipt_preserves_host_error_status_and_redacts_tokens(gateway, status):
    instance, core, _claim = gateway
    async def host(operation, payload):
        if operation == "session-receipt":
            httpx.Response(status, json={"detail": "rejected " + instance.tool_token},
                request=httpx.Request("POST", "http://runtime.local/session-receipt")).raise_for_status()
        return await core(operation, payload)
    instance.call = host
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=instance.app), base_url="http://host") as client:
        response = await client.post("/session-receipt", headers=auth(instance.tool_token),
            json={"kind": "turn_finished", "tools_paired": True})
    assert response.status_code == status
    assert response.json() == {"detail": "rejected [REDACTED]"}
    assert instance.session_receipt is None and instance.inflight == 0


async def test_stop_waits_for_active_tool_cleanup(gateway):
    instance, _core, _claim = gateway
    entered, cleaned = asyncio.Event(), asyncio.Event()
    async def blocked(arguments, context):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0.01)
            cleaned.set()
    instance.registry.register(Capability("fixture.block", "blocked"), blocked,
        lambda a, c: Action("fixture.read", ("fixture:x",)))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=instance.app), base_url="http://host") as client:
        request = asyncio.create_task(client.post("/tools/capability_invoke", headers=auth(instance.tool_token),
            json={"call_id": "block", "arguments": {"name": "fixture.block", "arguments": {}}}))
        await entered.wait()
        assert await instance.stop_requests()
        assert cleaned.is_set() and instance.inflight == 0
        await asyncio.gather(request, return_exceptions=True)


async def test_actual_request_metrics_keep_stable_prefix_and_host_model(body):
    fixture = ProviderFixture()
    body["model"] = "forged-model"
    with pytest.raises(HTTPException, match="unapproved model"):
        await fixture.adapter.complete(body)
    body["model"] = "offline"
    body["messages"].insert(0, {"role": "system", "content": "stable instructions"})
    await fixture.adapter.complete(body)
    first = fixture.usage[-1][1]
    body["messages"][-1]["content"] = "different user input"
    await fixture.adapter.complete(body)
    second = fixture.usage[-1][1]
    assert all(request["model"] == "offline" for request in fixture.requests)
    assert first["stable_prefix_sha256"] == second["stable_prefix_sha256"]
    assert first["request_sha256"] != second["request_sha256"]
    assert second["request_bytes"] == len(json.dumps(fixture.requests[-1], ensure_ascii=False, separators=(",", ":")).encode())
    assert second["input_tokens_estimated"] > 0 and second["coverage"] == "unknown"


async def test_provider_session_header_survives_warm_rebind(gateway):
    instance, _core, claim = gateway
    seen = []
    def upstream(request):
        seen.append(request.headers.get("x-opencode-session"))
        return httpx.Response(200, json={"choices": [{"message": {"role": "assistant", "content": "ok"}}]})
    instance.provider.transport = httpx.MockTransport(upstream)
    await instance.provider.complete({"model": "offline", "messages": [{"role": "user", "content": "hi"}]})
    instance.revoke()
    instance.bind({**claim, "run_id": "second-run"})
    instance.provider.transport = httpx.MockTransport(upstream)
    await instance.provider.complete({"model": "offline", "messages": [{"role": "user", "content": "again"}]})
    assert len(seen) == 2 and seen[0] and seen[0] == seen[1]


async def test_one_million_window_does_not_reject_by_utf8_token_heuristic(body):
    fixture = ProviderFixture(model_window=1000000)
    body["messages"][0]["content"] = "中文上下文。" * 10000
    await fixture.adapter.complete(body)
    assert len(fixture.requests) == 1
    assert fixture.usage[0][1]["input_tokens_estimated"] > 8000


@pytest.mark.parametrize("streaming", [False, True])
async def test_upstream_context_overflow_reaches_pi_without_private_body(body, streaming):
    fixture = ProviderFixture(lambda request: httpx.Response(400, json={
        "error": {"code": "context_length_exceeded", "message": "maximum context length exceeded SECRET"}}))
    if streaming:
        response = await fixture.adapter.complete({**body, "stream": True})
        raw = b"".join([chunk async for chunk in response.body_iterator]).decode()
        assert "context_length_exceeded" in raw and "SECRET" not in raw
    else:
        with pytest.raises(HTTPException) as error:
            await fixture.adapter.complete(body)
        assert error.value.status_code == 400
        assert error.value.detail == "maximum context length exceeded"
    assert len(fixture.requests) == len(fixture.usage) == 1


async def test_sse_disconnect_does_not_cancel_usage_commit(body):
    import anyio

    usage = {"prompt_tokens": 100, "completion_tokens": 4}
    chunk = ("data: " + json.dumps({"choices": [], "usage": usage}) + "\n\ndata: [DONE]\n\n").encode()
    fixture = ProviderFixture(lambda request: httpx.Response(200, headers={"content-type": "text/event-stream"},
                              stream=ChunkStream([chunk])))
    committed = []

    async def record(request_id, payload):
        await anyio.lowlevel.checkpoint()
        committed.append(payload)

    fixture.adapter.record = record
    response = await fixture.adapter.complete({**body, "stream": True})
    source = response.body_iterator
    assert await anext(source) == chunk
    with anyio.CancelScope() as scope:
        scope.cancel()
        await source.aclose()
    assert len(committed) == 1 and committed[0]["raw"] == usage
    assert fixture.adapter.active == 0


@pytest.mark.parametrize("streaming", [False, True])
async def test_unlimited_output_omits_client_token_limits_upstream(body, streaming):
    fixture = ProviderFixture()
    if streaming:
        fixture.responder = lambda request: httpx.Response(200, headers={"content-type": "text/event-stream"},
            text="data: [DONE]\n\n")
    response = await fixture.adapter.complete({**body, "max_tokens": 4096,
        "max_completion_tokens": 4096, "stream": streaming})
    if streaming:
        async for _ in response.body_iterator:
            pass
    assert "max_tokens" not in fixture.requests[0]
    assert "max_completion_tokens" not in fixture.requests[0]


def test_unlimited_output_is_default_and_old_explicit_limits_remain_readable():
    from qqbot.config import Settings
    from qqbot.domain.execution import ExecutionPlan

    settings = Settings()
    assert settings.execution.interactive_budget.output_tokens_per_call is None
    assert settings.execution.budget.output_tokens_per_call is None
    settings.context.limits(settings.model.context_window, None)
    old_plan = ExecutionPlan.model_validate_json('{"budget":{"output_tokens_per_call":4096}}')
    assert old_plan.budget.output_tokens_per_call == 4096
