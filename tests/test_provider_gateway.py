"""Deterministic provider and host-binding checks without external service credentials."""
import asyncio
import json
import logging
from contextlib import asynccontextmanager

import httpx
import pytest
from fastapi import HTTPException

from elephant.capabilities import Action, Capability
from elephant.harness.gateway import RunGateway
from elephant.providers import ProviderAdapter, normalize_usage, validate_transactions


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
    failures = []
    fixture.adapter.upstream_failure = lambda _payload, kind, code, status: failures.append((kind, code, status))
    with pytest.raises(HTTPException) as error:
        await fixture.adapter.complete(body)
    assert error.value.status_code == 422
    assert not fixture.requests and not fixture.reservations
    if isinstance(messages[0], dict) and messages[0].get("role") == "tool":
        assert failures == [("business", "invalid_tool_transcript", 422)]


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


async def test_model_timing_marks_real_http_send_and_sse_milestones(body, caplog, monkeypatch):
    logger = logging.getLogger("elephant.observability.timing")
    monkeypatch.setattr(logger, "handlers", [caplog.handler])
    monkeypatch.setattr(logger, "propagate", False)
    keepalive = b": ping\n\n"
    delta = b'data: {"choices":[{"delta":{"content":"hi"}}]}\n\n'
    release_delta = asyncio.Event()

    async def upstream(reader, writer):
        try:
            headers = await reader.readuntil(b"\r\n\r\n")
            length = next(line.split(b":", 1)[1] for line in headers.lower().splitlines()
                          if line.startswith(b"content-length:"))
            await reader.readexactly(int(length))
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: "
                         + str(len(keepalive + delta)).encode() + b"\r\n\r\n" + keepalive)
            await writer.drain()
            await release_delta.wait()
            writer.write(delta)
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    fixture = ProviderFixture(event_id="inbox-event", run_id="leader-run")
    async with await asyncio.start_server(upstream, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        fixture.adapter.transport = None
        fixture.adapter.endpoint = f"http://127.0.0.1:{port}/v1/chat/completions"
        response = await fixture.adapter.complete({**body, "stream": True})
        with caplog.at_level(logging.INFO, logger=logger.name):
            async with asyncio.timeout(3):
                try:
                    assert await anext(response.body_iterator) == keepalive
                finally:
                    release_delta.set()
                assert b"".join([chunk async for chunk in response.body_iterator]) == delta

    trace = next(record.data for record in caplog.records if record.msg == "model.timing")
    assert (trace["event_id"], trace["run_id"], trace["request_id"]) == (
        "inbox-event", "leader-run", fixture.reservations[0][0],
    )
    offsets = trace["offsets_ms"]
    assert offsets["waiting_for_slot"] == 0
    assert offsets["slot_acquired"] <= offsets["request_dispatched"] <= offsets["request_send_started"]
    assert offsets["request_send_started"] <= offsets["request_sent"] <= offsets["response_headers"]
    assert offsets["response_headers"] <= offsets["first_body_byte"] < offsets["first_effective_delta"]
    assert offsets["first_content_delta"] <= offsets["first_effective_delta"]
    assert trace["chunks"] == 2
    assert set(trace["stream_ms"]) == {"read_wait", "authorize", "downstream_wait"}
    assert all(value >= 0 for value in trace["stream_ms"].values())


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


async def test_outcome_references_are_validated_before_acceptance(gateway):
    instance, core, _claim = gateway
    validations = []

    async def host(operation, payload):
        if operation == "validate-result-refs":
            validations.append(payload)
            if "missing-observation" in payload["observation_refs"]:
                httpx.Response(422, json={"detail": "success check not verified: observation"},
                    request=httpx.Request("POST", "http://runtime.local/validate-result-refs")).raise_for_status()
            return {"accepted": True}
        return await core(operation, payload)

    instance.call = host
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=instance.app), base_url="http://gateway") as client:
        rejected = await client.post("/tools/task_outcome", headers=auth(instance.tool_token),
            json={"call_id": "bad-outcome", "arguments": {"status": "completed",
                "observation_refs": ["missing-observation"], "attachments": ["outputs/report.md"]}})
        assert rejected.status_code == 422 and "observation" in rejected.json()["detail"]
        assert instance.outcome is None and not instance.artifacts
        accepted = await client.post("/tools/task_outcome", headers=auth(instance.tool_token),
            json={"call_id": "corrected-outcome", "arguments": {"status": "completed",
                "observation_refs": ["prior-attempt-observation"], "effect_refs": []}})
        assert accepted.status_code == 200, accepted.text
    assert len(validations) == 2
    assert instance.outcome["observation_refs"] == ["prior-attempt-observation"]


async def test_scheduled_outcome_preflight_allows_agent_to_correct_final_prose(gateway):
    instance, core, _claim = gateway
    instance.claim.update(role="leader", purpose="attention")
    instance.artifacts.append("outputs/report.md")
    validations = []

    async def host(operation, payload):
        if operation == "validate-result-refs":
            validations.append(payload)
            return ({"accepted": False, "verified_status": "blocked", "facts": "一项子任务受阻。"}
                    if payload["status"] == "completed" else {"accepted": True})
        return await core(operation, payload)

    instance.call = host
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=instance.app), base_url="http://gateway") as client:
        rejected = await client.post("/tools/task_outcome", headers=auth(instance.tool_token),
            json={"call_id": "scheduled-unverified", "arguments": {"status": "completed"}})
        assert rejected.status_code == 200
        assert rejected.json()["accepted"] is False
        assert "一项子任务受阻" in rejected.json()["facts"]
        assert instance.outcome is None
        revised = await client.post("/tools/task_outcome", headers=auth(instance.tool_token),
            json={"call_id": "scheduled-blocked", "arguments": {"status": "blocked"}})
        assert revised.status_code == 200 and revised.json()["accepted"] is True
    assert instance.outcome["status"] == "blocked"
    assert validations[0]["has_direct_artifacts"] is True


@pytest.mark.parametrize("explicit", [False, True])
@pytest.mark.parametrize("effect_state", ["verified", "unknown", "failed"])
async def test_receipt_and_result_share_reference_and_status_rules(gateway, explicit, effect_state):
    instance, core, _claim = gateway
    instance.observations = {"automatic-observation": {}}
    instance.effects = {"automatic-effect": effect_state}
    instance.outcome = {"status": "completed", "unresolved_items": ["fixture limitation"]}
    if explicit:
        instance.outcome.update(observation_refs=["prior-attempt-observation"], effect_refs=[])
    receipts = []

    async def host(operation, payload):
        if operation == "session-receipt":
            receipts.append(payload.copy())
            return {"accepted": True}
        return await core(operation, payload)

    instance.call = host
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=instance.app), base_url="http://gateway") as client:
        response = await client.post("/session-receipt", headers=auth(instance.tool_token),
            json={"kind": "turn_finished", "tools_paired": True, "summary": "Verified report"})
    assert response.status_code == 200, response.text
    result = instance.result("Verified report")
    assert receipts[0]["result"] == result.model_dump(mode="json")
    assert result.status == ("partial" if effect_state == "unknown" else "completed")
    assert result.observation_refs == (("prior-attempt-observation",) if explicit else ("automatic-observation",))
    assert result.effect_refs == (("automatic-effect",) if not explicit and effect_state == "verified" else ())


async def test_completed_outcome_omits_failed_operations_without_discarding_the_ledger(gateway):
    instance, core, _claim = gateway
    records = [{"id": "failed-attempt", "state": "FAILED"}, {"id": "verified-retry", "state": "VERIFIED"}]

    async def host(operation, payload):
        if operation == "ledger" and payload["operation"] == "list":
            return records
        return await core(operation, payload)

    instance.call = host
    instance.effects = {"failed-attempt": "failed", "verified-retry": "verified"}
    result = instance.result("The corrected operation completed.")
    assert result.status == "completed" and result.effect_refs == ("verified-retry",)
    assert await instance.list_operations({}, instance.context) == records
    assert instance.effects["failed-attempt"] == "failed"


def test_explicit_empty_outcome_references_stay_empty(gateway):
    instance, _core, _claim = gateway
    instance.observations = {"automatic-observation": {}}
    instance.effects = {"automatic-effect": "verified"}
    instance.outcome = {"status": "completed", "observation_refs": [], "effect_refs": []}
    result = instance.result("Report")
    assert result.observation_refs == result.effect_refs == ()


@pytest.mark.parametrize(("status", "expected"), [
    ("waiting_input", "等待用户补充"),
    ("waiting_children", "等待子任务"),
    ("waiting_external", "等待外部结果"),
])
def test_empty_wait_checkpoint_has_user_facing_fallback(gateway, status, expected):
    instance, _core, _claim = gateway
    instance.waiting, instance.wait_kind = True, status
    result = instance.result("")
    assert result.status == status
    assert expected in result.summary
    assert "Waiting checkpoint persisted" not in result.summary


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


@pytest.mark.parametrize(("role", "effort"), [("leader", "low"), ("worker", "max")])
async def test_host_reasoning_effort_overrides_pi_and_is_recorded(gateway, role, effort):
    instance, core, claim = gateway
    instance.revoke()
    instance.bind({**claim, "role": role, "reasoning_effort": effort})
    fixture = ProviderFixture()
    instance.provider.transport = fixture.adapter.transport
    await instance.provider.complete({
        "model": "offline", "messages": [{"role": "user", "content": "hi"}], "reasoning_effort": "high",
    })
    assert fixture.requests[0]["reasoning_effort"] == effort
    usage, = (data["usage"] for operation, data in core.calls if operation == "usage")
    assert usage["requested_reasoning_effort"] == effort


async def test_provider_session_header_survives_warm_rebind(gateway):
    instance, _core, claim = gateway
    seen = []
    def upstream(request):
        seen.append(request.headers.get("x-opencode-session"))
        return httpx.Response(200, json={"choices": [{"message": {"role": "assistant", "content": "ok"}}]})
    instance.provider.transport = httpx.MockTransport(upstream)
    await instance.provider.complete({"model": "offline", "messages": [{"role": "user", "content": "hi"}]})
    instance.revoke()
    instance.bind({**claim, "run_id": "second-run", "event_json": json.dumps({"event_id": "second-event"})})
    assert (instance.provider.event_id, instance.provider.run_id) == ("second-event", "second-run")
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
    from elephant.config import Settings
    from elephant.domain.execution import ExecutionPlan

    settings = Settings()
    assert settings.execution.interactive_budget.output_tokens_per_call is None
    assert settings.execution.budget.output_tokens_per_call is None
    settings.context.limits(settings.model.context_window, None)
    old_plan = ExecutionPlan.model_validate_json('{"budget":{"output_tokens_per_call":4096}}')
    assert old_plan.budget.output_tokens_per_call == 4096


@pytest.mark.parametrize("endpoint,field", [
    ("https://api.openai.com/v1/chat/completions", "max_completion_tokens"),
    ("https://api.openai.com.fixture.invalid/v1/chat/completions", "max_tokens"),
    ("https://openrouter.ai/api/v1/chat/completions", "max_tokens"),
])
async def test_trusted_output_limit_uses_provider_parameter(body, endpoint, field):
    fixture = ProviderFixture(output_tokens=512)
    fixture.adapter.endpoint = endpoint
    await fixture.adapter.complete(body)
    request, = fixture.requests
    assert request[field] == 512
    assert not {"max_tokens", "max_completion_tokens"}.difference({field}).intersection(request)


@pytest.mark.parametrize(("mode", "delta", "allowed"), [
    ("text", 1, True), ("text", 0, False), ("text", -1, False), ("documents", 0, False),
    ("attachment", 0, False), ("tools", 0, False), ("bytes", 0, True), ("bytes", -1, False),
])
async def test_leader_context_gate_uses_expanded_final_request(tmp_path, mode, delta, allowed):
    from types import SimpleNamespace

    from elephant.providers import pi_request_tokens

    fixture = ProviderFixture()
    body = {"model": "offline", "messages": [{"role": "system", "content": "policy"},
                                            {"role": "user", "content": "context" * 100}]}
    projected = {**body}
    if mode in {"documents", "attachment"}:
        block = {"role": "user", "content": [{"type": "text", "text": "授权文档内容" * 300}]
                 if mode == "documents" else [{"type": "image_url", "image_url": {"url": "data:image/png;base64,image"}}]}
        projected["messages"] = [*body["messages"], block]
    if mode == "tools":
        body["tools"] = [{"type": "function", "function": {"name": "task", "description": "schema" * 300}}]
        projected["tools"] = body["tools"]

    async def core(operation, packet):
        if operation == "authorize":
            return {"allowed": True}
        if operation == "reserve-budget":
            return await fixture.reserve(packet["request_id"], packet["kind"])
        assert operation == "usage"

    gateway = RunGateway(tmp_path, "offline", "fixture-key", "https://model.invalid/v1/chat/completions",
                         call=core, transport=fixture.adapter.transport)

    async def expand(payload, *args, **kwargs):
        return {**payload, "messages": projected["messages"]}

    if mode == "documents":
        gateway.documents = SimpleNamespace(with_documents=expand)
    elif mode == "attachment":
        gateway.with_attachments = expand
    limits = {"force_compaction_tokens": pi_request_tokens(projected) + delta}
    if mode == "bytes":
        limits.update(force_compaction_tokens=4096,
                      max_bytes=len(json.dumps({**projected, "stream": False}, ensure_ascii=False).encode()) + delta)
    gateway.bind({"task_id": "leader", "run_id": "run", "fencing_token": 1, "role": "leader",
                  "permission_snapshot": {"principal": {}}, "context": {"limits": limits}})
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://local") as client:
        response = await client.post("/v1/chat/completions", json=body,
                                     headers={"Authorization": "Bearer " + gateway.model_token})
    assert response.status_code == (200 if allowed else 413), response.text
    assert len(fixture.requests) == len(fixture.reservations) == int(allowed)
    if not allowed:
        assert gateway.context_failure["tokens"] == pi_request_tokens(projected)


async def test_gateway_persists_rejected_transcript_as_nonretryable(tmp_path, body):
    fixture = ProviderFixture()
    context_failure = []

    async def core(operation, packet):
        if operation == "authorize":
            return {"allowed": True}
        assert operation == "context-compaction"
        context_failure.append(packet)
        return {"accepted": True}

    gateway = RunGateway(tmp_path, "offline", "fixture-key", "https://model.invalid/v1/chat/completions",
                         call=core, transport=fixture.adapter.transport)
    gateway.bind({"task_id": "leader", "run_id": "run", "fencing_token": 1, "role": "leader",
                  "permission_snapshot": {"principal": {}}, "context": {"limits": {"force_compaction_tokens": 4096}}})
    body["messages"] = [{"role": "tool", "tool_call_id": "orphan", "content": "receipt"}]
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://local") as client:
        response = await client.post("/v1/chat/completions", json=body,
                                     headers={"Authorization": "Bearer " + gateway.model_token})
    assert response.status_code == 422
    assert not fixture.requests
    await gateway.persist_context_failure()
    assert len(context_failure) == 1
    assert context_failure[0]["reason"] == "invalid_tool_transcript"
    assert context_failure[0]["phase"] == "failure" and context_failure[0]["retryable"] is False
