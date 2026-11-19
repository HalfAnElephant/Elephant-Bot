"""Leader turns share the durable public-answer stream and final outbox gate."""
import json
from types import SimpleNamespace

import httpx
import pytest
from sqlalchemy import select, update
from test_agent_runtime import leader_complete, root_goal, worker_complete
from test_agent_runtime import runtime as agent_runtime
from test_leader_core import finish_leader, make_core

from elephant.domain import stable_id
from elephant.jobs.streaming import StreamDelivery
from elephant.storage.schema import outbox, reply_counters
from elephant.storage.stream_schema import stream_messages, stream_runs
from elephant.storage.task_schema import events, runs, tasks


@pytest.fixture
async def leader_stream(tmp_path, official_config, official_event, clock):
    from conftest import official_payload

    config = official_config.model_copy(update={"streaming_enabled": True})
    core = make_core(tmp_path, config, clock)
    calls = []
    setup = SimpleNamespace(core=core, calls=calls, fail_reads=False)

    def request(req):
        if req.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "fixture", "expires_in": 7200})
        calls.append(req)
        if setup.fail_reads:
            raise httpx.ReadTimeout("receipt lost", request=req)
        return httpx.Response(200, json={"id": "leader-stream", "remain_msg_len": 0})

    adapter = core.adapters["qq"]
    await adapter.client.aclose()
    adapter.client = httpx.AsyncClient(transport=httpx.MockTransport(request))
    event = adapter.normalize(official_payload(official_event, private=True, text="你好"))
    setup.event = event
    core.inbox.accept(event)
    await core.process(event)
    try:
        yield setup
    finally:
        await core.stop()
        core.db.close()


async def append(core, claim, seq, kind, text="你好", event_id=None):
    return await core.leader.invoke("event", (claim["run_id"], claim["fencing_token"], "fixture"),
        {"kind": kind, "message_id": "answer", "text": text},
        seq=seq, event_id=event_id or f"event-{seq}")


async def test_private_leader_claim_and_final_stream_suppress_duplicate_outbox(leader_stream):
    s = leader_stream
    claim = s.core.sessions.claim("fixture")
    assert claim["public_answer_events"] is True
    with s.core.db.engine.connect() as conn:
        gate = conn.execute(select(stream_runs)).mappings().one()
        assert gate["task_id"] is None and gate["run_id"] == claim["run_id"]
        assert conn.execute(select(runs.c.task_id)).scalar_one() is None
    await append(s.core, claim, 1, "public_answer_end")
    await append(s.core, claim, 2, "public_answer_final")
    finish_leader(s.core, claim, "你好")
    with s.core.db.engine.connect() as conn:
        row = conn.execute(select(outbox)).mappings().one()
        assert row["origin_kind"] == "chat" and row["state"] == "STREAM_PENDING"
    await s.core.deliver()
    await s.core.deliver()
    assert len(s.calls) == 1
    assert s.calls[0].url.path.endswith("/stream_messages")
    assert json.loads(s.calls[0].content)["input_state"] == 10
    with s.core.db.engine.connect() as conn:
        assert conn.execute(select(outbox.c.state)).scalar_one() == "SENT"


@pytest.mark.parametrize("outcome", ["accepted", "rejected", "unknown"])
async def test_delivery_streams_committed_result_without_pi_deltas(leader_stream, outcome):
    s = leader_stream
    adapter = s.core.adapters["qq"]
    adapter.config = adapter.config.model_copy(update={"typing_enabled": True})
    original = adapter.client._transport.handler

    def respond(request):
        if outcome == "rejected" and request.url.path.endswith("/stream_messages"):
            s.calls.append(request)
            return httpx.Response(403)
        return original(request)

    adapter.client._transport = httpx.MockTransport(respond)
    claim = s.core.sessions.claim("fixture")
    await s.core.deliver()
    typing = json.loads(s.calls[0].content)
    assert typing["msg_type"] == 6

    answer = "检查完成。"
    finish_leader(s.core, claim, answer)
    s.fail_reads = outcome == "unknown"
    await s.core.deliver()
    await s.core.deliver()

    assert len(s.calls) == (3 if outcome == "rejected" else 2)
    assert s.calls[1].url.path.endswith("/stream_messages")
    frame = json.loads(s.calls[1].content)
    assert frame["input_state"] == 10 and frame["content_raw"] == answer
    assert frame["msg_seq"] == typing["msg_seq"] + 1
    if outcome == "rejected":
        assert s.calls[2].url.path.endswith("/messages")
        assert json.loads(s.calls[2].content)["msg_seq"] == frame["msg_seq"]
    with s.core.db.engine.connect() as conn:
        assert conn.execute(select(outbox.c.state)).scalar_one() == ("UNKNOWN" if outcome == "unknown" else "SENT")


async def test_disabled_streaming_rejects_public_events(leader_stream):
    s = leader_stream
    adapter = s.core.adapters["qq"]
    adapter.config = adapter.config.model_copy(update={"streaming_enabled": False})
    claim = s.core.sessions.claim("fixture")
    assert claim["public_answer_events"] is False
    with pytest.raises(PermissionError, match="not enabled"):
        await append(s.core, claim, 1, "public_answer_delta")
    with s.core.db.engine.connect() as conn:
        assert not conn.execute(select(stream_runs)).first()
        assert conn.execute(select(runs.c.last_seq)).scalar_one() == 0


async def test_leader_public_event_replay_requires_same_payload_and_sequence(leader_stream):
    s = leader_stream
    claim = s.core.sessions.claim("fixture")
    assert await append(s.core, claim, 1, "public_answer_delta") == {"accepted": True}
    assert await append(s.core, claim, 1, "public_answer_delta") == {"accepted": True}
    with pytest.raises(ValueError, match="conflicting event replay"):
        await append(s.core, claim, 1, "public_answer_delta", text="changed")
    with pytest.raises(ValueError, match="conflicting event replay"):
        await append(s.core, claim, 2, "public_answer_delta", event_id="event-1")
    with pytest.raises(ValueError, match="sequence gap"):
        await append(s.core, claim, 3, "public_answer_delta")
    with pytest.raises(ValueError, match="text budget"):
        await append(s.core, claim, 2, "public_answer_delta", text="x" * 10001)
    with s.core.db.engine.connect() as conn:
        assert conn.execute(select(runs.c.last_seq)).scalar_one() == 1


async def test_leader_replays_legacy_ascii_encoded_event(leader_stream):
    s = leader_stream
    claim = s.core.sessions.claim("fixture")
    payload = {"kind": "progress", "message": "旧事件"}
    identity = (claim["run_id"], claim["fencing_token"], "fixture")
    await s.core.leader.invoke("event", identity, payload, seq=1, event_id="legacy")
    with s.core.db.transaction() as conn:
        conn.execute(update(events).where(events.c.event_id == "legacy").values(payload=json.dumps(payload)))
    assert await s.core.leader.invoke("event", identity, payload, seq=1, event_id="legacy") == {"accepted": True}


async def test_leader_unknown_stream_receipt_never_resends_final_answer(leader_stream):
    s = leader_stream
    claim = s.core.sessions.claim("fixture")
    s.fail_reads = True
    await append(s.core, claim, 1, "public_answer_delta", text="Partial")
    await s.core.stream_delivery.dispatch_once()
    assert len(s.calls) == 1
    s.fail_reads = False
    await append(s.core, claim, 2, "public_answer_end", text="Partial answer")
    await append(s.core, claim, 3, "public_answer_final", text="Partial answer")
    finish_leader(s.core, claim, "Partial answer")
    await s.core.deliver()
    await s.core.deliver()
    assert len(s.calls) == 1
    with s.core.db.engine.connect() as conn:
        assert conn.execute(select(outbox.c.state)).scalar_one() == "UNKNOWN"
        assert conn.execute(select(stream_runs.c.state)).scalar_one() == "RECONCILIATION"


async def test_leader_acknowledged_stream_resumes_after_bridge_restart(leader_stream):
    s = leader_stream
    claim = s.core.sessions.claim("fixture")
    await append(s.core, claim, 1, "public_answer_delta", text="Partial")
    await s.core.stream_delivery.dispatch_once()
    s.core.stream_delivery = StreamDelivery(s.core)
    s.core.stream_delivery.recover()
    await append(s.core, claim, 2, "public_answer_end", text="Partial answer")
    await append(s.core, claim, 3, "public_answer_final", text="Partial answer")
    finish_leader(s.core, claim, "Partial answer")
    await s.core.deliver()
    assert len(s.calls) == 2
    first, final = [json.loads(req.content) for req in s.calls]
    assert first["index"] == 0 and final["index"] == 1
    assert final["stream_msg_id"] == "leader-stream"
    assert final["content_raw"] == "Partial answer" and final["input_state"] == 10
    with s.core.db.engine.connect() as conn:
        assert conn.execute(select(outbox.c.state)).scalar_one() == "SENT"


async def test_leader_host_failure_closes_prefix_without_final_output(leader_stream):
    s = leader_stream
    claim = s.core.sessions.claim("fixture")
    await append(s.core, claim, 1, "public_answer_delta", text="Partial")
    await s.core.stream_delivery.dispatch_once()
    s.core.leader.complete(claim["run_id"], claim["fencing_token"], "fixture",
        {"status": "failed", "summary": "Host stopped execution", "agent_authored": False},
        stopped=True, quiescent=False)
    await s.core.deliver()
    await s.core.deliver()
    assert len(s.calls) == 2
    final = json.loads(s.calls[-1].content)
    assert final["input_state"] == 10 and final["content_raw"] == "Partial"
    with s.core.db.engine.connect() as conn:
        assert not conn.execute(select(outbox)).first()
        assert conn.execute(select(stream_runs.c.state)).scalar_one() == "DONE"
        assert conn.execute(select(stream_messages.c.state)).scalar_one() == "CANCELLED"
    assert not s.core.stream_delivery.live


async def test_recalled_leader_input_prevents_later_stream_writes(leader_stream):
    s = leader_stream
    claim = s.core.sessions.claim("fixture")
    await append(s.core, claim, 1, "public_answer_delta", text="Partial")
    await s.core.stream_delivery.dispatch_once()
    recall = s.event.model_copy(update={"event_id": "recall", "event_type": "message.recalled"})
    s.core.inbox.accept(recall)
    await append(s.core, claim, 2, "public_answer_end", text="Partial answer")
    await s.core.stream_delivery.dispatch_once()
    assert len(s.calls) == 1
    with s.core.db.engine.connect() as conn:
        assert conn.execute(select(stream_messages.c.state)).scalar_one() == "UNKNOWN"


runtime = agent_runtime


@pytest.mark.parametrize("purpose", ["progress", "finalize"])
def test_task_leader_holds_actual_notification_for_stream_resolution(runtime, purpose):
    root = root_goal(runtime)
    if purpose == "finalize":
        worker_complete(runtime)
    else:
        worker = runtime.service.claim("worker")
        with runtime.db.transaction() as conn:
            child = conn.execute(select(tasks).where(tasks.c.id == worker["task_id"])).mappings().one()
            runtime.service.graph.event(conn, child, "worker_update", {"public_text": "检查中"}, wake=True)
        runtime.clock.advance(runtime.sessions.settings.orchestration.leader_inbox_coalesce_seconds + 1)
    runtime.service.stream_eligibility = lambda event: True
    runtime.leader.core.stream_delivery = StreamDelivery(runtime.leader.core)
    claim = runtime.sessions.claim("leader")
    assert claim["purpose"] == purpose and claim["public_answer_events"] is True
    leader_complete(runtime, claim, "已经完成" if purpose == "finalize" else "正在检查")
    with runtime.db.engine.connect() as conn:
        gate = conn.execute(select(stream_runs).where(stream_runs.c.run_id == claim["run_id"])).mappings().one()
        assert gate["task_id"] == root["task_id"]
        assert gate["result_origin_kind"] == "task"
        delivery = conn.execute(select(outbox).where(outbox.c.origin_id == gate["result_origin_id"])).mappings().one()
        assert delivery["state"] == "STREAM_PENDING"
        assert delivery["origin_kind"] == "task"


async def test_locked_pi_leader_delivers_chinese_prefix_before_upstream_finishes(leader_stream, tmp_path):
    import asyncio
    from pathlib import Path

    from elephant.harness.pool import SessionPool
    from elephant.jobs.worker import GeneralRun, Spool, Worker
    from elephant.providers import ModelCapacity
    from elephant.runtime.scheduler import RuntimeClient

    pi = Path("node_modules/.bin/pi").resolve()
    if not pi.exists():
        pytest.skip("locked pi package is not installed")
    s = leader_stream
    release = asyncio.Event()
    first_text = "第一段中文正文已经开始发送。"
    answer = first_text + "现在生成完毕。"
    first = {"id": "fixture", "object": "chat.completion.chunk", "model": "offline", "created": 1,
        "choices": [{"index": 0, "delta": {"role": "assistant", "content": first_text}, "finish_reason": None}]}
    last = {**first, "choices": [{"index": 0, "delta": {"content": "现在生成完毕。"}, "finish_reason": None}]}
    end = {**first, "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
        "usage": {"prompt_tokens": 300, "completion_tokens": 30}}

    class PausedAnswer(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield ("data: " + json.dumps(first, ensure_ascii=False) + "\n\n").encode()
            await asyncio.wait_for(release.wait(), 30)
            yield ("data: " + json.dumps(last, ensure_ascii=False) + "\n\ndata: "
                + json.dumps(end) + "\n\ndata: [DONE]\n\n").encode()

    def upstream(request):
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=PausedAnswer())

    client = RuntimeClient(s.core, s.core.leader)
    args = SimpleNamespace(workspace=str(tmp_path / "workers"), archive=str(tmp_path / "artifacts"),
        pi=str(pi), model="offline", model_endpoint="https://model.invalid/v1/chat/completions", worker_id="fixture")

    def factory(claim, process_id, session_id):
        runner = GeneralRun(args, claim, client, process_id, session_id, ModelCapacity())
        runner.gateway.transport = httpx.MockTransport(upstream)
        return runner

    pool = SessionPool(factory)
    worker = Worker(client, "fixture", Spool(tmp_path / "spool"), None, pool=pool)
    execution = asyncio.create_task(worker.once())
    try:
        async with asyncio.timeout(20):
            while not s.calls:
                assert not execution.done(), "Leader finished before delivering its first streaming frame"
                await s.core.stream_delivery.dispatch_once()
                await asyncio.sleep(0.02)
        first_frame = json.loads(s.calls[0].content)
        assert first_frame["input_state"] == 1 and first_frame["content_raw"] == first_text
        assert not execution.done()
        release.set()
        assert await asyncio.wait_for(execution, 20)
        await s.core.deliver()
        frames = [json.loads(req.content) for req in s.calls]
        assert frames[-1]["input_state"] == 10 and frames[-1]["content_raw"] == answer
        assert frames[-1]["stream_msg_id"] == "leader-stream"
        assert all(frame["msg_seq"] == first_frame["msg_seq"] for frame in frames)
        assert all(req.url.path.endswith("/stream_messages") for req in s.calls)
        with s.core.db.engine.connect() as conn:
            assert conn.execute(select(outbox.c.state)).scalar_one() == "SENT"
        count = len(s.calls)
        await s.core.deliver()
        assert len(s.calls) == count
    finally:
        release.set()
        if not execution.done():
            execution.cancel()
        try:
            await asyncio.wait_for(asyncio.gather(execution, return_exceptions=True), 30)
        finally:
            await asyncio.wait_for(pool.close(), 30)


async def test_unknown_leader_host_failure_blocks_source_retry_but_admits_new_input(leader_stream):
    s = leader_stream
    claim = s.core.sessions.claim("fixture")
    s.fail_reads = True
    await append(s.core, claim, 1, "public_answer_delta", text="Partial")
    await s.core.stream_delivery.dispatch_once()
    s.fail_reads = False
    s.core.leader.complete(claim["run_id"], claim["fencing_token"], "fixture",
        {"status": "failed", "summary": "Host stopped execution", "agent_authored": False},
        stopped=True, quiescent=False)
    assert s.core.sessions.claim("fixture") is None
    await s.core.stream_delivery.dispatch_once()
    with s.core.db.engine.connect() as conn:
        assert conn.execute(select(stream_runs.c.state)).scalar_one() == "RECONCILIATION"
        assert not conn.execute(select(outbox)).first()
    assert s.core.sessions.claim("fixture") is None
    new_input = s.event.model_copy(update={"event_id": "independent-input", "platform_message_id": "independent-message",
        "reply_context": s.event.reply_context.model_copy(update={"platform_message_id": "independent-message"})})
    s.core.inbox.accept(new_input)
    await s.core.process(new_input)
    new_claim = s.core.sessions.claim("fixture")
    assert new_claim is not None
    assert set(new_claim["source_ids"]).isdisjoint(claim["source_ids"])
    assert len(s.calls) == 1


@pytest.mark.parametrize("unknown", [False, True])
async def test_recovered_leader_without_result_settles_stream_and_fences_source_retry(leader_stream, unknown):
    from elephant.storage.agent_schema import dialogue_inputs

    s = leader_stream
    claim = s.core.sessions.claim("fixture")
    s.fail_reads = unknown
    await append(s.core, claim, 1, "public_answer_delta", text="Partial")
    await s.core.stream_delivery.dispatch_once()
    s.fail_reads = False
    with s.core.db.transaction() as conn:
        conn.execute(update(runs).where(runs.c.id == claim["run_id"]).values(
            state="RECOVERED", slot_reserved=0, result_json=None))
        conn.execute(update(dialogue_inputs).where(dialogue_inputs.c.id.in_(claim["source_ids"])).values(
            claimed_attempt_id=None))
    assert s.core.sessions.claim("fixture") is None
    await s.core.stream_delivery.dispatch_once()
    await s.core.stream_delivery.dispatch_once()
    assert len(s.calls) == (1 if unknown else 2)
    with s.core.db.engine.connect() as conn:
        assert conn.execute(select(stream_runs.c.state)).scalar_one() == ("RECONCILIATION" if unknown else "DONE")
        assert conn.execute(select(stream_messages.c.state)).scalar_one() == ("UNKNOWN" if unknown else "CANCELLED")
        assert not conn.execute(select(outbox)).first()
    assert not s.core.stream_delivery.live
    if unknown:
        assert s.core.sessions.claim("fixture") is None
    else:
        close = json.loads(s.calls[-1].content)
        assert close["input_state"] == 10 and close["content_raw"] == "Partial"
        assert s.core.sessions.claim("fixture") is not None


@pytest.mark.parametrize("status", ["completed", "failed"])
async def test_task_leader_delivers_final_answer_through_native_stream(leader_stream, status):
    s = leader_stream
    user = s.core.sessions.claim("fixture")
    with s.core.db.transaction() as conn:
        run, claim = s.core.leader.lease(conn, user["run_id"], user["fencing_token"], "fixture")
        root = s.core.leader.task_tool(conn, run, claim, {"operation": "tasks.submit", "call_id": "dispatch",
            "arguments": {"goal": "检查项目", "children": [{"goal": "检查", "delegation_key": "only"}]}})
    worker = s.core.tasks.claim("worker")
    s.core.tasks.submit(worker["run_id"], worker["fencing_token"], "worker",
        {"status": "completed", "summary": "检查完成"}, receipt={"kind": "turn_finished",
            "session_id": worker["context"]["session_id"], "run_id": worker["run_id"], "session_revision": "leaf",
            "input_ids": worker["context"]["source_input_ids"], "inflight": 0, "tools_paired": True})
    finish_leader(s.core, user, "已接受")
    await s.core.deliver()
    s.calls.clear()
    finalizer = s.core.sessions.claim("fixture")
    assert finalizer["purpose"] == "finalize"
    await append(s.core, finalizer, 1, "public_answer_delta", "检查")
    await s.core.stream_delivery.dispatch_once()
    await append(s.core, finalizer, 2, "public_answer_end", "检查完毕")
    await append(s.core, finalizer, 3, "public_answer_final", "检查完毕")
    s.core.leader.complete(finalizer["run_id"], finalizer["fencing_token"], "fixture",
        {"status": status, "summary": "检查完毕"}, stopped=True, quiescent=False,
        receipt={"kind": "turn_finished", "session_id": finalizer["session_id"], "run_id": finalizer["run_id"],
                 "session_revision": "leaf", "input_ids": finalizer["source_ids"], "inflight": 0, "tools_paired": True})
    await s.core.deliver()
    await s.core.deliver()
    assert len(s.calls) == 2
    assert all(call.url.path.endswith("/stream_messages") for call in s.calls)
    final = json.loads(s.calls[-1].content)
    assert final["input_state"] == 10 and final["content_raw"] == "检查完毕"
    with s.core.db.engine.connect() as conn:
        gate = conn.execute(select(stream_runs).where(stream_runs.c.run_id == finalizer["run_id"])).mappings().one()
        assert gate["task_id"] == root["task_id"] and gate["state"] == "DONE"
        assert conn.execute(select(outbox.c.state).where(
            outbox.c.origin_id == gate["result_origin_id"])).scalar_one() == "SENT"


@pytest.mark.parametrize("markdown", [False, True])
async def test_final_stream_uses_last_reply_slot_without_reserving_duplicate_fallback(leader_stream, markdown):
    s = leader_stream
    adapter = s.core.adapters["qq"]
    adapter.config = adapter.config.model_copy(update={"markdown_enabled": markdown})
    s.core.outbox.markdown_adapters = frozenset({"qq"}) if markdown else frozenset()
    with s.core.db.transaction() as conn:
        conn.execute(reply_counters.insert().values(key=stable_id(s.event.conversation.key,
            s.event.platform_message_id), seq=3))
    claim = s.core.sessions.claim("fixture")
    answer = "第一段。\n\n第二段。"
    await append(s.core, claim, 1, "public_answer_end", answer)
    await append(s.core, claim, 2, "public_answer_final", answer)
    finish_leader(s.core, claim, answer)
    with s.core.db.engine.connect() as conn:
        assert conn.execute(select(reply_counters.c.seq)).scalar_one() == 3
    await s.core.deliver()
    await s.core.deliver()
    assert len(s.calls) == 1 and s.calls[0].url.path.endswith("/stream_messages")
    assert json.loads(s.calls[0].content)["msg_seq"] == 4
    with s.core.db.engine.connect() as conn:
        assert set(conn.execute(select(outbox.c.state)).scalars()) == {"SENT"}
        assert conn.execute(select(reply_counters.c.seq)).scalar_one() == 4


@pytest.mark.parametrize("markdown", [False, True])
async def test_rejected_native_stream_reuses_unused_last_sequence_for_normal_fallback(leader_stream, markdown):
    s = leader_stream
    adapter = s.core.adapters["qq"]
    adapter.config = adapter.config.model_copy(update={"markdown_enabled": markdown})
    s.core.outbox.markdown_adapters = frozenset({"qq"}) if markdown else frozenset()
    original = adapter.client._transport.handler

    def reject_native(request):
        if request.url.path.endswith("/stream_messages"):
            s.calls.append(request)
            return httpx.Response(403, json={"code": 403, "message": "stream unavailable"})
        return original(request)

    adapter.client._transport = httpx.MockTransport(reject_native)
    with s.core.db.transaction() as conn:
        conn.execute(reply_counters.insert().values(key=stable_id(s.event.conversation.key,
            s.event.platform_message_id), seq=3))
    claim = s.core.sessions.claim("fixture")
    answer = "你好。\n\n这是第二段。"
    await append(s.core, claim, 1, "public_answer_end", answer)
    await append(s.core, claim, 2, "public_answer_final", answer)
    finish_leader(s.core, claim, answer)
    await s.core.deliver()
    await s.core.deliver()
    assert len(s.calls) == 2
    assert s.calls[0].url.path.endswith("/stream_messages") and s.calls[1].url.path.endswith("/messages")
    assert [json.loads(call.content)["msg_seq"] for call in s.calls] == [4, 4]
    body = json.loads(s.calls[1].content)
    assert (body["markdown"]["content"] if markdown else body["content"]) == answer
    with s.core.db.engine.connect() as conn:
        assert conn.execute(select(outbox.c.state)).scalar_one() == "SENT"


@pytest.mark.parametrize("expired", [False, True])
async def test_leader_final_result_uses_proactive_delivery_after_reply_unavailable(leader_stream, expired):
    s = leader_stream
    adapter = s.core.adapters["qq"]
    adapter.config = adapter.config.model_copy(update={"proactive_verified": True})
    claim = s.core.sessions.claim("fixture")
    finish_leader(s.core, claim, "安装已完成，验证通过。")
    if expired:
        s.core.clock.advance(301)
    else:
        with s.core.db.transaction() as conn:
            conn.execute(reply_counters.insert().values(key=stable_id(s.event.conversation.key,
                s.event.platform_message_id), seq=4))
    await s.core.deliver()
    await s.core.deliver()
    assert len(s.calls) == 1 and s.calls[0].url.path.endswith("/messages")
    body = json.loads(s.calls[0].content)
    assert "msg_id" not in body and "msg_seq" not in body
    with s.core.db.engine.connect() as conn:
        row = conn.execute(select(outbox)).mappings().one()
        envelope = json.loads(row["envelope_json"])
        assert row["state"] == "SENT" and envelope["allow_proactive"]
        assert (envelope["expires_at"] > row["created_at"])


async def test_expired_acknowledged_prefix_allows_final_proactive_result(leader_stream):
    s = leader_stream
    adapter = s.core.adapters["qq"]
    adapter.config = adapter.config.model_copy(update={"proactive_verified": True})
    claim = s.core.sessions.claim("fixture")
    await append(s.core, claim, 1, "public_answer_delta", "正在核验")
    await s.core.stream_delivery.dispatch_once()
    finish_leader(s.core, claim, "安装完成，实际核验通过。")
    s.core.clock.advance(301)
    await s.core.deliver()
    await s.core.deliver()
    assert len(s.calls) == 2
    assert s.calls[0].url.path.endswith("/stream_messages")
    assert s.calls[1].url.path.endswith("/messages")
    assert "msg_id" not in json.loads(s.calls[1].content)
    with s.core.db.engine.connect() as conn:
        assert conn.execute(select(outbox.c.state)).scalar_one() == "SENT"
        assert conn.execute(select(stream_messages.c.state)).scalar_one() == "ABANDONED"


async def test_protocol_recovery_failure_settles_input_and_delivers_host_report(leader_stream):
    from elephant.storage.agent_schema import dialogue_inputs

    s = leader_stream
    adapter = s.core.adapters["qq"]
    adapter.config = adapter.config.model_copy(update={"proactive_verified": True})
    claim = s.core.sessions.claim("fixture")
    with s.core.db.transaction() as conn:
        conn.execute(reply_counters.insert().values(key=stable_id(s.event.conversation.key,
            s.event.platform_message_id), seq=4))
    s.core.leader.complete(claim["run_id"], claim["fencing_token"], "fixture",
        {"status": "failed", "agent_authored": False, "summary": "工具记录已保存，模型纠正失败。",
         "limitations": ["model_tool_protocol_recovery_failed"]}, stopped=True, quiescent=False)
    assert s.core.sessions.claim("fixture") is None
    await s.core.deliver()
    with s.core.db.engine.connect() as conn:
        assert conn.execute(select(dialogue_inputs.c.handled_at)).scalar_one()
        assert conn.execute(select(outbox.c.state)).scalar_one() == "SENT"
    assert len(s.calls) == 1 and "msg_id" not in json.loads(s.calls[0].content)


async def test_new_ingress_retriggers_persisted_chat_result(leader_stream):
    from datetime import timedelta

    from elephant.domain import TextSegment

    s = leader_stream
    claim = s.core.sessions.claim("fixture")
    finish_leader(s.core, claim, "安装完成")
    s.core.clock.advance(301)
    await s.core.deliver()
    with s.core.db.engine.connect() as conn:
        assert conn.execute(select(outbox.c.state)).scalar_one() == "WAITING_FOR_TRIGGER"
    trigger = s.event.model_copy(update={"event_id": "followup", "platform_message_id": "followup-message",
        "occurred_at": s.core.clock.now(), "received_at": s.core.clock.now(),
        "segments": (TextSegment(text="好了吗"),), "reply_context": s.event.reply_context.model_copy(update={
            "platform_message_id": "followup-message", "expires_at": s.core.clock.now() + timedelta(minutes=5)})})
    trigger, _ = s.core.inbox.accept(trigger)
    await s.core.process(trigger)
    await s.core.deliver()
    assert len(s.calls) == 1 and json.loads(s.calls[0].content)["msg_id"] == "followup-message"
    with s.core.db.engine.connect() as conn:
        assert conn.execute(select(outbox.c.state)).scalar_one() == "SENT"


@pytest.mark.parametrize("disposition", ["covered", "covered_with_primary", "deferred", "unseen", "ambiguous", "invalid", "independent"])
async def test_leader_publication_respects_steering_receipt(leader_stream, disposition):
    from elephant.domain import TextSegment
    from elephant.storage.agent_schema import dialogue_inputs, dialogues, leader_events

    s = leader_stream
    with s.core.db.engine.connect() as conn:
        dialogue = conn.execute(select(dialogues.c.id)).scalar_one()
    config = s.core.settings.orchestration.model_copy(update={"leader_steering_dialogues": (dialogue,)})
    s.core.sessions.settings = s.core.settings.model_copy(update={"orchestration": config})
    claim = s.core.sessions.claim("fixture")
    assert not claim["public_answer_events"]
    extra = s.event.model_copy(update={"event_id": "additional-input", "platform_message_id": "additional-message",
        "reply_to": s.event.platform_message_id if disposition != "independent" else None,
        "segments": (TextSegment(text="重点测乱序"),),
        "reply_context": s.event.reply_context.model_copy(update={"platform_message_id": "additional-message"})})
    extra, _ = s.core.inbox.accept(extra)
    await s.core.process(extra)
    identity = (claim["run_id"], claim["fencing_token"], "fixture")
    receipt = {"kind": "turn_finished", "session_id": claim["session_id"], "run_id": claim["run_id"],
               "session_revision": "leaf", "input_ids": list(claim["source_ids"]), "inflight": 0, "tools_paired": True,
               "summary": "已记录测试要求。"}
    if disposition == "independent":
        assert await s.core.leader.invoke("inputs", identity) == []
    elif disposition != "unseen":
        candidates = await s.core.leader.invoke("inputs", identity)
        assert [item["id"] for item in candidates] == [extra.event_id]
        assert "重点测乱序" in candidates[0]["text"]
        receipt["input_ids"].append(extra.event_id)
        if disposition in {"covered", "covered_with_primary"}:
            covered_ids = [extra.event_id]
            if disposition == "covered_with_primary":
                covered_ids.extend(claim["source_ids"])
            receipt["outcome"] = {"covered_input_ids": covered_ids}
        elif disposition == "deferred":
            receipt["outcome"] = {"deferred_input_ids": [extra.event_id]}
        elif disposition == "invalid":
            receipt["outcome"] = {"covered_input_ids": ["unverified-input"]}
    result = {"status": "completed", "summary": "已记录测试要求。"}
    s.core.leader.complete(*identity, result, stopped=True, quiescent=False, receipt=receipt)
    s.core.leader.complete(*identity, result, stopped=True, quiescent=False, receipt=receipt)
    with s.core.db.engine.connect() as conn:
        published = conn.execute(select(outbox.c.id)).scalars().all()
        handled = conn.execute(select(dialogue_inputs.c.id).where(dialogue_inputs.c.handled_at.is_not(None))).scalars().all()
        sequenced = conn.execute(select(leader_events.c.source_id).order_by(leader_events.c.seq)).scalars().all()
    assert sequenced == [*claim["source_ids"], extra.event_id]
    if disposition in {"unseen", "ambiguous", "invalid"}:
        assert not handled and not published
        retry = s.core.sessions.claim("fixture")
        assert retry["source_ids"] == claim["source_ids"]
        assert retry.get("steer_without_supplements", False) == (disposition in {"ambiguous", "invalid"})
    else:
        assert len(published) == 1
        includes_extra = disposition in {"covered", "covered_with_primary"}
        assert set(handled) == set(receipt["input_ids"] if includes_extra else claim["source_ids"])
        if disposition in {"deferred", "independent"}:
            assert s.core.sessions.claim("fixture")["source_ids"] == [extra.event_id]


async def test_core_streaming_and_regular_delivery_share_target_slots(leader_stream):
    import asyncio

    s = leader_stream
    adapter = s.core.adapters["qq"]
    adapter.config = adapter.config.model_copy(update={"allowed_users": ("300", "301"), "proactive_verified": True})
    target = s.event.conversation
    other = target.model_copy(update={"remote_conversation_id": "301"})
    claim = s.core.sessions.claim("fixture")
    await append(s.core, claim, 1, "public_answer_delta", "正在检查")
    entered, release, independent = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def request(req):
        if req.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "fixture", "expires_in": 7200})
        s.calls.append(req)
        if req.url.path.endswith("/stream_messages"):
            entered.set()
            await release.wait()
        elif req.url.path == "/v2/users/301/messages":
            independent.set()
        return httpx.Response(200, json={"id": req.url.path})

    adapter.client._transport = httpx.MockTransport(request)
    await s.core.deliver(wait=False)
    try:
        await asyncio.wait_for(entered.wait(), 3)
        with s.core.db.transaction() as conn:
            s.core.outbox.enqueue_text(conn, target=target, text="same",
                origin_kind="command", origin_id="same", allow_proactive=True, ttl_seconds=3600)
            s.core.outbox.enqueue_text(conn, target=other, text="other",
                origin_kind="command", origin_id="other", allow_proactive=True, ttl_seconds=3600)
        await s.core.deliver(wait=False)
        await asyncio.wait_for(independent.wait(), 3)
        assert not any(req.url.path == "/v2/users/300/messages" for req in s.calls)
    finally:
        release.set()
        await s.core.deliver()
    assert [req.url.path for req in s.calls if "/300/" in req.url.path] == ["/v2/users/300/stream_messages"]
    await append(s.core, claim, 2, "public_answer_end", "正在检查")
    await s.core.deliver()
    assert [req.url.path for req in s.calls if "/300/" in req.url.path] == [
        "/v2/users/300/stream_messages", "/v2/users/300/stream_messages", "/v2/users/300/messages",
    ]


async def test_real_pi_steer_merges_supplement_before_final_publication(leader_stream, tmp_path):
    import asyncio
    from pathlib import Path

    from elephant.harness.pool import SessionPool
    from elephant.jobs.worker import GeneralRun, Spool, Worker
    from elephant.providers import ModelCapacity
    from elephant.runtime.scheduler import RuntimeClient
    from elephant.storage.agent_schema import dialogue_inputs, dialogues, leader_events

    pi = Path("node_modules/.bin/pi").resolve()
    if not pi.exists():
        pytest.skip("locked pi package is not installed")
    s = leader_stream
    adapter = s.core.adapters["qq"]
    adapter.config = adapter.config.model_copy(update={"typing_enabled": True})
    with s.core.db.engine.connect() as conn:
        dialogue = conn.execute(select(dialogues.c.id)).scalar_one()
    config = s.core.settings.orchestration.model_copy(update={"leader_steering_dialogues": (dialogue,)})
    s.core.sessions.settings = s.core.settings.model_copy(update={"orchestration": config})
    entered, release = asyncio.Event(), asyncio.Event()
    starting, boot = asyncio.Event(), asyncio.Event()
    requests, runners = [], []
    extra_ids = []

    def frames(delta, stop):
        chunk = {"id": "steer-test", "object": "chat.completion.chunk", "model": "offline", "created": 1}
        first = {**chunk, "choices": [{"index": 0, "delta": {"role": "assistant", **delta}, "finish_reason": None}]}
        last = {**chunk, "choices": [{"index": 0, "delta": {}, "finish_reason": stop}],
                "usage": {"prompt_tokens": 300, "completion_tokens": 30}}
        return "".join("data: " + json.dumps(item, ensure_ascii=False) + "\n\n" for item in (first, last)).encode() + b"data: [DONE]\n\n"

    class PausedAnswer(httpx.AsyncByteStream):
        async def __aiter__(self):
            entered.set()
            await asyncio.wait_for(release.wait(), 15)
            yield frames({"content": "这份草稿还没有考虑补充。"}, "stop")

    def upstream(request):
        requests.append(request.content.decode())
        headers = {"content-type": "text/event-stream"}
        if len(requests) == 1:
            return httpx.Response(200, headers=headers, stream=PausedAnswer())
        if len(requests) == 2:
            call = {"index": 0, "id": "cover-input", "type": "function",
                    "function": {"name": "task_outcome",
                                 "arguments": json.dumps({"status": "completed", "covered_input_ids": extra_ids})}}
            return httpx.Response(200, headers=headers, content=frames({"tool_calls": [call]}, "tool_calls"))
        return httpx.Response(200, headers=headers, content=frames({"content": "已纳入乱序检查。"}, "stop"))

    client = RuntimeClient(s.core, s.core.leader)
    args = SimpleNamespace(workspace=str(tmp_path / "workers"), archive=str(tmp_path / "artifacts"),
        pi=str(pi), model="offline", model_endpoint="https://model.invalid/v1/chat/completions", worker_id="fixture")

    def factory(claim, process_id, session_id):
        runner = GeneralRun(args, claim, client, process_id, session_id, ModelCapacity())
        runner.gateway.transport = httpx.MockTransport(upstream)
        start = runner.start

        async def delayed_start():
            starting.set()
            await boot.wait()
            await start()

        runner.start = delayed_start
        runners.append(runner)
        return runner

    pool = SessionPool(factory)
    worker = Worker(client, "fixture", Spool(tmp_path / "spool"), None, pool=pool)
    execution = asyncio.create_task(worker.once())
    try:
        await asyncio.wait_for(starting.wait(), 15)
        for index, text in enumerate(("重点测乱序", "也要记录耗时")):
            msg = f"real-quote-{index}"
            extra = adapter.normalize({"op": 0, "id": msg, "t": "C2C_MESSAGE_CREATE", "d": {
                "id": msg, "author": {"user_openid": "300"}, "content": text,
                "timestamp": s.core.clock.now().isoformat(), "message_reference": {
                    "message_id": s.event.platform_message_id}}})
            assert [segment.type for segment in extra.segments] == ["text", "reply"]
            extra, _ = s.core.inbox.accept(extra)
            extra_ids.append(extra.event_id)
            await s.core.process(extra)
        assert not runners[0].accepting_input
        boot.set()
        await asyncio.wait_for(entered.wait(), 15)
        async with asyncio.timeout(10):
            while not set(extra_ids) <= runners[0].steered_inputs:
                assert not execution.done()
                await asyncio.sleep(0.02)
        assert "重点测乱序" not in requests[0]
        await s.core.deliver()
        assert len(s.calls) == 1 and json.loads(s.calls[0].content)["msg_type"] == 6
        release.set()
        assert await asyncio.wait_for(execution, 20)
        assert len(requests) == 3
        assert "重点测乱序" in requests[1] and "也要记录耗时" in requests[1]
        await s.core.deliver()
        assert len(s.calls) == 2 and json.loads(s.calls[1].content)["content_raw"] == "已纳入乱序检查。"
        with s.core.db.engine.connect() as conn:
            handled = conn.execute(select(dialogue_inputs.c.handled_at).where(
                dialogue_inputs.c.id.in_(extra_ids))).scalars().all()
            assert len(handled) == len(extra_ids) and all(handled)
            receipt = conn.execute(select(leader_events.c.payload_json).where(
                leader_events.c.source_kind == "delivery")).scalar_one()
            assert json.loads(receipt)["state"] == "SENT"
    finally:
        boot.set()
        release.set()
        if not execution.done():
            execution.cancel()
        await asyncio.gather(execution, return_exceptions=True)
        await pool.close()


@pytest.mark.parametrize(("started", "routed"), [(False, False), (False, True), (True, True)])
async def test_user_revision_guards_operation_start_but_preserves_started_receipt(leader_stream, started, routed):
    from elephant.domain import TextSegment
    from elephant.storage.action_schema import operations
    from elephant.storage.agent_schema import dialogues

    s = leader_stream
    with s.core.db.engine.connect() as conn:
        dialogue = conn.execute(select(dialogues.c.id)).scalar_one()
    config = s.core.settings.orchestration.model_copy(update={"leader_steering_dialogues": (dialogue,)})
    s.core.sessions.settings = s.core.settings.model_copy(update={"orchestration": config})
    claim = s.core.sessions.claim("fixture")
    identity = (claim["run_id"], claim["fencing_token"], "fixture")
    prepared = await s.core.leader.invoke("ledger", identity, {"operation": "prepare",
        "operation_id": "write-fixture", "arguments_hash": "fixture", "arguments": {},
        "action": {"name": "workspace.file.write", "effect": "write", "resources": []}})
    assert prepared["state"] == "PREPARED"
    begin = {"operation": "transition", "operation_id": "write-fixture",
             "expected": "PREPARED", "state": "DISPATCHED", "result": {}}
    if started:
        assert (await s.core.leader.invoke("ledger", identity, begin))["changed"]
    extra = s.event.model_copy(update={"event_id": "correction", "platform_message_id": "correction-message",
        "segments": (TextSegment(text="改成只读检查"),),
        "reply_context": s.event.reply_context.model_copy(update={"platform_message_id": "correction-message"})})
    extra, _ = s.core.inbox.accept(extra)
    if routed:
        await s.core.process(extra)
    if started:
        assert (await s.core.leader.invoke("ledger", identity, {**begin, "expected": "DISPATCHED",
            "state": "VERIFIED", "result": {"observed": True}}))["changed"]
    else:
        with pytest.raises(PermissionError, match="Review pending user input"):
            await s.core.leader.invoke("ledger", identity, begin)
    with s.core.db.engine.connect() as conn:
        assert conn.execute(select(operations.c.state)).scalar_one() == ("VERIFIED" if started else "PREPARED")


def test_pilot_orders_worker_result_and_user_input_in_one_dialogue(runtime):
    from elephant.domain import TextSegment
    from elephant.storage.agent_schema import dialogues, leader_events

    root_goal(runtime)
    worker_complete(runtime)
    with runtime.db.engine.connect() as conn:
        dialogue = conn.execute(select(dialogues.c.id)).scalar_one()
    config = runtime.sessions.settings.orchestration.model_copy(update={"leader_steering_dialogues": (dialogue,)})
    runtime.sessions.settings = runtime.sessions.settings.model_copy(update={"orchestration": config})
    extra = runtime.event.model_copy(update={"event_id": "after-worker",
        "segments": (TextSegment(text="现在说另一件事"),)})
    runtime.sessions.enqueue(extra, runtime.access)
    runtime.service.stream_eligibility = lambda _event: True
    worker_turn = runtime.sessions.claim("leader")
    assert worker_turn["purpose"] == "finalize" and not worker_turn["public_answer_events"]
    assert runtime.sessions.claim("second") is None
    leader_complete(runtime, worker_turn)
    with runtime.db.engine.connect() as conn:
        joined = conn.execute(select(leader_events.c.source_kind, leader_events.c.published_run_id)
            .order_by(leader_events.c.seq)).all()
        assert joined[-1] == ("user", None)
        assert any(item == ("worker", worker_turn["run_id"]) for item in joined[:-1])
    user_turn = runtime.sessions.claim("leader")
    assert user_turn["source_ids"] == [extra.event_id]
    assert user_turn["session_id"] == worker_turn["session_id"]


async def test_steered_task_revision_uses_real_supplement_provenance(runtime):
    from elephant.domain import TextSegment
    from elephant.ingress.service import Inbox
    from elephant.storage.agent_schema import dialogues
    from elephant.storage.task_schema import inputs

    root = root_goal(runtime)
    runtime.sessions.enqueue(runtime.event, runtime.access)
    with runtime.db.engine.connect() as conn:
        dialogue = conn.execute(select(dialogues.c.id)).scalar_one()
    config = runtime.sessions.settings.orchestration.model_copy(update={"leader_steering_dialogues": (dialogue,)})
    runtime.sessions.settings = runtime.sessions.settings.model_copy(update={"orchestration": config})
    claim = runtime.sessions.claim("leader")
    identity = (claim["run_id"], claim["fencing_token"], "leader")
    extra = runtime.event.model_copy(update={"event_id": "source-correction", "platform_message_id": "new",
        "reply_to": runtime.event.platform_message_id,
        "segments": (TextSegment(text="只读验证"),)})
    extra, _ = Inbox(runtime.db).accept(extra)
    runtime.sessions.enqueue(extra, runtime.access)
    assert [item["id"] for item in await runtime.leader.invoke("inputs", identity)] == [extra.event_id]
    await runtime.leader.invoke("session-receipt", identity, {"kind": "input_consumed",
        "session_id": claim["session_id"], "run_id": claim["run_id"], "session_revision": "fixture",
        "input_ids": [*claim["source_ids"], extra.event_id]})
    await runtime.leader.invoke("tasks", identity, {"operation": "tasks.send_input", "call_id": "revision-fixture",
        "arguments": {"task_ref": root["task_id"], "kind": "followup", "text": "模型改写",
                      "source_input_id": extra.event_id}})
    with runtime.db.engine.connect() as conn:
        sources = conn.execute(select(inputs.c.source_event_id, inputs.c.text).where(inputs.c.kind == "followup")).all()
        assert sources and all(source == (extra.event_id, "只读验证") for source in sources)


@pytest.mark.parametrize("children", [1, 2])
def test_new_worker_result_supersedes_draft_progress(runtime, children):
    from elephant.storage.task_schema import notifications

    root_goal(runtime, children=children)
    worker = runtime.service.claim("worker")
    with runtime.db.transaction() as conn:
        child = conn.execute(select(tasks).where(tasks.c.id == worker["task_id"])).mappings().one()
        runtime.service.graph.event(conn, child, "worker_update", {"public_text": "仍在检查"}, wake=True)
    runtime.clock.advance(3)
    progress = runtime.sessions.claim("leader")
    assert progress["purpose"] == "progress"
    runtime.service.submit(worker["run_id"], worker["fencing_token"], "worker",
        {"status": "completed", "summary": "检查完成"}, receipt={"kind": "turn_finished",
            "session_id": worker["context"]["session_id"], "run_id": worker["run_id"], "session_revision": "leaf",
            "input_ids": worker["context"]["source_input_ids"], "inflight": 0, "tools_paired": True})
    leader_complete(runtime, progress, "仍在检查")
    with runtime.db.engine.connect() as conn:
        assert not conn.scalar(select(notifications.c.id).where(notifications.c.body == "仍在检查"))
    next_claim = runtime.sessions.claim("leader")
    assert next_claim["purpose"] == ("finalize" if children == 1 else "progress")
    assert set(progress["source_ids"]) < set(next_claim["source_ids"])
    leader_complete(runtime, next_claim, "第一个子任务已完成")
    with runtime.db.engine.connect() as conn:
        assert conn.scalar(select(notifications.c.id).where(notifications.c.body == "第一个子任务已完成"))


async def test_committed_draft_ignores_future_recalled_session_source(leader_stream):
    from elephant.domain import OutboundEnvelope, TextSegment
    from elephant.storage.agent_schema import dialogues
    from elephant.storage.task_schema import sessions

    s = leader_stream
    with s.core.db.engine.connect() as conn:
        dialogue = conn.execute(select(dialogues.c.id)).scalar_one()
    config = s.core.settings.orchestration.model_copy(update={"leader_steering_dialogues": (dialogue,)})
    s.core.sessions.settings = s.core.settings.model_copy(update={"orchestration": config})
    claim = s.core.sessions.claim("fixture")
    result = {"status": "completed", "summary": "已提交的旧定稿"}
    s.core.leader.complete(claim["run_id"], claim["fencing_token"], "fixture", result, stopped=True,
        quiescent=False, receipt={"kind": "turn_finished", "summary": result["summary"],
            "session_id": claim["session_id"], "run_id": claim["run_id"], "session_revision": "leaf",
            "input_ids": claim["source_ids"], "inflight": 0, "tools_paired": True})
    later, _ = s.core.inbox.accept(s.event.model_copy(update={"event_id": "later-source",
        "platform_message_id": "later-source", "segments": (TextSegment(text="后来另一个话题"),)}))
    history = s.core.context_for(later)
    history["steering_sources"] = True
    with s.core.db.transaction() as conn:
        conn.execute(sessions.insert().values(id=claim["session_id"], scope_key=claim["session_id"],
            session_file="fixture.jsonl", epoch=1, manifest_json=json.dumps(history)))
        saved = conn.scalar(select(outbox.c.envelope_json))
    s.core.inbox.accept(later.model_copy(update={"event_id": "recall-later", "event_type": "message.recalled"}))
    envelope = OutboundEnvelope.model_validate_json(saved)
    assert not s.core.validate_context(s.core.access(s.event), {"session_id": claim["session_id"]})
    assert s.core.pre_send(envelope)


async def test_independent_input_defers_goal_without_consuming_original(runtime):
    from elephant.domain import TextSegment
    from elephant.storage.agent_schema import dialogue_inputs

    r = runtime
    queued = r.sessions.enqueue(r.event, r.access)
    config = r.sessions.settings.orchestration.model_copy(update={"leader_steering_dialogues": (queued["dialogue_id"],)})
    r.sessions.settings = r.sessions.settings.model_copy(update={"orchestration": config})

    def complete(claim, body):
        return r.leader.complete(claim["run_id"], claim["fencing_token"], "leader",
            {"status": "completed", "summary": body}, stopped=True, quiescent=False,
            receipt={"kind": "turn_finished", "summary": body, "session_id": claim["session_id"],
                     "run_id": claim["run_id"], "session_revision": "leaf", "input_ids": claim["source_ids"],
                     "inflight": 0, "tools_paired": True})

    original = r.sessions.claim("leader")
    extra = r.event.model_copy(update={"event_id": "unrelated", "platform_message_id": "unrelated-msg",
        "segments": (TextSegment(text="另一件独立的事"),)})
    r.sessions.enqueue(extra, r.access)
    payload = {"operation": "tasks.submit", "call_id": "allocation",
               "arguments": {"goal": "交给 Worker", "children": [{"goal": "计算", "delegation_key": "worker"}]}}
    first = await r.leader.invoke("tasks", (original["run_id"], original["fencing_token"], "leader"), payload)
    assert first == {"accepted": False, "deferred": True, "reason": "pending_user_input"}
    complete(original, "等待处理新消息")
    with r.db.engine.connect() as conn:
        assert not conn.scalar(select(dialogue_inputs.c.handled_at).where(dialogue_inputs.c.id == r.event.event_id))
        assert not conn.scalar(select(outbox.c.id))
        assert not conn.scalar(select(tasks.c.id))
    other = r.sessions.claim("leader")
    assert other["source_ids"] == [extra.event_id]
    complete(other, "独立消息已处理")
    retry = r.sessions.claim("leader")
    assert retry["source_ids"] == original["source_ids"]
    accepted = await r.leader.invoke("tasks", (retry["run_id"], retry["fencing_token"], "leader"), payload)
    assert accepted["task_id"]


async def test_older_outbox_row_precedes_unopened_native_stream(leader_stream):
    s = leader_stream
    adapter = s.core.adapters["qq"]
    adapter.config = adapter.config.model_copy(update={"proactive_verified": True})
    claim = s.core.sessions.claim("fixture")
    s.core.clock.advance(.1)
    with s.core.db.transaction() as conn:
        s.core.outbox.enqueue_text(conn, target=s.event.conversation, text="先入队",
            origin_kind="command", origin_id="older", allow_proactive=True)
    await append(s.core, claim, 1, "public_answer_delta", "后来的流")
    await s.core.deliver()
    await s.core.deliver()
    assert [req.url.path.rsplit("/", 1)[-1] for req in s.calls] == ["messages", "stream_messages"]


async def test_background_target_admission_stays_bounded(leader_stream):
    import asyncio

    s = leader_stream
    adapter = s.core.adapters["qq"]
    targets = tuple(str(n + 1000) for n in range(120))
    adapter.config = adapter.config.model_copy(update={"allowed_users": targets, "proactive_verified": True,
                                                        "official_bot_messages_per_minute": 600})
    entered = []
    all_entered, release = asyncio.Event(), asyncio.Event()

    async def transport(req):
        if req.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "fixture", "expires_in": 7200})
        entered.append(req.url.path)
        if len(entered) == 8:
            all_entered.set()
        await release.wait()
        return httpx.Response(200, json={"id": req.url.path})

    adapter.client._transport = httpx.MockTransport(transport)
    with s.core.db.transaction() as conn:
        for user in targets:
            target = s.event.conversation.model_copy(update={"remote_conversation_id": user})
            s.core.outbox.enqueue_text(conn, target=target, text="压力测试",
                origin_kind="command", origin_id=user, allow_proactive=True)
    try:
        await s.core.deliver(wait=False)
        await asyncio.wait_for(all_entered.wait(), 3)
        for _ in range(3):
            await s.core.deliver(wait=False)
        assert len(s.core.stream_delivery.inflight) == len(entered) == 8
    finally:
        release.set()
    for _ in range(16):
        await s.core.deliver()
    assert len(set(entered)) == len(entered) == len(targets)
