"""Leader turns share the durable public-answer stream and final outbox gate."""
import json
from types import SimpleNamespace

import httpx
import pytest
from sqlalchemy import select, update
from test_agent_runtime import leader_complete, root_goal, worker_complete
from test_agent_runtime import runtime as agent_runtime
from test_leader_core import finish_leader, make_core

from qqbot.domain import stable_id
from qqbot.jobs.streaming import StreamDelivery
from qqbot.storage.schema import outbox, reply_counters
from qqbot.storage.stream_schema import stream_messages, stream_runs
from qqbot.storage.task_schema import events, runs, tasks


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

    from qqbot.harness.pool import SessionPool
    from qqbot.jobs.worker import GeneralRun, Spool, Worker
    from qqbot.providers import ModelCapacity
    from qqbot.runtime.scheduler import RuntimeClient

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
    from qqbot.storage.agent_schema import dialogue_inputs

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
