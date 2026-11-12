"""Durable Core/worker stream bridge with mock QQ and model endpoints."""
import json
from datetime import timedelta
from types import SimpleNamespace

import httpx
import pytest
from sqlalchemy import insert, select, update

from qqbot.adapters.qq_official.adapter import OfficialAdapter
from qqbot.domain import AccessContext, ConversationRef, InboundEvent, PrincipalRef, ReplyContext, Surface
from qqbot.jobs.service import TaskService
from qqbot.jobs.streaming import StreamDelivery
from qqbot.outbox.service import Outbox
from qqbot.storage.schema import outbox, reply_counters
from qqbot.storage.stream_schema import stream_messages, stream_runs


@pytest.fixture
def stream_setup(db, clock, incoming, official_config):
    calls = []
    def request(req):
        if req.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "fixture", "expires_in": 7200})
        calls.append(json.loads(req.content))
        return httpx.Response(200, json={"id": "stream-1", "remain_msg_len": 10000})
    client = httpx.AsyncClient(transport=httpx.MockTransport(request))
    cfg = official_config.model_copy(update={"streaming_enabled": True})
    adapter = OfficialAdapter(cfg, "d", incoming, clock, client)
    conversation = ConversationRef(deployment_id="d", adapter_instance_id="qq", bot_identity_id="bot",
                                   surface=Surface.PRIVATE, remote_conversation_id="300")
    principal = PrincipalRef(adapter_instance_id="qq", identity_namespace="qq_app:app", remote_user_id="300")
    event = InboundEvent(event_id="e", event_type="message.created", conversation=conversation,
                         principal=principal, platform_message_id="m", occurred_at=clock.now(),
                         received_at=clock.now(), trace_id="t", reply_context=ReplyContext(
                             platform_message_id="m", expires_at=clock.now() + timedelta(minutes=5), max_replies=4))
    core = SimpleNamespace(db=db, clock=clock, adapters={"qq": adapter}, allowed=True)
    core.pre_send = lambda envelope, **kwargs: core.allowed
    bridge = StreamDelivery(core)
    service = TaskService(db, clock, Outbox(db, clock), stream_eligibility=bridge.eligible)
    task = service.accept(event, AccessContext(principal=principal, conversation=conversation), "hello")
    claim = service.claim("worker")
    assert claim["public_answer_events"] is True
    return SimpleNamespace(core=core, bridge=bridge, service=service, task=task, claim=claim,
                           calls=calls, client=client, event=event)


def append(setup, seq, kind, text="Hello", message_id="answer"):
    payload = {"kind": kind, "message_id": message_id, "text": text}
    return setup.service.append_event(setup.claim["run_id"], setup.claim["fencing_token"], "worker",
                                      f"event-{seq}", seq, payload)


def submit(setup, text="Hello"):
    return setup.service.submit(setup.claim["run_id"], setup.claim["fencing_token"], "worker",
                                {"status": "completed", "summary": text})


def states(setup):
    with setup.core.db.engine.connect() as c:
        return c.execute(select(outbox.c.state)).scalars().all()


async def test_live_prefix_then_final_is_delivered_once(stream_setup):
    s = stream_setup
    append(s, 1, "public_answer_start", "")
    append(s, 2, "public_answer_delta", "Hel")
    await s.bridge.dispatch_once()
    assert [call["content_raw"] for call in s.calls] == ["Hel"]
    assert s.calls[0]["index"] == 0
    append(s, 3, "public_answer_end", "Hello")
    append(s, 4, "public_answer_final", "Hello")
    submit(s)
    assert states(s) == ["STREAM_PENDING"]
    await s.bridge.dispatch_once()
    assert states(s) == ["SENT"]
    assert len(s.calls) == 2 and s.calls[-1]["input_state"] == 10
    assert s.calls[0]["msg_seq"] == s.calls[-1]["msg_seq"]
    assert s.calls[-1]["stream_msg_id"] == "stream-1"
    await s.bridge.dispatch_once()
    assert len(s.calls) == 2
    await s.client.aclose()


async def test_fast_completion_held_until_matching_final_ack(stream_setup):
    s = stream_setup
    append(s, 1, "public_answer_end")
    append(s, 2, "public_answer_final")
    submit(s)
    await s.bridge.dispatch_once()
    assert states(s) == ["SENT"]
    assert len(s.calls) == 1 and s.calls[0]["input_state"] == 10
    await s.client.aclose()


async def test_final_without_final_marker_does_not_suppress_result(stream_setup):
    s = stream_setup
    append(s, 1, "public_answer_end")
    submit(s)
    await s.bridge.dispatch_once()
    assert states(s) == ["PENDING"]
    await s.client.aclose()


async def test_stream_unknown_does_not_fallback_or_retry(stream_setup):
    s = stream_setup
    def fail(request):
        if request.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "fixture", "expires_in": 7200})
        s.calls.append(json.loads(request.content))
        raise httpx.ReadTimeout("receipt lost")
    await s.client.aclose()
    s.core.adapters["qq"].client = httpx.AsyncClient(transport=httpx.MockTransport(fail))
    append(s, 1, "public_answer_end")
    append(s, 2, "public_answer_final")
    submit(s)
    await s.bridge.dispatch_once()
    assert states(s) == ["UNKNOWN"]
    await s.bridge.dispatch_once()
    assert len(s.calls) == 1
    await s.core.adapters["qq"].client.aclose()


async def test_recover_inflight_write_is_unknown(stream_setup):
    s = stream_setup
    row = s.bridge._create(s.claim["run_id"], "answer")
    with s.core.db.transaction() as c:
        c.execute(update(stream_messages).where(stream_messages.c.id == row["id"]).values(state="SENDING"))
    submit(s)
    s.bridge.recover()
    await s.bridge.dispatch_once()
    assert states(s) == ["UNKNOWN"] and not s.calls
    await s.client.aclose()


async def test_revoked_permission_prevents_stream(stream_setup):
    s = stream_setup
    append(s, 1, "public_answer_delta")
    s.core.allowed = False
    await s.bridge.dispatch_once()
    assert not s.calls
    with s.core.db.engine.connect() as c:
        assert c.execute(select(stream_runs.c.state)).scalar_one() == "DISABLED"
    await s.client.aclose()


def test_public_payload_is_bounded_and_opt_in(stream_setup):
    s = stream_setup
    with pytest.raises(ValueError, match="budget"):
        append(s, 1, "public_answer_delta", "x" * 10001)
    with s.core.db.transaction() as c:
        c.execute(stream_runs.delete())
    with pytest.raises(PermissionError, match="enabled"):
        append(s, 1, "public_answer_delta")


async def test_reply_sequence_respects_existing_outbox_reservations(stream_setup):
    s = stream_setup
    from qqbot.domain import stable_id
    with s.core.db.transaction() as c:
        c.execute(insert(reply_counters).values(key=stable_id(s.event.conversation.key, "m"), seq=2))
    append(s, 1, "public_answer_end")
    await s.bridge.dispatch_once()
    assert s.calls[0]["msg_seq"] == 3
    await s.client.aclose()


async def test_group_not_opted_in(stream_setup):
    s = stream_setup
    group = s.event.model_copy(update={"conversation": s.event.conversation.model_copy(update={"surface": Surface.GROUP})})
    assert not s.bridge.eligible(group)
    await s.client.aclose()


async def test_cumulative_replacement_is_rejected(stream_setup):
    s = stream_setup
    append(s, 1, "public_answer_delta", "Hello")
    await s.bridge.dispatch_once()
    append(s, 2, "public_answer_end", "Different")
    await s.bridge.dispatch_once()
    assert len(s.calls) == 2
    assert s.calls[-1]["input_state"] == 10
    assert s.calls[-1]["content_raw"] == "Hello"
    with s.core.db.engine.connect() as c:
        assert c.execute(select(stream_runs.c.error)).scalar_one() == "PUBLIC_ANSWER_REPLACED"
    await s.client.aclose()


async def test_typing_is_once_and_uses_a_distinct_reply_sequence(stream_setup):
    s = stream_setup
    adapter = s.core.adapters["qq"]
    adapter.config = adapter.config.model_copy(update={"typing_enabled": True})
    await s.bridge.dispatch_once()
    await s.bridge.dispatch_once()
    assert len(s.calls) == 1 and s.calls[0]["msg_type"] == 6
    append(s, 1, "public_answer_end")
    append(s, 2, "public_answer_final")
    await s.bridge.dispatch_once()
    assert len(s.calls) == 2 and s.calls[1]["msg_seq"] > s.calls[0]["msg_seq"]
    submit(s)
    await s.bridge.dispatch_once()
    assert states(s) == ["SENT"]
    await s.client.aclose()


async def test_typing_only_does_not_stream_answer(stream_setup):
    s = stream_setup
    adapter = s.core.adapters["qq"]
    adapter.config = adapter.config.model_copy(update={"typing_enabled": True, "streaming_enabled": False})
    await s.bridge.dispatch_once()
    append(s, 1, "public_answer_end")
    append(s, 2, "public_answer_final")
    submit(s)
    await s.bridge.dispatch_once()
    assert len(s.calls) == 1 and s.calls[0]["msg_type"] == 6
    assert states(s) == ["PENDING"]
    await s.client.aclose()


async def test_restart_after_ack_resumes_next_index_without_repeating_prefix(stream_setup):
    s = stream_setup
    append(s, 1, "public_answer_delta", "Hel")
    await s.bridge.dispatch_once()
    bridge = StreamDelivery(s.core)
    bridge.recover()
    append(s, 2, "public_answer_end", "Hello")
    append(s, 3, "public_answer_final", "Hello")
    submit(s)
    await bridge.dispatch_once()
    assert len(s.calls) == 2
    assert s.calls[1]["index"] == 1
    assert s.calls[1]["stream_msg_id"] == "stream-1"
    assert states(s) == ["SENT"]
    await s.client.aclose()


async def test_conflicting_replay_not_accepted(stream_setup):
    s = stream_setup
    append(s, 1, "public_answer_delta", "Hello")
    assert append(s, 1, "public_answer_delta", "Hello") == {"acknowledged": 1}
    with pytest.raises(ValueError, match="conflicting"):
        append(s, 1, "public_answer_delta", "Changed")
    await s.bridge.dispatch_once()
    assert len(s.calls) == 1
    await s.client.aclose()


async def test_locked_pi_worker_to_native_qq_final_without_duplicate(stream_setup, tmp_path):
    """Real locked Pi RPC -> durable worker spool -> TaskService -> QQ mock API."""
    from pathlib import Path

    from fastapi import FastAPI

    from qqbot.harness.pool import SessionPool
    from qqbot.jobs.api import internal_router
    from qqbot.jobs.worker import GeneralRun, Spool, Worker
    from qqbot.providers import ModelCapacity
    from qqbot.storage.task_schema import events, runs, tasks

    s = stream_setup
    pi = Path("node_modules/.bin/pi").resolve()
    if not pi.exists():
        pytest.skip("locked pi package is not installed")
    # This fixture's claim is replaced with the worker-owned claim in this test.
    with s.core.db.transaction() as c:
        c.execute(stream_runs.delete())
        c.execute(runs.delete())
        c.execute(update(tasks).values(state="QUEUED", fencing=0))
    response_text = "This is the public answer from Pi."
    event = {"id": "fixture", "object": "chat.completion.chunk", "model": "offline", "created": 1,
             "choices": [{"index": 0, "delta": {"role": "assistant", "content": response_text},
                          "finish_reason": None}]}
    end = {**event, "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
           "usage": {"prompt_tokens": 300, "completion_tokens": 12}}
    def upstream(request):
        return httpx.Response(200, headers={"content-type": "text/event-stream"},
            text="data: " + json.dumps(event) + "\n\ndata: " + json.dumps(end) + "\n\ndata: [DONE]\n\n")
    app = FastAPI()
    app.include_router(internal_router(s.service, "t" * 32))
    args = SimpleNamespace(workspace=str(tmp_path / "workers"), archive=str(tmp_path / "artifacts"),
        pi=str(pi), model="offline", model_endpoint="https://model.invalid/v1/chat/completions", worker_id="worker")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://core",
        headers={"Authorization": "Bearer " + "t" * 32}) as client:
        def factory(claim, process_id, session_id):
            runner = GeneralRun(args, claim, client, process_id, session_id, ModelCapacity())
            runner.gateway.transport = httpx.MockTransport(upstream)
            return runner
        pool = SessionPool(factory)
        worker = Worker(client, "worker", Spool(tmp_path / "spool"), None, pool=pool)
        try:
            assert await worker.once()
            with s.core.db.engine.connect() as c:
                assert c.execute(select(tasks.c.state)).scalar_one() == "SUCCEEDED"
                payloads = [json.loads(v) for v in c.execute(select(events.c.payload)).scalars()]
                assert any(v.get("kind") == "public_answer_final" for v in payloads)
            assert states(s) == ["STREAM_PENDING"]
            await s.bridge.dispatch_once()
            assert states(s) == ["SENT"]
            assert s.calls[-1]["content_raw"] == response_text and s.calls[-1]["input_state"] == 10
            count = len(s.calls)
            await s.bridge.dispatch_once()
            assert len(s.calls) == count
        finally:
            await pool.close()
            await s.client.aclose()


async def test_persisted_server_length_budget_prevents_oversend(stream_setup):
    s = stream_setup
    append(s, 1, "public_answer_delta", "Hel")
    await s.bridge.dispatch_once()
    with s.core.db.transaction() as c:
        c.execute(update(stream_messages).values(remain_msg_len=1))
    bridge = StreamDelivery(s.core)
    bridge.recover()
    append(s, 2, "public_answer_end", "Hello")
    submit(s)
    await bridge.dispatch_once()
    assert len(s.calls) == 2 and s.calls[-1]["input_state"] == 10
    assert s.calls[-1]["content_raw"] == "Hel"
    assert states(s) == ["PENDING"]
    await s.client.aclose()


async def test_revocation_during_final_throttle_prevents_network_write(stream_setup):
    s = stream_setup
    append(s, 1, "public_answer_delta", "Hel")
    await s.bridge.dispatch_once()
    transport = next(iter(s.bridge.live.values()))
    async def revoke(_delay):
        s.core.allowed = False
    transport.sleep = revoke
    transport.monotonic = lambda: transport.last_attempt
    append(s, 2, "public_answer_end", "Hello")
    submit(s)
    await s.bridge.dispatch_once()
    assert len(s.calls) == 1
    assert states(s) == ["UNKNOWN"]  # Remote stream could not be closed after revocation.
    await s.client.aclose()


@pytest.mark.parametrize("terminal", ["CANCELLED", "CANCEL_REQUESTED", "FAILED", "EXPIRED"])
async def test_terminal_task_closes_acknowledged_prefix(stream_setup, terminal):
    from qqbot.storage.task_schema import tasks
    s = stream_setup
    append(s, 1, "public_answer_delta", "Visible")
    await s.bridge.dispatch_once()
    with s.core.db.transaction() as c:
        c.execute(update(tasks).values(state=terminal))
    await s.bridge.dispatch_once()
    assert len(s.calls) == 2
    assert s.calls[-1]["content_raw"] == "Visible"
    assert s.calls[-1]["input_state"] == 10
    with s.core.db.engine.connect() as c:
        assert c.execute(select(stream_messages.c.state)).scalar_one() == "CANCELLED"
    await s.bridge.dispatch_once()
    assert len(s.calls) == 2


async def test_error_empty_end_closes_existing_prefix(stream_setup):
    s = stream_setup
    append(s, 1, "public_answer_delta", "Visible")
    await s.bridge.dispatch_once()
    s.service.append_event(s.claim["run_id"], s.claim["fencing_token"], "worker", "event-2", 2,
                           {"kind": "public_answer_end", "message_id": "answer", "text": "", "error": True})
    await s.bridge.dispatch_once()
    assert s.calls[-1]["input_state"] == 10
    assert s.calls[-1]["content_raw"] == "Visible"


async def test_typing_renews_but_preserves_answer_sequence(stream_setup):
    s = stream_setup
    s.core.adapters["qq"].config = s.core.adapters["qq"].config.model_copy(update={"typing_enabled": True})
    for _ in range(6):
        await s.bridge.dispatch_once()
        s.core.clock.advance(26)
        s.service.renew(s.claim["run_id"], s.claim["fencing_token"], "worker")
    assert len(s.calls) == 3
    assert [call["msg_seq"] for call in s.calls] == [1, 2, 3]
    append(s, 1, "public_answer_end", "Answer")
    await s.bridge.dispatch_once()
    assert s.calls[-1]["msg_seq"] == 4


async def test_result_without_end_closes_prefix_before_fallback(stream_setup):
    s = stream_setup
    append(s, 1, "public_answer_delta", "Partial")
    await s.bridge.dispatch_once()
    submit(s, "Full answer")
    await s.bridge.dispatch_once()
    assert len(s.calls) == 2 and s.calls[-1]["content_raw"] == "Partial"
    assert s.calls[-1]["input_state"] == 10
    assert states(s) == ["PENDING"]


async def test_close_receipt_lost_is_unknown_and_not_retried(stream_setup):
    s = stream_setup
    append(s, 1, "public_answer_delta", "Visible")
    await s.bridge.dispatch_once()
    def lost(request):
        s.calls.append(json.loads(request.content))
        raise httpx.ReadTimeout("receipt lost")
    s.client._transport = httpx.MockTransport(lost)
    submit(s, "Full answer")
    await s.bridge.dispatch_once()
    assert states(s) == ["UNKNOWN"]
    count = len(s.calls)
    await s.bridge.dispatch_once()
    assert len(s.calls) == count
    with s.core.db.engine.connect() as c:
        assert c.execute(select(stream_messages.c.state)).scalar_one() == "UNKNOWN"


async def test_cleanup_does_not_bypass_revoked_permission(stream_setup):
    from qqbot.storage.task_schema import tasks
    s = stream_setup
    append(s, 1, "public_answer_delta", "Visible")
    await s.bridge.dispatch_once()
    with s.core.db.transaction() as c:
        c.execute(update(tasks).values(state="CANCEL_REQUESTED"))
    s.core.allowed = False
    await s.bridge.dispatch_once()
    assert len(s.calls) == 1
    with s.core.db.engine.connect() as c:
        assert c.execute(select(stream_messages.c.state)).scalar_one() == "UNKNOWN"


async def test_core_cleanup_gate_requires_persisted_stream_and_current_permission(stream_setup):
    from qqbot.domain import OutboundEnvelope
    from qqbot.runtime import Core
    from qqbot.storage.task_schema import tasks
    s = stream_setup
    append(s, 1, "public_answer_delta", "Visible")
    await s.bridge.dispatch_once()
    with s.core.db.transaction() as c:
        c.execute(update(tasks).values(state="CANCEL_REQUESTED"))
        row = c.execute(select(stream_messages)).mappings().one()
        envelope = OutboundEnvelope.model_validate_json(row["envelope_json"])
    s.core.permissions = SimpleNamespace(check=lambda *args: SimpleNamespace(allowed=s.core.allowed))
    s.core.access = lambda event: AccessContext(principal=event.principal, conversation=event.conversation)
    s.core.validate_context = lambda *args: True
    s.core.knowledge = SimpleNamespace(delivery_valid=lambda *args: True)
    assert not Core.pre_send(s.core, envelope)
    assert not Core.pre_send(s.core, envelope, stream_cleanup=True)  # No cleanup intent yet.
    with s.core.db.transaction() as c:
        c.execute(update(stream_messages).values(state="SENDING"))
    assert Core.pre_send(s.core, envelope, stream_cleanup=True)
    s.core.allowed = False
    assert not Core.pre_send(s.core, envelope, stream_cleanup=True)


@pytest.mark.parametrize("restart", [False, True])
@pytest.mark.parametrize("remove", [False, True])
async def test_removed_or_disabled_adapter_leaves_close_unknown(stream_setup, restart, remove):
    s = stream_setup
    append(s, 1, "public_answer_delta", "Visible")
    await s.bridge.dispatch_once()
    if remove:
        s.core.adapters.clear()
    else:
        adapter = s.core.adapters["qq"]
        adapter.config = adapter.config.model_copy(update={"enabled": False})
    if restart:
        s.bridge = StreamDelivery(s.core)
        s.bridge.recover()
    submit(s, "Full answer")
    await s.bridge.dispatch_once()
    assert len(s.calls) == 1
    assert states(s) == ["UNKNOWN"]
    with s.core.db.engine.connect() as c:
        assert c.execute(select(stream_messages.c.state)).scalar_one() == "UNKNOWN"
    await s.bridge.dispatch_once()
    assert len(s.calls) == 1
