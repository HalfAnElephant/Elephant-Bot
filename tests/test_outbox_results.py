"""Final replies retain a durable delivery route after passive credentials run out."""
import json
from datetime import timedelta

import httpx
import pytest
from sqlalchemy import insert, select, update

from elephant.adapters.qq_official.adapter import OfficialAdapter
from elephant.domain import OutboundEnvelope, SendReceipt, stable_id
from elephant.outbox.service import Outbox
from elephant.storage.schema import outbox, reply_counters


def result(service, db, event, clock, origin, **changes):
    envelope = OutboundEnvelope(delivery_id=origin, target=event.conversation,
        text="安装完成", origin_kind="chat", origin_id="turn:session:" + origin,
        requester=event.principal, reply_context=event.reply_context,
        purpose="result", expires_at=clock.now() + timedelta(days=1))
    with db.transaction() as connection:
        return service.enqueue(connection, envelope.model_copy(update=changes))


@pytest.mark.parametrize("expired", [False, True])
async def test_final_uses_proactive_without_allocating_invalid_passive_sequence(
    db, clock, incoming, official_config, official_event, expired
):
    calls = []

    def transport(request):
        if request.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "fixture", "expires_in": 7200})
        calls.append(json.loads(request.content))
        return httpx.Response(200, json={"id": "final"})

    adapter = OfficialAdapter(official_config.model_copy(update={"proactive_verified": True}), "d", incoming,
        clock, httpx.AsyncClient(transport=httpx.MockTransport(transport)))
    try:
        event = adapter.normalize(official_event)
        event = event.model_copy(update={"reply_context": event.reply_context.model_copy(update={"max_replies": 4})})
        counter = stable_id(event.conversation.key, event.reply_context.platform_message_id)
        if expired:
            clock.advance(301)
        else:
            with db.transaction() as connection:
                connection.execute(insert(reply_counters).values(key=counter, seq=4))
        outgoing = Outbox(db, clock)
        identity = result(outgoing, db, event, clock, "final", allow_proactive=True)
        await outgoing.dispatch_once({"qq": adapter})
        assert len(calls) == 1 and "msg_id" not in calls[0] and "msg_seq" not in calls[0]
        with db.engine.connect() as connection:
            assert connection.execute(select(outbox.c.state).where(outbox.c.id == identity)).scalar_one() == "SENT"
            assert connection.execute(select(reply_counters.c.seq).where(reply_counters.c.key == counter)).scalar() == (
                None if expired else 4)
    finally:
        await adapter.stop()


async def test_waiting_final_resumes_when_proactive_permission_becomes_available(
    db, clock, incoming, official_config, official_event
):
    calls = []

    def transport(request):
        if request.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "fixture", "expires_in": 7200})
        calls.append(json.loads(request.content))
        return httpx.Response(200, json={"id": "final"})

    adapter = OfficialAdapter(official_config, "d", incoming, clock,
        httpx.AsyncClient(transport=httpx.MockTransport(transport)))
    try:
        event = adapter.normalize(official_event)
        outgoing = Outbox(db, clock)
        identity = result(outgoing, db, event, clock, "waiting", allow_proactive=True, reply_context=None)
        await outgoing.dispatch_once({"qq": adapter})
        with db.engine.connect() as connection:
            assert connection.execute(select(outbox.c.state).where(outbox.c.id == identity)).scalar_one() == "WAITING_FOR_TRIGGER"
        adapter.config = official_config.model_copy(update={"proactive_verified": True})
        await outgoing.dispatch_once({"qq": adapter})
        assert len(calls) == 1 and "msg_id" not in calls[0]
        await outgoing.dispatch_once({"qq": adapter})
        assert len(calls) == 1
    finally:
        await adapter.stop()


def test_progress_preserves_last_passive_reply_for_result(db, clock, incoming, official_config, official_event):
    adapter = OfficialAdapter(official_config, "d", incoming, clock)
    event = adapter.normalize(official_event)
    event = event.model_copy(update={"reply_context": event.reply_context.model_copy(update={"max_replies": 4})})
    outgoing = Outbox(db, clock)
    with db.transaction() as connection:
        connection.execute(insert(reply_counters).values(
            key=stable_id(event.conversation.key, event.reply_context.platform_message_id), seq=3))
    progress = result(outgoing, db, event, clock, "progress", purpose="progress")
    final = result(outgoing, db, event, clock, "final")
    with db.engine.connect() as connection:
        envelopes = {row.id: json.loads(row.envelope_json) for row in connection.execute(select(outbox))}
        assert envelopes[progress]["reply_context"]["msg_seq"] is None
        assert envelopes[final]["reply_context"]["msg_seq"] == 4
        assert connection.execute(select(reply_counters.c.seq)).scalar_one() == 4


def test_chat_retrigger_is_owner_bound_and_reserves_current_request_slot(
    db, clock, incoming, official_config, official_event
):
    adapter = OfficialAdapter(official_config, "d", incoming, clock)
    event = adapter.normalize(official_event)
    outgoing = Outbox(db, clock)
    expected = [result(outgoing, db, event, clock, "old-" + str(index)) for index in range(4)]
    other = result(outgoing, db, event, clock, "other", requester=event.principal.model_copy(
        update={"remote_user_id": "someone-else"}))
    with db.transaction() as connection:
        connection.execute(update(outbox).values(state="WAITING_FOR_TRIGGER"))
        trigger = event.model_copy(update={"reply_context": event.reply_context.model_copy(
            update={"platform_message_id": "new", "max_replies": 4})})
        renewed = outgoing.retrigger(connection, trigger)
        assert set(renewed) == set(expected[:3])
        assert connection.execute(select(outbox.c.state).where(outbox.c.id == expected[3])).scalar_one() == "WAITING_FOR_TRIGGER"
        assert connection.execute(select(outbox.c.state).where(outbox.c.id == other)).scalar_one() == "WAITING_FOR_TRIGGER"
        assert connection.execute(select(reply_counters.c.seq).where(
            reply_counters.c.key == stable_id(event.conversation.key, "new"))).scalar_one() == 3


def test_new_input_refreshes_held_worker_reply_without_publishing(
    db, clock, incoming, official_config, official_event
):
    from conftest import official_payload

    adapter = OfficialAdapter(official_config, "d", incoming, clock)
    event = adapter.normalize(official_payload(official_event, private=True))
    outgoing = Outbox(db, clock)
    envelope = OutboundEnvelope(delivery_id="held-worker", target=event.conversation,
        text="待发布的文件说明", origin_kind="task", origin_id="worker-notice",
        requester=event.principal, reply_context=event.reply_context, purpose="status",
        expires_at=clock.now() + timedelta(minutes=5))
    with db.transaction() as conn:
        outgoing.enqueue(conn, envelope, defer_to_leader=True)
    clock.advance(301)
    trigger = event.model_copy(update={"reply_context": event.reply_context.model_copy(update={
        "platform_message_id": "fresh-message", "expires_at": clock.now() + timedelta(minutes=5),
    })})
    with db.transaction() as conn:
        assert outgoing.retrigger(conn, trigger) == []
        row = conn.execute(select(outbox)).mappings().one()
        staged = OutboundEnvelope.model_validate_json(row["envelope_json"])
        assert row["state"] == "LEADER_PENDING"
        assert staged.reply_context.platform_message_id == "fresh-message"
        assert staged.reply_context.msg_seq is None
        assert staged.expires_at > clock.now()
        assert not conn.execute(select(reply_counters.c.key)).first()


def test_restart_upgrades_recent_finals_and_skips_older_delivered_dialogue(
    db, clock, incoming, official_config, official_event
):
    adapter = OfficialAdapter(official_config, "d", incoming, clock)
    event = adapter.normalize(official_event)
    outgoing = Outbox(db, clock)
    stale = result(outgoing, db, event, clock, "stale", text="旧安装记录")
    clock.advance(1)
    sent = result(outgoing, db, event, clock, "sent", text="已向用户确认")
    clock.advance(1)
    waiting = result(outgoing, db, event, clock, "waiting", text="新任务完成",
        expires_at=clock.now() + timedelta(seconds=300))
    unknown = result(outgoing, db, event, clock, "unknown", text="回执丢失")
    with db.transaction() as connection:
        connection.execute(update(outbox).where(outbox.c.id.in_([stale, waiting])).values(state="WAITING_FOR_TRIGGER"))
        connection.execute(update(outbox).where(outbox.c.id == sent).values(state="SENT"))
        connection.execute(update(outbox).where(outbox.c.id == unknown).values(state="UNKNOWN"))
    clock.advance(301)
    Outbox(db, clock).recover()
    with db.engine.connect() as connection:
        records = {row.id: row for row in connection.execute(select(outbox))}
        assert records[stale].state == "WAITING_FOR_TRIGGER"
        assert records[waiting].state == "PENDING"
        envelope = OutboundEnvelope.model_validate_json(records[waiting].envelope_json)
        assert envelope.allow_proactive and envelope.expires_at > clock.now() + timedelta(hours=23)
        assert records[unknown].state == "UNKNOWN"
    with db.transaction() as connection:
        trigger = event.model_copy(update={"reply_context": event.reply_context.model_copy(update={
            "platform_message_id": "fresh", "expires_at": clock.now() + timedelta(seconds=300)})})
        assert stale not in outgoing.retrigger(connection, trigger)


def test_delivered_new_result_preserves_distinct_outstanding_result(
    db, clock, incoming, official_config, official_event
):
    adapter = OfficialAdapter(official_config, "d", incoming, clock)
    event = adapter.normalize(official_event)
    outgoing = Outbox(db, clock)
    old = result(outgoing, db, event, clock, "old", text="原任务已完成")
    clock.advance(1)
    current = result(outgoing, db, event, clock, "current", text="已完成，并补上实际验证结果")
    with db.transaction() as connection:
        connection.execute(update(outbox).where(outbox.c.id == old).values(state="WAITING_FOR_TRIGGER"))
    outgoing._receipt(current, SendReceipt(state="RETRYABLE"))
    with db.engine.connect() as connection:
        assert connection.execute(select(outbox.c.state).where(outbox.c.id == old)).scalar_one() == "WAITING_FOR_TRIGGER"
    outgoing._receipt(current, SendReceipt(state="SENT", platform_message_id="sent"))
    with db.engine.connect() as connection:
        assert connection.execute(select(outbox.c.state).where(outbox.c.id == old)).scalar_one() == "WAITING_FOR_TRIGGER"


def test_identical_followup_replaces_pending_result_but_preserves_unknown_and_other_sessions(
    db, clock, incoming, official_config, official_event
):
    adapter = OfficialAdapter(official_config, "d", incoming, clock)
    event = adapter.normalize(official_event)
    outgoing = Outbox(db, clock)
    old = result(outgoing, db, event, clock, "old")
    unknown = result(outgoing, db, event, clock, "unknown", text="回执不确定")
    other = result(outgoing, db, event, clock, "other", origin_id="turn:other-session:old")
    with db.transaction() as connection:
        connection.execute(update(outbox).where(outbox.c.id == old).values(state="WAITING_FOR_TRIGGER"))
        connection.execute(update(outbox).where(outbox.c.id == unknown).values(state="UNKNOWN"))
    result(outgoing, db, event, clock, "new")
    with db.engine.connect() as connection:
        states = dict(connection.execute(select(outbox.c.id, outbox.c.state)).all())
        assert states[old] == "SUPERSEDED"
        assert states[unknown] == "UNKNOWN"
        assert states[other] == "PENDING"
