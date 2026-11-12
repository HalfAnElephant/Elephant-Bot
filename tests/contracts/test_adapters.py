import asyncio
import json
from datetime import timedelta
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import func, select

from qqbot.adapters.qq_official.adapter import OfficialAdapter
from qqbot.config import Settings
from qqbot.domain import ObservationMode, OutboundEnvelope
from qqbot.outbox.service import Outbox
from qqbot.storage.schema import inbox, outbox


def signed(adapter, payload, clock):
    body = json.dumps(payload, separators=(",", ":")).encode()
    stamp = str(int(clock.now().timestamp()))
    headers = {"x-signature-timestamp":stamp,"x-signature-ed25519":adapter.key().sign(stamp.encode()+body).hex()}
    return body, headers


def envelope(event, clock, kind="chat"):
    return OutboundEnvelope(delivery_id=str(uuid4()), target=event.conversation, text="reply",
        origin_kind=kind, origin_id=event.event_id, reply_context=event.reply_context,
        expires_at=clock.now()+timedelta(seconds=300))


def test_official_events_are_deduplicated(official_config, incoming, clock, official_event, db):
    qq = OfficialAdapter(official_config, "d", incoming, clock)
    event = qq.normalize(official_event)
    for i in range(10):
        accepted, new = incoming.accept(event)
        assert new == (i == 0)
        assert accepted.ingest_seq == 1
    with db.engine.connect() as c:
        assert c.execute(select(func.count()).select_from(inbox)).scalar() == 1


async def test_A02_signature_replay_and_challenge(official_config, incoming, clock, official_event, db):
    qq = OfficialAdapter(official_config, "d", incoming, clock)
    body, headers = signed(qq, official_event, clock)
    assert qq.callback(body, headers) == {"op":12}
    assert qq.callback(body, headers) == {"op":12}
    with pytest.raises(ValueError):
        qq.callback(body.replace(b'/status', b'attack')+b' ', headers)
    with pytest.raises(ValueError):
        qq.callback(body, {})
    clock.advance(301)
    with pytest.raises(ValueError):
        qq.callback(body, headers)
    stamp = str(int(clock.now().timestamp()))
    challenge = qq.callback(json.dumps({"op":13,"d":{"plain_token":"hello","event_ts":stamp}}).encode(), {})
    qq.key().public_key().verify(bytes.fromhex(challenge["signature"]), (stamp+"hello").encode())
    with db.engine.connect() as c:
        assert c.execute(select(func.count()).select_from(inbox)).scalar() == 1
    await qq.stop()


async def test_A05_modes(official_config, incoming, clock, official_event):
    qq = OfficialAdapter(official_config, "d", incoming, clock)
    at = qq.normalize(official_event)
    assert at.mentions_bot and at.observation_mode == ObservationMode.MENTIONS_ONLY
    full = {**official_event,"t":"GROUP_MESSAGE_CREATE"}
    assert qq.normalize(full) is None
    qq.config = official_config.model_copy(update={"full_mode_enabled": True})
    assert not (await qq.capabilities(at.conversation)).can_observe_unaddressed_messages
    seen = qq.normalize(full)
    assert not seen.mentions_bot and seen.observation_mode == ObservationMode.FULL
    assert (await qq.capabilities(at.conversation)).can_observe_unaddressed_messages
    assert not (await qq.capabilities(at.conversation)).can_initiate_message
    await qq.stop()


async def test_token_merge_and_official_outbox(official_config, incoming, clock, official_event, db):
    calls = []
    async def transport(request):
        calls.append(request)
        if request.url.path.endswith("getAppAccessToken"):
            await asyncio.sleep(0.01)
            return httpx.Response(200, json={"access_token":"fixture","expires_in":"7200"})
        return httpx.Response(200, json={"id":"sent-1"})
    qq = OfficialAdapter(official_config, "d", incoming, clock, httpx.AsyncClient(transport=httpx.MockTransport(transport)))
    await asyncio.gather(*(qq.access_token() for _ in range(10)))
    assert len(calls) == 1
    outgoing = Outbox(db, clock)
    event = qq.normalize(official_event)
    with db.transaction() as c:
        outgoing.enqueue(c, envelope(event, clock))
        outgoing.enqueue(c, envelope(event, clock))
    await outgoing.dispatch_once({"qq":qq})
    await outgoing.dispatch_once({"qq":qq})
    assert len(calls) == 2
    sent = json.loads(calls[-1].content)
    assert sent["msg_seq"] == 1 and sent["msg_id"] == "400"
    assert calls[-1].url.path == "/v2/groups/200/messages"
    await qq.stop()


async def test_D02_D03_unknown_and_expired(official_config, incoming, clock, official_event, db):
    sent = 0
    def transport(request):
        nonlocal sent
        if request.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token":"fixture","expires_in":7200})
        sent += 1
        raise httpx.ReadTimeout("accepted but receipt lost")
    qq = OfficialAdapter(official_config, "d", incoming, clock, httpx.AsyncClient(transport=httpx.MockTransport(transport)))
    event = qq.normalize(official_event)
    outgoing = Outbox(db, clock)
    with db.transaction() as c:
        first = outgoing.enqueue(c, envelope(event, clock))
    await outgoing.dispatch_once({"qq":qq})
    await outgoing.dispatch_once({"qq":qq})
    assert sent == 1
    with db.engine.connect() as c:
        assert c.execute(select(outbox.c.state).where(outbox.c.id == first)).scalar() == "UNKNOWN"
    next_event = event.model_copy(update={"event_id":"task-result"})
    with db.transaction() as c:
        waiting = outgoing.enqueue(c, envelope(next_event, clock, "task"))
    clock.advance(301)
    await outgoing.dispatch_once({"qq":qq})
    with db.engine.connect() as c:
        assert c.execute(select(outbox.c.state).where(outbox.c.id == waiting)).scalar() == "WAITING_FOR_TRIGGER"
    fresh = event.model_copy(update={"reply_context":event.reply_context.model_copy(update={"platform_message_id":"new", "expires_at":clock.now()+timedelta(seconds=300)})})
    with db.transaction() as c:
        outgoing.retrigger(c, fresh)
    with db.engine.connect() as c:
        assert c.execute(select(outbox.c.state).where(outbox.c.id == waiting)).scalar() == "PENDING"
        assert c.execute(select(outbox.c.state).where(outbox.c.id == first)).scalar() == "UNKNOWN"
    await qq.stop()


async def test_removed_adapter_deliveries_become_terminal_before_official_queue_continues(
    official_config, incoming, clock, official_event, db
):
    calls = []

    async def transport(request):
        calls.append(request)
        if request.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "fixture", "expires_in": 7200})
        return httpx.Response(200, json={"id": "official-sent"})

    qq = OfficialAdapter(official_config, "d", incoming, clock, httpx.AsyncClient(transport=httpx.MockTransport(transport)))
    event = qq.normalize(official_event)
    retired_target = event.conversation.model_copy(update={"adapter_instance_id": "removed-napcat"})
    outgoing = Outbox(db, clock)
    retired_ids = []
    with db.transaction() as conn:
        for index in range(50):
            stale = event.model_copy(update={"event_id": f"legacy-{index}", "conversation": retired_target})
            retired_ids.append(outgoing.enqueue(conn, envelope(stale, clock)))
    clock.advance(1)
    with db.transaction() as conn:
        official_id = outgoing.enqueue(conn, envelope(event.model_copy(update={"event_id": "official-after-retired"}), clock))

    await outgoing.dispatch_once({"qq": qq})
    with db.engine.connect() as conn:
        assert set(conn.execute(select(outbox.c.state).where(outbox.c.id.in_(retired_ids))).scalars()) == {"FAILED"}
        assert conn.execute(select(outbox.c.receipt_json).where(outbox.c.id == retired_ids[0])).scalar_one()
        assert conn.execute(select(outbox.c.state).where(outbox.c.id == official_id)).scalar_one() == "PENDING"

    await outgoing.dispatch_once({"qq": qq})
    with db.engine.connect() as conn:
        assert conn.execute(select(outbox.c.state).where(outbox.c.id == official_id)).scalar_one() == "SENT"
    assert calls[-1].url.path.endswith("messages")
    await qq.stop()


def test_full_and_at_dual_notification_updates_invitation_once(official_config, incoming, clock, official_event):
    qq = OfficialAdapter(official_config.model_copy(update={"full_mode_enabled":True}), "d", incoming, clock)
    full = qq.normalize({**official_event,"t":"GROUP_MESSAGE_CREATE", "id":"full-event"})
    stored, new = incoming.accept(full)
    assert new and not stored.mentions_bot
    incoming.finish(stored.event_id, expected_seq=stored.ingest_seq)
    at = qq.normalize({**official_event,"id":"at-event"})
    updated, new = incoming.accept(at)
    assert not new and updated.event_id == stored.event_id and updated.mentions_bot
    assert updated.ingest_seq > stored.ingest_seq
    incoming.finish(stored.event_id, expected_seq=stored.ingest_seq)
    assert incoming.pending()[0].mentions_bot


@pytest.mark.parametrize("budget, expected", [(0, 8), (1, 1)])
async def test_outbox_unlimited_default_and_explicit_limit(official_config, incoming, clock, official_event, db, budget, expected):
    from qqbot.domain import DeliveryCheck, SendReceipt

    qq = OfficialAdapter(official_config, "d", incoming, clock)
    event = qq.normalize(official_event)
    sent = []

    class Adapter:
        async def validate_delivery(self, item):
            return DeliveryCheck(allowed=True, reason="OK")

        async def send(self, item):
            sent.append(item.origin_id)
            return SendReceipt(state="SENT", platform_message_id=str(len(sent)))

    outgoing = Outbox(db, clock)
    with db.transaction() as c:
        for index in range(8):
            outgoing.enqueue(c, envelope(event.model_copy(update={"event_id": str(index)}), clock))
    options = {"budget": budget} if budget else {}
    await outgoing.dispatch_once({"qq": Adapter()}, **options)
    assert len(sent) == 1
    for _ in range(7):
        clock.advance(3)
        await outgoing.dispatch_once({"qq": Adapter()}, **options)
    assert len(sent) == len(set(sent)) == expected
    assert Settings().send_budget_per_minute == 0
