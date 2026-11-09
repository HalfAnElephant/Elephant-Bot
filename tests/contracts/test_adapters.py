import asyncio
import json
import time
from datetime import timedelta
from uuid import uuid4

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select

from qqbot.adapters.onebot_v11.adapter import OneBotAdapter, segments
from qqbot.adapters.qq_official.adapter import OfficialAdapter
from qqbot.app import create_app
from qqbot.config import Settings
from qqbot.domain import MediaSegment, ObservationMode, OutboundEnvelope, TextSegment, UnknownSegment
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


def test_A01_A03_A06(nap_config, official_config, incoming, clock, nap_event, official_event, db):
    nap = OneBotAdapter(nap_config, "d", incoming, clock)
    qq = OfficialAdapter(official_config, "d", incoming, clock)
    event = nap.normalize(nap_event)
    for i in range(10):
        accepted, new = incoming.accept(event)
        assert new == (i == 0)
        assert accepted.ingest_seq == 1
    official = qq.normalize(official_event)
    assert event.conversation.key != official.conversation.key
    assert event.principal.key != official.principal.key
    chain = segments('[CQ:at,qq=100]a&#91;b[CQ:image,file=temp][CQ:future,x=y]')
    assert isinstance(chain[1], TextSegment) and chain[1].text == 'a[b'
    assert isinstance(chain[2], MediaSegment) and chain[2].attachment_ref.startswith("unresolved:")
    assert isinstance(chain[3], UnknownSegment)
    recall = nap.normalize({"post_type":"notice", "notice_type":"group_recall", "self_id":100,
        "user_id":300,"group_id":200,"message_id":400,"time":int(clock.now().timestamp())})
    assert recall.event_type == "message.recalled"
    with db.engine.connect() as c:
        assert c.execute(select(func.count()).select_from(inbox)).scalar() == 1


@pytest.mark.parametrize("data, expected", [
    ({"file": "local.jpg", "url": "https://gchat.qpic.cn/pic?token=one&size=2"},
     "https://gchat.qpic.cn/pic?token=one&size=2"),
    ({"file": "https://gchat.qpic.cn/fallback"}, "https://gchat.qpic.cn/fallback"),
    ({"file": "base64://large-image"}, None),
    ({"file": "file:///tmp/local.jpg"}, None),
    ({"url": "https://example.com/" + "x" * 8192}, None),
    ({"url": {"invalid": "value"}}, None),
])
def test_onebot_image_source_survives_inbox_recovery(nap_config, incoming, clock, nap_event, data, expected):
    nap = OneBotAdapter(nap_config, "d", incoming, clock)
    event = nap.normalize({**nap_event, "message": [{"type": "image", "data": data}]})
    assert event.segments[0].image_url == expected
    assert event.segments[0].attachment_ref.startswith("unresolved:")
    incoming.accept(event)
    assert incoming.pending()[0].segments[0].image_url == expected


def test_onebot_cq_image_url_decodes_and_non_images_keep_no_source():
    chain = segments("[CQ:image,file=local.jpg,url=https://gchat.qpic.cn/pic?a=1&amp;b=2&#44;3]")
    assert chain[0].image_url == "https://gchat.qpic.cn/pic?a=1&b=2,3"
    chain = segments([{"type": "record", "data": {"url": "https://example.com/audio"}}])
    assert chain[0].type == "audio" and chain[0].image_url is None


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


def test_A04_real_local_websocket(tmp_path, nap_config, clock, nap_event):
    settings = Settings(interactive_execution_enabled=False, database=str(tmp_path/"ws.db"), adapters=(nap_config,))
    app = create_app(settings, clock=clock)
    headers = {"authorization":"Bearer test-secret", "x-self-id":"100"}
    with TestClient(app) as client:
        with client.websocket_connect("/onebot/nap", headers=headers) as ws:
            for _ in range(10):
                ws.send_json(nap_event)
            request = ws.receive_json()
            assert request["action"] == "send_group_msg"
            assert [part["type"] for part in request["params"]["message"]] == ["text"]
            ws.send_json({"echo": request["echo"],"status":"ok","retcode":0,"data":{"message_id":999}})
        assert app.state.adapters["nap"].context_gap
        for _ in range(100):
            with app.state.db.engine.connect() as conn:
                if conn.execute(select(outbox.c.state)).scalar_one() == "SENT":
                    break
            time.sleep(0.01)
        else:
            pytest.fail("first WebSocket acknowledgement was not committed")
        clock.advance(3)
        with client.websocket_connect("/onebot/nap", headers=headers) as ws:
            event = {**nap_event, "message_id":401}
            ws.send_json({"echo":request["echo"],"status":"ok","retcode":0,"data":{"message_id":1000}})
            ws.send_json(event)
            current = ws.receive_json()
            assert current["echo"] != request["echo"]
            ws.send_json({"echo":current["echo"],"status":"ok","retcode":0,"data":{"message_id":1001}})
        with app.state.db.engine.connect() as c:
            assert c.execute(select(func.count()).select_from(inbox)).scalar() == 2


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
async def test_outbox_unlimited_default_and_explicit_limit(nap_config, incoming, clock, nap_event, db, budget, expected):
    from qqbot.domain import DeliveryCheck, SendReceipt

    nap = OneBotAdapter(nap_config, "d", incoming, clock)
    event = nap.normalize(nap_event)
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
    await outgoing.dispatch_once({"nap": Adapter()}, **options)
    await outgoing.dispatch_once({"nap": Adapter()}, **options)
    assert len(sent) == 1
    for _ in range(7):
        clock.advance(3)
        await outgoing.dispatch_once({"nap": Adapter()}, **options)
    assert len(sent) == len(set(sent)) == expected
    assert Settings().send_budget_per_minute == 0
