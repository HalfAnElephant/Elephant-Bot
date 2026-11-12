import json
from datetime import timedelta

import httpx
import pytest
from sqlalchemy import select
from test_qq_rich_messages import envelope, target

from qqbot.adapters.qq_official.adapter import OfficialAdapter
from qqbot.domain import Surface
from qqbot.storage.relationship_schema import qq_relationships


def callback(qq, clock, kind, *, stamp=None, remote="300", event_id=None):
    field = "group_openid" if kind.startswith("GROUP_") else "openid"
    body = json.dumps({"op": 0, "t": kind, "id": event_id or kind + str(stamp),
        "d": {field: remote, "timestamp": (stamp or clock.now()).isoformat()}}).encode()
    timestamp = str(int(clock.now().timestamp()))
    headers = {"x-signature-timestamp": timestamp,
        "x-signature-ed25519": qq.key().sign(timestamp.encode() + body).hex()}
    return qq.callback(body, headers)


@pytest.mark.parametrize("surface,reject,receive", [
    (Surface.PRIVATE, "C2C_MSG_REJECT", "C2C_MSG_RECEIVE"),
    (Surface.GROUP, "GROUP_MSG_REJECT", "GROUP_MSG_RECEIVE")])
async def test_opt_out_persists_but_does_not_block_valid_reply(official_config, incoming, clock, surface, reject, receive):
    cfg = official_config.model_copy(update={"proactive_verified": True})
    qq = OfficialAdapter(cfg, "d", incoming, clock, httpx.AsyncClient())
    message = envelope(clock, target=target(surface), reply_context=None, allow_proactive=True)
    callback(qq, clock, reject)
    assert (await qq.validate_delivery(message)).reason == "PROACTIVE_REJECTED"
    assert (await qq.validate_delivery(envelope(clock, target=target(surface)))).allowed
    restarted = OfficialAdapter(cfg, "d", incoming, clock, httpx.AsyncClient())
    assert (await restarted.validate_delivery(message)).reason == "PROACTIVE_REJECTED"
    # A stale permit or equal-time permit cannot override a denial.
    callback(restarted, clock, receive, stamp=clock.now() - timedelta(seconds=1))
    callback(restarted, clock, receive)
    assert (await restarted.validate_delivery(message)).reason == "PROACTIVE_REJECTED"
    clock.advance(1)
    callback(restarted, clock, receive, event_id="new-permit")
    assert (await restarted.validate_delivery(message)).allowed
    # The event is not an authorization grant on an unverified app.
    disabled = OfficialAdapter(official_config, "d", incoming, clock, httpx.AsyncClient())
    assert not (await disabled.validate_delivery(message)).allowed
    await qq.stop(); await restarted.stop(); await disabled.stop()


@pytest.mark.parametrize("surface,remove,add", [
    (Surface.PRIVATE, "FRIEND_DEL", "FRIEND_ADD"),
    (Surface.GROUP, "GROUP_DEL_ROBOT", "GROUP_ADD_ROBOT")])
async def test_relationship_removal_blocks_passive_and_replay_cannot_revive(official_config, incoming, clock,
                                                                         surface, remove, add):
    qq = OfficialAdapter(official_config, "d", incoming, clock, httpx.AsyncClient())
    message = envelope(clock, target=target(surface))
    callback(qq, clock, add, stamp=clock.now() - timedelta(seconds=1), event_id="old-add")
    callback(qq, clock, remove)
    assert (await qq.validate_delivery(message)).reason == "RELATIONSHIP_REMOVED"
    callback(qq, clock, add, stamp=clock.now() + timedelta(seconds=1), event_id="old-add")
    assert (await qq.validate_delivery(message)).reason == "RELATIONSHIP_REMOVED"
    clock.advance(2)
    callback(qq, clock, add, event_id="new-add")
    assert (await qq.validate_delivery(message)).allowed
    await qq.stop()


async def test_relationship_requires_valid_signature_and_isolated_identity(official_config, incoming, clock):
    qq = OfficialAdapter(official_config, "d", incoming, clock, httpx.AsyncClient())
    body = json.dumps({"op": 0, "t": "FRIEND_DEL", "id": "attack", "d": {"openid": "300"}}).encode()
    with pytest.raises(ValueError):
        qq.callback(body, {})
    with incoming.db.engine.connect() as c:
        assert not c.execute(select(qq_relationships)).all()
    callback(qq, clock, "FRIEND_DEL", remote="other")
    assert (await qq.validate_delivery(envelope(clock))).allowed
    await qq.stop()
