"""Verified signed relationship events constrain delivery, never grant permissions.

Protocol fields: tencent-connect/botpy e25f3e84bad7217357d8200a9d12939b58285b84,
manage.py and connection.py. No welcome message or task is implicitly created.
"""
from datetime import UTC, datetime, timedelta

from sqlalchemy import insert, select, update

from qqbot.domain import ConversationRef, Surface, stable_id
from qqbot.storage.relationship_schema import qq_relationship_events, qq_relationships

EVENTS = {
    "FRIEND_ADD": (Surface.PRIVATE, "membership", True),
    "FRIEND_DEL": (Surface.PRIVATE, "membership", False),
    "C2C_MSG_RECEIVE": (Surface.PRIVATE, "proactive", True),
    "C2C_MSG_REJECT": (Surface.PRIVATE, "proactive", False),
    "GROUP_ADD_ROBOT": (Surface.GROUP, "membership", True),
    "GROUP_DEL_ROBOT": (Surface.GROUP, "membership", False),
    "GROUP_MSG_RECEIVE": (Surface.GROUP, "proactive", True),
    "GROUP_MSG_REJECT": (Surface.GROUP, "proactive", False),
}


class RelationshipStore:
    def __init__(self, db, clock):
        self.db, self.clock = db, clock

    def accept(self, payload, adapter):
        """Call only after signature validation. Return False for unrelated events."""
        event_type = payload.get("t")
        if event_type not in EVENTS:
            return False
        surface, dimension, allowed = EVENTS[event_type]
        data, event_id = payload.get("d"), payload.get("id")
        if not isinstance(data, dict) or not isinstance(event_id, str) or not 1 <= len(event_id) <= 256:
            raise ValueError("invalid relationship event")
        remote = data.get("openid" if surface == Surface.PRIVATE else "group_openid")
        if not isinstance(remote, str) or not 1 <= len(remote) <= 256:
            raise ValueError("invalid relationship target")
        raw = data.get("timestamp")
        try:
            if isinstance(raw, bool) or raw is None:
                raise ValueError
            stamp = (datetime.fromtimestamp(float(raw), UTC) if isinstance(raw, (int, float))
                     or (isinstance(raw, str) and raw.isdecimal()) else datetime.fromisoformat(raw))
            if stamp.tzinfo is None or stamp > self.clock.now() + timedelta(minutes=5):
                raise ValueError
            stamp = stamp.astimezone(UTC).isoformat()
        except (ValueError, TypeError, OverflowError, OSError):
            raise ValueError("invalid relationship timestamp") from None
        target = ConversationRef(deployment_id=adapter.deployment_id, adapter_instance_id=adapter.config.instance_id,
            bot_identity_id=adapter.config.bot_id, surface=surface, remote_conversation_id=remote)
        key = stable_id(target.key, event_type, event_id)
        with self.db.transaction() as c:
            if c.execute(select(qq_relationship_events.c.key).where(qq_relationship_events.c.key == key)).first():
                return True
            c.execute(insert(qq_relationship_events).values(key=key, received_at=self.clock.now().isoformat()))
            where = (qq_relationships.c.conversation_id == target.key) & (qq_relationships.c.dimension == dimension)
            old = c.execute(select(qq_relationships).where(where)).mappings().first()
            # Older events cannot revive opt-out; ties prefer denial conservatively.
            if old and (old["occurred_at"] > stamp or (old["occurred_at"] == stamp and not old["allowed"])):
                return True
            values = dict(allowed=int(allowed), occurred_at=stamp, event_id=event_id)  # noqa: C408
            if old:
                c.execute(update(qq_relationships).where(where).values(**values))
            else:
                c.execute(insert(qq_relationships).values(conversation_id=target.key, dimension=dimension, **values))
        return True

    def denied(self, target, *, proactive=False):
        with self.db.engine.connect() as c:
            values = dict(c.execute(select(qq_relationships.c.dimension, qq_relationships.c.allowed).where(
                qq_relationships.c.conversation_id == target.key)).all())
        if values.get("membership") == 0:
            return "RELATIONSHIP_REMOVED"
        if proactive and values.get("proactive") == 0:
            return "PROACTIVE_REJECTED"
        return None
