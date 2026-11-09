from datetime import datetime

import pytest
from pydantic import ValidationError

from qqbot.cli import migrate
from qqbot.clock import FakeClock
from qqbot.config import Settings
from qqbot.domain import ConversationRef, InboundEvent, PrincipalRef, Surface
from qqbot.storage.db import Database


def test_namespaces_and_strict_config():
    base = {"deployment_id":"a", "adapter_instance_id":"b", "bot_identity_id":"c", "surface":Surface.GROUP, "remote_conversation_id":"1"}
    a = ConversationRef(**base)
    assert a.key != a.model_copy(update={"surface": Surface.PRIVATE}).key
    assert a.key != a.model_copy(update={"adapter_instance_id": "other"}).key
    with pytest.raises(ValidationError):
        Settings(unknown=True)
    with pytest.raises(ValidationError):
        ConversationRef(**{**base, "remote_conversation_id": 1})
    clock = FakeClock()
    with pytest.raises(ValidationError):
        InboundEvent(event_id="1", event_type="message.created", conversation=a,
                     principal=PrincipalRef(adapter_instance_id="b", identity_namespace="qq", remote_user_id="u"),
                     platform_message_id="m", occurred_at=datetime(2026, 1, 1), received_at=clock.now(), trace_id="t")


def test_migration_and_clock(tmp_path):
    path = str(tmp_path / "db.sqlite")
    migrate(path)
    migrate(path)
    db = Database(path)
    with db.transaction() as c:
        assert c.exec_driver_sql("PRAGMA integrity_check").scalar() == "ok"
        assert c.exec_driver_sql("PRAGMA journal_mode").scalar() == "wal"
    db.close()
    clock = FakeClock()
    before = clock.now()
    clock.advance(5)
    assert (clock.now() - before).total_seconds() == 5
