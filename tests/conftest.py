import pytest

from qqbot.cli import migrate
from qqbot.clock import FakeClock
from qqbot.config import AdapterConfig
from qqbot.ingress.service import Inbox
from qqbot.storage.db import Database


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def db(tmp_path):
    path = str(tmp_path / "core.db")
    migrate(path)
    database = Database(path)
    yield database
    database.close()


@pytest.fixture
def incoming(db):
    return Inbox(db)


@pytest.fixture
def official_config(monkeypatch):
    monkeypatch.setenv("TEST_QQ_SECRET", "protocol-fixture-secret")
    return AdapterConfig(instance_id="qq", kind="official", bot_id="bot", app_id="app",
        secret_env="TEST_QQ_SECRET", allowed_groups=("200",), allowed_users=("300",))


@pytest.fixture
def official_event(clock):
    return {"op":0,"id":"event-1","t":"GROUP_AT_MESSAGE_CREATE","d":{
        "id":"400","group_openid":"200","author":{"member_openid":"300"},
        "content":"/状态","timestamp":clock.now().isoformat()}}


def official_payload(payload, *, message_id=None, text=None, user_id=None, group_id=None,
                     private=False, attachments=None, timestamp=None):
    data = {**payload["d"]}
    author = {**data["author"]}
    if message_id is not None:
        data["id"] = str(message_id)
    if text is not None:
        data["content"] = text
    if timestamp is not None:
        data["timestamp"] = timestamp
    if user_id is not None:
        author["user_openid" if private else "member_openid"] = str(user_id)
    if private:
        data.pop("group_openid", None)
        author.setdefault("user_openid", author.get("member_openid", "300"))
        author.pop("member_openid", None)
        data["author"] = author
        kind = "C2C_MESSAGE_CREATE"
    else:
        if group_id is not None:
            data["group_openid"] = str(group_id)
        data["author"] = author
        kind = payload["t"]
    if attachments is not None:
        data["attachments"] = attachments
    return {**payload, "t": kind, "d": data}
