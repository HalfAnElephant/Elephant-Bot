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
def nap_config(monkeypatch):
    monkeypatch.setenv("TEST_NAP_TOKEN", "test-secret")
    return AdapterConfig(instance_id="nap", kind="onebot", bot_id="100", secret_env="TEST_NAP_TOKEN",
                         allowed_groups=("200",), allowed_users=("300",))


@pytest.fixture
def official_config(monkeypatch):
    monkeypatch.setenv("TEST_QQ_SECRET", "protocol-fixture-secret")
    return AdapterConfig(instance_id="qq", kind="official", bot_id="bot", app_id="app",
        secret_env="TEST_QQ_SECRET", allowed_groups=("200",), allowed_users=("300",))


@pytest.fixture
def nap_event(clock):
    return {"post_type":"message", "message_type":"group", "self_id":100, "user_id":300,
        "group_id":200, "message_id":400, "time":int(clock.now().timestamp()),
        "message":[{"type":"at","data":{"qq":"100"}}, {"type":"text","data":{"text":" /状态"}}]}


@pytest.fixture
def official_event(clock):
    return {"op":0,"id":"event-1","t":"GROUP_AT_MESSAGE_CREATE","d":{
        "id":"400","group_openid":"200","author":{"member_openid":"300"},
        "content":"/状态","timestamp":clock.now().isoformat()}}
