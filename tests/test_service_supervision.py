import asyncio
import json
import logging
import time
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from qqbot.app import create_app
from qqbot.config import Settings
from qqbot.domain import ConversationRef, DeliveryCheck, SendReceipt, Surface
from qqbot.observability import JSONFormatter
from qqbot.observability.supervision import LoopSupervisor
from qqbot.outbox.service import Outbox
from qqbot.runtime import Core
from qqbot.storage.schema import outbox


async def until(condition):
    async with asyncio.timeout(2):
        while not condition():
            await asyncio.sleep(0.001)


async def test_supervisor_retries_persistent_failure_and_recovers():
    supervisor = LoopSupervisor(lambda: False)
    supervisor.retry_base = 0.001
    failing = True

    async def step():
        if failing:
            raise RuntimeError("sensitive message")

    supervisor.start("execution", step, interval=0.01, critical=True)
    try:
        await until(lambda: supervisor.snapshot()["execution"]["failures"] >= 3)
        failed = supervisor.snapshot()["execution"]
        assert not failed["healthy"] and failed["last_error"] == "RuntimeError"
        assert failed["heartbeat"] and failed["last_error_at"]
        assert "sensitive message" not in json.dumps(failed)
        failing = False
        await until(lambda: supervisor.snapshot()["execution"]["last_success"] is not None)
        assert supervisor.snapshot()["execution"]["healthy"]
    finally:
        await supervisor.stop()


async def test_stalled_await_is_visible_without_cancelling_side_effects():
    supervisor = LoopSupervisor(lambda: False)
    started = asyncio.Event()
    release = asyncio.Event()

    async def step():
        started.set()
        await release.wait()

    task = supervisor.start("delivery", step, interval=1, critical=True)
    try:
        await started.wait()
        supervisor.loops["delivery"]["started"] -= 121
        state = supervisor.snapshot()["delivery"]
        assert state["stalled"] and not state["healthy"] and not task.done()
        release.set()
        await until(lambda: supervisor.snapshot()["delivery"]["last_success"] is not None)
        assert supervisor.snapshot()["delivery"]["healthy"]
    finally:
        await supervisor.stop()


async def test_core_optional_maintenance_failure_is_isolated(tmp_path, monkeypatch):
    app = create_app(Settings(database=str(tmp_path / "core.db")), run_background=False)
    core = app.state.core
    core.supervisor.retry_base = 0.001
    calls = {"memory": 0, "expression": 0, "execution": 0, "delivery": 0, "speakup": 0}

    async def maintenance(kind):
        calls[kind] += 1
        if kind == "memory":
            raise ValueError("private source")

    async def tick(*, dispatch):
        calls["execution"] += 1
        assert not dispatch

    async def delivery():
        calls["delivery"] += 1

    async def speakup():
        calls["speakup"] += 1

    monkeypatch.setattr(core, "maintenance_once", maintenance)
    monkeypatch.setattr(core, "tick", tick)
    monkeypatch.setattr(core, "delivery_once", delivery)
    monkeypatch.setattr(core.speak_up, "tick", speakup)
    run = asyncio.create_task(core.run())
    try:
        await until(lambda: calls["memory"] >= 3 and calls["execution"] >= 2)
        snapshot = core.supervisor.snapshot()
        assert not snapshot["memory"]["healthy"]
        assert all(snapshot[name]["healthy"] for name in ("execution", "delivery", "expression", "speakup"))
        assert all(calls.values()) and not run.done()
    finally:
        run.cancel()
        await asyncio.gather(run, return_exceptions=True)
        await core.stop()
        app.state.db.close()


async def test_delivery_receipt_failure_recovers_unknown_without_resending(db, clock, monkeypatch):
    service = Outbox(db, clock)
    target = ConversationRef(
        deployment_id="d",
        adapter_instance_id="a",
        bot_identity_id="b",
        surface=Surface.PRIVATE,
        remote_conversation_id="u",
    )
    sent = []

    class Adapter:
        async def validate_delivery(self, envelope):
            return DeliveryCheck(allowed=True, reason="OK")

        async def send(self, envelope):
            sent.append(envelope.delivery_id)
            return SendReceipt(state="SENT", platform_message_id="p")

    with db.transaction() as conn:
        service.enqueue_text(conn, target=target, text="message", origin_kind="chat", origin_id="origin")
    core = SimpleNamespace(
        outbox=service,
        adapters={"a": Adapter()},
        settings=SimpleNamespace(send_budget_per_minute=0),
        pre_send=None,
    )
    receipt = service._receipt

    def failed_receipt(*args, **kwargs):
        raise RuntimeError("receipt database unavailable")

    monkeypatch.setattr(service, "_receipt", failed_receipt)
    with pytest.raises(RuntimeError):
        await Core.delivery_once(core)
    monkeypatch.setattr(service, "_receipt", receipt)
    await Core.delivery_once(core)
    assert len(sent) == 1
    with db.engine.connect() as conn:
        assert conn.execute(select(outbox.c.state)).scalar_one() == "UNKNOWN"


def test_exception_formatter_keeps_stack_without_exception_credentials():
    try:
        raise ValueError("secret-token-and-message")
    except ValueError:
        import sys

        record = logging.LogRecord(
            "qqbot.test", logging.ERROR, __file__, 1, "Background loop failed", (), sys.exc_info()
        )
    result = JSONFormatter().format(record)
    assert "secret-token-and-message" not in result
    data = json.loads(result)
    assert data["error_type"] == "ValueError"
    assert (
        data["traceback"][-1]["function"]
        == "test_exception_formatter_keeps_stack_without_exception_credentials"
    )


def test_background_disabled_preserves_ready_and_reports_backlog(tmp_path, monkeypatch):
    monkeypatch.setenv("QQBOT_ADMIN_TOKEN", "token")
    app = create_app(Settings(database=str(tmp_path / "core.db")), run_background=False)
    with TestClient(app) as client:
        assert client.get("/health/ready").status_code == 200
        status = client.get("/api/v1/status", headers={"Authorization": "Bearer token"}).json()
        assert status["background"]["enabled"] is False
        assert status["oldest_pending_inbox_at"] is None
        assert status["oldest_pending_delivery_at"] is None


def test_ready_checks_critical_failures_stalls_and_core_exit(tmp_path, monkeypatch):
    monkeypatch.setattr(LoopSupervisor, "retry_base", 0.001)
    failing = True

    async def tick(self, *, dispatch):
        if failing:
            raise OSError("private details")

    monkeypatch.setattr(Core, "tick", tick)
    settings = Settings(
        database=str(tmp_path / "core.db"), tasks_enabled=False, interactive_execution_enabled=False
    )
    app = create_app(settings)
    with TestClient(app) as client:
        deadline = time.monotonic() + 2
        while client.get("/health/ready").status_code != 503:
            assert time.monotonic() < deadline
            time.sleep(0.005)
        body = client.get("/health/ready").json()
        assert "execution" in body["background"]["degraded"]
        assert "private details" not in json.dumps(body)
        failing = False
        while client.get("/health/ready").status_code != 200:
            assert time.monotonic() < deadline
            time.sleep(0.005)
        state = app.state.core.supervisor.loops["delivery"]
        state.update(state="RUNNING", started=time.monotonic() - 121)
        assert client.get("/health/ready").status_code == 503
        # Cancellation must be scheduled on the application's event loop.
        client.portal.call(app.state.core_task.cancel)
        deadline = time.monotonic() + 2
        while not app.state.core_task.done():
            assert time.monotonic() < deadline
            time.sleep(0.005)
        assert client.get("/health/ready").status_code == 503


async def test_failed_conversation_does_not_starve_other_maintenance(
    tmp_path, nap_config, nap_event, clock, monkeypatch
):
    settings = Settings(
        database=str(tmp_path / "core.db"), adapters=(nap_config,), memory={"maintenance_enabled": True}
    )
    app = create_app(settings, clock=clock, run_background=False)
    core = app.state.core
    for group in (200, 201):
        event = core.adapters["nap"].normalize({**nap_event, "group_id": group, "message_id": group})
        core.inbox.accept(event)
    visited = []

    async def maintain(access, *, auto_extract):
        visited.append(access.conversation.remote_conversation_id)
        if len(visited) == 1:
            raise ValueError("bad conversation")

    monkeypatch.setattr(core.maintenance, "maintenance_once", maintain)
    try:
        with pytest.raises(ValueError, match="bad conversation"):
            await core.maintenance_once("memory")
        assert set(visited) == {"200", "201"}
    finally:
        await core.stop()
        app.state.db.close()


async def test_core_persists_real_memory_maintenance_report(tmp_path, nap_config, nap_event, clock):
    settings = Settings(
        database=str(tmp_path / "core.db"), adapters=(nap_config,),
        memory={"maintenance_enabled": True, "raw_retention_days": None},
    )
    app = create_app(settings, clock=clock, run_background=False)
    core = app.state.core
    event = core.adapters["nap"].normalize(nap_event)
    accepted, _ = core.inbox.accept(event)
    try:
        await core.maintenance_once("memory")
        report = json.loads(core.state(accepted.conversation.key)["state_json"])["memory_maintenance"]
        assert report["at"] == clock.now().isoformat()
        assert report["state"] == "DONE"
        assert report["summary_seq"] == accepted.ingest_seq
        assert report["extraction_state"] == "SKIPPED"
        assert report["rejections"] == []
    finally:
        await core.stop()
        app.state.db.close()


def test_ready_reports_temporary_database_failure_and_recovery(tmp_path, monkeypatch):
    from sqlalchemy.exc import OperationalError

    app = create_app(Settings(database=str(tmp_path / "health.db")), run_background=False)
    with TestClient(app) as client:
        def unavailable():
            raise OperationalError("SELECT 1", {}, Exception("private database details"))

        with monkeypatch.context() as failure:
            failure.setattr(app.state.db.engine, "connect", unavailable)
            response = client.get("/health/ready")
            assert response.status_code == 503
            assert response.json()["database"] == "unavailable"
            assert "private database details" not in response.text
            assert client.get("/health/live").status_code == 200
        assert client.get("/health/ready").status_code == 200
