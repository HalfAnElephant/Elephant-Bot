import asyncio

import pytest

from qqbot.observability.supervision import LoopSupervisor


async def until(condition):
    async with asyncio.timeout(2):
        while not condition():
            await asyncio.sleep(0.001)


async def test_supervisor_keeps_retrying_and_recovers_after_repeated_failures():
    supervisor = LoopSupervisor(lambda: False)
    supervisor.retry_base = 0.001
    calls = 0
    recover = False

    async def step():
        nonlocal calls
        calls += 1
        if not recover:
            raise RuntimeError("private details")

    task = supervisor.start("execution", step, interval=1, critical=True)
    await until(lambda: calls > supervisor.max_failures)
    state = supervisor.snapshot()["execution"]
    assert calls > supervisor.max_failures
    assert state["state"] == "FAILED" and not state["healthy"] and not state["exited"]
    assert state["last_error"] == "RuntimeError"
    assert "private details" not in str(state)
    recover = True
    await until(lambda: supervisor.snapshot()["execution"]["last_success"] is not None)
    assert supervisor.snapshot()["execution"]["healthy"] and not task.done()
    await supervisor.stop()
    assert supervisor.snapshot()["execution"]["state"] == "STOPPED"


async def test_transient_failure_recovers_and_duplicate_registration_is_rejected():
    supervisor = LoopSupervisor(lambda: False)
    supervisor.retry_base = 0.001
    calls = 0

    async def step():
        nonlocal calls
        calls += 1
        if calls < 3:
            raise OSError("retry")

    supervisor.start("delivery", step, interval=0.01, critical=True)
    try:
        with pytest.raises(ValueError, match="already"):
            supervisor.start("delivery", step, interval=1)
        await until(lambda: supervisor.snapshot()["delivery"]["last_success"] is not None)
        state = supervisor.snapshot()["delivery"]
        assert state["healthy"] and state["failures"] == 0 and state["next_retry_at"] is None
    finally:
        await supervisor.stop()


async def test_stall_marks_unhealthy_without_cancelling_inflight_operation():
    supervisor = LoopSupervisor(lambda: False)
    started, release = asyncio.Event(), asyncio.Event()

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


def test_readiness_reflects_critical_loop_state_and_database_failure(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient
    from sqlalchemy.exc import OperationalError

    from qqbot.app import create_app
    from qqbot.config import Settings

    app = create_app(Settings(database=str(tmp_path / "health.db")), run_background=False)
    states = {"execution": {"critical": True, "healthy": True, "failures": 0},
              "memory": {"critical": False, "healthy": False, "failures": 5}}
    app.state.core.supervisor = LoopSupervisor(lambda: False)
    monkeypatch.setattr(app.state.core.supervisor, "snapshot", lambda: states)
    with TestClient(app) as client:
        assert client.get("/health/ready").status_code == 200
        states["execution"]["healthy"] = False
        response = client.get("/health/ready")
        assert response.status_code == 503 and not response.json()["background"]["healthy"]
        states["execution"]["healthy"] = True

        def unavailable():
            raise OperationalError("SELECT 1", {}, Exception("private database details"))

        with monkeypatch.context() as failure:
            failure.setattr(app.state.db.engine, "connect", unavailable)
            response = client.get("/health/ready")
            assert response.status_code == 503 and response.json()["database"] == "unavailable"
            assert "private database details" not in response.text
            assert client.get("/health/live").status_code == 200
        assert client.get("/health/ready").status_code == 200
