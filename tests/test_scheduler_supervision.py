import asyncio
from types import SimpleNamespace

import pytest

from qqbot.observability.supervision import LoopSupervisor
from qqbot.runtime.scheduler import Scheduler


def scheduler_for(once):
    scheduler = Scheduler.__new__(Scheduler)
    scheduler.core = SimpleNamespace(tasks=SimpleNamespace(max_active=1))
    scheduler.worker = SimpleNamespace(once=once)
    async def sweep():
        pass
    scheduler.pool = SimpleNamespace(sweep=sweep, close=sweep)
    scheduler.active = set()
    scheduler.jobs_recovered = True
    scheduler.job_maintenance = None
    scheduler.next_job_maintenance = float("inf")
    return scheduler


def supervise(scheduler, *, stall_seconds=0.01):
    supervisor = LoopSupervisor(lambda: False)
    supervisor.retry_base = 0.001
    supervisor.start("scheduler", scheduler.step, interval=0.001, critical=True, stall_seconds=stall_seconds)
    return supervisor


async def test_repeated_async_worker_failures_exhaust_supervision_without_spawn_resets():
    attempts = 0
    async def once():
        nonlocal attempts
        attempts += 1
        raise ValueError("claim decoding failed")
    scheduler = scheduler_for(once)
    supervisor = supervise(scheduler)
    try:
        await asyncio.wait_for(supervisor.tasks["scheduler"], 2)
        state = supervisor.snapshot()["scheduler"]
        assert attempts == supervisor.max_failures
        assert state["state"] == "FAILED"
        assert state["failures"] == supervisor.max_failures
        assert state["healthy"] is False
        assert state["exited"] is True
    finally:
        await supervisor.stop()
        await scheduler.close()


async def test_actual_completed_idle_worker_clears_errors_after_recovery():
    started, release, third = asyncio.Event(), asyncio.Event(), asyncio.Event()
    attempts = 0
    async def once():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ValueError("claim decoding failed")
        if attempts == 2:
            started.set()
            await release.wait()
            return False  # A decoded idle claim is a successful Worker cycle.
        third.set()
        await asyncio.Event().wait()
    scheduler = scheduler_for(once)
    supervisor = supervise(scheduler)
    try:
        await asyncio.wait_for(started.wait(), 1)
        await asyncio.sleep(0.03)
        state = supervisor.snapshot()["scheduler"]
        assert state["failures"] == 1
        assert state["last_success"] is None
        assert state["stalled"] is False
        release.set()
        await asyncio.wait_for(third.wait(), 1)
        state = supervisor.snapshot()["scheduler"]
        assert state["failures"] == 0
        assert state["last_success"] is not None
        assert state["healthy"] is True
    finally:
        await supervisor.stop()
        await scheduler.close()


async def test_long_running_worker_keeps_heartbeat_without_false_success_or_stall():
    started = asyncio.Event()
    async def once():
        started.set()
        await asyncio.Event().wait()
    scheduler = scheduler_for(once)
    supervisor = supervise(scheduler)
    try:
        await asyncio.wait_for(started.wait(), 1)
        initial = supervisor.snapshot()["scheduler"]["heartbeat"]
        await asyncio.sleep(0.05)
        state = supervisor.snapshot()["scheduler"]
        assert state["heartbeat"] != initial
        assert state["last_success"] is None
        assert state["failures"] == 0
        assert state["stalled"] is False
        assert state["healthy"] is True
    finally:
        await supervisor.stop()
        await scheduler.close()


async def test_failed_batch_is_drained_before_retry_admission():
    async def once():
        await asyncio.Event().wait()
    scheduler = scheduler_for(once)
    a, b = asyncio.get_running_loop().create_future(), asyncio.get_running_loop().create_future()
    a.set_exception(ValueError("a"))
    b.set_exception(RuntimeError("b"))
    scheduler.active.update((a, b))
    try:
        with pytest.raises((ValueError, RuntimeError)):
            await scheduler.step()
        assert not scheduler.active
        assert await scheduler.step() is False
        assert len(scheduler.active) == 1
    finally:
        await scheduler.close()


async def test_none_returning_supervised_steps_keep_existing_success_semantics():
    ran = asyncio.Event()
    supervisor = LoopSupervisor(lambda: False)
    async def step():
        ran.set()
    supervisor.start("ordinary", step, interval=1)
    try:
        await asyncio.wait_for(ran.wait(), 1)
        assert supervisor.snapshot()["ordinary"]["last_success"] is not None
    finally:
        await supervisor.stop()
