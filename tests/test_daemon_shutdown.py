"""Daemon stop preserves the durable run for the next host-epoch recovery."""
import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest
from sqlalchemy import select
from test_worker_failures import environment as worker_environment
from test_worker_failures import pool_for

from qqbot.jobs.worker import Spool, Worker
from qqbot.runtime.scheduler import Scheduler
from qqbot.storage.task_schema import runs, tasks

environment = worker_environment


@pytest.mark.parametrize("persisted_result", [False, True])
async def test_scheduler_shutdown_stops_runner_without_terminalizing_task(environment, tmp_path, persisted_result):
    service, original, app = environment
    pool, created = pool_for(tmp_path, service, error="wait")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://core",
                                headers={"Authorization": "Bearer " + "x" * 32}) as client:
        worker = Worker(client, "worker", Spool(tmp_path / "spool"), None, pool=pool)
        scheduler = Scheduler.__new__(Scheduler)
        scheduler.worker, scheduler.pool = worker, pool
        scheduler.job_maintenance = None
        execution = asyncio.create_task(worker.once())
        scheduler.active = {execution}
        while not created:
            await asyncio.sleep(0)
        await created[0].started.wait()
        with service.db.engine.connect() as conn:
            before = dict(conn.execute(select(runs)).mappings().one())
        path = None
        if persisted_result:
            path = worker.spool.save({"run_id": before["id"], "fencing_token": created[0].claim["fencing_token"],
                "result": {"status": "completed", "summary": "Original durable result"}, "stopped": True})
            saved = path.read_bytes()
        await scheduler.close()
        assert execution.cancelled()
        assert created[0].stopped_verified
        assert not pool.entries
        with service.db.engine.connect() as conn:
            after = conn.execute(select(runs)).mappings().one()
            task = conn.execute(select(tasks).where(tasks.c.id == original["id"])).mappings().one()
        assert after["id"] == before["id"]
        assert after["slot_reserved"] == 1
        assert after["result_json"] is None
        assert task["state"] == "RUNNING"
        assert task["failure_notice_json"] is None
        assert task["session_id"] == created[0].claim["session_id"]
        if path:
            assert path.read_bytes() == saved
            assert json.loads(path.read_text())["result"]["summary"] == "Original durable result"
        else:
            assert not list(worker.spool.directory.glob("*.json"))
        assert await worker.once() is False


async def test_scheduler_marks_shutdown_before_cancelling_maintenance_and_workers():
    order = []
    started = asyncio.Event()
    async def running():
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            order.append("cancelled")
            raise
    scheduler = Scheduler.__new__(Scheduler)
    scheduler.worker = SimpleNamespace(begin_shutdown=lambda: order.append("shutdown"))
    scheduler.job_maintenance = asyncio.create_task(running())
    await started.wait()
    scheduler.active = {asyncio.create_task(running())}
    async def close_pool():
        order.append("pool_closed")
    scheduler.pool = SimpleNamespace(close=close_pool)
    await asyncio.sleep(0)
    await scheduler.close()
    assert order == ["shutdown", "cancelled", "cancelled", "pool_closed"]
