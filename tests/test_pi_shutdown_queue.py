"""Native stdout backpressure cannot prevent a verified Worker cancellation."""
import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import select
from test_agent_runtime import root_goal
from test_agent_runtime import runtime as agent_runtime

from qqbot.adapters.execution.runtime import JobRuntime
from qqbot.harness.pi import PiRunnerAdapter
from qqbot.harness.pool import SessionPool
from qqbot.jobs.api import internal_router
from qqbot.jobs.worker import GeneralRun, Spool, Worker
from qqbot.providers import ModelCapacity
from qqbot.storage.task_schema import processes, runs, tasks

runtime = agent_runtime


async def test_worker_cancel_during_pi_initialization_with_full_stdout_queue_records_stop(runtime, tmp_path, monkeypatch):
    core = runtime.leader.core
    core.settings, core.sessions = runtime.sessions.settings, runtime.sessions
    jobs = JobRuntime(core, tmp_path, provider=SimpleNamespace())
    root = root_goal(runtime)
    app = FastAPI()
    app.include_router(internal_router(runtime.service, "x" * 32))
    started = asyncio.Event()
    created, stopped_receipts = [], []
    args = SimpleNamespace(workspace=str(tmp_path / "pi"), archive=str(tmp_path / "archive"),
        pi=__file__, model="offline", model_endpoint="https://model.invalid/v1/chat/completions", worker_id="worker")
    def process_gone(*_):
        raise ProcessLookupError
    monkeypatch.setattr("qqbot.harness.pi.os.killpg", process_gone)
    class InitializingRun(GeneralRun):
        async def start(self):
            stdout = asyncio.StreamReader()
            self.process = SimpleNamespace(pid=123456, returncode=0, stdout=stdout)
            for index in range(self.queue.maxsize + 1):
                stdout.feed_data((json.dumps({"type": "session_start", "index": index}) + "\n").encode())
            self.readers = [asyncio.create_task(self._stdout())]
            started.set()
            await asyncio.Event().wait()  # Cancellation lands before Pi initialization has completed.
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://core",
                                headers={"Authorization": "Bearer " + "x" * 32}) as client:
        class RecordingClient:
            async def post(self, path, **kwargs):
                if path.endswith("/process-stopped"):
                    stopped_receipts.append(kwargs["json"]["payload"])
                return await client.post(path, **kwargs)
        transport = RecordingClient()
        def factory(claim, process_id, session_id):
            runner = InitializingRun(args, claim, transport, process_id, session_id, ModelCapacity())
            created.append(runner)
            return runner
        pool = SessionPool(factory)
        worker = Worker(transport, "worker", Spool(tmp_path / "spool"), None, pool=pool)
        running = asyncio.create_task(worker.once())
        try:
            await asyncio.wait_for(started.wait(), 1)
            runner = created[0]
            while not runner.queue.full():
                await asyncio.sleep(0)
            assert runtime.service.cancel(root["public_id"], runtime.access)["state"] == "CANCEL_REQUESTED"
            done, _ = await asyncio.wait({running}, timeout=1)
            assert running in done, "Worker stop blocked behind a full stdout queue"
            assert running.result() is True
            runtime.service.recover()
            await jobs.maintain()
            assert stopped_receipts and all(item["stopped"] is True for item in stopped_receipts)
            assert all(item["process_id"] == runner.process_id and item["generation"] == 1 for item in stopped_receipts)
            with runtime.db.engine.connect() as conn:
                run = conn.execute(select(runs).where(runs.c.role == "worker")).mappings().one()
                assert run["slot_reserved"] == 0 and run["runner_stopped_at"]
                assert conn.execute(select(processes.c.state)).scalar_one() == "STOPPED"
                assert set(conn.execute(select(tasks.c.state)).scalars()) == {"CANCELLED"}
            assert all(task.done() for task in runner.readers)
        finally:
            if not running.done():
                # Release the reproduction's bounded queue so a failing assertion can clean up.
                while not created[0].queue.empty():
                    created[0].queue.get_nowait()
                await asyncio.wait_for(running, 2)
            await pool.close()


@pytest.mark.parametrize("queued", [0, 8])
async def test_normal_stdout_eof_preserves_queued_events_and_fails_pending_rpc(tmp_path, queued):
    runner = PiRunnerAdapter(__file__, tmp_path)
    stdout = asyncio.StreamReader()
    runner.process = SimpleNamespace(stdout=stdout)
    expected = [{"type": "session_start", "index": index} for index in range(queued)]
    for event in expected:
        runner.queue.put_nowait(event)
    pending = asyncio.get_running_loop().create_future()
    runner.responses["rpc"] = pending
    stdout.feed_eof()
    reader = asyncio.create_task(runner._stdout())
    await asyncio.sleep(0)
    with pytest.raises(RuntimeError, match="RPC closed before command acknowledgement"):
        await pending
    observed = [await runner.queue.get() for _ in range(queued + 1)]
    assert observed == [*expected, {"type": "host_eof"}]
    await asyncio.wait_for(reader, 1)
