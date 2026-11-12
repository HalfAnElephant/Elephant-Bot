"""Supervise independent periodic work without cancelling in-flight side effects."""

import asyncio
import logging
import time
from datetime import UTC, datetime, timedelta

logger = logging.getLogger(__name__)


class LoopSupervisor:
    retry_base = 1
    max_failures = 5

    def __init__(self, stopping):
        self.stopping = stopping
        self.loops = {}
        self.tasks = {}

    def start(self, name, step, *, interval, critical=False, stall_seconds=120):
        if name in self.tasks:
            raise ValueError("loop is already supervised")
        self.loops[name] = {
            "critical": critical,
            "state": "STARTING",
            "failures": 0,
            "heartbeat": None,
            "last_success": None,
            "last_error": None,
            "last_error_at": None,
            "next_retry_at": None,
            "stall_seconds": stall_seconds,
            "started": time.monotonic(),
        }
        self.tasks[name] = asyncio.create_task(self._run(name, step, interval), name=f"core:{name}")
        return self.tasks[name]

    async def _run(self, name, step, interval):
        state = self.loops[name]
        try:
            while not self.stopping():
                state.update(
                    next_retry_at=None, state="RUNNING", heartbeat=datetime.now(UTC).isoformat(), started=time.monotonic()
                )
                try:
                    await step()
                except Exception as exc:  # Isolate periodic subsystems and retry safely.
                    state.update(
                        state="RETRYING",
                        failures=state["failures"] + 1,
                        last_error=type(exc).__name__,
                        last_error_at=datetime.now(UTC).isoformat(),
                    )
                    logger.exception(
                        "Background loop failed",
                        extra={
                            "data": {
                                "loop": name,
                                "error_type": type(exc).__name__,
                                "failures": state["failures"],
                            }
                        },
                    )
                    if state["failures"] >= self.max_failures:
                        state["state"] = "FAILED"
                        return
                    delay = min(30, self.retry_base * 2 ** min(state["failures"] - 1, 5))
                    state["next_retry_at"] = (datetime.now(UTC) + timedelta(seconds=delay)).isoformat()
                else:
                    state.update(state="IDLE", failures=0, last_success=datetime.now(UTC).isoformat())
                    delay = interval
                await asyncio.sleep(delay)
        finally:
            if state["state"] != "FAILED":
                state["state"] = "STOPPED"

    def snapshot(self):
        now = time.monotonic()
        result = {}
        for name, state in self.loops.items():
            stalled = (
                state["state"] in {"RUNNING", "STARTING"} and now - state["started"] > state["stall_seconds"]
            )
            exited = self.tasks[name].done()
            result[name] = {key: value for key, value in state.items() if key != "started"} | {
                "stalled": stalled,
                "exited": exited,
                "healthy": not stalled and not exited and state["failures"] < 3,
            }
        return result

    async def stop(self):
        for task in self.tasks.values():
            task.cancel()
        await asyncio.gather(*self.tasks.values(), return_exceptions=True)
