"""Correlate latency samples without logging message or request contents."""

import asyncio
import logging
import os
import time
from contextlib import contextmanager

logger = logging.getLogger(__name__)


def record_timing(stage, *, event_id=None, **fields):
    logger.info(stage, extra={"data": {
        "event_id": event_id, "pid": os.getpid(), "monotonic_ns": time.monotonic_ns(), **fields,
    }})


@contextmanager
def measure(stage, event_id, **fields):
    started = time.monotonic_ns()
    try:
        yield
    finally:
        record_timing(stage, event_id=event_id, started_ns=started,
                      elapsed_ms=(time.monotonic_ns() - started) / 1_000_000, **fields)


async def sample_event_loop():
    due = time.monotonic_ns() + 1_000_000_000
    await asyncio.sleep(1)
    lag = time.monotonic_ns() - due
    if lag >= 100_000_000:
        record_timing("event_loop.lag", due_ns=due, lag_ms=lag / 1_000_000)
