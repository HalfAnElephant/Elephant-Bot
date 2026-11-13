"""Model request backpressure leaves an interactive request slot when available."""
import asyncio
from unittest.mock import AsyncMock

import pytest

from qqbot.harness.gateway import RunGateway
from qqbot.providers import ModelCapacity


async def occupy(capacity, background, entered, release):
    async with capacity.slot(background):
        entered.set()
        await release.wait()


async def test_waiting_background_requests_do_not_occupy_leader_reserve():
    capacity = ModelCapacity(total=2, reserved=1)
    first, second, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    a = asyncio.create_task(occupy(capacity, True, first, release))
    await first.wait()
    b = asyncio.create_task(occupy(capacity, True, second, release))
    try:
        async with asyncio.timeout(1):
            async with capacity.slot(False):
                assert not second.is_set() and not a.done()
        b.cancel()
        await asyncio.gather(b, return_exceptions=True)
        release.set()
        await a
        async with asyncio.timeout(1):
            async with capacity.slot(True):
                pass
    finally:
        release.set()
        await asyncio.gather(a, b, return_exceptions=True)


async def test_one_request_configuration_retains_provider_concurrency_limit():
    capacity = ModelCapacity(total=1)
    worker, leader, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    a = asyncio.create_task(occupy(capacity, True, worker, release))
    await worker.wait()
    b = asyncio.create_task(occupy(capacity, False, leader, release))
    try:
        await asyncio.sleep(0)
        assert not leader.is_set()
        release.set()
        await asyncio.wait_for(asyncio.gather(a, b), 1)
        assert leader.is_set()
    finally:
        release.set()
        await asyncio.gather(a, b, return_exceptions=True)


@pytest.mark.parametrize(("role", "background"), [("leader", False), ("worker", True), ("skill_review", True)])
def test_model_capacity_classification_follows_role(tmp_path, role, background):
    gateway = RunGateway(tmp_path, "offline", "", "https://model.invalid", call=AsyncMock())
    gateway.bind({"task_id": "task", "run_id": "run", "fencing_token": 1, "goal": "test",
        "role": role, "delivery": "ATTACHED", "permission_snapshot": {"principal": {}}})
    assert gateway.provider.background is background
    gateway.revoke()
