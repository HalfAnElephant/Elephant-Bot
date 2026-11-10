"""Two-level scheduling checks with real leases and deterministic model transports."""

import asyncio
import sys
from types import SimpleNamespace

import httpx
from sqlalchemy import select, update
from test_tasks_control import setup as _task_setup

from qqbot.domain.execution import ExecutionPlan
from qqbot.harness.research import ResearchCapabilities
from qqbot.jobs.worker import GeneralRun, ResearchRun
from qqbot.providers import ModelCapacity, ProviderAdapter
from qqbot.storage.task_schema import runs, tasks

task_setup = _task_setup


async def until(condition):
    async with asyncio.timeout(1):
        while not condition():
            await asyncio.sleep(0)


def accept(service, event, access, number, *, plan=None):
    return service.accept(
        event.model_copy(update={"event_id": f"event-{number}"}), access, f"request {number}", plan=plan
    )


def four_attached(service, event, access):
    claims = []
    for number in range(4):
        accept(service, event, access, number)
        claims.append(service.claim("worker"))
    return claims


def test_long_tools_detach_without_releasing_runs_and_allow_one_interaction(task_setup):
    service, event, access, clock = task_setup
    original = four_attached(service, event, access)
    clock.advance(6)
    service.detach_due()
    accept(service, event, access, 4)
    fresh = service.claim("worker")
    assert fresh and fresh["execution_priority"] == "INTERACTIVE"
    accept(service, event, access, 5)
    assert service.claim("worker") is None
    with service.db.engine.connect() as conn:
        rows = conn.execute(select(runs.c.id, runs.c.slot_reserved)).all()
    assert len(rows) == 5 and all(reserved == 1 for _, reserved in rows)
    assert {claim["run_id"] for claim in original} <= {run_id for run_id, _ in rows}
    assert all(
        service.control(claim["run_id"], claim["fencing_token"], "worker")["execution_priority"]
        == "BACKGROUND"
        for claim in original
    )
    service.uncertain(fresh["run_id"], fresh["fencing_token"], "worker")
    assert service.claim("worker") is None
    with service.db.engine.connect() as conn:
        assert (
            conn.execute(select(runs.c.slot_reserved).where(runs.c.id == fresh["run_id"])).scalar_one() == 1
        )


def test_queued_request_detaches_before_claim_and_priority_is_independent(task_setup):
    service, event, access, clock = task_setup
    four_attached(service, event, access)
    old_queued = accept(service, event, access, 4)
    clock.advance(6)
    assert service.claim("worker") is None
    fresh = accept(service, event, access, 5)
    claim = service.claim("worker")
    assert claim["task_id"] == fresh["id"]
    with service.db.transaction() as conn:
        assert (
            conn.execute(
                select(tasks.c.execution_priority).where(tasks.c.id == old_queued["id"])
            ).scalar_one()
            == "BACKGROUND"
        )
        conn.execute(update(tasks).where(tasks.c.id == fresh["id"]).values(execution_priority="BACKGROUND"))
    permission = service.authorize(
        claim["run_id"], claim["fencing_token"], "worker", {"action": "model.request"}
    )
    assert permission["execution_priority"] == "BACKGROUND"
    assert service.control(claim["run_id"], claim["fencing_token"], "worker")["delivery"] == "ATTACHED"


def test_new_background_claims_keep_sustained_limit(task_setup):
    service, event, access, _clock = task_setup
    for number in range(3):
        accept(service, event, access, number, plan=ExecutionPlan(initial_delivery="DETACHED"))
    assert service.claim("worker") and service.claim("worker")
    assert service.claim("worker") is None


async def noop(*args):
    return {"remaining": 10}


def model_body(text):
    return {"model": "offline", "messages": [{"role": "user", "content": text}]}


async def test_four_inflight_models_detach_and_new_model_starts_with_reserved_slot(task_setup):
    service, event, access, clock = task_setup
    claims = four_attached(service, event, access)
    capacity = ModelCapacity(total=5, reserved=1)
    release = asyncio.Event()
    started = []
    sustained_at_request = []

    async def upstream(request):
        started.append(request)
        sustained_at_request.append(capacity.sustained._value)
        if len(started) <= 4:
            await release.wait()
        return httpx.Response(200, json={"choices": [{"message": {"content": "done"}}]})

    def provider(claim):
        async def authorize(action):
            return service.authorize(claim["run_id"], claim["fencing_token"], "worker", {"action": action})

        return ProviderAdapter(
            "offline",
            "https://model.invalid",
            "key",
            authorize=authorize,
            reserve=noop,
            record=noop,
            capacity=capacity,
            transport=httpx.MockTransport(upstream),
        )

    providers = [provider(claim) for claim in claims]
    running = [asyncio.create_task(adapter.complete(model_body("slow"))) for adapter in providers]
    try:
        await until(lambda: len(started) == 4)
        clock.advance(6)
        service.detach_due()
        fresh_task = accept(service, event, access, 4)
        claim = service.claim("worker")
        assert claim["task_id"] == fresh_task["id"]
        await asyncio.wait_for(provider(claim).complete(model_body("你好")), timeout=0.5)
        assert len(started) == 5 and not any(task.done() for task in running)
        release.set()
        await asyncio.gather(*running)
        # The same adapters read the changed priority at their next model boundary.
        await providers[0].complete(model_body("continue"))
        assert sustained_at_request[-1] == 3
        assert capacity.total._value == 5 and capacity.sustained._value == 4
    finally:
        release.set()
        await asyncio.gather(*running, return_exceptions=True)


async def test_waiting_model_reclassifies_after_detach_and_leaves_foreground_slot():
    capacity = ModelCapacity(total=2, reserved=1)
    background_gate, foreground_gate = asyncio.Event(), asyncio.Event()
    acquired = set()

    async def occupy(name, background, gate):
        async with capacity.slot(background):
            acquired.add(name)
            await gate.wait()

    background = asyncio.create_task(occupy("background", True, background_gate))
    foreground = asyncio.create_task(occupy("foreground", False, foreground_gate))
    priority = "INTERACTIVE"
    requests = []

    async def authorize(action):
        return {"execution_priority": priority}

    async def upstream(request):
        requests.append(request)
        return httpx.Response(200, json={"choices": []})

    adapter = ProviderAdapter(
        "offline",
        "https://model.invalid",
        "key",
        authorize=authorize,
        reserve=noop,
        record=noop,
        capacity=capacity,
        transport=httpx.MockTransport(upstream),
    )
    queued = None
    try:
        await until(lambda: len(acquired) == 2)
        queued = asyncio.create_task(adapter.complete(model_body("queued")))
        await until(lambda: bool(capacity.total._waiters))
        priority = "BACKGROUND"
        foreground_gate.set()
        await foreground
        # Give the reclassified request time to move to the sustained queue.
        await until(lambda: bool(capacity.sustained._waiters))
        async with asyncio.timeout(0.5), capacity.slot(False):
            assert not requests and not queued.done()
        background_gate.set()
        await asyncio.wait_for(queued, 0.5)
        assert len(requests) == 1
    finally:
        foreground_gate.set()
        background_gate.set()
        if queued and not queued.done():
            queued.cancel()
        await asyncio.gather(background, foreground, *([queued] if queued else []), return_exceptions=True)
    assert capacity.total._value == 2 and capacity.sustained._value == 1


async def test_cancelled_capacity_wait_does_not_release_running_slot():
    capacity = ModelCapacity(total=2, reserved=1)
    async with capacity.slot(True):
        pending = asyncio.create_task(capacity.slot(True).__aenter__())
        await until(lambda: bool(capacity.sustained._waiters))
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
        assert capacity.total._value == 1 and capacity.sustained._value == 0
    assert capacity.total._value == 2 and capacity.sustained._value == 1


async def test_legacy_research_and_general_runners_share_model_capacity(tmp_path, monkeypatch):
    capacity = ModelCapacity(total=2, reserved=1)
    args = SimpleNamespace(
        worker_id="worker",
        workspace=str(tmp_path / "work"),
        archive=str(tmp_path / "archive"),
        spool=str(tmp_path / "spool"),
        model="offline",
        pi=sys.executable,
        model_endpoint="https://model.invalid",
    )
    claim = {
        "run_id": "run",
        "session_id": "session",
        "task_id": "task",
        "fencing_token": 1,
        "permission_snapshot": {"principal": {}},
        "context": {},
        "goal": "test",
    }
    monkeypatch.setattr("qqbot.jobs.worker.configured_github", lambda args: None)
    general = GeneralRun(args, claim, None, "process", "session", capacity)
    research = ResearchRun(args, claim, None, capacity)
    assert general.gateway.capacity is capacity
    assert research.research_capabilities.capacity is capacity
    # The legacy route must actually wait on the same model slots.
    service = ResearchCapabilities(
        tmp_path / "legacy",
        "offline",
        "key",
        capacity=capacity,
        background=True,
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={"choices": []})),
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=service.app), base_url="http://research"
    ) as client:
        async with capacity.slot(True):
            pending = asyncio.create_task(
                client.post(
                    "/v1/chat/completions",
                    json=model_body("legacy"),
                    headers={"Authorization": "Bearer " + service.model_token},
                )
            )
            await until(lambda: bool(capacity.sustained._waiters))
            assert not pending.done()
        assert (await pending).status_code == 200
