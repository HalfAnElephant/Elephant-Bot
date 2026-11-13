"""Workers are admitted dynamically while idle caches and authority stay bounded."""
import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import select, update
from test_agent_runtime import runtime as runtime  # noqa: PLC0414 - Register the shared pytest fixture.

from qqbot.domain.agents import GoalSpec, WaitSpec
from qqbot.domain.messages import TextSegment
from qqbot.harness.pool import SessionPool
from qqbot.runtime.scheduler import Scheduler
from qqbot.storage.agent_schema import agent_events, leader_inbox
from qqbot.storage.task_schema import tasks


def submit(r, index=0, children=1):
    event = r.event.model_copy(update={"event_id": str(uuid4()), "segments": (TextSegment(text=f"检查项目{index}"),)})
    with r.db.transaction() as conn:
        dialogue = r.sessions.dialogue(conn, event)
        return r.service.graph.submit(conn, dialogue, event, r.access,
            {"goal": event.text, "children": [{"goal": f"检查{index}-{part}", "delegation_key": str(part)}
                                             for part in range(children)]},
            str(uuid4()), r.sessions.binding(), {})


def test_legacy_root_node_depth_and_parallelism_values_do_not_restrict_workers(runtime):
    r = runtime
    settings = r.service.graph.settings
    r.service.graph.settings = settings.model_copy(update={"orchestration": settings.orchestration.model_copy(update={
        "pending_root_tasks_per_owner": 1, "max_nodes_per_root": 2, "max_task_depth": 1, "child_parallelism": 1})})
    root = submit(r, children=40)
    submit(r, index=1)
    with r.db.transaction() as conn:
        parent = conn.execute(select(tasks).where(tasks.c.parent_task_id == root["task_id"])).mappings().first()
        for depth in range(6):
            child = r.service.graph.spawn(conn, parent, {"goal": f"深入检查{depth}", "delegation_key": str(depth)}, r.access)
            parent = conn.execute(select(tasks).where(tasks.c.id == child["task_id"])).mappings().one()
    claims = []
    while claim := r.service.claim("worker-" + str(len(claims))):
        claims.append(claim)
    assert len(claims) == 47
    assert len({claim["task_id"] for claim in claims}) == 47
    assert len(GoalSpec.model_validate({"goal": "many", "children": [
        {"goal": "child", "delegation_key": str(i)} for i in range(40)]}).children) == 40
    assert len(WaitSpec(children=[claim["task_id"] for claim in claims]).children) == 47


class Runner:
    def __init__(self, claim, process_id, session_id):
        self.process_id = process_id
        self.quiescent_verified = True
        self.stopped_verified = False

    async def bind(self, claim):
        pass

    async def stop(self):
        self.stopped_verified = True
        return True


async def test_pool_admits_more_than_32_busy_processes_and_keeps_only_idle_cache():
    pool = SessionPool(Runner, max_resident=1, max_idle=2)
    try:
        runners = [await pool.acquire({"task_id": str(index), "session_id": str(index),
            "context": {"pool": {"max_resident": 1, "max_idle": 2, "ttl": 300}}}) for index in range(40)]
        assert len(pool.entries) == 40
        for runner in runners:
            await pool.release(runner, committed=True)
        assert len(pool.entries) == 2
        assert all(entry["state"] == "IDLE" for entry in pool.entries.values())
    finally:
        await pool.close()


async def test_unknown_process_is_retained_without_blocking_new_admission():
    pool = SessionPool(Runner, max_resident=1)
    try:
        first = await pool.acquire({"task_id": "first"})
        async def failed_stop():
            return False
        first.stop = failed_stop
        await pool.release(first, committed=False)
        assert pool.entries[first.pool_id]["state"] == "UNKNOWN"
        second = await pool.acquire({"task_id": "second"})
        assert second is not first and len(pool.entries) == 2
    finally:
        await pool.close()


async def test_scheduler_drains_real_pending_claims_and_creates_no_idle_cycles():
    scheduler = Scheduler.__new__(Scheduler)
    scheduler.active = set()
    scheduler.jobs_recovered = True
    scheduler.job_maintenance = None
    scheduler.next_job_maintenance = float("inf")
    pending = [{"role": "leader" if index == 39 else "worker", "run_id": str(index)} for index in range(40)]
    started = []
    release = asyncio.Event()
    async def call(*args, **kwargs):
        return pending.pop(0) if pending else None
    async def replay():
        pass
    async def once(claim):
        started.append(claim)
        await release.wait()
        return True
    async def sweep():
        pass
    scheduler.worker = SimpleNamespace(call=call, replay=replay, replay_lock=asyncio.Lock(),
        once=once, begin_shutdown=lambda: None)
    scheduler.pool = SimpleNamespace(sweep=sweep, close=sweep)
    try:
        assert await scheduler.step() is False
        assert len(scheduler.active) == len(started) == 40
        assert started[-1]["role"] == "leader"
        assert await scheduler.step() is False
        assert len(scheduler.active) == 40
        release.set()
        await asyncio.gather(*scheduler.active)
        assert await scheduler.step() is True
        assert not scheduler.active
        assert await scheduler.step() is True
        assert len(started) == 40
    finally:
        await scheduler.close()


@pytest.mark.parametrize("state", ["FAILED", "BLOCKED", "EXPIRED", "PARTIAL"])
@pytest.mark.parametrize("nested", [False, True])
def test_problem_results_alert_leader_immediately_even_with_unfinished_siblings(runtime, state, nested):
    r = runtime
    root = submit(r, children=2)
    with r.db.transaction() as conn:
        child = conn.execute(select(tasks).where(tasks.c.parent_task_id == root["task_id"])).mappings().first()
        if nested:
            descendant = r.service.graph.spawn(conn, child, {"goal": "nested", "delegation_key": "nested"}, r.access)
            child = conn.execute(select(tasks).where(tasks.c.id == descendant["task_id"])).mappings().one()
        conn.execute(update(tasks).where(tasks.c.id == child["id"]).values(state=state))
        child = {**dict(child), "state": state}
        r.service.graph.settled(conn, child, "run-problem", {"status": "failed", "summary": "needs decision"})
        r.service.graph.settled(conn, child, "run-problem", {"status": "failed", "summary": "needs decision"})
    with r.db.engine.connect() as conn:
        alerts = conn.execute(select(leader_inbox).where(leader_inbox.c.resume_reason == "needs_attention")).mappings().all()
        assert len(alerts) == 1 and alerts[0]["root_task_id"] == root["task_id"]
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == root["task_id"])).scalar_one() == "WAITING_CHILDREN"
        assert conn.execute(select(agent_events).where(agent_events.c.kind == "child_result")).first()


def test_root_answer_event_wakes_leader_once(runtime):
    r = runtime
    root = submit(r)
    with r.db.transaction() as conn:
        task = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
        for _ in range(2):
            r.service.graph.event(conn, task, "user_answer", {"answer": "选择A"},
                event_id="root-answer", wake=True)
    with r.db.engine.connect() as conn:
        notices = conn.execute(select(leader_inbox).where(leader_inbox.c.resume_reason == "user_answer")).mappings().all()
        assert len(notices) == 1 and notices[0]["root_task_id"] == root["task_id"]
