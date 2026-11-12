import json

import httpx
import pytest
from sqlalchemy import func, select, update
from test_agent_runtime import prepare_conditional_review, root_goal
from test_agent_runtime import runtime as agent_runtime_fixture

from qqbot.domain.execution import RunBudget
from qqbot.providers import ProviderAdapter
from qqbot.storage.agent_schema import dialogue_inputs, leader_inbox
from qqbot.storage.learning_schema import learning_sessions
from qqbot.storage.schema import outbox
from qqbot.storage.task_schema import model_requests, notifications, runs, tasks

runtime = agent_runtime_fixture


def short_budget(runtime):
    settings = runtime.sessions.settings
    budget = RunBudget(model_calls=4, tool_calls=10, active_seconds=300, total_wall_seconds=600)
    settings = settings.model_copy(update={"execution": settings.execution.model_copy(update={
        "budget": budget, "interactive_budget": budget})})
    runtime.sessions.settings = settings
    runtime.sessions.learning.settings = settings
    runtime.service.graph.settings = settings


def finish_worker(runtime, claim, worker):
    proof = {"kind": "turn_finished", "session_id": claim["session_id"], "run_id": claim["run_id"],
        "session_revision": "leaf", "input_ids": claim["context"]["source_input_ids"], "inflight": 0, "tools_paired": True}
    return runtime.service.submit(claim["run_id"], claim["fencing_token"], worker,
        {"status": "completed", "summary": "检查完成"}, receipt=proof)


def fail_leader(runtime, claim):
    return runtime.leader.complete(claim["run_id"], claim["fencing_token"], "leader",
        {"status": "failed", "summary": "transport diagnostic", "agent_authored": False}, stopped=True, quiescent=False)


async def reserve_leader(runtime, claim, request_id, kind="business"):
    return await runtime.leader.invoke("reserve-budget", (claim["run_id"], claim["fencing_token"], "leader"),
        {"request_id": request_id, "kind": kind})


def ready_goal(runtime):
    short_budget(runtime)
    root = root_goal(runtime, children=2)
    a, b = runtime.service.claim("a"), runtime.service.claim("b")
    for claim, worker, count in ((a, "a", 2), (b, "b", 1)):
        for index in range(count):
            result = runtime.service.reserve_budget(claim["run_id"], claim["fencing_token"], worker,
                {"request_id": worker + str(index), "kind": "business"})
    assert result["remaining"] == 0
    return root, a, b


async def test_workers_cannot_spend_final_leader_reserve_and_last_request_is_toolless(runtime):
    root, a, b = ready_goal(runtime)
    with pytest.raises(ValueError, match="root model_calls budget exhausted"):
        runtime.service.reserve_budget(a["run_id"], a["fencing_token"], "a", {"request_id": "worker-final", "kind": "completion"})
    finish_worker(runtime, a, "a")
    finish_worker(runtime, b, "b")
    final = runtime.sessions.claim("leader")
    sent = []
    async def transport(request):
        sent.append(json.loads(request.content))
        return httpx.Response(200, json={"choices": [{"message": {"role": "assistant", "content": "检查完成"}}]})
    async def authorize(action):
        return await runtime.leader.invoke("authorize", (final["run_id"], final["fencing_token"], "leader"), {"action": action})
    async def reserve(request_id, kind):
        return await reserve_leader(runtime, final, request_id, kind)
    async def usage(request_id, value):
        return await runtime.leader.invoke("usage", (final["run_id"], final["fencing_token"], "leader"),
            {"request_id": request_id, "usage": value})
    provider = ProviderAdapter("model", "https://example.test/completions", "fixture-key",
        authorize=authorize, reserve=reserve, record=usage, transport=httpx.MockTransport(transport))
    await provider.complete({"model": "model", "messages": [{"role": "user", "content": "核验结果"}],
        "tools": [{"type": "function", "function": {"name": "tasks_status", "parameters": {"type": "object"}}}]})
    assert sent[0]["tool_choice"] == "none"
    assert provider.business_exhausted
    with runtime.db.engine.connect() as conn:
        assert json.loads(conn.execute(select(tasks.c.budget_json).where(tasks.c.id == root["task_id"])).scalar_one())["model_calls"] == 4
    proof = {"kind": "turn_finished", "session_id": final["session_id"], "run_id": final["run_id"],
        "session_revision": "leaf", "input_ids": final["context"]["source_input_ids"], "inflight": 0, "tools_paired": True}
    runtime.leader.complete(final["run_id"], final["fencing_token"], "leader",
        {"status": "completed", "summary": "检查完成"}, stopped=True, quiescent=False, receipt=proof)
    runtime.service.recover()
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == root["task_id"])).scalar_one() == "SUCCEEDED"
        assert len(conn.execute(select(notifications)).all()) == 1


async def test_failed_final_reserved_request_settles_once_without_reopening_inbox(runtime):
    root, a, b = ready_goal(runtime)
    finish_worker(runtime, a, "a")
    finish_worker(runtime, b, "b")
    final = runtime.sessions.claim("leader")
    assert (await reserve_leader(runtime, final, "last-request"))["remaining"] == 0
    fail_leader(runtime, final)
    fail_leader(runtime, final)
    runtime.service.recover()
    assert runtime.sessions.claim("leader") is None
    with runtime.db.engine.connect() as conn:
        task = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
        assert task["state"] == "BLOCKED"
        assert "transport diagnostic" not in task["result_json"]
        assert all(conn.execute(select(leader_inbox.c.handled_at)).scalars())
        assert len(conn.execute(select(notifications)).all()) == 1
        assert len(conn.execute(select(outbox)).all()) == 1


async def test_progress_cannot_spend_final_reserve_or_retry_its_exhausted_inbox(runtime):
    _root, a, _b = ready_goal(runtime)
    with runtime.db.transaction() as conn:
        child = conn.execute(select(tasks).where(tasks.c.id == a["task_id"])).mappings().one()
        runtime.service.graph.event(conn, child, "milestone", {"public_text": "第一阶段完成"}, wake=True)
    runtime.clock.advance(3)
    progress = runtime.sessions.claim("leader")
    assert progress["purpose"] == "progress"
    with pytest.raises(ValueError, match="root budget exhausted"):
        await reserve_leader(runtime, progress, "progress-budget")
    fail_leader(runtime, progress)
    assert runtime.sessions.claim("leader") is None
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(leader_inbox.c.handled_at)).scalar_one()


def test_execution_budget_boundary_settles_idle_children_but_waits_for_live_resources(runtime):
    short_budget(runtime)
    root = root_goal(runtime)
    with runtime.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.id == root["task_id"]).values(budget_json=json.dumps({"model_calls": 3})))
    runtime.service.job_tree_quiescent = lambda _task: False
    runtime.service.recover()
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == root["task_id"])).scalar_one() == "WAITING_CHILDREN"
    runtime.service.job_tree_quiescent = lambda _task: True
    runtime.service.recover()
    runtime.service.recover()
    with runtime.db.engine.connect() as conn:
        assert set(conn.execute(select(tasks.c.state)).scalars()) == {"BLOCKED"}
        assert len(conn.execute(select(outbox)).all()) == 1


async def test_user_retries_share_one_budget_and_fixed_deadline(runtime):
    short_budget(runtime)
    runtime.sessions.enqueue(runtime.event, runtime.access)
    first = runtime.sessions.claim("leader")
    for index in range(2):
        await reserve_leader(runtime, first, "first-" + str(index))
    fail_leader(runtime, first)
    runtime.clock.advance(1)
    second = runtime.sessions.claim("leader")
    assert second["deadline"] == first["deadline"]
    assert (await reserve_leader(runtime, second, "second-business"))["remaining"] == 0
    await reserve_leader(runtime, second, "second-completion", "completion")
    fail_leader(runtime, second)
    assert runtime.sessions.claim("leader") is None
    assert runtime.sessions.claim("leader") is None
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(dialogue_inputs.c.handled_at)).scalar_one()
        assert conn.execute(select(func.count()).select_from(model_requests)).scalar_one() == 4
        assert conn.execute(select(func.count()).select_from(runs)).scalar_one() == 2
        assert len(conn.execute(select(outbox)).all()) == 1


def test_user_deadline_is_measured_from_acceptance_across_failures(runtime):
    short_budget(runtime)
    runtime.sessions.enqueue(runtime.event, runtime.access)
    first = runtime.sessions.claim("leader")
    fail_leader(runtime, first)
    runtime.clock.advance(601)
    assert runtime.sessions.claim("leader") is None
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(runs)).scalar_one() == 1
        assert conn.execute(select(dialogue_inputs.c.handled_at)).scalar_one()


async def test_review_retries_share_sixteen_calls_and_a_fixed_deadline(runtime):
    await prepare_conditional_review(runtime)
    first = runtime.leader.claim_review("leader")
    for index in range(8):
        await reserve_leader(runtime, first, "review-first-" + str(index))
    fail_leader(runtime, first)
    runtime.clock.advance(1)
    second = runtime.leader.claim_review("leader")
    assert second["deadline"] == first["deadline"]
    for index in range(7):
        await reserve_leader(runtime, second, "review-second-" + str(index))
    await reserve_leader(runtime, second, "review-completion", "completion")
    fail_leader(runtime, second)
    assert runtime.leader.claim_review("leader") is None
    with runtime.db.engine.connect() as conn:
        row = conn.execute(select(learning_sessions)).mappings().one()
        assert row["pending_json"] is None
        assert json.loads(row["state_json"])["review_pending"] is False
        assert conn.execute(select(func.count()).select_from(model_requests).join(runs, runs.c.id == model_requests.c.run_id)
            .where(runs.c.role == "skill_review")).scalar_one() == 16


async def test_expired_review_intent_does_not_start_a_new_attempt(runtime):
    await prepare_conditional_review(runtime)
    runtime.clock.advance(3601)
    assert runtime.leader.claim_review("leader") is None
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(learning_sessions.c.pending_json)).scalar_one() is None
        assert conn.execute(select(func.count()).select_from(runs).where(runs.c.role == "skill_review")).scalar_one() == 0
