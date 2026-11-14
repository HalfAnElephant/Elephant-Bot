"""Escalation decisions keep retries, direct execution and user answers host-authorized."""
import json
from datetime import timedelta

import pytest
from sqlalchemy import select, update
from test_agent_runtime import leader_complete, root_goal, worker_complete
from test_agent_runtime import runtime as agent_runtime
from test_worker_escalation import expire_task_budget

from qqbot.domain import TextSegment
from qqbot.runtime.tools import native_tools
from qqbot.storage.action_schema import operations
from qqbot.storage.task_schema import inputs, runs, tasks, waits

runtime = agent_runtime


def failed_attention(runtime, *, children=1):
    root = root_goal(runtime, children=children)
    worker = runtime.service.claim("worker")
    runtime.service.submit(worker["run_id"], worker["fencing_token"], "worker",
        {"status": "failed", "summary": "连接超时，等待下一步决定。", "agent_authored": False})
    return root, worker, runtime.sessions.claim("leader")


async def tool(runtime, claim, name, arguments=None):
    return await runtime.leader.invoke("tasks", (claim["run_id"], claim["fencing_token"], "leader"), {
        "operation": name, "call_id": name, "arguments": arguments or {}})


async def authorize_write(runtime, claim):
    return await runtime.leader.invoke("authorize", (claim["run_id"], claim["fencing_token"], "leader"), {
        "action": "github.issue.write", "effect": "write", "resources": ["repo:github.com/org/repo"]})


async def test_attention_rework_yields_then_worker_and_final_leader_finish(runtime):
    root, failed, attention = failed_attention(runtime)
    retry = await tool(runtime, attention, "tasks.spawn", {
        "goal": "核对已有操作后重试检查", "delegation_key": "retry", "rework_of_task_id": failed["task_id"]})
    await tool(runtime, attention, "tasks.wait", {"children": [retry["task_id"]]})
    leader_complete(runtime, attention, "我已经调整处理方式，会继续跟进。")
    completed = worker_complete(runtime)
    assert completed["task_id"] == retry["task_id"]
    assert runtime.service.claim("duplicate-retry") is None
    final = runtime.sessions.claim("leader")
    assert final["purpose"] == "finalize"
    leader_complete(runtime, final, "已经处理好了。")
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == root["task_id"])).scalar_one() == "SUCCEEDED"
        assert conn.execute(select(tasks.c.rework_of_task_id).where(tasks.c.id == retry["task_id"])).scalar_one() == failed["task_id"]


async def test_takeover_enables_bound_write_and_ledger_rejects_wrong_bindings(runtime):
    root, worker, attention = failed_attention(runtime)
    with pytest.raises(PermissionError, match="takeover"):
        await authorize_write(runtime, attention)
    assert (await tool(runtime, attention, "tasks.take_over"))["takeover"]
    assert (await authorize_write(runtime, attention))["allowed"]
    identity = (attention["run_id"], attention["fencing_token"], "leader")
    payload = {"operation": "prepare", "operation_id": "operation", "arguments_hash": "hash",
        "arguments": {"body": "处理完成"}, "action": {"name": "github.issue.write", "effect": "write",
            "resources": ["repo:github.com/org/repo"]}}
    assert (await runtime.leader.invoke("ledger", identity, payload))["state"] == "PREPARED"
    assert (await runtime.leader.invoke("ledger", identity, payload))["state"] == "PREPARED"
    assert await runtime.leader.invoke("ledger", identity, {"operation": "transition", "operation_id": "operation",
        "expected": "PREPARED", "state": "DISPATCHED", "result": {}}) == {"changed": True}
    assert await runtime.leader.invoke("ledger", identity, {"operation": "transition", "operation_id": "operation",
        "expected": "DISPATCHED", "state": "VERIFIED", "result": {"url": "https://github.com/org/repo/issues/1"}}) == {"changed": True}
    with pytest.raises(PermissionError):
        await runtime.leader.invoke("ledger", (attention["run_id"], attention["fencing_token"], "other"), payload)
    with pytest.raises((PermissionError, LookupError)):
        await runtime.leader.invoke("ledger", (worker["run_id"], worker["fencing_token"], "worker"), payload)
    with runtime.db.transaction() as conn:
        conn.execute(update(operations).where(operations.c.id == "operation").values(task_id="another-root"))
    with pytest.raises(PermissionError, match="another binding"):
        await runtime.leader.invoke("ledger", identity, {"operation": "transition", "operation_id": "operation",
            "expected": "DISPATCHED", "state": "VERIFIED", "result": {}})
    with runtime.db.engine.connect() as conn:
        assert json.loads(conn.execute(select(runs.c.request_json).where(runs.c.id == attention["run_id"])).scalar_one())["takeover"]
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == root["task_id"])).scalar_one() == "RUNNING"


@pytest.mark.parametrize("unsafe", ["active", "unknown"])
async def test_takeover_requires_stopped_reconciled_work(runtime, unsafe):
    _, _, attention = failed_attention(runtime, children=2 if unsafe == "active" else 1)
    if unsafe == "unknown":
        with runtime.db.transaction() as conn:
            conn.execute(update(tasks).where(tasks.c.kind != "goal").values(state="NEEDS_RECONCILIATION"))
    with pytest.raises(PermissionError, match="stopped work"):
        await tool(runtime, attention, "tasks.take_over")


@pytest.mark.parametrize("early", [False, True])
async def test_root_question_answer_continues_leader_including_early_revision(runtime, early):
    root, _, attention = failed_attention(runtime)
    identity = (attention["run_id"], attention["fencing_token"], "leader")
    await runtime.leader.invoke("ask", identity, {"question_id": "root-question", "kind": "input",
        "text": "是否继续尝试？", "checkpoint": "等待用户决定"})
    event = runtime.event.model_copy(update={"event_id": "user-answer", "segments": (TextSegment(text="继续"),)})
    def answer():
        return runtime.service.add_input(root["public_id"], event, runtime.access, "继续", question_id="root-question")
    if early:
        answer()
    leader_complete(runtime, attention, "我等你的决定。")
    if not early:
        with runtime.db.engine.connect() as conn:
            assert conn.execute(select(tasks.c.state).where(tasks.c.id == root["task_id"])).scalar_one() == "WAITING_INPUT"
        answer()
    followup = runtime.sessions.claim("leader")
    assert followup["purpose"] == "attention"
    assert [item["text"] for item in followup["continuation"]["inputs"]] == ["继续"]
    await runtime.leader.invoke("session-receipt", (followup["run_id"], followup["fencing_token"], "leader"), {
        "kind": "input_consumed", "session_id": followup["session_id"], "run_id": followup["run_id"],
        "session_revision": "answer-leaf", "input_ids": followup["context"]["source_input_ids"]})
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(waits.c.state).where(waits.c.id == "root-question")).scalar_one() == "CONSUMED"
        assert all(conn.execute(select(inputs.c.consumed_at)).scalars())
    assert runtime.service.claim("root-must-stay-leader") is None


async def test_native_decision_tools_and_ordinary_user_authority(runtime):
    assert {"tasks.take_over", "tasks.spawn", "tasks.wait"} <= native_tools("leader", "attention").keys()
    assert "tasks.take_over" not in native_tools("leader", "user")
    runtime.sessions.enqueue(runtime.event, runtime.access)
    user = runtime.sessions.claim("leader")
    with pytest.raises(PermissionError, match="bound result"):
        await tool(runtime, user, "tasks.take_over")
    assert (await authorize_write(runtime, user))["allowed"]


async def test_worker_unlimited_time_does_not_extend_leader_decision_deadline(runtime):
    root_goal(runtime)
    expire_task_budget(runtime)
    worker = runtime.service.claim("worker")
    assert worker["active_seconds_remaining"] is None
    runtime.service.submit(worker["run_id"], worker["fencing_token"], "worker",
        {"status": "failed", "summary": "需要接手", "agent_authored": False})
    attention = runtime.sessions.claim("leader")
    await tool(runtime, attention, "tasks.take_over")
    assert (await authorize_write(runtime, attention))["allowed"]
    with runtime.db.transaction() as conn:
        run = conn.execute(select(runs).where(runs.c.id == attention["run_id"])).mappings().one()
        persisted = json.loads(run["request_json"])
        persisted["deadline"] = (runtime.clock.now() - timedelta(seconds=1)).isoformat()
        conn.execute(update(runs).where(runs.c.id == attention["run_id"]).values(request_json=json.dumps(persisted)))
    with pytest.raises(PermissionError, match="deadline expired"):
        await authorize_write(runtime, attention)


def test_pending_escalation_survives_new_user_revision(runtime):
    root, _, _ = failed_attention(runtime, children=2)
    # Return this unstarted decision claim to the inbox before the new user revision.
    with runtime.db.transaction() as conn:
        from qqbot.storage.agent_schema import leader_inbox
        conn.execute(update(runs).where(runs.c.role == "leader").values(slot_reserved=0))
        conn.execute(update(leader_inbox).values(claimed_attempt_id=None))
        current = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
        event = runtime.event.model_copy(update={"event_id": "revision", "segments": (TextSegment(text="优先检查安全"),)})
        runtime.service.add_graph_input(conn, current, event, runtime.access, event.text)
    attention = runtime.sessions.claim("leader")
    assert attention["purpose"] == "attention" and attention["basis_goal_revision"] == 1


async def test_unknown_external_operation_blocks_takeover(runtime):
    _, failed, attention = failed_attention(runtime)
    with runtime.db.transaction() as conn:
        conn.execute(operations.insert().values(id="uncertain-write", task_id=failed["task_id"],
            run_id=failed["run_id"], fence=failed["fencing_token"], arguments_hash="hash",
            action_json=json.dumps({"name": "github.issue.write", "resources": [], "effect": "write"}),
            arguments_json="{}", result_json="{}", state="UNKNOWN", updated_at=runtime.clock.now().isoformat()))
    with pytest.raises(PermissionError, match="reconciled operations"):
        await tool(runtime, attention, "tasks.take_over")


@pytest.mark.parametrize("state", ["INTENT", "STARTING", "RUNNING", "UNKNOWN"])
async def test_takeover_waits_for_job_even_before_cancellation_is_registered(runtime, state):
    from qqbot.storage.job_schema import jobs
    root, worker, attention = failed_attention(runtime)
    now = runtime.clock.now().isoformat()
    with runtime.db.transaction() as conn:
        conn.execute(jobs.insert().values(id="old-job", job_key="old-job", task_id=worker["task_id"],
            root_task_id=root["task_id"], owner_id="owner", workspace="/tmp/workspace", world_id="local",
            arguments_hash="hash", request_json="{}", state=state, unit="old-job.service", description="job",
            deadline=now, created_at=now, updated_at=now, log_ref="old-job.log"))
    with pytest.raises(PermissionError, match="stopped work"):
        await tool(runtime, attention, "tasks.take_over")
    with runtime.db.transaction() as conn:
        conn.execute(update(jobs).where(jobs.c.id == "old-job").values(state="EXITED", stopped_at=now))
    assert (await tool(runtime, attention, "tasks.take_over"))["takeover"]
