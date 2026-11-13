"""Parent waits require paired receipts and verified stops before releasing authority."""
import json
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from test_agent_runtime import root_goal
from test_agent_runtime import runtime as agent_runtime

from qqbot.storage.agent_schema import agent_events
from qqbot.storage.task_schema import processes, runs, tasks

runtime = agent_runtime


def proof(claim, kind="run_yielded", **changes):
    return {"kind": kind, "run_id": claim["run_id"], "session_id": claim["session_id"],
        "session_revision": "paired-native-leaf", "input_ids": claim["context"]["source_input_ids"],
        "inflight": 0, "tools_paired": True, **changes}


def delegate_and_wait(runtime, parent, worker):
    identity = parent["run_id"], parent["fencing_token"], worker
    spawned = runtime.service.task_operation(*identity, {"operation": "tasks.spawn", "call_id": "spawn",
        "arguments": {"goal": "检查子任务", "delegation_key": "child"}})
    response = runtime.service.task_operation(*identity, {"operation": "tasks.wait", "call_id": "wait",
        "arguments": {"children": [spawned["task_id"]]}})
    assert response == {"accepted": True, "waiting": True, "wait_revision": 1}
    return spawned


def submit(runtime, claim, worker, result, **kwargs):
    return runtime.service.submit(claim["run_id"], claim["fencing_token"], worker, result, **kwargs)


def test_parent_releases_authority_only_after_paired_stopped_boundary_and_wakes_once(runtime):
    root_goal(runtime, children=2)
    parents = [(runtime.service.claim(name), name) for name in ("parent-a", "parent-b")]
    children = {}
    for parent, worker in parents:
        children[parent["task_id"]] = delegate_and_wait(runtime, parent, worker)["task_id"]
        process_id = str(uuid4())
        runtime.service.bind_process(parent["run_id"], parent["fencing_token"], worker,
            {"process_id": process_id, "session_id": parent["session_id"], "epoch": 1, "generation": 1,
             "scope_key": parent["session_id"], "session_file": str(runtime.path / (parent["session_id"] + ".jsonl")),
             "identity": {}})
        parent["process_id"] = process_id
    waiting = {"status": "waiting_children", "summary": "等待已持久化的子任务"}
    for parent, worker in parents:
        for boundary in (proof(parent, tools_paired=False), proof(parent, inflight=1)):
            with pytest.raises(PermissionError, match="Harness still has activity"):
                submit(runtime, parent, worker, waiting, receipt=boundary)
        with pytest.raises(PermissionError, match="stop has not been verified"):
            submit(runtime, parent, worker, waiting, receipt=proof(parent))
        with runtime.db.engine.connect() as conn:
            run = conn.execute(select(runs).where(runs.c.id == parent["run_id"])).mappings().one()
            task = conn.execute(select(tasks).where(tasks.c.id == parent["task_id"])).mappings().one()
            assert run["slot_reserved"] == 1 and run["receipt_json"] is None
            assert json.loads(task["wait_json"])["yielded"] is False
    for parent, worker in parents:
        runtime.service.process_stopped(worker, {"process_id": parent["process_id"], "epoch": 1,
                                                "generation": 1, "stopped": True})
        assert submit(runtime, parent, worker, waiting, receipt=proof(parent))["state"] == "WAITING_CHILDREN"
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(runs).where(runs.c.slot_reserved == 1)).scalar_one() == 0
        assert set(conn.execute(select(processes.c.state)).scalars()) == {"STOPPED"}
        for parent, _ in parents:
            run = conn.execute(select(runs).where(runs.c.id == parent["run_id"])).mappings().one()
            assert json.loads(run["result_json"])["status"] == "waiting_children"
            assert json.loads(run["receipt_json"])["tools_paired"] is True
    child_runs = [(runtime.service.claim(name), name) for name in ("child-a", "child-b")]
    assert {claim["task_id"] for claim, _ in child_runs} == set(children.values())
    completed = {"status": "completed", "summary": "已检查"}
    for claim, worker in child_runs:
        submit(runtime, claim, worker, completed, receipt=proof(claim, "turn_finished"))
        submit(runtime, claim, worker, completed, receipt=proof(claim, "turn_finished"))
    with runtime.db.engine.connect() as conn:
        for parent, _ in parents:
            wakeups = conn.execute(select(agent_events).where(agent_events.c.task_id == parent["task_id"],
                                                             agent_events.c.kind == "children_ready")).mappings().all()
            assert len(wakeups) == 1
            assert json.loads(wakeups[0]["payload_json"])["wait_revision"] == 1
    resumed = [runtime.service.claim(name) for name in ("resumed-a", "resumed-b")]
    assert {claim["task_id"] for claim in resumed} == set(children)
    assert {claim["session_id"] for claim in resumed} == {claim["session_id"] for claim, _ in parents}
    assert runtime.service.claim("duplicate-resume") is None


def test_child_completed_before_parent_yield_wakes_only_at_persisted_boundary(runtime):
    root_goal(runtime)
    parent = runtime.service.claim("parent")
    child = delegate_and_wait(runtime, parent, "parent")
    claim = runtime.service.claim("child")
    assert claim["task_id"] == child["task_id"]
    completed = {"status": "completed", "summary": "已检查"}
    submit(runtime, claim, "child", completed, receipt=proof(claim, "turn_finished"))
    with runtime.db.engine.connect() as conn:
        assert not conn.execute(select(agent_events).where(agent_events.c.kind == "children_ready")).first()
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == parent["task_id"])).scalar_one() == "RUNNING"
    waiting = {"status": "waiting_children", "summary": "等待子任务"}
    submit(runtime, parent, "parent", waiting, receipt=proof(parent))
    submit(runtime, claim, "child", completed, receipt=proof(claim, "turn_finished"))
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(agent_events).where(
            agent_events.c.kind == "children_ready")).scalar_one() == 1
    assert runtime.service.claim("resumed")["task_id"] == parent["task_id"]


def test_wait_any_revokes_remaining_child_and_root_waits_for_its_stop(runtime):
    root = root_goal(runtime)
    parent = runtime.service.claim("parent")
    identity = parent["run_id"], parent["fencing_token"], "parent"
    children = [runtime.service.task_operation(*identity, {"operation": "tasks.spawn", "call_id": key,
        "arguments": {"goal": "独立检查", "delegation_key": key}}) for key in ("first", "remaining")]
    runtime.service.task_operation(*identity, {"operation": "tasks.wait", "call_id": "any",
        "arguments": {"children": [item["task_id"] for item in children], "predicate": "any"}})
    submit(runtime, parent, "parent", {"status": "waiting_children", "summary": "等待任一子任务"}, receipt=proof(parent))
    first, remaining = runtime.service.claim("first"), runtime.service.claim("remaining")
    process_id = str(uuid4())
    runtime.service.bind_process(remaining["run_id"], remaining["fencing_token"], "remaining", {
        "process_id": process_id, "session_id": remaining["session_id"], "epoch": 1, "generation": 1,
        "scope_key": remaining["session_id"], "session_file": str(runtime.path / "remaining.jsonl"), "identity": {}})
    completed = {"status": "completed", "summary": "核验完成"}
    submit(runtime, first, "first", completed, receipt=proof(first, "turn_finished"))
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == remaining["task_id"])).scalar_one() == "CANCEL_REQUESTED"
        assert conn.execute(select(runs.c.slot_reserved).where(runs.c.id == remaining["run_id"])).scalar_one() == 1
    with pytest.raises(PermissionError, match="no longer authorized"):
        runtime.service.authorize(remaining["run_id"], remaining["fencing_token"], "remaining",
                                  {"action": "workspace.write", "effect": "write"})
    resumed = runtime.service.claim("parent-resumed")
    assert resumed["task_id"] == parent["task_id"] and resumed["session_id"] == parent["session_id"]
    submit(runtime, resumed, "parent-resumed", completed, receipt=proof(resumed, "turn_finished"))
    assert runtime.sessions.claim("premature-finalizer") is None
    runtime.service.process_stopped("remaining", {"process_id": process_id, "epoch": 1, "generation": 1, "stopped": True})
    stopped = {"status": "failed", "summary": "已确认取消"}
    submit(runtime, remaining, "remaining", stopped, receipt=proof(remaining, "turn_finished"))
    submit(runtime, remaining, "remaining", stopped, receipt=proof(remaining, "turn_finished"))
    submit(runtime, first, "first", completed, receipt=proof(first, "turn_finished"))
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == remaining["task_id"])).scalar_one() == "CANCELLED"
        assert conn.execute(select(func.count()).select_from(agent_events).where(agent_events.c.task_id == parent["task_id"],
            agent_events.c.kind == "children_ready")).scalar_one() == 1
        assert conn.execute(select(runs.c.slot_reserved).where(runs.c.id == remaining["run_id"])).scalar_one() == 0
    finalizer = runtime.sessions.claim("leader")
    assert finalizer["purpose"] == "finalize" and finalizer["target_root_task_id"] == root["task_id"]
