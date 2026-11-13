import json
from dataclasses import replace
from datetime import timedelta

import pytest
from sqlalchemy import select, update
from test_agent_runtime import root_goal
from test_agent_runtime import runtime as agent_runtime_fixture

from qqbot.adapters.execution.jobs import UnitReceipt
from qqbot.adapters.execution.runtime import JobRuntime
from qqbot.contracts.execution import ExecutionCapabilities
from qqbot.storage.agent_schema import agent_events, leader_inbox
from qqbot.storage.task_schema import notifications, tasks, waits

runtime = agent_runtime_fixture


def proof(claim, *, yielded=False):
    return {"kind": "run_yielded" if yielded else "turn_finished", "run_id": claim["run_id"],
        "session_id": claim["session_id"], "session_revision": "leaf",
        "input_ids": claim["context"]["source_input_ids"], "inflight": 0, "tools_paired": True}


def wait_worker(runtime, claim, kind):
    question = {"question_id": "wait-1", "kind": kind, "text": "等待输入", "checkpoint": "持久检查点"}
    if kind == "external":
        question["condition"] = {"capability": "web.fetch", "arguments": {"url": "https://example.com"},
            "path": "ready", "equals": True, "interval_seconds": 15}
    runtime.service.ask(claim["run_id"], claim["fencing_token"], "worker", question)
    return runtime.service.submit(claim["run_id"], claim["fencing_token"], "worker",
        {"status": "waiting_input" if kind == "input" else "waiting_external", "summary": "等待输入",
         "evidence": [{"url": "https://example.com", "retrieved_at": runtime.clock.now().isoformat(), "sha256": "a" * 64}]},
        receipt=proof(claim, yielded=True))


def consume_inbox(runtime):
    claim = runtime.sessions.claim("leader")
    if claim:
        runtime.leader.complete(claim["run_id"], claim["fencing_token"], "leader",
            {"status": "completed", "summary": "请补充输入。"}, stopped=True, quiescent=False, receipt=proof(claim))
    return claim


def finalize_blocked(runtime, purpose="finalize"):
    claim = runtime.sessions.claim("leader")
    assert claim["purpose"] == purpose
    runtime.leader.complete(claim["run_id"], claim["fencing_token"], "leader",
        {"status": "blocked", "summary": "等待已结束，本次任务未完成。"}, stopped=True, quiescent=False, receipt=proof(claim))


@pytest.mark.parametrize("kind", ["input", "external"])
def test_wait_timeout_persists_result_and_wakes_root_once(runtime, kind):
    root = root_goal(runtime)
    child = runtime.service.claim("worker")
    wait_worker(runtime, child, kind)
    if kind == "input":
        assert consume_inbox(runtime)["purpose"] == "question"
    with runtime.db.transaction() as conn:
        conn.execute(update(waits).where(waits.c.id == "wait-1").values(
            expires_at=(runtime.clock.now() + timedelta(seconds=1)).isoformat()))
    runtime.clock.advance(2)
    runtime.service.recover()
    runtime.service.recover()
    with runtime.db.engine.connect() as conn:
        row = conn.execute(select(tasks).where(tasks.c.id == child["task_id"])).mappings().one()
        result = json.loads(row["result_json"])
        assert row["state"] == "BLOCKED"
        assert result["status"] == "blocked" and result["agent_authored"] is False
        assert result["evidence"][0]["sha256"] == "a" * 64
        assert len(conn.execute(select(agent_events).where(agent_events.c.id == "host-wait-expired:wait-1")).all()) == 1
        assert len(conn.execute(select(leader_inbox).where(leader_inbox.c.handled_at.is_(None))).all()) == 1
    finalize_blocked(runtime, purpose="attention")
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == root["task_id"])).scalar_one() == "BLOCKED"
        assert len(conn.execute(select(notifications).where(notifications.c.purpose == "result")).all()) == 1


def test_queued_background_worker_survives_old_deadline_and_can_start(runtime):
    root = root_goal(runtime)
    runtime.clock.advance(7201)
    runtime.service.recover()
    runtime.service.recover()
    with runtime.db.engine.connect() as conn:
        child = conn.execute(select(tasks).where(tasks.c.parent_task_id == root["task_id"])).mappings().one()
        assert child["state"] == "QUEUED" and child["result_json"] is None
        assert not conn.execute(select(leader_inbox)).first()
    started = runtime.service.claim("worker")
    assert started["task_id"] == child["id"] and started["active_seconds_remaining"] is None


def test_nested_wait_timeout_escalates_without_automatically_resuming_parent(runtime):
    root_goal(runtime)
    parent = runtime.service.claim("worker")
    spawned = runtime.service.task_operation(parent["run_id"], parent["fencing_token"], "worker", {
        "operation": "tasks.spawn", "call_id": "spawn", "arguments": {"goal": "检查分支", "delegation_key": "grandchild"}})
    runtime.service.task_operation(parent["run_id"], parent["fencing_token"], "worker", {
        "operation": "tasks.wait", "call_id": "wait", "arguments": {"children": [spawned["task_id"]]}})
    runtime.service.submit(parent["run_id"], parent["fencing_token"], "worker",
        {"status": "waiting_children", "summary": "等待子任务"}, receipt=proof(parent, yielded=True))
    child = runtime.service.claim("worker")
    assert child["task_id"] == spawned["task_id"]
    wait_worker(runtime, child, "external")
    with runtime.db.transaction() as conn:
        conn.execute(update(waits).where(waits.c.id == "wait-1").values(
            expires_at=(runtime.clock.now() + timedelta(seconds=1)).isoformat()))
    runtime.clock.advance(2)
    runtime.service.recover()
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == parent["task_id"])).scalar_one() == "WAITING_CHILDREN"
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == child["task_id"])).scalar_one() == "BLOCKED"
    assert runtime.service.claim("automatic-parent-retry") is None
    assert runtime.sessions.claim("leader")["purpose"] == "attention"


class CancelProvider:
    def __init__(self):
        self.units = {}

    async def describe(self):
        return ExecutionCapabilities(persistent_jobs=True, daemon_restart_survival=True, process_tree_cancel=True)

    async def launch(self, **request):
        self.units[request["unit"]] = UnitReceipt(True, request["unit"], request["description"], "a" * 32,
            "/user.slice/" + request["unit"], 123, "1000", "active", "running", "success", None, cgroup_empty=False)

    async def inspect(self, unit):
        return self.units[unit]

    async def cancel(self, unit, *, cgroup):
        assert cgroup == self.units[unit].cgroup
        self.units[unit] = replace(self.units[unit], active_state="inactive", sub_state="dead", cgroup_empty=True, pid=0)
        return {"unit": unit, "stopped": True}


async def test_persisted_job_stop_before_daemon_restart_still_wakes_cancelled_child_parent(runtime):
    core = runtime.leader.core
    core.settings, core.sessions = runtime.sessions.settings, runtime.sessions
    provider = CancelProvider()
    jobs = JobRuntime(core, runtime.path, provider=provider)
    root = root_goal(runtime)
    child = runtime.service.claim("worker")
    (runtime.path / "workspaces" / child["task_id"]).mkdir(parents=True)
    await jobs.invoke((child["run_id"], child["fencing_token"], "worker"), {
        "operation": "start", "arguments": {"job_key": "running", "argv": ["/usr/bin/true"]}})
    wait_worker(runtime, child, "input")
    consume_inbox(runtime)
    with runtime.db.transaction() as conn:
        row = conn.execute(select(tasks).where(tasks.c.id == child["task_id"])).mappings().one()
        runtime.service.graph.cancel_tree(conn, row)
    # Persist the unit stop, then reconstruct the daemon before its Task transition ran.
    await jobs.jobs.cancel_task(child["task_id"], core.settings.owner.id)
    restarted = JobRuntime(core, runtime.path, provider=provider)
    await restarted.maintain()
    await restarted.maintain()
    with runtime.db.engine.connect() as conn:
        row = conn.execute(select(tasks).where(tasks.c.id == child["task_id"])).mappings().one()
        assert row["state"] == "CANCELLED"
        result = json.loads(row["result_json"])
        assert result["status"] == "failed" and result["agent_authored"] is False
        assert result["evidence"][0]["sha256"] == "a" * 64
        assert len(conn.execute(select(leader_inbox).where(leader_inbox.c.handled_at.is_(None))).all()) == 1
    finalize_blocked(runtime)
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == root["task_id"])).scalar_one() == "BLOCKED"
