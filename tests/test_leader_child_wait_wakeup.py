"""Leader child waits resume from the dependency boundary, including completed children."""
import pytest
from sqlalchemy import func, select
from test_agent_runtime import leader_complete, root_goal, worker_complete
from test_agent_runtime import runtime as agent_runtime
from test_leader_decisions import failed_attention, tool
from test_leader_dispatch_recovery import native_pending_submit

from elephant.adapters.pi.sessions import restore_tool_results, unfinished_tools
from elephant.domain import TextSegment
from elephant.storage.agent_schema import agent_events, leader_inbox
from elephant.storage.task_schema import runs, tasks, waits

runtime = agent_runtime


@pytest.mark.parametrize("cancel_child", [False, True])
def test_worker_revision_preserves_child_wait_and_enters_the_resumed_claim(runtime, cancel_child):
    root_goal(runtime)
    parent = runtime.service.claim("parent")
    identity = (parent["run_id"], parent["fencing_token"], "parent")
    child = runtime.service.task_operation(*identity, {"operation": "tasks.spawn", "call_id": "spawn-check",
        "arguments": {"goal": "核对事实", "delegation_key": "facts"}})
    runtime.service.task_operation(*identity, {"operation": "tasks.wait", "call_id": "await-check",
        "arguments": {"children": [child["task_id"]]}})
    with runtime.db.engine.connect() as conn:
        ref = conn.execute(select(tasks.c.public_id).where(tasks.c.id == parent["task_id"])).scalar_one()
    source = runtime.event.model_copy(update={"event_id": "parent-revision", "segments": (TextSegment(text="按周汇总"),)})
    revision = runtime.service.add_input(ref, source, runtime.access, source.text)
    assert runtime.service.submit(*identity, {"status": "waiting_children", "summary": "等待事实核对"}, receipt={
        "kind": "run_yielded", "run_id": parent["run_id"], "session_id": parent["session_id"],
        "session_revision": "parent-yield", "input_ids": parent["context"]["source_input_ids"],
        "inflight": 0, "tools_paired": True})["state"] == "WAITING_CHILDREN"
    if cancel_child:
        assert runtime.service.cancel(child["public_id"], runtime.access)["state"] == "CANCELLED"
    else:
        assert worker_complete(runtime)["task_id"] == child["task_id"]
    resumed = runtime.service.claim("parent")
    assert resumed["task_id"] == parent["task_id"]
    assert resumed["continuation"]["inputs"] == [{"id": revision["input_id"], "text": source.text}]


def test_cancelled_worker_closes_its_question_and_wakes_parent(runtime):
    root_goal(runtime)
    parent = runtime.service.claim("parent")
    parent_identity = (parent["run_id"], parent["fencing_token"], "parent")
    child = runtime.service.task_operation(*parent_identity, {"operation": "tasks.spawn", "call_id": "spawn-check",
        "arguments": {"goal": "核对事实", "delegation_key": "facts"}})
    runtime.service.task_operation(*parent_identity, {"operation": "tasks.wait", "call_id": "await-check",
        "arguments": {"children": [child["task_id"]]}})
    assert runtime.service.submit(*parent_identity, {"status": "waiting_children", "summary": "等待核对"}, receipt={
        "kind": "run_yielded", "run_id": parent["run_id"], "session_id": parent["session_id"],
        "session_revision": "parent-yield", "input_ids": parent["context"]["source_input_ids"],
        "inflight": 0, "tools_paired": True})["state"] == "WAITING_CHILDREN"
    worker = runtime.service.claim("child")
    identity = (worker["run_id"], worker["fencing_token"], "child")
    runtime.service.ask(*identity, {"question_id": "pending-question", "text": "确认范围？", "checkpoint": "等待确认"})
    assert runtime.service.cancel(child["public_id"], runtime.access)["state"] == "CANCEL_REQUESTED"
    result = {"status": "failed", "summary": "已停止", "agent_authored": False}
    assert runtime.service.submit(*identity, result)["state"] == "CANCELLED"
    assert runtime.service.submit(*identity, result)["state"] == "CANCELLED"
    assert runtime.service.claim("parent")["task_id"] == parent["task_id"]
    with runtime.db.engine.connect() as conn:
        assert conn.scalar(select(waits.c.state).where(waits.c.id == "pending-question")) == "CANCELLED"
        assert conn.scalar(select(func.count()).select_from(agent_events).where(
            agent_events.c.task_id == parent["task_id"], agent_events.c.kind == "children_ready")) == 1


def finish_wait(runtime, claim, ending, arguments, accepted):
    if ending == "completed":
        leader_complete(runtime, claim, "等待已经保存的检查结果。")
    elif ending == "protocol_failure":
        runtime.leader.complete(claim["run_id"], claim["fencing_token"], "leader", {
            "status": "failed", "agent_authored": False, "summary": "等待已保存，模型结算纠正失败。",
            "limitations": ["model_tool_protocol_recovery_failed"]}, stopped=True, quiescent=False)
    else:
        path = native_pending_submit(runtime, claim, arguments, tool_name="tasks_wait")
        pending = unfinished_tools(path)
        restore_tool_results(path, [{"call": pending[0], "result": accepted}],
            pi_executable=runtime.sessions.settings.orchestration.pi_executable)
        runtime.clock.advance(31)
        assert runtime.leader.recover_attempt(claim["run_id"])["state"] == "RECOVERED"
        assert runtime.leader.recover_attempt(claim["run_id"])["state"] == "RECOVERED"


@pytest.mark.parametrize("ending", ["completed", "protocol_failure", "recovered"])
async def test_finalizer_waiting_on_already_completed_child_resumes_once(runtime, ending):
    root = root_goal(runtime)
    worker = worker_complete(runtime)
    finalizer = runtime.sessions.claim("leader")
    assert finalizer["purpose"] == "finalize"
    arguments = {"children": [worker["task_id"]]}
    accepted = await tool(runtime, finalizer, "tasks.wait", arguments)
    finish_wait(runtime, finalizer, ending, arguments, accepted)
    with runtime.db.transaction() as conn:
        queued = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
        assert not runtime.service.graph.wake_parent(conn, {**dict(queued), "state": "WAITING_CHILDREN"})
    assert runtime.service.claim("root-is-not-a-worker") is None
    resumed = runtime.sessions.claim("leader")
    assert resumed is not None
    assert resumed["purpose"] == "finalize" and resumed["target_root_task_id"] == root["task_id"]
    assert runtime.sessions.claim("duplicate-writer") is None
    leader_complete(runtime, resumed, "已经核验完成。")
    assert runtime.sessions.claim("duplicate-wakeup") is None
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == root["task_id"])).scalar_one() == "SUCCEEDED"
        assert conn.execute(select(func.count()).select_from(agent_events).where(
            agent_events.c.task_id == root["task_id"], agent_events.c.kind == "children_ready")).scalar_one() == 1
        assert all(conn.execute(select(leader_inbox.c.handled_at)).scalars())


@pytest.mark.parametrize("early", [False, True])
@pytest.mark.parametrize("ending", ["completed", "protocol_failure", "recovered"])
async def test_root_rework_wait_wakes_for_children_finished_before_or_after_yield(runtime, early, ending):
    root, failed, attention = failed_attention(runtime)
    retry = await tool(runtime, attention, "tasks.spawn", {
        "goal": "核对后继续", "delegation_key": "retry", "rework_of_task_id": failed["task_id"]})
    arguments = {"children": [retry["task_id"]]}
    accepted = await tool(runtime, attention, "tasks.wait", arguments)
    if early:
        assert worker_complete(runtime)["task_id"] == retry["task_id"]
        assert runtime.sessions.claim("leader-still-running") is None
    finish_wait(runtime, attention, ending, arguments, accepted)
    if not early:
        assert runtime.sessions.claim("premature-resume") is None
        with runtime.db.engine.connect() as conn:
            assert not conn.execute(select(agent_events).where(agent_events.c.kind == "children_ready")).first()
        assert worker_complete(runtime)["task_id"] == retry["task_id"]
    assert runtime.service.claim("root-is-not-a-worker") is None
    resumed = runtime.sessions.claim("leader")
    assert resumed["purpose"] == "finalize" and resumed["target_root_task_id"] == root["task_id"]
    leader_complete(runtime, resumed, "已经核验完成。")
    assert runtime.sessions.claim("duplicate-wakeup") is None
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == root["task_id"])).scalar_one() == "SUCCEEDED"
        assert conn.execute(select(func.count()).select_from(agent_events).where(
            agent_events.c.task_id == root["task_id"], agent_events.c.kind == "children_ready")).scalar_one() == 1


@pytest.mark.parametrize("early", [False, True])
@pytest.mark.parametrize("failed", [False, True])
async def test_root_wait_any_revokes_remaining_work_and_waits_for_stop(runtime, early, failed):
    root, _, attention = failed_attention(runtime)
    children = [await tool(runtime, attention, "tasks.spawn", {
        "goal": "独立检查", "delegation_key": key}) for key in ("first", "remaining")]
    arguments = {"children": [child["task_id"] for child in children], "predicate": "any"}
    accepted = await tool(runtime, attention, "tasks.wait", arguments)
    first = runtime.service.claim("first")
    remaining = runtime.service.claim("remaining")
    def finish_first():
        runtime.service.submit(first["run_id"], first["fencing_token"], "first", {
            "status": "failed" if failed else "completed", "agent_authored": False, "summary": "检查已结束。"})
    if early:
        finish_first()
    finish_wait(runtime, attention, "completed", arguments, accepted)
    if not early:
        finish_first()
    assert runtime.service.claim("root-is-not-a-worker") is None
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == remaining["task_id"])).scalar_one() == "CANCEL_REQUESTED"
        assert conn.execute(select(runs.c.slot_reserved).where(runs.c.id == remaining["run_id"])).scalar_one() == 1
        current = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
        assert not runtime.service.graph.finalize_ready(conn, current)
    runtime.service.submit(remaining["run_id"], remaining["fencing_token"], "remaining", {
        "status": "failed", "agent_authored": False, "summary": "已经确认停止。"})
    resumed = runtime.sessions.claim("leader")
    assert resumed["target_root_task_id"] == root["task_id"]
    leader_complete(runtime, resumed, "已核对结束。")
    assert runtime.sessions.claim("duplicate-wakeup") is None
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == root["task_id"])).scalar_one() == "SUCCEEDED"
        assert conn.execute(select(func.count()).select_from(agent_events).where(
            agent_events.c.task_id == root["task_id"], agent_events.c.kind == "children_ready")).scalar_one() == 1


def test_cancel_during_child_wait_still_wakes_root(runtime):
    root = root_goal(runtime)
    parent = runtime.service.claim("parent")
    ident = parent["run_id"], parent["fencing_token"], "parent"
    child = runtime.service.task_operation(*ident, {"operation": "tasks.spawn", "call_id": "spawn-check",
        "arguments": {"goal": "核对事实", "delegation_key": "facts"}})
    runtime.service.task_operation(*ident, {"operation": "tasks.wait", "call_id": "await-check",
        "arguments": {"children": [child["task_id"]]}})
    with runtime.db.engine.connect() as conn:
        ref = conn.scalar(select(tasks.c.public_id).where(tasks.c.id == parent["task_id"]))
    assert runtime.service.cancel(ref, runtime.access)["state"] == "CANCEL_REQUESTED"
    assert runtime.service.submit(*ident, {"status": "failed", "agent_authored": False,
        "summary": "已停止"})["state"] == "CANCELLED"
    with runtime.db.engine.connect() as conn:
        other = list(conn.execute(select(tasks.c.state).where(tasks.c.root_task_id == root["task_id"])).scalars())
        assert other.count("CANCELLED") == 2
    resumed = runtime.sessions.claim("leader")
    assert resumed and resumed["purpose"] == "finalize"
