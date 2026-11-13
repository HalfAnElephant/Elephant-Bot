"""User revisions cross the Leader/Worker boundary once and keep chat turns independent."""
from sqlalchemy import func, select
from test_agent_runtime import leader_complete, root_goal
from test_agent_runtime import runtime as agent_runtime

from qqbot.domain import TextSegment
from qqbot.storage.agent_schema import dialogue_inputs
from qqbot.storage.task_schema import inputs, runs, tasks

runtime = agent_runtime

def send_followup(runtime, claim, root, call_id):
    with runtime.db.transaction() as conn:
        run = conn.execute(select(runs).where(runs.c.id == claim["run_id"])).mappings().one()
        return runtime.leader.task_tool(conn, run, claim, {
            "operation": "tasks.send_input", "call_id": call_id,
            "arguments": {"task_ref": root["public_id"], "kind": "followup", "text": "模型改写的内容"}})


def test_followups_reach_running_worker_once_and_release_chat_turn(runtime):
    root = root_goal(runtime)
    worker = runtime.service.claim("worker")
    for index, text in enumerate(("先检查安全问题", "再补充性能问题")):
        event = runtime.event.model_copy(update={"event_id": f"followup-{index}",
            "segments": (TextSegment(text=text),)})
        runtime.sessions.enqueue(event, runtime.access)
        claim = runtime.sessions.claim("leader")
        assert claim["purpose"] == "user" and claim["target_root_task_id"] is None
        assert claim["context"]["active_tasks"][0]["task_id"] == root["task_id"]
        first = send_followup(runtime, claim, root, "first-call")
        repeated = send_followup(runtime, claim, root, "retry-call")
        assert repeated["input_id"] == first["input_id"]
        leader_complete(runtime, claim, "收到，我会把这条要求一并处理。")
        pending = runtime.service.take_inputs(worker["run_id"], worker["fencing_token"], "worker")
        assert [item["text"] for item in pending] == [text]
        runtime.service.session_receipt(worker["run_id"], worker["fencing_token"], "worker", {
            "kind": "input_consumed", "session_id": worker["context"]["session_id"],
            "run_id": worker["run_id"], "session_revision": f"leaf-{index}",
            "input_ids": [item["id"] for item in pending]})
        assert runtime.service.take_inputs(worker["run_id"], worker["fencing_token"], "worker") == []
        with runtime.db.engine.connect() as conn:
            root_row = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
            assert root_row["goal_revision"] == index + 1
            assert all(conn.execute(select(inputs.c.consumed_at)).scalars())
            assert conn.execute(select(dialogue_inputs.c.handled_at).where(
                dialogue_inputs.c.id == event.event_id)).scalar_one()
    assert runtime.service.claim("duplicate-worker") is None
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(runs).where(runs.c.role == "worker")).scalar_one() == 1


def test_active_tasks_stay_in_selected_dialogue(runtime):
    root_goal(runtime)
    with runtime.db.transaction() as conn:
        runtime.sessions.dialogue(conn, runtime.event, new_title="其他对话")
    runtime.sessions.enqueue(runtime.event, runtime.access)
    claim = runtime.sessions.claim("leader")
    assert claim["context"]["active_tasks"] == []


def test_original_call_key_replay_keeps_preexisting_revision(runtime):
    from qqbot.domain import stable_id

    root = root_goal(runtime)
    runtime.sessions.enqueue(runtime.event, runtime.access)
    claim = runtime.sessions.claim("leader")
    with runtime.db.transaction() as conn:
        task = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
        original = runtime.service.add_graph_input(conn, task, runtime.event, runtime.access,
            runtime.event.text, source_id=stable_id(runtime.event.event_id, "original-call"))
    replay = send_followup(runtime, claim, root, "original-call")
    assert replay["input_id"] == original["input_id"]
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.goal_revision).where(tasks.c.id == root["task_id"])).scalar_one() == 1


def test_reused_submit_native_recovery_checks_its_committed_intent(runtime):
    from test_leader_dispatch_recovery import native_pending_submit

    from qqbot.adapters.pi.sessions import unfinished_tools

    runtime.sessions.enqueue(runtime.event, runtime.access)
    claim = runtime.sessions.claim("leader")
    original = {"goal": "检查项目", "children": [{"goal": "独立检查", "delegation_key": "primary"}]}
    alias = {**original, "children": [{"goal": "独立检查", "delegation_key": "rewritten-key"}]}
    with runtime.db.transaction() as conn:
        run = conn.execute(select(runs).where(runs.c.id == claim["run_id"])).mappings().one()
        first = runtime.leader.task_tool(conn, run, claim, {
            "operation": "tasks.submit", "call_id": "first-call", "arguments": original})
        reused = runtime.leader.task_tool(conn, run, claim, {
            "operation": "tasks.submit", "call_id": "native-submit-call", "arguments": alias})
        assert reused["task_id"] == first["task_id"] and reused["reused"]
    path = native_pending_submit(runtime, claim, alias)
    runtime.clock.advance(31)
    assert runtime.leader.recover_attempt(claim["run_id"])["state"] == "RECOVERED"
    assert unfinished_tools(path) == ()
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(tasks).where(tasks.c.kind == "goal")).scalar_one() == 1
