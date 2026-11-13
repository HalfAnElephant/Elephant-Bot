"""Background attempts keep running and escalate unfinished work to their Leader."""
import json
from datetime import timedelta

from sqlalchemy import func, select, update
from test_agent_runtime import root_goal
from test_agent_runtime import runtime as agent_runtime

from qqbot.storage.agent_schema import leader_inbox
from qqbot.storage.task_schema import runs, tasks, waits

runtime = agent_runtime

def expire_task_budget(runtime):
    with runtime.db.transaction() as conn:
        conn.execute(update(tasks).values(deadline=(runtime.clock.now() - timedelta(days=1)).isoformat(),
            budget_json=json.dumps({"active_seconds": 999999})))


def receipt(claim, kind="turn_finished"):
    return {"kind": kind, "session_id": claim["context"]["session_id"], "run_id": claim["run_id"],
        "session_revision": "leaf", "input_ids": claim["context"]["source_input_ids"],
        "inflight": 0, "tools_paired": True}


def test_background_old_deadline_and_active_budget_allow_claim_renew_and_success(runtime):
    root_goal(runtime)
    expire_task_budget(runtime)
    claim = runtime.service.claim("worker")
    assert claim["deadline"] is None and claim["active_seconds_remaining"] is None
    assert runtime.service.renew(claim["run_id"], claim["fencing_token"], "worker")["state"] == "RUNNING"
    control = runtime.service.control(claim["run_id"], claim["fencing_token"], "worker")
    assert control["state"] == "RUNNING" and control["active_seconds_remaining"] is None
    runtime.service.authorize(claim["run_id"], claim["fencing_token"], "worker", {"action": "task.execute"})
    result = runtime.service.submit(claim["run_id"], claim["fencing_token"], "worker",
        {"status": "completed", "summary": "完成检查"}, receipt=receipt(claim))
    assert result["state"] == "SUCCEEDED"
    assert runtime.sessions.claim("leader")["purpose"] == "finalize"


def test_worker_failure_escalates_before_other_children_finish_without_retry(runtime):
    root = root_goal(runtime, children=2)
    failed = runtime.service.claim("worker")
    other = runtime.service.claim("other")
    assert other
    runtime.service.submit(failed["run_id"], failed["fencing_token"], "worker",
        {"status": "failed", "summary": "远端请求超时，需要决定下一步。", "agent_authored": False})
    attention = runtime.sessions.claim("leader")
    assert attention["purpose"] == "attention"
    assert attention["target_root_task_id"] == root["task_id"]
    assert runtime.service.claim("automatic-retry") is None
    with runtime.db.engine.connect() as conn:
        run = conn.execute(select(runs).where(runs.c.id == attention["run_id"])).mappings().one()
        task = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
        assert task["state"] == "WAITING_CHILDREN"
        assert run["task_id"] == root["task_id"] and run["fencing"] == task["fencing"]


def test_lost_worker_lease_escalates_once_and_keeps_execution_reserved(runtime):
    root_goal(runtime)
    worker = runtime.service.claim("worker")
    runtime.clock.advance(31)
    runtime.service.recover()
    runtime.service.recover()
    attention = runtime.sessions.claim("leader")
    assert attention["purpose"] == "attention"
    assert runtime.service.claim("retry") is None
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(runs.c.slot_reserved).where(runs.c.id == worker["run_id"])).scalar_one() == 1
        assert conn.execute(select(func.count()).select_from(leader_inbox)).scalar_one() == 1


def test_root_call_totals_are_accounting_and_single_worker_limits_remain(runtime):
    root = root_goal(runtime)
    worker = runtime.service.claim("worker")
    with runtime.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.id == root["task_id"]).values(
            budget_json=json.dumps({"model_calls": 999999, "tool_calls": 999999})))
    assert runtime.service.reserve_budget(worker["run_id"], worker["fencing_token"], "worker",
        {"request_id": "one", "kind": "business"})["accepted"]
    runtime.service.recover()
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == root["task_id"])).scalar_one() == "WAITING_CHILDREN"


def test_explicit_wait_timeout_escalates_without_expiring_root(runtime):
    root = root_goal(runtime)
    worker = runtime.service.claim("worker")
    runtime.service.ask(worker["run_id"], worker["fencing_token"], "worker", {
        "question_id": "question", "text": "需要一个信息", "checkpoint": "已完成初步检查"})
    runtime.service.submit(worker["run_id"], worker["fencing_token"], "worker",
        {"status": "waiting_input", "summary": "等待信息"}, receipt=receipt(worker, "run_yielded"))
    with runtime.db.transaction() as conn:
        conn.execute(update(waits).where(waits.c.id == "question").values(
            expires_at=(runtime.clock.now() - timedelta(seconds=1)).isoformat()))
    runtime.service.recover()
    attention = runtime.sessions.claim("leader")
    assert attention["purpose"] == "attention"
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == root["task_id"])).scalar_one() == "WAITING_CHILDREN"


def test_historical_detached_task_ignores_its_old_deadline(runtime):
    from qqbot.domain.execution import ExecutionPlan

    old = runtime.service.accept(runtime.event, runtime.access, "历史后台事项",
        plan=ExecutionPlan(initial_delivery="DETACHED"))
    expire_task_budget(runtime)
    claim = runtime.service.claim("worker")
    assert claim["task_id"] == old["id"]
    assert claim["deadline"] is None and claim["active_seconds_remaining"] is None
    assert runtime.service.renew(claim["run_id"], claim["fencing_token"], "worker")["state"] == "RUNNING"


def test_root_question_early_answer_resumes_leader_and_never_worker(runtime):
    from qqbot.domain import TextSegment

    root = root_goal(runtime)
    worker = runtime.service.claim("worker")
    runtime.service.submit(worker["run_id"], worker["fencing_token"], "worker",
        {"status": "failed", "summary": "需要用户决策", "agent_authored": False})
    attention = runtime.sessions.claim("leader")
    with runtime.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.id == root["task_id"]).values(state="RUNNING"))
    runtime.service.ask(attention["run_id"], attention["fencing_token"], "leader", {
        "question_id": "root-question", "text": "需要我继续处理吗？", "checkpoint": "等待决定"})
    event = runtime.event.model_copy(update={"event_id": "answer", "segments": (TextSegment(text="继续"),)})
    with runtime.db.transaction() as conn:
        task = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
        answer = runtime.service.add_graph_input(conn, task, event, runtime.access, "继续",
            kind="answer", question_id="root-question")
        assert conn.execute(select(waits.c.state).where(waits.c.id == "root-question")).scalar_one() == "INTENT"
        conn.execute(update(waits).where(waits.c.id == "root-question").values(state="WAITING"))
        conn.execute(update(tasks).where(tasks.c.id == root["task_id"]).values(state="WAITING_INPUT"))
        assert runtime.service._wake(conn, root["task_id"], "root-question")
        conn.execute(update(runs).where(runs.c.id == attention["run_id"]).values(slot_reserved=0))
    next_turn = runtime.sessions.claim("leader")
    assert next_turn["purpose"] == "attention"
    assert next_turn["task_input_ids"] == [answer["input_id"]]
    assert next_turn["continuation"]["inputs"][0]["text"] == "继续"
    assert runtime.service.claim("incorrect-root-worker") is None


def test_root_question_timeout_leaves_goal_open_and_wakes_leader(runtime):
    root = root_goal(runtime)
    worker_complete = runtime.service.claim("worker")
    runtime.service.submit(worker_complete["run_id"], worker_complete["fencing_token"], "worker",
        {"status": "failed", "summary": "待用户决定", "agent_authored": False})
    attention = runtime.sessions.claim("leader")
    with runtime.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.id == root["task_id"]).values(state="RUNNING"))
    runtime.service.ask(attention["run_id"], attention["fencing_token"], "leader", {
        "question_id": "root-timeout", "text": "继续吗？", "checkpoint": "等待用户决定"})
    with runtime.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.id == root["task_id"]).values(state="WAITING_INPUT"))
        conn.execute(update(waits).where(waits.c.id == "root-timeout").values(state="WAITING",
            expires_at=(runtime.clock.now() - timedelta(seconds=1)).isoformat()))
        conn.execute(update(runs).where(runs.c.id == attention["run_id"]).values(slot_reserved=0))
        conn.execute(update(leader_inbox).values(handled_at=runtime.clock.now().isoformat()))
    runtime.service.recover()
    assert runtime.sessions.claim("leader")["purpose"] == "attention"
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == root["task_id"])).scalar_one() == "WAITING_CHILDREN"
