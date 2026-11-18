"""Blocked task roots cannot hide another root's results or their own reconciliation."""
import json

import pytest
from sqlalchemy import select, update
from test_agent_runtime import leader_complete, root_goal, worker_complete
from test_agent_runtime import runtime as agent_runtime

from elephant.storage.action_schema import operations
from elephant.storage.agent_schema import leader_inbox
from elephant.storage.schema import outbox
from elephant.storage.task_schema import tasks

runtime = agent_runtime


def fill_child_results(runtime, worker):
    with runtime.db.transaction() as conn:
        child = conn.execute(select(tasks).where(tasks.c.id == worker["task_id"])).mappings().one()
        for index in range(runtime.sessions.settings.orchestration.leader_inbox_batch_limit + 1):
            runtime.service.graph.event(conn, child, "child_result", {"index": index},
                event_id="extra-result:" + str(index), wake=True)


@pytest.mark.parametrize("failed", [False, True])
def test_incomplete_root_larger_than_batch_does_not_starve_another_root(runtime, failed):
    old = root_goal(runtime, children=2)
    first = worker_complete(runtime)
    runtime.service.claim("busy-worker")
    fill_child_results(runtime, first)
    runtime.clock.advance(seconds=1)
    new = root_goal(runtime, key="new-root")
    if failed:
        worker = runtime.service.claim("worker")
        runtime.service.submit(worker["run_id"], worker["fencing_token"], "worker",
            {"status": "failed", "summary": "结果提交被拒绝", "agent_authored": False})
    else:
        worker_complete(runtime)
    claim = runtime.sessions.claim("leader")
    assert claim["target_root_task_id"] == new["task_id"]
    assert claim["purpose"] == ("attention" if failed else "finalize")
    assert runtime.sessions.claim("another-writer") is None
    with runtime.db.engine.connect() as conn:
        assert not any(conn.execute(select(leader_inbox.c.claimed_attempt_id)
            .where(leader_inbox.c.root_task_id == old["task_id"])).scalars())


def test_attention_beyond_child_result_batch_is_claimed(runtime):
    root_goal(runtime, children=2)
    first = worker_complete(runtime)
    fill_child_results(runtime, first)
    runtime.clock.advance(seconds=1)
    worker = runtime.service.claim("worker")
    runtime.service.submit(worker["run_id"], worker["fencing_token"], "worker",
        {"status": "failed", "summary": "需要处理异常", "agent_authored": False})
    claim = runtime.sessions.claim("leader")
    assert claim["purpose"] == "attention"
    with runtime.db.engine.connect() as conn:
        assert "needs_attention" in conn.execute(select(leader_inbox.c.resume_reason)
            .where(leader_inbox.c.id.in_(claim["source_ids"]))).scalars().all()


@pytest.mark.parametrize("operation_state", ["UNKNOWN", "PARTIAL", "PREPARED", "DISPATCHED", "RECONCILIATION"])
@pytest.mark.parametrize("reconciled", [False, True])
def test_success_with_unsettled_operation_gets_one_attention_turn(runtime, operation_state, reconciled):
    root = root_goal(runtime)
    worker = worker_complete(runtime)
    with runtime.db.transaction() as conn:
        conn.execute(operations.insert().values(id="uncertain-log", task_id=worker["task_id"],
            run_id=worker["run_id"], fence=worker["fencing_token"], arguments_hash="hash",
            action_json=json.dumps({"name": "workspace.file.write", "resources": [], "effect": "write"}),
            arguments_json="{}", result_json="{}", state=operation_state, updated_at=runtime.clock.now().isoformat()))
        task = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
        assert not runtime.service.graph.finalize_ready(conn, task)
    claim = runtime.sessions.claim("leader")
    assert claim["purpose"] == "attention"
    assert claim["target_root_task_id"] == root["task_id"]
    if reconciled:
        with runtime.db.transaction() as conn:
            conn.execute(update(operations).where(operations.c.id == "uncertain-log").values(state="VERIFIED"))
    leader_complete(runtime, claim, "执行结果已保存。")
    assert runtime.sessions.claim("leader") is None
    assert runtime.sessions.claim("leader") is None
    with runtime.db.engine.connect() as conn:
        expected = "SUCCEEDED" if reconciled else "WAITING_CHILDREN"
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == root["task_id"])).scalar_one() == expected
        assert conn.execute(select(outbox.c.id)).scalar_one()
        assert all(conn.execute(select(leader_inbox.c.handled_at)).scalars())


def test_coalescing_progress_does_not_hold_another_roots_final_result(runtime):
    root_goal(runtime)
    worker = runtime.service.claim("busy-worker")
    with runtime.db.transaction() as conn:
        child = conn.execute(select(tasks).where(tasks.c.id == worker["task_id"])).mappings().one()
        runtime.service.graph.event(conn, child, "progress", {"summary": "正在处理"}, wake=True)
    runtime.clock.advance(seconds=1)
    new = root_goal(runtime, key="new-root")
    worker_complete(runtime)
    claim = runtime.sessions.claim("leader")
    assert claim["target_root_task_id"] == new["task_id"]
    assert claim["purpose"] == "finalize"
