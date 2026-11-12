"""Root finalizers can cite verified facts from their own task tree."""
import json

import pytest
from sqlalchemy import insert, select, update
from test_agent_runtime import root_goal, worker_complete
from test_agent_runtime import runtime as agent_runtime

from qqbot.domain.tasks import TaskResult
from qqbot.storage.action_schema import operations
from qqbot.storage.task_schema import observations, tasks

runtime = agent_runtime


def record_refs(runtime, worker, *, owner=None, effect_state="VERIFIED"):
    owner = owner or worker["task_id"]
    now = runtime.clock.now().isoformat()
    with runtime.db.transaction() as conn:
        conn.execute(insert(observations).values(id="child-observation", task_id=owner, run_id=worker["run_id"],
            retrieved_at=now, sha256="a" * 64, data_json="{}"))
        conn.execute(insert(operations).values(id="child-effect", task_id=owner, run_id=worker["run_id"], fence=1,
            arguments_hash="hash", action_json="{}", arguments_json="{}", state=effect_state,
            result_json="{}", updated_at=now))


def complete_with_refs(runtime, claim, status, **refs):
    receipt = {"kind": "turn_finished", "session_id": claim["session_id"], "run_id": claim["run_id"],
        "session_revision": "leaf", "input_ids": claim["context"]["source_input_ids"],
        "inflight": 0, "tools_paired": True}
    return runtime.leader.complete(claim["run_id"], claim["fencing_token"], "leader",
        {"status": status, "summary": "已核验执行结果", **refs}, stopped=True, quiescent=False, receipt=receipt)


@pytest.mark.parametrize("status", ["completed", "partial"])
async def test_finalizer_accepts_explicit_verified_child_refs(runtime, status):
    root = root_goal(runtime)
    worker = worker_complete(runtime)
    record_refs(runtime, worker)
    with runtime.db.transaction() as conn:
        row = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
        goal = json.loads(row["goal_json"])
        goal["success_checks"] = ["observation", "effect"]
        conn.execute(update(tasks).where(tasks.c.id == row["id"]).values(goal_json=json.dumps(goal)))
    claim = runtime.sessions.claim("leader")
    assert complete_with_refs(runtime, claim, status, observation_refs=["child-observation"],
                              effect_refs=["child-effect"]) == {"state": "FINISHED"}
    with runtime.db.engine.connect() as conn:
        row = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
    assert row["state"] == ("SUCCEEDED" if status == "completed" else "PARTIAL")
    result = json.loads(row["result_json"])
    assert result["observation_refs"] == ["child-observation"]
    assert result["effect_refs"] == ["child-effect"]


@pytest.mark.parametrize("status", ["completed", "partial"])
@pytest.mark.parametrize("check", ["observation", "effect"])
def test_finalizer_rejects_refs_from_another_root(runtime, status, check):
    root_goal(runtime)
    worker = worker_complete(runtime)
    claim = runtime.sessions.claim("leader")
    foreign = root_goal(runtime, key="foreign-root")
    record_refs(runtime, worker, owner=foreign["task_id"])
    refs = {check + "_refs": ["child-" + check]}
    with pytest.raises(ValueError, match="root success check not verified: " + check):
        complete_with_refs(runtime, claim, status, **refs)


@pytest.mark.parametrize("status", ["completed", "partial"])
def test_finalizer_rejects_unverified_child_effect(runtime, status):
    root_goal(runtime)
    worker = worker_complete(runtime)
    record_refs(runtime, worker, effect_state="FAILED")
    claim = runtime.sessions.claim("leader")
    with pytest.raises(ValueError, match="root success check not verified: effect"):
        complete_with_refs(runtime, claim, status, effect_refs=["child-effect"])


@pytest.mark.parametrize("check", ["observation", "effect"])
def test_worker_check_still_rejects_other_task_refs(runtime, check):
    root = root_goal(runtime)
    worker = worker_complete(runtime)
    record_refs(runtime, worker, owner=root["task_id"])
    result = TaskResult.model_validate({"status": "completed", "summary": "result",
        check + "_refs": ["child-" + check]})
    with runtime.db.engine.connect() as conn, pytest.raises(ValueError, match="success check not verified"):
        runtime.service._verify_checks(conn, (check,), result, worker["task_id"])
