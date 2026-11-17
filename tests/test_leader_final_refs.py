"""Root finalizers can cite verified facts from their own task tree."""
import json

import pytest
from sqlalchemy import insert, select, update
from test_agent_runtime import root_goal, worker_complete
from test_agent_runtime import runtime as agent_runtime

from elephant.domain.tasks import TaskResult
from elephant.storage.action_schema import operations
from elephant.storage.task_schema import observations, tasks

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


@pytest.mark.parametrize("check", ["observation", "effect"])
@pytest.mark.parametrize("foreign", [False, True])
async def test_finalizer_reference_preflight_uses_the_same_root_scope(runtime, check, foreign):
    from elephant.runtime.scheduler import RuntimeClient

    root_goal(runtime)
    worker = worker_complete(runtime)
    claim = runtime.sessions.claim("leader")
    owner = root_goal(runtime, key="foreign-root")["task_id"] if foreign else worker["task_id"]
    record_refs(runtime, worker, owner=owner)
    client = RuntimeClient(runtime.leader.core, runtime.leader)
    response = await client.post("/internal/tasks/validate-result-refs", json={"worker_id": "leader",
        "run_id": claim["run_id"], "fencing_token": claim["fencing_token"],
        "payload": {"status": "completed", check + "_refs": ["child-" + check]}})
    assert response.status_code == (422 if foreign else 200), response.text
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.result_json).where(tasks.c.id == claim["task_id"])).scalar_one() is None


@pytest.mark.parametrize("check", ["observation", "effect"])
async def test_worker_reference_preflight_accepts_prior_attempt_and_rejects_missing(runtime, check):
    from elephant.runtime.scheduler import RuntimeClient

    root_goal(runtime)
    worker = runtime.service.claim("worker")
    record_refs(runtime, {**worker, "run_id": "prior-attempt"})
    client = RuntimeClient(runtime.leader.core, runtime.leader)
    body = {"worker_id": "worker", "run_id": worker["run_id"], "fencing_token": worker["fencing_token"]}
    valid = await client.post("/internal/tasks/validate-result-refs", json={**body,
        "payload": {"status": "completed", check + "_refs": ["child-" + check]}})
    assert valid.status_code == 200, valid.text
    invalid = await client.post("/internal/tasks/validate-result-refs", json={**body,
        "payload": {"status": "completed", check + "_refs": ["missing-" + check]}})
    assert invalid.status_code == 422
    stale = await client.post("/internal/tasks/validate-result-refs", json={**body, "fencing_token": 100,
        "payload": {"status": "completed", check + "_refs": ["child-" + check]}})
    assert stale.status_code == 409
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.result_json).where(tasks.c.id == worker["task_id"])).scalar_one() is None
