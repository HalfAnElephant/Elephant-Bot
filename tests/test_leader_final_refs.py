"""Root finalizers can cite verified facts from their own task tree."""
import json

import pytest
from sqlalchemy import insert, select, update
from test_agent_runtime import leader_complete, root_goal, worker_complete
from test_agent_runtime import runtime as agent_runtime

from elephant.domain.tasks import TaskResult
from elephant.harness.artifacts import export_artifacts
from elephant.storage.action_schema import operations
from elephant.storage.schema import outbox
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


@pytest.mark.parametrize("worker_status", ["completed", "partial"])
async def test_worker_status_archive_and_verified_report_share_the_current_leader(runtime, worker_status):
    root = root_goal(runtime)
    worker = runtime.service.claim("worker")
    assert worker["task_id"] == root["children"][0]["id"]
    foreign = root_goal(runtime, key="foreign-artifact")
    runtime.sessions.enqueue(runtime.event, runtime.access)
    user = runtime.sessions.claim("leader")

    async def call(claim, operation, **arguments):
        return await runtime.leader.invoke("tasks", (claim["run_id"], claim["fencing_token"], "leader"),
            {"operation": operation, "arguments": arguments, "call_id": operation})

    snapshot = await call(user, "tasks.status", task_ref=root["public_id"])
    assert snapshot["children"][0]["state"] == "RUNNING"
    complete_with_refs(runtime, user, "completed")

    text = "1. 番茄牛腩\n2. 菌菇鸡汤"
    (runtime.path / "dinner.md").write_text(text, encoding="utf-8")
    artifact, = export_artifacts(runtime.path, runtime.service.archive_root, ["dinner.md"])
    record_refs(runtime, worker, effect_state="VERIFIED" if worker_status == "completed" else "FAILED")
    receipt = {"kind": "turn_finished", "session_id": worker["context"]["session_id"],
        "run_id": worker["run_id"], "session_revision": "leaf",
        "input_ids": worker["context"]["source_input_ids"], "inflight": 0, "tools_paired": True}
    runtime.service.submit(worker["run_id"], worker["fencing_token"], "worker",
        {"status": worker_status, "summary": "清单已生成", "artifacts": [artifact], "effect_refs": ["child-effect"]},
        receipt=receipt)
    with runtime.db.transaction() as conn:
        row = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
        goal = json.loads(row["goal_json"])
        goal.update(success_checks=["artifact"], requested_outputs=["dinner.md"])
        conn.execute(update(tasks).where(tasks.c.id == row["id"]).values(goal_json=json.dumps(goal)))
    final = runtime.sessions.claim("leader")
    assert final["session_id"] == user["session_id"]
    assert final["purpose"] == ("finalize" if worker_status == "completed" else "attention")
    listed, = await call(final, "tasks.list")
    assert listed["public_id"] == root["public_id"]
    status = await call(final, "tasks.status", task_ref=root["public_id"])
    child, = status["children"]
    assert child["state"] == ("SUCCEEDED" if worker_status == "completed" else "PARTIAL")
    assert child["result"]["artifacts"] == [artifact]
    with pytest.raises(PermissionError, match="another root"):
        await call(final, "tasks.read_artifact", task_ref=foreign["children"][0]["id"], artifact_id=artifact["id"])
    with pytest.raises(KeyError, match="artifact unavailable"):
        await call(final, "tasks.read_artifact", task_ref=child["id"], artifact_id="unknown")
    read = {"task_ref": child["id"], "artifact_id": artifact["id"]}
    page = await call(final, "tasks.read_artifact", **read, limit=7)
    tail = await call(final, "tasks.read_artifact", **read, offset=page["next_offset"])
    assert page["text"] + tail["text"] == text and tail["next_offset"] is None
    path = runtime.service.archive_root / artifact["id"]
    path.write_bytes(b"X" * artifact["size"])
    with pytest.raises(ValueError, match="digest changed"):
        await call(final, "tasks.read_artifact", **read)
    path.write_text(text, encoding="utf-8")
    # The Leader need not cite a side effect to deliver a verified file.
    leader_complete(runtime, final, text)
    leader_complete(runtime, final, text)
    with runtime.db.engine.connect() as conn:
        target = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
        assert target["state"] == "SUCCEEDED"
        assert json.loads(target["result_json"])["effect_refs"] == (["child-effect"] if worker_status == "completed" else [])
        deliveries = conn.execute(select(outbox.c.envelope_json).where(outbox.c.origin_kind == "task")).scalars().all()
        assert [json.loads(delivery)["text"] for delivery in deliveries] == ["1）番茄牛腩\n2）菌菇鸡汤"]


@pytest.mark.parametrize("worker_status", ["blocked", "partial"])
@pytest.mark.parametrize("takeover", ["none", "without_evidence", "verified"])
async def test_incomplete_worker_artifacts_cannot_complete_scheduled_root(runtime, worker_status, takeover):
    root = root_goal(runtime)
    with runtime.db.transaction() as conn:
        task = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
        context = json.loads(task["context_json"]) | {"schedule_fire_id": "fire"}
        goal = json.loads(task["goal_json"]) | {"success_checks": ["artifact"], "requested_outputs": ["report.md"]}
        conn.execute(update(tasks).where(tasks.c.id == task["id"]).values(
            context_json=json.dumps(context), goal_json=json.dumps(goal)))
    worker = runtime.service.claim("worker")
    (runtime.path / "report.md").write_text("diagnostics only, host inspection incomplete", encoding="utf-8")
    artifact, = export_artifacts(runtime.path, runtime.service.archive_root, ["report.md"])
    record_refs(runtime, worker)
    receipt = {"kind": "turn_finished", "session_id": worker["context"]["session_id"],
        "run_id": worker["run_id"], "session_revision": "leaf",
        "input_ids": worker["context"]["source_input_ids"], "inflight": 0, "tools_paired": True}
    runtime.service.submit(worker["run_id"], worker["fencing_token"], "worker",
        {"status": worker_status, "summary": "主机检查未完成", "artifacts": [artifact], "effect_refs": ["child-effect"]},
        receipt=receipt)
    final = runtime.sessions.claim("leader")
    assert final["purpose"] == "attention"
    if takeover != "none":
        await runtime.leader.invoke("tasks", (final["run_id"], final["fencing_token"], "leader"), {
            "operation": "tasks.take_over", "arguments": {}, "call_id": "take-over"})
    refs = {"artifacts": [artifact], "effect_refs": ["child-effect"]} if takeover != "none" else {}
    if takeover == "verified":
        (runtime.path / "report.md").write_text("verified on host", encoding="utf-8")
        verified, = export_artifacts(runtime.path, runtime.service.archive_root, ["report.md"])
        refs["artifacts"].append(verified)
    complete_with_refs(runtime, final, "completed", **refs)
    complete_with_refs(runtime, final, "completed", **refs)
    with runtime.db.engine.connect() as conn:
        row = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
        result = json.loads(row["result_json"])
        assert row["state"] == ("SUCCEEDED" if takeover == "verified" else "BLOCKED")
        assert result["artifacts"] == ([verified] if takeover == "verified" else [])
        assert result["effect_refs"] == []
        worker_result = conn.scalar(select(tasks.c.result_json).where(tasks.c.id == worker["task_id"]))
        assert json.loads(worker_result)["artifacts"] == [artifact]
        delivery = conn.scalar(select(outbox.c.envelope_json))
        assert json.loads(delivery)["text"] == result["summary"]
