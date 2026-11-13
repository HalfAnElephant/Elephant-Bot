"""Worker progress is narrated once by Leader and expires after a user revision."""
from sqlalchemy import select
from test_host_controls import control_runtime as control_runtime  # noqa: PLC0414 - Register the shared fixture.

from qqbot.domain import OutboundEnvelope
from qqbot.storage.agent_schema import leader_inbox
from qqbot.storage.schema import outbox
from qqbot.storage.task_schema import notifications, tasks


def complete_progress(r, text):
    r.core.clock.advance(3)
    claim = r.core.sessions.claim("leader")
    assert claim and claim["purpose"] == "progress"
    r.core.leader.complete(claim["run_id"], claim["fencing_token"], "leader",
        {"status": "completed", "summary": text}, stopped=True, quiescent=False,
        receipt={"kind": "turn_finished", "session_id": claim["session_id"], "run_id": claim["run_id"],
            "session_revision": "leaf", "input_ids": claim["source_ids"], "inflight": 0, "tools_paired": True})


def progress_rows(r):
    with r.core.db.engine.connect() as conn:
        return conn.execute(select(outbox).join(notifications, notifications.c.id == outbox.c.origin_id)
            .where(notifications.c.purpose == "progress")).mappings().all()


async def test_worker_progress_goes_through_leader_and_identical_updates_are_coalesced(control_runtime):
    r = control_runtime
    r.root()
    worker = r.core.tasks.claim("worker")
    for index in range(2):
        r.core.tasks.notify(worker["run_id"], worker["fencing_token"], "worker",
            {"text": "Worker 内部执行细节", "notification_id": "update-" + str(index)})
        with r.core.db.engine.connect() as conn:
            assert conn.execute(select(leader_inbox.c.id).where(leader_inbox.c.handled_at.is_(None))).first()
            assert len(conn.execute(select(outbox)).all()) == index
        complete_progress(r, "已经核对完第一部分啦")
    rows = progress_rows(r)
    assert len(rows) == 1
    assert OutboundEnvelope.model_validate_json(rows[0]["envelope_json"]).text == "已经核对完第一部分啦"


async def test_new_user_revision_invalidates_queued_progress(control_runtime):
    r = control_runtime
    r.root()
    worker = r.core.tasks.claim("worker")
    r.core.tasks.notify(worker["run_id"], worker["fencing_token"], "worker",
        {"text": "完成一部分", "notification_id": "old-update"})
    complete_progress(r, "第一部分完成啦")
    envelope = OutboundEnvelope.model_validate_json(progress_rows(r)[0]["envelope_json"])
    assert r.core.pre_send(envelope)
    await r.receive(r.event("给刚才那个补充 只保留最近一周"))
    assert not r.core.pre_send(envelope)
    pending = r.core.tasks.take_inputs(worker["run_id"], worker["fencing_token"], "worker")
    r.core.tasks.session_receipt(worker["run_id"], worker["fencing_token"], "worker", {
        "kind": "input_consumed", "session_id": worker["session_id"], "run_id": worker["run_id"],
        "session_revision": "updated", "input_ids": [item["id"] for item in pending]})
    r.core.tasks.notify(worker["run_id"], worker["fencing_token"], "worker",
        {"text": "已按新要求完成一部分", "notification_id": "new-update"})
    complete_progress(r, "第一部分完成啦")
    assert len(progress_rows(r)) == 2


async def test_leader_readback_uses_original_worker_workspace_before_takeover(control_runtime):
    r = control_runtime
    root = r.root()
    worker = r.core.tasks.claim("worker")
    with r.core.db.engine.connect() as conn:
        child = conn.execute(select(tasks).where(tasks.c.id == worker["task_id"])).mappings().one()
    workspace = r.core.recovery._workspace(child, strict=False)
    (workspace / "outputs").mkdir(parents=True)
    (workspace / "outputs/a.txt").write_text("expected")
    identity = (worker["run_id"], worker["fencing_token"], "worker")
    await r.core.tasks.ledger(*identity, {"operation": "prepare", "operation_id": "worker-file",
        "arguments_hash": "file-hash", "arguments": {"path": "outputs/a.txt", "text": "expected"},
        "action": {"name": "workspace.file.write", "effect": "write", "resources": ["workspace:" + str(workspace)]}})
    for before, after in (("PREPARED", "DISPATCHED"), ("DISPATCHED", "UNKNOWN")):
        await r.core.tasks.ledger(*identity, {"operation": "transition", "operation_id": "worker-file",
            "expected": before, "state": after, "result": {}})
    r.core.tasks.submit(*identity, {"status": "failed", "summary": "写入需要核实", "agent_authored": False})
    attention = r.core.sessions.claim("leader")
    assert attention["purpose"] == "attention" and attention["target_root_task_id"] == root["task_id"]
    leader = (attention["run_id"], attention["fencing_token"], "leader")
    result = await r.core.leader.invoke("ledger", leader, {"operation": "reconcile", "operation_id": "worker-file"})
    assert result["state"] == "VERIFIED" and result["result"]["verified_effect"] == "file_content_matches"
    takeover = await r.core.leader.invoke("tasks", leader,
        {"operation": "tasks.take_over", "call_id": "takeover", "arguments": {}})
    assert takeover["takeover"]
