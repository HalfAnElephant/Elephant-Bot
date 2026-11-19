"""Worker events drive bounded progress and durable result delivery."""
import json
from datetime import timedelta

import httpx
import pytest
from sqlalchemy import select
from test_host_controls import control_runtime as control_runtime  # noqa: PLC0414 - Register the shared fixture.

from elephant.app import create_app
from elephant.domain import OutboundEnvelope
from elephant.storage.action_schema import operations
from elephant.storage.agent_schema import agent_events, leader_inbox
from elephant.storage.schema import outbox
from elephant.storage.task_schema import notifications, tasks


def report_progress(r, worker, key, text):
    identity = (worker["run_id"], worker["fencing_token"], "worker")
    reference = "observation:" + key
    r.core.tasks.observation(*identity, {"id": reference, "data": {"checked": key}, "sha256": "a" * 64,
        "retrieved_at": r.core.clock.now().isoformat()})
    arguments = {"phase": "checking", "public_text": text, "evidence_refs": [reference]}
    result = r.core.tasks.task_operation(*identity, {"operation": "tasks.report_progress", "call_id": key,
        "arguments": arguments})
    assert result["accepted"]
    return arguments


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


async def test_last_worker_result_survives_restart_before_parent_admission(control_runtime, tmp_path):
    r = control_runtime
    source = r.event()
    root = r.root(source, children=2)
    result = {"status": "completed", "summary": "已核对"}
    effect = tmp_path / "verified-effect.txt"
    prepare = {"operation": "prepare", "operation_id": "verify-once", "arguments_hash": "fixture-hash",
        "arguments": {"path": effect.name}, "action": {"name": "workspace.file.write", "effect": "write",
            "resources": ["workspace:" + str(tmp_path)]}}
    for index in range(2):
        worker = r.core.tasks.claim("worker")
        identity = (worker["run_id"], worker["fencing_token"], "worker")
        if index == 1:
            await r.core.tasks.ledger(*identity, prepare)
            await r.core.tasks.ledger(*identity, {"operation": "transition", "operation_id": "verify-once",
                "expected": "PREPARED", "state": "DISPATCHED", "result": {}})
            effect.write_text("执行一次\n")
            await r.core.tasks.ledger(*identity, {"operation": "transition", "operation_id": "verify-once",
                "expected": "DISPATCHED", "state": "VERIFIED", "result": {"verified_effect": "file_content_matches"}})
            result["effect_refs"] = ["verify-once"]
        receipt = {"kind": "turn_finished", "run_id": worker["run_id"], "session_id": worker["session_id"],
            "session_revision": "worker-leaf", "input_ids": worker["context"]["source_input_ids"],
            "inflight": 0, "tools_paired": True}
        assert r.core.tasks.submit(*identity, result, receipt=receipt)["state"] == "SUCCEEDED"
        if index == 0:
            assert r.core.sessions.claim("leader") is None
    delivered = []

    def qq(request):
        if request.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "fixture", "expires_in": 7200})
        delivered.append(json.loads(request.content))
        return httpx.Response(200, json={"id": "qq-" + str(len(delivered))})

    client = httpx.AsyncClient(transport=httpx.MockTransport(qq))
    fresh = create_app(r.core.settings, clock=r.core.clock, run_background=False, official_client=client).state.core
    try:
        assert fresh.tasks.submit(*identity, result, receipt=receipt)["state"] == "SUCCEEDED"
        with pytest.raises(PermissionError, match="stale run"):
            await fresh.tasks.ledger(*identity, prepare)
        assert fresh.tasks.claim("worker") is None
        claim = fresh.sessions.claim("leader")
        assert claim["target_root_task_id"] == root["task_id"] and claim["purpose"] == "finalize"
        fresh.leader.complete(claim["run_id"], claim["fencing_token"], "leader",
            {"status": "completed", "summary": "两项核对均已完成"}, stopped=True, quiescent=False, receipt={
                "kind": "turn_finished", "run_id": claim["run_id"], "session_id": claim["session_id"],
                "session_revision": "root-leaf", "input_ids": claim["source_ids"], "inflight": 0, "tools_paired": True})
        assert fresh.sessions.claim("leader") is None
        with fresh.db.transaction() as conn:
            task = conn.execute(select(tasks).where(tasks.c.id == worker["task_id"])).mappings().one()
            event_id = "result:" + worker["run_id"]
            stored = conn.execute(select(agent_events).where(agent_events.c.id == event_id)).mappings().one()
            assert stored["kind"] == "child_result"
            payload = json.loads(stored["payload_json"])
            assert fresh.tasks.graph.event(conn, task, "child_result", payload,
                attempt_id=worker["run_id"], event_id=event_id, wake=True) == event_id
            with pytest.raises(ValueError, match="conflicting task event replay"):
                fresh.tasks.graph.event(conn, task, "needs_attention", payload,
                    attempt_id=worker["run_id"], event_id=event_id, wake=True)
            assert conn.execute(select(operations.c.state)).scalar_one() == "VERIFIED"
            notice = conn.execute(select(notifications.c.id).where(notifications.c.task_id == root["task_id"],
                notifications.c.purpose == "result")).scalar_one()
        assert effect.read_text() == "执行一次\n"

        fresh.clock.advance((source.reply_context.expires_at - fresh.clock.now()).total_seconds() + 1)
        await fresh.deliver()
        assert delivered == []
        with fresh.db.engine.connect() as conn:
            task = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
            snapshot = fresh.tasks.graph.status(conn, task, access=fresh.access(source))
        assert snapshot["state"] == "SUCCEEDED" and snapshot["result"]["effect_refs"] == ["verify-once"]
        assert snapshot["progress"]["delivery"] == {"required": True, "execution_complete": True,
            "delivered": False, "states": ["ACCEPTED"], "outbox_states": ["WAITING_FOR_TRIGGER"]}

        incoming = r.event("/任务 " + root["public_id"])
        trigger, _ = fresh.inbox.accept(incoming.model_copy(update={"occurred_at": fresh.clock.now(),
            "reply_context": incoming.reply_context.model_copy(update={"expires_at": fresh.clock.now() + timedelta(minutes=5)})}))
        await fresh.process(trigger)
        with fresh.db.engine.connect() as conn:
            status = conn.execute(select(outbox.c.envelope_json).where(outbox.c.origin_id == trigger.event_id)).scalar_one()
        assert fresh.pre_send(OutboundEnvelope.model_validate_json(status))
        for _ in range(3):
            await fresh.deliver()
            fresh.clock.advance(3)
        with fresh.db.engine.connect() as conn:
            assert conn.execute(select(outbox.c.state).where(outbox.c.origin_id == notice)).scalar_one() == "SENT"
            task = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
            assert fresh.tasks.graph.status(conn, task)["progress"]["delivery"]["delivered"]
        messages = [item for item in delivered if item.get("markdown", {}).get("content", item.get("content")) == "两项核对均已完成"]
        assert len(messages) == 1 and messages[0]["msg_id"] == trigger.platform_message_id
    finally:
        await fresh.stop()
        fresh.db.close()
        await client.aclose()


async def test_worker_progress_goes_through_leader_and_identical_updates_are_coalesced(control_runtime):
    r = control_runtime
    r.root()
    worker = r.core.tasks.claim("worker")
    for index in range(2):
        arguments = report_progress(r, worker, "update-" + str(index), "已核验阶段事实")
        duplicate = r.core.tasks.task_operation(worker["run_id"], worker["fencing_token"], "worker", {
            "operation": "tasks.report_progress", "call_id": "duplicate-" + str(index),
            "arguments": {**arguments, "public_text": "相同事实的另一种表述"}})
        assert duplicate == {"accepted": False, "reason": "coalesced"}
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
    report_progress(r, worker, "old-update", "完成一部分")
    complete_progress(r, "第一部分完成啦")
    envelope = OutboundEnvelope.model_validate_json(progress_rows(r)[0]["envelope_json"])
    assert r.core.pre_send(envelope)
    await r.receive(r.event("给刚才那个补充 只保留最近一周"))
    assert not r.core.pre_send(envelope)
    pending = r.core.tasks.take_inputs(worker["run_id"], worker["fencing_token"], "worker")
    r.core.tasks.session_receipt(worker["run_id"], worker["fencing_token"], "worker", {
        "kind": "input_consumed", "session_id": worker["session_id"], "run_id": worker["run_id"],
        "session_revision": "updated", "input_ids": [item["id"] for item in pending]})
    report_progress(r, worker, "new-update", "已按新要求完成一部分")
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
