"""Interrupted Leader decisions preserve native receipts and external-write evidence."""
import json

import pytest
from sqlalchemy import func, select
from test_agent_runtime import root_goal
from test_agent_runtime import runtime as runtime  # noqa: PLC0414 - Register the shared pytest fixture.
from test_leader_decisions import failed_attention, tool
from test_leader_dispatch_recovery import native_pending_submit

from qqbot.adapters.pi.sessions import branch_entries, load_entries, restore_tool_results, unfinished_tools
from qqbot.domain import TextSegment
from qqbot.storage.action_schema import operations
from qqbot.storage.task_schema import runs, waits


def prepare(operation_id, arguments_hash="same-write"):
    return {"operation": "prepare", "operation_id": operation_id, "arguments_hash": arguments_hash,
        "arguments": {"body": "完成"}, "action": {"name": "github.issue.write", "effect": "write",
            "resources": ["repo:github.com/org/repo"]}}


async def transition(r, identity, operation_id, before, after, *, leader=True):
    payload = {"operation": "transition", "operation_id": operation_id, "expected": before,
        "state": after, "result": {"url": "https://github.com/org/repo/issues/1"}}
    if leader:
        return await r.leader.invoke("ledger", identity, payload)
    return await r.service.ledger(*identity, payload)


def native_results(path):
    return [entry["message"] for entry in branch_entries(load_entries(path)) if entry.get("type") == "message"
            and entry["message"]["role"] == "toolResult"]


async def test_stopped_takeover_recovers_interrupted_native_write_without_replaying_it(runtime):
    r = runtime
    _, _, attention = failed_attention(r)
    await tool(r, attention, "tasks.take_over")
    identity = (attention["run_id"], attention["fencing_token"], "leader")
    await r.leader.invoke("ledger", identity, prepare("dispatched"))
    await transition(r, identity, "dispatched", "PREPARED", "DISPATCHED")
    await r.leader.invoke("ledger", identity, prepare("prepared", "different-write"))
    path = native_pending_submit(r, attention, {"name": "github.issue.write", "arguments": {"body": "完成"}},
        tool_name="capability_invoke")
    original = path.read_bytes()
    r.clock.advance(31)
    r.service.stop_verifier = lambda *_: False
    assert r.leader.recover_attempt(attention["run_id"])["state"] == "NEEDS_RECONCILIATION"
    assert path.read_bytes() == original
    with r.db.engine.connect() as conn:
        assert conn.execute(select(runs.c.slot_reserved).where(runs.c.id == attention["run_id"])).scalar_one() == 1
        assert dict(conn.execute(select(operations.c.id, operations.c.state)).all()) == {
            "dispatched": "DISPATCHED", "prepared": "PREPARED"}
    user = r.event.model_copy(update={"event_id": "new-chat", "segments": (TextSegment(text="先聊另一件事"),)})
    r.sessions.enqueue(user, r.access)
    assert r.sessions.claim("replacement-leader") is None
    r.service.stop_verifier = lambda *_: True
    assert r.leader.recover_attempt(attention["run_id"])["state"] == "RECOVERED"
    assert path.read_bytes().startswith(original) and unfinished_tools(path) == ()
    results = native_results(path)
    assert len(results) == 1 and results[0]["isError"] is True
    assert results[0]["toolCallId"] == "native-submit-call"
    assert json.loads(results[0]["content"][0]["text"])["state"] == "INTERRUPTED"
    with r.db.engine.connect() as conn:
        assert dict(conn.execute(select(operations.c.id, operations.c.state)).all()) == {
            "dispatched": "UNKNOWN", "prepared": "FAILED"}
        assert conn.execute(select(runs.c.slot_reserved).where(runs.c.id == attention["run_id"])).scalar_one() == 0
        assert conn.execute(select(func.count()).select_from(operations)).scalar_one() == 2
    assert r.leader.recover_attempt(attention["run_id"])["state"] == "RECOVERED"
    assert len(native_results(path)) == 1
    resumed = r.sessions.claim("leader")
    assert resumed is not None and resumed["session_id"] == attention["session_id"]
    assert resumed["run_id"] != attention["run_id"] and resumed["purpose"] in {"user", "attention"}
    assert len(native_results(path)) == 1


@pytest.mark.parametrize("early", [False, True])
async def test_root_question_intent_survives_paired_tool_without_final_boundary(runtime, early):
    r = runtime
    root, _, attention = failed_attention(r)
    identity = (attention["run_id"], attention["fencing_token"], "leader")
    question = {"question_id": "crashed-question", "kind": "input", "text": "是否继续？", "checkpoint": "等待决定"}
    accepted = await r.leader.invoke("ask", identity, question)
    path = native_pending_submit(r, attention, question, tool_name="task_ask_user")
    pending = unfinished_tools(path)
    restore_tool_results(path, [{"call": pending[0], "result": accepted}],
        pi_executable=r.sessions.settings.orchestration.pi_executable)
    assert unfinished_tools(path) == () and native_results(path)[0]["isError"] is False
    answer = r.event.model_copy(update={"event_id": "real-answer", "segments": (TextSegment(text="继续"),)})
    def respond():
        r.service.add_input(root["public_id"], answer, r.access, answer.text, question_id="crashed-question")
    if early:
        respond()
    r.clock.advance(31)
    assert r.leader.recover_attempt(attention["run_id"])["state"] == "RECOVERED"
    with r.db.engine.connect() as conn:
        assert conn.execute(select(runs.c.slot_reserved).where(runs.c.id == attention["run_id"])).scalar_one() == 0
        assert conn.execute(select(waits.c.state).where(waits.c.id == "crashed-question")).scalar_one() == (
            "CONSUMED" if early else "WAITING")
    if not early:
        respond()
    resumed = r.sessions.claim("leader")
    assert resumed["purpose"] == "attention"
    assert [item["text"] for item in resumed["continuation"]["inputs"]] == ["继续"]
    assert r.service.claim("root-is-not-a-worker") is None


@pytest.mark.parametrize("takeover", [False, True])
async def test_takeover_and_rework_share_verified_child_write_but_cannot_read_other_tree(runtime, takeover):
    r = runtime
    root = root_goal(r)
    worker = r.service.claim("worker")
    worker_identity = worker["run_id"], worker["fencing_token"], "worker"
    await r.service.ledger(*worker_identity, prepare("worker-write"))
    await transition(r, worker_identity, "worker-write", "PREPARED", "DISPATCHED", leader=False)
    await transition(r, worker_identity, "worker-write", "DISPATCHED", "VERIFIED", leader=False)
    r.service.submit(*worker_identity, {"status": "failed", "summary": "需要后续处理", "agent_authored": False})
    attention = r.sessions.claim("leader")
    leader_identity = attention["run_id"], attention["fencing_token"], "leader"
    if takeover:
        await tool(r, attention, "tasks.take_over")
        reused = await r.leader.invoke("ledger", leader_identity, prepare("leader-would-repeat"))
        assert reused["operation_id"] == "worker-write" and reused["state"] == "VERIFIED"
    with r.db.engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(operations)).scalar_one() == 1
    listed = await r.leader.invoke("ledger", leader_identity, {"operation": "list"})
    read = await r.leader.invoke("ledger", leader_identity, {"operation": "read", "operation_id": "worker-write"})
    assert [item["id"] for item in listed] == ["worker-write"]
    assert read[0]["task_id"] == worker["task_id"] and read[0]["run_id"] == worker["run_id"]
    if not takeover:
        rework = await tool(r, attention, "tasks.spawn", {"goal": "核实既有结果后继续", "delegation_key": "rework",
            "rework_of_task_id": worker["task_id"]})
        retry = r.service.claim("retry")
        assert retry["task_id"] == rework["task_id"]
        inherited = await r.service.ledger(retry["run_id"], retry["fencing_token"], "retry", prepare("retry-would-repeat"))
        assert inherited["operation_id"] == "worker-write" and inherited["state"] == "VERIFIED"
    foreign_event = r.event.model_copy(update={"event_id": "foreign-input", "segments": (TextSegment(text="处理另一项目"),)})
    with r.db.transaction() as conn:
        dialogue = r.sessions.dialogue(conn, foreign_event)
        other = r.service.graph.submit(conn, dialogue, foreign_event, r.access,
            {"goal": "处理另一项目", "children": [{"goal": "其他检查", "delegation_key": "foreign"}]},
            "foreign-dispatch", r.sessions.binding(), {})
        conn.execute(operations.insert().values(id="foreign-write", task_id=other["task_id"], run_id="foreign-run", fence=1,
            arguments_hash="foreign-hash", action_json=json.dumps(prepare("foreign")["action"]),
            arguments_json="{}", result_json="{}", state="VERIFIED", updated_at=r.clock.now().isoformat()))
    assert await r.leader.invoke("ledger", leader_identity, {"operation": "read", "operation_id": "foreign-write"}) == []
    assert [item["id"] for item in await r.leader.invoke("ledger", leader_identity, {"operation": "list"})] == ["worker-write"]
    with r.db.engine.connect() as conn:
        assert conn.execute(select(func.count()).select_from(operations)).scalar_one() == 2
    assert root["task_id"] != other["task_id"]
