"""A committed memory mutation closes its retired session without replaying tools."""
import json
from unittest.mock import Mock
from uuid import uuid4

import pytest
from sqlalchemy import select
from test_agent_runtime import memory_runtime
from test_agent_runtime import runtime as agent_runtime
from test_leader_dispatch_recovery import native_pending_submit

from qqbot.adapters.pi.sessions import unfinished_tools
from qqbot.contracts.memory import MutationReceipt
from qqbot.domain import TextSegment
from qqbot.storage.agent_schema import dialogue_inputs, dialogues
from qqbot.storage.schema import outbox
from qqbot.storage.task_schema import processes, runs

runtime = agent_runtime


@pytest.mark.parametrize("completed", [False, True])
async def test_committed_memory_marker_recovery_finishes_once_after_stop_proof(runtime, monkeypatch, completed):
    knowledge, scope = memory_runtime(runtime)
    runtime.service.max_active = 1
    runtime.sessions.enqueue(runtime.event, runtime.access, context={"memory_scope": scope.model_dump(),
        "memory_epoch": 0, "retrieved_data": [{"content": "OLD_PRIVATE_MEMORY"}]})
    old = runtime.sessions.claim("leader")
    path = native_pending_submit(runtime, old, {"source_ids": ["forgotten-source"]}, tool_name="memory_forget")
    process_id = str(uuid4())
    await runtime.leader.invoke("bind-process", (old["run_id"], old["fencing_token"], "leader"), {
        "process_id": process_id, "session_id": old["session_id"], "epoch": 1, "generation": 1,
        "scope_key": old["session_id"], "session_file": str(path),
        "identity": {"launch_task_id": old["task_id"], "actual_session_file": str(path)}})
    # The real host rotation commits the operation receipt and replaces the Dialogue Session.
    receipt = MutationReceipt(operation_id="forget-original", state="APPLIED" if completed else "PENDING",
        completed=completed, memory_epoch=1, affected_source_ids=("forgotten-source",))
    knowledge.rotate(scope, (old["run_id"], old["fencing_token"], "leader"), receipt)
    with runtime.db.engine.connect() as conn:
        persisted = json.loads(conn.execute(select(runs.c.request_json).where(runs.c.id == old["run_id"])).scalar_one())
        replacement = conn.execute(select(dialogues.c.session_id)).scalar_one()
    assert persisted["memory_mutation"]["operation_id"] == receipt.operation_id
    assert replacement != old["session_id"]
    native_before = path.read_bytes()
    runtime.leader.core.validate_context = lambda *_: False
    external_forget = Mock(side_effect=AssertionError("The committed memory operation must never replay"))
    knowledge.memory.forget = external_forget
    native_repair = Mock(side_effect=AssertionError("The retired native Session must remain untouched"))
    monkeypatch.setattr("qqbot.adapters.pi.sessions.restore_tool_results", native_repair)
    followup = runtime.event.model_copy(update={"event_id": "new-user-input",
        "segments": (TextSegment(text="继续处理新的问题"),)})
    runtime.sessions.enqueue(followup, runtime.access)
    runtime.clock.advance(31)
    runtime.service.stop_verifier = lambda *_: False
    assert runtime.leader.recover_attempt(old["run_id"])["state"] == "NEEDS_RECONCILIATION"
    assert runtime.sessions.claim("new-session-before-stop") is None
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(runs.c.slot_reserved).where(runs.c.id == old["run_id"])).scalar_one() == 1
        assert conn.execute(select(processes.c.state).where(processes.c.id == process_id)).scalar_one() == "BUSY"
        assert conn.execute(select(dialogue_inputs.c.handled_at).where(dialogue_inputs.c.id == old["source_ids"][0])).scalar_one() is None
        assert not conn.execute(select(outbox)).first()
    runtime.service.stop_verifier = lambda *_: True
    assert runtime.leader.recover_attempt(old["run_id"])["state"] == "FINISHED"
    assert runtime.leader.recover_attempt(old["run_id"])["state"] == "FINISHED"
    assert path.read_bytes() == native_before and len(unfinished_tools(path)) == 1
    with runtime.db.engine.connect() as conn:
        original = conn.execute(select(runs).where(runs.c.id == old["run_id"])).mappings().one()
        assert original["slot_reserved"] == 0 and original["runner_stopped_at"]
        assert conn.execute(select(processes.c.state).where(processes.c.id == process_id)).scalar_one() == "STOPPED"
        assert conn.execute(select(dialogue_inputs.c.handled_at).where(dialogue_inputs.c.id == old["source_ids"][0])).scalar_one()
        confirmations = conn.execute(select(outbox)).mappings().all()
        assert len(confirmations) == 1
        result = json.loads(original["result_json"])
        assert result["agent_authored"] is False
        assert result["summary"] == persisted["memory_mutation"]["summary"]
        assert json.loads(confirmations[0]["envelope_json"])["text"] == result["summary"]
        assert "OLD_PRIVATE_MEMORY" not in confirmations[0]["envelope_json"]
        assert conn.execute(select(dialogues.c.session_id)).scalar_one() == replacement
    next_turn = runtime.sessions.claim("new-session-after-stop")
    assert next_turn["session_id"] == replacement
    assert next_turn["source_ids"] == [followup.event_id]
    external_forget.assert_not_called()
    native_repair.assert_not_called()


def test_retired_memory_writer_blocks_only_its_dialogue_until_stopped(runtime):
    knowledge, scope = memory_runtime(runtime)
    runtime.sessions.enqueue(runtime.event, runtime.access)
    old = runtime.sessions.claim("leader")
    knowledge.rotate(scope, (old["run_id"], old["fencing_token"], "leader"), MutationReceipt(
        operation_id="forget-isolated", state="APPLIED", completed=True, memory_epoch=1))
    waiting = runtime.event.model_copy(update={"event_id": "same-dialogue-after-forget",
        "segments": (TextSegment(text="继续这个对话"),)})
    runtime.sessions.enqueue(waiting, runtime.access)
    assert runtime.sessions.claim("replacement-before-stop") is None
    independent = runtime.event.model_copy(update={"event_id": "independent-dialogue",
        "segments": (TextSegment(text="另一个对话的新问题"),)})
    other = runtime.sessions.enqueue(independent, runtime.access, new_title="独立对话")
    claim = runtime.sessions.claim("independent-leader")
    assert claim["context"]["dialogue_id"] == other["dialogue_id"]
    assert claim["source_ids"] == [independent.event_id]
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(runs.c.slot_reserved).where(runs.c.id == old["run_id"])).scalar_one() == 1
        assert conn.execute(select(dialogue_inputs.c.claimed_attempt_id).where(
            dialogue_inputs.c.id == waiting.event_id)).scalar_one() is None
