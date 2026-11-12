import json
from types import SimpleNamespace

import pytest
from sqlalchemy import select, update

from qqbot.clock import FakeClock
from qqbot.config import Settings
from qqbot.domain.messages import (
    AccessContext,
    ConversationRef,
    InboundEvent,
    PrincipalRef,
    Surface,
    TextSegment,
)
from qqbot.jobs.service import TaskService
from qqbot.outbox.service import Outbox
from qqbot.runtime.leader import LeaderRuntime
from qqbot.runtime.sessions import SessionService
from qqbot.runtime.task_graph import TaskGraph
from qqbot.storage.agent_schema import dialogue_inputs, leader_inbox
from qqbot.storage.db import Database
from qqbot.storage.schema import metadata, outbox
from qqbot.storage.task_schema import inputs, runs, tasks


@pytest.fixture
def runtime(tmp_path):
    db = Database(str(tmp_path / "agent.db"))
    metadata.create_all(db.engine)
    clock = FakeClock()
    settings = Settings(owner={"qq_bindings": (
        {"app_id": "app", "user_openid": "owner", "verified_at": clock.now().isoformat()}
    ,)})
    service = TaskService(db, clock, Outbox(db, clock), stop_verifier=lambda *_: True)
    service.graph = TaskGraph(service, settings)
    sessions = SessionService(service, settings)
    core = SimpleNamespace(tasks=service, db=db, clock=clock, outbox=service.outbox, allowed=lambda _: True)
    leader = LeaderRuntime(core, sessions)
    event = InboundEvent(event_id="input-1", event_type="message.created",
        conversation=ConversationRef(deployment_id="d", adapter_instance_id="a", bot_identity_id="b",
            surface=Surface.PRIVATE, remote_conversation_id="c"),
        principal=PrincipalRef(adapter_instance_id="a", identity_namespace="qq_app:app", remote_user_id="owner"),
        platform_message_id="m", occurred_at=clock.now(), received_at=clock.now(), trace_id="trace", segments=(TextSegment(text="检查项目"),))
    access = AccessContext(principal=event.principal, conversation=event.conversation, role="owner")
    yield SimpleNamespace(service=service, sessions=sessions, leader=leader, event=event,
        access=access, db=db, clock=clock, path=tmp_path)
    db.close()


def root_goal(runtime, key="dispatch-1", children=1):
    with runtime.db.transaction() as conn:
        dialogue = runtime.sessions.dialogue(conn, runtime.event)
        result = runtime.service.graph.submit(conn, dialogue, runtime.event, runtime.access,
            {"goal": "检查项目", "children": [
                {"goal": f"检查部分 {index}", "delegation_key": str(index)} for index in range(children)
            ]}, key, runtime.sessions.binding(), {})
    return result


def worker_complete(runtime):
    claim = runtime.service.claim("worker")
    assert claim is not None
    receipt = {"kind": "turn_finished", "session_id": claim["context"]["session_id"],
        "run_id": claim["run_id"], "session_revision": "leaf", "input_ids": claim["context"]["source_input_ids"],
        "inflight": 0, "tools_paired": True}
    runtime.service.submit(claim["run_id"], claim["fencing_token"], "worker",
        {"status": "completed", "summary": "检查完成"}, receipt=receipt)
    return claim


def leader_complete(runtime, claim, summary="已经完成"):
    receipt = {"kind": "turn_finished", "session_id": claim["session_id"], "run_id": claim["run_id"],
        "session_revision": "leaf", "input_ids": claim["context"]["source_input_ids"],
        "inflight": 0, "tools_paired": True}
    return runtime.leader.complete(claim["run_id"], claim["fencing_token"], "leader",
        {"status": "completed", "summary": summary}, stopped=True, quiescent=False, receipt=receipt)


def test_dispatch_replay_conflict_and_worker_results_finalize_once(runtime):
    root = root_goal(runtime, children=2)
    assert root_goal(runtime, children=2)["task_id"] == root["task_id"]
    with pytest.raises(ValueError, match="dispatch key"):
        root_goal(runtime, children=1)
    worker_complete(runtime)
    assert runtime.sessions.claim("leader") is None
    worker_complete(runtime)
    claim = runtime.sessions.claim("leader")
    assert claim["purpose"] == "finalize"
    assert runtime.sessions.claim("second-leader") is None
    leader_complete(runtime, claim)
    leader_complete(runtime, claim)
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == root["task_id"])).scalar_one() == "SUCCEEDED"
        assert len(conn.execute(select(outbox)).all()) == 1
        assert all(conn.execute(select(leader_inbox.c.handled_at)).scalars())


def test_root_followup_after_children_settle_reaches_finalize_and_is_consumed(runtime):
    root = root_goal(runtime)
    worker_complete(runtime)
    with runtime.db.transaction() as conn:
        row = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
        runtime.service.add_graph_input(conn, row,
            runtime.event.model_copy(update={"event_id": "revision", "segments": (TextSegment(text="只给摘要"),)}),
            runtime.access, "只给摘要")
    claim = runtime.sessions.claim("leader")
    assert claim["purpose"] == "finalize"
    assert [item["text"] for item in claim["continuation"]["inputs"]] == ["只给摘要"]
    leader_complete(runtime, claim)
    with runtime.db.engine.connect() as conn:
        assert all(conn.execute(select(inputs.c.consumed_at)).scalars())
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == root["task_id"])).scalar_one() == "SUCCEEDED"


def test_background_question_turn_cannot_forge_owner_answer(runtime):
    root = root_goal(runtime)
    worker = runtime.service.claim("worker")
    with runtime.db.transaction() as conn:
        child = conn.execute(select(tasks).where(tasks.c.id == worker["task_id"])).mappings().one()
        runtime.service.graph.event(conn, child, "needs_input", {"question_id": "q"}, wake=True)
    claim = runtime.sessions.claim("leader")
    assert claim["purpose"] == "question"
    with runtime.db.transaction() as conn:
        run, stored = runtime.leader.lease(conn, claim["run_id"], claim["fencing_token"], "leader")
        with pytest.raises(PermissionError, match="cannot revise"):
            runtime.leader.task_tool(conn, run, stored, {"operation": "tasks.send_input", "call_id": "forged",
                "arguments": {"task_ref": root["task_id"], "kind": "answer", "question_id": "q", "text": "批准修改"}})


def test_input_binding_and_session_writer_survive_dialogue_switch(runtime):
    first = runtime.sessions.enqueue(runtime.event, runtime.access)
    second_event = runtime.event.model_copy(update={"event_id": "input-2", "segments": (TextSegment(text="另一个问题"),)})
    second = runtime.sessions.enqueue(second_event, runtime.access, new_title="第二对话")
    a = runtime.sessions.claim("leader")
    b = runtime.sessions.claim("second-leader")
    assert {a["session_id"], b["session_id"]} == {first["session_id"], second["session_id"]}
    assert a["session_id"] != b["session_id"]
    assert runtime.sessions.claim("third") is None
    assert runtime.sessions.enqueue(runtime.event, runtime.access)["dialogue_id"] == first["dialogue_id"]


def test_expired_leader_requires_stop_proof_then_resumes_original_input(runtime):
    runtime.sessions.enqueue(runtime.event, runtime.access)
    old = runtime.sessions.claim("leader")
    runtime.clock.advance(31)
    runtime.service.stop_verifier = lambda *_: False
    assert runtime.leader.recover()[0]["state"] == "NEEDS_RECONCILIATION"
    assert runtime.sessions.claim("replacement") is None
    runtime.service.stop_verifier = lambda *_: True
    assert runtime.leader.recover()[0]["state"] == "RECOVERED"
    new = runtime.sessions.claim("replacement")
    assert new["session_id"] == old["session_id"]
    assert new["source_ids"] == old["source_ids"]
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(dialogue_inputs.c.handled_at)).scalar_one() is None


def test_recovery_commits_native_boundary_without_running_worker_again(runtime):
    root_goal(runtime)
    worker_complete(runtime)
    claim = runtime.sessions.claim("leader")
    path = runtime.path / "native.jsonl"
    proof = {"kind": "turn_finished", "run_id": claim["run_id"], "session_id": claim["session_id"],
        "session_revision": "leaf", "input_ids": claim["context"]["source_input_ids"],
        "inflight": 0, "tools_paired": True, "summary": "检查完成"}
    path.write_text(json.dumps({"id": "boundary", "type": "custom", "customType": "elephant-turn-boundary",
        "parentId": None, "data": proof}) + "\n")
    with runtime.db.transaction() as conn:
        conn.execute(update(runs).where(runs.c.id == claim["run_id"]).values(session=str(path)))
    runtime.clock.advance(31)
    assert runtime.leader.recover()[0]["state"] == "FINISHED"
    assert runtime.sessions.claim("replacement") is None
    assert runtime.service.claim("replacement-worker") is None
    with runtime.db.engine.connect() as conn:
        assert len(conn.execute(select(outbox)).all()) == 1


def test_cancelled_root_rejects_old_finalizer_and_keeps_worker_settled(runtime):
    root = root_goal(runtime)
    worker_complete(runtime)
    claim = runtime.sessions.claim("leader")
    with runtime.db.transaction() as conn:
        task = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
        runtime.service.graph.cancel_tree(conn, task)
    with pytest.raises(PermissionError, match="no longer current"):
        leader_complete(runtime, claim)
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == root["task_id"])).scalar_one() == "CANCEL_REQUESTED"
        assert len(conn.execute(select(outbox)).all()) == 0


def test_host_failure_does_not_handle_or_discard_accepted_input(runtime):
    runtime.sessions.enqueue(runtime.event, runtime.access)
    claim = runtime.sessions.claim("leader")
    runtime.leader.complete(claim["run_id"], claim["fencing_token"], "leader",
        {"status": "failed", "summary": "transport failed", "agent_authored": False}, stopped=True, quiescent=False)
    retry = runtime.sessions.claim("retry")
    assert retry["source_ids"] == claim["source_ids"]
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(dialogue_inputs.c.handled_at)).scalar_one() is None
        assert len(conn.execute(select(outbox)).all()) == 0


def test_revoked_context_rejects_further_leader_tools(runtime):
    runtime.sessions.enqueue(runtime.event, runtime.access)
    claim = runtime.sessions.claim("leader")
    runtime.leader.core.validate_context = lambda *_: False
    with runtime.db.transaction() as conn:
        run, stored = runtime.leader.lease(conn, claim["run_id"], claim["fencing_token"], "leader")
        with pytest.raises(PermissionError, match="context sources"):
            runtime.leader.authorize(conn, run, stored, {"action": "model.request"})


def test_root_unknown_success_check_cannot_be_declared_complete(runtime):
    root = root_goal(runtime)
    worker_complete(runtime)
    with runtime.db.transaction() as conn:
        row = conn.execute(select(tasks.c.goal_json).where(tasks.c.id == root["task_id"])).scalar_one()
        goal = json.loads(row)
        goal["success_checks"] = ["unsupported-verifier"]
        conn.execute(update(tasks).where(tasks.c.id == root["task_id"]).values(goal_json=json.dumps(goal)))
    claim = runtime.sessions.claim("leader")
    with pytest.raises(ValueError, match="root success check not verified"):
        leader_complete(runtime, claim)


def test_memory_mutation_ends_old_context_with_the_durable_host_receipt(runtime):
    runtime.sessions.enqueue(runtime.event, runtime.access)
    claim = runtime.sessions.claim("leader")
    with runtime.db.transaction() as conn:
        stored = json.loads(conn.execute(select(runs.c.request_json).where(runs.c.id == claim["run_id"])).scalar_one())
        stored["memory_mutation"] = {"summary": "已忘记指定资料。", "operation_id": "forget-1", "completed": True}
        conn.execute(update(runs).where(runs.c.id == claim["run_id"]).values(request_json=json.dumps(stored)))
    runtime.leader.core.validate_context = lambda *_: False
    result = runtime.leader.complete(claim["run_id"], claim["fencing_token"], "leader",
        {"status": "failed", "summary": "model failed", "agent_authored": False}, stopped=True, quiescent=False)
    assert result["state"] == "FINISHED"
    assert runtime.sessions.claim("replacement") is None
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(dialogue_inputs.c.handled_at)).scalar_one()
        assert "已忘记指定资料" in conn.execute(select(outbox.c.envelope_json)).scalar_one()


def test_cancel_waits_for_background_jobs_even_without_an_active_pi_attempt(runtime):
    root = root_goal(runtime)
    runtime.service.job_tree_quiescent = lambda _task: False
    with runtime.db.transaction() as conn:
        row = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
        runtime.service.graph.cancel_tree(conn, row)
    with runtime.db.engine.connect() as conn:
        assert set(conn.execute(select(tasks.c.state)).scalars()) == {"CANCEL_REQUESTED"}


def native_source_log(runtime, claim):
    proof = {"kind": "turn_finished", "session_id": claim["session_id"], "run_id": claim["run_id"],
        "session_revision": "final", "input_ids": claim["context"]["source_input_ids"],
        "inflight": 0, "tools_paired": True, "summary": "检查完成"}
    entries = [
        {"id": "user", "parentId": None, "type": "message", "message": {
            "role": "user", "content": [{"type": "text", "text": "检查项目"}]}},
        {"id": "final", "parentId": "user", "type": "message", "message": {
            "role": "assistant", "content": [{"type": "text", "text": "检查完成"}]}},
        {"id": "boundary", "parentId": "final", "type": "custom", "customType": "elephant-turn-boundary", "data": proof},
        {"id": "later", "parentId": "boundary", "type": "message", "message": {
            "role": "user", "content": [{"type": "text", "text": "之后的新消息"}]}},
    ]
    path = runtime.path / (claim["run_id"] + ".jsonl")
    path.write_text("".join(json.dumps(entry, ensure_ascii=False) + "\n" for entry in entries))
    with runtime.db.transaction() as conn:
        conn.execute(update(runs).where(runs.c.id == claim["run_id"]).values(session=str(path)))
    return path


def enable_short_learning_interval(runtime):
    settings = runtime.sessions.settings
    settings = settings.model_copy(update={"skills": settings.skills.model_copy(update={
        "hermes": settings.skills.hermes.model_copy(update={"creation_nudge_interval": 2})})})
    runtime.sessions.settings = settings
    runtime.sessions.learning.settings = settings


async def reserve_iteration(runtime, claim, request_id):
    identity = (claim["run_id"], claim["fencing_token"], "leader")
    payload = {"request_id": request_id, "kind": "business"}
    result = await runtime.leader.invoke("reserve-budget", identity, payload)
    runtime.leader.observe_operation(identity, "reserve-budget", payload, result)


async def prepare_conditional_review(runtime):
    enable_short_learning_interval(runtime)
    runtime.sessions.enqueue(runtime.event, runtime.access)
    source = runtime.sessions.claim("leader")
    await reserve_iteration(runtime, source, "model-1")
    await reserve_iteration(runtime, source, "model-1")
    await reserve_iteration(runtime, source, "model-2")
    native_source_log(runtime, source)
    leader_complete(runtime, source)
    identity = (source["run_id"], source["fencing_token"], "leader")
    runtime.leader.observe_operation(identity, "result", {})
    runtime.leader.observe_operation(identity, "result", {})
    return source


async def test_conditional_review_is_durable_per_agent_and_uses_a_frozen_snapshot(runtime):
    from qqbot.storage.learning_schema import learning_events, learning_sessions

    source = await prepare_conditional_review(runtime)
    with runtime.db.engine.connect() as conn:
        state = json.loads(conn.execute(select(learning_sessions.c.state_json)).scalar_one())
        assert state["iterations_since_skill"] == 0
        assert state["review_pending"] is True
        assert len(conn.execute(select(learning_events)).all()) == 3
    restored_sessions = SessionService(runtime.service, runtime.sessions.settings)
    restored = LeaderRuntime(runtime.leader.core, restored_sessions)
    review = restored.claim_review("leader")
    assert review["role"] == "skill_review"
    assert review["session_id"] != source["session_id"]
    assert "检查项目" in review["goal"]
    assert "之后的新消息" not in review["goal"]
    assert restored.claim_review("another") is None
    assert review["plan"]["budget"]["model_calls"] == 16
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(learning_sessions.c.claimed_attempt_id)).scalar_one() == review["run_id"]


async def test_review_tools_share_read_marks_enforce_origin_and_cannot_recurse(runtime):
    from qqbot.adapters.hermes_skills import HermesSkills
    from qqbot.runtime.tools import native_tools
    from qqbot.storage.learning_schema import learning_sessions

    await prepare_conditional_review(runtime)
    review = runtime.leader.claim_review("leader")
    assert set(native_tools("skill_review", "skill_review")) == {"skills_list", "skill_view", "skill_manage"}
    adapter = HermesSkills(runtime.path / "owner")
    content = "---\nname: sample\ndescription: Test reusable workflow\n---\nRead the source.\n"
    assert adapter.skill_manage({"operations": [{"action": "create", "name": "sample", "content": content}]})["success"]
    runtime.service.skills_provider = lambda _access: adapter
    identity = (review["run_id"], review["fencing_token"], "leader")
    patch = {"operation": "skill_manage", "arguments": {"operations": [{"action": "patch", "name": "sample",
        "old_string": "Read the source.", "new_string": "Read and verify the source."}]}}
    refused = await runtime.leader.invoke("skills", identity, patch)
    assert not refused["success"]
    assert (await runtime.leader.invoke("skills", identity, {"operation": "skill_view", "arguments": {"name": "sample"}}))["success"]
    assert (await runtime.leader.invoke("skills", identity, patch))["success"]
    denied = await runtime.leader.invoke("skills", identity, {"operation": "skill_manage",
        "arguments": {"operations": [{"action": "delete", "name": "sample"}]}})
    assert not denied["success"]
    assert next(item for item in adapter.ledger(skill="sample") if item["action"] == "patch")["actor"] == "curator"
    with runtime.db.transaction() as conn:
        run, stored = runtime.leader.lease(conn, *identity)
        with pytest.raises(PermissionError, match="cannot execute business"):
            runtime.leader.authorize(conn, run, stored, {"action": "github.issue.create", "effect": "write"})
    await reserve_iteration(runtime, review, "review-model")
    leader_complete(runtime, review, "Nothing to save.")
    runtime.leader.observe_operation(identity, "result", {})
    assert runtime.leader.claim_review("next") is None
    with runtime.db.engine.connect() as conn:
        state = json.loads(conn.execute(select(learning_sessions.c.state_json)).scalar_one())
        assert state["review_pending"] is False
        assert state["iterations_since_skill"] == 0


async def test_worker_iterations_do_not_accumulate_on_the_owner_leader(runtime):
    from qqbot.storage.learning_schema import learning_sessions

    enable_short_learning_interval(runtime)
    root_goal(runtime)
    worker = runtime.service.claim("worker")
    runtime.sessions.enqueue(runtime.event.model_copy(update={"event_id": "chat"}), runtime.access)
    leader = runtime.sessions.claim("leader")
    await reserve_iteration(runtime, leader, "leader-model")
    worker_identity = (worker["run_id"], worker["fencing_token"], "worker")
    for index in range(2):
        payload = {"request_id": "worker-model-" + str(index), "kind": "business"}
        result = runtime.service.reserve_budget(*worker_identity, payload)
        runtime.leader.observe_operation(worker_identity, "reserve-budget", payload, result)
    receipt = {"kind": "turn_finished", "session_id": worker["context"]["session_id"],
        "run_id": worker["run_id"], "session_revision": "leaf", "input_ids": worker["context"]["source_input_ids"],
        "inflight": 0, "tools_paired": True}
    runtime.service.submit(*worker_identity, {"status": "completed", "summary": "检查完成"}, receipt=receipt)
    runtime.leader.observe_operation(worker_identity, "result", {})
    with runtime.db.engine.connect() as conn:
        rows = {row["session_id"]: json.loads(row["state_json"])
            for row in conn.execute(select(learning_sessions)).mappings()}
        assert rows[leader["session_id"]]["iterations_since_skill"] == 1
        assert rows[leader["session_id"]]["review_pending"] is False
        assert rows[worker["context"]["session_id"]]["iterations_since_skill"] == 0
        assert rows[worker["context"]["session_id"]]["review_pending"] is True


async def test_foreground_skill_manage_resets_the_source_counter(runtime):
    from qqbot.adapters.hermes_skills import HermesSkills
    from qqbot.storage.learning_schema import learning_sessions

    enable_short_learning_interval(runtime)
    runtime.sessions.enqueue(runtime.event, runtime.access)
    claim = runtime.sessions.claim("leader")
    identity = (claim["run_id"], claim["fencing_token"], "leader")
    await reserve_iteration(runtime, claim, "model-1")
    adapter = HermesSkills(runtime.path / "owner")
    runtime.service.skills_provider = lambda _access: adapter
    payload = {"call_id": "create-skill", "operation": "skill_manage", "arguments": {"operations": [{
        "action": "create", "name": "sample", "content": "---\nname: sample\ndescription: Test workflow\n---\nRead first.\n"}]}}
    result = await runtime.leader.invoke("skills", identity, payload)
    assert result["success"]
    runtime.leader.observe_operation(identity, "skills", payload, result)
    runtime.leader.observe_operation(identity, "skills", payload, result)
    await reserve_iteration(runtime, claim, "model-2")
    leader_complete(runtime, claim)
    runtime.leader.observe_operation(identity, "result", {})
    with runtime.db.engine.connect() as conn:
        state = json.loads(conn.execute(select(learning_sessions.c.state_json)).scalar_one())
        assert state["iterations_since_skill"] == 1
        assert state["review_pending"] is False


async def test_crashed_review_resumes_its_own_session_and_preserves_pending(runtime):
    from qqbot.storage.learning_schema import learning_sessions

    source = await prepare_conditional_review(runtime)
    old = runtime.leader.claim_review("leader")
    runtime.clock.advance(31)
    assert runtime.leader.recover()[0]["state"] == "RECOVERED"
    new = runtime.leader.claim_review("replacement")
    assert new["session_id"] == old["session_id"]
    assert new["session_id"] != source["session_id"]
    assert new["source_ids"] == old["source_ids"]
    with runtime.db.engine.connect() as conn:
        assert json.loads(conn.execute(select(learning_sessions.c.state_json)).scalar_one())["review_pending"] is True


def test_learning_tables_are_created_by_the_normal_migration(db):
    from qqbot.storage.learning_schema import learning_sessions

    with db.engine.connect() as conn:
        assert conn.execute(select(learning_sessions)).all() == []


def test_same_input_id_cannot_be_replayed_with_another_authenticated_subject(runtime):
    runtime.sessions.enqueue(runtime.event, runtime.access)
    principal = runtime.event.principal.model_copy(update={"remote_user_id": "someone-else"})
    forged = runtime.event.model_copy(update={"principal": principal})
    access = runtime.access.model_copy(update={"principal": principal})
    with pytest.raises(ValueError, match="conflicting dialogue input"):
        runtime.sessions.enqueue(forged, access)


def memory_runtime(runtime):
    from sqlalchemy import insert

    from qqbot.contracts.memory import MemoryBinding, MemoryScope, MemorySearchResult
    from qqbot.runtime.knowledge import KnowledgeRuntime
    from qqbot.storage.memory_sync_schema import memory_owners

    scope = MemoryScope(owner_id=runtime.sessions.settings.owner.id)
    binding = MemoryBinding(owner_id=scope.owner_id, account_id="elephant", user_id="owner", api_key_env="UNUSED")

    class Memory:
        config = SimpleNamespace(bindings=(binding,), max_query_chars=1000)

        def enabled(self, _scope):
            return True

        async def status(self, _scope):
            return {"state": "READY", "memory_epoch": 1}

        async def search(self, _scope, _query):
            return MemorySearchResult(status="ready", memory_epoch=1)

    core = runtime.leader.core
    core.sessions, core.openviking, core.scheduler = runtime.sessions, Memory(), None
    knowledge = KnowledgeRuntime(core)
    with runtime.db.transaction() as conn:
        conn.execute(insert(memory_owners).values(id=scope.key, scope_json=scope.model_dump_json(),
            binding_json=binding.model_dump_json(), binding_hash="fixture", remote_identity="fixture", epoch=1,
            state="READY", updated_at=runtime.clock.now().isoformat()))
    return knowledge, scope


async def test_memory_change_cleanly_replans_linked_work_and_fences_queued_delivery(runtime):
    from qqbot.contracts.memory import MutationReceipt
    from qqbot.storage.task_schema import notifications

    knowledge, scope = memory_runtime(runtime)
    runtime.sessions.enqueue(runtime.event, runtime.access)
    user_claim = runtime.sessions.claim("leader")
    root_id = root_goal(runtime)["task_id"]
    with runtime.db.transaction() as conn:
        rows = conn.execute(select(tasks).where(tasks.c.root_task_id == root_id)).mappings().all()
        child_id = next(row["id"] for row in rows if row["id"] != root_id)
        old_context = {"memory_scope": scope.model_dump(), "memory_epoch": 0,
            "retrieved_data": [{"content": "FORGOTTEN_SECRET"}], "source_manifest": [{"id": runtime.event.event_id}],
            "dispatch_spec": {"goal": "FORGOTTEN_SECRET"}}
        conn.execute(update(tasks).where(tasks.c.root_task_id == root_id).values(
            context_json=json.dumps(old_context), goal="FORGOTTEN_SECRET"))
        root = conn.execute(select(tasks).where(tasks.c.id == root_id)).mappings().one()
        runtime.service._notification(conn, root, user_claim["run_id"], "FORGOTTEN_SECRET", "result", "important", "old-result")
    receipt = MutationReceipt(operation_id="forget-1", state="APPLIED", completed=True,
        memory_epoch=1, affected_source_ids=("memory-source",))
    knowledge.rotate(scope, (user_claim["run_id"], 1, "leader"), receipt)
    await knowledge.replan()
    with runtime.db.engine.connect() as conn:
        root = conn.execute(select(tasks).where(tasks.c.id == root_id)).mappings().one()
        assert "memory_replan_pending" in json.loads(root["context_json"])
        assert conn.execute(select(outbox.c.state)).scalar_one() == "FAILED"
        assert conn.execute(select(notifications.c.state)).scalar_one() == "CANCELLED"
    leader_complete(runtime, user_claim)
    runtime.service.job_tree_quiescent = lambda _: False
    await knowledge.replan()
    with runtime.db.engine.connect() as conn:
        assert "memory_replan_pending" in json.loads(conn.execute(select(tasks.c.context_json).where(tasks.c.id == root_id)).scalar_one())
    runtime.service.job_tree_quiescent = lambda _: True
    await knowledge.replan()
    claim = runtime.sessions.claim("leader")
    assert claim["purpose"] == "finalize" and claim["target_root_task_id"] == root_id
    assert claim["session_id"] != user_claim["session_id"]
    assert "FORGOTTEN_SECRET" not in json.dumps(claim)
    assert claim["context"]["memory_epoch"] == 1
    with runtime.db.transaction() as conn:
        root = conn.execute(select(tasks).where(tasks.c.id == root_id)).mappings().one()
        assert not knowledge.delivery_valid(conn, root, {"run_id": user_claim["run_id"], "state": "ACCEPTED"})
        with pytest.raises(PermissionError, match="linked rework"):
            runtime.service.graph.spawn(conn, root, {"goal": "重新检查", "delegation_key": "new"}, runtime.access)
        new = runtime.service.graph.spawn(conn, root, {"goal": "重新检查", "delegation_key": "new",
            "rework_of_task_id": child_id}, runtime.access)
        child = conn.execute(select(tasks).where(tasks.c.id == new["task_id"])).mappings().one()
        assert child["rework_of_task_id"] == child_id
        assert json.loads(child["context_json"])["memory_epoch"] == 1
        assert "FORGOTTEN_SECRET" not in json.dumps(runtime.service.graph.status(conn, root))


async def test_memory_change_blocks_replanning_when_original_business_source_is_forgotten(runtime):
    from qqbot.contracts.memory import MutationReceipt

    knowledge, scope = memory_runtime(runtime)
    runtime.sessions.enqueue(runtime.event, runtime.access)
    claim = runtime.sessions.claim("leader")
    root_id = root_goal(runtime)["task_id"]
    with runtime.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.root_task_id == root_id).values(
            context_json=json.dumps({"memory_scope": scope.model_dump(), "memory_epoch": 0})))
    knowledge.rotate(scope, (claim["run_id"], 1, "leader"), MutationReceipt(operation_id="forget-business",
        state="APPLIED", completed=True, memory_epoch=1, affected_source_ids=(runtime.event.event_id,)))
    leader_complete(runtime, claim)
    await knowledge.replan()
    with runtime.db.engine.connect() as conn:
        root = conn.execute(select(tasks).where(tasks.c.id == root_id)).mappings().one()
        assert root["state"] == "BLOCKED" and root["stop_reason"] == "business_source_revoked"
        assert not conn.execute(select(leader_inbox).where(leader_inbox.c.handled_at.is_(None))).first()
