"""Original memory sources remain stable across relays, pages and restart."""
import hashlib
import json
from types import SimpleNamespace

import pytest
from sqlalchemy import insert, select, update
from test_agent_runtime import leader_complete, memory_runtime, root_goal
from test_agent_runtime import runtime as agent_runtime

from qqbot.adapters.openviking.service import affected_sources
from qqbot.adapters.pi.sessions import export_session
from qqbot.contracts.memory import MemoryBinding, MutationReceipt
from qqbot.runtime.knowledge import KnowledgeRuntime
from qqbot.storage.memory_sync_schema import memory_barriers, memory_sources, memory_streams
from qqbot.storage.task_schema import observations, sessions, tasks

runtime = agent_runtime


def native_log(path, *, run_id, role, purpose, input_id, summary):
    entries = [
        {"type": "custom", "customType": "elephant-run", "data": {
            "run_id": run_id, "role": role, "purpose": purpose}},
        {"type": "custom", "customType": "elephant-source", "data": {
            "id": input_id, "kind": "user" if purpose == "user" else "relay"}},
        {"type": "message", "message": {"role": "user", "content": f"[[elephant-input:{input_id}]]\n输入资料"}},
        {"type": "custom", "customType": "elephant-consumed", "data": {"run_id": run_id, "input_ids": [input_id]}},
        {"type": "message", "message": {"role": "assistant", "content": [{"type": "text", "text": summary}]}},
        {"type": "custom", "customType": "elephant-turn-boundary", "data": {"run_id": run_id}},
    ]
    for index, entry in enumerate(entries):
        entry.update(id=str(index), parentId=str(index - 1) if index else None)
    path.write_text("".join(json.dumps(entry, ensure_ascii=False) + "\n" for entry in entries))
    return entries


def test_assistant_page_preserves_worker_relay_origin_and_turn_purpose(tmp_path):
    path = tmp_path / "leader.jsonl"
    native_log(path, run_id="leader-run", role="leader", purpose="finalize", input_id="inbox-id", summary="成果转述")
    options = {"session_id": "leader", "input_origins": {"inbox-id": ["result:worker-run"]}}
    exported = export_session(path, **options)
    page = export_session(path, after=4, **options)
    assert page["events"] == exported["events"][1:]
    assert all(event.kind == "relay" and event.origins == ("result:worker-run",) for event in exported["events"])
    legacy = export_session(path, projection_version="pi-original-v1", **options)
    assert legacy["events"][-1].kind == "original"


def test_only_worker_summary_with_frozen_host_evidence_is_a_fact(tmp_path):
    path = tmp_path / "worker.jsonl"
    native_log(path, run_id="work", role="worker", purpose="work", input_id="task-id", summary="已核对值为42")
    turns = {"work": {"run_id": "work", "role": "worker", "purpose": "work", "summary": "已核对值为42",
                      "evidence_refs": ["observation-1"]}}
    exported = export_session(path, session_id="worker", turns=turns)
    fact = exported["events"][-1]
    assert fact.kind == "verified_fact" and fact.origins == ("result:work",)
    assert fact.evidence_refs == ("observation-1",)
    assert export_session(path, session_id="worker")["events"][-1].kind == "relay"


@pytest.mark.parametrize("parts", [["值为", "42"], ["已" * 10000 + "未核实的尾部"]], ids=["multiple-text-parts", "long-output"])
def test_worker_fact_uses_exact_persisted_pi_summary(tmp_path, parts):
    path = tmp_path / "worker.jsonl"
    text = "".join(parts)
    entries = native_log(path, run_id="work", role="worker", purpose="work", input_id="task-id", summary=text)
    entries[4]["message"]["content"] = [{"type": "text", "text": part} for part in parts]
    path.write_text("".join(json.dumps(entry, ensure_ascii=False) + "\n" for entry in entries))
    turns = {"work": {"run_id": "work", "role": "worker", "purpose": "work", "summary": text[:10000],
        "evidence_refs": ["observation-1"]}}
    fact = export_session(path, session_id="worker", turns=turns)["events"][-1]
    assert fact.kind == "verified_fact" and fact.content == text[:10000]
    assert fact.origins == ("result:work",) and fact.evidence_refs == ("observation-1",)
    assert "未核实的尾部" not in fact.content
    legacy = export_session(path, session_id="worker", projection_version="pi-original-v1")["events"][-1]
    assert legacy.content == "\n".join(parts)
    turns["work"]["summary"] = "不同的宿主回执"
    assert export_session(path, session_id="worker", turns=turns)["events"][-1].kind == "relay"


def test_rebound_resume_prompt_keeps_already_persisted_input_origins(tmp_path):
    path = tmp_path / "resumed.jsonl"
    entries = native_log(path, run_id="leader-run", role="leader", purpose="finalize",
        input_id="inbox-id", summary="恢复后的成果转述")
    entries[0]["data"]["source_event_ids"] = ["inbox-id"]
    entries[2]["message"]["content"] = "恢复已经持久化但尚未结算的轮次"
    path.write_text("".join(json.dumps(entry, ensure_ascii=False) + "\n" for entry in entries))
    options = {"session_id": "leader", "input_origins": {"inbox-id": ["result:worker-run"]}}
    page = export_session(path, after=4, **options)
    assert len(page["events"]) == 1
    assert page["events"][0].kind == "relay" and page["events"][0].origins == ("result:worker-run",)


@pytest.mark.asyncio
async def test_runtime_syncs_worker_evidence_and_leader_relay_with_same_source(runtime):
    root_goal(runtime)
    worker = runtime.service.claim("worker")
    summary = "已核对项目事实42"
    with runtime.db.transaction() as conn:
        conn.execute(insert(observations).values(id="worker-proof", task_id=worker["task_id"],
            run_id=worker["run_id"], retrieved_at=runtime.clock.now().isoformat(), sha256="a" * 64,
            data_json=json.dumps({"value": 42})))
        conn.execute(insert(observations).values(id="upstream-proof", task_id=worker["task_id"],
            run_id="old-worker-run", retrieved_at=runtime.clock.now().isoformat(), sha256="b" * 64,
            data_json=json.dumps({"value": 41})))
    receipt = {"kind": "turn_finished", "session_id": worker["context"]["session_id"], "run_id": worker["run_id"],
        "session_revision": "leaf", "input_ids": worker["context"]["source_input_ids"], "inflight": 0, "tools_paired": True}
    runtime.service.submit(worker["run_id"], worker["fencing_token"], "worker",
        {"status": "completed", "summary": summary, "observation_refs": ["worker-proof", "upstream-proof"]}, receipt=receipt)
    leader = runtime.sessions.claim("leader")
    leader_complete(runtime, leader, summary="项目事实42，已完成")
    for claim in (worker, leader):
        session_id = claim.get("session_id") or claim["context"]["session_id"]
        path = runtime.path / (session_id + ".jsonl")
        native_log(path, run_id=claim["run_id"], role=claim["role"], purpose=claim["purpose"],
            input_id=claim["context"]["source_input_ids"][0],
            summary=summary if claim["role"] == "worker" else "项目事实42，已完成")
        with runtime.db.transaction() as conn:
            conn.execute(insert(sessions).values(id=session_id, scope_key=session_id, session_file=str(path),
                manifest_json="{}", active_run_id=None, epoch=0))
    binding = MemoryBinding(owner_id=runtime.sessions.settings.owner.id, account_id="test", user_id="test", api_key_env="UNUSED")

    class Memory:
        config = SimpleNamespace(bindings=(binding,))

        def __init__(self):
            self.batches = []

        def enabled(self, _scope):
            return True

        async def sync(self, batch):
            self.batches.append(batch)

        async def reconcile(self):
            pass

    core = runtime.leader.core
    core.sessions, core.openviking = runtime.sessions, Memory()
    knowledge = KnowledgeRuntime(core)
    with runtime.db.transaction() as conn:
        conn.execute(insert(memory_streams).values(id="old-worker-projection", scope_id=knowledge.scope(runtime.event).key,
            session_id=worker["context"]["session_id"], branch="main", projection_version="pi-original-v1",
            epoch=0, exported_seq=999, extracted_seq=999, indexed_seq=999))
    await knowledge.sync()
    assert len(core.openviking.batches) == 2
    facts = [event for batch in core.openviking.batches for event in batch.events if event.kind == "verified_fact"]
    assert len(facts) == 1 and facts[0].origins == ("result:" + worker["run_id"],)
    assert facts[0].evidence_refs == ("worker-proof",)
    relays = [event for batch in core.openviking.batches if batch.session_id == leader["session_id"] for event in batch.events]
    assert all(event.kind == "relay" and event.origins == facts[0].origins for event in relays)
    with runtime.db.engine.connect() as conn:
        assert len(conn.execute(select(sessions)).all()) == 2


@pytest.mark.parametrize("entry", ["active-mutation", "restored-ledger"])
async def test_frozen_shared_source_is_removed_from_replanned_goal(runtime, entry):
    knowledge, scope = memory_runtime(runtime)
    if entry == "active-mutation":
        runtime.sessions.enqueue(runtime.event, runtime.access)
        claim = runtime.sessions.claim("leader")
    root_id = root_goal(runtime)["task_id"]
    with runtime.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.root_task_id == root_id).values(
            context_json=json.dumps({"memory_scope": scope.model_dump(), "memory_epoch": 0})))
        conn.execute(insert(memory_sources).values(id="frozen-origin", scope_id=scope.key,
            source_id=runtime.event.event_id, revision="1", event_id="native-user", operation_id="old-batch",
            content_hash=hashlib.sha256(runtime.event.text.encode()).hexdigest(), state="FROZEN"))
        conn.execute(insert(memory_barriers).values(id="uri-only-barrier", scope_id=scope.key,
            epoch=1, operation_id="forget-a", source_id=None,
            uri="viking://user/owner/memories/entities/a.md", created_at=runtime.clock.now().isoformat()))
    if entry == "active-mutation":
        receipt = MutationReceipt(operation_id="forget-a", state="APPLIED", completed=True, memory_epoch=1,
            affected_source_ids=affected_sources({"source_ids": [], "frozen_source_ids": [runtime.event.event_id]}))
        knowledge.rotate(scope, (claim["run_id"], claim["fencing_token"], "leader"), receipt)
        leader_complete(runtime, claim)
    else:
        # Restored ledgers carry URI barriers and FROZEN receipts without mutation operations.
        await knowledge.replan()
    await knowledge.replan()
    with runtime.db.engine.connect() as conn:
        root = conn.execute(select(tasks).where(tasks.c.id == root_id)).mappings().one()
    assert root["state"] == "BLOCKED" and root["stop_reason"] == "business_source_revoked"
    assert runtime.event.text not in root["goal"] and not knowledge.memory.search_calls
    assert json.loads(root["context_json"])["memory_replan"]["source_ids"] == [runtime.event.event_id]


async def test_pending_sync_operations_drain_even_when_new_export_queue_is_full(runtime):
    from unittest.mock import AsyncMock

    runtime.sessions.enqueue(runtime.event, runtime.access)
    claim = runtime.sessions.claim("leader")
    leader_complete(runtime, claim)
    path = runtime.path / "source.jsonl"
    native_log(path, run_id=claim["run_id"], role="leader", purpose="user",
        input_id=runtime.event.event_id, summary="收到")
    with runtime.db.transaction() as conn:
        conn.execute(insert(sessions).values(id=claim["session_id"], scope_key=claim["session_id"],
            session_file=str(path), manifest_json="{}", active_run_id=None, epoch=0))
    binding = MemoryBinding(owner_id=runtime.sessions.settings.owner.id, account_id="test", user_id="test", api_key_env="UNUSED")
    memory = SimpleNamespace(config=SimpleNamespace(bindings=(binding,)), enabled=lambda _: True,
        sync=AsyncMock(side_effect=ValueError("memory synchronization queue is full")), reconcile=AsyncMock())
    core = runtime.leader.core
    core.sessions, core.openviking = runtime.sessions, memory
    knowledge = KnowledgeRuntime(core)
    with pytest.raises(ValueError, match="queue is full"):
        await knowledge.sync()
    memory.reconcile.assert_awaited_once()
    memory.sync.assert_awaited_once()
