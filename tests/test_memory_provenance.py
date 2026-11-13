"""Original memory sources remain stable across relays, pages and restart."""
import hashlib
import json
from types import SimpleNamespace

import pytest
from sqlalchemy import insert, select, update
from test_agent_runtime import leader_complete, memory_runtime, root_goal
from test_agent_runtime import runtime as agent_runtime

from qqbot.adapters.openviking.service import OpenVikingMemory, affected_sources
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


@pytest.mark.parametrize("validator_owner", ["core", "tasks"])
async def test_runtime_syncs_worker_evidence_and_leader_relay_with_same_source(runtime, validator_owner):
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
    allow_source = True
    def validate_context(access, context):
        assert access.principal == runtime.access.principal
        return allow_source or not context.get("task_sources")
    if validator_owner == "core":
        core.validate_context = validate_context
        runtime.service.context_validator = lambda *_: True
    else:
        runtime.service.context_validator = validate_context
    with runtime.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.id == worker["task_id"]).values(
            context_json=json.dumps({"task_sources": ["handoff-source"]})))
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
    accepted = next(batch for batch in core.openviking.batches if batch.session_id == worker["session_id"])
    expected_refs = [OpenVikingMemory._source_ref(event) for event in accepted.events]
    allow_source = False
    with runtime.db.engine.connect() as conn:
        completed = conn.execute(select(tasks).where(tasks.c.id == worker["task_id"])).mappings().one()
    assert not validate_context(runtime.service._access_for(completed), json.loads(completed["context_json"]))
    restored = knowledge.load_source(accepted.scope, accepted.session_id, accepted.from_seq, accepted.to_seq,
        accepted.branch, accepted.projection_version)
    assert restored == accepted
    assert [OpenVikingMemory._source_ref(event) for event in restored.events] == expected_refs


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


async def test_large_worker_exports_advance_in_utf8_bounded_pages_and_resume_exact_ranges(runtime):
    from sqlalchemy.dialects.sqlite import insert as sqlite_insert

    root_goal(runtime)
    worker = runtime.service.claim("worker")
    runtime.service.submit(worker["run_id"], worker["fencing_token"], "worker",
        {"status": "completed", "summary": "已核对大输出"}, receipt={"kind": "turn_finished",
            "session_id": worker["session_id"], "run_id": worker["run_id"], "session_revision": "worker-leaf",
            "input_ids": worker["context"]["source_input_ids"], "inflight": 0, "tools_paired": True})
    leader_complete(runtime, runtime.sessions.claim("leader"))
    later_event = runtime.event.model_copy(update={"event_id": "later-dialogue-input"})
    runtime.sessions.enqueue(later_event, runtime.access, new_title="后续独立对话")
    later = runtime.sessions.claim("leader")
    leader_complete(runtime, later)
    worker_entries = None
    for claim in (worker, later):
        path = runtime.path / (claim["session_id"] + ".jsonl")
        entries = native_log(path, run_id=claim["run_id"], role=claim["role"], purpose=claim["purpose"],
            input_id=claim["context"]["source_input_ids"][0], summary="已核对大输出" if claim is worker else "收到")
        if claim is worker:
            outputs = []
            for index in range(9):
                outputs.extend([
                    {"type": "message", "message": {"role": "assistant", "content": [{"type": "toolCall",
                        "id": str(index), "name": "process.read", "arguments": {"limit": 65536}}]}},
                    {"type": "message", "message": {"role": "toolResult", "toolCallId": str(index),
                        "toolName": "process.read", "content": [{"type": "text", "text": "𠮷" * 64000}]}},
                ])
            entries[4:4] = outputs
            for index, item in enumerate(entries):
                item.update(id=str(index), parentId=str(index - 1) if index else None)
            path.write_text("".join(json.dumps(item, ensure_ascii=False) + "\n" for item in entries))
            worker_entries = entries
        with runtime.db.transaction() as conn:
            conn.execute(insert(sessions).values(id=claim["session_id"], scope_key=claim["session_id"],
                session_file=str(path), manifest_json="{}", active_run_id=None, epoch=0))
    binding = MemoryBinding(owner_id=runtime.sessions.settings.owner.id, account_id="test", user_id="test", api_key_env="UNUSED")
    batches = []
    async def sync(batch):
        assert sum(len(event.content.encode()) for event in batch.events) <= 512 * 1024
        assert knowledge.load_source(batch.scope, batch.session_id, batch.from_seq, batch.to_seq,
            batch.branch, batch.projection_version) == batch
        batches.append(batch)
        with runtime.db.transaction() as conn:
            conn.execute(sqlite_insert(memory_streams).values(id=batch.session_id,
                scope_id=batch.scope.key, session_id=batch.session_id, branch=batch.branch,
                projection_version=batch.projection_version, epoch=0, exported_seq=batch.to_seq,
                extracted_seq=batch.to_seq, indexed_seq=batch.to_seq).on_conflict_do_update(
                    index_elements=[memory_streams.c.id], set_={"exported_seq": batch.to_seq,
                        "extracted_seq": batch.to_seq, "indexed_seq": batch.to_seq}))
    async def reconcile():
        pass
    core = runtime.leader.core
    core.sessions = runtime.sessions
    core.openviking = SimpleNamespace(config=SimpleNamespace(bindings=(binding,)), enabled=lambda _: True,
        sync=sync, reconcile=reconcile)
    knowledge = KnowledgeRuntime(core)
    await knowledge.sync()
    assert {batch.session_id for batch in batches} == {worker["session_id"], later["session_id"]}
    assert 0 < next(batch.to_seq for batch in batches if batch.session_id == worker["session_id"]) < len(worker_entries)
    for _ in range(10):
        await knowledge.sync()
        with runtime.db.engine.connect() as conn:
            cursor = conn.execute(select(memory_streams.c.exported_seq).where(
                memory_streams.c.session_id == worker["session_id"])).scalar_one()
        if cursor == len(worker_entries):
            break
    assert cursor == len(worker_entries)
    worker_pages = [batch for batch in batches if batch.session_id == worker["session_id"]]
    assert [batch.from_seq for batch in worker_pages] == [0, *[batch.to_seq for batch in worker_pages[:-1]]]
    assert len([event for batch in worker_pages for event in batch.events if event.role == "tool"]) == 9
    legacy = export_session(runtime.path / (worker["session_id"] + ".jsonl"), session_id=worker["session_id"],
        limit=2, projection_version="pi-original-v1")
    restored = knowledge.load_source(knowledge.scope(runtime.event), worker["session_id"], 0,
        legacy["next_cursor"], legacy["branch"], "pi-original-v1")
    assert restored.events == legacy["events"]
