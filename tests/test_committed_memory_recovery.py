"""A registered memory change survives the gap before its host confirmation."""
import asyncio
import json
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import insert, select, update
from test_agent_runtime import runtime as agent_runtime
from test_leader_dispatch_recovery import native_pending_submit
from test_openviking import Viking

from qqbot.adapters.openviking import OpenVikingMemory
from qqbot.adapters.pi.sessions import mutation_calls
from qqbot.contracts.memory import MemoryBinding, MemoryScope
from qqbot.domain import OutboundEnvelope, stable_id
from qqbot.harness.gateway import RunGateway
from qqbot.memory.openviking_config import OpenVikingConfig
from qqbot.runtime.core import Core
from qqbot.runtime.knowledge import KnowledgeRuntime
from qqbot.storage.agent_schema import dialogue_inputs, dialogues
from qqbot.storage.memory_sync_schema import memory_operations
from qqbot.storage.schema import outbox
from qqbot.storage.task_schema import processes, runs

runtime = agent_runtime


@pytest.fixture
async def host(runtime, monkeypatch):
    scope = MemoryScope(owner_id=runtime.sessions.settings.owner.id)
    binding = MemoryBinding(owner_id=scope.owner_id, account_id="elephant", user_id="owner", api_key_env="TEST_MUTATION_KEY")
    monkeypatch.setenv("TEST_MUTATION_KEY", "fixture-key")
    remote = Viking()
    client = httpx.AsyncClient(transport=httpx.MockTransport(remote))
    memory = OpenVikingMemory(runtime.db, runtime.clock,
        OpenVikingConfig(endpoint="http://127.0.0.1:1933", bindings=(binding,)), client=client)
    core = runtime.leader.core
    core.sessions, core.openviking, core.leader = runtime.sessions, memory, runtime.leader
    core.knowledge = KnowledgeRuntime(core)
    core.permissions = runtime.service.permissions
    core.access = lambda _event: runtime.access
    core.validate_context = lambda _access, context: core.knowledge.valid(context)
    runtime.service.context_validator = core.validate_context
    memory._owner(scope)
    runtime.sessions.enqueue(runtime.event, runtime.access, context={
        "memory_scope": scope.model_dump(), "memory_epoch": 0, "retrieved_data": []})
    claim = runtime.sessions.claim("leader")
    uri = "viking://user/owner/memories/entities/test.md"
    arguments = {"uris": [uri]}
    path = native_pending_submit(runtime, claim, arguments, tool_name="memory_forget")
    process_id = str(uuid4())
    await runtime.leader.invoke("bind-process", (claim["run_id"], claim["fencing_token"], "leader"), {
        "process_id": process_id, "session_id": claim["session_id"], "epoch": 1, "generation": 1,
        "scope_key": claim["session_id"], "session_file": str(path),
        "identity": {"launch_task_id": claim["task_id"], "actual_session_file": str(path)}})
    remote.files[uri] = "synthetic old fact"
    yield SimpleNamespace(runtime=runtime, core=core, memory=memory, remote=remote, scope=scope,
        claim=claim, path=path, arguments=arguments, process_id=process_id, uri=uri)
    await memory.close()
    await client.aclose()


async def register_gap(host):
    request = host.core.knowledge.mutation_request(host.scope, host.claim, "native-submit-call", "memory_forget",
        host.arguments, host.path)
    return await host.memory.forget(request)


@pytest.mark.parametrize("applied", [False, True])
async def test_recover_finished_failed_attempt_and_later_failures_without_repeating_forget(host, applied):
    r, core = host.runtime, host.core
    receipt = await register_gap(host)
    if applied:
        await host.memory.reconcile()
    native_before = host.path.read_bytes()
    failure = json.dumps({"status": "failed", "summary": "Execution stopped after run authority changed.", "agent_authored": False})
    with r.db.transaction() as conn:
        conn.execute(update(runs).where(runs.c.id == host.claim["run_id"]).values(
            state="FINISHED", slot_reserved=0, runner_stopped_at=r.clock.now().isoformat(), result_json=failure))
        original = dict(conn.execute(select(runs)).mappings().one())
        later = []
        for index in range(2):
            run_id = str(uuid4())
            claim = {**host.claim, "run_id": run_id, "task_id": run_id}
            row = {**original, "id": run_id, "process_id": None, "process_epoch": None,
                   "binding_generation": None, "session_generation": index + 2,
                   "request_json": json.dumps(claim), "result_json": "later-startup-failure-" + str(index)}
            conn.execute(insert(runs).values(**row))
            later.append((run_id, row["result_json"]))
        conn.execute(update(dialogue_inputs).values(claimed_attempt_id=None))
    r.service.stop_verifier = lambda *_: False
    assert core.knowledge.recover_mutations() == []
    assert r.sessions.claim("admission-before-stop-proof") is None
    with r.db.engine.connect() as conn:
        assert not conn.execute(select(outbox)).first()
        assert conn.execute(select(dialogue_inputs.c.handled_at)).scalar_one() is None
    with r.db.transaction() as conn:
        conn.execute(update(processes).values(state="STOPPED", stopped_at=r.clock.now().isoformat()))
    calls_before = list(host.remote.calls)
    recovered = core.knowledge.recover_mutations()
    assert len(recovered) == 1 and recovered[0]["operation_id"] == receipt.operation_id
    assert core.knowledge.recover_mutations() == []
    assert host.path.read_bytes() == native_before and host.remote.calls == calls_before
    with r.db.engine.connect() as conn:
        source = conn.execute(select(dialogue_inputs)).mappings().one()
        original = conn.execute(select(runs).where(runs.c.id == host.claim["run_id"])).mappings().one()
        assert source["state"] == "HANDLED" and source["claimed_attempt_id"] == original["id"]
        assert json.loads(original["receipt_json"])["recovered_result"] == failure
        assert original["state"] == "FINISHED" and original["slot_reserved"] == 0
        assert original["runner_stopped_at"] == r.clock.now().isoformat()
        result = json.loads(original["result_json"])
        assert result["agent_authored"] is False and result["status"] == "completed"
        assert ("已完成" in result["summary"]) is applied
        assert {row["id"]: row["result_json"] for row in conn.execute(select(runs).where(
            runs.c.id.in_([item[0] for item in later]))).mappings()} == dict(later)
        replacement = conn.execute(select(dialogues.c.session_id)).scalar_one()
        assert replacement != host.claim["session_id"]
        deliveries = conn.execute(select(outbox)).mappings().all()
        assert len(deliveries) == 1
        envelope = OutboundEnvelope.model_validate_json(deliveries[0]["envelope_json"])
        assert envelope.text == result["summary"] and Core.pre_send(core, envelope) is True
    assert r.sessions.claim("later") is None


async def test_registered_gap_with_active_slot_uses_existing_stop_proof_recovery(host):
    receipt = await register_gap(host)
    r = host.runtime
    r.service.stop_verifier = lambda *_: True
    r.clock.advance(31)
    assert host.core.knowledge.recover_mutations() == []
    with r.db.engine.connect() as conn:
        claim = json.loads(conn.execute(select(runs.c.request_json)).scalar_one())
        assert claim["memory_mutation"]["operation_id"] == receipt.operation_id
        assert conn.execute(select(runs.c.slot_reserved)).scalar_one() == 1
        assert not conn.execute(select(outbox)).first()
    assert r.leader.recover_attempt(host.claim["run_id"])["state"] == "FINISHED"
    assert host.core.knowledge.recover_mutations() == []
    with r.db.engine.connect() as conn:
        assert conn.execute(select(dialogue_inputs.c.state)).scalar_one() == "HANDLED"
        assert conn.execute(select(outbox.c.id)).one()


@pytest.mark.parametrize("conflict", ["missing", "hash", "scope", "native_call", "owner_restore"])
async def test_unmatched_mutation_never_gets_a_host_confirmation(host, conflict):
    r = host.runtime
    if conflict != "missing":
        await register_gap(host)
    with r.db.transaction() as conn:
        conn.execute(update(runs).values(state="FINISHED", slot_reserved=0, runner_stopped_at=r.clock.now().isoformat()))
        conn.execute(update(processes).values(state="STOPPED", stopped_at=r.clock.now().isoformat()))
        conn.execute(update(dialogue_inputs).values(claimed_attempt_id=None))
        if conflict in {"hash", "scope"}:
            conn.execute(update(memory_operations).values(**{"payload_hash" if conflict == "hash" else "scope_id": "wrong"}))
        elif conflict == "owner_restore":
            from qqbot.storage.memory_sync_schema import memory_owners
            conn.execute(update(memory_owners).values(state="RESTORE_REQUIRED"))
    if conflict == "native_call":
        host.path.write_text(host.path.read_text().replace("native-submit-call", "other-native-call"))
    before = host.path.read_bytes()
    assert host.core.knowledge.recover_mutations() == []
    assert not host.remote.calls and host.path.read_bytes() == before
    with r.db.engine.connect() as conn:
        assert not conn.execute(select(outbox)).first()
        assert conn.execute(select(dialogue_inputs.c.handled_at)).scalar_one() is None
        assert conn.execute(select(dialogues.c.session_id)).scalar_one() == host.claim["session_id"]


async def test_registration_rotates_before_control_and_rejects_parallel_personal_tools(host):
    r = host.runtime
    async with host.memory.locks[host.scope.owner_id]:
        pending = asyncio.create_task(host.core.knowledge.invoke(
            (host.claim["run_id"], host.claim["fencing_token"], "leader"),
            {"operation": "memory_forget", "arguments": host.arguments, "call_id": "native-submit-call"}))
        await asyncio.sleep(0)
        assert pending.done(), "Mutation registration waited for remote work before fencing its host turn"
        receipt = pending.result()
    assert receipt["end_turn"] and not receipt["completed"] and not host.remote.calls
    identity = (host.claim["run_id"], host.claim["fencing_token"], "leader")
    assert (await r.leader.invoke("control", identity))["state"] == "RUNNING"
    with pytest.raises(PermissionError, match="ended this Leader turn"):
        await r.leader.invoke("skills", identity, {"operation": "skills_list"})
    with pytest.raises(PermissionError, match="ended this Leader turn"):
        await r.leader.invoke("reserve-budget", identity, {"request_id": "late-model", "kind": "business"})
    await host.memory.reconcile()
    assert host.uri not in host.remote.files


async def test_native_mutation_calls_end_at_the_next_runs_consumed_marker(host):
    entries = [json.loads(line) for line in host.path.read_text().splitlines()]
    parent = entries[-1]["id"]
    with host.path.open("a") as output:
        for entry in [
            {"type": "custom", "id": "next-run", "parentId": parent, "customType": "elephant-consumed",
             "data": {"run_id": "other", "input_ids": host.claim["source_ids"]}},
            {"type": "message", "id": "next-call", "parentId": "next-run", "message": {"role": "assistant",
             "content": [{"type": "toolCall", "name": "memory_forget", "id": "foreign-call", "arguments": host.arguments}]}},
        ]:
            output.write(json.dumps(entry) + "\n")
    assert [call["id"] for call in mutation_calls(host.path, run_id=host.claim["run_id"],
        input_ids=host.claim["source_ids"])] == ["native-submit-call"]


@pytest.mark.parametrize("status", [401, 403, 409, 500])
async def test_gateway_authority_maps_only_current_authorization_denials(tmp_path, status):
    async def denied(*_):
        response = httpx.Response(status, request=httpx.Request("POST", "http://runtime.local/authorize"))
        response.raise_for_status()
    gateway = RunGateway(tmp_path, "offline", "", "http://model.invalid", call=denied)
    gateway.revoked = False
    if status == 500:
        with pytest.raises(httpx.HTTPStatusError):
            await gateway.authority()
    else:
        assert await gateway.authority() is False


async def test_committed_correction_recovery_matches_original_source_and_uses_background_driver(host):
    r = host.runtime
    host.path = native_pending_submit(r, host.claim, {"uri": host.uri}, tool_name="memory_correct")
    request = host.core.knowledge.mutation_request(host.scope, host.claim, "native-submit-call", "memory_correct",
        {"uri": host.uri}, host.path)
    from qqbot.adapters.openviking.service import digest
    from qqbot.storage.memory_sync_schema import memory_documents
    with r.db.transaction() as conn:
        conn.execute(insert(memory_documents).values(id=stable_id(host.scope.key, host.uri), scope_id=host.scope.key, uri=host.uri,
            content_hash=digest(host.remote.files[host.uri]), epoch=0, state="ACTIVE", index_state="INDEXED",
            updated_at=r.clock.now().isoformat()))
    receipt = await host.memory.correct(request)
    with r.db.transaction() as conn:
        conn.execute(update(runs).values(state="FINISHED", slot_reserved=0, runner_stopped_at=r.clock.now().isoformat(),
            result_json=json.dumps({"status": "failed", "agent_authored": False, "summary": "interrupted"})))
        conn.execute(update(processes).values(state="STOPPED", stopped_at=r.clock.now().isoformat()))
        conn.execute(update(dialogue_inputs).values(claimed_attempt_id=None))
    assert host.core.knowledge.recover_mutations()[0]["operation_id"] == receipt.operation_id
    assert not host.remote.calls
    await host.memory.reconcile()
    assert (await host.memory.poll(receipt.operation_id)).completed
    assert (await host.memory.read(host.scope, host.uri)).content == request.source.content


async def test_later_attempt_and_session_writer_must_stop_before_confirmation(host):
    await register_gap(host)
    r = host.runtime
    with r.db.transaction() as conn:
        conn.execute(update(runs).values(state="FINISHED", slot_reserved=0, runner_stopped_at=r.clock.now().isoformat()))
        conn.execute(update(processes).values(state="STOPPED", stopped_at=r.clock.now().isoformat()))
        original = dict(conn.execute(select(runs)).mappings().one())
        later_id = str(uuid4())
        later_claim = {**host.claim, "run_id": later_id, "task_id": later_id}
        later = {**original, "id": later_id, "state": "RUNNING", "slot_reserved": 1,
            "runner_stopped_at": None, "process_id": None, "request_json": json.dumps(later_claim)}
        conn.execute(insert(runs).values(**later))
        conn.execute(update(dialogue_inputs).values(claimed_attempt_id=later_id))
    assert host.core.knowledge.recover_mutations() == []
    with r.db.engine.connect() as conn:
        assert not conn.execute(select(outbox)).first()
        assert conn.execute(select(dialogues.c.session_id)).scalar_one() != host.claim["session_id"]
    with r.db.transaction() as conn:
        conn.execute(update(runs).where(runs.c.id == later_id).values(state="FINISHED", slot_reserved=0,
            runner_stopped_at=r.clock.now().isoformat()))
    assert len(host.core.knowledge.recover_mutations()) == 1


async def test_conflicting_existing_delivery_is_kept_without_false_confirmation(host):
    await register_gap(host)
    r = host.runtime
    with r.db.transaction() as conn:
        conn.execute(update(runs).values(state="FINISHED", slot_reserved=0, runner_stopped_at=r.clock.now().isoformat()))
        conn.execute(update(processes).values(state="STOPPED", stopped_at=r.clock.now().isoformat()))
        conn.execute(update(dialogue_inputs).values(claimed_attempt_id=None))
        r.service.outbox.enqueue_text(conn, target=r.event.conversation, text="Different historical reply",
            origin_kind="chat", origin_id="turn:" + host.claim["session_id"] + ":" + r.event.event_id,
            requester=r.event.principal)
    assert host.core.knowledge.recover_mutations() == []
    with r.db.engine.connect() as conn:
        assert conn.execute(select(dialogue_inputs.c.handled_at)).scalar_one() is None
        assert json.loads(conn.execute(select(outbox.c.envelope_json)).scalar_one())["text"] == "Different historical reply"
    assert r.sessions.claim("after-conflict") is None
    with r.db.engine.connect() as conn:
        assert conn.execute(select(dialogue_inputs.c.handled_at)).scalar_one() is None
        assert len(conn.execute(select(runs)).all()) == 1


async def test_unrelated_old_session_writer_keeps_original_receipt_queued_during_admission(host):
    await register_gap(host)
    r = host.runtime
    with r.db.transaction() as conn:
        conn.execute(update(runs).values(state="FINISHED", slot_reserved=0, runner_stopped_at=r.clock.now().isoformat()))
        conn.execute(update(processes).values(state="STOPPED", stopped_at=r.clock.now().isoformat()))
        conn.execute(update(dialogue_inputs).values(claimed_attempt_id=None))
        original = dict(conn.execute(select(runs)).mappings().one())
        writer_id = str(uuid4())
        writer_claim = {**host.claim, "run_id": writer_id, "task_id": writer_id, "source_ids": ["another-input"]}
        conn.execute(insert(runs).values(**{**original, "id": writer_id, "state": "RUNNING", "slot_reserved": 1,
            "runner_stopped_at": None, "process_id": None, "request_json": json.dumps(writer_claim)}))
    assert host.core.knowledge.recover_mutations() == []
    assert r.sessions.claim("normal-admission") is None
    with r.db.engine.connect() as conn:
        assert conn.execute(select(dialogue_inputs.c.handled_at)).scalar_one() is None
        assert conn.execute(select(dialogues.c.session_id)).scalar_one() != host.claim["session_id"]
        assert not conn.execute(select(outbox)).first()
    with r.db.transaction() as conn:
        conn.execute(update(runs).where(runs.c.id == writer_id).values(state="FINISHED", slot_reserved=0,
            runner_stopped_at=r.clock.now().isoformat()))
    assert len(host.core.knowledge.recover_mutations()) == 1


@pytest.mark.parametrize("field", ["consumed", "message", "content", "part", "arguments"])
async def test_malformed_native_mutation_is_isolated_from_admission(host, field):
    await register_gap(host)
    r = host.runtime
    with r.db.transaction() as conn:
        conn.execute(update(runs).values(state="FINISHED", slot_reserved=0, runner_stopped_at=r.clock.now().isoformat()))
        conn.execute(update(processes).values(state="STOPPED", stopped_at=r.clock.now().isoformat()))
        conn.execute(update(dialogue_inputs).values(claimed_attempt_id=None))
    entries = [json.loads(line) for line in host.path.read_text().splitlines()]
    for entry in entries:
        if field == "consumed" and entry.get("customType") == "elephant-consumed":
            entry["data"] = None
        if entry.get("type") == "message" and entry["message"].get("role") == "assistant":
            if field == "message":
                entry["message"] = None
            elif field == "content":
                entry["message"]["content"] = None
            elif field == "part":
                entry["message"]["content"] = [None]
            elif field == "arguments":
                entry["message"]["content"][0]["arguments"] = None
    host.path.write_text("".join(json.dumps(entry) + "\n" for entry in entries))
    assert host.core.knowledge.recover_mutations() == []
    assert r.sessions.claim("after-malformed-native") is None
    with r.db.engine.connect() as conn:
        assert not conn.execute(select(outbox)).first()
        assert len(conn.execute(select(runs)).all()) == 1


async def test_missing_stop_proof_rotates_other_dialogues_before_new_admission(host):
    r = host.runtime
    with r.db.transaction() as conn:
        other = r.sessions.dialogue(conn, r.event, new_title="Other owner dialogue")
        old_other_session = other["session_id"]
    await register_gap(host)
    with r.db.transaction() as conn:
        conn.execute(update(runs).values(state="FINISHED", slot_reserved=0, runner_stopped_at=r.clock.now().isoformat()))
        conn.execute(update(dialogue_inputs).values(claimed_attempt_id=None))
    r.service.stop_verifier = lambda *_: False
    assert host.core.knowledge.recover_mutations() == []
    assert r.sessions.claim("before-stop-proof") is None
    with r.db.engine.connect() as conn:
        assert conn.execute(select(dialogue_inputs.c.handled_at)).scalar_one() is None
        new_other_session = conn.execute(select(dialogues.c.session_id).where(dialogues.c.id == other["id"])).scalar_one()
        assert new_other_session != old_other_session
        assert not conn.execute(select(outbox)).first()
    followup = r.event.model_copy(update={"event_id": "new-owner-input"})
    r.sessions.enqueue(followup, r.access, context={"memory_scope": host.scope.model_dump(),
        "memory_epoch": 1, "retrieved_data": []}, select_ref=other["id"])
    claim = r.sessions.claim("new-session")
    assert claim["source_ids"] == [followup.event_id] and claim["session_id"] == new_other_session
    with r.db.transaction() as conn:
        conn.execute(update(processes).values(state="STOPPED", stopped_at=r.clock.now().isoformat()))
    assert len(host.core.knowledge.recover_mutations()) == 1
    assert host.core.knowledge.recover_mutations() == []
    with r.db.engine.connect() as conn:
        assert conn.execute(select(outbox.c.id)).one()
        assert conn.execute(select(dialogues.c.session_id).where(dialogues.c.id == other["id"])).scalar_one() == new_other_session
