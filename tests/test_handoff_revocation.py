"""Transferred task data keeps its source authority through execution and delivery."""
import hashlib
import json
import os
from contextlib import contextmanager
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from conftest import official_payload
from sqlalchemy import insert, select, update

from qqbot.app import create_app
from qqbot.config import Settings
from qqbot.domain import OutboundEnvelope
from qqbot.permissions import PermissionRule
from qqbot.storage.agent_schema import dialogue_inputs, dialogues, leader_inbox
from qqbot.storage.task_schema import runs, sessions, tasks


def receipt(claim):
    return {"kind": "turn_finished", "session_id": claim["session_id"], "run_id": claim["run_id"],
        "session_revision": "paired", "input_ids": claim["context"]["source_input_ids"],
        "tools_paired": True, "inflight": 0}


def finish_worker(core, claim, worker, **result):
    return core.tasks.submit(claim["run_id"], claim["fencing_token"], worker,
        {"status": "completed", "summary": "PRIVATE_HANDOFF", **result}, receipt=receipt(claim))


def revoke(state, kind="task"):
    if kind == "recall":
        state.core.inbox.accept(state.source.model_copy(update={"event_id": "source-recall", "event_type": "message.recalled"}))
        return
    action, resource = ("task.read", "task:" + state.first["task_id"]) if kind == "task" else (
        "history.source.read", "event:" + state.source.event_id)
    state.core.permissions.upsert(PermissionRule(id="revoke-handoff", actions=(action,), resources=(resource,),
        effect="deny"), expected_version=state.core.permissions.policy_version, actor="test")


def claim_fresh_user(state):
    session_id = str(uuid4())
    with state.core.db.transaction() as conn:
        conn.execute(update(dialogues).where(dialogues.c.id == state.dialogue["id"]).values(session_id=session_id))
        conn.execute(update(tasks).where(tasks.c.origin_dialogue_id == state.dialogue["id"],
            tasks.c.kind == "goal").values(coordinator_session_id=session_id))
    event = state.event.model_copy(update={"event_id": "fresh-user", "platform_message_id": "fresh-user"})
    event, _ = state.core.inbox.accept(event)
    context = state.core.context_for(event)
    context["source_manifest"] = [{"kind": "inbox", "id": event.event_id,
        "digest": hashlib.sha256(event.model_dump_json().encode()).hexdigest()}]
    state.core.sessions.enqueue(event, state.core.access(event), context)
    return state.core.sessions.claim("user-leader")


async def bind_user(state, claim, tmp_path):
    identity = (claim["run_id"], claim["fencing_token"], "user-leader")
    await state.core.leader.invoke("bind-process", identity, {"process_id": str(uuid4()),
        "session_id": claim["session_id"], "epoch": 1, "generation": 1,
        "session_file": str(tmp_path / "user-leader.jsonl")})
    return identity


@pytest.fixture
async def handoff(tmp_path, official_config, official_event, clock, monkeypatch):
    monkeypatch.setenv("QQBOT_ADMIN_TOKEN", "handoff-admin-token")
    settings = Settings(database=str(tmp_path / "core.db"), adapters=(official_config,), tasks_enabled=True,
        owner={"qq_bindings": ({"app_id": "app", "user_openid": "300", "verified_at": clock.now().isoformat()},)})
    app = create_app(settings, clock=clock, run_background=False)
    core = app.state.core
    event = core.adapters["qq"].normalize(official_payload(official_event, private=True, text="检查项目", message_id="root"))
    core.inbox.accept(event)
    access = core.access(event)
    with core.db.transaction() as conn:
        dialogue = core.sessions.dialogue(conn, event)
        root = core.tasks.graph.submit(conn, dialogue, event, access,
            {"goal": "检查项目", "children": [{"goal": "原始检查", "delegation_key": "first"}]},
            "handoff-root", core.sessions.binding(), core.context_for(event))
        conn.execute(insert(sessions).values(id=dialogue["session_id"], scope_key=dialogue["id"],
            session_file=str(tmp_path / "leader.jsonl"), manifest_json=json.dumps(core.sessions.binding()), epoch=1))
    first = core.tasks.claim("first")
    source = core.adapters["qq"].normalize(official_payload(official_event, private=True,
        text="PRIVATE_HANDOFF", message_id="source-only"))
    source, _ = core.inbox.accept(source)
    with core.db.transaction() as conn:
        row = conn.execute(select(tasks).where(tasks.c.id == first["task_id"])).mappings().one()
        context = json.loads(row["context_json"])
        context["source_manifest"].append({"kind": "inbox", "id": source.event_id,
            "digest": hashlib.sha256(source.model_dump_json().encode()).hexdigest()})
        conn.execute(update(tasks).where(tasks.c.id == row["id"]).values(context_json=json.dumps(context)))
    finish_worker(core, first, "first")
    with core.db.transaction() as conn:
        parent = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
        second = core.tasks.graph.spawn(conn, parent, {"goal": "根据前序成果返工", "delegation_key": "second",
            "input_refs": [first["task_id"]]}, access)
    claim = core.tasks.claim("second")
    assert claim["task_id"] == second["task_id"]
    state = SimpleNamespace(app=app, core=core, event=event, access=access, source=source,
        root=root, first=first, claim=claim, dialogue=dialogue)
    try:
        yield state
    finally:
        await core.stop()
        core.db.close()


@pytest.mark.parametrize("kind", ["task", "source", "recall"])
async def test_handoff_revocation_blocks_models_results_recovery_and_control(handoff, kind):
    s = handoff
    identity = (s.claim["run_id"], s.claim["fencing_token"], "second")
    assert s.core.tasks.authorize(*identity, {"action": "model.request"})["allowed"]
    revoke(s, kind)
    with pytest.raises(PermissionError, match="context"):
        s.core.tasks.authorize(*identity, {"action": "model.request"})
    with pytest.raises(PermissionError, match="handoff"):
        finish_worker(s.core, s.claim, "second")
    with s.core.db.engine.connect() as conn:
        task = conn.execute(select(tasks).where(tasks.c.id == s.claim["task_id"])).mappings().one()
        assert s.core.recovery._terminal_reason(task) == ("BLOCKED", "context_revoked")
        root = conn.execute(select(tasks).where(tasks.c.id == s.root["task_id"])).mappings().one()
        status = s.core.tasks.graph.status(conn, root)
    assert status["context_revoked"] and status["result"] is None and status["goal"] is None
    assert "PRIVATE_HANDOFF" not in json.dumps(status)
    view = s.core.tasks.get(task["public_id"], s.access)
    assert view["context_revoked"] and view["result_json"] is None
    assert s.core.tasks.control(*identity)["state"] == "CANCEL_REQUESTED"
    assert s.core.tasks.cancel(task["public_id"], s.access)["state"] == "CANCEL_REQUESTED"


async def test_reclaim_preserves_revoked_dependencies_and_admits_independent_work(handoff):
    s = handoff
    revoke(s)
    with s.core.db.transaction() as conn:
        conn.execute(update(runs).where(runs.c.id == s.claim["run_id"]).values(slot_reserved=0))
        conn.execute(update(tasks).where(tasks.c.id == s.claim["task_id"]).values(state="QUEUED"))
        event = s.event.model_copy(update={"event_id": "independent"})
        root = s.core.tasks.graph.submit(conn, s.dialogue, event, s.access,
            {"goal": "独立检查", "children": [{"goal": "独立检查", "delegation_key": "independent"}]},
            "independent", s.core.sessions.binding(), s.core.context_for(event))
    claim = s.core.tasks.claim("independent")
    assert claim["root_task_id"] == root["task_id"]
    assert s.core.tasks.claim("drain-revoked") is None
    with s.core.db.engine.connect() as conn:
        row = conn.execute(select(tasks).where(tasks.c.id == s.claim["task_id"])).mappings().one()
        assert row["state"] == "BLOCKED" and row["session_id"] == s.claim["session_id"]
        assert s.first["task_id"] in json.loads(row["context_json"])["task_sources"]


@pytest.mark.parametrize("revocation_phase", ["before_finalize", "before_send", "during_download"])
async def test_finalizer_and_queued_delivery_recheck_persisted_handoff(handoff, monkeypatch, revocation_phase):
    s = handoff
    content, artifact_id = b"PRIVATE_HANDOFF", str(uuid4())
    archive = s.core.tasks.archive_root
    archive.mkdir(parents=True, exist_ok=True)
    (archive / artifact_id).write_bytes(content)
    artifact = {"id": artifact_id, "relative_path": "outputs/result.txt", "mime_type": "text/plain",
        "size": len(content), "sha256": hashlib.sha256(content).hexdigest()}
    finish_worker(s.core, s.claim, "second", artifacts=[artifact])
    final = s.core.sessions.claim("leader")
    assert final["purpose"] == "finalize"
    def complete():
        return s.core.leader.complete(final["run_id"], final["fencing_token"], "leader",
            {"status": "completed", "summary": "PRIVATE_HANDOFF"}, stopped=True, quiescent=False, receipt=receipt(final))
    if revocation_phase == "before_finalize":
        revoke(s)
        with pytest.raises(PermissionError, match="handoff|context"):
            complete()
        return
    complete()
    from qqbot.storage.schema import outbox
    with s.core.db.engine.connect() as conn:
        row = conn.execute(select(outbox).where(outbox.c.task_id == s.root["task_id"])).mappings().one()
    envelope = OutboundEnvelope.model_validate_json(row["envelope_json"])
    assert s.core.pre_send(envelope)
    if revocation_phase == "during_download":
        original_fdopen = os.fdopen

        @contextmanager
        def revoke_on_read(*args, **kwargs):
            with original_fdopen(*args, **kwargs) as source:
                def read(size):
                    content = source.read(size)
                    revoke(s)
                    return content
                yield SimpleNamespace(fileno=source.fileno, read=read)

        monkeypatch.setattr(os, "fdopen", revoke_on_read)
    else:
        revoke(s)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=s.app), base_url="http://fixture") as client:
        auth = {"Authorization": "Bearer handoff-admin-token"}
        response = await client.get("/api/v1/artifacts/" + s.root["task_id"] + "/" + artifact["sha256"] + "/download", headers=auth)
        assert response.status_code == 409
        status = await client.get("/api/v1/tasks/" + s.root["public_id"], headers=auth)
        assert status.status_code == 200 and status.json()["context_revoked"]
        assert "PRIVATE_HANDOFF" not in status.text
    assert not s.core.pre_send(envelope)


async def test_revoked_leader_history_rotates_without_consuming_new_input(handoff):
    s = handoff
    revoke(s)
    event = s.core.adapters["qq"].normalize(official_payload({"op": 0, "id": "fresh-event", "d": {
        "id": "fresh", "author": {"member_openid": "300"}, "content": "新问题", "timestamp": s.core.clock.now().isoformat()}},
        private=True, text="新问题"))
    s.core.inbox.accept(event)
    accepted = s.core.sessions.enqueue(event, s.core.access(event), s.core.context_for(event))
    with s.core.db.engine.connect() as conn:
        before = conn.execute(select(dialogue_inputs).where(dialogue_inputs.c.id == event.event_id)).mappings().one()
    assert s.core.sessions.claim("leader") is None
    fresh = s.core.sessions.claim("leader")
    assert fresh["purpose"] == "user" and fresh["source_ids"] == [event.event_id]
    assert fresh["session_id"] != accepted["session_id"]
    assert not fresh["context"].get("task_sources")
    with s.core.db.engine.connect() as conn:
        after = conn.execute(select(dialogue_inputs).where(dialogue_inputs.c.id == event.event_id)).mappings().one()
        assert after["handled_at"] is None and after["persisted_at"] is None
        assert json.loads(after["context_json"])["leader_turn"] == json.loads(before["context_json"])["leader_turn"]
        assert conn.execute(select(dialogues.c.session_id).where(dialogues.c.id == s.dialogue["id"])).scalar_one() == fresh["session_id"]


async def test_revoked_root_stops_then_settles_while_independent_root_runs(handoff, tmp_path):
    from qqbot.adapters.execution.runtime import JobRuntime

    s = handoff
    revoke(s)
    assert s.core.sessions.claim("leader") is None
    assert s.core.sessions.claim("leader") is None
    with s.core.db.transaction() as conn:
        root = conn.execute(select(tasks).where(tasks.c.id == s.root["task_id"])).mappings().one()
        child = conn.execute(select(tasks).where(tasks.c.id == s.claim["task_id"])).mappings().one()
        assert root["state"] == child["state"] == "CANCEL_REQUESTED"
        assert root["stop_reason"] == child["stop_reason"] == "revoked"
        assert conn.execute(select(leader_inbox.c.id).where(
            leader_inbox.c.root_task_id == root["id"],
            leader_inbox.c.output_ref == "CONTEXT_REVOKED")).first()
        event = s.event.model_copy(update={"event_id": "independent-root"})
        dialogue = conn.execute(select(dialogues).where(dialogues.c.id == s.dialogue["id"])).mappings().one()
        independent = s.core.tasks.graph.submit(conn, dialogue, event, s.access,
            {"goal": "独立检查", "children": [{"goal": "独立检查", "delegation_key": "independent"}]},
            "independent-root", s.core.sessions.binding(), s.core.context_for(event))
    claim = s.core.tasks.claim("independent")
    assert claim["root_task_id"] == independent["task_id"]
    runtime = JobRuntime(s.core, tmp_path)
    await runtime.maintain()
    assert s.core.tasks.get(s.root["public_id"], s.access)["state"] == "CANCEL_REQUESTED"
    s.core.tasks.submit(s.claim["run_id"], s.claim["fencing_token"], "second",
        {"status": "failed", "agent_authored": False, "summary": "Runner stopped."}, stopped=True)
    await runtime.maintain()
    assert s.core.tasks.get(s.root["public_id"], s.access)["state"] == "CANCELLED"
    with s.core.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == claim["task_id"])).scalar_one() == "RUNNING"


async def test_user_active_task_snapshot_keeps_dependencies_in_fresh_session(handoff, tmp_path):
    s = handoff
    claim = claim_fresh_user(s)
    assert claim["purpose"] == "user" and claim["target_root_task_id"] is None
    assert "PRIVATE_HANDOFF" in json.dumps(claim["context"]["active_tasks"])
    assert {s.root["task_id"], s.first["task_id"]} <= set(claim["context"]["task_sources"])
    identity = await bind_user(s, claim, tmp_path)
    with s.core.db.engine.connect() as conn:
        history = json.loads(conn.execute(select(sessions.c.manifest_json).where(
            sessions.c.id == claim["session_id"])).scalar_one())
    assert s.first["task_id"] in history["task_sources"]
    revoke(s)
    with pytest.raises(PermissionError, match="context"):
        await s.core.leader.invoke("authorize", identity, {"action": "model.request"})


@pytest.mark.parametrize("operation", ["tasks.status", "tasks.list"])
async def test_user_tool_snapshot_persists_new_dependencies_after_bind(handoff, tmp_path, operation):
    s = handoff
    with s.core.db.transaction() as conn:
        old = conn.execute(select(tasks).where(tasks.c.id == s.root["task_id"])).mappings().one()
        source_context = json.loads(old["context_json"])
        conn.execute(update(tasks).where(tasks.c.id == old["id"]).values(state="SUCCEEDED"))
    claim = claim_fresh_user(s)
    assert not claim["context"].get("task_sources")
    identity = await bind_user(s, claim, tmp_path)
    with s.core.db.transaction() as conn:
        dialogue = conn.execute(select(dialogues).where(dialogues.c.id == s.dialogue["id"])).mappings().one()
        source = s.core.tasks.graph.submit(conn, dialogue, s.event, s.access,
            {"goal": "PRIVATE_HANDOFF", "children": [{"goal": "新工作", "delegation_key": "new"}]},
            "new-source", s.core.sessions.binding(), source_context)
    snapshot = await s.core.leader.invoke("tasks", identity, {"operation": operation,
        "arguments": {"task_ref": source["public_id"]}})
    assert "PRIVATE_HANDOFF" in json.dumps(snapshot)
    with s.core.db.engine.connect() as conn:
        stored = json.loads(conn.execute(select(runs.c.request_json).where(runs.c.id == claim["run_id"])).scalar_one())
        history = json.loads(conn.execute(select(sessions.c.manifest_json).where(
            sessions.c.id == claim["session_id"])).scalar_one())
    assert {source["task_id"], s.first["task_id"]} <= set(stored["context"]["task_sources"])
    assert {source["task_id"], s.first["task_id"]} <= set(history["task_sources"])
    revoke(s)
    with pytest.raises(PermissionError, match="context"):
        await s.core.leader.invoke("authorize", identity, {"action": "model.request"})


@pytest.mark.parametrize("operation", ["tasks.status", "tasks.list"])
async def test_revoked_original_task_context_is_redacted_without_poisoning_user(handoff, operation):
    s = handoff
    with s.core.db.transaction() as conn:
        root = conn.execute(select(tasks).where(tasks.c.id == s.root["task_id"])).mappings().one()
        context = json.loads(root["context_json"])
        context.pop("task_sources")
        conn.execute(update(tasks).where(tasks.c.id == root["id"]).values(context_json=json.dumps(context)))
    revoke(s, "source")
    claim = claim_fresh_user(s)
    assert claim["purpose"] == "user" and not claim["context"].get("task_sources")
    snapshot = claim["context"]["active_tasks"][0]
    assert snapshot["context_revoked"] and snapshot["goal"] is None and snapshot["result"] is None
    assert "PRIVATE_HANDOFF" not in json.dumps(snapshot)
    identity = (claim["run_id"], claim["fencing_token"], "user-leader")
    snapshot = await s.core.leader.invoke("tasks", identity, {"operation": operation,
        "arguments": {"task_ref": s.root["public_id"]}})
    assert "PRIVATE_HANDOFF" not in json.dumps(snapshot)
    assert (await s.core.leader.invoke("authorize", identity, {"action": "model.request"}))["allowed"]


async def test_authorized_control_does_not_acquire_write_transaction(handoff, monkeypatch):
    s = handoff

    def unexpected_write():
        pytest.fail("authorized control acquired a write transaction")

    monkeypatch.setattr(s.core.db, "transaction", unexpected_write)
    assert s.core.tasks.control(s.claim["run_id"], s.claim["fencing_token"], "second")["state"] == "RUNNING"
