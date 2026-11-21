"""Core context failures survive settlement and maintenance yields its writer."""
import json

import pytest
from sqlalchemy import insert, select, update
from test_agent_runtime import runtime as runtime  # noqa: PLC0414
from test_leader_core import finish_leader, make_core

from elephant.storage.agent_schema import dialogue_inputs
from elephant.storage.schema import outbox
from elephant.storage.task_schema import compaction_due, compaction_retry_at, runs, sessions


async def prepare(core, official_event, text="hello"):
    from conftest import official_payload
    event = core.adapters["qq"].normalize(official_payload(official_event, private=True, text=text))
    core.inbox.accept(event)
    await core.tick(dispatch=False)
    claim = core.sessions.claim("fixture")
    with core.db.transaction() as conn:
        conn.execute(insert(sessions).values(id=claim["session_id"], scope_key=claim["session_id"],
            session_file="fixture.jsonl", manifest_json=json.dumps(claim["context"]),
            active_run_id=claim["run_id"], epoch=1))
    return event, claim


async def test_deterministic_failure_blocks_original_input_and_deduplicates_notice(tmp_path, official_config, official_event, clock):
    core = make_core(tmp_path, official_config, clock)
    try:
        event, claim = await prepare(core, official_event)
        identity = (claim["run_id"], claim["fencing_token"], "fixture")
        payload = {"phase": "failure", "reason": "context_length_exceeded", "deterministic": True,
            "fingerprint": "stable-content", "limits_fingerprint": "model-limit"}
        for _ in range(2):
            await core.leader.invoke("context-compaction", identity, payload)
        core.leader.complete(*identity, {"status": "failed", "summary": "Context too large", "agent_authored": False},
            stopped=True, quiescent=False)
        assert core.sessions.claim("fixture") is None
        with core.db.engine.connect() as conn:
            source = conn.execute(select(dialogue_inputs).where(dialogue_inputs.c.id == event.event_id)).mappings().one()
            assert source["state"] == "CONTEXT_FAILED"
            assert json.loads(source["context_json"])["context_failure"]["fingerprint"] == "stable-content"
            assert len(conn.execute(select(outbox).where(outbox.c.origin_id == "context-failure:" + event.event_id)).all()) == 1
            assert conn.scalar(select(runs.c.slot_reserved).where(runs.c.id == claim["run_id"])) == 0
    finally:
        await core.stop()
        core.db.close()


async def test_maintenance_fence_preemption_and_saved_record(tmp_path, official_config, official_event, clock):
    core = make_core(tmp_path, official_config, clock)
    try:
        event, claim = await prepare(core, official_event)
        finish_leader(core, claim, "done")
        with core.db.transaction() as conn:
            manifest = json.loads(conn.scalar(select(sessions.c.manifest_json)))
            manifest["context_compaction"] = {"elastic_due": True}
            conn.execute(update(sessions).values(manifest_json=json.dumps(manifest)))
        maintenance = core.leader.compaction.claim("fixture")
        assert maintenance["purpose"] == "compaction"
        identity = (maintenance["run_id"], maintenance["fencing_token"], "fixture")
        begun = await core.leader.invoke("context-compaction", identity, {"phase": "begin", "branch_id": "leaf"})
        with pytest.raises(PermissionError, match="stale compaction"):
            await core.leader.invoke("context-compaction", identity, {"phase": "commit", "attempt_id": "old", "branch_id": "leaf"})
        payload = {"attempt_id": begun["attempt_id"], "branch_id": "leaf"}
        await core.leader.invoke("context-compaction", identity, {**payload, "phase": "commit"})
        with core.db.transaction() as conn:
            conn.execute(update(dialogue_inputs).where(dialogue_inputs.c.id == event.event_id).values(
                handled_at=None, claimed_attempt_id=None, state="PENDING"))
        assert core.leader.renew(*identity)["state"] == "CANCEL_REQUESTED"
        with pytest.raises(PermissionError, match="foreground"):
            await core.leader.invoke("context-compaction", identity, {**payload, "phase": "request"})
        await core.leader.invoke("context-compaction", identity, {**payload, "phase": "saved", "entry_id": "native-record"})
        core.leader.complete(*identity, {"status": "completed", "summary": "Compacted", "agent_authored": False}, stopped=True, quiescent=False)
        assert core.sessions.claim("fixture")
    finally:
        await core.stop()
        core.db.close()


async def test_native_record_recovery_is_idempotent_and_rejects_old_branch(tmp_path, official_config, official_event, clock):
    from types import SimpleNamespace
    core = make_core(tmp_path, official_config, clock)
    try:
        _, claim = await prepare(core, official_event)
        identity = (claim["run_id"], claim["fencing_token"], "fixture")
        begun = await core.leader.invoke("context-compaction", identity, {"phase": "begin", "branch_id": "leaf"})
        await core.leader.invoke("context-compaction", identity, {"phase": "commit", **begun,
            "branch_id": "leaf", "first_kept_entry_id": "kept", "summary": "must not persist"})
        record = {"id": "native", "parentId": "leaf", "firstKeptEntryId": "kept",
            "details": {"elephant_compaction": {**begun, "branch_id": "leaf", "after_tokens": 100}}}
        adapter = SimpleNamespace(compaction_records=lambda _: [record])
        with core.db.transaction() as conn:
            run = conn.execute(select(runs).where(runs.c.id == claim["run_id"])).mappings().one()
            core.leader.compaction.recover(conn, run, claim, adapter, "ignored")
            core.leader.compaction.recover(conn, run, claim, adapter, "ignored")
            manifest = json.loads(conn.scalar(select(sessions.c.manifest_json)))
            assert manifest["context_compaction"]["entry_id"] == "native"
            assert "summary" not in manifest["context_compaction"]
            record["parentId"] = "late-writer"
            with pytest.raises(ValueError, match="branch"):
                core.leader.compaction.recover(conn, run, claim, adapter, "ignored")
    finally:
        await core.stop()
        core.db.close()


async def test_bounded_transient_retry_and_explicit_recovery(tmp_path, official_config, official_event, clock):
    from elephant.domain import AccessContext
    core = make_core(tmp_path, official_config, clock)
    try:
        event, claim = await prepare(core, official_event)
        for attempt in range(3):
            identity = (claim["run_id"], claim["fencing_token"], "fixture")
            for _ in range(2):
                await core.leader.invoke("context-compaction", identity, {
                    "phase": "failure", "reason": "timeout", "deterministic": False})
            core.leader.complete(*identity, {"status": "failed", "summary": "Timed out", "agent_authored": False},
                stopped=True, quiescent=False)
            assert core.sessions.claim("fixture") is None
            clock.advance(45)
            if attempt < 2:
                claim = core.sessions.claim("fixture")
                assert claim
        assert core.sessions.claim("fixture") is None
        followup = event.model_copy(update={"event_id": "independent", "platform_message_id": "independent"})
        core.sessions.enqueue(followup, AccessContext(principal=followup.principal, conversation=followup.conversation))
        independent = core.sessions.claim("fixture")
        assert independent["source_ids"] == ["independent"]
        assert independent["context"]["context_failed_input_ids"] == [event.event_id]
        identity = (independent["run_id"], independent["fencing_token"], "fixture")
        await core.leader.invoke("context-compaction", identity, {"phase": "recover", "input_id": event.event_id})
        finish_leader(core, independent, "Recovery requested")
        recovered = core.sessions.claim("fixture")
        assert recovered["source_ids"] == [event.event_id]
        assert recovered["context"]["context_failed_input_ids"] == []
    finally:
        await core.stop()
        core.db.close()


async def test_larger_limit_unblocks_but_changed_failure_metadata_does_not(tmp_path, official_config, official_event, clock):
    core = make_core(tmp_path, official_config, clock)
    try:
        core.settings = core.settings.model_copy(update={"model": core.settings.model.model_copy(update={"model": "fixture"}),
            "context": core.settings.context.model_copy(update={"force_compaction_tokens": 10000})})
        core.sessions.settings = core.settings
        event, claim = await prepare(core, official_event)
        identity = (claim["run_id"], claim["fencing_token"], "fixture")
        failure = {"phase": "failure", "reason": "context_budget_exceeded", "deterministic": True,
            "tokens": 10001, "request_bytes": 5000,
            "limits_fingerprint": f"{core.settings.model.model}:{core.settings.model.context_window}:10000:{core.settings.context.max_request_bytes}"}
        await core.leader.invoke("context-compaction", identity, failure)
        core.leader.complete(*identity, {"status": "failed", "summary": "Too large", "agent_authored": False},
            stopped=True, quiescent=False)
        clock.advance(60)
        assert core.sessions.claim("fixture") is None
        core.settings = core.settings.model_copy(update={"context": core.settings.context.model_copy(update={"force_compaction_tokens": 11000})})
        core.sessions.settings = core.settings
        assert core.sessions.claim("fixture")["source_ids"] == [event.event_id]
    finally:
        await core.stop()
        core.db.close()


@pytest.mark.parametrize("exhausted", [False, True])
async def test_admin_recovery_route_requires_auth_and_stopped_original_writer(
    tmp_path, official_config, official_event, clock, monkeypatch, exhausted
):
    from fastapi.testclient import TestClient

    from elephant.app import create_app
    monkeypatch.setenv("ELEPHANT_ADMIN_TOKEN", "recovery-token")
    core = make_core(tmp_path, official_config, clock)
    try:
        event, claim = await prepare(core, official_event)
        identity = (claim["run_id"], claim["fencing_token"], "fixture")
        await core.leader.invoke("context-compaction", identity, {"phase": "failure", "deterministic": True})
        app = create_app(core.settings, clock=clock, run_background=False)
        with TestClient(app) as client:
            path = "/api/v1/context-inputs/" + event.event_id + "/recover"
            auth = {"Authorization": "Bearer recovery-token"}
            assert client.post(path).status_code == 401
            assert client.post(path, headers=auth).status_code == 409
            core.leader.complete(*identity, {"status": "failed", "summary": "Too large", "agent_authored": False},
                stopped=True, quiescent=False)
            if exhausted:
                with core.db.transaction() as conn:
                    source = conn.scalar(select(dialogue_inputs.c.context_json).where(dialogue_inputs.c.id == event.event_id))
                    context = json.loads(source)
                    turn = context["leader_turn"]
                    turn["used"]["active_seconds"] = turn["plan"]["budget"]["active_seconds"]
                    conn.execute(update(dialogue_inputs).where(dialogue_inputs.c.id == event.event_id).values(
                        context_json=json.dumps(context)))
            response = client.post(path, headers=auth)
            assert response.status_code == (409 if exhausted else 200)
            if exhausted:
                assert "execution budget exhausted" in response.json()["detail"]
                with core.db.engine.connect() as conn:
                    assert conn.scalar(select(dialogue_inputs.c.state)) == "CONTEXT_FAILED"
            else:
                assert client.post(path, headers=auth).status_code == 409
        pending = core.sessions.claim("fixture")
        assert pending is None if exhausted else pending["source_ids"] == [event.event_id]
    finally:
        await core.stop()
        core.db.close()


@pytest.mark.parametrize("retryable", [False, True])
async def test_inbox_context_failure_is_durable_and_retries_back_off(runtime, retryable):
    from test_agent_runtime import root_goal, worker_complete

    from elephant.storage.agent_schema import leader_inbox
    from elephant.storage.task_schema import notifications, tasks
    root_goal(runtime)
    worker_complete(runtime)
    claim = runtime.sessions.claim("leader")
    assert claim["purpose"] == "finalize"
    identity = (claim["run_id"], claim["fencing_token"], "leader")
    with runtime.db.transaction() as conn:
        conn.execute(insert(sessions).values(id=claim["session_id"], scope_key=claim["session_id"],
            session_file="fixture.jsonl", manifest_json="{}", active_run_id=claim["run_id"], epoch=1))
    for attempt in range(3 if retryable else 1):
        failure = {"phase": "failure", "reason": "timeout" if retryable else "context_budget_exceeded",
            "retryable": retryable, "fingerprint": "expanded-task-context"}
        await runtime.leader.invoke("context-compaction", identity, failure)
        await runtime.leader.invoke("context-compaction", identity, failure)
        runtime.leader.complete(*identity, {"status": "failed", "summary": "Context failed", "agent_authored": False},
            stopped=True, quiescent=False)
        assert runtime.sessions.claim("leader") is None
        with runtime.db.engine.connect() as conn:
            source = conn.execute(select(leader_inbox).where(leader_inbox.c.id == claim["source_ids"][0])).mappings().one()
            state = json.loads(source["output_ref"])
            assert state["retry_count"] == attempt + 1
            assert state["context_failure"]["fingerprint"] == "expanded-task-context"
            assert conn.scalar(select(tasks.c.state).where(tasks.c.id == claim["target_root_task_id"])) == "WAITING_CHILDREN"
        runtime.clock.advance(45)
        if retryable and attempt < 2:
            claim = runtime.sessions.claim("leader")
            assert claim and claim["purpose"] == "finalize"
            identity = (claim["run_id"], claim["fencing_token"], "leader")
    assert runtime.sessions.claim("leader") is None
    with runtime.db.engine.connect() as conn:
        assert conn.scalar(select(leader_inbox.c.handled_at).where(leader_inbox.c.id == claim["source_ids"][0]))
        assert conn.scalar(select(notifications.c.id).where(notifications.c.id == "context-failure:" + claim["source_ids"][0]))


async def test_compaction_and_subsequent_model_requests_recheck_historical_authority(
    tmp_path, official_config, official_event, clock, monkeypatch
):
    core = make_core(tmp_path, official_config, clock)
    revoked = False

    def validate(_access, context):
        return not (revoked and context.get("old_compacted_source"))

    monkeypatch.setattr(core, "validate_context", validate)
    try:
        _, claim = await prepare(core, official_event)
        identity = (claim["run_id"], claim["fencing_token"], "fixture")
        with core.db.transaction() as conn:
            manifest = json.loads(conn.scalar(select(sessions.c.manifest_json)))
            manifest["old_compacted_source"] = True
            conn.execute(update(sessions).values(manifest_json=json.dumps(manifest)))
        started = await core.leader.invoke("context-compaction", identity,
            {"phase": "begin", "reason": "force", "branch_id": "leaf"})
        attempt = {"attempt_id": started["attempt_id"], "branch_id": "leaf"}
        revoked = True
        for phase in ("request", "commit"):
            with pytest.raises(PermissionError, match="session sources"):
                await core.leader.invoke("context-compaction", identity, {**attempt, "phase": phase})
        revoked = False
        await core.leader.invoke("context-compaction", identity, {**attempt, "phase": "commit"})
        await core.leader.invoke("context-compaction", identity, {**attempt, "phase": "saved", "entry_id": "native"})
        revoked = True
        with pytest.raises(PermissionError, match="session sources"):
            await core.leader.invoke("authorize", identity, {"action": "model.request"})
    finally:
        await core.stop()
        core.db.close()


@pytest.mark.parametrize("delta", [-1, 0, 1])
async def test_elastic_threshold_is_inclusive_after_settlement(tmp_path, official_config, official_event, clock, delta):
    core = make_core(tmp_path, official_config, clock)
    try:
        _, claim = await prepare(core, official_event)
        threshold = claim["context"]["limits"]["elastic_compaction_tokens"]
        identity = (claim["run_id"], claim["fencing_token"], "fixture")
        await core.leader.invoke("context-compaction", identity, {"phase": "status", "tokens": threshold + delta})
        assert core.leader.compaction.claim("fixture") is None
        finish_leader(core, claim, "done")
        maintenance = core.leader.compaction.claim("fixture")
        assert bool(maintenance) is (delta >= 0)
    finally:
        await core.stop()
        core.db.close()


async def test_waiting_for_another_worker_does_not_block_elastic_maintenance(runtime):
    from test_agent_runtime import leader_complete, root_goal, worker_complete

    runtime.sessions.enqueue(runtime.event, runtime.access)
    leader = runtime.sessions.claim("leader")
    with runtime.db.transaction() as conn:
        conn.execute(insert(sessions).values(id=leader["session_id"], scope_key=leader["session_id"],
            session_file="fixture.jsonl", manifest_json=json.dumps({"context_compaction": {"elastic_due": True}}),
            active_run_id=leader["run_id"], epoch=1))
    leader_complete(runtime, leader)
    root_goal(runtime, children=2)
    worker_complete(runtime)
    assert runtime.service.claim("still-running")
    assert runtime.sessions.claim("leader") is None
    maintenance = runtime.leader.compaction.claim("maintenance")
    assert maintenance and maintenance["session_id"] == leader["session_id"]

    independent = runtime.event.model_copy(update={"event_id": "foreground", "platform_message_id": "foreground"})
    runtime.sessions.enqueue(independent, runtime.access)
    assert runtime.leader.renew(maintenance["run_id"], maintenance["fencing_token"],
                                "maintenance")["state"] == "CANCEL_REQUESTED"


@pytest.mark.parametrize("automatic", [False, True])
async def test_failed_coordination_event_can_resume_original_finalization(
    tmp_path, official_config, official_event, clock, automatic, monkeypatch
):
    from types import SimpleNamespace

    from conftest import official_payload
    from fastapi.testclient import TestClient
    from test_agent_runtime import root_goal, worker_complete

    from elephant.app import create_app

    monkeypatch.setenv("ELEPHANT_ADMIN_TOKEN", "recovery-token")
    core = make_core(tmp_path, official_config, clock)
    try:
        event = core.adapters["qq"].normalize(official_payload(official_event, private=True))
        runtime = SimpleNamespace(event=event, access=core.access(event), db=core.db,
                                  service=core.tasks, sessions=core.sessions)
        root_goal(runtime)
        worker_complete(runtime)
        claim = core.sessions.claim("leader")
        source_id, = claim["source_ids"]
        limits = claim["context"]["limits"]
        with core.db.transaction() as conn:
            conn.execute(insert(sessions).values(id=claim["session_id"], scope_key=claim["session_id"],
                session_file="fixture.jsonl", manifest_json="{}", active_run_id=claim["run_id"], epoch=1))
        identity = (claim["run_id"], claim["fencing_token"], "leader")
        await core.leader.invoke("context-compaction", identity, {
            "phase": "failure", "reason": "context_budget_exceeded", "deterministic": True,
            "tokens": limits["force_compaction_tokens"] + 100, "request_bytes": 10000,
            "limits_fingerprint": f"fixture:{claim['binding']['context_window']}:{limits['force_compaction_tokens']}:{limits['max_bytes']}",
        })
        core.leader.complete(*identity, {"status": "failed", "summary": "Too large", "agent_authored": False},
                             stopped=True, quiescent=False)
        assert core.sessions.claim("leader") is None
        if automatic:
            with core.db.transaction() as conn:
                conn.execute(update(sessions).values(manifest_json=json.dumps({
                    "context_compaction": {"saved_record": {
                        "entry_id": "new-summary", "before_tokens": 11000, "after_tokens": 10000}}
                })))
        else:
            app = create_app(core.settings, clock=clock, run_background=False)
            with TestClient(app) as client:
                path = "/api/v1/context-inputs/" + source_id + "/recover"
                auth = {"Authorization": "Bearer recovery-token"}
                assert client.post(path).status_code == 401
                assert client.post(path, headers=auth).status_code == 200
                assert client.post(path, headers=auth).status_code == 409
        recovered = core.sessions.claim("leader")
        assert recovered and recovered["purpose"] == "finalize" and recovered["source_ids"] == [source_id]
    finally:
        await core.stop()
        core.db.close()


def test_compaction_poll_uses_candidate_index(db, clock):
    candidates = select(sessions).where(sessions.c.active_run_id.is_(None), compaction_due,
                                        compaction_retry_at <= clock.now().isoformat())
    compiled = candidates.compile(db.engine, compile_kwargs={"literal_binds": True})
    with db.engine.connect() as conn:
        plan = conn.exec_driver_sql("EXPLAIN QUERY PLAN " + str(compiled)).all()
    assert any("ix_pi_sessions_compaction_due" in row[-1] for row in plan)
