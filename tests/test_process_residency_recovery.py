"""Only root-proved dead daemon processes stop consuming resident capacity."""
import json
from uuid import uuid4

import pytest
from sqlalchemy import insert, select
from test_agent_runtime import runtime as agent_runtime
from test_runtime_recovery import NEW_EPOCH, OLD_EPOCH, bound_worker, prove_stop
from test_runtime_recovery import recovery as runtime_recovery

from qqbot.storage.task_schema import processes, runs, sessions

recovery = runtime_recovery
runtime = agent_runtime
UNPROVED_EPOCH = "c" * 32


def idle_process(recovery, *, epoch=OLD_EPOCH, active_run=None):
    process_id, session_id = str(uuid4()), str(uuid4())
    with recovery.runtime.db.transaction() as conn:
        conn.execute(insert(processes).values(id=process_id, session_id=session_id, worker="daemon",
            epoch=1, generation=1, state="IDLE", identity_json=json.dumps({"host_epoch": epoch,
                "launch_task_id": "original-launch"}), last_used=recovery.runtime.clock.now().isoformat()))
        conn.execute(insert(sessions).values(id=session_id, scope_key=session_id, session_file=session_id + ".jsonl",
            manifest_json="{}", active_run_id=active_run, epoch=1))
    return process_id, session_id


def test_old_idle_residents_reconcile_only_with_proof_and_keep_active_run_ownership(recovery):
    claim = bound_worker(recovery)
    with recovery.runtime.db.engine.connect() as conn:
        active_process = conn.execute(select(runs.c.process_id).where(runs.c.id == claim["run_id"])).scalar_one()
        historical = dict(conn.execute(select(runs).where(runs.c.id == claim["run_id"])).mappings().one())
    old_a, session_a = idle_process(recovery, active_run="finished-old-run")
    old_b, _ = idle_process(recovery)
    unproved, _ = idle_process(recovery, epoch=UNPROVED_EPOCH)
    current, _ = idle_process(recovery, epoch=NEW_EPOCH)
    with recovery.runtime.db.transaction() as conn:
        conn.execute(insert(runs).values(**{**historical, "id": "finished-old-run", "session_id": session_a,
            "process_id": old_a, "slot_reserved": 0, "state": "FINISHED"}))
    assert recovery.service.recover_processes() == []
    with recovery.runtime.db.engine.connect() as conn:
        assert conn.execute(select(processes.c.state).where(processes.c.id == active_process)).scalar_one() == "BUSY"
        assert conn.execute(select(runs.c.slot_reserved).where(runs.c.id == claim["run_id"])).scalar_one() == 1
    prove_stop(recovery)
    # Even a marker claiming the current epoch is stopped cannot retire current-daemon residents.
    recovery.receipts[NEW_EPOCH] = {**recovery.receipts[OLD_EPOCH], "host_epoch": NEW_EPOCH}
    retired = recovery.service.recover_processes()
    assert {item["process_id"] for item in retired} == {active_process, old_a, old_b}
    assert recovery.service.recover_processes() == []
    with recovery.runtime.db.engine.connect() as conn:
        states = dict(conn.execute(select(processes.c.id, processes.c.state)).all())
        assert states[active_process] == states[old_a] == states[old_b] == "STOPPED"
        assert states[unproved] == states[current] == "IDLE"
        assert conn.execute(select(sessions.c.active_run_id).where(sessions.c.id == session_a)).scalar_one() is None
        assert conn.execute(select(sessions.c.active_run_id).where(sessions.c.id == claim["session_id"])).scalar_one() == claim["run_id"]
        assert conn.execute(select(runs.c.slot_reserved).where(runs.c.id == claim["run_id"])).scalar_one() == 1
        assert conn.execute(select(runs.c.state).where(runs.c.id == claim["run_id"])).scalar_one() == "RUNNING"


def test_new_bind_waits_for_root_proof_of_old_idle_resident(recovery):
    from test_agent_runtime import root_goal
    runtime = recovery.runtime
    root_goal(runtime)
    claim = runtime.service.claim("new-worker")
    old_process, _ = idle_process(recovery)
    runtime.service.max_resident = 1
    binding = {"process_id": str(uuid4()), "session_id": claim["session_id"], "epoch": 1, "generation": 1,
        "scope_key": claim["session_id"], "session_file": str(runtime.path / "new-session.jsonl"),
        "identity": {"host_epoch": NEW_EPOCH, "launch_task_id": claim["task_id"]}}
    for reconcile in (False, True):
        if reconcile:
            assert recovery.service.recover_processes() == []
        with pytest.raises(ValueError, match="resident process limit"):
            runtime.service.bind_process(claim["run_id"], claim["fencing_token"], "new-worker", binding)
    prove_stop(recovery)
    assert recovery.service.recover_processes() == [{"process_id": old_process, "state": "STOPPED"}]
    assert runtime.service.bind_process(claim["run_id"], claim["fencing_token"], "new-worker", binding) == {"bound": True}
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(processes.c.state).where(processes.c.id == binding["process_id"])).scalar_one() == "BUSY"
        assert conn.execute(select(runs.c.slot_reserved).where(runs.c.id == claim["run_id"])).scalar_one() == 1


async def test_startup_retires_proved_residents_before_attempt_recovery_and_admission(
    tmp_path, official_config, clock, monkeypatch,
):
    from test_leader_core import make_core
    core = make_core(tmp_path, official_config, clock)
    events = []
    def residents():
        assert core.scheduler is not None
        assert core.tasks.job_tree_quiescent.__self__ is core.job_runtime
        events.append("residents")
    def attempts():
        assert events[0] == "residents"
        events.append("attempts")
    def admit(*args, **kwargs):
        assert events == ["residents", "attempts", "attempts", "attempts"]
    monkeypatch.setattr(core.recovery, "recover_processes", residents)
    monkeypatch.setattr(core.tasks, "recover", attempts)
    monkeypatch.setattr(core.recovery, "recover_workers", attempts)
    monkeypatch.setattr(core.leader, "recover", attempts)
    monkeypatch.setattr(core.supervisor, "start", admit)
    core.stopping = True
    try:
        await core.run()
        assert events == ["residents", "attempts", "attempts", "attempts"]
    finally:
        await core.stop()
        core.db.close()
