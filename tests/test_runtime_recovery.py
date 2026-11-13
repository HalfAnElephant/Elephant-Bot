import asyncio
import importlib.machinery
import importlib.util
import json
import os
import stat
import threading
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import select, update
from test_agent_runtime import root_goal
from test_agent_runtime import runtime as agent_runtime_fixture

from qqbot.capabilities.ledger import SQLiteActionLedger
from qqbot.capabilities.types import Action, ToolContext
from qqbot.capabilities.verification import GitHubVerifier
from qqbot.permissions import PermissionRule
from qqbot.runtime.recovery import RuntimeRecovery
from qqbot.storage.action_schema import operations
from qqbot.storage.agent_schema import leader_inbox
from qqbot.storage.schema import outbox
from qqbot.storage.task_schema import inputs, processes, runs, tasks, waits

runtime = agent_runtime_fixture

OLD_EPOCH = "a" * 32
NEW_EPOCH = "b" * 32
BOOT_ID = "11111111-1111-4111-8111-111111111111"


@pytest.fixture
def recovery(runtime, monkeypatch):
    core = runtime.leader.core
    core.settings, core.sessions, core.leader = runtime.sessions.settings, runtime.sessions, runtime.leader
    monkeypatch.setenv("INVOCATION_ID", NEW_EPOCH)
    receipts = {}
    def root_receipt(path):
        if path.stem not in receipts:
            raise FileNotFoundError(path)
        return receipts[path.stem]
    monkeypatch.setattr("qqbot.runtime.recovery.trusted_json", root_receipt)
    value = RuntimeRecovery(core, directory=runtime.path / "root-stops")
    runtime.service.stop_verifier = value.stopped
    return SimpleNamespace(runtime=runtime, core=core, service=value, receipts=receipts)


def prove_stop(recovery):
    recovery.receipts[OLD_EPOCH] = {"host_epoch": OLD_EPOCH, "stopped": True, "cgroup_empty": True,
        "cgroup": "/system.slice/elephant.service", "boot_id": BOOT_ID}


def bound_worker(recovery):
    runtime = recovery.runtime
    root_goal(runtime)
    claim = runtime.service.claim("daemon")
    claim["host_epoch"] = OLD_EPOCH
    path = runtime.path / "pi" / claim["session_id"] / "sessions" / (claim["session_id"] + ".jsonl")
    path.parent.mkdir(parents=True)
    workspace = runtime.path / "workspaces" / claim["task_id"]
    (workspace / "outputs").mkdir(parents=True)
    process_id = str(uuid4())
    runtime.service.bind_process(claim["run_id"], claim["fencing_token"], "daemon", {
        "process_id": process_id, "session_id": claim["session_id"], "epoch": 1, "generation": 1,
        "scope_key": claim["session_id"], "session_file": str(path),
        "identity": {"host_epoch": OLD_EPOCH, "launch_task_id": claim["task_id"]}})
    with runtime.db.transaction() as conn:
        conn.execute(update(runs).where(runs.c.id == claim["run_id"]).values(request_json=json.dumps(claim)))
    claim["native_path"], claim["workspace"] = path, workspace
    return claim


def native_log(claim, *, boundary=True, tool=False, paired=True, yielded=False, outcome=None):
    entries = []
    def entry(kind, **data):
        identifier = str(len(entries))
        entries.append({"id": identifier, "parentId": entries[-1]["id"] if entries else None, "type": kind, **data})
    entry("message", message={"role": "user", "content": [{"type": "text", "text": "检查项目"}]})
    entry("custom", customType="elephant-consumed", data={"run_id": claim["run_id"],
        "input_ids": claim["context"]["source_input_ids"]})
    if tool:
        entry("message", message={"role": "assistant", "content": [{"type": "toolCall", "id": "write-1",
            "name": "capability_invoke", "arguments": {"name": "workspace.write", "arguments": {"path": "outputs/a.txt", "text": "done"}}}]})
        if paired:
            entry("message", message={"role": "toolResult", "toolCallId": "write-1", "toolName": "capability_invoke",
                "content": [{"type": "text", "text": "verified"}], "isError": False})
    if yielded:
        entry("custom", customType="elephant-yield-boundary", data={"run_id": claim["run_id"], "paired_results": ["write-1"]})
    entry("message", message={"role": "assistant", "content": [{"type": "text", "text": "检查完成"}]})
    proof = {"kind": "run_yielded" if yielded else "turn_finished", "run_id": claim["run_id"],
        "session_id": claim["session_id"], "session_revision": entries[-1]["id"],
        "input_ids": claim["context"]["source_input_ids"], "inflight": 0, "tools_paired": True,
        "summary": "检查完成", "outcome": outcome}
    if boundary:
        entry("custom", customType="elephant-turn-boundary", data=proof)
    claim["native_path"].write_text("".join(json.dumps(value, ensure_ascii=False) + "\n" for value in entries))
    return proof


async def operation(recovery, claim, *, state="DISPATCHED", action=None, arguments=None, data=None):
    runtime = recovery.runtime
    context = ToolContext(task_id=claim["task_id"], run_id=claim["run_id"], fence=claim["fencing_token"],
        principal=runtime.access, workspace=str(claim["workspace"]))
    ledger = SQLiteActionLedger(runtime.db)
    action = action or Action("workspace.file.write", ("workspace:" + str(claim["workspace"]),), "write")
    arguments = arguments or {"path": "outputs/a.txt", "text": "done"}
    await ledger.prepare("op-1", context, action, "digest", arguments)
    if state != "PREPARED":
        await ledger.transition("op-1", "PREPARED", "DISPATCHED", {})
    if state not in {"PREPARED", "DISPATCHED"}:
        await ledger.transition("op-1", "DISPATCHED", state, data or {})


def expire(recovery):
    recovery.runtime.clock.advance(31)
    recovery.runtime.service.recover()


def task_row(runtime, task_id):
    with runtime.db.engine.connect() as conn:
        return conn.execute(select(tasks).where(tasks.c.id == task_id)).mappings().one()


def test_stop_receipt_requires_old_epoch_identity_and_complete_root_attestation(recovery):
    claim = bound_worker(recovery)
    with recovery.runtime.db.engine.connect() as conn:
        process_id = conn.execute(select(runs.c.process_id).where(runs.c.id == claim["run_id"])).scalar_one()
    prove_stop(recovery)
    assert recovery.service.stopped(claim["task_id"], process_id)
    assert not recovery.service.stopped("another-task", process_id)
    recovery.receipts[OLD_EPOCH]["cgroup_empty"] = False
    assert not recovery.service.stopped(claim["task_id"], process_id)
    recovery.receipts[OLD_EPOCH]["cgroup_empty"] = True
    recovery.receipts[OLD_EPOCH]["boot_id"] = ""
    assert not recovery.service.stopped(claim["task_id"], process_id)
    recovery.receipts[OLD_EPOCH]["boot_id"] = BOOT_ID
    recovery.service.epoch = OLD_EPOCH
    assert not recovery.service.stopped(claim["task_id"], process_id)


def test_missing_stop_proof_retains_writer_and_capacity(recovery):
    claim = bound_worker(recovery)
    native_log(claim)
    expire(recovery)
    assert recovery.service.recover_workers()[0]["state"] == "NEEDS_RECONCILIATION"
    with recovery.runtime.db.engine.connect() as conn:
        assert conn.execute(select(runs.c.slot_reserved)).scalar_one() == 1
    assert task_row(recovery.runtime, claim["task_id"])["state"] == "NEEDS_RECONCILIATION"


def test_current_daemon_verified_stop_recovers_after_lease_expiry(recovery):
    runtime = recovery.runtime
    claim = bound_worker(recovery)
    recovery.service.epoch = OLD_EPOCH
    with runtime.db.engine.connect() as conn:
        run = conn.execute(select(runs).where(runs.c.id == claim["run_id"])).mappings().one()
    runtime.service.process_stopped("daemon", {"process_id": run["process_id"], "epoch": 1,
        "generation": 1, "stopped": True})
    assert recovery.service.stopped(claim["task_id"], run["process_id"])
    assert recovery.service.recover_worker(claim["run_id"])["state"] == "RUNNING"
    expire(recovery)
    assert recovery.service.recover_worker(claim["run_id"])["state"] == "QUEUED"
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(runs.c.slot_reserved).where(runs.c.id == claim["run_id"])).scalar_one() == 0
    next_claim = runtime.service.claim("daemon")
    assert next_claim["task_id"] == claim["task_id"]
    assert next_claim["run_id"] != claim["run_id"]


def test_stopped_process_from_another_binding_does_not_release_run(recovery):
    runtime = recovery.runtime
    claim = bound_worker(recovery)
    with runtime.db.transaction() as conn:
        conn.execute(update(processes).values(state="STOPPED", generation=2))
    expire(recovery)
    assert recovery.service.recover_worker(claim["run_id"])["state"] == "NEEDS_RECONCILIATION"
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(runs.c.slot_reserved).where(runs.c.id == claim["run_id"])).scalar_one() == 1


def test_uncertain_stopped_attempt_releases_capacity_and_preserves_recovery(recovery):
    runtime = recovery.runtime
    claim = bound_worker(recovery)
    native_log(claim)
    recovery.service.epoch = OLD_EPOCH
    with runtime.db.engine.connect() as conn:
        run = conn.execute(select(runs).where(runs.c.id == claim["run_id"])).mappings().one()
    runtime.service.process_stopped("daemon", {"process_id": run["process_id"], "epoch": 1,
        "generation": 1, "stopped": True})
    assert runtime.service.uncertain(claim["run_id"], claim["fencing_token"], "daemon")["state"] == "NEEDS_RECONCILIATION"
    with runtime.db.engine.connect() as conn:
        saved = conn.execute(select(runs).where(runs.c.id == claim["run_id"])).mappings().one()
    assert saved["slot_reserved"] == 0 and saved["runner_stopped_at"]
    assert recovery.service.recover_workers()[0]["state"] == "SUCCEEDED"
    assert recovery.service.recover_workers() == []


def test_legacy_worker_cannot_query_tasks_without_a_root(recovery):
    runtime = recovery.runtime
    claim = bound_worker(recovery)
    with runtime.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.id == claim["task_id"]).values(root_task_id=None))
    with pytest.raises(PermissionError, match="no root goal"):
        runtime.service.task_operation(claim["run_id"], claim["fencing_token"], "daemon", {"operation": "tasks.list"})


async def test_maintenance_recovery_leaves_event_loop_available():
    from qqbot.runtime.core import Core

    entered, release = threading.Event(), threading.Event()
    loop_thread = threading.get_ident()
    calls = []

    def workers():
        assert threading.get_ident() != loop_thread
        entered.set()
        assert release.wait(3)
        calls.append("workers")

    def leaders():
        assert threading.get_ident() != loop_thread
        calls.append("leaders")

    async def sync():
        calls.append("sync")

    core = SimpleNamespace(recovery=SimpleNamespace(recover_workers=workers),
        leader=SimpleNamespace(recover=leaders),
        knowledge=SimpleNamespace(recover_mutations=lambda: None, sync=sync))
    maintenance = asyncio.create_task(Core.maintenance(core))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        assert not maintenance.done()
        # A renewal or cancellation can run while native recovery is blocked.
        await asyncio.sleep(0)
    finally:
        release.set()
        await maintenance
    assert calls == ["workers", "leaders", "sync"]


@pytest.mark.parametrize("operation", ["maintenance", "cleanup_artifacts"])
async def test_background_file_work_finishes_before_shutdown(operation, monkeypatch, tmp_path):
    from qqbot.runtime.core import Core

    entered, release, finished = threading.Event(), threading.Event(), threading.Event()

    def blocking(*args):
        entered.set()
        assert release.wait(3)
        finished.set()

    async def sync():
        pass

    monkeypatch.setattr("qqbot.harness.artifacts.cleanup_artifacts", blocking)
    monkeypatch.setattr("qqbot.harness.attachments.cleanup_attachment_cache", blocking)
    core = SimpleNamespace(recovery=SimpleNamespace(recover_workers=blocking),
        leader=SimpleNamespace(recover=lambda: None),
        knowledge=SimpleNamespace(recover_mutations=lambda: None, sync=sync),
        db=SimpleNamespace(path=tmp_path / "state.db"), tasks=SimpleNamespace(archive_root=None),
        clock=SimpleNamespace(now=lambda: None))
    work = asyncio.create_task(getattr(Core, operation)(core))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        work.cancel()
        await asyncio.sleep(0)
        assert not work.done()
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await work
    assert finished.is_set()


def test_native_completed_boundary_restores_original_result_artifacts_and_one_inbox(recovery):
    runtime = recovery.runtime
    claim = bound_worker(recovery)
    native_log(claim, outcome={"status": "completed", "attachments": ["outputs/report.txt"]})
    (claim["workspace"] / "outputs/report.txt").write_text("verified report")
    with runtime.db.transaction() as conn:
        goal = json.loads(task_row(runtime, claim["task_id"])["goal_json"])
        goal["requested_outputs"] = ["outputs/report.txt"]
        conn.execute(update(tasks).where(tasks.c.id == claim["task_id"]).values(goal_json=json.dumps(goal)))
    prove_stop(recovery)
    expire(recovery)
    assert recovery.service.recover_workers()[0]["state"] == "SUCCEEDED"
    result = json.loads(task_row(runtime, claim["task_id"])["result_json"])
    assert result["summary"] == "检查完成"
    assert result["artifacts"][0]["relative_path"] == "outputs/report.txt"
    assert (runtime.service.archive_root / result["artifacts"][0]["id"]).read_text() == "verified report"
    assert recovery.service.recover_workers() == []
    with runtime.db.engine.connect() as conn:
        assert len(conn.execute(select(runs)).all()) == 1
        assert len(conn.execute(select(leader_inbox).where(leader_inbox.c.handled_at.is_(None))).all()) == 1
        assert conn.execute(select(processes.c.state)).scalar_one() == "STOPPED"


def test_native_partial_outcome_is_not_promoted_to_success(recovery):
    claim = bound_worker(recovery)
    native_log(claim, outcome={"status": "partial", "unresolved_items": ["one check remains"]})
    prove_stop(recovery)
    expire(recovery)
    assert recovery.service.recover_workers()[0]["state"] == "PARTIAL"
    result = json.loads(task_row(recovery.runtime, claim["task_id"])["result_json"])
    assert result["limitations"] == ["one check remains"]


def test_validated_result_spool_wins_without_regenerating_artifact_ids(recovery):
    from qqbot.harness.artifacts import export_artifacts
    from qqbot.jobs.worker import Spool

    runtime = recovery.runtime
    claim = bound_worker(recovery)
    proof = native_log(claim)
    (claim["workspace"] / "outputs/a.txt").write_text("done")
    artifacts = export_artifacts(claim["workspace"], runtime.service.archive_root, ["outputs/a.txt"])
    spool = Spool(runtime.path / "spool")
    spool.save({"run_id": claim["run_id"], "fencing_token": claim["fencing_token"], "receipt": proof,
        "stopped": False, "quiescent": True, "result": {"status": "completed", "summary": "原有结果", "artifacts": artifacts}})
    prove_stop(recovery)
    expire(recovery)
    assert recovery.service.recover_workers()[0]["state"] == "SUCCEEDED"
    result = json.loads(task_row(runtime, claim["task_id"])["result_json"])
    assert result["artifacts"] == artifacts
    assert result["summary"] == "原有结果"
    assert len(list(runtime.service.archive_root.iterdir())) == 1


async def test_paired_continuation_keeps_task_session_budget_and_verified_side_effects(recovery):
    runtime = recovery.runtime
    claim = bound_worker(recovery)
    native_log(claim, boundary=False, tool=True)
    target = claim["workspace"] / "outputs/a.txt"
    target.write_text("done")
    before = target.stat().st_mtime_ns
    await operation(recovery, claim, state="VERIFIED", data={"path": "outputs/a.txt"})
    runtime.service.reserve_budget(claim["run_id"], claim["fencing_token"], "daemon", {"request_id": "request-1", "kind": "business"})
    prove_stop(recovery)
    expire(recovery)
    assert recovery.service.recover_workers()[0]["state"] == "QUEUED"
    assert recovery.service.recover_workers() == []
    continued = runtime.service.claim("replacement")
    assert continued["task_id"] == claim["task_id"]
    assert continued["session_id"] == claim["session_id"]
    assert continued["run_id"] != claim["run_id"]
    assert continued["goal"] == claim["goal"]
    assert target.stat().st_mtime_ns == before
    assert json.loads(task_row(runtime, claim["task_id"])["budget_json"])["model_calls"] == 1
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(operations.c.state)).scalar_one() == "VERIFIED"


async def test_unpaired_write_never_reenters_the_model_and_releases_only_stopped_resources(recovery):
    runtime = recovery.runtime
    claim = bound_worker(recovery)
    native_log(claim, boundary=False, tool=True, paired=False)
    await operation(recovery, claim)
    prove_stop(recovery)
    expire(recovery)
    assert recovery.service.recover_workers()[0]["reason"] == "unpaired_tools"
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(operations.c.state)).scalar_one() == "UNKNOWN"
        assert conn.execute(select(runs.c.slot_reserved)).scalar_one() == 0
        assert conn.execute(select(processes.c.state)).scalar_one() == "STOPPED"
    assert runtime.service.claim("replacement") is None
    result = recovery.service.reconcile_task(task_row(runtime, claim["task_id"])["public_id"], runtime.access,
        claim["run_id"], resume=True)
    assert result["state"] == "NEEDS_RECONCILIATION"


def test_paired_wait_recovers_original_question_without_a_second_model_turn(recovery):
    runtime = recovery.runtime
    claim = bound_worker(recovery)
    runtime.service.ask(claim["run_id"], claim["fencing_token"], "daemon",
        {"question_id": "question-1", "text": "选择哪个分支？", "checkpoint": "检查后等待选择"})
    native_log(claim, boundary=False, tool=True, paired=True, yielded=True)
    prove_stop(recovery)
    expire(recovery)
    assert recovery.service.recover_workers()[0]["state"] == "WAITING_INPUT"
    assert runtime.service.claim("replacement") is None
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(waits.c.state)).scalar_one() == "WAITING"
        assert len(conn.execute(select(waits)).all()) == 1
        assert len(conn.execute(select(leader_inbox).where(leader_inbox.c.handled_at.is_(None))).all()) == 1


async def test_readback_uses_existing_verifier_gets_and_preserves_unknown_until_evidence(recovery):
    runtime = recovery.runtime
    claim = bound_worker(recovery)
    native_log(claim, tool=True, paired=True)
    arguments = {"argv": ["issue", "close", "7", "--repo", "owner/repo"], "_preconditions": {
        "repository": "owner/repo", "number": "7", "actor": "alice", "target_id": 123,
        "requested": {"state": "closed"}, "before": {"state": "open"}}}
    await operation(recovery, claim, action=Action("github.issue.close", ("repo:github.com/owner/repo",), "write"), arguments=arguments)
    prove_stop(recovery)
    expire(recovery)
    assert recovery.service.recover_workers()[0]["reason"] == "uncertain_operations"
    commands = []
    async def github(args, context):
        commands.append(args["argv"])
        return {"exit_code": 0, "stdout": json.dumps({"id": 123, "state": "closed"})}
    recovery.service._github = GitHubVerifier(github)
    task = task_row(runtime, claim["task_id"])
    status = await recovery.service.readback_operation(task["public_id"], runtime.access, "op-1")
    assert status["state"] == "PARTIAL"
    assert commands == [["api", "repos/owner/repo/issues/7", "--method", "GET"]]
    assert runtime.service.claim("replacement") is None
    with pytest.raises(PermissionError, match="uncertain operations"):
        recovery.service.reconcile_task(task["public_id"], runtime.access, claim["run_id"],
            result={"status": "completed", "summary": "done", "agent_authored": False})


async def test_unknown_delivery_readback_confirms_only_durable_enqueue_and_never_resends(recovery):
    from qqbot.domain import stable_id

    runtime = recovery.runtime
    claim = bound_worker(recovery)
    await operation(recovery, claim, action=Action("notification.send", ("task:" + claim["task_id"],), "write"),
        arguments={"request_id": "message-1", "text": "result"})
    with runtime.db.transaction() as conn:
        conn.execute(update(operations).where(operations.c.id == "op-1").values(state="UNKNOWN"))
        runtime.service.outbox.enqueue_text(conn, target=runtime.event.conversation, text="result", origin_kind="task",
            origin_id=stable_id("qq-rich", claim["task_id"], "message-1"), requester=runtime.event.principal,
            parent_task_id=claim["task_id"])
        conn.execute(update(outbox).values(state="UNKNOWN"))
    task = task_row(runtime, claim["task_id"])
    status = await recovery.service.readback_operation(task["public_id"], runtime.access, "op-1")
    assert status["state"] == "VERIFIED"
    assert status["result"]["state"] == "UNKNOWN"
    assert status["result"]["delivered"] is False
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(outbox.c.state)).scalar_one() == "UNKNOWN"
        assert len(conn.execute(select(outbox)).all()) == 1


async def test_readback_rechecks_current_read_permission_and_subject(recovery):
    runtime = recovery.runtime
    claim = bound_worker(recovery)
    await operation(recovery, claim, state="UNKNOWN")
    task = task_row(runtime, claim["task_id"])
    other = runtime.access.model_copy(update={"principal": runtime.access.principal.model_copy(update={"remote_user_id": "other"})})
    with pytest.raises(PermissionError):
        recovery.service.list_operations(task["public_id"], other)
    runtime.service.permissions.upsert(PermissionRule(id="deny-readback", effect="deny", actions=("workspace.file.read",)),
        expected_version=0, actor="test")
    with pytest.raises(PermissionError):
        await recovery.service.readback_operation(task["public_id"], runtime.access, "op-1")


def load_stop_helper():
    path = Path(__file__).resolve().parents[1] / "scripts/elephant-daemon-stopped"
    loader = importlib.machinery.SourceFileLoader("elephant_daemon_stopped", str(path))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    return module


def test_root_stop_helper_checks_nested_cgroups(tmp_path):
    helper = load_stop_helper()
    (tmp_path / "cgroup.procs").write_text("123\n")
    child = tmp_path / "child"
    child.mkdir()
    (child / "cgroup.procs").write_text("456\n")
    assert not helper.cgroup_is_drained(tmp_path, 123)
    (child / "cgroup.procs").write_text("")
    assert helper.cgroup_is_drained(tmp_path, 123)


def test_root_stop_receipt_stays_daemon_readable_under_umask_0077(tmp_path, monkeypatch):
    helper = load_stop_helper()
    original_fstat = os.fstat
    def root_stat(fd):
        values = list(original_fstat(fd))
        values[4] = 0
        return os.stat_result(values)
    monkeypatch.setattr(helper.os, "fstat", root_stat)
    mask = os.umask(0o077)
    directory = tmp_path / "root-stops"
    try:
        helper.write_receipt(directory, OLD_EPOCH, {"host_epoch": OLD_EPOCH, "stopped": True})
    finally:
        os.umask(mask)
    assert stat.S_IMODE(directory.stat().st_mode) == 0o755
    assert stat.S_IMODE((directory / (OLD_EPOCH + ".json")).stat().st_mode) == 0o644
    assert json.loads((directory / (OLD_EPOCH + ".json")).read_text())["stopped"] is True


def test_root_stop_helper_refuses_an_unprivileged_caller(monkeypatch):
    helper = load_stop_helper()
    monkeypatch.setattr(helper.os, "geteuid", lambda: 501)
    with pytest.raises(SystemExit, match="root is required"):
        helper.main()


def test_stop_helper_proves_the_service_root_above_control_subgroups():
    helper = load_stop_helper()
    assert helper.service_cgroup("/system.slice/elephant.service/.control") == "/system.slice/elephant.service"
    with pytest.raises(ValueError, match="service cgroup"):
        helper.service_cgroup("/a/random-scope")


async def test_verified_readback_allows_the_original_finished_attempt_to_settle(recovery):
    runtime = recovery.runtime
    claim = bound_worker(recovery)
    native_log(claim, tool=True)
    await operation(recovery, claim)
    prove_stop(recovery)
    expire(recovery)
    assert recovery.service.recover_workers()[0]["reason"] == "uncertain_operations"
    calls = []
    async def verify(context, action, arguments, data):
        calls.append((context.task_id, action.name))
        return "VERIFIED", {"observed": True}
    recovery.service.verifier = verify
    task = task_row(runtime, claim["task_id"])
    result = await recovery.service.readback_operation(task["public_id"], runtime.access, "op-1")
    assert result["state"] == "VERIFIED"
    assert recovery.service.recover_workers()[0]["state"] == "SUCCEEDED"
    assert calls == [(claim["task_id"], "workspace.file.write")]
    with runtime.db.engine.connect() as conn:
        assert len(conn.execute(select(runs)).all()) == 1
        assert conn.execute(select(operations.c.state)).scalar_one() == "VERIFIED"


@pytest.mark.parametrize("new_matches", [False, True])
async def test_rework_readback_uses_the_executing_run_workspace(recovery, new_matches):
    runtime = recovery.runtime
    original = bound_worker(recovery)
    with runtime.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.id == original["task_id"]).values(state="FAILED"))
        conn.execute(update(runs).where(runs.c.id == original["run_id"]).values(slot_reserved=0))
        root = conn.execute(select(tasks).where(tasks.c.id == original["root_task_id"])).mappings().one()
        rework = runtime.service.graph.spawn(conn, root, {"goal": "重做文件", "delegation_key": "retry-file",
            "rework_of_task_id": original["task_id"]}, runtime.access)
    claim = runtime.service.claim("retry-worker")
    assert claim["task_id"] == rework["task_id"]
    destination = runtime.path / "workspaces" / claim["task_id"]
    (destination / "outputs").mkdir(parents=True)
    (original["workspace"] / "outputs/a.txt").write_text("wrong" if new_matches else "expected")
    (destination / "outputs/a.txt").write_text("expected" if new_matches else "wrong")
    identity = (claim["run_id"], claim["fencing_token"], "retry-worker")
    await runtime.service.ledger(*identity, {"operation": "prepare", "operation_id": "rework-file",
        "arguments_hash": "new-file-hash", "arguments": {"path": "outputs/a.txt", "text": "expected"},
        "action": {"name": "workspace.file.write", "effect": "write", "resources": ["workspace:" + str(destination)]}})
    for before, after in (("PREPARED", "DISPATCHED"), ("DISPATCHED", "UNKNOWN")):
        await runtime.service.ledger(*identity, {"operation": "transition", "operation_id": "rework-file",
            "expected": before, "state": after, "result": {}})
    previous = task_row(runtime, original["task_id"])
    result = await recovery.service.readback_operation(previous["public_id"], runtime.access, "rework-file")
    assert result["state"] == ("VERIFIED" if new_matches else "UNKNOWN")


def test_late_native_result_cannot_consume_a_newer_revision(recovery):
    from qqbot.domain.messages import TextSegment

    runtime = recovery.runtime
    claim = bound_worker(recovery)
    native_log(claim)
    event = runtime.event.model_copy(update={"event_id": "new-revision", "segments": (TextSegment(text="只检查，不修改"),)})
    with runtime.db.transaction() as conn:
        row = conn.execute(select(tasks).where(tasks.c.id == claim["task_id"])).mappings().one()
        runtime.service.add_graph_input(conn, row, event, runtime.access, event.text)
    prove_stop(recovery)
    expire(recovery)
    assert recovery.service.recover_workers()[0]["state"] == "QUEUED"
    with runtime.db.engine.connect() as conn:
        row = conn.execute(select(inputs).where(inputs.c.task_id == claim["task_id"])).mappings().one()
        assert row["state"] == "READY"
        assert row["consumed_at"] is None
    continued = runtime.service.claim("replacement")
    assert continued["session_id"] == claim["session_id"]
    assert continued["continuation"]["inputs"][0]["text"] == event.text


async def test_cancelled_attempt_records_late_operation_facts_without_reviving(recovery):
    runtime = recovery.runtime
    claim = bound_worker(recovery)
    native_log(claim, tool=True)
    await operation(recovery, claim)
    with runtime.db.transaction() as conn:
        root = conn.execute(select(tasks).where(tasks.c.id == claim["root_task_id"])).mappings().one()
        runtime.service.graph.cancel_tree(conn, root)
    prove_stop(recovery)
    expire(recovery)
    assert recovery.service.recover_workers()[0]["state"] == "CANCELLED"
    task = task_row(runtime, claim["task_id"])
    async def verify(*_):
        return "VERIFIED", {"effect_observed": True}
    recovery.service.verifier = verify
    assert (await recovery.service.readback_operation(task["public_id"], runtime.access, "op-1"))["state"] == "VERIFIED"
    assert recovery.service.recover_workers() == []
    assert task_row(runtime, claim["task_id"])["state"] == "CANCELLED"
    assert runtime.service.claim("replacement") is None


def test_invalid_artifact_evidence_requires_explicit_reconciliation(recovery):
    runtime = recovery.runtime
    claim = bound_worker(recovery)
    native_log(claim, outcome={"status": "completed", "attachments": ["outputs/missing.txt"]})
    prove_stop(recovery)
    expire(recovery)
    result = recovery.service.recover_workers()[0]
    assert result["state"] == "NEEDS_RECONCILIATION"
    assert result["reason"] == "invalid_recovery_evidence"
    # Another task creating that path later cannot retrospectively become this attempt's output.
    (claim["workspace"] / "outputs/missing.txt").write_text("created later")
    assert recovery.service.recover_workers() == []
    assert task_row(runtime, claim["task_id"])["state"] == "NEEDS_RECONCILIATION"


def test_proven_old_daemon_can_recover_before_its_lease_timeout(recovery):
    claim = bound_worker(recovery)
    native_log(claim)
    prove_stop(recovery)
    assert recovery.service.recover_workers()[0]["state"] == "SUCCEEDED"


def test_activity_budget_charges_until_stop_not_daemon_downtime(recovery):
    from datetime import timedelta

    runtime = recovery.runtime
    claim = bound_worker(recovery)
    native_log(claim, boundary=False)
    prove_stop(recovery)
    recovery.receipts[OLD_EPOCH]["stopped_at"] = (runtime.clock.now() + timedelta(seconds=5)).isoformat()
    runtime.clock.advance(120)
    assert recovery.service.recover_workers()[0]["state"] == "QUEUED"
    budget = json.loads(task_row(runtime, claim["task_id"])["budget_json"])
    assert budget["active_seconds"] == 5
    assert recovery.service.recover_workers() == []
    assert json.loads(task_row(runtime, claim["task_id"])["budget_json"])["active_seconds"] == 5


@pytest.mark.parametrize("reason", ["deadline", "active_budget"])
def test_background_legacy_time_stop_recovers_native_result(recovery, reason):
    runtime = recovery.runtime
    claim = bound_worker(recovery)
    native_log(claim)
    with runtime.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.id == claim["task_id"]).values(state="CANCEL_REQUESTED", stop_reason=reason))
    prove_stop(recovery)
    assert recovery.service.recover_workers()[0]["state"] == "SUCCEEDED"


def test_reconciling_an_already_restored_result_is_idempotent(recovery):
    runtime = recovery.runtime
    claim = bound_worker(recovery)
    native_log(claim)
    prove_stop(recovery)
    assert recovery.service.recover_workers()[0]["state"] == "SUCCEEDED"
    original = task_row(runtime, claim["task_id"])["result_json"]
    task = task_row(runtime, claim["task_id"])
    assert recovery.service.reconcile_task(task["public_id"], runtime.access, claim["run_id"], resume=True)["state"] == "SUCCEEDED"
    assert task_row(runtime, claim["task_id"])["result_json"] == original


async def test_completed_native_result_ignores_unsupported_implicit_sidecar(recovery):
    runtime = recovery.runtime
    claim = bound_worker(recovery)
    native_log(claim, outcome={"status": "completed", "attachments": ["outputs/report.txt"]})
    (claim["workspace"] / "outputs/report.txt").write_text("ISOLATED_OK")
    (claim["workspace"] / "outputs/.keep").write_text("")
    await operation(recovery, claim, state="VERIFIED", arguments={"path": "outputs/.keep", "text": ""},
                    data={"path": "outputs/.keep"})
    prove_stop(recovery)
    expire(recovery)
    assert recovery.service.recover_workers()[0]["state"] == "SUCCEEDED"
    result = json.loads(task_row(runtime, claim["task_id"])["result_json"])
    assert len(result["artifacts"]) == 1
    artifact = result["artifacts"][0]
    assert artifact["relative_path"] == "outputs/report.txt"
    assert (runtime.service.archive_root / artifact["id"]).read_bytes() == b"ISOLATED_OK"


def test_background_old_deadline_survives_daemon_result_recovery(recovery):
    from datetime import timedelta

    runtime = recovery.runtime
    claim = bound_worker(recovery)
    native_log(claim)
    with runtime.db.transaction() as conn:
        conn.execute(update(tasks).values(deadline=(runtime.clock.now() - timedelta(days=1)).isoformat(),
            budget_json=json.dumps({"active_seconds": 999999})))
    prove_stop(recovery)
    expire(recovery)
    assert recovery.service.recover_workers()[0]["state"] == "SUCCEEDED"
    assert runtime.sessions.claim("leader")["purpose"] == "finalize"


def test_unproved_worker_stop_escalates_once_without_releasing_execution(recovery):
    runtime = recovery.runtime
    claim = bound_worker(recovery)
    native_log(claim)
    runtime.clock.advance(31)
    assert recovery.service.recover_worker(claim["run_id"])["state"] == "NEEDS_RECONCILIATION"
    assert recovery.service.recover_worker(claim["run_id"])["state"] == "NEEDS_RECONCILIATION"
    attention = runtime.sessions.claim("leader")
    assert attention["purpose"] == "attention"
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(runs.c.slot_reserved).where(runs.c.id == claim["run_id"])).scalar_one() == 1
        assert len(conn.execute(select(leader_inbox)).all()) == 1


def test_unpaired_native_tool_escalates_to_leader_decision(recovery):
    runtime = recovery.runtime
    claim = bound_worker(recovery)
    native_log(claim, boundary=False, tool=True, paired=False)
    prove_stop(recovery)
    assert recovery.service.recover_worker(claim["run_id"])["reason"] == "unpaired_tools"
    assert runtime.sessions.claim("leader")["purpose"] == "attention"


@pytest.mark.parametrize("protected", ["claimed", "business_blocked"])
def test_recovered_result_preserves_claimed_decision_and_business_alerts(recovery, protected):
    runtime = recovery.runtime
    claim = bound_worker(recovery)
    native_log(claim)
    event_id = "lease-lost:" + claim["run_id"] if protected == "claimed" else "business-blocked"
    with runtime.db.transaction() as conn:
        task = conn.execute(select(tasks).where(tasks.c.id == claim["task_id"])).mappings().one()
        runtime.service.graph.event(conn, task, "needs_attention", {"reason": protected},
            attempt_id=claim["run_id"], event_id=event_id, wake=True)
        if protected == "claimed":
            conn.execute(update(leader_inbox).where(leader_inbox.c.event_id == event_id).values(
                claimed_attempt_id="ongoing-decision"))
    prove_stop(recovery)
    assert recovery.service.recover_worker(claim["run_id"])["state"] == "SUCCEEDED"
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(leader_inbox.c.handled_at).where(leader_inbox.c.event_id == event_id)).scalar_one() is None
