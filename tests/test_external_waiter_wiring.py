import json

import pytest
from sqlalchemy import func, select
from test_leader_core import make_core

from qqbot.domain.execution import TaskGoal
from qqbot.runtime.scheduler import Scheduler
from qqbot.storage.task_schema import inputs, model_requests, tasks, waits


def external_wait(core, payload, *, capability="gh.exec", arguments=None):
    event = core.adapters["qq"].normalize(payload)
    event, _ = core.inbox.accept(event)
    task = core.tasks.accept(event, core.access(event), TaskGoal(text="等待 CI 完成",
        target_refs=("repo:github.com/org/repo",)))
    claim = core.tasks.claim("daemon")
    core.tasks.ask(claim["run_id"], claim["fencing_token"], "daemon", {
        "question_id": "external-ci", "kind": "external", "checkpoint": "等待原有 CI，不启动新的执行",
        "text": "等待 CI 完成", "condition": {"capability": capability,
            "arguments": arguments or {"argv": ["run", "view", "123", "--repo", "org/repo", "--json", "status"]},
            "path": "status", "equals": "completed", "interval_seconds": 15}})
    core.tasks.submit(claim["run_id"], claim["fencing_token"], "daemon",
        {"status": "waiting_external", "summary": "等待 CI 完成"})
    return task, claim


async def test_scheduler_wired_waiter_wakes_persistent_task_via_runtime_client_without_models(
    tmp_path, official_config, official_event, clock, monkeypatch
):
    core = make_core(tmp_path, official_config, clock)
    observed, configured = [], []
    async def github(arguments, context):
        observed.append((arguments["argv"], context.host, context.repository))
        return {"exit_code": 0, "stdout": json.dumps({"status": "completed"}), "truncated": False}
    def configured_github(args):
        configured.append(args.github_host)
        return github
    monkeypatch.setattr("qqbot.runtime.scheduler.configured_github", configured_github)
    task, first = external_wait(core, official_event)
    core.scheduler = Scheduler(core, core.leader)
    try:
        await core.scheduler.waiters.once()
        await core.scheduler.waiters.once()
        assert configured == ["github.com"]
        assert observed == [(["run", "view", "123", "--repo", "org/repo", "--json", "status"], "github.com", "org/repo")]
        with core.db.engine.connect() as conn:
            assert conn.execute(select(tasks.c.state).where(tasks.c.id == task["id"])).scalar_one() == "QUEUED"
            assert conn.execute(select(waits.c.state)).scalar_one() == "CONSUMED"
            assert conn.execute(select(func.count()).select_from(inputs)).scalar_one() == 1
            assert conn.execute(select(func.count()).select_from(model_requests)).scalar_one() == 0
        continuation = core.tasks.claim("daemon")
        assert continuation["task_id"] == first["task_id"]
        assert continuation["session_id"] == first["session_id"]
        assert json.loads(continuation["continuation"]["inputs"][0]["text"])["status"] == "completed"
    finally:
        await core.stop()
        core.db.close()


async def test_core_supervises_external_waiter_as_an_independent_critical_loop(
    tmp_path, official_config, clock, monkeypatch
):
    core = make_core(tmp_path, official_config, clock)
    registered = {}
    def register(name, step, **options):
        registered[name] = (step, options)
    monkeypatch.setattr(core.supervisor, "start", register)
    core.stopping = True
    try:
        await core.run()
        step, options = registered["external_waiters"]
        assert step.__self__ is core.scheduler.waiters
        assert step.__name__ == "once"
        assert options == {"interval": 1, "critical": True}
    finally:
        await core.stop()
        core.db.close()


@pytest.mark.parametrize(("capability", "arguments"), [
    ("gh.exec", {"argv": ["issue", "close", "123", "--repo", "org/repo"]}),
    ("job.inspect", {"job_id": "unrelated-job"}),
])
async def test_wired_waiter_preserves_remote_read_allowlist(
    tmp_path, official_config, official_event, clock, monkeypatch, capability, arguments
):
    core = make_core(tmp_path, official_config, clock)
    executed = []
    async def github(arguments, context):
        executed.append(arguments)
        return {"exit_code": 0, "stdout": json.dumps({"status": "completed"})}
    monkeypatch.setattr("qqbot.runtime.scheduler.configured_github", lambda _args: github)
    task, _first = external_wait(core, official_event, capability=capability, arguments=arguments)
    core.scheduler = Scheduler(core, core.leader)
    try:
        await core.scheduler.waiters.once()
        assert executed == []
        with core.db.engine.connect() as conn:
            assert conn.execute(select(tasks.c.state).where(tasks.c.id == task["id"])).scalar_one() == "WAITING_EXTERNAL"
            assert conn.execute(select(func.count()).select_from(inputs)).scalar_one() == 0
            assert conn.execute(select(func.count()).select_from(model_requests)).scalar_one() == 0
    finally:
        await core.stop()
        core.db.close()
