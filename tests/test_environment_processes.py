"""Host execution reaches host paths without changing the sandbox default."""
import json
import shutil
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from qqbot.adapters.execution.jobs import JobService, SystemdJobProvider
from qqbot.adapters.execution.world import LocalExecution
from qqbot.capabilities.process import ProcessService
from qqbot.capabilities.types import ToolContext
from qqbot.contracts.execution import JobScope, JobStart


@pytest.fixture
def context(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    return ToolContext("task", "run", 1, {}, str(workspace))


async def test_host_process_command_and_credentials_with_cgroup_launcher(context, tmp_path, monkeypatch):
    from qqbot.capabilities import process, subprocesses

    group = tmp_path / "fake-cgroup"
    group.mkdir()
    (group / "cgroup.procs").touch()
    async def stop_group(group):
        shutil.rmtree(group)
    monkeypatch.setattr(process, "create_host_cgroup", lambda: group)
    monkeypatch.setattr(subprocesses, "stop_host_cgroup", stop_group)
    monkeypatch.setenv("QQBOT_MODEL_KEY", "model-secret")
    monkeypatch.setenv("GH_TOKEN", "github-secret")
    marker = tmp_path / "host-file"
    marker.write_text("host data")
    host = replace(context, environment="host")
    processes = ProcessService()
    script = (
        "import json, os, pathlib; "
        "print(json.dumps([pathlib.Path('host-file').read_text(), "
        "os.getenv('QQBOT_MODEL_KEY'), os.getenv('GH_TOKEN')]))"
    )
    started = await processes.start({"argv": [sys.executable, "-c", script], "cwd": str(tmp_path)}, host)
    try:
        result = await processes.wait({"process_ref": started["process_ref"], "wait_seconds": 10}, host)
        assert result["exit_code"] == 0
        assert json.loads(result["text"]) == ["host data", None, None]
        assert started["environment"] == "host"
        with pytest.raises(PermissionError):
            await processes.read({"process_ref": started["process_ref"]}, replace(host, run_id="other"))
    finally:
        await processes.close_run(host)
    assert not group.exists()


async def test_host_process_rejects_platform_without_cgroups(context, monkeypatch):
    from qqbot.capabilities import subprocesses

    monkeypatch.setattr(subprocesses.sys, "platform", "darwin")
    with pytest.raises(RuntimeError, match="delegated cgroup"):
        await ProcessService().start({"argv": ["true"]}, replace(context, environment="host"))


async def test_failed_host_cleanup_keeps_process_active(context, monkeypatch):
    from unittest.mock import AsyncMock

    from qqbot.capabilities import process

    record = {"owner": (context.task_id, context.run_id, context.fence), "process": object(),
              "cgroup": Path("/unavailable"), "cleanup_done": False}
    service = ProcessService()
    service.processes["test"] = record
    monkeypatch.setattr(process, "stop_process", AsyncMock(side_effect=PermissionError("cannot kill cgroup")))
    with pytest.raises(PermissionError, match="cgroup"):
        await service._cleanup(record)
    assert service.active_run(context)


async def test_cancelled_host_launch_retains_failed_cleanup_for_retry(context, monkeypatch):
    import asyncio
    from unittest.mock import AsyncMock

    from qqbot.capabilities import process

    host = replace(context, environment="host")
    service = ProcessService()
    monkeypatch.setattr(process, "create_host_cgroup", lambda: Path("/unavailable"))
    monkeypatch.setattr(process, "start_process", AsyncMock(side_effect=asyncio.CancelledError))
    cleanup = AsyncMock(side_effect=PermissionError("cannot kill cgroup"))
    monkeypatch.setattr(process, "stop_process", cleanup)
    with pytest.raises(PermissionError, match="cgroup"):
        await service.start({"argv": ["true"]}, host)
    assert service.active_run(host)
    cleanup.side_effect = None
    await service.close_run(host)
    assert not service.active_run(host)


async def test_sandbox_cannot_fall_back_to_host(context, monkeypatch):
    from qqbot.capabilities import process

    monkeypatch.setattr(process.shutil, "which", lambda _: None)
    with pytest.raises(RuntimeError, match="bubblewrap"):
        await ProcessService().start({"argv": ["sh", "-c", "true"], "environment": "host"}, context)


def test_sandbox_command_keeps_namespaces_and_workspace_cwd(context):
    workspace = Path(context.workspace)
    directory = workspace / "sub"
    directory.mkdir()
    command = ProcessService()._sandbox_command(workspace, directory, ["sh", "-c", "pwd"])
    for flag in ("--unshare-all", "--clearenv", "--die-with-parent"):
        assert flag in command
    assert command[command.index("--chdir") + 1] == "/workspace/sub"
    assert command[command.index("--bind") + 1:command.index("--bind") + 3] == [str(workspace), "/workspace"]


async def test_sandbox_process_rejects_host_cwd(context, tmp_path, monkeypatch):
    from qqbot.capabilities import process

    monkeypatch.setattr(process.sys, "platform", "linux")
    monkeypatch.setattr(process.shutil, "which", lambda _: "/usr/bin/bwrap")
    with pytest.raises(ValueError, match="escapes"):
        await ProcessService().start({"argv": ["true"], "cwd": str(tmp_path)}, context)


async def test_host_file_access_and_sandbox_absolute_path_boundary(context, tmp_path):
    execution = LocalExecution()
    path = tmp_path / "host-file"
    host = replace(context, environment="host")
    await execution.write_file(host, str(path), b"host data")
    assert await execution.read_file(host, str(path), 0, 100) == b"host data"
    with pytest.raises(ValueError, match="workspace-relative"):
        await execution.read_file(context, str(path), 0, 100)
    with pytest.raises(FileExistsError):
        await execution.write_file(host, str(path), b"replacement")
    assert path.read_bytes() == b"host data"
    local_path = Path(context.workspace) / "local-file"
    await execution.write_file(context, "local-file", b"local data")
    assert local_path.read_bytes() == b"local data"


async def test_host_job_uses_host_command_and_explicit_environment(context, tmp_path):
    commands = []

    async def command(argv):
        commands.append(argv)
        return 0, ""

    provider = SystemdJobProvider(command=command)
    await provider.launch(unit="elephant-job-" + "1" * 32 + ".service", description="host test",
        request=JobStart(job_key="host", argv=("/usr/bin/true",), cwd=str(tmp_path)),
        workspace=Path(context.workspace), deadline_seconds=30, environment="host")
    argv = commands[0]
    assert "bwrap" not in argv
    assert "--working-directory=" + str(tmp_path.resolve()) in argv
    assert argv[argv.index("--") + 1:argv.index("--") + 5] == ("/usr/bin/env", "-i", "--", "PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin")
    assert "HOME=" + str(Path.home()) in argv


def test_job_environment_comes_from_scope_and_changes_intent(context, tmp_path):
    request = JobStart(job_key="test", argv=("true",))
    scope = JobScope(owner_id="owner", task_id="task", root_task_id="root", workspace=context.workspace)
    with pytest.raises(ValueError, match="relative path"):
        JobService._workspace(scope, str(tmp_path))
    assert JobService._workspace(scope.model_copy(update={"environment": "host"}), str(tmp_path))[1] == str(tmp_path.resolve())
    assert "environment" not in json.loads(JobService.arguments(request, "sandbox"))
    assert json.loads(JobService.arguments(request, "host"))["environment"] == "host"
    with pytest.raises(ValueError):
        JobStart.model_validate({"job_key": "test", "argv": ["true"], "environment": "host"})
