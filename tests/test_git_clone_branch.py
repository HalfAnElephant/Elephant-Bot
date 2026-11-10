"""An explicitly selected branch enables a bounded clone without changing the default."""
import asyncio
import sys

import pytest

from qqbot.capabilities import GitService, ToolContext


@pytest.mark.parametrize("branch", [None, "elephant-acceptance/test"])
async def test_clone_preserves_default_argv_and_can_select_a_shallow_branch(tmp_path, branch):
    context = ToolContext("task", "run", 1, {}, str(tmp_path), "owner/repo")
    service = GitService(lambda context: {})
    calls = []

    async def run(argv, directory, context, *, authenticate=False):
        calls.append((argv, authenticate))
        (directory / "repository" / ".git" / "objects").mkdir(parents=True)
        (directory / "repository" / "marker.txt").write_text("fixture")
        return {"exit_code": 0, "stdout": "", "stderr": ""}

    service._run = run
    arguments = {"operation": "clone"}
    if branch is not None:
        arguments["branch"] = branch
    result = await service(arguments, context)
    expected = ["clone", "--template="]
    if branch is not None:
        expected += ["--depth=1", "--single-branch", "--branch", branch]
    assert calls == [([*expected, "--", "https://github.com/owner/repo.git", "repository"], True)]
    assert result["verified"] and (tmp_path / "repository" / "marker.txt").read_text() == "fixture"


@pytest.mark.parametrize("branch", ["", "-main", "/main", "a//b", "a..b", "main:other", "a\\b", 1])
@pytest.mark.parametrize("operation", ["clone", "push"])
async def test_branch_validation_precedes_process_or_network(tmp_path, branch, operation):
    context = ToolContext("task", "run", 1, {}, str(tmp_path), "owner/repo")
    service = GitService(lambda context: {})
    calls = []

    async def run(*args, **kwargs):
        calls.append(args)
        raise AssertionError("invalid branch reached Git")

    service._run = run
    with pytest.raises(ValueError, match="invalid .* branch"):
        await service({"operation": operation, "branch": branch, "commit": "a" * 40}, context)
    assert calls == []


@pytest.mark.parametrize("git_proxy", [None, "http://127.0.0.1:17890"])
async def test_transport_uses_only_trusted_proxy_environment(tmp_path, monkeypatch, git_proxy):
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:7890")
    monkeypatch.setenv("NO_PROXY", "localhost")
    monkeypatch.setenv("GH_TOKEN", "parent-token-must-stay-out")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "unrelated-secret")
    if git_proxy:
        monkeypatch.setenv("QQBOT_GIT_HTTPS_PROXY", git_proxy)
    else:
        monkeypatch.delenv("QQBOT_GIT_HTTPS_PROXY", raising=False)
    captured = {}

    async def start(*argv, **kwargs):
        captured.update(kwargs["env"])
        return await asyncio.create_subprocess_exec(
            sys.executable, "-c", "pass", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            env=kwargs["env"], start_new_session=True,
        )

    monkeypatch.setattr("qqbot.capabilities.git.start_process", start)
    context = ToolContext("task", "run", 1, {"HTTPS_PROXY": "http://untrusted.invalid"}, str(tmp_path), "owner/repo")
    service = GitService(lambda context: {})
    result = await service._run(["--version"], tmp_path, context)
    assert result["exit_code"] == 0
    assert captured["HTTPS_PROXY"] == (git_proxy or "http://127.0.0.1:7890")
    if git_proxy:
        assert captured["https_proxy"] == git_proxy
    assert captured["NO_PROXY"] == "localhost"
    assert "GH_TOKEN" not in captured and "AWS_SECRET_ACCESS_KEY" not in captured
