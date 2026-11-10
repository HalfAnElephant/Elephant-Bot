"""An explicitly selected branch enables a bounded clone without changing the default."""
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
