import os
import sys

import pytest

from qqbot.capabilities import (
    Action,
    Capability,
    CapabilityRegistry,
    GitHubCLI,
    SQLiteActionLedger,
    ToolContext,
    ToolGateway,
    classify,
    standard_registry,
)
from qqbot.capabilities.process import ProcessService
from qqbot.storage.action_schema import operations
from qqbot.storage.db import Database


@pytest.fixture
def context(tmp_path):
    return ToolContext("task", "run", 1, {"user_id": "user"}, str(tmp_path), "owner/repo")


@pytest.fixture
def ledger(tmp_path):
    database = Database(str(tmp_path / "actions.db"))
    operations.create(database.engine)
    yield SQLiteActionLedger(database)
    database.close()


@pytest.mark.parametrize(("argv", "stdin", "action", "effect"), [
    (["pr", "list"], "", "github.pr.read", "read"),
    (["pr", "review", "4", "--approve"], "", "github.pr.review.approve", "write"),
    (["api", "repos/owner/repo/issues", "-f", "title=x"], "", "github.issue.create", "write"),
    (["api", "repos/owner/repo/issues", "-f", "q=x", "-X", "GET"], "", "github.issue.read", "read"),
    (["api", "graphql", "-f", "query=query { viewer { login } }"], "", "github.graphql.read", "read"),
    (["api", "graphql", "--input", "-"], '{"query":"mutation { x }"}', "github.unclassified", "unknown"),
    (["something", "new"], "", "github.unclassified", "unknown"),
    (["custom", "create"], "", "github.unclassified", "unknown"),
    (["custom", "list"], "", "github.unclassified", "unknown"),
    (["pr", "comment", "4", "--body", "--help"], "", "github.pr.comment", "write"),
    (["api", "repos/a/b/pulls/1/reviews", "--input=-"], '{"event":"REQUEST_CHANGES"}',
     "github.pr.review.request_changes", "write"),
    (["api", "graphql", "--input=-"], '{"query":"query { viewer { login } }"}', "github.graphql.read", "read"),
    (["api", "graphql", "-f", 'query=query Q { viewer { login } } mutation M { x }'], "",
     "github.unclassified", "unknown"),
    (["api", "repos/a/b/pulls/1/merge", "-XPUT"], "", "github.pr.merge", "write"),
    (["api", "repos/a/b/actions/runs/1/rerun", "-X", "POST"], "", "github.run.rerun", "write"),
    (["api", "repos/a/b/pulls/1", "-X", "PATCH"], "", "github.unclassified", "unknown"),
    (["api", "repos/a/%62/pulls/1", "-X", "PATCH"], "", "github.unclassified", "unknown"),
    (["api", "repos/owner/repo/pulls/4/reviews", "--input", "-"], '{"event":"APPROVE"}',
     "github.pr.review.approve", "write"),
])
def test_classification(context, argv, stdin, action, effect):
    actual = classify(argv, context, stdin)
    assert (actual.name, actual.effect) == (action, effect)


async def test_durable_unknown_never_replays(context, ledger):
    calls = []
    registry = CapabilityRegistry()
    async def execute(arguments, ctx):
        calls.append(arguments)
        return {"exit_code": 0, "url": "https://github.com/owner/repo/issues/1"}
    registry.register(Capability("write", "write"), execute, lambda a, c: Action("write", (), "write"))
    gateway = ToolGateway(registry, authority=lambda c: True, permission=lambda c, a: True, ledger=ledger)
    first = await gateway.invoke("write", {"text": "literal"}, context, operation_id="stable")
    replay = await gateway.invoke("write", {"text": "literal"}, context, operation_id="stable")
    assert first.status == replay.status == "unknown"
    assert len(calls) == 1
    with pytest.raises(ValueError, match="conflicts"):
        await gateway.invoke("write", {"text": "changed"}, context, operation_id="stable")


async def test_permission_denies_before_execution(context, ledger):
    registry = standard_registry()
    gateway = ToolGateway(registry, authority=lambda c: True, permission=lambda c, a: False, ledger=ledger)
    with pytest.raises(PermissionError):
        await gateway.invoke("workspace.write", {"path": "x", "text": "x"}, context)
    assert not os.path.exists(context.workspace + "/x")


async def test_workspace_pages_symlink_hardlink_escape(context, ledger, tmp_path):
    registry = standard_registry()
    gateway = ToolGateway(registry, authority=lambda c: True, permission=lambda c, a: True, ledger=ledger)
    (tmp_path / "safe").write_text("abcdefgh")
    result = await gateway.invoke("workspace.read", {"path": "safe", "limit": 4}, context)
    assert result.data == {"text": "abcd", "next_offset": 4}
    (tmp_path / "link").symlink_to("/etc/passwd")
    for path in ("../outside", "link", ".git/config"):
        with pytest.raises((ValueError, OSError)):
            await gateway.invoke("workspace.read", {"path": path}, context)
    os.link(tmp_path / "safe", tmp_path / "hardlink")
    with pytest.raises(ValueError):
        await gateway.invoke("workspace.read", {"path": "hardlink"}, context)


async def test_cli_literal_stdin_and_bounded_output(context, tmp_path):
    program = tmp_path / "fake-gh"
    program.write_text('#!' + sys.executable + '\nimport sys\nsys.stdout.write(sys.stdin.read())\n')
    program.chmod(0o700)
    literal = '中文\n"quotes" `touch nope` $HOME'
    cli = GitHubCLI(str(program), max_bytes=1000)
    result = await cli({"argv": ["issue", "create", "--body-file", "-"], "stdin": literal}, context)
    assert result["stdout"] == literal
    assert not (tmp_path / "nope").exists()
    result = await cli({"argv": [], "stdin": "a" * 2000}, context)
    assert len(result["stdout"]) == 1000 and result["truncated"]


def test_credential_host(context):
    with pytest.raises(ValueError, match="host"):
        classify(["api", "https://evil.example/repos/a/b"], context)
    with pytest.raises(ValueError, match="host"):
        classify(["pr", "list", "-R", "evil.example/a/b"], context)


async def test_verification_result_and_output_reference(context, ledger):
    registry = CapabilityRegistry()
    registry.register(Capability("write", "write", max_output_bytes=8), lambda a, c: {"value": "x" * 40},
                      lambda a, c: Action("write", (), "write"))
    gateway = ToolGateway(registry, authority=lambda c: True, permission=lambda c, a: True, ledger=ledger,
        verify=lambda c, a, args, data: ("VERIFIED", data), content_store=lambda c, data: "content:full")
    result = await gateway.invoke("write", {}, context, operation_id="verified")
    assert result.status == "verified" and result.truncated and result.content_ref == "content:full"


async def test_process_owner_and_platform(context):
    service = ProcessService()
    service.processes["process:other"] = {"owner": ("other", "run", 1)}
    with pytest.raises(PermissionError):
        await service.read({"process_ref": "process:other"}, context)
    if sys.platform != "linux":
        with pytest.raises(RuntimeError, match="bubblewrap"):
            await service.start({"argv": ["true"]}, context)


async def test_concurrent_duplicate_dispatch_once(context, ledger):
    import asyncio
    calls = []
    registry = CapabilityRegistry()
    async def execute(arguments, ctx):
        calls.append(1)
        await asyncio.sleep(0.02)
        return {"done": True}
    registry.register(Capability("write", "write"), execute, lambda a, c: Action("write", (), "write"))
    gateway = ToolGateway(registry, authority=lambda c: True, permission=lambda c, a: True, ledger=ledger)
    await asyncio.gather(*(gateway.invoke("write", {}, context, operation_id="same") for _ in range(2)))
    assert calls == [1]


async def test_review_readback_requires_submitted_identity_commit(context):
    from qqbot.capabilities import GitHubVerifier
    review = {"id": 42, "user": {"login": "actor"}, "state": "APPROVED", "body": "好\n$HOME",
              "commit_id": "reviewed", "submitted_at": "2026-11-09T01:00:00Z"}
    reads = 0
    async def cli(arguments, ctx):
        import json
        nonlocal reads
        endpoint = arguments["argv"][1]
        if endpoint == "user":
            data = {"login": "actor"}
        elif "/reviews?" in endpoint:
            reads += 1
            data = [] if reads == 1 else [review]
        else:
            data = {"head": {"sha": "reviewed" if reads < 2 else "new_head"}}
        return {"exit_code": 0, "stdout": json.dumps(data)}
    verifier = GitHubVerifier(cli)
    action = Action("github.pr.review.approve", ("repo:github.com/owner/repo",), "write")
    arguments = {"argv": ["pr", "review", "4", "--approve"], "stdin": "好\n$HOME"}
    arguments["_preconditions"] = await verifier.prepare(context, action, arguments)
    state, data = await verifier(context, action, arguments, {"exit_code": 0})
    assert state == "VERIFIED" and data["review"]["id"] == 42 and data["head_changed"]
    review["state"] = "PENDING"
    assert await verifier(context, action, arguments, {"exit_code": 0}) is None


async def test_git_push_uses_sanitized_repo_and_verifies_remote(context, tmp_path):
    import asyncio

    from qqbot.capabilities import GitService
    process = await asyncio.create_subprocess_exec("git", "init", "--bare", str(tmp_path / ".git"),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    await process.communicate()
    assert process.returncode == 0
    (tmp_path / ".git" / "config").write_text('[credential]\nhelper = !touch /tmp/credential-leak\n')
    commit = "a" * 40
    service = GitService(lambda ctx: {"GH_TOKEN": "host-secret"})
    original = service._run
    async def run(arguments, directory, ctx, authenticate=False):
        if arguments[0] == "init":
            return await original(arguments, directory, ctx)
        assert "credential-leak" not in (directory / "config").read_text()
        assert authenticate
        if arguments[0] == "push":
            assert arguments[-1] == commit + ":refs/heads/fix"
            return {"exit_code": 0, "stdout": "accepted", "stderr": ""}
        return {"exit_code": 0, "stdout": commit + "\trefs/heads/fix\n", "stderr": ""}
    service._run = run
    result = await service({"operation": "push", "commit": commit, "branch": "fix"}, context)
    assert result["verified"]
    (tmp_path / ".git" / "objects" / "evil").symlink_to("/etc/passwd")
    with pytest.raises((ValueError, OSError)):
        await service({"operation": "push", "commit": commit, "branch": "fix"}, context)


async def test_process_launch_isolated_and_cleanup(context, monkeypatch):
    import asyncio

    import qqbot.capabilities.process as module
    original = asyncio.create_subprocess_exec
    commands = []
    async def spawn(*argv, **kwargs):
        commands.append(argv)
        return await original(sys.executable, "-c", "import time; time.sleep(20)", **kwargs)
    monkeypatch.setattr(module.sys, "platform", "linux")
    monkeypatch.setattr(module.shutil, "which", lambda name: "/usr/bin/bwrap")
    monkeypatch.setattr(module.asyncio, "create_subprocess_exec", spawn)
    service = ProcessService()
    result = await service.start({"argv": ["python", "-V"]}, context)
    assert service.active_run(context)
    assert "--unshare-all" in commands[0] and "--clearenv" in commands[0]
    assert "--proc" in commands[0]
    await service.close_run(context)
    assert not service.active_run(context)
    assert result["process_ref"].startswith("process:")


async def test_create_nonzero_with_remote_object_is_partial(context):
    import json

    from qqbot.capabilities import GitHubVerifier
    created = {"id": 9, "html_url": "https://github.com/owner/repo/issues/7", "user": {"login": "actor"},
               "created_at": "2026-11-09T01:00:01Z", "body": "中文\n`$HOME`"}
    async def cli(arguments, ctx):
        return {"exit_code": 0, "stdout": json.dumps(created)}
    verifier = GitHubVerifier(cli)
    args = {"argv": ["issue", "create", "--body-file", "-"], "stdin": "中文\n`$HOME`",
            "_preconditions": {"repository": "owner/repo", "actor": "actor",
                               "observed_at": "2026-11-09T01:00:00+00:00"}}
    state, data = await verifier(context, Action("github.issue.create", (), "write"), args,
                                 {"exit_code": 1, "stdout": created["html_url"]})
    assert state == "PARTIAL" and data["created"]["id"] == 9


async def test_merge_auto_is_distinct_from_merged(context):
    import json

    from qqbot.capabilities import GitHubVerifier
    pull = {"id": 7, "merged": False, "auto_merge": {"merge_method": "SQUASH"}}
    async def cli(arguments, ctx):
        return {"exit_code": 0, "stdout": json.dumps(pull)}
    verifier = GitHubVerifier(cli)
    args = {"argv": ["pr", "merge", "7", "--auto"],
            "_preconditions": {"repository": "owner/repo", "number": "7", "actor": "actor"}}
    state, data = await verifier(context, Action("github.pr.merge", (), "write"), args, {"exit_code": 0})
    assert state == "VERIFIED" and data["effect"] == "auto_merge_enabled"
    pull.update(merged=True, auto_merge=None)
    assert (await verifier(context, Action("github.pr.merge", (), "write"), args, {"exit_code": 0}))[1]["effect"] == "merged"


async def test_commit_bound_review_transmits_literal_json(context, tmp_path):
    import json
    program = tmp_path / "fake-gh-review"
    program.write_text('#!' + sys.executable + '\nimport json,sys\nprint(json.dumps({"argv":sys.argv[1:],"stdin":sys.stdin.read()}))\n')
    program.chmod(0o700)
    body = '中文\n"quotes" `$HOME`'
    result = await GitHubCLI(str(program))({"argv": ["pr", "review", "7", "--approve", "--body-file", "-"],
        "stdin": body, "reviewed_head_sha": "a" * 40}, context)
    captured = json.loads(result["stdout"])
    assert captured["argv"] == ["api", "repos/owner/repo/pulls/7/reviews", "--method", "POST", "--input", "-"]
    assert json.loads(captured["stdin"]) == {"event": "APPROVE", "body": body, "commit_id": "a" * 40}


async def test_preflight_is_persisted_before_dispatch(context, ledger):
    registry = CapabilityRegistry()
    registry.register(Capability("write", "write"), lambda a, c: {"seen": a["_preconditions"]},
                      lambda a, c: Action("write", (), "write"))
    gateway = ToolGateway(registry, authority=lambda c: True, permission=lambda c, a: True,
        ledger=ledger, prepare=lambda c, a, args: {"before_id": 17},
        verify=lambda c, a, args, data: ("VERIFIED", data))
    result = await gateway.invoke("write", {}, context, operation_id="prepared")
    assert result.data == {"seen": {"before_id": 17}}
    with ledger.database.engine.connect() as connection:
        import json

        from sqlalchemy import select
        row = connection.execute(select(operations).where(operations.c.id == "prepared")).mappings().one()
        assert json.loads(row["arguments_json"])["_preconditions"] == {"before_id": 17}


async def test_discovery_describes_and_invokes_new_capability(context, ledger):
    registry = standard_registry()
    registry.register(Capability("fixture.new_business", "New discoverable business capability",
        {"type": "object", "properties": {"value": {"type": "string"}}, "required": ["value"]}),
        lambda a, c: {"echo": a["value"]}, lambda a, c: Action("fixture.read", ("task:" + c.task_id,)))
    gateway = ToolGateway(registry, authority=lambda c: True, permission=lambda c, a: True, ledger=ledger)
    results = await gateway.invoke("capability.search", {"query": "new discoverable"}, context)
    assert [entry["name"] for entry in results.data] == ["fixture.new_business"]
    described = await gateway.invoke("capability.describe", {"name": "fixture.new_business"}, context)
    assert described.data["parameters"]["required"] == ["value"]
    invoked = await gateway.invoke("capability.invoke", {"name": "fixture.new_business", "arguments": {"value": "literal"}}, context)
    assert invoked.status == "ok" and invoked.data == {"echo": "literal"}


async def test_empty_pr_list_is_successful_without_files(context, ledger):
    async def empty_cli(arguments, ctx):
        assert arguments["argv"] == ["pr", "list", "--json", "number,url"]
        return {"exit_code": 0, "stdout": "[]", "stderr": "", "truncated": False}
    registry = standard_registry(github=empty_cli)
    gateway = ToolGateway(registry, authority=lambda c: True, permission=lambda c, a: True, ledger=ledger)
    result = await gateway.invoke("gh.exec", {"argv": ["pr", "list", "--json", "number,url"]}, context)
    assert result.status == "ok" and result.data["stdout"] == "[]"
    assert result.operation_id is None and result.effect_refs == ()


async def test_local_process_same_command_distinct_ids_dispatch_twice_same_id_once(context, ledger, monkeypatch):
    import asyncio

    import qqbot.capabilities.process as module
    original = asyncio.create_subprocess_exec
    launches = []
    async def spawn(*argv, **kwargs):
        launches.append(argv)
        return await original(sys.executable, "-c", "print('pytest fixture')", **kwargs)
    monkeypatch.setattr(module.sys, "platform", "linux")
    monkeypatch.setattr(module.shutil, "which", lambda name: "/usr/bin/bwrap")
    monkeypatch.setattr(module.asyncio, "create_subprocess_exec", spawn)
    service = ProcessService()
    registry = CapabilityRegistry()
    service.register(registry)
    gateway = ToolGateway(registry, authority=lambda c: True, permission=lambda c, a: True, ledger=ledger,
        verify=lambda c, a, args, data: ("VERIFIED", data))
    try:
        first = await gateway.invoke("process.start", {"argv": ["pytest", "-q"]}, context, operation_id="first")
        second = await gateway.invoke("process.start", {"argv": ["pytest", "-q"]}, context, operation_id="after-edit")
        replay = await gateway.invoke("process.start", {"argv": ["pytest", "-q"]}, context, operation_id="first")
        assert len(launches) == 2
        assert first.data["process_ref"] != second.data["process_ref"]
        assert replay.operation_id == first.operation_id == "first"
    finally:
        await service.close_run(context)


@pytest.mark.parametrize("state", ["PREPARED", "DISPATCHED", "UNKNOWN", "PARTIAL", "VERIFIED"])
@pytest.mark.parametrize(("action_name", "effect"), [
    ("github.issue.create", "write"), ("github.unclassified", "unknown"), ("git.push", "write"),
    ("icloud.event.create", "write"), ("icloud.event.update", "write"), ("icloud.event.delete", "write"),
])
async def test_unsafe_github_same_hash_distinct_id_never_dispatches(context, ledger, state, action_name, effect):
    import hashlib
    import json
    from dataclasses import asdict
    action = Action(action_name, ("repo:github.com/owner/repo",), effect)
    arguments = {"argv": ["issue", "create", "--title", "same"]}
    digest = hashlib.sha256(json.dumps({"name": "gh.exec", "arguments": arguments, "action": asdict(action)},
                                      sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    await ledger.prepare("original", context, action, digest, arguments)
    if state != "PREPARED":
        await ledger.transition("original", "PREPARED", "DISPATCHED", {})
    if state not in {"PREPARED", "DISPATCHED"}:
        await ledger.transition("original", "DISPATCHED", state, {})
    called = []
    registry = CapabilityRegistry()
    registry.register(Capability("gh.exec", "fixture"), lambda a, c: called.append(1), lambda a, c: action)
    gateway = ToolGateway(registry, authority=lambda c: True, permission=lambda c, a: True, ledger=ledger)
    result = await gateway.invoke("gh.exec", arguments, context, operation_id="new-call-id")
    assert result.operation_id == "original" and result.status == state.lower()
    assert not called


async def test_repeated_local_git_fetch_is_not_semantically_deduplicated(context, ledger):
    called = []
    registry = CapabilityRegistry()
    registry.register(Capability("git.transport", "fixture"), lambda a, c: called.append(1) or {"verified": True},
                      lambda a, c: Action("git.fetch", ("repo:github.com/owner/repo",), "write"))
    gateway = ToolGateway(registry, authority=lambda c: True, permission=lambda c, a: True, ledger=ledger,
                          verify=lambda c, a, args, data: ("VERIFIED", data))
    for identity in ("first", "second", "first"):
        await gateway.invoke("git.transport", {"operation": "fetch"}, context, operation_id=identity)
    assert len(called) == 2


async def test_git_clone_followup_fetch_push_use_same_bounded_path(context, tmp_path):
    from qqbot.capabilities import GitService
    service = GitService(lambda ctx: {})
    original = service._run
    operations_seen = []
    commit = "a" * 40
    (tmp_path / "projects").mkdir()
    async def run(arguments, directory, ctx, authenticate=False):
        operations_seen.append(arguments[0])
        if arguments[0] == "clone":
            clone = directory / "repository"
            (clone / ".git" / "objects" / "aa").mkdir(parents=True)
            (clone / ".git" / "objects" / "aa" / ("b" * 38)).write_bytes(b"fixture object")
            (clone / "README.md").write_text("fixture")
            return {"exit_code": 0, "stdout": "", "stderr": ""}
        if arguments[0] == "init":
            return await original(arguments, directory, ctx)
        assert (directory / "objects" / "aa" / ("b" * 38)).read_bytes() == b"fixture object"
        if arguments[0] == "fetch":
            (directory / "FETCH_HEAD").write_text(commit + "\tfixture\n")
            (directory / "objects" / "cc").mkdir()
            (directory / "objects" / "cc" / ("d" * 38)).write_bytes(b"fetched fixture")
        return {"exit_code": 0, "stdout": commit + "\trefs/heads/fix\n", "stderr": ""}
    service._run = run
    clone = await service({"operation": "clone", "path": "projects/repository"}, context)
    assert clone["path"] == "projects/repository" and clone["verified"]
    fetched = await service({"operation": "fetch", "path": clone["path"]}, context)
    assert fetched["verified"] and fetched["path"] == clone["path"]
    assert (tmp_path / clone["path"] / ".git" / "objects" / "cc" / ("d" * 38)).read_bytes() == b"fetched fixture"
    pushed = await service({"operation": "push", "path": clone["path"], "commit": commit, "branch": "fix"}, context)
    assert pushed["verified"] and operations_seen == ["clone", "init", "fetch", "init", "push", "ls-remote"]
    (tmp_path / "projects" / "linked").symlink_to(tmp_path / clone["path"], target_is_directory=True)
    for path in ("../outside", str(tmp_path / clone["path"]), "projects/linked"):
        with pytest.raises((ValueError, OSError)):
            await service({"operation": "fetch", "path": path}, context)


@pytest.mark.parametrize("adapter", ["gh", "git"])
@pytest.mark.parametrize("leader_exits", [False, True])
async def test_cancelled_cli_waits_for_parent_and_kills_descendants(context, tmp_path, adapter, leader_exits):
    import asyncio
    import json

    from qqbot.capabilities import GitService
    marker = tmp_path / "pids.json"
    executable = tmp_path / "fake-cli"
    executable.write_text('#!' + sys.executable + '\nimport os,sys,json,time,subprocess\n'
        'child=subprocess.Popen([sys.executable,"-c","import time; time.sleep(60)"])\n'
        + 'open(' + repr(str(marker)) + ',"w").write(json.dumps([os.getpid(),child.pid]))\n'
        + 'sys.stdout.write("x"*300000);sys.stdout.flush();' + ('sys.exit(0)' if leader_exits else 'time.sleep(60)') + '\n')
    executable.chmod(0o700)
    if adapter == "gh":
        task = asyncio.create_task(GitHubCLI(str(executable))({"argv": ["fixture"]}, context))
    else:
        task = asyncio.create_task(GitService(lambda ctx: {}, executable=str(executable))._run(["fixture"], tmp_path, context))
    try:
        async with asyncio.timeout(3):
            while not marker.exists():
                await asyncio.sleep(0.01)
        parent, child = json.loads(marker.read_text())
        if leader_exits:
            async with asyncio.timeout(3):
                while True:
                    try:
                        os.kill(parent, 0)
                    except ProcessLookupError:
                        break
                    await asyncio.sleep(0.01)
            assert not task.done()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 3)
        with pytest.raises(ProcessLookupError):
            os.kill(parent, 0)
        ps = await asyncio.create_subprocess_exec("ps", "-o", "stat=", "-p", str(child), stdout=asyncio.subprocess.PIPE)
        status, _ = await ps.communicate()
        assert not status.strip() or status.strip().startswith(b"Z")
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_cancel_during_launch_waits_for_spawn_then_reaps(tmp_path, monkeypatch):
    import asyncio

    import qqbot.capabilities.subprocesses as module
    original = asyncio.create_subprocess_exec
    started, release = asyncio.Event(), asyncio.Event()
    children = []
    async def slow_spawn(*argv, **kwargs):
        process = await original(*argv, **kwargs)
        children.append(process)
        started.set()
        await release.wait()
        return process
    monkeypatch.setattr(module.asyncio, "create_subprocess_exec", slow_spawn)
    task = asyncio.create_task(module.start_process(sys.executable, "-c", "import time;time.sleep(60)",
        start_new_session=True, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE))
    await asyncio.wait_for(started.wait(), 2)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 3)
    assert children[0].returncode is not None


async def test_process_wait_cancellation_stops_owned_subprocess(context, monkeypatch):
    import asyncio

    import qqbot.capabilities.process as module
    original = asyncio.create_subprocess_exec
    children = []
    async def spawn(*argv, **kwargs):
        child = await original(sys.executable, "-c", "import time;time.sleep(60)", **kwargs)
        children.append(child)
        return child
    monkeypatch.setattr(module.sys, "platform", "linux")
    monkeypatch.setattr(module.shutil, "which", lambda name: "/usr/bin/bwrap")
    monkeypatch.setattr(module.asyncio, "create_subprocess_exec", spawn)
    service = ProcessService()
    result = await service.start({"argv": ["sleep", "60"]}, context)
    waiting = asyncio.create_task(service.wait({"process_ref": result["process_ref"], "wait_seconds": 30}, context))
    await asyncio.sleep(0)
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(waiting, 3)
    assert children[0].returncode is not None
    assert not service.active_run(context)
    await service.close_run(context)


async def test_repeated_cancellation_waits_for_subprocess_cleanup():
    import asyncio

    from qqbot.capabilities.subprocesses import stop_process
    process = await asyncio.create_subprocess_exec(sys.executable, "-c", "import time;time.sleep(60)",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, start_new_session=True)
    original = process.communicate
    entered, release = asyncio.Event(), asyncio.Event()
    async def slow_cleanup():
        result = await original()
        entered.set()
        await release.wait()
        return result
    process.communicate = slow_cleanup
    task = asyncio.create_task(stop_process(process))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 3)
        assert process.returncode is not None
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("argv", [
    ["api", "--method", "GET", "https://evil.example/repos/a/b"],
    ["api", "-XGET", "https://evil.example/repos/a/b"],
    ["api", "--method=GET", "--", "https://evil.example/repos/a/b"],
    ["api", "--raw-field", "body=https://example.org/source", "--method", "GET", "https://evil.example/repos/a/b"],
    ["api", "-iXGET", "HTTPS://evil.example/repos/a/b"],
    ["api", "--method", "GET", "https://github.com:8443/repos/a/b"],
    ["api", "--method", "GET", "https://user:password@github.com/repos/a/b"],
])
def test_api_option_order_cannot_bypass_credential_host(context, argv):
    with pytest.raises(ValueError, match="host"):
        classify(argv, context)


@pytest.mark.parametrize("argv,action,effect", [
    (["api", "--method", "GET", "https://github.com/repos/a/b/issues"], "github.issue.read", "read"),
    (["api", "-iXGET", "repos/a/b/pulls"], "github.pr.read", "read"),
    (["api", "--method=POST", "-f", "body=https://example.org/reference", "repos/a/b/issues"], "github.issue.create", "write"),
    (["api", "--header", "Referer: https://example.org/", "--method", "GET", "repos/a/b/issues"], "github.issue.read", "read"),
    (["api", "--template", "https://example.org/template", "repos/a/b/issues"], "github.issue.read", "read"),
    (["api", "-iXPOST", "repos/a/b/issues", "-ftitle=literal"], "github.issue.create", "write"),
])
def test_api_option_order_preserves_body_links_and_actual_method(context, argv, action, effect):
    result = classify(argv, context)
    assert (result.name, result.effect) == (action, effect)
    assert "repo:github.com/a/b" in result.resources


async def test_api_execution_checks_endpoint_before_credential_lookup(context):
    credential_reads = []
    cli = GitHubCLI("unreachable-fixture-executable", credentials=lambda c: credential_reads.append(c) or {})
    with pytest.raises(ValueError, match="host"):
        await cli({"argv": ["api", "--method", "GET", "https://evil.example/repos/a/b"]}, context)
    assert not credential_reads


async def test_api_execution_preserves_option_order_and_literal_body(context, tmp_path):
    import json
    program = tmp_path / "fake-api"
    program.write_text('#!' + sys.executable + '\nimport sys,json\nprint(json.dumps({"argv":sys.argv[1:],"stdin":sys.stdin.read()}))\n')
    program.chmod(0o700)
    argv = ["api", "--method", "POST", "--input", "-", "repos/a/b/issues"]
    body = json.dumps({"title": "literal", "body": 'https://example.org/source\n中文 "$HOME" `text`'}, ensure_ascii=False)
    result = await GitHubCLI(str(program))({"argv": argv, "stdin": body}, context)
    assert json.loads(result["stdout"]) == {"argv": argv, "stdin": body}
