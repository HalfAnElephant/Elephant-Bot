"""Resolve command identity once and verify actual GitHub state after each supported write."""
import json
from datetime import UTC, datetime
from urllib.parse import parse_qs, urlsplit

import pytest

from qqbot.capabilities import Action, GitHubCLI, GitHubVerifier, ToolContext, classify
from qqbot.capabilities.github import api_payload, normalize_argv


@pytest.fixture
def context(tmp_path):
    return ToolContext("task", "run", 1, {}, str(tmp_path), "HalfAnElephant/Example")


def stamp():
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def result(value):
    return {"exit_code": 0, "stdout": json.dumps(value), "truncated": False}


def paginated(pages):
    return {"exit_code": 0, "stdout": "\n".join(json.dumps(page) for page in pages), "truncated": False}


@pytest.mark.parametrize("method", ["GET", "HEAD", "OPTIONS"])
@pytest.mark.parametrize("endpoint", ["user", "user/repos", "users/HalfAnElephant/repos", "repos/pulls/body/contents/issue.md"])
def test_rest_reads_are_not_recorded_as_unknown_writes(context, method, endpoint):
    action = classify(["api", "--method", method, "--header", "Accept: application/vnd.github+json", endpoint], context)
    assert (action.name, action.effect) == ("github.api.read", "read")
    expected = "repo:github.com/pulls/body" if endpoint.startswith("repos/") else "github:github.com"
    assert action.resources == (expected,)


def test_query_encoding_does_not_hide_safe_read_and_path_encoding_remains_unknown(context):
    assert classify(["api", "user/repos?sort=created&filter=foo%2Fbar"], context).effect == "read"
    for endpoint in ("repos/a/%62/issues", "../repos/a/b/issues", "repos/a/b/../issues"):
        assert classify(["api", endpoint], context).effect == "unknown"
    assert classify(["api", "user/repos", "-fname=test"], context).effect == "unknown"


@pytest.mark.parametrize("argv,body,name", [
    (["api", "-XPATCH", "-fbody=new", "repos/a/b/issues/7"], "", "github.issue.edit"),
    (["api", "--input", "-", "--method=PATCH", "repos/a/b/pulls/7"], '{"title":"new"}', "github.pr.edit"),
    (["api", "repos/a/b/issues/7", "-X", "PATCH", "-fstate=closed"], "", "github.issue.close"),
    (["api", "repos/a/b/pulls/7", "-X", "PATCH", "-fstate=open"], "", "github.pr.reopen"),
    (["api", "--input=-", "-XPOST", "repos/a/b/actions/workflows/test.yml/dispatches"], '{"ref":"test"}', "github.workflow.run"),
])
def test_supported_rest_writes_keep_their_actual_operation(context, argv, body, name):
    action = classify(argv, context, body)
    assert (action.name, action.effect) == (name, "write")
    assert action.resources[0] == "repo:github.com/a/b"


@pytest.mark.parametrize("argv", [
    ["pr", "edit", "--body", "https://evil.example/not/a/target", "-R", "A/B", "7"],
    ["pr", "edit", "-RA/B", "-bhttps://evil.example/not/a/target", "7"],
    ["pr", "edit", "--body=--repo", "--repo=A/B", "7"],
    ["pr", "edit", "--body", "--help", "--repo=A/B", "7"],
    ["pr", "edit", "--body", "-Rother/repo", "--repo=A/B", "7"],
    ["pr", "edit", "--body", "1", "--repo=A/B", "7"],
])
def test_cli_flag_values_cannot_replace_the_real_target(context, argv):
    action = classify(argv, context)
    assert action.resources == ("repo:github.com/a/b", "pr:github.com/a/b#7")
    assert (action.name, action.effect) == ("github.pr.edit", "write")
    assert GitHubVerifier(None)._number({"argv": argv}) == "7"


def test_actual_api_stdin_body_is_not_overwritten_by_query_fields():
    argv = ["api", "-fbody=query-only", "--input", "-", "repos/a/b/issues/7"]
    assert api_payload(argv, '{"body":"actual JSON"}') == {"body": "actual JSON"}
    assert normalize_argv(["issue", "edit", "--body", "--repo", "7", "-RA/B"])[2] == "7"


@pytest.mark.parametrize("family,path", [("issue", "issues"), ("pr", "pull")])
@pytest.mark.parametrize("mismatch", [None, "repository", "kind", "readback_url", "actor", "body", "title"])
async def test_create_casefolds_repository_but_requires_the_actual_target(context, family, path, mismatch):
    url = f"https://github.com/HalfAnElephant/Example/{path}/7"
    created = {"id": 7, "html_url": url, "created_at": stamp(), "user": {"login": "Actor"},
               "title": "Title", "body": "literal\n中文 `$HOME`"}
    output = url
    if mismatch == "repository":
        output = output.replace("HalfAnElephant/Example", "other/repo")
    elif mismatch == "kind":
        output = output.replace(f"/{path}/", "/pull/" if path == "issues" else "/issues/")
    elif mismatch == "readback_url":
        created["html_url"] = created["html_url"].replace("Example", "other-repo")
    elif mismatch == "actor":
        created["user"]["login"] = "other"
    elif mismatch in {"body", "title"}:
        created[mismatch] = "different"

    async def cli(arguments, ctx):
        assert arguments["argv"][1].lower() == f"repos/halfanElephant/example/{'pulls' if path == 'pull' else 'issues'}/7".lower()
        return result(created)

    verifier = GitHubVerifier(cli)
    arguments = {"argv": [family, "create", "--body-file", "-", "--title", "Title"],
                 "stdin": "literal\n中文 `$HOME`", "_preconditions": {
                     "repository": "halfanelephant/example", "actor": "actor", "observed_at": "2020-01-01T00:00:00+00:00",
                 }}
    actual = await verifier(context, Action(f"github.{family}.create", (), "write"), arguments,
                            {"exit_code": 0, "stdout": output})
    assert (actual[0] if actual else None) == ("VERIFIED" if mismatch is None else None)


@pytest.mark.parametrize("family", ["issue", "pr"])
@pytest.mark.parametrize("operation", ["edit", "close", "reopen", "comment"])
@pytest.mark.parametrize("style", ["cli", "api"])
async def test_common_writes_verify_requested_fields_and_new_comments(context, family, operation, style):
    number, repository = "7", "halfanelephant/example"
    wanted = {"title": "new title", "body": "new body"} if operation == "edit" else {
        "state": "closed" if operation == "close" else "open"} if operation != "comment" else {"body": "new comment"}
    if style == "api":
        endpoint = f"repos/{repository}/{'pulls' if family == 'pr' else 'issues'}/{number}"
        if operation == "comment":
            endpoint = f"repos/{repository}/issues/{number}/comments"
        arguments = {"argv": ["api", "--input", "-", "-X", "POST" if operation == "comment" else "PATCH", endpoint],
                     "stdin": json.dumps(wanted)}
    else:
        flags = ["--title", wanted["title"], "--body", wanted["body"]] if operation == "edit" else (
            ["--body-file", "-"] if operation == "comment" else [])
        arguments = {"argv": [family, operation, *flags, "-R", "HalfAnElephant/Example", number]}
        if operation == "comment":
            arguments["stdin"] = wanted["body"]
    state = {"after": False}
    target = {"id": 17, "title": "old title", "body": "old body", "state": "open" if operation == "close" else "closed"}
    new_comment = {"id": 2, "body": "new comment", "user": {"login": "Actor"}, "created_at": stamp()}
    old_comment = {"id": 1, "body": "old comment", "user": {"login": "actor"}, "created_at": stamp()}

    async def cli(call, ctx):
        endpoint = call["argv"][1]
        if endpoint == "user":
            return result({"login": "actor"})
        if "/comments?" in endpoint:
            assert "--paginate" in call["argv"] and "--slurp" not in call["argv"]
            return paginated([[old_comment], [new_comment]] if state["after"] else [[old_comment]])
        return result({**target, **(wanted if state["after"] and operation != "comment" else {})})

    action = classify(arguments["argv"], context, arguments.get("stdin", ""))
    verifier = GitHubVerifier(cli)
    arguments["_preconditions"] = await verifier.prepare(context, action, arguments)
    assert arguments["_preconditions"]["number"] == number
    state["after"] = True
    actual = await verifier(context, action, arguments, {"exit_code": 0})
    assert actual[0] == "VERIFIED"
    if operation == "comment":
        assert actual[1]["comment"]["id"] == 2
    else:
        assert actual[1]["confirmed_fields"] == wanted
    if operation == "edit":
        wanted["body"] = "not present"
        target["body"] = "not present"
        arguments["_preconditions"]["requested"]["body"] = "expected missing body"
        assert await verifier(context, action, arguments, {"exit_code": 0}) is None


@pytest.mark.parametrize("change", ["old_id", "other_actor", "other_body", "multiple"])
async def test_comments_require_unique_new_matching_actor_and_body(context, change):
    comments = []

    async def cli(arguments, ctx):
        if arguments["argv"][1] == "user":
            return result({"login": "actor"})
        if "/comments?" in arguments["argv"][1]:
            return paginated([comments])
        return result({"id": 7})

    verifier = GitHubVerifier(cli)
    arguments = {"argv": ["issue", "comment", "7", "--body", "body"]}
    action = classify(arguments["argv"], context)
    arguments["_preconditions"] = await verifier.prepare(context, action, arguments)
    comment = {"id": 2, "body": "body", "user": {"login": "actor"}, "created_at": stamp()}
    if change == "old_id":
        arguments["_preconditions"]["comment_ids"] = [2]
    elif change == "other_actor":
        comment["user"]["login"] = "someone"
    elif change == "other_body":
        comment["body"] = "different"
    comments.append(comment)
    if change == "multiple":
        comments.append({**comment, "id": 3})
    assert await verifier(context, action, arguments, {"exit_code": 0}) is None


@pytest.mark.parametrize("argv", [
    ["issue", "edit", "7", "--body", "new", "--add-label", "unchecked"],
    ["pr", "comment", "7", "--body", "new", "--edit-last"],
    ["pr", "close", "7", "--delete-branch"],
    ["issue", "edit", "7", "8", "--body", "new"],
])
async def test_unsupported_or_multi_target_writes_remain_unverified(context, argv):
    async def cli(arguments, ctx):
        assert arguments["argv"][1] == "user"
        return result({"login": "actor"})

    verifier = GitHubVerifier(cli)
    action = classify(argv, context)
    arguments = {"argv": argv}
    arguments["_preconditions"] = await verifier.prepare(context, action, arguments)
    assert await verifier(context, action, arguments, {"exit_code": 0}) is None


@pytest.mark.parametrize("style", ["cli", "api"])
@pytest.mark.parametrize("wrong", [None, "old_id", "actor", "head", "workflow", "event", "branch", "multiple", "response_id"])
async def test_workflow_dispatch_confirms_acceptance_with_bound_identity_and_head(context, style, wrong):
    sha = "a" * 40
    arguments = {"argv": ["workflow", "run", "--ref", "acceptance/branch", "-R", "HalfAnElephant/Example", "hugo.yml"]}
    if style == "api":
        arguments = {"argv": ["api", "-XPOST", "--input", "-", "repos/HalfAnElephant/Example/actions/workflows/hugo.yml/dispatches"],
                     "stdin": '{"ref":"acceptance/branch"}'}
    runs = [{"id": 1, "workflow_id": 11, "actor": {"login": "actor"}, "head_sha": sha,
             "head_branch": "acceptance/branch", "event": "workflow_dispatch", "created_at": stamp()}]
    calls = []

    async def cli(call, ctx):
        endpoint = call["argv"][1]
        calls.append(endpoint)
        if endpoint == "user":
            return result({"login": "actor"})
        if endpoint.endswith("/workflows/hugo.yml"):
            return result({"id": 11, "path": ".github/workflows/hugo.yml"})
        if "/commits/" in endpoint:
            assert endpoint.endswith("acceptance%2Fbranch")
            return result({"sha": sha})
        assert "/actions/workflows/11/runs?" in endpoint
        query = parse_qs(urlsplit(endpoint).query)
        assert query["branch"] == ["acceptance/branch"] and query["head_sha"] == [sha]
        return result({"total_count": len(runs), "workflow_runs": runs})

    verifier = GitHubVerifier(cli)
    action = classify(arguments["argv"], context, arguments.get("stdin", ""))
    assert action.name == "github.workflow.run"
    arguments["_preconditions"] = await verifier.prepare(context, action, arguments)
    new = {**runs[0], "id": 2, "actor": {"login": "Actor"}, "status": "queued", "conclusion": None}
    if wrong == "old_id":
        # A distinct run exists, but it was already present in the pre-dispatch snapshot.
        arguments["_preconditions"]["run_ids"].append(2)
        new["head_sha"] = "different"
    elif wrong == "actor":
        new["actor"] = {"login": "someone"}
    elif wrong == "head":
        new["head_sha"] = "b" * 40
    elif wrong == "workflow":
        new["workflow_id"] = 12
    elif wrong == "event":
        new["event"] = "push"
    elif wrong == "branch":
        new["head_branch"] = "other"
    runs.append(new)
    if wrong == "multiple":
        runs.append({**new, "id": 3})
    data = {"exit_code": 0, "stdout": json.dumps({"workflow_run_id": 99 if wrong == "response_id" else 2})}
    actual = await verifier(context, action, arguments, data)
    if wrong is None:
        assert actual[0] == "VERIFIED"
        assert actual[1]["effect"] == "dispatch_accepted"
        assert actual[1]["run"]["status"] == "queued" and actual[1]["run"]["conclusion"] is None
    else:
        assert actual is None
    assert calls.count(arguments["_preconditions"]["runs_endpoint"]) >= 2


async def test_commit_bound_review_uses_the_same_flag_order_target(context, tmp_path):
    import sys

    executable = tmp_path / "gh-fixture"
    executable.write_text("#!" + sys.executable + "\nimport json,sys\nprint(json.dumps({'argv':sys.argv[1:],'stdin':sys.stdin.read()}))\n")
    executable.chmod(0o700)
    actual = await GitHubCLI(str(executable))({
        "argv": ["pr", "review", "--body", "literal body", "-R", "HalfAnElephant/Example", "--approve", "7"],
        "reviewed_head_sha": "a" * 40,
    }, context)
    captured = json.loads(actual["stdout"])
    assert captured["argv"][1] == "repos/halfanelephant/example/pulls/7/reviews"
    assert json.loads(captured["stdin"]) == {"body": "literal body", "event": "APPROVE", "commit_id": "a" * 40}


@pytest.mark.parametrize("argv", [
    ["issue", "create", "--body-file", "secret=local.txt"],
    ["api", "--input=secret=local.json", "repos/a/b/issues"],
])
async def test_file_options_cannot_hide_local_paths_with_equals(context, argv):
    cli = GitHubCLI("unreachable-executable")
    with pytest.raises(ValueError, match="file input"):
        await cli({"argv": argv}, context)


@pytest.mark.parametrize("argv", [
    ["run", "rerun", "--repo", "HalfAnElephant/Example", "7"],
    ["api", "--method", "POST", "repos/HalfAnElephant/Example/actions/runs/7/rerun"],
])
async def test_rerun_readback_keeps_flag_order_and_api_target(context, argv):
    attempt = 1

    async def cli(arguments, ctx):
        if arguments["argv"][1] == "user":
            return result({"login": "actor"})
        assert arguments["argv"][1] == "repos/halfanelephant/example/actions/runs/7"
        return result({"id": 7, "run_attempt": attempt})

    verifier = GitHubVerifier(cli)
    arguments = {"argv": argv}
    action = classify(argv, context)
    arguments["_preconditions"] = await verifier.prepare(context, action, arguments)
    attempt = 2
    actual = await verifier(context, action, arguments, {"exit_code": 0})
    assert actual[0] == "VERIFIED" and actual[1]["effect"] == "rerun_accepted"


@pytest.mark.parametrize("stdin,query,effect", [
    ('{"query":"mutation { createIssue { id } }"}', "query { viewer { login } }", "unknown"),
    ('{"query":"query { viewer { login } }"}', "mutation { createIssue { id } }", "read"),
    ('{"query":123}', "query { viewer { login } }", "unknown"),
    ('not-json', "query { viewer { login } }", "unknown"),
])
def test_graphql_classification_uses_actual_input_body(context, stdin, query, effect):
    action = classify(["api", "--input=-", "-f", "query=" + query, "graphql"], context, stdin)
    assert action.effect == effect


@pytest.mark.parametrize("value,expected", [("001", 1), ("-01", -1), ("true", True), ("null", None)])
def test_typed_fields_match_gh_integer_conversion_without_changing_raw_fields(value, expected):
    assert api_payload(["api", "user", "-F", "value=" + value]) == {"value": expected}
    assert api_payload(["api", "user", "-f", "value=" + value]) == {"value": value}


@pytest.mark.parametrize("argv", [
    ["pr", "edit", "--body", "literal", "https://github.com/Other/Repo/pull/7"],
    ["pr", "review", "--comment", "https://github.com/Other/Repo/pull/7"],
    ["pr", "merge", "-s", "https://github.com/Other/Repo/pull/7"],
    ["pr", "view", "--json", "url", "https://github.com/Other/Repo/pull/7"],
    ["issue", "view", "--template", "https://ignored.example/body", "https://github.com/Other/Repo/issues/7"],
])
def test_options_before_url_resolve_real_repository_and_number(context, argv):
    action = classify(argv, context)
    assert action.resources[0] == "repo:github.com/other/repo"
    assert action.resources[1].endswith("/other/repo#7")
    assert GitHubVerifier(None)._number({"argv": argv}) == "7"


@pytest.mark.parametrize("argv", [
    ["pr", "edit", "--unknown", "https://github.com/Other/Repo/pull/7"],
    ["pr", "edit", "--body"],
    ["pr", "edit", "--", "--repo=Other/Repo"],
    ["release", "delete", "--yes", "v1"],
])
def test_unknown_or_ambiguous_cli_target_is_rejected(context, argv):
    with pytest.raises(ValueError):
        classify(argv, context)


async def test_cli_target_host_is_checked_before_credentials(context):
    credentials = []
    cli = GitHubCLI("unreachable", credentials=lambda c: credentials.append(c) or {})
    with pytest.raises(ValueError, match="host"):
        await cli({"argv": ["pr", "edit", "--body", "literal", "https://evil.example/a/b/pull/7"]}, context)
    assert not credentials


@pytest.mark.parametrize("argv,resources", [
    (["repo", "view", "--json", "name", "Other/Repo"], ("repo:github.com/other/repo",)),
    (["repo", "list", "--json", "name", "Other"], ("github:github.com",)),
    (["release", "view", "--json", "name", "--repo", "Other/Repo", "v1"], ("repo:github.com/other/repo",)),
    (["project", "view", "--owner", "Other", "7"], ("github:github.com",)),
])
def test_read_scope_tracks_actual_target_instead_of_context_repository(context, argv, resources):
    assert classify(argv, context).resources == resources


async def test_unclassified_effect_is_rejected_before_credentials(context):
    credentials = []
    cli = GitHubCLI("unreachable", credentials=lambda c: credentials.append(c) or {})
    with pytest.raises(ValueError, match="cannot be resolved"):
        await cli({"argv": ["api", "graphql", "--input", "-"],
                   "stdin": '{"query":"mutation { createIssue { id } }"}'}, context)
    assert not credentials
