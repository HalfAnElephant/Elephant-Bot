"""Thin argv-only GitHub CLI adapter with conservative effect classification."""
import asyncio
import json
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlsplit

from .subprocesses import start_process, stop_process
from .types import Action


def flag_values(argv, *names):
    values = []
    for index, item in enumerate(argv):
        for name in names:
            if item == name and index + 1 < len(argv):
                values.append(argv[index + 1])
            elif item.startswith(name + "="):
                values.append(item.split("=", 1)[1])
            elif len(name) == 2 and item.startswith(name) and item != name:
                values.append(item[2:])
    return values



def api_arguments(argv):
    """Resolve the positional endpoint without treating flag values as destinations."""
    value_flags = {"--cache", "--field", "--raw-field", "--header", "--hostname", "--input",
                   "--jq", "--method", "--preview", "--template"}
    boolean_flags = {"--allow-escape-sequences", "--include", "--paginate", "--silent", "--slurp",
                     "--verbose", "--help"}
    short_flags = {"F": "--field", "f": "--raw-field", "H": "--header", "q": "--jq",
                   "X": "--method", "p": "--preview", "t": "--template", "i": "--include"}
    endpoints, options = [], []
    arguments = iter(argv[1:])
    for item in arguments:
        if item == "--":
            endpoints.extend(arguments)
            break
        if item.startswith("--"):
            name, separator, attached = item.partition("=")
            if name in value_flags:
                value = attached if separator else next(arguments, None)
                if value is None:
                    raise ValueError("GitHub API option requires a value")
                options.extend((name, value))
            elif name in boolean_flags:
                options.append(item)
            else:
                raise ValueError("cannot resolve GitHub API target with an unknown option")
        elif item.startswith("-") and item != "-":
            cluster = item[1:]
            while cluster:
                name = short_flags.get(cluster[0])
                if name is None:
                    raise ValueError("cannot resolve GitHub API target with an unknown option")
                cluster = cluster[1:]
                if name in value_flags:
                    value = cluster.lstrip("=") if cluster else next(arguments, None)
                    if value is None:
                        raise ValueError("GitHub API option requires a value")
                    options.extend((name, value))
                    break
                options.append(name)
        else:
            endpoints.append(item)
    if len(endpoints) != 1:
        raise ValueError("GitHub API requires exactly one resolvable endpoint")
    return ["api", endpoints[0], *options]

def classify(argv, context, stdin=""):
    if not argv or any(not isinstance(item, str) or "\x00" in item for item in argv):
        raise ValueError("argv must contain nonempty string arguments")
    if argv[0] == "api" and argv != ["api", "--help"]:
        argv = api_arguments(argv)
    repo_values = flag_values(argv, "--repo", "-R")
    repository = repo_values[-1] if repo_values else context.repository
    host_values = flag_values(argv, "--hostname")
    host = host_values[-1] if host_values else context.host
    if repository and len(repository.split("/")) == 3:
        host, repository = repository.split("/", 1)
    targets = argv[1:2] if argv[0] == "api" else argv[2:3]
    for item in targets:
        parsed = urlsplit(item)
        if parsed.scheme or parsed.netloc:
            if (parsed.hostname != context.host or parsed.scheme != "https" or parsed.username or parsed.password
                    or parsed.port not in {None, 443}):
                raise ValueError("GitHub target host does not match credential host")
            pieces = parsed.path.strip("/").split("/")
            if len(pieces) >= 2:
                repository = "/".join(pieces[:2])
    if host != context.host:
        raise ValueError("GitHub target host does not match credential host")
    repository = repository.lower() if repository else None
    resources = (f"repo:{host}/{repository}",) if repository else (f"github:{host}",)
    if len(argv) > 2 and argv[0] in {"pr", "issue"}:
        number = argv[2] if argv[2].isdigit() else None
        if not number and argv[2].startswith("https://"):
            match = re.search(r"/(?:pull|issues)/(\d+)", argv[2])
            number = match[1] if match else None
        if number and repository:
            resources += (f"{argv[0]}:{host}/{repository}#{number}",)
    family, command = argv[0], argv[1] if len(argv) > 1 else ""
    if (len(argv) <= 3 and argv[-1] == "--help") or argv in (["--version"], ["version"]):
        return Action("github.cli.read", resources)
    if family == "api":
        endpoint = command
        if endpoint.startswith("-"):
            return Action("github.unclassified", resources, "unknown")
        endpoint = endpoint.replace("{owner}/{repo}", repository or "{owner}/{repo}")
        if "%" in endpoint or "/../" in endpoint or "/./" in endpoint:
            return Action("github.unclassified", resources, "unknown")
        match = re.search(r"(?:^|/)repos/([^/]+/[^/]+)(?:/|$)", endpoint)
        if match:
            repository = match[1].lower()
            resources = (f"repo:{host}/{repository}",)
            target = re.search(r"/(pulls|issues)/(\d+)(?:/|$)", endpoint)
            if target:
                kind = "pr" if target[1] == "pulls" else "issue"
                resources += (f"{kind}:{host}/{repository}#{target[2]}",)
        fields = flag_values(argv, "-f", "-F", "--field", "--raw-field")
        method = flag_values(argv, "-X", "--method")
        method = method[-1].upper() if method else ("POST" if fields or flag_values(argv, "--input") else "GET")
        if endpoint.rstrip("/").endswith("graphql"):
            query = next((value.split("=", 1)[1] for value in fields if value.startswith("query=")), "")
            if not query and flag_values(argv, "--input"):
                try:
                    query = json.loads(stdin).get("query", "")
                except (ValueError, AttributeError):
                    pass
            # Multiple operations, directives, file input and aliases require richer resolution.
            clean = re.sub(r'"(?:\\.|[^"\\])*"|#[^\n]*', '""', query).strip()
            if clean.startswith("mutation"):
                return Action("github.unclassified", resources, "unknown")
            if clean and clean.startswith(("query", "{")) and not re.search(
                r"\b(mutation|subscription)\b", clean
            ):
                return Action("github.graphql.read", (f"github:{host}",))
            return Action("github.unclassified", resources, "unknown")
        suffix = endpoint.split("?")[0].split("/")
        kind = next((kind for segment, kind in (("pulls", "pr"), ("issues", "issue"),
            ("runs", "run"), ("workflows", "workflow"), ("releases", "release")) if segment in suffix), "api")
        effect = "read" if method in {"GET", "HEAD", "OPTIONS"} else "write"
        operation = "read" if effect == "read" else method.lower()
        if effect == "write":
            operation = None
            if kind in {"pr", "issue"} and suffix[-1] in {"pulls", "issues"} and method == "POST":
                operation = "create"
            elif kind in {"pr", "issue"} and suffix[-1] == "comments" and method == "POST":
                operation = "comment"
            elif kind == "pr" and suffix[-1] == "merge" and method == "PUT":
                operation = "merge"
            elif kind == "run" and suffix[-1] in {"rerun", "cancel"} and method == "POST":
                operation = suffix[-1]
            if operation is None and "reviews" not in suffix:
                return Action("github.unclassified", resources, "unknown")
        if "reviews" in suffix and effect == "write":
            body = dict(value.split("=", 1) for value in fields if "=" in value)
            try:
                body.update(json.loads(stdin) if stdin else {})
            except (ValueError, TypeError):
                pass
            operation = "review." + {"APPROVE": "approve", "REQUEST_CHANGES": "request_changes",
                                      "COMMENT": "comment"}.get(body.get("event"), "create")
        if kind == "api":
            return Action("github.unclassified", resources, "unknown")
        return Action(f"github.{kind}.{operation}", resources, effect)
    if family in {"pr", "issue", "repo", "run", "workflow", "release", "project"} and command in {
        "list", "view", "diff", "checks", "status", "search"
    }:
        return Action(f"github.{family}.read", resources)
    if family == "search":
        return Action("github.search.read", resources)
    if family == "pr" and command == "review":
        kind = "approve" if "--approve" in argv or "-a" in argv else (
            "request_changes" if "--request-changes" in argv or "-r" in argv else "comment"
        )
        return Action("github.pr.review." + kind, resources, "write")
    if family in {"pr", "issue", "repo", "run", "workflow", "release", "project"} and command in {
                   "create", "edit", "close", "reopen", "comment", "merge", "rerun", "cancel",
                   "delete", "enable", "disable", "dispatch", "run", "checkout", "clone", "download"}:
        return Action(f"github.{family}.{command}", resources, "write")
    return Action("github.unclassified", resources, "unknown")


class GitHubCLI:
    def __init__(self, executable="gh", *, credentials=None, config_directory=None, max_bytes=1048576):
        self.executable = executable
        self.credentials = credentials or (lambda context: {})
        self.config_directory = config_directory
        self.max_bytes = max_bytes

    async def inventory(self, context):
        result = await self({"argv": ["--version"]}, context)
        return {"adapter_version": "1", "cli_version": result["stdout"].splitlines()[0]
                if result["exit_code"] == 0 and result["stdout"] else "unavailable"}

    async def __call__(self, arguments, context):
        argv = arguments["argv"]
        stdin = arguments.get("stdin", "")
        timeout = arguments.get("timeout_ms", 30000)
        if not isinstance(timeout, (int, float)) or not 1 <= timeout <= 120000:
            raise ValueError("GitHub timeout_ms must be 1..120000")
        if not isinstance(stdin, str) or len(stdin.encode()) > 1048576:
            raise ValueError("GitHub stdin must be a bounded string")
        if arguments.get("reviewed_head_sha") and argv[:2] == ["pr", "review"]:
            reviewed = arguments["reviewed_head_sha"]
            if not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", reviewed):
                raise ValueError("reviewed_head_sha must be a full commit hash")
            action = classify(argv, context, stdin)
            repository = next((resource.removeprefix("repo:" + context.host + "/")
                               for resource in action.resources if resource.startswith("repo:")), None)
            number = argv[2] if len(argv) > 2 and argv[2].isdigit() else None
            if not repository or not number:
                raise ValueError("commit-bound review requires explicit repository and PR number")
            bodies = flag_values(argv, "--body", "-b")
            body = stdin if flag_values(argv, "--body-file", "-F") == ["-"] else (bodies[-1] if bodies else "")
            event = {"approve": "APPROVE", "request_changes": "REQUEST_CHANGES", "comment": "COMMENT"}[
                action.name.rsplit(".", 1)[-1]]
            argv = ["api", f"repos/{repository}/pulls/{number}/reviews", "--method", "POST", "--input", "-"]
            stdin = json.dumps({"event": event, "body": body, "commit_id": reviewed}, ensure_ascii=False)
        checked = api_arguments(argv) if argv[:1] == ["api"] and argv != ["api", "--help"] else argv
        if argv[:1] == ["api"]:
            classify(argv, context, stdin)
        for value in flag_values(checked, "--input", "--body-file", "--template-file", "-F"):
            if value != "-" and "=" not in value:
                raise ValueError("file input must use literal stdin or stdin_ref")
        for value in flag_values(checked, "--field", "-F"):
            if "=@" in value and not value.endswith("=@-"):
                raise ValueError("field file input must use stdin")
        if len(argv) > 1 and argv[1] in {"checkout", "clone", "download", "upload"}:
            raise ValueError("local GitHub file operations require the isolated workspace Git provider")
        environment = {"PATH": os.defpath, "LANG": "C.UTF-8", "GH_HOST": context.host,
                       "GH_PROMPT_DISABLED": "1", "GH_PAGER": "cat", "PAGER": "cat",
                       "GH_CONFIG_DIR": self.config_directory or "/nonexistent/elephant-gh",
                       "HOME": "/nonexistent/elephant-gh", "GIT_CONFIG_NOSYSTEM": "1",
                       "GIT_CONFIG_GLOBAL": os.devnull, "GIT_TERMINAL_PROMPT": "0"}
        if context.repository:
            environment["GH_REPO"] = context.repository
        credentials = self.credentials(context)
        if set(credentials) - {"GH_TOKEN", "GH_ENTERPRISE_TOKEN"}:
            raise ValueError("credential provider returned unsupported environment fields")
        environment.update(credentials)
        working = tempfile.TemporaryDirectory(prefix="elephant-gh-")
        command = [self.executable, *argv]
        if credentials:
            bubblewrap = shutil.which("bwrap")
            if sys.platform != "linux" or not bubblewrap:
                working.cleanup()
                raise RuntimeError("credentialed GitHub CLI requires Linux bubblewrap isolation")
            executable = shutil.which(self.executable) or self.executable
            command = [bubblewrap, "--unshare-all", "--share-net", "--die-with-parent", "--new-session"]
            for directory in ("/usr", "/bin", "/lib", "/lib64"):
                if Path(directory).exists():
                    command.extend(["--ro-bind", directory, directory])
            for file in ("/etc/resolv.conf", "/etc/hosts", "/etc/nsswitch.conf", "/etc/ssl/certs"):
                if Path(file).exists():
                    command.extend(["--ro-bind", str(Path(file).resolve()), file])
            if not str(Path(executable).resolve()).startswith(("/usr/", "/bin/")):
                working.cleanup()
                raise ValueError("credentialed gh executable must be installed under /usr or /bin")
            command.extend(["--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp",
                            "--bind", working.name, "/work", "--chdir", "/work", "--", executable, *argv])
        try:
            process = await start_process(
                *command, cwd=working.name, env=environment,
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE, start_new_session=True,
            )
        except BaseException:
            working.cleanup()
            raise
        async def drain(stream):
            retained = bytearray()
            total = 0
            while chunk := await stream.read(65536):
                total += len(chunk)
                retained.extend(chunk[:max(0, self.max_bytes - len(retained))])
            return retained.decode(errors="replace"), total
        async def feed():
            try:
                process.stdin.write(stdin.encode())
                await process.stdin.drain()
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                process.stdin.close()
        async def communicate():
            stdout, stderr, _ = await asyncio.gather(drain(process.stdout), drain(process.stderr), feed())
            await process.wait()
            return {"exit_code": process.returncode, "stdout": stdout[0], "stderr": stderr[0],
                    "truncated": stdout[1] > self.max_bytes or stderr[1] > self.max_bytes}
        try:
            result = await asyncio.wait_for(communicate(), timeout / 1000)
            for token in credentials.values():
                if token:
                    result["stdout"] = result["stdout"].replace(token, "[REDACTED]")
                    result["stderr"] = result["stderr"].replace(token, "[REDACTED]")
            return result
        finally:
            try:
                await stop_process(process)
            finally:
                working.cleanup()
