"""Credentialed Git transport runs only against a trusted, sanitized repository."""
import asyncio
import base64
import os
import re
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path, PurePosixPath

from .subprocesses import start_process, stop_process
from .types import Action, Capability


class GitService:
    def __init__(self, credentials, *, executable="git"):
        self.credentials, self.executable = credentials, executable
        self.locks = {}

    async def _run(self, arguments, directory, context, *, authenticate=False):
        env = {"PATH": os.defpath, "HOME": "/nonexistent/elephant-git", "LANG": "C.UTF-8",
               "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull, "GIT_TERMINAL_PROMPT": "0"}
        env.update({name: os.environ[name] for name in (
            "HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy", "NO_PROXY", "no_proxy",
        ) if name in os.environ})
        if proxy := os.environ.get("QQBOT_GIT_HTTPS_PROXY"):
            env.update(HTTPS_PROXY=proxy, https_proxy=proxy)
        if authenticate:
            token = self.credentials(context).get("GH_TOKEN") or self.credentials(context).get("GH_ENTERPRISE_TOKEN")
            if token:
                encoded = base64.b64encode(("x-access-token:" + token).encode()).decode()
                env.update(GIT_CONFIG_COUNT="1", GIT_CONFIG_KEY_0="http.extraHeader",
                           GIT_CONFIG_VALUE_0="Authorization: Basic " + encoded)
        process = await start_process(self.executable, "-c", "credential.helper=",
            "-c", "core.hooksPath=/dev/null", "-c", "protocol.file.allow=never", "-c", "http.followRedirects=false",
            *arguments, cwd=directory, env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            start_new_session=True)
        async def drain(stream):
            retained = bytearray()
            total = 0
            while chunk := await stream.read(65536):
                total += len(chunk)
                retained.extend(chunk[:max(0, 524288 - len(retained))])
            return retained.decode(errors="replace"), total > len(retained)
        async def collect():
            stdout, stderr = await asyncio.gather(drain(process.stdout), drain(process.stderr))
            await process.wait()
            return {"exit_code": process.returncode, "stdout": stdout[0], "stderr": stderr[0],
                    "truncated": stdout[1] or stderr[1]}
        try:
            return await asyncio.wait_for(collect(), 120)
        finally:
            await stop_process(process)

    def _copy_objects(self, source, target):
        import stat
        total = count = 0
        if isinstance(source, int):
            root_fd = os.dup(source)
        else:
            parent_fd = os.open(source.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                root_fd = os.open(source.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent_fd)
            finally:
                os.close(parent_fd)
        try:
            for directory, directories, files, directory_fd in os.fwalk(".", dir_fd=root_fd,
                                                                       follow_symlinks=False):
                relative = Path(directory)
                (target / relative).mkdir(parents=True, exist_ok=True)
                for name in directories:
                    if stat.S_ISLNK(os.stat(name, dir_fd=directory_fd, follow_symlinks=False).st_mode):
                        raise ValueError("Git object directories cannot be symlinks")
                for name in files:
                    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd)
                    try:
                        info = os.fstat(fd)
                        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                            raise ValueError("Git objects must be regular unlinked files")
                        total += info.st_size
                        count += 1
                        if total > 536870912 or count > 100000:
                            raise ValueError("Git object transfer budget exceeded")
                        with os.fdopen(fd, "rb", closefd=False) as incoming:
                            destination = target / relative / name
                            out = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                            with os.fdopen(out, "wb") as outgoing:
                                shutil.copyfileobj(incoming, outgoing)
                    finally:
                        os.close(fd)
        finally:
            os.close(root_fd)

    def _import_object(self, git_directory, relative, source):
        directory_fd = os.dup(git_directory)
        try:
            for component in ("objects", *relative.parts):
                try:
                    os.mkdir(component, mode=0o700, dir_fd=directory_fd)
                except FileExistsError:
                    pass
                child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory_fd)
                os.close(directory_fd)
                directory_fd = child
            try:
                fd = os.open(source.name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                             0o600, dir_fd=directory_fd)
            except FileExistsError:
                return
            with os.fdopen(fd, "wb") as outgoing, source.open("rb") as incoming:
                shutil.copyfileobj(incoming, outgoing)
        finally:
            os.close(directory_fd)

    def _relative_path(self, value):
        if not isinstance(value, str) or len(value) > 1024:
            raise ValueError("Git path must be a bounded workspace-relative directory")
        path = PurePosixPath(value)
        if path.is_absolute() or len(path.parts) > 16 or any(
            part == ".." or not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", part) for part in path.parts
        ):
            raise ValueError("Git path must remain inside the bound workspace")
        return path

    @contextmanager
    def _directory(self, context, relative):
        directory_fd = os.open(context.workspace, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            for component in relative.parts:
                child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory_fd)
                os.close(directory_fd)
                directory_fd = child
            yield directory_fd
        finally:
            os.close(directory_fd)

    def _copy_clone(self, source, target_fd, budget=None):
        budget = budget if budget is not None else {"bytes": 0, "files": 0}
        for path in source.iterdir():
            budget["files"] += 1
            if budget["files"] > 100000:
                raise ValueError("Git clone file count exceeds budget")
            if path.is_symlink():
                os.symlink(os.readlink(path), path.name, dir_fd=target_fd)
            elif path.is_dir():
                os.mkdir(path.name, mode=0o700, dir_fd=target_fd)
                child_fd = os.open(path.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=target_fd)
                try:
                    self._copy_clone(path, child_fd, budget)
                finally:
                    os.close(child_fd)
            elif path.is_file():
                size = path.stat().st_size
                budget["bytes"] += size
                if budget["bytes"] > 536870912:
                    raise ValueError("Git clone size exceeds budget")
                fd = os.open(path.name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                             path.stat().st_mode & 0o777, dir_fd=target_fd)
                with os.fdopen(fd, "wb") as outgoing, path.open("rb") as incoming:
                    shutil.copyfileobj(incoming, outgoing)
            else:
                raise ValueError("Git clone contains an unsupported file type")

    async def __call__(self, arguments, context):
        if not context.repository or not re.fullmatch(r"[\w.-]+/[\w.-]+", context.repository):
            raise ValueError("explicit bound GitHub repository required")
        operation = arguments["operation"]
        if operation not in {"clone", "fetch", "push"}:
            raise ValueError("unsupported Git transport operation")
        branch = arguments.get("branch")
        if (operation == "push" or operation == "clone" and branch is not None) and (
                not isinstance(branch, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_/-]*", branch) or "//" in branch):
            raise ValueError("invalid " + operation + " branch")
        path = self._relative_path(arguments.get("path", "repository" if operation == "clone" else "."))
        url = "https://" + context.host + "/" + context.repository + ".git"
        async with self.locks.setdefault(context.workspace, asyncio.Lock()):
            with tempfile.TemporaryDirectory(prefix="elephant-git-") as temporary:
                root = Path(temporary)
                if operation == "clone":
                    if not path.parts:
                        raise ValueError("clone requires a new workspace subdirectory")
                    with self._directory(context, path.parent) as parent_fd:
                        try:
                            os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
                        except FileNotFoundError:
                            pass
                        else:
                            raise ValueError("clone destination already exists")
                        clone_argv = ["clone", "--template="]
                        if branch is not None:
                            clone_argv += ["--depth=1", "--single-branch", "--branch", branch]
                        result = await self._run([*clone_argv, "--", url, "repository"], root,
                                                 context, authenticate=True)
                        if result["exit_code"] == 0:
                            os.mkdir(path.name, mode=0o700, dir_fd=parent_fd)
                            target_fd = os.open(path.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                                dir_fd=parent_fd)
                            try:
                                self._copy_clone(root / "repository", target_fd)
                            except BaseException:
                                shutil.rmtree(path.name, dir_fd=parent_fd)
                                raise
                            finally:
                                os.close(target_fd)
                            result.update(path=str(path), verified=True)
                        return result
                with self._directory(context, path) as repository_fd:
                    git_fd = os.open(".git", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=repository_fd)
                    try:
                        result = await self._run(["init", "--bare", "--template=", "."], root, context)
                        if result["exit_code"]:
                            return result
                        objects_fd = os.open("objects", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=git_fd)
                        try:
                            self._copy_objects(objects_fd, root / "objects")
                        finally:
                            os.close(objects_fd)
                        if operation == "push":
                            commit = arguments["commit"]
                            if not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", commit):
                                raise ValueError("push requires full commit hash")
                            result = await self._run(["push", "--porcelain", "--", url,
                                commit + ":refs/heads/" + branch], root, context, authenticate=True)
                            observed = await self._run(["ls-remote", "--refs", "--", url, "refs/heads/" + branch],
                                                      root, context, authenticate=True)
                            result["remote_observation"] = observed
                            result["verified"] = observed["exit_code"] == 0 and any(
                                line.split() == [commit, "refs/heads/" + branch]
                                for line in observed["stdout"].splitlines())
                            return result
                        result = await self._run(["fetch", "--no-tags", "--", url], root, context, authenticate=True)
                        if result["exit_code"] == 0:
                            for directory, _, files in os.walk(root / "objects"):
                                relative = Path(directory).relative_to(root / "objects")
                                for name in files:
                                    self._import_object(git_fd, relative, Path(directory) / name)
                            result["fetch_head"] = (root / "FETCH_HEAD").read_text()[:8192]
                            result["verified"] = True
                        result["path"] = str(path)
                        return result
                    finally:
                        os.close(git_fd)

    def register(self, registry):
        registry.register(Capability("git.transport", "Controlled authenticated clone, fetch and push",
            {"type": "object", "properties": {"operation": {"type": "string", "enum": ["clone", "fetch", "push"]},
             "path": {"type": "string"}, "commit": {"type": "string"}, "branch": {"type": "string"}},
             "required": ["operation"], "additionalProperties": False}, timeout_seconds=245), self,
            lambda a, c: Action("git." + a["operation"],
                ("repo:" + c.host + "/" + (c.repository or ""), "workspace:" + c.workspace), "write"))
