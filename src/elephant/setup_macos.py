"""Opt-in Elephant LaunchAgent, scoped to the logged-in macOS user."""

from __future__ import annotations

import os
import platform
import plistlib
import re
import shutil
import stat
import subprocess
import sys
from pathlib import Path

from elephant.pi_installation import DEFAULT_PI, MARKER, runtime_pi_path

LABEL = "dev.elephant.bot"


class LaunchAgentError(RuntimeError):
    """Safe to print in the setup wizard."""


class _LaunchctlUncertain(LaunchAgentError):
    """The command may have already modified the GUI domain."""


def _launchctl(*args: str) -> bool:
    try:
        return (
            subprocess.run(
                ["/bin/launchctl", *args],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=12,
                check=False,
            ).returncode
            == 0
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise _LaunchctlUncertain("未能确认 macOS 启动项状态，请先检查系统登录项。") from exc


def _private(path: Path, uid: int, *, is_directory: bool = False, secret: bool = False) -> bool:
    try:
        info = path.lstat()
    except OSError:
        return False
    kind = stat.S_ISDIR if is_directory else stat.S_ISREG
    unsafe = 0o077 if secret else 0o022
    return kind(info.st_mode) and info.st_uid == uid and not (info.st_mode & unsafe)


_ACL_ENTRY = re.compile(r"^\s*\d+:\s+.+?\s+(allow|deny)\s+([^\s]+)\s*$")
_ACL_READ = {
    "read",
    "execute",
    "list",
    "search",
    "readattr",
    "readextattr",
    "readsecurity",
    "file_inherit",
    "directory_inherit",
    "limit_inherit",
    "only_inherit",
    "inherited",
}


def _safe_acl(path: Path, *, secret: bool = False) -> bool:
    """Darwin POSIX mode bits alone do not account for non-owner ACL grants."""
    try:
        result = subprocess.run(
            ["/bin/ls", "-lde", str(path)],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=5,
            check=False,
            env={"LC_ALL": "C"},
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    lines = result.stdout.decode("utf-8", "replace").splitlines()
    if result.returncode or not lines:
        return False
    for line in lines[1:]:
        entry = _ACL_ENTRY.fullmatch(line)
        if not entry:
            return False
        if entry[1] == "allow" and (secret or not set(entry[2].split(",")).issubset(_ACL_READ)):
            return False
    return True


def _trusted_path(path: Path, uid: int, *, is_directory: bool = False) -> Path:
    """Validate both the spelled and resolved chain, including symlink parents and ACLs."""
    if not path.is_absolute() or ".." in path.parts or any(c in str(path) for c in "\r\n"):
        raise LaunchAgentError("登录启动需要安装在当前用户的受保护目录中。")
    try:
        pending = list(path.parts[1:])
        current = Path("/")
        seen_links: set[Path] = set()
        verified: set[Path] = set()
        while True:
            info = current.lstat()
            symlink = stat.S_ISLNK(info.st_mode)
            if info.st_uid not in {0, uid} or (
                not symlink and (info.st_mode & 0o022 or (current not in verified and not _safe_acl(current)))
            ):
                raise ValueError
            verified.add(current)
            if symlink:
                if current in seen_links or len(seen_links) >= 40:
                    raise ValueError
                seen_links.add(current)
                target = Path(os.readlink(current))
                if not target.is_absolute():
                    target = current.parent / target
                # Restart at root to vet ancestors of every intermediate link.
                target = Path(os.path.normpath(target))
                pending = [*target.parts[1:], *pending]
                current = Path("/")
                continue
            if not pending:
                if not (stat.S_ISDIR if is_directory else stat.S_ISREG)(info.st_mode):
                    raise ValueError
                if path.resolve(strict=True) != current:
                    raise ValueError
                return current
            if not stat.S_ISDIR(info.st_mode):
                raise ValueError
            current /= pending.pop(0)
    except (OSError, ValueError, RuntimeError) as exc:
        raise LaunchAgentError("登录启动目录或运行工具可被其他用户修改，请改用前台启动。") from exc


def _installed_runtime(uid: int) -> tuple[Path, Path]:
    venv = Path(sys.prefix)
    root = venv.parent
    command = venv / "bin" / "elephant"
    if (
        venv.name != ".venv"
        or not root.is_absolute()
        or not all(_private(path, uid, is_directory=True) for path in (root, venv, venv / "bin"))
        or not _private(command, uid)
        or not os.access(command, os.X_OK)
        or not _private(root / "package-lock.json", uid)
    ):
        raise LaunchAgentError("未找到当前用户的 Elephant 安装。请从已安装的源码目录打开向导。")
    _trusted_path(command, uid)
    # The console entry invokes the venv interpreter; validate its link chain too.
    _trusted_path(venv / "bin" / "python", uid)
    _trusted_path(venv / "pyvenv.cfg", uid)
    return root, command


def _trusted_pi(root: Path, uid: int, configured: str) -> Path:
    marker = root / MARKER
    if marker.exists() or marker.is_symlink():
        if not _private(marker, uid) and not _private(marker, 0):
            raise LaunchAgentError("Pi 安装记录不安全，请重新安装后再设置登录启动。")
        _trusted_path(marker, uid)
    try:
        chosen = runtime_pi_path(configured, root=root)
        executable = Path(chosen)
    except (OSError, ValueError) as exc:
        raise LaunchAgentError("无法读取 Pi 安装记录，请重新安装后再设置登录启动。") from exc
    if not executable.is_absolute():
        # A simple PATH name could differ between the wizard and the clean
        # login daemon. Installer selections are absolute or bundle-relative.
        if "/" not in chosen:
            raise LaunchAgentError("登录启动需要明确的 Pi 安装路径，请使用已校验的安装方式。")
        executable = root / executable
    resolved = _trusted_path(executable, uid)
    if not os.access(resolved, os.X_OK):
        raise LaunchAgentError("找不到可执行的 Pi，请重新安装后再设置登录启动。")
    return resolved


def _node_path(uid: int) -> Path:
    node = shutil.which("node", path=os.environ.get("PATH", ""))
    if not node or not Path(node).is_absolute() or ":" in node:
        raise LaunchAgentError("找不到原生 Node.js。请在装有 Node.js 的终端中重试。")
    resolved = _trusted_path(Path(node), uid)
    arch = {"arm64": "arm64", "x86_64": "x64"}.get(platform.machine())
    if arch is None:
        raise LaunchAgentError("当前 macOS 架构不受支持。")
    try:
        result = subprocess.run(
            [
                str(resolved),
                "-e",
                (
                    'const fs=require("node:fs"); process.exit(process.platform === "darwin" && '
                    "process.arch === process.argv[1] && fs.realpathSync(process.execPath) === process.argv[2] ? 0 : 1)"
                ),
                arch,
                str(resolved),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=5,
            check=False,
            env={"PATH": "/usr/bin:/bin"},
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise LaunchAgentError("无法验证 Node.js，请在已安装原生 Node.js 的终端重试。") from exc
    if result.returncode != 0:
        raise LaunchAgentError("Node.js 与当前 macOS 架构不匹配。")
    if resolved.name != "node":
        raise LaunchAgentError("找不到可用于登录启动的原生 Node.js 可执行文件。")
    return resolved


def _plist(settings: Path, root: Path, command: Path, home: Path, uid: int) -> bytes:
    # Set up a clean child environment equivalent to the wizard profile. In
    # particular, launchctl's user-wide variables cannot override dotenv keys.
    node_parent = _node_path(uid).parent
    if ":" in str(node_parent):
        raise LaunchAgentError("Node.js 路径不能用于登录启动。")
    sources = [node_parent, Path("/usr/bin"), Path("/bin")]
    for entry in (Path("/opt/homebrew/bin"), Path("/usr/local/bin")):
        if entry.exists():
            try:
                trusted = _trusted_path(entry, uid, is_directory=True)
            except LaunchAgentError:
                continue  # Optional discovery for e.g. Homebrew FFmpeg.
            if ":" not in str(trusted):
                sources.append(trusted)
    path = os.pathsep.join(str(entry) for entry in dict.fromkeys(sources))
    return plistlib.dumps(
        {
            "Label": LABEL,
            "ProgramArguments": [
                "/usr/bin/env",
                "-i",
                f"HOME={home}",
                f"PATH={path}",
                str(command),
                "serve",
                "--config",
                str(settings / "config.yaml"),
                "--env-file",
                str(settings / "credentials.env"),
            ],
            "WorkingDirectory": str(root),
            "RunAtLoad": True,
        },
        fmt=plistlib.FMT_XML,
    )


def install_launch_agent(directory: Path | str, *, pi_executable: str = DEFAULT_PI) -> bool:
    """Install and load the explicit user opt-in; never replace or restart an existing job.

    Returns True if launchctl loaded the job, False for an identical already-loaded job.
    A successful load is not evidence that the daemon is healthy.
    """
    if sys.platform != "darwin" or os.getuid() == 0:
        raise LaunchAgentError("仅支持登录 macOS 桌面的普通用户，不能通过 sudo 安装。")

    uid = os.getuid()
    domain = f"gui/{uid}"
    service = f"{domain}/{LABEL}"
    if not _launchctl("print", domain):
        raise LaunchAgentError("没有可用的 macOS 桌面会话，仍可从终端前台启动。")
    root, command = _installed_runtime(uid)
    _trusted_pi(root, uid, pi_executable)
    settings = Path(directory).absolute()
    if (
        not _private(settings, uid, is_directory=True)
        or not _private(settings / "config.yaml", uid, secret=True)
        or not _private(settings / "credentials.env", uid, secret=True)
    ):
        raise LaunchAgentError("未找到安全的本地配置，未创建启动项。")
    _trusted_path(settings, uid, is_directory=True)
    for name in ("config.yaml", "credentials.env"):
        if not _safe_acl(_trusted_path(settings / name, uid), secret=True):
            raise LaunchAgentError("本地配置存在额外的访问授权，未创建启动项。")
    home = _trusted_path(Path.home(), uid, is_directory=True)
    data = _plist(settings, root, command, home, uid)
    library = home / "Library"
    agents = library / "LaunchAgents"
    if not _private(library, uid, is_directory=True):
        raise LaunchAgentError("用户 Library 目录不安全，未创建启动项。")
    _trusted_path(library, uid, is_directory=True)
    try:
        agents.mkdir(mode=0o700, exist_ok=True)
    except OSError as exc:
        raise LaunchAgentError("无法创建用户 LaunchAgents 目录。") from exc
    if not _private(agents, uid, is_directory=True):
        raise LaunchAgentError("用户 LaunchAgents 目录不安全，未创建启动项。")

    _trusted_path(agents, uid, is_directory=True)
    path = agents / f"{LABEL}.plist"
    if path.exists() or path.is_symlink():
        try:
            same = _private(path, uid) and _trusted_path(path, uid) and path.read_bytes() == data
        except OSError:
            same = False
        if not same:
            raise LaunchAgentError("已存在不同的 Elephant 启动项，未替换或重启。")
        if _launchctl("print", service):
            return False
        if not _launchctl("bootstrap", domain, str(path)):
            raise LaunchAgentError("macOS 未能重新加载已有启动项；可以前台启动。")
        return True
    if _launchctl("print", service):
        raise LaunchAgentError("已加载同名启动项，未替换或重启。")

    fd = -1
    fingerprint = None
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        info = os.fstat(fd)
        fingerprint = (info.st_dev, info.st_ino)
        with os.fdopen(fd, "wb") as stream:
            fd = -1
            stream.write(data)
        if not _launchctl("bootstrap", domain, str(path)):
            if _launchctl("print", service):
                raise _LaunchctlUncertain("启动项可能已加载，未移除文件。请先检查系统登录项。")
            raise LaunchAgentError("macOS 未能加载 Elephant 启动项；可以前台启动。")
        return True
    except (OSError, LaunchAgentError) as exc:
        if fingerprint is not None and not isinstance(exc, _LaunchctlUncertain):
            try:
                current = path.lstat()
                if (current.st_dev, current.st_ino) == fingerprint:
                    path.unlink()
            except OSError:
                pass
        if isinstance(exc, LaunchAgentError):
            raise
        raise LaunchAgentError("无法安全写入 Elephant 启动项。") from exc
    finally:
        if fd != -1:
            os.close(fd)
