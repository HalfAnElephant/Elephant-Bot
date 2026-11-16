"""Opt-in Elephant LaunchAgent, scoped to the logged-in macOS user."""

from __future__ import annotations

import os
import platform
import plistlib
import shutil
import stat
import subprocess
import sys
from pathlib import Path

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
    return root, command


def _node_path() -> Path:
    node = shutil.which("node", path=os.environ.get("PATH", ""))
    if not node or not Path(node).is_absolute() or ":" in node:
        raise LaunchAgentError("找不到原生 Node.js。请在装有 Node.js 的终端中重试。")
    arch = {"arm64": "arm64", "x86_64": "x64"}.get(platform.machine())
    if arch is None:
        raise LaunchAgentError("当前 macOS 架构不受支持。")
    try:
        result = subprocess.run(
            [
                node,
                "-e",
                'process.exit(process.platform === "darwin" && process.arch === process.argv[1] ? 0 : 1)',
                arch,
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise LaunchAgentError("无法验证 Node.js，请在已安装原生 Node.js 的终端重试。") from exc
    if result.returncode != 0:
        raise LaunchAgentError("Node.js 与当前 macOS 架构不匹配。")
    return Path(node)


def _plist(settings: Path, root: Path, command: Path) -> bytes:
    # Preserve an explicitly detected Node installation (for example, nvm) in
    # launchd, without copying arbitrary terminal environment or secrets.
    path = f"{_node_path().parent}:/usr/bin:/bin:/opt/homebrew/bin:/usr/local/bin"
    return plistlib.dumps(
        {
            "Label": LABEL,
            "ProgramArguments": [
                str(command),
                "serve",
                "--config",
                str(settings / "config.yaml"),
                "--env-file",
                str(settings / "credentials.env"),
            ],
            "WorkingDirectory": str(root),
            "EnvironmentVariables": {"PATH": path},
            "RunAtLoad": True,
        },
        fmt=plistlib.FMT_XML,
    )


def install_launch_agent(directory: Path | str) -> bool:
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
    settings = Path(directory).absolute()
    if (
        not _private(settings, uid, is_directory=True)
        or not _private(settings / "config.yaml", uid, secret=True)
        or not _private(settings / "credentials.env", uid, secret=True)
    ):
        raise LaunchAgentError("未找到安全的本地配置，未创建启动项。")
    data = _plist(settings, root, command)
    home = Path.home()
    library = home / "Library"
    agents = library / "LaunchAgents"
    if not _private(library, uid, is_directory=True):
        raise LaunchAgentError("用户 Library 目录不安全，未创建启动项。")
    try:
        agents.mkdir(mode=0o700, exist_ok=True)
    except OSError as exc:
        raise LaunchAgentError("无法创建用户 LaunchAgents 目录。") from exc
    if not _private(agents, uid, is_directory=True):
        raise LaunchAgentError("用户 LaunchAgents 目录不安全，未创建启动项。")

    path = agents / f"{LABEL}.plist"
    if path.exists() or path.is_symlink():
        try:
            same = _private(path, uid) and path.read_bytes() == data
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
