"""Run the system setup's QQ flow under the service account, without TCP auth."""

import asyncio
import hashlib
import os
import pwd
import select
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path

from elephant.config import load_runtime_settings, load_settings
from elephant.credentials_env import credential_env

ACCOUNT = "elephant"
GROUP = "elephant-worker"
ROOT = Path("/opt/elephant")
CONFIG = Path("/etc/elephant/core.yaml")
STATE = Path("/var/lib/elephant")
UNIT = Path("/etc/systemd/system/elephant.service")
STAMP = CONFIG.with_name(".installed-unit.sha256")


def _service_is_stopped():
    try:
        result = subprocess.run(
            ["/usr/bin/systemctl", "show", "--property=ActiveState", "--value", "elephant.service"],
            env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"}, text=True,
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, check=False, timeout=5,
        )
    except subprocess.TimeoutExpired as exc:
        raise ValueError("cannot determine the service's active state") from exc
    state = result.stdout.strip()
    if result.returncode or state not in {
        "active", "reloading", "refreshing", "inactive", "failed", "activating", "deactivating", "maintenance",
    }:
        raise ValueError("cannot determine the service's active state")
    return state in {"inactive", "failed"}


def _managed_database(database):
    database = Path(database)
    if database.parent != STATE or database.resolve().parent != STATE:
        raise ValueError("system setup requires an installer-managed database")
    return database


def _service_command(*args):
    source = ROOT / ".venv/bin/python"
    if not source.is_file() or source.stat().st_uid != 0 or source.stat().st_mode & 0o022:
        raise ValueError("system setup requires the root-owned installed interpreter")
    return [
        "/usr/sbin/runuser", "-u", ACCOUNT, "-g", GROUP, "--", str(source),
        # Both root and service entry points must leave the trusted runtime unchanged.
        "-I", "-B", "-m", "elephant.setup_system", *args,
    ]


@contextmanager
def system_configuration_lock(settings):
    from elephant.setup import _DatabaseBusy

    database = _managed_database(settings.database)
    if not _service_is_stopped():
        raise _DatabaseBusy("the system service is already active")

    # Create/acquire the actual daemon lock under the service identity, never
    # as root in a directory controlled by the service. The child holds it for
    # the lifespan of its pipe, including while the administrator edits files.
    with subprocess.Popen(
        _service_command("lock", str(database)), cwd=ROOT,
        env={"PATH": "/usr/bin:/bin", "HOME": str(STATE), "LANG": "C.UTF-8"},
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
    ) as child:
        try:
            if not select.select([child.stdout], [], [], 5)[0]:
                raise ValueError("could not verify the system daemon lock")
            response = child.stdout.read(1)
            if response == b"B":
                raise _DatabaseBusy("the system service is already active")
            if response != b"K" or child.poll() is not None:
                raise ValueError("could not acquire the system daemon lock")
            # Also fence a unit using an older database from before a pending
            # admin config change: it may not use this particular file lock.
            if not _service_is_stopped():
                raise _DatabaseBusy("the system service is starting or active")
            yield
        finally:
            child.stdin.close()
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()


def _lock_child(database):
    from elephant.setup import _DatabaseBusy, _exclusive_database

    try:
        uid = os.geteuid()
        if uid == 0 or uid != pwd.getpwnam(ACCOUNT).pw_uid:
            return 1
        os.umask(0o077)
        with _exclusive_database(_managed_database(database)):
            sys.stdout.buffer.write(b"K")
            sys.stdout.buffer.flush()
            # Wait for the parent to close its write end. A parent crash also
            # closes the pipe, letting the unprivileged process drop the lock.
            sys.stdin.buffer.read(1)
        return 0
    except _DatabaseBusy:
        sys.stdout.buffer.write(b"B")
        sys.stdout.buffer.flush()
        return 1
    except (OSError, ValueError, KeyError):
        return 1


def start_installed_service(*, quiet=False):
    """Called only after the terminal operator explicitly opts in to enable/start."""
    if os.geteuid() != 0 or any(
        path.is_symlink() or not path.is_file() or path.stat().st_uid != 0 or path.stat().st_mode & 0o022
        for path in (UNIT, STAMP)
    ):
        raise ValueError("Elephant service is not managed by this installer")
    if hashlib.sha256(UNIT.read_bytes()).hexdigest() != STAMP.read_text().strip():
        raise ValueError("Elephant system unit differs from the installed copy")
    output = {"stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL} if quiet else {}
    return subprocess.run(
        ["/usr/bin/systemctl", "enable", "--now", "elephant.service"],
        env={"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"}, check=False, **output,
    ).returncode


def scan_as_service_account(settings, values):
    from elephant.setup import ADMIN_TOKEN, CREDENTIAL_KEY

    if os.geteuid() != 0 or Path(settings.database).resolve().parent != STATE:
        raise ValueError("system scan requires the installer-managed service and database")
    source = ROOT / ".venv/bin/python"
    if not source.is_file() or source.stat().st_uid != 0 or source.stat().st_mode & 0o022:
        raise ValueError("system scan requires the root-owned installed interpreter")

    # No credentials on argv and no broad pass-through of environment values.
    # The PID runs under the same identity as the systemd daemon, never root.
    key_name = settings.admin_token_env
    token, key = values.get(key_name), values.get(CREDENTIAL_KEY)
    if key_name not in {ADMIN_TOKEN, "QQBOT_ADMIN_TOKEN"} or not token or not key:
        raise ValueError("the managed service is missing credentials for onboarding")
    env = {
        "PATH": "/opt/elephant-node/bin:/usr/bin:/bin", "HOME": str(STATE), "LANG": "C.UTF-8",
        ADMIN_TOKEN: token, CREDENTIAL_KEY: key,
    }
    env["TERM"] = os.environ.get("TERM", "dumb")
    if "NO_COLOR" in os.environ:
        env["NO_COLOR"] = ""
    return subprocess.run(
        ["/usr/sbin/runuser", "-u", ACCOUNT, "-g", GROUP, "--", str(source), "-I", "-B", "-m", "elephant.setup_system"],
        cwd=ROOT, env=env, check=False,
    ).returncode


def _child_main():
    from elephant.setup import _scan_local

    try:
        # This entry point is not a daemon command. Require the real service identity.
        uid = os.geteuid()
        if uid == 0 or uid != pwd.getpwnam(ACCOUNT).pw_uid:
            return 1
        os.umask(0o077)
        settings = load_settings(CONFIG)
        runtime = load_runtime_settings(settings)
        if (runtime.database != settings.database or runtime.admin_token_env != settings.admin_token_env
                or Path(settings.database).resolve().parent != STATE):
            return 1
        token = credential_env(settings.admin_token_env)
        if not token:
            return 1
        return asyncio.run(_scan_local(settings, token))
    except KeyboardInterrupt:
        return 130
    except (OSError, ValueError, KeyError):
        print("无法启动终端扫码。如果服务正在运行，请用管理页扫码，或稍后重试。")
        return 1


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "lock":
        raise SystemExit(_lock_child(sys.argv[2]))
    raise SystemExit(_child_main() if len(sys.argv) == 1 else 2)
