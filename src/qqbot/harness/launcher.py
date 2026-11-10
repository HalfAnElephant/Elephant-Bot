"""Fixed Ubuntu launcher, shared manifest checks and root-owned stop evidence."""

import argparse
import fcntl
import json
import os
import pwd
import re
import stat
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID, uuid4

from qqbot.harness.skills import SKILLSETS, skill_files, skill_version, trusted_skill_directory

POLICY = Path("/etc/qqbot/runner-policy.json")
MANIFESTS = Path("/var/lib/qqbot-worker/launch")
ROOT_MANIFESTS = Path("/var/lib/qqbot-runner-manifests")
WORKSPACES = Path("/var/lib/qqbot-runners")
LOCKS = Path("/var/lib/qqbot-runner-locks")
STOPS = Path("/var/lib/qqbot-runner-stops")
ROOT_DIRECTORY = Path("/var/lib/qqbot-runner-root")
LAUNCHER = "/usr/local/libexec/pi-runner-launch"
ENTRY = "/opt/qqbot/bin/pi-runner-entry"
EXTENSION = "/opt/qqbot/research_extension.ts"
GENERAL_EXTENSION = "/opt/qqbot/pi_bridge.ts"
NETWORK_NAMESPACE = "/run/netns/qqbot-runner"


def canonical_uuid(value):
    if not isinstance(value, str) or str(UUID(value)) != value:
        raise ValueError("canonical UUID required")
    return value


def trusted_json(path, *, uid=0, max_bytes=65536):
    path = Path(path)
    directory = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for component in path.parts[1:-1]:
            next_directory = os.open(
                component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory
            )
            os.close(directory)
            directory = next_directory
            info = os.fstat(directory)
            if info.st_uid not in {0, uid} or info.st_mode & 0o022:
                raise ValueError("untrusted parent directory")
        fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        try:
            info = os.fstat(fd)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_nlink != 1
                or info.st_uid != uid
                or info.st_mode & 0o022
            ):
                raise ValueError("untrusted configuration ownership or mode")
            content = os.read(fd, max_bytes + 1)
            if len(content) > max_bytes:
                raise ValueError("configuration exceeds size limit")
            value = json.loads(content)
            if not isinstance(value, dict):
                raise TypeError("configuration must be an object")
            return value
        finally:
            os.close(fd)
    finally:
        os.close(directory)


def production_policy(path=POLICY, *, require_verified=True):
    if sys.platform != "linux":
        raise RuntimeError("systemd runner requires Linux")
    policy = trusted_json(path)
    if require_verified and policy.get("isolation_verified") is not True:
        raise ValueError("target-host isolation acceptance must be explicitly verified")
    if set(policy) != {
        "isolation_verified",
        "approved_models",
        "proxy_host",
        "proxy_port_min",
        "proxy_port_max",
    }:
        raise ValueError("unknown or missing runner policy fields")
    if (
        not isinstance(policy["approved_models"], list)
        or not policy["approved_models"]
        or not all(
            isinstance(m, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}", m)
            for m in policy["approved_models"]
        )
    ):
        raise ValueError("approved model list required")
    import ipaddress

    host = ipaddress.ip_address(policy["proxy_host"])
    if host.version != 4 or host.is_loopback or not host.is_private:
        raise ValueError("proxy must use the dedicated private veth address")
    lower, upper = policy["proxy_port_min"], policy["proxy_port_max"]
    if (
        type(lower) is not int
        or type(upper) is not int
        or not 1024 <= lower <= upper <= 65535
        or upper - lower > 1000
    ):
        raise ValueError("invalid proxy port range")
    return policy


def validate_manifest(manifest, task_id, run_id, policy, *, check_deadline=True):
    version = manifest.get("schema_version")
    common = {"schema_version", "task_id", "run_id", "session_id", "profile", "model", "proxy_url", "expires_at"}
    expected = common | ({"model_token", "tool_token"} if version == 1 else
                         {"process_id", "process_epoch", "process_token", "model_context_window", "output_tokens", "soft_input_tokens", "keep_recent_tokens"})
    if version == 2 and (check_deadline or "skill_version" in manifest):
        expected.add("skill_version")
    if type(version) is not int or version not in {1, 2} or set(manifest) != expected:
        raise ValueError("invalid launch manifest schema")
    if canonical_uuid(manifest["task_id"]) != task_id:
        raise ValueError("launch identity mismatch")
    canonical_uuid(manifest["run_id"])
    canonical_uuid(manifest["session_id"])
    if version == 1:
        if manifest["run_id"] != run_id or manifest["session_id"] != run_id or manifest["profile"] != "pi_research":
            raise ValueError("launch identity mismatch")
    elif (canonical_uuid(manifest["process_id"]) != run_id or manifest["profile"] != "pi"
          or type(manifest["process_epoch"]) is not int or manifest["process_epoch"] < 1):
        raise ValueError("invalid general process identity")
    if version == 2 and (type(manifest["model_context_window"]) is not int
            or not 8192 <= manifest["model_context_window"] <= 2000000
            or type(manifest["output_tokens"]) is not int or not 1 <= manifest["output_tokens"] <= 32768):
        raise ValueError("invalid model limits")
    if version == 2 and "skill_version" in manifest and (
            not isinstance(manifest["skill_version"], str) or not re.fullmatch(r"[0-9a-f]{64}", manifest["skill_version"])):
        raise ValueError("invalid skill version")
    if version == 2 and (type(manifest["soft_input_tokens"]) is not int or type(manifest["keep_recent_tokens"]) is not int
            or not 0 < manifest["keep_recent_tokens"] < manifest["soft_input_tokens"]
            or manifest["soft_input_tokens"] + manifest["output_tokens"] > manifest["model_context_window"]):
        raise ValueError("invalid compaction limits")
    if manifest["model"] not in policy["approved_models"]:
        raise ValueError("model is not approved")
    parsed = urlsplit(manifest["proxy_url"])
    if (parsed.scheme != "http" or parsed.hostname != policy["proxy_host"] or parsed.username
            or parsed.password or parsed.path not in {"", "/"} or parsed.query or parsed.fragment):
        raise ValueError("invalid capability proxy URL")
    if not parsed.port or not policy["proxy_port_min"] <= parsed.port <= policy["proxy_port_max"]:
        raise ValueError("proxy port is not approved")
    for key in (("model_token", "tool_token") if version == 1 else ("process_token",)):
        if (not isinstance(manifest[key], str) or not 32 <= len(manifest[key]) <= 128
                or not all(c.isalnum() or c in "_-" for c in manifest[key])):
            raise ValueError("invalid capability token")
    deadline = datetime.fromisoformat(manifest["expires_at"])
    if deadline.tzinfo is None or (check_deadline and not 0 < (deadline - datetime.now(UTC)).total_seconds() <= (1800 if version == 1 else 86400)):
        raise ValueError("launch deadline expired or exceeds budget")
    return manifest


def verify_stop_marker(task_id, run_id, directory=STOPS):
    canonical_uuid(task_id)
    canonical_uuid(run_id)
    try:
        marker = trusted_json(Path(directory) / (run_id + ".json"))
        return (
            marker.get("task_id") == task_id
            and marker.get("run_id") == run_id
            and marker.get("unit") == "qqbot-run-" + run_id + ".service"
            and marker.get("stopped") is True
        )
    except (OSError, ValueError, TypeError):
        return False


def atomic_root_json(path, value, mode=0o444):
    path.parent.mkdir(mode=0o755 if path.parent == STOPS else 0o700, parents=True, exist_ok=True)
    parent_info = path.parent.stat(follow_symlinks=False)
    if not stat.S_ISDIR(parent_info.st_mode) or parent_info.st_uid != 0 or parent_info.st_mode & 0o022:
        raise ValueError("unsafe root-owned output directory")
    temporary = path.with_suffix(".tmp-" + str(uuid4()))
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode)
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, "w") as output:
            json.dump(value, output)
            output.flush()
            os.fsync(output.fileno())
        temporary.replace(path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def unit_stopped(unit):
    result = subprocess.run(
        [
            "/usr/bin/systemctl",
            "show",
            unit,
            "--property=ActiveState",
            "--property=LoadState",
            "--property=MainPID",
            "--property=ControlGroup",
        ],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
        env={"PATH": "/usr/bin:/bin"},
    )
    values = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
    if values.get("LoadState") == "not-found":
        return True
    if values.get("ActiveState") not in {"inactive", "failed"} or values.get("MainPID") != "0":
        return False
    group = values.get("ControlGroup")
    if not group:
        return True
    if not group.startswith("/") or ".." in Path(group).parts:
        return False
    events_path = Path("/sys/fs/cgroup") / group.lstrip("/") / "cgroup.events"
    try:
        return "populated 0" in events_path.read_text().splitlines()
    except FileNotFoundError:
        return True


def record_stop(task_id, run_id, unit):
    if not unit_stopped(unit):
        return False
    atomic_root_json(
        STOPS / (run_id + ".json"),
        {
            "task_id": task_id,
            "run_id": run_id,
            "unit": unit,
            "stopped": True,
            "verified_at": datetime.now(UTC).isoformat(),
        },
    )
    return True


def prepare_workspace(run_id):
    canonical_uuid(run_id)
    worker, runner = pwd.getpwnam("qqbot-worker"), pwd.getpwnam("qqbot-runner")
    workspace = WORKSPACES / run_id
    workspace.mkdir(mode=0o770)
    os.chown(workspace, runner.pw_uid, worker.pw_gid)
    os.chmod(workspace, 0o770, follow_symlinks=False)
    return workspace


def systemd_argv(task_id, run_id, policy, *, general=False, skill_version=None):
    workspace = str(WORKSPACES / run_id)
    extension = GENERAL_EXTENSION if general else EXTENSION
    properties = {
        "User": "qqbot-runner",
        "Group": "qqbot-worker",
        "UMask": "0007",
        "RootDirectory": str(ROOT_DIRECTORY),
        "MountAPIVFS": "yes",
        "WorkingDirectory": "/workspace",
        "NoNewPrivileges": "yes",
        "CapabilityBoundingSet": "",
        "PrivateTmp": "yes",
        "PrivateDevices": "yes",
        "ProtectSystem": "strict",
        "ProtectHome": "yes",
        "ProtectProc": "invisible",
        "ProcSubset": "pid",
        "ProtectKernelTunables": "yes",
        "ProtectKernelModules": "yes",
        "ProtectControlGroups": "yes",
        "RestrictSUIDSGID": "yes",
        "RestrictNamespaces": "yes",
        "RestrictAddressFamilies": "AF_UNIX AF_INET",
        "NetworkNamespacePath": NETWORK_NAMESPACE,
        "IPAddressDeny": "any",
        "IPAddressAllow": policy["proxy_host"],
        "TasksMax": "64",
        "MemoryMax": "1G",
        "CPUQuota": "100%",
        "RuntimeMaxSec": "86400" if general else "1800",
        "KillMode": "control-group",
        "ExitType": "cgroup",
        "TimeoutStopSec": "5",
        "BindPaths": workspace + ":/workspace",
        "BindReadOnlyPaths": f"/usr:/usr /lib:/lib -/lib64:/lib64 /opt/qqbot/node_modules:/opt/qqbot/node_modules {ENTRY}:{ENTRY} {extension}:{extension} {ROOT_MANIFESTS / (run_id + '.json')}:/run/qqbot/manifest.json",
        "ReadWritePaths": "/workspace",
    }
    if general:
        if not isinstance(skill_version, str) or not re.fullmatch(r"[0-9a-f]{64}", skill_version):
            raise ValueError("a pinned skill version is required for general runners")
        properties["BindReadOnlyPaths"] += f" {SKILLSETS / skill_version}:/skills"
    return [
        "/usr/bin/systemd-run",
        "--quiet",
        "--wait",
        "--pipe",
        "--collect",
        "--unit=qqbot-run-" + run_id,
        "--service-type=exec",
        *["--property=" + key + "=" + value for key, value in properties.items()],
        ENTRY,
        task_id,
        run_id,
    ]


@contextmanager
def run_lock(run_id):
    LOCKS.mkdir(mode=0o700, exist_ok=True)
    info = LOCKS.stat(follow_symlinks=False)
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
        raise ValueError("unsafe launch lock directory")
    fd = os.open(LOCKS / (run_id + ".lock"), os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        os.close(fd)


def registered(unit):
    result = subprocess.run(
        ["/usr/bin/systemctl", "show", unit, "--property=LoadState", "--value"],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
        env={"PATH": "/usr/bin:/bin"},
    )
    return result.returncode == 0 and result.stdout.strip() not in {"", "not-found"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=["start", "stop"])
    parser.add_argument("task_id")
    parser.add_argument("run_id")
    args = parser.parse_args()
    if os.geteuid() != 0:
        raise SystemExit("root-owned fixed launcher must run as root")
    task_id, run_id = canonical_uuid(args.task_id), canonical_uuid(args.run_id)
    policy = production_policy(require_verified=args.operation == "start")
    unit = "qqbot-run-" + run_id + ".service"
    env = {"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LANG": "C.UTF-8"}
    account = pwd.getpwnam("qqbot-worker")
    with run_lock(run_id):
        if args.operation == "stop":
            try:
                manifest = trusted_json(ROOT_MANIFESTS / (run_id + ".json"))
            except FileNotFoundError:
                manifest = validate_manifest(
                    trusted_json(MANIFESTS / (run_id + ".json"), uid=account.pw_uid),
                    task_id,
                    run_id,
                    policy,
                    check_deadline=False,
                )
            identity_key = "process_id" if manifest.get("schema_version") == 2 else "run_id"
            if manifest.get("task_id") != task_id or manifest.get(identity_key) != run_id:
                raise SystemExit("stop identity mismatch")
            subprocess.run(["/usr/bin/systemctl", "stop", unit], timeout=15, check=False, env=env)
            if not record_stop(task_id, run_id, unit):
                raise SystemExit("runner stop could not be verified")
            return
        manifest = validate_manifest(
            trusted_json(MANIFESTS / (run_id + ".json"), uid=account.pw_uid), task_id, run_id, policy
        )
        if manifest["schema_version"] == 2:
            directory = SKILLSETS / manifest["skill_version"]
            trusted_skill_directory(directory)
            if skill_version(skill_files(directory)) != manifest["skill_version"]:
                raise ValueError("production skill version does not match its content")
        if (STOPS / (run_id + ".json")).exists() or (ROOT_MANIFESTS / (run_id + ".json")).exists():
            raise SystemExit("run identity cannot be relaunched")
        for directory in (WORKSPACES, ROOT_DIRECTORY):
            info = directory.stat(follow_symlinks=False)
            if not stat.S_ISDIR(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
                raise SystemExit("untrusted runner installation directory")
        for executable in (ENTRY, GENERAL_EXTENSION if manifest["schema_version"] == 2 else EXTENSION):
            info = Path(executable).stat(follow_symlinks=False)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
                raise SystemExit("untrusted runner executable or extension")
        prepare_workspace(run_id)
        atomic_root_json(ROOT_MANIFESTS / (run_id + ".json"), manifest)
        process = subprocess.Popen(systemd_argv(task_id, run_id, policy, general=manifest["schema_version"] == 2,
                                   skill_version=manifest.get("skill_version")), env=env)
        deadline = time.monotonic() + 10
        while process.poll() is None and not registered(unit):
            if time.monotonic() >= deadline:
                process.terminate()
                process.wait(timeout=5)
                raise SystemExit("unit registration could not be confirmed")
            time.sleep(0.05)
    try:
        code = process.wait()
    finally:
        with run_lock(run_id):
            record_stop(task_id, run_id, unit)
    raise SystemExit(code)


if __name__ == "__main__":
    main()
