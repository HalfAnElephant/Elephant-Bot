"""Installer tests never use real commands, accounts, network or systemd writes."""

import hashlib
import re
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="staged Linux system installer")

DEPLOY = Path(__file__).resolve().parents[1] / "deploy"
INSTALLER = DEPLOY / "install-system.sh"
JOBS_SETUP = DEPLOY / "elephant-job-user-setup.sh"


def test_default_and_explicit_dry_run_do_not_call_host_tools(tmp_path):
    marker = tmp_path / "called"
    for command in ("id", "systemctl", "apt-get", "curl"):
        mock = tmp_path / command
        mock.write_text(f"#!/bin/sh\ntouch {shlex.quote(str(marker))}; exit 90\n")
        mock.chmod(0o755)
    for args in ([], ["--dry-run"]):
        result = subprocess.run(
            ["/bin/sh", str(INSTALLER), *args],
            env={"PATH": f"{tmp_path}:/usr/bin:/bin"},
            capture_output=True, text=True, check=True,
        )
        assert "elephant.service" in result.stdout
        assert "Keep /etc/elephant/core.yaml" in result.stdout
        assert not marker.exists()


@pytest.mark.parametrize("options", [
    ("--start-now",),
    ("--install-pi",),
    ("--dry-run", "--install-pi"),
    ("--apply", "--install-pi", "--install-pi"),
])
def test_invalid_option_does_not_apply(options):
    result = subprocess.run(
        ["/bin/sh", str(INSTALLER), *options], check=False, capture_output=True, text=True,
    )
    assert result.returncode == 2
    assert "Usage:" in result.stderr


def staged_preflight(
    tmp_path, *, uid="0", version="24.04", distro="ubuntu", architecture="x86_64",
    active="inactive", legacy=False, unmanaged=False, partial_jobs=False, systemd=True, cgroup=True,
    child_kill=True, root_kill=False, config_access=None, allow_pi=False,
    pending_content=None, stamped=False, unit_content="# operator-owned unit\n", active_later=False,
):
    root = tmp_path / "root"
    mockdir = tmp_path / "commands"
    mockdir.mkdir()
    marker = tmp_path / "mutation"
    audit = shlex.quote(str(marker))
    record_package = f"""printf '%s %s\n' "${{0##*/}}" "$*" >> {audit}"""
    commands = {
        "id": f'case "$*" in "-gn elephant") echo elephant-worker;; "-u elephant") echo 601;; *) echo {uid};; esac',
        "uname": f'case "$1" in -s) echo Linux;; -m) echo {architecture};; *) exit 2;; esac',
        "dpkg": '[ "$1" = --compare-versions ] && [ "$3" = ge ] && '
                'test "$(printf "%s\\n" "$2" "$4" | sort -V | tail -1)" = "$2"',
        "flock": "exit 0",
        "stat": "echo 0",
        "systemctl": (
            f"""case "$*" in
                'show -p ActiveState --value elephant.service') echo {active} ;;
                'show -p FragmentPath --value elephant.service') echo ;;
                *) exit 91 ;;
            esac"""
        ),
        "apt-get": record_package + '\n[ "$1" = update ] && exit 0; exit 92',
        "dnf": record_package + "\nexit 92",
        "pacman": record_package + "\nexit 92",
        "curl": f"touch {shlex.quote(str(marker))}; exit 92",
        "getent": "exit 2",
        "runuser": "exit 91",
    }
    if active_later:
        status_seen = shlex.quote(str(tmp_path / "active_checked"))
        commands["systemctl"] = (
            'case "$*" in\n'
            "'show -p ActiveState --value elephant.service') "
            f'if [ -e {status_seen} ]; then echo active; else touch {status_seen}; echo inactive; fi ;;\n'
            "'show -p FragmentPath --value elephant.service') echo ;;\n"
            '*) exit 91 ;; esac'
        )
        commands["apt-get"] = record_package
        empty_checksum = hashlib.sha256(b"").hexdigest()
        commands["curl"] = (
            'path=; while [ "$#" -gt 0 ]; do '
            'if [ "$1" = -o ]; then path=$2; break; fi; shift; done; '
            'case "$path" in */SHASUMS256.txt) '
            f'echo "{empty_checksum}  node-v22.19.0-linux-x64.tar.xz" >"$path" ;; '
            '*) : >"$path" ;; esac'
        )
    if config_access is not None:
        state = shlex.quote(str(root / "var/lib/elephant"))
        commands["getent"] = f'[ "$*" = "passwd elephant" ] && echo "elephant:x:601:601::{state}:/usr/sbin/nologin"'
        commands["runuser"] = (
            'test "$#" -eq 8 && test "$1 $2 $3 $4 $5 $6" = "-u elephant -g elephant-worker -- /usr/bin/test"'
            f' && test "$7" != {shlex.quote(config_access)}'
        )
    for name, source in commands.items():
        executable = mockdir / name
        executable.write_text(f"#!/bin/sh\n{source}\n")
        executable.chmod(0o755)
    for path in (
        "etc/systemd/system", "usr/local/libexec", "run/systemd/system", "run/lock",
        "sys/fs/cgroup", "opt/elephant/configs", "opt/elephant/deploy",
        "opt/elephant/scripts",
    ):
        (root / path).mkdir(parents=True, exist_ok=True)
    release = f'ID="{distro}"\n'
    if version:
        release += f'VERSION_ID="{version}"\n'
    (root / "etc/os-release").write_text(release)
    if cgroup:
        (root / "sys/fs/cgroup/cgroup.controllers").touch()
        if root_kill:
            (root / "sys/fs/cgroup/cgroup.kill").touch()
        if child_kill:
            (root / "sys/fs/cgroup/system.slice").mkdir()
            (root / "sys/fs/cgroup/system.slice/cgroup.kill").touch()
    if not systemd:
        (root / "run/systemd/system").rmdir()
    if legacy:
        (root / "etc/qqbot").mkdir()
    if unmanaged:
        (root / "etc/systemd/system/elephant.service").write_text(unit_content)
    if pending_content is not None or stamped:
        conf = root / "etc/elephant"
        conf.mkdir(exist_ok=True)
        if pending_content is not None:
            (conf / ".pending-unit.sha256").write_text(pending_content)
        if stamped:
            digest = hashlib.sha256(unit_content.encode()).hexdigest()
            (conf / ".installed-unit.sha256").write_text(digest + "\n")
    if partial_jobs:
        (root / "etc/elephant").mkdir()
        (root / "etc/elephant/jobs.env").write_text("# existing operator Job config\n")
    if config_access is not None:
        conf = root / "etc/elephant"
        conf.mkdir(exist_ok=True)
        (conf / "core.yaml").write_text("# existing operator settings\n")
        (conf / "core.yaml").chmod(0o640)
        (conf / "core.env").write_text("ELEPHANT_MODEL_KEY=fixture\n")
        (conf / "core.env").chmod(0o600)
    for path in (
        "uv.lock", "package-lock.json", "configs/production.yaml", ".env.example",
        "deploy/elephant.service", "deploy/elephant-job-user-setup.sh", "deploy/pi-install.sh",
        "scripts/elephant-daemon-stopped",
    ):
        (root / "opt/elephant" / path).touch()
    script = INSTALLER.read_text()
    script = script.replace("PATH=/usr/sbin:/usr/bin:/sbin:/bin", f"PATH={mockdir}:/usr/bin:/bin")
    script = re.sub(
        r'(?<![\w/])/(?:etc|opt|run|usr/local/libexec|var/lib|sys/fs)(?=/|[\s"])',
        lambda match: str(root) + match.group(),
        script,
    )
    # The staged source is intentionally owned by the test user.
    script = script.replace("! -user root -o ", "")
    executable = tmp_path / "install-system-staged.sh"
    executable.write_text(script)
    args = ["--apply", "--install-pi"] if allow_pi else ["--apply"]
    result = subprocess.run(
        ["/bin/sh", str(executable), *args], check=False, capture_output=True, text=True,
    )
    return result, marker


def test_apply_rejects_unprivileged_or_unsupported_host(tmp_path):
    user_dir = tmp_path / "user"
    user_dir.mkdir()
    result, mutation = staged_preflight(user_dir, uid="1000")
    assert result.returncode == 1
    assert "Run --apply as root" in result.stderr
    assert not mutation.exists()

    version_dir = tmp_path / "version"
    version_dir.mkdir()
    result, mutation = staged_preflight(version_dir, version="22.04")
    assert result.returncode == 1
    assert "Ubuntu 24.04 or newer" in result.stderr
    assert not mutation.exists()


def test_apply_rejects_legacy_before_packages(tmp_path):
    result, mutation = staged_preflight(tmp_path, legacy=True)
    assert result.returncode == 1
    assert "Legacy installation detected" in result.stderr
    assert not mutation.exists()


def test_apply_rejects_active_unit_before_packages(tmp_path):
    result, mutation = staged_preflight(tmp_path, active="active")
    assert result.returncode == 1
    assert "Stop elephant.service" in result.stderr
    assert not mutation.exists()


@pytest.mark.parametrize(("distro", "version", "architecture", "manager", "dependency"), [
    ("ubuntu", "24.04", "x86_64", "apt-get", "ffmpeg"),
    ("debian", "13", "aarch64", "apt-get", "dbus-user-session"),
    ("fedora", "43", "aarch64", "dnf", "ffmpeg-free"),
    ("arch", "", "x86_64", "pacman", "ffmpeg"),
])
def test_supported_hosts_use_their_own_package_manager(tmp_path, distro, version, architecture, manager, dependency):
    result, mutation = staged_preflight(tmp_path, distro=distro, version=version, architecture=architecture)
    assert result.returncode == (1 if distro == "arch" else 92), result.stderr
    invocations = [line.split() for line in mutation.read_text().splitlines()]
    assert all(parts[0] == manager for parts in invocations)
    assert dependency in invocations[-1]
    if distro == "arch":
        assert invocations[-1][1:4] == ["-S", "--needed", "--noconfirm"]
        assert "-Sy" not in invocations[-1] and "-Syu" not in invocations[-1]


@pytest.mark.parametrize(("host", "message"), [
    ({"distro": "debian", "version": "12"}, "Debian 13 or newer"),
    ({"distro": "fedora", "version": "42"}, "Fedora 43 or newer"),
    ({"distro": "mint"}, "Unsupported distribution"),
    ({"distro": "arch", "version": "", "architecture": "aarch64"}, "requires x86_64"),
    ({"architecture": "riscv64"}, "Unsupported CPU architecture"),
    ({"systemd": False}, "running systemd"),
    ({"cgroup": False}, "cgroup v2"),
    ({"child_kill": False}, "cgroup.kill in a non-root cgroup"),
    ({"child_kill": False, "root_kill": True}, "cgroup.kill in a non-root cgroup"),
])
def test_unsupported_hosts_fail_before_package_mutation(tmp_path, host, message):
    result, mutation = staged_preflight(tmp_path, **host)
    assert result.returncode == 1
    assert message in result.stderr
    assert not mutation.exists()


def test_apply_preserves_unmanaged_unit(tmp_path):
    result, mutation = staged_preflight(tmp_path, unmanaged=True)
    assert result.returncode == 1
    assert "Existing unit is unmanaged or locally edited" in result.stderr
    assert not mutation.exists()
    assert (
        tmp_path / "root/etc/systemd/system/elephant.service"
    ).read_text() == "# operator-owned unit\n"


def test_apply_preserves_incomplete_existing_job_config(tmp_path):
    result, mutation = staged_preflight(tmp_path, partial_jobs=True)
    assert result.returncode == 1
    assert "Existing Job provisioning is incomplete" in result.stderr
    assert not mutation.exists()
    assert (
        tmp_path / "root/etc/elephant/jobs.env"
    ).read_text() == "# existing operator Job config\n"

@pytest.mark.parametrize(("failure", "message"), [
    ("-x", "Service account cannot traverse"),
    ("-r", "Service account cannot read"),
])
def test_unusable_existing_config_fails_before_packages_without_mutating_files(tmp_path, failure, message):
    result, mutation = staged_preflight(tmp_path, config_access=failure)
    assert result.returncode == 1 and message in result.stderr
    assert not mutation.exists()
    config = tmp_path / "root/etc/elephant"
    assert (config / "core.yaml").read_text() == "# existing operator settings\n"
    assert (config / "core.yaml").stat().st_mode & 0o777 == 0o640
    assert (config / "core.env").read_text() == "ELEPHANT_MODEL_KEY=fixture\n"
    assert (config / "core.env").stat().st_mode & 0o777 == 0o600


def test_readable_existing_config_allows_preflight(tmp_path):
    result, mutation = staged_preflight(tmp_path, config_access="none")
    assert result.returncode == 92, result.stderr
    assert "apt-get install" in mutation.read_text()


def test_explicit_pi_approval_still_requires_privileged_preflight(tmp_path):
    result, mutation = staged_preflight(tmp_path, uid="1000", allow_pi=True)
    assert result.returncode == 1
    assert "Run --apply as root" in result.stderr
    assert not mutation.exists()


@pytest.mark.parametrize(("mode", "expected", "error"), [
    ("matching", 92, ""),
    ("mismatched", 1, "Existing unit is unmanaged or locally edited"),
    ("malformed", 1, "Pending installer unit digest is invalid"),
    ("pre_publish", 92, ""),
])
def test_unit_pending_digest_allows_only_exact_interrupted_state(tmp_path, mode, expected, error):
    installed = "# trusted existing unit\n"
    value = {
        "matching": hashlib.sha256(installed.encode()).hexdigest() + "\n",
        "mismatched": "a" * 64 + "\n",
        "malformed": "a" * 64 + "\nextra",
        # The earlier unit's final stamp still proves ownership; the pending
        # digest belongs to the not-yet-published unit.
        "pre_publish": "a" * 64 + "\n",
    }[mode]
    result, mutation = staged_preflight(tmp_path, unmanaged=True, unit_content=installed,
                                        pending_content=value, stamped=mode == "pre_publish")
    assert result.returncode == expected, result.stderr
    if error:
        assert error in result.stderr
    assert mutation.exists() == (expected == 92)
    assert (tmp_path / "root/etc/systemd/system/elephant.service").read_text() == installed
    assert (tmp_path / "root/etc/elephant/.pending-unit.sha256").read_text() == value

def test_service_start_during_preparation_aborts_before_runtime_replacement(tmp_path):
    result, mutation = staged_preflight(tmp_path, active_later=True, unmanaged=True, stamped=True)
    assert result.returncode == 1
    assert "Stop elephant.service" in result.stderr
    assert "apt-get install" in mutation.read_text()
    assert not (tmp_path / "root/opt/elephant-node").exists()
    assert (tmp_path / "root/etc/systemd/system/elephant.service").read_text() == "# operator-owned unit\n"


@pytest.fixture
def staged_jobs(tmp_path):
    config = tmp_path / "etc/elephant"
    dropin = tmp_path / "etc/systemd/system/elephant.service.d"
    config.mkdir(parents=True)
    dropin.mkdir(parents=True)
    tools = tmp_path / "tools"
    tools.mkdir()
    linger = tmp_path / "linger"
    for name, body in {
        "id": 'if [ "$#" -eq 1 ]; then echo 0; else echo 601; fi',
        "stat": "echo 0",
        "loginctl": f"touch {shlex.quote(str(linger))}",
        "systemctl": '[ "$1" != --user ] || echo running',
        "systemd-run": "exit 90",
        "journalctl": "exit 91",
        "bwrap": "exit 92",
        "runuser": 'echo running',
        "ln": 'case "$3:${FAULT:-}" in */jobs.conf:1) exit 93 ;; esac; exec /usr/bin/ln "$@"',
    }.items():
        command = tools / name
        command.write_text("#!/bin/sh\n" + body + "\n")
        command.chmod(0o700)
    script = tmp_path / "jobs-setup.sh"
    script.write_text(JOBS_SETUP.read_text().replace("/etc/", str(tmp_path) + "/etc/"))

    def run(fault=False):
        return subprocess.run(
            ["/bin/sh", str(script), "elephant", "elephant.service"],
            env={"PATH": f"{tools}:/usr/bin:/bin", "FAULT": str(int(fault))},
            text=True, capture_output=True, check=False,
        )

    return config / "jobs.env", dropin / "jobs.conf", linger, run


def test_interrupted_job_files_are_completed_only_if_survivor_matches(staged_jobs):
    env, dropin, _, run = staged_jobs
    assert run(fault=True).returncode == 93
    original = env.read_bytes()
    assert b"XDG_RUNTIME_DIR=/run/user/601" in original
    assert not dropin.exists()
    recovered = run()
    assert recovered.returncode == 0, recovered.stderr
    assert env.read_bytes() == original
    assert b"Requires=user@601.service" in dropin.read_bytes()


def test_job_helper_never_replaces_custom_existing_file(staged_jobs):
    env, dropin, linger, run = staged_jobs
    env.write_text("# operator-owned\n")
    result = run()
    assert result.returncode == 1
    assert "differs from generated defaults" in result.stderr
    assert env.read_text() == "# operator-owned\n"
    assert not dropin.exists() and not linger.exists()
