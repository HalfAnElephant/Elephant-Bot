"""Installer tests never use real commands, accounts, network or systemd writes."""

import re
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="staged Linux system installer")

INSTALLER = Path(__file__).resolve().parents[1] / "deploy" / "install-system.sh"


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
        (root / "etc/systemd/system/elephant.service").write_text("# operator-owned unit\n")
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
