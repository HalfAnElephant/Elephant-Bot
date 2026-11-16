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


def test_invalid_option_does_not_apply():
    result = subprocess.run(
        ["/bin/sh", str(INSTALLER), "--start-now"], check=False, capture_output=True, text=True,
    )
    assert result.returncode == 2
    assert "Usage:" in result.stderr


def staged_preflight(
    tmp_path, *, uid="0", version="24.04", distro="ubuntu", architecture="x86_64",
    active="inactive", legacy=False, unmanaged=False, partial_jobs=False, systemd=True, cgroup=True,
):
    root = tmp_path / "root"
    mockdir = tmp_path / "commands"
    mockdir.mkdir()
    marker = tmp_path / "mutation"
    audit = shlex.quote(str(marker))
    record_package = f"""printf '%s %s\n' "${{0##*/}}" "$*" >> {audit}"""
    commands = {
        "id": f"echo {uid}",
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
    }
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
    if not systemd:
        (root / "run/systemd/system").rmdir()
    if legacy:
        (root / "etc/qqbot").mkdir()
    if unmanaged:
        (root / "etc/systemd/system/elephant.service").write_text("# operator-owned unit\n")
    if partial_jobs:
        (root / "etc/elephant").mkdir()
        (root / "etc/elephant/jobs.env").write_text("# existing operator Job config\n")
    for path in (
        "uv.lock", "package-lock.json", "configs/production.yaml", ".env.example",
        "deploy/elephant.service", "deploy/elephant-job-user-setup.sh",
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
    result = subprocess.run(
        ["/bin/sh", str(executable), "--apply"], check=False, capture_output=True, text=True,
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
