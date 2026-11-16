"""Installer tests never use real commands, accounts, network or systemd writes."""

import re
import shlex
import subprocess
from pathlib import Path

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
    tmp_path, *, uid="0", ubuntu="24.04", active="inactive", legacy=False,
    unmanaged=False, partial_jobs=False,
):
    root = tmp_path / "root"
    mockdir = tmp_path / "commands"
    mockdir.mkdir()
    marker = tmp_path / "mutation"
    commands = {
        "id": f"echo {uid}",
        "uname": "echo Linux",
        "flock": "exit 0",
        "stat": "echo 0",
        "systemctl": (
            f"""case "$*" in
                'show -p ActiveState --value elephant.service') echo {active} ;;
                'show -p FragmentPath --value elephant.service') echo ;;
                *) exit 91 ;;
            esac"""
        ),
        "apt-get": f"touch {shlex.quote(str(marker))}; exit 92",
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
    (root / "etc/os-release").write_text(f'ID=ubuntu\nVERSION_ID="{ubuntu}"\n')
    (root / "sys/fs/cgroup/cgroup.controllers").touch()
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
    result, mutation = staged_preflight(version_dir, ubuntu="22.04")
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


def test_fresh_preflight_reaches_stubbed_package_step(tmp_path):
    result, mutation = staged_preflight(tmp_path)
    assert result.returncode == 92, result.stderr
    assert mutation.exists()


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
