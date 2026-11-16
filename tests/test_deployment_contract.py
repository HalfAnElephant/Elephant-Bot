import configparser
import os
import subprocess
from pathlib import Path

import pytest

from elephant.config import load_settings

ROOT = Path(__file__).resolve().parents[1]


def test_fresh_deployment_paths_and_identity_match_service():
    settings = load_settings(ROOT / "configs/production.yaml")
    service = configparser.ConfigParser(interpolation=None, strict=False)
    service.read(ROOT / "deploy/elephant.service")
    unit = service["Service"]
    instructions = (ROOT / "deploy/README.md").read_text()

    assert settings.database == "/var/lib/elephant/core.db"
    assert settings.internal_socket == f"/run/{unit['RuntimeDirectory']}/internal.sock"
    assert settings.orchestration.pi_executable == str(
        Path(unit["WorkingDirectory"]) / "node_modules/.bin/pi"
    )
    assert settings.tasks_enabled
    assert settings.interactive_execution_enabled
    assert f"getent passwd {unit['User']}" in instructions
    for group in (unit["Group"], "elephant"):
        assert f"groupadd --system {group}" in instructions
    assert f"--gid {unit['Group']}" in instructions
    assert f"install -d -o {unit['User']} -g {unit['Group']} -m 0700 /var/lib/elephant" in instructions
    assert "configs/production.yaml /etc/elephant/core.yaml" in instructions
    assert "deploy/elephant.service /etc/systemd/system/elephant.service" in instructions
    assert "deploy/elephant-job-user-setup.sh elephant elephant.service" in instructions
    assert "systemctl enable --now elephant.service" in instructions
    assert "!configs/production.yaml" in (ROOT / ".gitignore").read_text().splitlines()


@pytest.fixture
def job_setup(tmp_path, monkeypatch):
    commands = tmp_path / "bin"
    commands.mkdir()
    for name in ("loginctl", "systemctl", "systemd-run", "journalctl", "bwrap", "runuser", "id"):
        content = "#!/bin/sh\nexit 0\n"
        if name == "id":
            content = '#!/bin/sh\nif [ "$#" -eq 1 ]; then echo 0; else echo 1234; fi\n'
        elif name == "runuser":
            content = "#!/bin/sh\necho running\n"
        command = commands / name
        command.write_text(content)
        command.chmod(0o755)
    monkeypatch.setenv("PATH", str(commands) + os.pathsep + os.environ["PATH"])
    script = tmp_path / "setup.sh"
    script.write_text(
        (ROOT / "deploy/elephant-job-user-setup.sh").read_text()
        .replace("/etc/elephant", str(tmp_path / "etc/elephant"))
        .replace("/etc/systemd/system", str(tmp_path / "etc/systemd/system"))
    )
    return script


@pytest.mark.parametrize("unit", [None, "elephant.service", "custom-daemon.service"])
def test_job_setup_targets_selected_unit_and_is_repeatable(job_setup, tmp_path, unit):
    arguments = ["sh", str(job_setup), "elephant"]
    if unit:
        arguments.append(unit)
    expected_unit = unit or "elephant.service"
    drop_in = tmp_path / "etc/systemd/system" / f"{expected_unit}.d/jobs.conf"
    environment = tmp_path / "etc/elephant/jobs.env"

    subprocess.run(arguments, check=True, capture_output=True, text=True)
    expected = (
        "[Unit]\nRequires=user@1234.service\nAfter=user@1234.service\n"
        "[Service]\nBindReadOnlyPaths=/run/user/1234\n"
    )
    assert drop_in.read_text() == expected
    assert environment.read_text() == (
        "XDG_RUNTIME_DIR=/run/user/1234\nDBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1234/bus\n"
    )
    subprocess.run(arguments, check=True, capture_output=True, text=True)
    assert drop_in.read_text() == expected
    assert list((tmp_path / "etc/systemd/system").iterdir()) == [drop_in.parent]


@pytest.mark.parametrize("unit", ["", "../elephant.service", "elephant.service\n[Service]", "elephant", ".service"])
def test_job_setup_rejects_invalid_unit_before_writing(job_setup, tmp_path, unit):
    result = subprocess.run(["sh", str(job_setup), "elephant", unit], check=False, capture_output=True, text=True)
    assert result.returncode == 2
    assert "SERVICE_UNIT" in result.stderr
    assert not (tmp_path / "etc").exists()
