"""No real platform provisioning: macOS tools are read-only stubs in subprocesses."""

import os
import plistlib
import shutil
import subprocess
from pathlib import Path

import pytest

from elephant import setup_macos

SOURCE = Path(__file__).resolve().parents[1] / "deploy" / "install-macos.sh"


@pytest.fixture
def fake_mac(tmp_path):
    deploy = tmp_path / "deploy"
    deploy.mkdir()
    script = deploy / SOURCE.name
    shutil.copyfile(SOURCE, script)
    for name in ("pyproject.toml", "uv.lock", "package.json", "package-lock.json"):
        (tmp_path / name).touch()
    (tmp_path / ".python-version").write_text("3.13.9\n")
    settings = tmp_path / "settings.yaml"
    settings.write_text("keep me\n")
    tools = tmp_path / "fake-tools"
    tools.mkdir()
    trace = tmp_path / "trace"
    env = {
        **os.environ,
        "PATH": f"{tools}:{os.defpath}",
        "TRACE": str(trace),
        "VIRTUAL_ENV": "/wrong-venv",
    }
    for tool, body in {
        "uname": '[ "$1" = -s ] && echo Darwin || echo "${TEST_ARCH:-arm64}"',
        "id": 'echo "${TEST_UID:-501}"',
        "sysctl": 'echo "${TEST_TRANSLATED:-0}"',
        "node": '[ "${TEST_BAD_NODE:-0}" != 1 ]',
        "npm": 'echo "npm $*" >>"$TRACE"',
        "uv": (
            '[ -z "${VIRTUAL_ENV:-}" ] || exit 22\n'
            'echo "uv $*" >>"$TRACE"\n'
            "mkdir -p .venv/bin\ntouch .venv/bin/elephant\nchmod +x .venv/bin/elephant"
        ),
        "xcode-select": "exit 1",
        "xcrun": "exit 1",
    }.items():
        path = tools / tool
        path.write_text("#!/bin/sh\n" + body + "\n")
        path.chmod(0o700)

    def run(*args, **variables):
        result = subprocess.run(
            ["sh", str(script), *args],
            cwd=tmp_path.parent,
            env={**env, **variables},
            text=True,
            capture_output=True,
            check=False,
        )
        assert settings.read_text() == "keep me\n"
        return result

    return tmp_path, trace, run


def test_dry_run_never_invokes_installers(fake_mac):
    root, trace, run = fake_mac
    assert run(TEST_UID="0").returncode == 0
    assert not trace.exists()
    assert not (root / ".venv").exists()


def test_arm64_apply_installs_locked_dependencies_only(fake_mac):
    root, trace, run = fake_mac
    result = run("--apply")
    assert result.returncode == 0, result.stderr
    assert trace.read_text().splitlines() == [
        "uv sync --locked --no-dev --no-editable --python 3.13.9",
        "npm ci --ignore-scripts --no-audit --no-fund",
    ]
    assert "./elephant" in result.stdout
    assert not (root / ".elephant-install.lock").exists()


@pytest.mark.parametrize(
    ("variables", "error"),
    [
        ({"TEST_UID": "0"}, "sudo"),
        ({"TEST_ARCH": "x86_64", "TEST_TRANSLATED": "1"}, "Rosetta"),
        ({"TEST_ARCH": "x86_64"}, "Command Line Tools"),
        ({"TEST_BAD_NODE": "1"}, "Node.js"),
    ],
)
def test_unsupported_mac_preflight_does_not_install(fake_mac, variables, error):
    _, trace, run = fake_mac
    result = run("--apply", **variables)
    assert result.returncode == 1 and error in result.stderr
    assert not trace.exists()


def test_apply_refuses_linked_dependency_directory(fake_mac):
    root, trace, run = fake_mac
    (root / "node_modules").symlink_to(root.parent, target_is_directory=True)
    result = run("--apply")
    assert result.returncode == 1 and "node_modules" in result.stderr
    assert not trace.exists()


@pytest.fixture
def fake_launchd(tmp_path, monkeypatch):
    monkeypatch.setattr(setup_macos.sys, "platform", "darwin")
    monkeypatch.setattr(setup_macos.os, "getuid", lambda: 501)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setattr(setup_macos, "_node_path", lambda: Path("/native/node/bin/node"))
    root = tmp_path / "Elephant with spaces"
    command = root / ".venv/bin/elephant"
    monkeypatch.setattr(setup_macos, "_installed_runtime", lambda uid: (root, command))
    monkeypatch.setattr(setup_macos, "_private", lambda *args, **kwargs: True)
    (tmp_path / "Library").mkdir()
    settings = tmp_path / "settings with spaces"
    calls = []
    state = {"domain": True, "loaded": False, "bootstrap": True}

    def launchctl(*args):
        calls.append(args)
        if args[0] == "bootstrap":
            state["loaded"] = state["bootstrap"]
            return state["bootstrap"]
        return state["domain"] if args[-1] == "gui/501" else state["loaded"]

    monkeypatch.setattr(setup_macos, "_launchctl", launchctl)
    path = tmp_path / "Library" / "LaunchAgents" / f"{setup_macos.LABEL}.plist"
    return path, settings, root, command, calls, state


def test_launch_agent_is_private_scoped_and_idempotent(fake_launchd):
    path, settings, root, command, calls, _ = fake_launchd
    assert setup_macos.install_launch_agent(settings) is True
    assert path.stat().st_mode & 0o777 == 0o600
    plist = plistlib.loads(path.read_bytes())
    assert plist["Label"] == "dev.elephant.bot"
    assert plist["WorkingDirectory"] == str(root)
    assert plist["EnvironmentVariables"]["PATH"].split(":")[0] == "/native/node/bin"
    assert plist["ProgramArguments"] == [
        str(command),
        "serve",
        "--config",
        str(settings / "config.yaml"),
        "--env-file",
        str(settings / "credentials.env"),
    ]
    assert plist["RunAtLoad"] is True
    assert setup_macos.install_launch_agent(settings) is False
    assert calls.count(("bootstrap", "gui/501", str(path))) == 1


def test_launch_agent_fails_closed_for_no_gui_unsafe_existing_or_failed_bootstrap(fake_launchd):
    path, settings, _, _, calls, state = fake_launchd
    state["domain"] = False
    with pytest.raises(setup_macos.LaunchAgentError, match="桌面会话"):
        setup_macos.install_launch_agent(settings)
    assert calls == [("print", "gui/501")]
    assert not path.parent.exists()

    state["domain"] = True
    state["bootstrap"] = False
    with pytest.raises(setup_macos.LaunchAgentError, match="未能加载"):
        setup_macos.install_launch_agent(settings)
    assert not path.exists()

    path.write_bytes(b"other launch agent")
    with pytest.raises(setup_macos.LaunchAgentError, match="未替换"):
        setup_macos.install_launch_agent(settings)
    assert path.read_bytes() == b"other launch agent"
    assert sum(args[0] == "bootstrap" for args in calls) == 1


def test_launch_agent_uncertain_bootstrap_preserves_new_file(fake_launchd, monkeypatch):
    path, settings, _, _, _, _ = fake_launchd

    def launchctl(*args):
        if args[0] == "bootstrap":
            raise setup_macos._LaunchctlUncertain("状态不明确")
        return args[-1] == "gui/501"

    monkeypatch.setattr(setup_macos, "_launchctl", launchctl)
    with pytest.raises(setup_macos.LaunchAgentError, match="状态不明确"):
        setup_macos.install_launch_agent(settings)
    assert path.is_file()


def test_brew_dependencies_require_explicit_apply(fake_mac):
    _, trace, run = fake_mac
    assert run("--with-deps").returncode == 2
    assert run("--dry-run", "--with-deps").returncode == 2
    assert not trace.exists()


def test_launch_agent_checks_node_at_detected_absolute_path(monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setattr(setup_macos.platform, "machine", lambda: "arm64")
    monkeypatch.setattr(setup_macos.shutil, "which", lambda *args, **kwargs: "/native/node/bin/node")
    calls = []

    def run(args, **kwargs):
        calls.append(args)
        return SimpleNamespace(returncode=1)

    monkeypatch.setattr(setup_macos.subprocess, "run", run)
    with pytest.raises(setup_macos.LaunchAgentError, match="架构不匹配"):
        setup_macos._node_path()
    assert calls[0][0] == "/native/node/bin/node"
    assert calls[0][-1] == "arm64"


def test_launch_agent_rejects_shared_or_linked_secret_files(tmp_path):
    file = tmp_path / "config"
    file.touch(mode=0o600)
    uid = file.stat().st_uid
    assert setup_macos._private(file, uid, secret=True)
    file.chmod(0o640)
    assert not setup_macos._private(file, uid, secret=True)
    file.chmod(0o600)
    link = tmp_path / "link"
    link.symlink_to(file)
    assert not setup_macos._private(link, uid, secret=True)


def test_launch_agent_loads_matching_inactive_job_on_explicit_choice(fake_launchd):
    path, profile, _, _, _, state = fake_launchd
    assert setup_macos.install_launch_agent(profile)
    state["loaded"] = False
    existing = path.stat().st_ino
    assert setup_macos.install_launch_agent(profile)
    assert path.stat().st_ino == existing
