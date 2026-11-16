"""No real platform provisioning: macOS tools are read-only stubs in subprocesses."""

import os
import plistlib
import shlex
import shutil
import subprocess
from pathlib import Path

import pytest

from elephant import setup_macos

SOURCE = Path(__file__).resolve().parents[1] / "deploy" / "install-macos.sh"
PI_FIXTURE = """#!/bin/sh
[ "$PI_OFFLINE" = 1 ] && [ -z "${TEST_SECRET:-}" ] || exit 10
if [ "$*" = "--offline --version" ]; then
    printf '1.5.0\n'
else
    IFS= read -r request || exit 1
    [ "$request" = '{"id":"elephant-install-check","type":"get_state"}' ] || exit 2
    printf '%s\n' '{"id":"elephant-install-check","type":"response","command":"get_state","success":true,"data":{"isStreaming":false}}'
fi
"""


@pytest.fixture
def fake_mac(tmp_path):
    deploy = tmp_path / "deploy"
    deploy.mkdir()
    script = deploy / SOURCE.name
    fake_brew_bin = tmp_path / "homebrew" / "bin"
    fake_brew_bin.mkdir(parents=True)
    script.write_text(SOURCE.read_text().replace("/opt/homebrew/bin", str(fake_brew_bin)))
    shutil.copyfile(SOURCE.with_name("pi-install.sh"), deploy / "pi-install.sh")
    for name in ("pyproject.toml", "uv.lock", "package.json", "package-lock.json"):
        (tmp_path / name).touch()
    (tmp_path / ".python-version").write_text("3.13.9\n")
    settings = tmp_path / "settings.yaml"
    settings.write_text("keep me\n")
    (tmp_path / "pi-fixture").write_text(PI_FIXTURE)
    native_node = shlex.quote(shutil.which("node") or "/nonexistent/node")
    tools = tmp_path / "fake-tools"
    tools.mkdir()
    trace = tmp_path / "trace"
    env = {
        **os.environ,
        "PATH": str(tools),
        "TRACE": str(trace),
        "VIRTUAL_ENV": "/wrong-venv",
    }
    for command in ("awk", "chmod", "cp", "dirname", "env", "grep", "mkdir", "mktemp", "mv", "rm", "rmdir", "touch"):
        (tools / command).symlink_to(shutil.which(command))
    for tool, body in {
        "uname": '[ "$1" = -s ] && echo Darwin || echo "${TEST_ARCH:-arm64}"',
        "id": 'echo "${TEST_UID:-501}"',
        "sysctl": 'echo "${TEST_TRANSLATED:-0}"',
        "node": (
            'if [ "$1" = -p ]; then echo 1.5.0; exit 0; fi\n'
            f'case "$2" in *spawnSync*) exec {native_node} "$@";; esac\n'
            '[ "${TEST_BAD_NODE:-0}" != 1 ]'
        ),
        "npm": (
            'if [ "$*" = --version ]; then [ "${TEST_BAD_NPM:-0}" != 1 ]; exit; fi\n'
            'echo "npm $*" >>"$TRACE"\n'
            '[ "${TEST_MISSING_PI:-0}" = 1 ] && exit 0\n'
            'mkdir -p node_modules/.bin\n'
            'cp pi-fixture node_modules/.bin/pi\n'
            'chmod +x node_modules/.bin/pi'
        ),
        "ffmpeg": (
            '[ "${TEST_BROKEN_FFMPEG:-0}" != 1 ] || exit 11\n'
            'printf "ffmpeg version %s\\n" "${TEST_FFMPEG_VERSION:-n7.1}"'
        ),
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

    brew = fake_brew_bin / "brew"
    brew.write_text(
        "#!/bin/sh\n"
        f'if [ "$1" = --prefix ]; then echo "{fake_brew_bin.parent}"; exit 0; fi\n'
        'echo "brew $*" >>"$TRACE"\n'
        'shift\n'
        "for formula do\n"
        f'  if [ "$formula" = node ]; then cp "{tools}/node-template" "{fake_brew_bin}/node"; '
        f'cp "{tools}/npm-template" "{fake_brew_bin}/npm"; fi\n'
        "done\n"
    )
    brew.chmod(0o700)
    for name in ("node", "npm"):
        shutil.copy2(tools / name, tools / (name + "-template"))

    def run(*args, **variables):
        result = subprocess.run(
            ["/bin/sh", str(script), *args],
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
    result = run("--apply", "--install-pi")
    assert result.returncode == 0, result.stderr
    assert trace.read_text().splitlines() == [
        "npm ci --ignore-scripts --no-audit --no-fund",
        "uv sync --locked --no-dev --no-editable --python 3.13.9",
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
    monkeypatch.setattr(setup_macos, "_node_path", lambda uid, root: Path("/native/node/bin/node"))
    monkeypatch.setattr(setup_macos, "_trusted_path", lambda path, uid, **kwargs: path)
    monkeypatch.setattr(setup_macos, "_safe_acl", lambda *args, **kwargs: True)
    root = tmp_path / "Elephant with spaces"
    monkeypatch.setattr(setup_macos, "_trusted_pi", lambda *args: root / "node_modules/.bin/pi")
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
    assert "EnvironmentVariables" not in plist
    argv = plist["ProgramArguments"]
    assert argv[3].split(":")[0] == "PATH=/native/node/bin"
    assert argv == [
        "/usr/bin/env",
        "-i",
        f"HOME={path.parents[2]}",
        argv[3],
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


def test_launch_agent_checks_native_node_path_and_declared_minimum(tmp_path, monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setattr(setup_macos.platform, "machine", lambda: "arm64")
    monkeypatch.setattr(setup_macos.shutil, "which", lambda *args, **kwargs: "/native/node/bin/node")
    monkeypatch.setattr(setup_macos, "_trusted_path", lambda path, uid, **kwargs: path)
    monkeypatch.setattr(setup_macos, "_safe_acl", lambda *args, **kwargs: True)
    calls = []

    def run(args, **kwargs):
        calls.append(args)
        return SimpleNamespace(returncode=1)

    monkeypatch.setattr(setup_macos.subprocess, "run", run)
    with pytest.raises(setup_macos.LaunchAgentError, match="架构不匹配"):
        setup_macos._node_path(501, tmp_path)
    assert calls[0][0] == "/native/node/bin/node"
    assert calls[0][-3:] == ["arm64", "/native/node/bin/node", str(tmp_path / "package.json")]
    assert "process.versions.node" in calls[0][2] and "engines.node" in calls[0][2]


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


def test_npm_success_without_pi_is_not_installer_success(fake_mac):
    _, trace, run = fake_mac
    result = run("--apply", "--install-pi", TEST_MISSING_PI="1")
    assert result.returncode == 1
    assert "可执行 Pi" in result.stderr
    assert "安装完成" not in result.stdout
    assert any(line.startswith("npm ci") for line in trace.read_text().splitlines())


@pytest.mark.parametrize("fault", ["TEST_BAD_NODE", "TEST_BAD_NPM", "missing-npm"])
def test_with_deps_does_not_brew_other_packages_when_node_path_needs_repair(fake_mac, fault):
    root, trace, run = fake_mac
    (root / "fake-tools/ffmpeg").unlink()
    if fault == "missing-npm":
        (root / "fake-tools/npm").unlink()
        variables = {}
    elif fault == "TEST_BAD_NPM":
        (root / "fake-tools/npm").write_text("#!/bin/sh\nexit 1\n")
        variables = {}
    else:
        variables = {fault: "1"}
    result = run("--apply", "--with-deps", "--install-pi", **variables)
    assert result.returncode == 1 and "PATH" in result.stderr and "Node.js/npm" in result.stderr
    assert not trace.exists()


def test_with_deps_installs_completely_missing_node_pair_and_rechecks_it(fake_mac):
    root, trace, run = fake_mac
    for name in ("node", "npm"):
        (root / "fake-tools" / name).unlink()
    result = run("--apply", "--with-deps", "--install-pi")
    assert result.returncode == 0, result.stderr
    assert trace.read_text().splitlines()[0] == "brew install node"


@pytest.mark.parametrize(("variables", "expected"), [
    ({"TEST_FFMPEG_VERSION": "5.1.8"}, "版本低于 6"),
    ({"TEST_FFMPEG_VERSION": "snapshot"}, "无法确认 FFmpeg 版本"),
    ({"TEST_BROKEN_FFMPEG": "1"}, "FFmpeg 无法运行"),
])
def test_unsupported_ffmpeg_has_optional_voice_warning(fake_mac, variables, expected):
    _, _, run = fake_mac
    result = run("--apply", "--install-pi", **variables)
    assert result.returncode == 0, result.stderr
    assert expected in result.stdout


def test_missing_ffmpeg_has_distinct_optional_voice_warning(fake_mac):
    root, _, run = fake_mac
    (root / "fake-tools/ffmpeg").unlink()
    result = run("--apply", "--install-pi")
    assert result.returncode == 0, result.stderr
    assert "未检测到 FFmpeg" in result.stdout


def test_compatible_path_pi_is_reused_without_npm_install(fake_mac):
    root, trace, run = fake_mac
    pi = root / "fake-tools/pi"
    pi.write_text(PI_FIXTURE)
    pi.chmod(0o700)
    (root / "fake-tools/npm").unlink()
    result = run("--apply", TEST_SECRET="must-not-reach-probe")
    assert result.returncode == 0, result.stderr
    assert (root / ".elephant-pi").read_text().strip() == str(pi)
    assert trace.read_text().splitlines() == ["uv sync --locked --no-dev --no-editable --python 3.13.9"]


def test_unapproved_private_pi_is_not_installed_or_recorded(fake_mac):
    root, trace, run = fake_mac
    pi = root / "fake-tools/pi"
    pi.write_text("#!/bin/sh\necho 1.0.0\n")
    pi.chmod(0o700)
    result = run("--apply")
    assert result.returncode == 1
    assert "--install-pi" in result.stderr
    assert not (root / ".elephant-pi").exists()
    assert not (root / "node_modules").exists()
    assert not trace.exists()


def test_compatible_private_pi_is_reused_without_reinstall(fake_mac):
    root, trace, run = fake_mac
    assert run("--apply", "--install-pi").returncode == 0
    trace.unlink()
    assert run("--apply").returncode == 0
    assert (root / ".elephant-pi").read_text().strip() == "bundled"
    assert trace.read_text().splitlines() == ["uv sync --locked --no-dev --no-editable --python 3.13.9"]


def test_matching_version_without_rpc_is_not_compatible(fake_mac):
    root, trace, run = fake_mac
    pi = root / "fake-tools/pi"
    pi.write_text("#!/bin/sh\necho 1.5.0\n")
    pi.chmod(0o700)
    result = run("--apply")
    assert result.returncode == 1
    assert "无法验证为兼容" in result.stdout
    assert not (root / ".elephant-pi").exists()
    assert not trace.exists()


def test_unreadable_system_pi_falls_through_to_usable_private_copy(fake_mac):
    root, _, _ = fake_mac
    tools = root / "fake-tools"
    external = tools / "pi"
    private = root / "node_modules/.bin/pi"
    private.parent.mkdir(parents=True)
    for executable in (external, private):
        executable.write_text(PI_FIXTURE)
        executable.chmod(0o700)

    # Emulate the service account's effective denial without root, systemd, or
    # changing host ownership. The production ownership check stays independent.
    runuser = tools / "runuser"
    denied = root / "service-denial"
    runuser.write_text(
        '#!/bin/sh\n'
        '[ "$1 $2 $3 $4 $5" = "-u elephant -g elephant-worker --" ] || exit 3\n'
        'shift 5\n'
        f'if [ "$*" = "/usr/bin/test -x {external}" ]; then touch {shlex.quote(str(denied))}; exit 1; fi\n'
        'exec "$@"\n'
    )
    runuser.chmod(0o700)
    chown = tools / "chown"
    chown.write_text("#!/bin/sh\nexit 0\n")
    chown.chmod(0o700)
    realpath = tools / "realpath"
    realpath.write_text('#!/bin/sh\n[ "$1 $2" = "-e --" ] || exit 3\nprintf "%s\\n" "$3"\n')
    realpath.chmod(0o700)
    implementation = (root / "deploy/pi-install.sh").read_text()
    staged = root / "pi-provisioner.sh"
    staged.write_text(
        implementation
        .replace("/run/elephant-pi-probe.XXXXXX", str(root / "probe.XXXXXX"))
        .replace("extra_pi=/usr/local/bin/pi", "extra_pi=")
    )
    result = subprocess.run(
        ["/bin/sh", "-ec", '. "$1"; elephant_pi_root_path() { :; }; elephant_prepare_pi "$2" no system',
         "staged", str(staged), str(root)],
        env={"PATH": str(tools)},
        text=True, capture_output=True, check=False,
    )
    assert result.returncode == 0, result.stderr
    assert denied.exists()
    assert (root / ".elephant-pi").read_text() == "bundled\n"
