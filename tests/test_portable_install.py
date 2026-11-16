"""Portable installer dispatch: exercise its real shell with inert helpers."""
import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def script(path, source):
    path.write_text("#!/bin/sh\nset -eu\n" + source)
    path.chmod(0o755)


@pytest.fixture
def bundle(tmp_path):
    root = tmp_path / "Elephant bundle"
    deploy = root / "deploy"
    deploy.mkdir(parents=True)
    tools = tmp_path / "bin"
    tools.mkdir()
    uv_bin = root / ".venv/bin"
    uv_bin.mkdir(parents=True)
    for command in ("install.sh", "elephant"):
        shutil.copy2(ROOT / command, root)
    for name in ("install-system.sh", "install-local-linux.sh", "install-macos.sh"):
        script(deploy / name, 'printf "%s: %s\\n" "${0##*/}" "$*"\n')
    script(tools / "uname", 'printf "%s\\n" "$TEST_PLATFORM"\n')
    script(uv_bin / "elephant", 'printf "%s\\n" "$(pwd -P)" "$*"\n')
    return root, {**os.environ, "PATH": str(tools) + os.pathsep + os.environ["PATH"]}


@pytest.mark.parametrize(("platform", "args", "result"), [
    ("Linux", (), "install-local-linux.sh: --dry-run"),
    ("Linux", ("--system",), "install-system.sh: --dry-run"),
    ("Darwin", (), "install-macos.sh: --dry-run"),
    ("Darwin", ("--apply", "--with-deps"), "install-macos.sh: --apply --with-deps"),
])
def test_dispatch_selects_native_helper(bundle, platform, args, result):
    root, env = bundle
    run = subprocess.run([root / "install.sh", *args], env={**env, "TEST_PLATFORM": platform},
                         cwd=root.parent, capture_output=True, text=True, check=True)
    assert run.stdout.strip() == result


def test_guided_install_runs_launcher_from_the_bundle(bundle):
    root, env = bundle
    run = subprocess.run([root / "install.sh", "--apply", "--setup"], env={**env, "TEST_PLATFORM": "Linux"},
                         cwd=root.parent, capture_output=True, text=True, check=True)
    assert run.stdout.splitlines() == ["install-local-linux.sh: --apply", str(root.resolve()), "setup"]


@pytest.mark.parametrize(("platform", "args"), [
    ("Linux", ("--setup",)),
    ("Linux", ("--dry-run", "--apply")),
    ("Linux", ("--apply", "--with-deps")),
    ("Linux", ("--apply", "--system")),
    ("Darwin", ("--apply", "--system")),
    ("FreeBSD", ()),
])
def test_unsupported_or_unsafe_actions_stop_before_helper(bundle, platform, args):
    root, env = bundle
    run = subprocess.run([root / "install.sh", *args], env={**env, "TEST_PLATFORM": platform},
                         capture_output=True, text=True, check=False)
    assert run.returncode
    assert not run.stdout
