"""Check system Pi dependency trust without executing a candidate or using root."""

import shlex
import subprocess
import sys
from pathlib import Path

import pytest

SOURCE = Path(__file__).resolve().parents[1] / "deploy/pi-install.sh"


@pytest.fixture
def system_pi(tmp_path):
    root = tmp_path.resolve()
    bundle = root / "bundle"
    interpreter = bundle / ".venv/bin/python"
    interpreter.parent.mkdir(parents=True)
    interpreter.symlink_to(sys.executable)
    modules = root / "node_modules"
    package = modules / "@earendil-works/pi-coding-agent"
    package.mkdir(parents=True)
    executable = package / "cli.js"
    executable.write_text('#!/bin/sh\nexit 99\n')
    executable.chmod(0o755)
    link = modules / ".bin/pi"
    link.parent.mkdir()
    link.symlink_to("../@earendil-works/pi-coding-agent/cli.js")

    # Keep the trust algorithm intact. Only fixture ownership and the trust
    # boundary substitute for the root-owned system directories on a real host.
    source = SOURCE.read_text().replace(
        "if info.st_uid != 0:", "if info.st_uid not in {0, os.getuid()}:",
    ).replace(
        "if info.st_mode & 0o022:",
        f"if parent.is_relative_to(Path({str(root)!r})) and info.st_mode & 0o022:",
    )
    staged = root / "pi-install.sh"
    staged.write_text(source)

    def check(candidate=link, *, version=False):
        command = 'elephant_pi_root_path "$2" "$3"'
        if version:
            command = f'base={shlex.quote(str(bundle))}; elephant_pi_version "$2" 1.5.0 system'
        return subprocess.run(
            ["/bin/sh", "-c", '. "$1"; ' + command, "review", str(staged), str(candidate), sys.executable],
            capture_output=True, text=True, check=False,
        )

    return root, modules, executable, check


def test_standard_npm_symlink_and_nested_dependencies_are_trusted(system_pi):
    _, modules, _, check = system_pi
    dependency = modules / "dependency/node_modules/nested"
    dependency.mkdir(parents=True)
    (dependency / "index.js").write_text("export default 1;\n")
    result = check()
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("nested_entry", [False, True])
def test_writable_hoisted_dependency_is_rejected_before_candidate_execution(system_pi, nested_entry):
    _, modules, executable, check = system_pi
    dependency = modules / "hoisted"
    dependency.mkdir()
    (dependency / "index.js").write_text("export default 1;\n")
    dependency.chmod(0o777)
    if nested_entry:
        nested = executable.parent / "node_modules/launcher/cli.js"
        nested.parent.mkdir(parents=True)
        nested.write_text('#!/bin/sh\nexit 99\n')
        nested.chmod(0o755)
        executable = nested
    result = check(executable, version=True)
    assert result.returncode == 12
    assert "writable by another account" in result.stderr


def test_writable_sibling_module_is_rejected(system_pi):
    _, _, executable, check = system_pi
    dependency = executable.parent / "dependency.js"
    dependency.write_text("export default 1;\n")
    dependency.chmod(0o666)
    result = check()
    assert result.returncode != 0
    assert "writable by another account" in result.stderr


def test_intermediate_link_ancestor_is_checked(system_pi):
    root, _, executable, check = system_pi
    writable = root / "writable"
    writable.mkdir()
    writable.chmod(0o777)
    (writable / "hop").symlink_to(executable.parent, target_is_directory=True)
    alias = root / "alias"
    alias.symlink_to(writable / "hop", target_is_directory=True)
    result = check(alias / executable.name)
    assert result.returncode != 0
    assert "writable by another account" in result.stderr


def test_dependency_link_cannot_escape_with_dot_dot(system_pi):
    root, modules, _, check = system_pi
    outside = root / "outside.js"
    outside.write_text("export default 1;\n")
    (modules / ".bin/escape").symlink_to("../../outside.js")
    result = check()
    assert result.returncode != 0
    assert "leaves its npm tree" in result.stderr


def test_unknown_wrapper_is_skipped_without_execution(system_pi):
    root, _, _, check = system_pi
    wrapper = root / "wrapper"
    wrapper.write_text('#!/bin/sh\nexit 99\n')
    wrapper.chmod(0o755)
    result = check(wrapper, version=True)
    assert result.returncode == 12
