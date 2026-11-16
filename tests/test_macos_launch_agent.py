"""Exercise LaunchAgent security boundaries without invoking real launchctl."""

import json
import os
import plistlib
import stat
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from elephant import setup_macos


def test_plain_launch_environment_uses_profile_credentials(monkeypatch, tmp_path):
    uid = os.getuid()
    home = tmp_path / "Profile owner"
    settings = home / "Settings"
    settings.mkdir(parents=True, mode=0o700)
    profile = settings / "credentials.env"
    profile.write_text(
        "ELEPHANT_MODEL_KEY=profile-primary\n"
        "QQBOT_MODEL_KEY=profile-legacy-unused\n"
        "QQBOT_ADMIN_TOKEN=profile-legacy-admin\n"
        "ELEPHANT_CREDENTIAL_KEY=\n"
        "QQBOT_CREDENTIAL_KEY=profile-legacy-unused\n"
    )
    config = settings / "config.yaml"
    config.write_text("test fixture")
    module_root = Path(setup_macos.__file__).parents[1]
    command = home / "mock-elephant"
    command.write_text(
        "#!/bin/sh\n"
        + f'''exec "{sys.executable}" -I - "$@" <<'PYTHON_CODE'\n'''
        + f"import sys; sys.path.insert(0, {str(module_root)!r})\n"
        + """
import json, os
from dotenv import load_dotenv
from elephant.credentials_env import credential_env
assert sys.argv[1:4] == ["serve", "--config", sys.argv[3]]
assert sys.argv[4] == "--env-file"
assert "ELEPHANT_MODEL_KEY" not in os.environ
assert "QQBOT_ADMIN_TOKEN" not in os.environ and "QQBOT_CREDENTIAL_KEY" not in os.environ
assert "PYTHON_DOTENV_DISABLED" not in os.environ
assert "UNRELATED_GUI_TOKEN" not in os.environ
load_dotenv(sys.argv[5], override=False, interpolate=False)
print(json.dumps([credential_env("ELEPHANT_MODEL_KEY"), credential_env("ELEPHANT_ADMIN_TOKEN"),
                  credential_env("ELEPHANT_CREDENTIAL_KEY"), os.environ["HOME"]]))
PYTHON_CODE
"""
    )
    command.chmod(0o700)
    monkeypatch.setattr(setup_macos, "_node_path", lambda uid, root: Path("/trusted/native/bin/node"))
    monkeypatch.setattr(setup_macos, "_trusted_path", lambda path, uid, **kwargs: path)
    data = setup_macos._plist(settings, home, command, home, uid)
    args = plistlib.loads(data)["ProgramArguments"]
    assert args[:3] == ["/usr/bin/env", "-i", f"HOME={home}"]
    assert not any("profile-primary" in arg for arg in args)
    env = {
        **os.environ,
        "ELEPHANT_MODEL_KEY": "stale-gui-key",
        "QQBOT_ADMIN_TOKEN": "stale-gui-admin",
        "QQBOT_CREDENTIAL_KEY": "stale-gui-credential",
        "UNRELATED_GUI_TOKEN": "never-visible",
        "PYTHON_DOTENV_DISABLED": "1",
    }
    run = subprocess.run(args, env=env, text=True, capture_output=True, check=True)
    assert json.loads(run.stdout) == ["profile-primary", "profile-legacy-admin", "", str(home)]


@pytest.mark.parametrize(
    ("suffix", "secret", "safe"),
    [
        (b"", True, True),
        (b" 0: group:everyone deny delete\n", True, True),
        (b" 0: group:staff allow read,readattr\n", False, True),
        (b" 0: group:staff allow read,readattr\n", True, False),
        (b" 0: group:staff allow read,delete_child\n", False, False),
        (b"could not parse security data\n", False, False),
    ],
)
def test_darwin_acl_only_allows_non_mutating_entries(monkeypatch, suffix, secret, safe):
    def run(args, **kwargs):
        assert args == ["/bin/ls", "-lde", "/trusted path"]
        assert kwargs["env"] == {"LC_ALL": "C"}
        return SimpleNamespace(
            returncode=0, stdout=b"-rw-------+ 1 test staff 3 date /trusted path\n" + suffix
        )

    monkeypatch.setattr(setup_macos.subprocess, "run", run)
    assert setup_macos._safe_acl(Path("/trusted path"), secret=secret) is safe


def test_trusted_path_rejects_writable_ancestors_including_symlink_targets(monkeypatch, tmp_path):
    uid = os.getuid()
    outside = tmp_path / "owner-private"
    outside.mkdir(mode=0o700)
    work = tmp_path / "shared"
    work.mkdir(mode=0o700)
    leaf = work / "node"
    leaf.touch(mode=0o700)
    selected = outside / "node"
    selected.symlink_to(leaf)

    # Model trusted system-owned ancestors above the test workspace (/tmp may
    # be world-writable in CI). Real modes are kept for the workspace entries.
    real_stat = Path.lstat
    ancestors = set(tmp_path.parents)

    def lstat(path):
        info = real_stat(path)
        if path in ancestors:
            return SimpleNamespace(st_uid=0, st_mode=stat.S_IFDIR | 0o755)
        return info

    monkeypatch.setattr(Path, "lstat", lstat)
    monkeypatch.setattr(setup_macos, "_safe_acl", lambda path: True)
    assert setup_macos._trusted_path(selected, uid) == leaf
    work.chmod(0o770)
    with pytest.raises(setup_macos.LaunchAgentError, match="其他用户修改"):
        setup_macos._trusted_path(leaf, uid)
    with pytest.raises(setup_macos.LaunchAgentError, match="其他用户修改"):
        setup_macos._trusted_path(selected, uid)


def test_login_pi_uses_selected_external_without_bundled_runtime(monkeypatch, tmp_path):
    root = tmp_path / "release"
    root.mkdir(mode=0o700)
    external = tmp_path / "system-pi"
    external.touch(mode=0o700)
    marker = root / ".elephant-pi"
    uid = os.getuid()
    inspected = []

    def trusted(path, user, **kwargs):
        assert user == uid
        inspected.append(path)
        if not path.is_file():
            raise setup_macos.LaunchAgentError("not a trusted file")
        return path

    monkeypatch.setattr(setup_macos, "_trusted_path", trusted)
    with pytest.raises(setup_macos.LaunchAgentError):
        setup_macos._trusted_pi(root, uid, "node_modules/.bin/pi")

    inspected.clear()
    marker.write_text(f"{external}\n")
    marker.chmod(0o600)
    assert setup_macos._trusted_pi(root, uid, "node_modules/.bin/pi") == external
    assert inspected == [marker, external]
    assert not (root / "node_modules").exists()

    marker.chmod(0o666)
    with pytest.raises(setup_macos.LaunchAgentError, match="安装记录不安全"):
        setup_macos._trusted_pi(root, uid, "node_modules/.bin/pi")


def test_login_pi_disallows_path_only_command_with_different_login_environment(monkeypatch, tmp_path):
    monkeypatch.setattr(
        setup_macos, "_trusted_path", lambda *args, **kwargs: pytest.fail("must not resolve from wizard PATH")
    )
    with pytest.raises(setup_macos.LaunchAgentError, match="明确的 Pi 安装路径"):
        setup_macos._trusted_pi(tmp_path, os.getuid(), "pi")
