import json

import pytest

from elephant.cli import executable_paths
from elephant.config import Settings
from elephant.pi_installation import DEFAULT_PI, MARKER, runtime_pi_path


def test_installer_selection_replaces_only_the_standard_bundle_paths(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    marker = tmp_path / MARKER
    assert runtime_pi_path(DEFAULT_PI) == DEFAULT_PI
    selected = tmp_path / "separate path" / "pi"
    selected.parent.mkdir()
    selected.write_text("#!/bin/sh\n")
    selected.chmod(0o755)
    marker.write_text(str(selected) + "\n")
    assert runtime_pi_path(DEFAULT_PI) == str(selected)
    assert runtime_pi_path(str(tmp_path / DEFAULT_PI)) == str(selected)
    assert runtime_pi_path("custom/pi") == "custom/pi"
    assert executable_paths(Settings())["pi"] == str(selected)
    assert json.loads(Settings().model_dump_json())["orchestration"]["pi_executable"] == DEFAULT_PI


def test_bundled_selection_tracks_the_bundle_not_the_old_install_path(tmp_path):
    (tmp_path / MARKER).write_text("bundled\n")
    assert runtime_pi_path(DEFAULT_PI, root=tmp_path) == str(tmp_path / DEFAULT_PI)


@pytest.mark.parametrize("raw", ["pi\n", "/path\n/second\n", "/no-final-newline", "/has\tcontrol\n"])
def test_malformed_installer_selection_fails_closed(tmp_path, raw):
    (tmp_path / MARKER).write_text(raw)
    with pytest.raises(ValueError, match="invalid"):
        runtime_pi_path(DEFAULT_PI, root=tmp_path)


def test_marker_is_not_followed_or_honored_after_permissions_widen(tmp_path):
    marker = tmp_path / MARKER
    real = tmp_path / "real"
    real.write_text("/usr/bin/pi\n")
    marker.symlink_to(real)
    with pytest.raises(OSError):
        runtime_pi_path(DEFAULT_PI, root=tmp_path)
    marker.unlink()
    real.rename(marker)
    marker.chmod(0o666)
    with pytest.raises(ValueError, match="untrusted"):
        runtime_pi_path(DEFAULT_PI, root=tmp_path)
