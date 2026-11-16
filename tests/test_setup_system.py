import hashlib
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from elephant import setup_system


def make_fixture_root_owned(monkeypatch, paths):
    stat = Path.stat

    def fixture_stat(path, *args, **kwargs):
        info = stat(path, *args, **kwargs)
        if path in paths:
            return SimpleNamespace(st_uid=0, st_mode=info.st_mode)
        return info

    monkeypatch.setattr(Path, "stat", fixture_stat)
    monkeypatch.setattr(os, "geteuid", lambda: 0)


def test_system_scan_runs_unprivileged_and_passes_no_secrets_on_argv(tmp_path, monkeypatch):
    root = tmp_path / "install"
    interpreter = root / ".venv/bin/python"
    interpreter.parent.mkdir(parents=True)
    interpreter.touch(mode=0o755)
    state = tmp_path / "state"
    state.mkdir()
    monkeypatch.setattr(setup_system, "ROOT", root)
    monkeypatch.setattr(setup_system, "STATE", state)
    make_fixture_root_owned(monkeypatch, {interpreter})
    calls = []
    monkeypatch.setattr(setup_system.subprocess, "run", lambda argv, **opts: (
        calls.append((argv, opts)) or SimpleNamespace(returncode=0)))
    settings = SimpleNamespace(database=str(state / "core.db"), admin_token_env="ELEPHANT_ADMIN_TOKEN")
    assert setup_system.scan_as_service_account(settings, {
        "ELEPHANT_ADMIN_TOKEN": "synthetic-admin", "ELEPHANT_CREDENTIAL_KEY": "synthetic-qq",
        "ELEPHANT_MODEL_KEY": "unrelated-model-key",
    }) == 0
    [(argv, options)] = calls
    assert argv[:6] == ["/usr/sbin/runuser", "-u", "elephant", "-g", "elephant-worker", "--"]
    assert all("synthetic" not in argument for argument in argv)
    assert argv[-3:] == ["-I", "-m", "elephant.setup_system"]
    assert "ELEPHANT_MODEL_KEY" not in options["env"]
    with pytest.raises(ValueError, match="installer-managed"):
        setup_system.scan_as_service_account(SimpleNamespace(
            database=str(tmp_path / "other.db"), admin_token_env="ELEPHANT_ADMIN_TOKEN"), {})
    assert len(calls) == 1


def test_start_rejects_modified_unit_and_accepts_matching_installed_stamp(tmp_path, monkeypatch):
    unit, stamp = tmp_path / "elephant.service", tmp_path / "stamp"
    unit.write_text("[Unit]\n")
    stamp.write_text("not-the-digest")
    monkeypatch.setattr(setup_system, "UNIT", unit)
    monkeypatch.setattr(setup_system, "STAMP", stamp)
    make_fixture_root_owned(monkeypatch, {unit, stamp})
    calls = []
    monkeypatch.setattr(setup_system.subprocess, "run", lambda argv, **_opts: (
        calls.append(argv) or SimpleNamespace(returncode=0)))
    with pytest.raises(ValueError, match="differs"):
        setup_system.start_installed_service()
    assert not calls
    stamp.write_text(hashlib.sha256(unit.read_bytes()).hexdigest())
    assert setup_system.start_installed_service() == 0
    assert calls == [["/usr/bin/systemctl", "enable", "--now", "elephant.service"]]
