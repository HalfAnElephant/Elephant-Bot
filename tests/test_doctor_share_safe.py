"""Offline share-safe doctor checks use fake settings and disposable storage."""
import base64
import json
import socket
from pathlib import Path

import pytest

from elephant import cli


def deny_network(monkeypatch):
    def rejected(*_args, **_kwargs):
        raise AssertionError("doctor must not access network services")

    monkeypatch.setattr(socket.socket, "connect", rejected)
    monkeypatch.setattr(socket.socket, "connect_ex", rejected)
    monkeypatch.setattr(socket, "getaddrinfo", rejected)


def run_doctor(config, monkeypatch, capsys, *options):
    monkeypatch.setattr("sys.argv", ["elephant", "doctor", "--config", str(config), *options])
    cli.main()
    captured = capsys.readouterr()
    return json.loads(captured.out), captured.out + captured.err


@pytest.mark.parametrize(("memory_owner", "private_enabled", "memory_status"), [
    ("private-owner", True, "configured"),
    ("private-unrelated-owner", True, "incomplete"),
    ("private-owner", False, "disabled"),
])
def test_share_safe_doctor_reports_effective_config_without_live_verification(
        tmp_path, monkeypatch, capsys, memory_owner, private_enabled, memory_status):
    config = tmp_path / "configured.yaml"
    configured_db = tmp_path / "private-database-path.db"
    configured_db.write_bytes(b"keep existing bytes")
    config.write_text(json.dumps({
        "database": str(configured_db),
        "model": {
            "model": "private-model-name",
            "endpoint": "https://private-model.invalid/v1",
            "api_key_env": "PRIVATE_MODEL_ENV",
        },
        "admin_token_env": "PRIVATE_ADMIN_ENV",
        "owner": {"id": "private-owner"},
        "adapters": [{"instance_id": "private-adapter", "kind": "official", "enabled": True,
                      "bot_id": "private-bot", "app_id": "private-app", "secret_env": "PRIVATE_QQ_ENV",
                      "allowed_users": ["private-user"]}],
        "memory": {"openviking": {"endpoint": "https://private-memory.invalid",
            "private_enabled": private_enabled,
            "bindings": [{"owner_id": memory_owner, "account_id": "private-account",
                "user_id": "private-memory-user", "api_key_env": "PRIVATE_MEMORY_ENV"}]}},
    }), encoding="utf-8")
    saved = tmp_path / "runtime-config.json"
    saved.write_text(json.dumps({
        "permissions": {"preset": "custom", "default_effect": "deny"},
        "execution": {"leader_environment": "sandbox", "worker_environment": "host"},
    }), encoding="utf-8")
    before = saved.read_bytes()
    for name in ("PRIVATE_ADMIN_ENV", "PRIVATE_MODEL_ENV", "PRIVATE_QQ_ENV", "PRIVATE_MEMORY_ENV"):
        monkeypatch.setenv(name, "private-token-" + name)
    credential = base64.b64encode(b"x" * 32).decode()
    monkeypatch.setenv("ELEPHANT_CREDENTIAL_KEY", credential)
    monkeypatch.setattr(cli.shutil, "which", lambda name: str(Path("/private-executable-directory") / name))
    deny_network(monkeypatch)

    report, rendered = run_doctor(config, monkeypatch, capsys, "--share-safe")
    assert report["effective"]["permissions"] == {"preset": "custom", "default_effect": "deny"}
    assert report["effective"]["execution"] == {"leader": "sandbox", "worker": "host"}
    assert report["effective"]["listener"] == {
        "host": "127.0.0.1", "port": 8080, "scope": "builtin_serve_only",
    }
    checks = report["checks"]
    assert checks["configuration"] == checks["sqlite"] == "passed"
    assert checks["dependencies"] == {"node": "available", "pi": "available", "ffmpeg": "available"}
    assert checks["model"] == {"configuration": "configured", "connectivity": "not_tested"}
    assert checks["qq"] == {"configuration": "configured", "connectivity": "not_tested",
                            "scan_onboarding": "credential_available"}
    assert checks["openviking"] == {"configuration": memory_status, "connectivity": "not_tested"}
    assert checks["worker_isolation"] == {"verification": "not_tested"}
    for sentinel in (
        str(tmp_path), "private-", "PRIVATE_", credential,
    ):
        assert sentinel not in rendered
    assert configured_db.read_bytes() == b"keep existing bytes"
    assert saved.read_bytes() == before


def test_share_safe_doctor_missing_dependencies_do_not_change_sqlite_result(tmp_path, monkeypatch, capsys):
    config = tmp_path / "minimal.yaml"
    configured_db = tmp_path / "not-created.db"
    config.write_text(json.dumps({"database": str(configured_db)}), encoding="utf-8")
    monkeypatch.setattr(cli.shutil, "which", lambda _name: None)
    for name in ("ELEPHANT_ADMIN_TOKEN", "ELEPHANT_MODEL_KEY", "ELEPHANT_CREDENTIAL_KEY"):
        monkeypatch.delenv(name, raising=False)
    deny_network(monkeypatch)
    report, _ = run_doctor(config, monkeypatch, capsys, "--share-safe")
    checks = report["checks"]
    assert checks["configuration"] == checks["sqlite"] == "passed"
    assert checks["dependencies"] == {"node": "missing", "pi": "missing", "ffmpeg": "missing"}
    assert checks["model"] == checks["openviking"] == {"configuration": "not_configured", "connectivity": "not_tested"}
    assert checks["qq"] == {"configuration": "not_configured", "connectivity": "not_tested",
                            "scan_onboarding": "credential_missing_or_invalid"}
    assert not configured_db.exists()



def test_share_safe_invalid_config_does_not_print_validator_values(tmp_path, monkeypatch, capsys):
    config = tmp_path / "invalid.json"
    config.write_text('{"model":{"endpoint":["PRIVATE_BAD_URL"]}}', encoding="utf-8")
    monkeypatch.setattr("sys.argv", ["elephant", "doctor", "--config", str(config), "--share-safe"])
    deny_network(monkeypatch)
    assert cli.main() == 2
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {"checks": {"configuration": "failed", "sqlite": "not_tested"}}
    assert not captured.err
    assert "PRIVATE_BAD_URL" not in captured.out


def test_share_safe_probe_failure_reports_failed_without_path_or_exception_text(tmp_path, monkeypatch, capsys):
    config = tmp_path / "configured.json"
    configured_db = tmp_path / "private-db-path"
    config.write_text(json.dumps({"database": str(configured_db)}), encoding="utf-8")
    monkeypatch.setattr("sys.argv", ["elephant", "doctor", "--config", str(config), "--share-safe"])
    called = []

    def migrate_failure(path):
        called.append(Path(path))
        raise OSError("PRIVATE_PROBE_DETAIL " + str(path))

    monkeypatch.setattr(cli, "migrate", migrate_failure)
    deny_network(monkeypatch)
    assert cli.main() == 1
    assert len(called) == 1 and called[0] != configured_db
    captured = capsys.readouterr()
    checks = json.loads(captured.out)["checks"]
    assert checks["configuration"] == "passed"
    assert checks["sqlite"] == "failed"
    assert "PRIVATE_PROBE_DETAIL" not in captured.out + captured.err
    assert not configured_db.exists()
