"""The documented configuration reaches its first authenticated task."""
import base64
import json
from pathlib import Path

from fastapi.testclient import TestClient

from elephant import cli
from elephant.app import create_app
from elephant.config import Settings, load_settings


def test_example_accepts_first_admin_task(tmp_path, monkeypatch):
    monkeypatch.setenv("ELEPHANT_ADMIN_TOKEN", "first-run-test-token")
    settings = load_settings("configs/example.yaml").model_copy(update={
        "database": str(tmp_path / "core.db"), "internal_socket": str(tmp_path / "internal.sock"),
    })
    assert settings.tasks_enabled
    with TestClient(create_app(settings, run_background=False)) as client:
        headers = {"Authorization": "Bearer first-run-test-token", "Idempotency-Key": "first-hello-1"}
        response = client.post("/api/v1/tasks", json={"goal": "Say hello without tools"}, headers=headers)
        assert response.status_code == 200, response.text
        result = response.json()
        assert result["public_id"]
        repeated = client.post("/api/v1/tasks", json={"goal": "Say hello without tools"}, headers=headers)
        assert repeated.json() == result
        assert client.get("/api/v1/tasks/" + result["public_id"], headers=headers).status_code == 200


def test_validate_warns_about_incomplete_memory_without_exposing_secrets(tmp_path, monkeypatch, capsys):
    config = tmp_path / "local.yaml"
    config.write_text(f"database: {tmp_path / 'core.db'}\nmemory:\n  openviking:\n    endpoint: http://127.0.0.1:1933\n")
    monkeypatch.setenv("ELEPHANT_ADMIN_TOKEN", "private-admin-token")
    monkeypatch.setenv("ELEPHANT_MODEL_KEY", "private-model-token")
    monkeypatch.delenv("ELEPHANT_CREDENTIAL_KEY", raising=False)
    monkeypatch.setattr("sys.argv", ["elephant", "validate", "--config", str(config)])
    cli.main()
    output = capsys.readouterr()
    assert output.out.strip() == "configuration valid"
    assert "model.endpoint" in output.err
    assert "ELEPHANT_CREDENTIAL_KEY" in output.err
    assert "no bindings" in output.err
    assert "private-admin-token" not in output.err
    assert "private-model-token" not in output.err


def test_complete_setup_has_no_configuration_warnings(monkeypatch):
    for name in ("ELEPHANT_ADMIN_TOKEN", "ELEPHANT_MODEL_KEY", "ELEPHANT_OPENVIKING_KEY"):
        monkeypatch.setenv(name, "private-test-value")
    monkeypatch.setenv("ELEPHANT_CREDENTIAL_KEY", base64.b64encode(b"k" * 32).decode())
    settings = Settings.model_validate({
        "model": {"endpoint": "https://model.invalid/v1/chat/completions", "model": "test-model"},
        "memory": {"openviking": {"endpoint": "http://127.0.0.1:1933", "bindings": [{
            "owner_id": "personal-owner", "account_id": "elephant", "user_id": "assistant",
            "api_key_env": "ELEPHANT_OPENVIKING_KEY",
        }]}},
    }, strict=False)
    assert cli.configuration_warnings(settings) == []
    monkeypatch.delenv("ELEPHANT_OPENVIKING_KEY")
    assert cli.configuration_warnings(settings) == [
        "OpenViking USER credential is empty. Check memory.openviking.bindings[].api_key_env.",
    ]
    monkeypatch.setenv("ELEPHANT_OPENVIKING_KEY", "private-test-value")
    other_owner = settings.model_copy(update={"owner": settings.owner.model_copy(update={"id": "other-owner"})})
    assert cli.configuration_warnings(other_owner) == [
        "OpenViking has no binding for owner.id; this owner's memory is disabled.",
    ]


def test_doctor_reports_missing_executables_and_environment(tmp_path, monkeypatch, capsys):
    config = tmp_path / "local.yaml"
    config.write_text(f"database: {tmp_path / 'core.db'}\n")
    monkeypatch.setenv("PATH", str(tmp_path))
    for name in ("ELEPHANT_ADMIN_TOKEN", "ELEPHANT_MODEL_KEY", "ELEPHANT_CREDENTIAL_KEY"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr("sys.argv", ["elephant", "doctor", "--config", str(config)])
    cli.main()
    output = json.loads(capsys.readouterr().out)
    assert output["offline"] == "passed"
    assert output["dependencies"]["node"] is None
    assert output["dependencies"]["ffmpeg"] is None
    assert output["qq_live"] == output["pi_model_run"] == "not_tested"
    assert any("admin_token_env" in message for message in output["warnings"])
    assert any("model.api_key_env" in message for message in output["warnings"])
    assert any("Executable node" in message for message in output["warnings"])
    assert not (tmp_path / "core.db").exists()


def test_environment_template_includes_onboarding_key():
    values = dict(line.split("=", 1) for line in Path(".env.example").read_text().splitlines()
                  if line and not line.startswith("#"))
    assert {"ELEPHANT_ADMIN_TOKEN", "ELEPHANT_MODEL_KEY", "ELEPHANT_CREDENTIAL_KEY"} <= values.keys()
    assert all(not value for value in values.values())


def test_validate_reports_only_explicit_deprecated_quota_fields(tmp_path, monkeypatch, capsys):
    config = tmp_path / "legacy.yaml"
    config.write_text("execution:\n  max_active: 8\norchestration:\n  max_task_depth: 2\n")
    monkeypatch.setattr("sys.argv", ["elephant", "validate", "--config", str(config)])
    cli.main()
    output = capsys.readouterr()
    assert output.out.strip() == "configuration valid"
    assert "execution.max_active is deprecated" in output.err
    assert "orchestration.max_task_depth is deprecated" in output.err
    assert "sustained_limit is deprecated" not in output.err
    assert cli.deprecated_quota_warnings(load_settings("configs/example.yaml")) == []
