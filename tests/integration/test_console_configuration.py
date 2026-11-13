import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

import pytest
from fastapi.testclient import TestClient

from qqbot.app import create_app
from qqbot.config import Settings


@pytest.fixture
def console_settings(tmp_path, monkeypatch):
    monkeypatch.setenv("QQBOT_ADMIN_TOKEN", "console-admin-secret")
    monkeypatch.setenv("QQBOT_MODEL_KEY", "console-model-secret")
    return Settings(database=str(tmp_path / "core.db"))


AUTH = {"Authorization": "Bearer console-admin-secret"}


def test_console_assets_are_packaged_and_restricted(console_settings, clock):
    with TestClient(create_app(console_settings, clock=clock, run_background=False)) as client:
        page = client.get("/admin")
        assert page.status_code == 200 and 'id="login-screen"' in page.text
        assert "'unsafe-inline'" not in page.headers["content-security-policy"]
        assert "frame-ancestors 'none'" in page.headers["content-security-policy"]
        for name, media in (("console.js", "text/javascript"), ("console.css", "text/css")):
            response = client.get("/admin/assets/" + name)
            assert response.status_code == 200 and response.headers["content-type"].startswith(media)
            assert response.headers["cache-control"] == "no-store"
            assert response.headers["x-content-type-options"] == "nosniff"
            assert len(response.content) > 100
        assert client.get("/admin/assets/api.py").status_code == 404
        assert client.get("/admin/assets/missing.js").status_code == 404


def test_legacy_hot_update_does_not_mask_startup_edits(console_settings, clock):
    with TestClient(create_app(console_settings, clock=clock, run_background=False)) as client:
        first = client.get("/api/v1/config", headers=AUTH).json()
        response = client.post("/api/v1/config/apply", headers=AUTH, json={
            "expected_version": first["version"],
            "config": {**first["config"], "send_budget_per_minute": 3},
        })
        assert response.status_code == 200
    startup = console_settings.model_copy(update={"timezone": "UTC"})
    with TestClient(create_app(startup, clock=clock, run_background=False)) as client:
        config = client.get("/api/v1/config", headers=AUTH).json()["config"]
        assert config["timezone"] == "UTC" and config["send_budget_per_minute"] == 3


def test_owner_preferences_are_bounded_to_4000_characters():
    assert len(Settings(owner={"preferences": "x" * 4000}).owner.preferences) == 4000
    with pytest.raises(ValueError):
        Settings(owner={"preferences": "x" * 4001})


def test_console_config_schema_auth_and_secret_status(console_settings, clock):
    with TestClient(create_app(console_settings, clock=clock, run_background=False)) as client:
        for method, path in (
            ("GET", "/api/v1/config"),
            ("GET", "/api/v1/config/schema"),
            ("POST", "/api/v1/config/save"),
            ("POST", "/api/v1/config/validate"),
            ("POST", "/api/v1/config/apply"),
            ("GET", "/api/v1/config/versions"),
            ("GET", "/api/v1/permissions/schema"),
            ("PUT", "/api/v1/permissions"),
        ):
            assert client.request(method, path, json={}).status_code == 401
        response = client.get("/api/v1/config", headers=AUTH)
        data = response.json()
        assert data["config"] == data["saved_config"] == console_settings.model_dump(mode="json")
        assert data["version"] == data["saved_version"]
        assert data["restart_required"] is False and data["changed_fields"] == []
        assert data["config"]["execution"]["notify_before_tools"] is True
        assert "send_budget_per_minute" in data["hot_fields"]
        credentials = {item["name"]: item["configured"] for item in data["credentials"]}
        assert credentials["QQBOT_ADMIN_TOKEN"] and credentials["QQBOT_MODEL_KEY"]
        assert "console-admin-secret" not in response.text
        assert "console-model-secret" not in response.text
        schema = client.get("/api/v1/config/schema", headers=AUTH).json()
        assert set(schema["properties"]) == set(Settings.model_fields)
        assert schema["$defs"]["ExecutionConfig"]["properties"]["notify_before_tools"]["default"] is True
        assert schema["$defs"]["OwnerConfig"]["properties"]["preferences"]["maxLength"] == 4000
        assert {"persona_path", "judge", "expression", "speak_up", "social_evaluated_conversations"}.isdisjoint(schema["properties"])
        for path in ("/api/v1/persona", "/api/v1/memories?conversation_id=old", "/api/v1/expressions?conversation_id=old"):
            assert client.get(path, headers=AUTH).status_code == 404
        assert client.put("/api/v1/persona", headers=AUTH, json={}).status_code == 404


def test_save_all_settings_restart_and_restore_history(console_settings, tmp_path, clock):
    app = create_app(console_settings, clock=clock, run_background=False)
    with TestClient(app) as client:
        first = client.get("/api/v1/config", headers=AUTH).json()
        candidate = {
            **first["config"],
            "timezone": "UTC",
            "owner": {**first["config"]["owner"], "preferences": "请用中文简洁回答。"},
            "tasks_enabled": True,
            "execution": {**first["config"]["execution"], "max_active": 5, "notify_before_tools": False},
            "context": {**first["config"]["context"], "recent_messages": 20},
            "memory": {**first["config"]["memory"], "openviking": {
                **first["config"]["memory"]["openviking"], "max_query_chars": 5000}},
            "model": {**first["config"]["model"], "model": "console-model"},
        }
        body = {"expected_version": first["saved_version"], "config": candidate}
        validated = client.post("/api/v1/config/validate", headers=AUTH, json=body)
        assert validated.status_code == 200 and validated.json()["restart_required"] is True
        response = client.post("/api/v1/config/save", headers=AUTH, json=body)
        assert response.status_code == 200
        assert response.json()["saved"] is True and response.json()["applied"] is False
        pending = client.get("/api/v1/config", headers=AUTH).json()
        assert pending["config"] == first["config"] and pending["saved_config"] == candidate
        assert pending["restart_required"] is True
        assert set(pending["changed_fields"]) == {"timezone", "owner", "tasks_enabled", "execution", "context", "memory", "model"}
        assert client.post("/api/v1/config/save", headers=AUTH, json=body).status_code == 409
        assert client.post("/api/v1/config/apply", headers=AUTH, json={
            "expected_version": first["version"],
            "config": {**first["config"], "send_budget_per_minute": 3},
        }).status_code == 409
        versions = client.get("/api/v1/config/versions", headers=AUTH).json()
        assert first["version"] in versions and pending["saved_version"] in versions
        history = client.get("/api/v1/config/versions?include_metadata=true", headers=AUTH).json()
        assert {item["version"] for item in history} == set(versions)
        assert all(item["saved_at"].endswith("+00:00") for item in history)
        saved = tmp_path / "runtime-config.json"
        assert saved.stat().st_mode & 0o777 == 0o600
        assert json.loads(saved.read_text()) == candidate

    with TestClient(create_app(console_settings, clock=clock, run_background=False)) as client:
        restarted = client.get("/api/v1/config", headers=AUTH).json()
        assert restarted["config"] == candidate == restarted["saved_config"]
        assert restarted["restart_required"] is False
        original = client.get("/api/v1/config/versions/" + first["version"], headers=AUTH).json()
        response = client.post("/api/v1/config/save", headers=AUTH, json={
            "expected_version": restarted["saved_version"], "config": original,
        })
        assert response.status_code == 200
    with TestClient(create_app(console_settings, clock=clock, run_background=False)) as client:
        assert client.get("/api/v1/config", headers=AUTH).json()["config"] == first["config"]


def test_invalid_config_and_concurrent_save_do_not_overwrite(console_settings, tmp_path, clock):
    with TestClient(create_app(console_settings, clock=clock, run_background=False)) as client:
        first = client.get("/api/v1/config", headers=AUTH).json()
        invalid = {**first["config"], "execution": {**first["config"]["execution"], "max_active": -1}}
        for path in ("validate", "save", "apply"):
            response = client.post("/api/v1/config/" + path, headers=AUTH, json={
                "expected_version": first["version"], "config": invalid,
            })
            assert response.status_code == 422
        assert not (tmp_path / "runtime-config.json").exists()
        assert client.get("/api/v1/config", headers=AUTH).json() == first
        barrier = Barrier(2)

        def save(budget):
            barrier.wait()
            return budget, client.post("/api/v1/config/save", headers=AUTH, json={
                "expected_version": first["saved_version"],
                "config": {**first["config"], "send_budget_per_minute": budget},
            })

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(save, (3, 4)))
        assert sorted(response.status_code for _, response in results) == [200, 409]
        winner = next(budget for budget, response in results if response.status_code == 200)
        data = client.get("/api/v1/config", headers=AUTH).json()
        assert data["saved_config"]["send_budget_per_minute"] == winner
        assert data["config"] == first["config"]


def test_database_change_keeps_original_config_anchor(console_settings, tmp_path, clock):
    destination = tmp_path / "new-data" / "core.db"
    with TestClient(create_app(console_settings, clock=clock, run_background=False)) as client:
        first = client.get("/api/v1/config", headers=AUTH).json()
        response = client.post("/api/v1/config/save", headers=AUTH, json={
            "expected_version": first["saved_version"],
            "config": {**first["config"], "database": str(destination)},
        })
        assert response.status_code == 200 and not destination.exists()
    app = create_app(console_settings, clock=clock, run_background=False)
    with TestClient(app) as client:
        assert app.state.core.db.path == str(destination)
        data = client.get("/api/v1/config", headers=AUTH).json()
        response = client.post("/api/v1/config/apply", headers=AUTH, json={
            "expected_version": data["version"],
            "config": {**data["config"], "send_budget_per_minute": 2},
        })
        assert response.status_code == 200
        assert not (destination.parent / "runtime-config.json").exists()
    with TestClient(create_app(console_settings, clock=clock, run_background=False)) as client:
        data = client.get("/api/v1/config", headers=AUTH).json()
        assert data["config"]["database"] == str(destination)
        assert data["config"]["send_budget_per_minute"] == 2


def test_failed_config_replace_preserves_saved_and_active_configuration(console_settings, tmp_path, clock, monkeypatch):
    app = create_app(console_settings, clock=clock, run_background=False)
    with TestClient(app, raise_server_exceptions=False) as client:
        first = client.get("/api/v1/config", headers=AUTH).json()
        assert client.post("/api/v1/config/apply", headers=AUTH, json={
            "expected_version": first["version"],
            "config": {**first["config"], "send_budget_per_minute": 4},
        }).status_code == 200
        before = client.get("/api/v1/config", headers=AUTH).json()
        original_file = (tmp_path / "runtime-config.json").read_bytes()
        replace = Path.replace

        def fail_runtime_replace(path, destination):
            if Path(destination).name == "runtime-config.json":
                raise OSError("fixture rename failure")
            return replace(path, destination)

        monkeypatch.setattr(Path, "replace", fail_runtime_replace)
        for endpoint in ("save", "apply"):
            response = client.post("/api/v1/config/" + endpoint, headers=AUTH, json={
                "expected_version": before["version"],
                "config": {**before["config"], "send_budget_per_minute": 2},
            })
            assert response.status_code == 500
            assert client.get("/api/v1/config", headers=AUTH).json() == before
            assert (tmp_path / "runtime-config.json").read_bytes() == original_file


def test_model_metadata_hot_apply_when_execution_disabled(console_settings, clock):
    settings = console_settings.model_copy(update={"interactive_execution_enabled": False})
    with TestClient(create_app(settings, clock=clock, run_background=False)) as client:
        first = client.get("/api/v1/config", headers=AUTH).json()
        candidate = {**first["config"], "model": {**first["config"]["model"], "model": "offline-model"}}
        body = {"expected_version": first["version"], "config": candidate}
        validation = client.post("/api/v1/config/validate", headers=AUTH, json=body).json()
        assert validation["restart_required"] is False and "model" in validation["hot_fields"]
        assert client.post("/api/v1/config/apply", headers=AUTH, json=body).status_code == 200
        assert client.get("/api/v1/config", headers=AUTH).json()["config"] == candidate


def test_permission_policy_edit_persists_and_rejects_lockout(console_settings, clock):
    with TestClient(create_app(console_settings, clock=clock, run_background=False)) as client:
        schema = client.get("/api/v1/permissions/schema", headers=AUTH)
        assert schema.status_code == 200 and "rules" in schema.json()["properties"]
        first = client.get("/api/v1/permissions", headers=AUTH).json()
        policy = {
            "preset": "custom", "default_effect": "allow",
            "rules": [{"id": "deny-member-task", "effect": "deny", "actions": ["task.*"],
                       "subject": {"kind": "person", "adapter_instance_id": "qq",
                                   "identity_namespace": "qq", "user_id": "42"}}],
        }
        body = {"expected_version": first["policy_version"], "policy": policy}
        response = client.put("/api/v1/permissions", headers=AUTH, json=body)
        assert response.status_code == 200
        saved = response.json()
        assert saved["policy_version"] == first["policy_version"] + 1
        assert saved["rules"][0]["id"] == "deny-member-task"
        assert client.put("/api/v1/permissions", headers=AUTH, json=body).status_code == 409
        for invalid_policy in (
            {"preset": "custom", "default_effect": "deny", "rules": []},
            {**policy, "rules": [{"id": "block-login", "effect": "deny", "actions": ["admin.get"],
                                   "resources": ["http:/api/v1/config"]}]},
            {**policy, "rules": [{"id": "broken", "effect": "deny", "actions": ["invalid*action"]}]},
        ):
            assert client.put("/api/v1/permissions", headers=AUTH, json={
                "expected_version": saved["policy_version"], "policy": invalid_policy,
            }).status_code == 422
        assert client.get("/api/v1/permissions", headers=AUTH).json() == saved
    with TestClient(create_app(console_settings, clock=clock, run_background=False)) as client:
        assert client.get("/api/v1/permissions", headers=AUTH).json() == saved


def test_initial_policy_save_cannot_lock_out_console(console_settings, tmp_path, clock):
    with TestClient(create_app(console_settings, clock=clock, run_background=False)) as client:
        first = client.get("/api/v1/config", headers=AUTH).json()
        response = client.post("/api/v1/config/save", headers=AUTH, json={
            "expected_version": first["saved_version"],
            "config": {**first["config"], "permissions": {"default_effect": "deny", "rules": []}},
        })
        assert response.status_code == 422
        assert client.get("/api/v1/config", headers=AUTH).json() == first
        assert not (tmp_path / "runtime-config.json").exists()
