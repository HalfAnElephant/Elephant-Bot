import json

from fastapi.testclient import TestClient

from qqbot.app import create_app
from qqbot.config import Settings


def test_admin_config_auth_and_http_callback(tmp_path, official_config, official_event, clock, monkeypatch):
    monkeypatch.setenv("QQBOT_ADMIN_TOKEN", "admin-fixture")
    app = create_app(
        Settings(database=str(tmp_path / "http.db"), adapters=(official_config,)),
        clock=clock,
        run_background=False,
    )
    with TestClient(app) as client:
        assert client.get("/api/v1/config").status_code == 401
        assert client.post("/internal/tasks/claim", json={}).status_code == 404
        qq = app.state.adapters["qq"]
        body = json.dumps(official_event).encode()
        stamp = str(int(clock.now().timestamp()))
        signed = {
            "x-signature-timestamp": stamp,
            "x-signature-ed25519": qq.key().sign(stamp.encode() + body).hex(),
        }
        assert client.post("/webhooks/qq/qq", content=body, headers=signed).json() == {"op": 12}
        assert client.post("/webhooks/qq/qq", content=body + b" ", headers=signed).status_code == 401
        auth = {"Authorization": "Bearer admin-fixture"}
        config = client.get("/api/v1/config", headers=auth).json()
        config["config"]["send_budget_per_minute"] = 4
        result = client.post(
            "/api/v1/config/apply",
            headers=auth,
            json={"expected_version": config["version"], "config": config["config"]},
        )
        assert result.status_code == 200
        assert (
            client.post(
                "/api/v1/config/apply",
                headers=auth,
                json={"expected_version": config["version"], "config": config["config"]},
            ).status_code
            == 409
        )
        assert (tmp_path / "runtime-config.json").exists()
