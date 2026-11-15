"""Check the installed wheel while running from the extracted source bundle."""

import os
import tempfile
from importlib.resources import files
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from qqbot.app import create_app
from qqbot.config import load_settings


def main():
    package = files("qqbot")
    assert "site-packages" in str(package), "Smoke test must use the installed wheel"
    for name in ("harness/pi_bridge.ts",):
        assert package.joinpath(name).is_file(), f"Wheel is missing {name}"
    for name in ("alembic.ini", "uv.lock", "package-lock.json", "deploy/qqbot.service",
                 "configs/production.yaml", ".env.example"):
        assert Path(name).is_file(), f"Source bundle is missing {name}"
    assert os.access("scripts/pi-runner-launch", os.X_OK)

    settings = load_settings("configs/example.yaml")
    assert settings.tasks_enabled, "The first-run example must accept admin tasks"
    with tempfile.TemporaryDirectory() as folder:
        settings = settings.model_copy(update={
            "database": str(Path(folder) / "core.db"),
            "internal_socket": str(Path(folder) / "internal.sock"),
            "interactive_execution_enabled": False,
        })
        with patch.dict(os.environ, {settings.admin_token_env: "release-smoke-only"}), \
                TestClient(create_app(settings, run_background=False)) as client:
            for path in ("/health/live", "/health/ready", "/admin",
                         "/admin/assets/console.js", "/admin/assets/console.css"):
                response = client.get(path)
                assert response.status_code == 200, f"{path} returned {response.status_code}"
                assert response.content, f"{path} returned an empty response"
            response = client.post("/api/v1/tasks", json={"goal": "Say hello without tools"}, headers={
                "Authorization": "Bearer release-smoke-only", "Idempotency-Key": "release-first-task",
            })
            assert response.status_code == 200, response.text
            assert response.json()["public_id"]
    print("Installed release smoke test passed")


if __name__ == "__main__":
    main()
