"""Check the installed wheel while running from the extracted source bundle."""

import os
import tempfile
from importlib.resources import files
from pathlib import Path
from unittest.mock import patch

import httpx
from fastapi.testclient import TestClient

from elephant.app import create_app
from elephant.config import load_settings


def main():
    package = files("elephant")
    assert "site-packages" in str(package), "Smoke test must use the installed wheel"
    for name in ("harness/pi_bridge.ts",):
        assert package.joinpath(name).is_file(), f"Wheel is missing {name}"
    for name in ("alembic.ini", "uv.lock", "package-lock.json", "deploy/elephant.service",
                 "configs/production.yaml", ".env.example", "deploy/pi-install.sh"):
        assert Path(name).is_file(), f"Source bundle is missing {name}"
    for executable in ("scripts/pi-runner-launch", "install.sh", "elephant", "deploy/install-local-linux.sh",
                       "deploy/install-macos.sh", "deploy/install-system.sh"):
        assert os.access(executable, os.X_OK), f"Source bundle is missing executable {executable}"

    settings = load_settings("configs/example.yaml")
    assert settings.tasks_enabled, "The first-run example must accept admin tasks"
    with tempfile.TemporaryDirectory() as folder:
        settings = settings.model_copy(update={
            "database": str(Path(folder) / "core.db"),
            "internal_socket": str(Path(folder) / "internal.sock"),
            "interactive_execution_enabled": False,
        })

        def unexpected_external_request(request):
            raise AssertionError("Release intake smoke must not call external services")

        def open_app():
            offline_client = httpx.AsyncClient(transport=httpx.MockTransport(unexpected_external_request))
            return TestClient(create_app(settings, run_background=False, model_client=offline_client,
                                         official_client=offline_client))

        headers = {"Authorization": "Bearer release-smoke-only", "Idempotency-Key": "release-first-task"}
        payload = {"goal": "Say hello without tools"}
        with patch.dict(os.environ, {settings.admin_token_env: "release-smoke-only"}):
            with open_app() as client:
                for path in ("/health/live", "/health/ready", "/admin",
                             "/admin/assets/console.js", "/admin/assets/console.css"):
                    response = client.get(path)
                    assert response.status_code == 200, f"{path} returned {response.status_code}"
                    assert response.content, f"{path} returned an empty response"
                response = client.post("/api/v1/tasks", json=payload, headers=headers)
                assert response.status_code == 200, response.text
                public_id = response.json()["public_id"]
                assert public_id

            # New app and DB connection, same data directory and authenticated request.
            with open_app() as client:
                response = client.get(f"/api/v1/tasks/{public_id}", headers=headers)
                assert response.status_code == 200, response.text
                assert response.json()["public_id"] == public_id
                replay = client.post("/api/v1/tasks", json=payload, headers=headers)
                assert replay.status_code == 200, replay.text
                assert replay.json()["public_id"] == public_id
    print("Installed release smoke test passed (intake and restart persistence; no model execution)")


if __name__ == "__main__":
    main()
