"""Check the installed wheel while running from the extracted source bundle."""

import os
import tempfile
from importlib.resources import files
from pathlib import Path

from fastapi.testclient import TestClient

from qqbot.app import create_app
from qqbot.config import load_settings


def main():
    package = files("qqbot")
    assert "site-packages" in str(package), "Smoke test must use the installed wheel"
    for name in ("harness/pi_bridge.ts",):
        assert package.joinpath(name).is_file(), f"Wheel is missing {name}"
    for name in ("alembic.ini", "uv.lock", "package-lock.json", "deploy/qqbot.service"):
        assert Path(name).is_file(), f"Source bundle is missing {name}"
    assert os.access("scripts/pi-runner-launch", os.X_OK)

    settings = load_settings("configs/example.yaml")
    with tempfile.TemporaryDirectory() as folder:
        settings = settings.model_copy(update={
            "database": str(Path(folder) / "core.db"),
            "internal_socket": str(Path(folder) / "internal.sock"),
            "interactive_execution_enabled": False,
            "tasks_enabled": False,
        })
        with TestClient(create_app(settings, run_background=False)) as client:
            for path in ("/health/live", "/health/ready", "/admin",
                         "/admin/assets/console.js", "/admin/assets/console.css"):
                response = client.get(path)
                assert response.status_code == 200, f"{path} returned {response.status_code}"
                assert response.content, f"{path} returned an empty response"
    print("Installed release smoke test passed")


if __name__ == "__main__":
    main()
