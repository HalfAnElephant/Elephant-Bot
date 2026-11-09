"""Versioned configuration snapshots shared by the console and legacy apply API."""

import json
import os
import tempfile
from hashlib import sha256
from pathlib import Path
from threading import RLock

from qqbot.config import Settings, load_runtime_settings, runtime_config_path


def version(settings: Settings) -> str:
    return sha256(settings.model_dump_json().encode()).hexdigest()


def atomic_write(path: Path, content: str):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def changes(current: Settings, candidate: Settings) -> dict:
    before, after = current.model_dump(), candidate.model_dump()
    hot = ["send_budget_per_minute"]
    if not (current.interactive_execution_enabled or current.tasks_enabled):
        hot.append("model")
    changed = [key for key in before if before[key] != after[key]]
    return {
        "changed_fields": changed,
        "hot_fields": hot,
        "restart_required": any(key not in hot for key in changed),
    }


class ConfigurationStore:
    def __init__(self, startup: Settings):
        self.startup = startup
        self.path = runtime_config_path(startup)
        self.directory = self.path.parent / "config-versions"
        self.lock = RLock()

    def read(self):
        return load_runtime_settings(self.startup)

    def save(self, previous: Settings, candidate: Settings, *, hot=False):
        for settings in (previous, candidate):
            snapshot = self.directory / (version(settings) + ".json")
            if not snapshot.exists():
                atomic_write(snapshot, settings.model_dump_json(indent=2))
        values = candidate.model_dump(mode="json")
        if hot:
            # Keep legacy hot updates from masking unrelated future startup-file edits.
            persisted = json.loads(self.path.read_text()) if self.path.exists() else {}
            values = {**persisted, **{key: values[key] for key in ("model", "send_budget_per_minute")}}
        atomic_write(self.path, json.dumps(values, ensure_ascii=False, indent=2))
