import json
import os
import tempfile
from hashlib import sha256
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field


class PersonaSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    persona_id: str
    version: int = Field(ge=1)
    name: str
    identity: str
    traits: tuple[str, ...]
    style: dict[str, Any]
    boundaries: tuple[str, ...]


class PersonaStore:
    def __init__(self, path, overrides_path=None):
        self.path = Path(path)
        self.overrides_path = (
            Path(overrides_path) if overrides_path else self.path.with_suffix(".overrides.json")
        )
        self._core = None
        self._overrides = {}
        self.reload()

    def reload(self):
        parsed = PersonaSpec.model_validate(yaml.safe_load(self.path.read_text()))
        overrides = json.loads(self.overrides_path.read_text()) if self.overrides_path.exists() else {}
        if not isinstance(overrides, dict):
            raise TypeError("invalid persona override store")
        for values in overrides.values():
            self._validate_override(values)
        self._core, self._overrides = parsed, overrides

    @staticmethod
    def _validate_override(values):
        if not isinstance(values, dict) or set(values) - {
            "name",
            "formality",
            "reply_length",
            "participation",
        }:
            raise ValueError("unsupported group override")
        if any(not isinstance(value, (str, int, float, list)) for value in values.values()):
            raise ValueError("invalid group override value")

    def set_override(self, conversation_id, values, *, authorized=False):
        if not authorized:
            raise PermissionError("persona override requires administrator authorization")
        self._validate_override(values)
        updated = {**self._overrides, conversation_id: json.loads(json.dumps(values))}
        self.overrides_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=self.overrides_path.parent, delete=False
            ) as handle:
                temporary = Path(handle.name)
                json.dump(updated, handle, ensure_ascii=False, allow_nan=False)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.overrides_path)
        finally:
            if temporary and temporary.exists():
                temporary.unlink()
        self._overrides = updated

    def snapshot(self, conversation_id):
        data = {"core": self._core.model_dump(mode="json"), "group": self._overrides.get(conversation_id, {})}
        encoded = json.dumps(data, ensure_ascii=False, sort_keys=True)
        return encoded, sha256(encoded.encode()).hexdigest()


def assemble_prompt(
    persona_json,
    *,
    boundary,
    platform,
    request,
    recent_messages,
    memories=(),
    jargon=(),
    summary="",
    max_chars=32000,
):
    """Untrusted material stays in user data. Current request is never truncated."""
    persona = json.loads(persona_json)
    system = boundary + "\n人格配置\n" + json.dumps(persona, ensure_ascii=False)
    required = {"platform": platform, "request": request, "recent_messages": recent_messages}
    required_json = json.dumps(required, ensure_ascii=False, default=str)
    if len(system) + len(required_json) > max_chars:
        raise ValueError("current request and permission snapshot exceed context budget")
    optional = {
        "memories": [m.model_dump(mode="json") for m in memories[:8]],
        "jargon": [j.model_dump(mode="json") for j in jargon[:5]],
        "summary": summary,
    }
    allowance = min(max_chars // 2, max_chars - len(system) - len(required_json))
    while len(json.dumps(optional, ensure_ascii=False)) > allowance:
        if optional["jargon"]:
            optional["jargon"].pop()
        elif optional["memories"]:
            optional["memories"].pop()
        elif optional["summary"]:
            optional["summary"] = ""
        else:
            break
    return [
        {"role": "system", "content": system},
        {
            "role": "user",
            "content": json.dumps(
                {"untrusted_material": optional, **required}, ensure_ascii=False, default=str
            ),
        },
    ]
