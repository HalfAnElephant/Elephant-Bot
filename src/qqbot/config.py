import json
import os
from pathlib import Path
from typing import Literal

import yaml
from pydantic import Field, model_validator

from qqbot.domain import DTO


class AdapterConfig(DTO):
    instance_id: str
    kind: Literal["onebot", "official"]
    bot_id: str
    secret_env: str
    app_id: str | None = None
    websocket_url: str | None = None
    allowed_groups: tuple[str, ...] = ()
    allowed_users: tuple[str, ...] = ()
    admin_users: tuple[str, ...] = ()
    known_bot_ids: tuple[str, ...] = ()
    full_mode_enabled: bool = False
    full_mode_verified: bool = False
    proactive_verified: bool = False
    reply_window_seconds: int = Field(default=300, ge=1, le=86400)
    max_replies: int = Field(default=5, ge=1, le=5)
    signature_max_age_seconds: int = Field(default=300, ge=1, le=86400)
    api_base: str = "https://api.bot.qq.com"
    token_url: str = "https://api.bot.qq.com/app/getAppAccessToken"

    def secret(self) -> str:
        value = os.environ.get(self.secret_env, "")
        if not value:
            raise ValueError(f"Missing environment variable: {self.secret_env}")
        return value

    @model_validator(mode="after")
    def valid(self):
        if self.kind == "official" and not self.app_id:
            raise ValueError("official adapter requires app_id")
        if self.full_mode_verified and not self.full_mode_enabled:
            raise ValueError("full_mode_verified requires enabled permission")
        return self


class ModelConfig(DTO):
    endpoint: str | None = None
    model: str | None = None
    api_key_env: str = "QQBOT_MODEL_KEY"
    timeout_seconds: int = Field(default=30, ge=1, le=120)


class JudgeConfig(DTO):
    kind: Literal["rule", "jev", "structured"] = "rule"
    endpoint: str = "https://api.typesafe.ai/v1/systemone"
    model: str = "jev-1.14.0"
    api_key_env: str = "QQBOT_JUDGE_KEY"


class MemoryConfig(DTO):
    maintenance_enabled: bool = False
    auto_extract: bool = False
    extractor_endpoint: str | None = None
    extractor_model: str | None = None
    extractor_key_env: str = "QQBOT_MEMORY_KEY"
    embedding_endpoint: str | None = None
    embedding_model: str | None = None
    embedding_version: str = "v1"
    embedding_dimensions: int | None = Field(default=None, ge=1, le=65536)
    embedding_key_env: str = "QQBOT_EMBEDDING_KEY"
    raw_retention_days: int = Field(default=30, ge=1, le=3650)


class Settings(DTO):
    deployment_id: str = "elephant-local"
    database: str = "var/core.db"
    adapters: tuple[AdapterConfig, ...] = ()
    admin_token_env: str = "QQBOT_ADMIN_TOKEN"
    internal_token_env: str = "QQBOT_INTERNAL_TOKEN"
    model: ModelConfig = ModelConfig()
    source_url: str = ""
    send_budget_per_minute: int = Field(default=6, ge=1, le=60)
    timezone: str = "Asia/Shanghai"
    tasks_enabled: bool = False
    internal_socket: str = "var/internal.sock"
    persona_path: str = "personas/elephant_default.yaml"
    judge: JudgeConfig = JudgeConfig()
    memory: MemoryConfig = MemoryConfig()
    social_evaluated_conversations: tuple[str, ...] = ()

    @model_validator(mode="after")
    def distinct(self):
        ids = [a.instance_id for a in self.adapters]
        if len(ids) != len(set(ids)):
            raise ValueError("adapter instance IDs must be unique")
        return self


def load_settings(path: str | Path) -> Settings:
    return Settings.model_validate_json(json.dumps(yaml.safe_load(Path(path).read_text()) or {}))
