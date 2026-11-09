import json
import os
from pathlib import Path
from typing import Literal

import yaml
from pydantic import Field, model_validator

from qqbot.domain import DTO
from qqbot.domain.execution import RunBudget
from qqbot.permissions import PermissionPolicy


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
    context_window: int = Field(default=32768, ge=2048, le=2000000)


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


class ExecutionConfig(DTO):
    max_active: int = Field(default=4, ge=1, le=32)
    sustained_limit: int = Field(default=2, ge=1, le=31)
    foreground_wait_ms: int = Field(default=5000, ge=0, le=60000)
    interactive_budget: RunBudget = RunBudget(
        model_calls=6, tool_calls=8, active_seconds=60, total_wall_seconds=600
    )
    budget: RunBudget = RunBudget()
    warm_max: int = Field(default=4, ge=0, le=32)
    warm_idle_max: int = Field(default=2, ge=0, le=32)
    warm_ttl_seconds: int = Field(default=300, ge=1, le=3600)

    @model_validator(mode="after")
    def bounded_capacity(self):
        if self.sustained_limit >= self.max_active:
            raise ValueError("reserve at least one execution slot for new interaction")
        if self.warm_max and self.warm_idle_max > self.warm_max:
            raise ValueError("idle process limit exceeds resident limit")
        return self


class ContextConfig(DTO):
    max_request_bytes: int = Field(default=262144, ge=4096, le=8388608)
    max_tokens: int = Field(default=6000, ge=256, le=1000000)
    hard_input_tokens: int = Field(default=8000, ge=256, le=1000000)
    keep_recent_tokens: int = Field(default=2000, ge=128, le=1000000)
    compaction_input_tokens: int = Field(default=16000, ge=256, le=1000000)
    compaction_output_tokens: int = Field(default=1000, ge=128, le=100000)
    recent_messages: int = Field(default=16, ge=1, le=100)
    topic_idle_seconds: int = Field(default=1800, ge=60, le=86400)
    version: str = "context-v1"

    @model_validator(mode="after")
    def bounded_context(self):
        if not self.keep_recent_tokens < self.max_tokens < self.hard_input_tokens:
            raise ValueError("context limits require recent < soft < hard")
        if self.compaction_input_tokens < self.hard_input_tokens:
            raise ValueError("compaction input budget must cover the normal hard input budget")
        return self


class Settings(DTO):
    deployment_id: str = "elephant-local"
    database: str = "var/core.db"
    adapters: tuple[AdapterConfig, ...] = ()
    admin_token_env: str = "QQBOT_ADMIN_TOKEN"
    internal_token_env: str = "QQBOT_INTERNAL_TOKEN"
    model: ModelConfig = ModelConfig()
    source_url: str = ""
    send_budget_per_minute: int = Field(default=0, ge=0, le=60)
    timezone: str = "Asia/Shanghai"
    tasks_enabled: bool = False
    interactive_execution_enabled: bool = True
    permissions: PermissionPolicy = PermissionPolicy()
    execution: ExecutionConfig = ExecutionConfig()
    context: ContextConfig = ContextConfig()
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
        output = max(
            self.execution.interactive_budget.output_tokens_per_call,
            self.execution.budget.output_tokens_per_call,
        )
        if self.context.hard_input_tokens + output > self.model.context_window:
            raise ValueError("input and output budget exceed configured model context window")
        if (
            self.context.compaction_input_tokens + self.context.compaction_output_tokens
            > self.model.context_window
        ):
            raise ValueError("compaction budget exceeds configured model context window")
        return self


def load_settings(path: str | Path) -> Settings:
    return Settings.model_validate_json(json.dumps(yaml.safe_load(Path(path).read_text()) or {}))


def runtime_config_path(settings: Settings) -> Path:
    # Anchor to the startup file's database, even when the saved database changes.
    return Path(settings.database).resolve().parent / "runtime-config.json"


def load_runtime_settings(settings: Settings) -> Settings:
    path = runtime_config_path(settings)
    if not path.exists():
        return settings
    values = json.loads(path.read_text())
    return Settings.model_validate_json(json.dumps({**settings.model_dump(mode="json"), **values}))
