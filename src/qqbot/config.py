import json
import os
from pathlib import Path
from typing import Literal

import yaml
from pydantic import Field, model_validator

from qqbot.domain import DTO
from qqbot.domain.execution import RunBudget
from qqbot.memory.openviking_config import OpenVikingConfig
from qqbot.permissions import PermissionPolicy
from qqbot.skills.config import SkillsConfig


class AdapterConfig(DTO):
    instance_id: str
    enabled: bool = True
    kind: Literal["official"]
    transport: Literal["webhook", "websocket"] = "webhook"
    intents: int = Field(default=1 << 25, strict=True, gt=0)
    bot_id: str
    secret_env: str = ""
    credential_ref: str | None = None
    credential_store_dir: str | None = None
    app_id: str | None = None
    allowed_groups: tuple[str, ...] = ()
    allowed_users: tuple[str, ...] = ()
    admin_users: tuple[str, ...] = ()
    full_mode_enabled: bool = False
    full_mode_verified: bool = False
    proactive_verified: bool = False
    markdown_enabled: bool = True
    rich_messages_enabled: bool = True
    streaming_enabled: bool = False
    typing_enabled: bool = False
    official_messages_per_minute: int = Field(default=20, ge=1, le=20)
    official_bot_messages_per_minute: int = Field(default=30, ge=1, le=600)
    official_messages_per_day: int = Field(default=1000, ge=1, le=1000)
    c2c_reply_window_seconds: int | None = Field(default=None, ge=1, le=3600)
    c2c_max_replies: int = Field(default=4, ge=1, le=4)
    group_reply_window_seconds: int | None = Field(default=None, ge=1, le=300)
    group_max_replies: int | None = Field(default=None, ge=1, le=5)
    reply_window_seconds: int = Field(default=300, ge=1, le=86400)
    max_replies: int = Field(default=5, ge=1, le=5)
    signature_max_age_seconds: int = Field(default=300, ge=1, le=86400)
    api_base: str = "https://api.bot.qq.com"
    token_url: str = "https://api.bot.qq.com/app/getAppAccessToken"

    @model_validator(mode="before")
    @classmethod
    def reject_removed_onebot(cls, value):
        if isinstance(value, dict) and value.get("kind") == "onebot":
            raise ValueError("OneBot/NapCat configuration was removed; migrate this adapter to QQ official credentials")
        return value

    def secret(self) -> str:
        if self.credential_ref:
            from qqbot.adapters.qq_official.credential_store import load_credential
            return load_credential(self.credential_store_dir, self.credential_ref, self.app_id)
        value = os.environ.get(self.secret_env, "")
        if not value:
            raise ValueError(f"Missing environment variable: {self.secret_env}")
        return value

    @model_validator(mode="after")
    def valid(self):
        if self.kind == "official" and not self.app_id:
            raise ValueError("official adapter requires app_id")
        if self.credential_ref and (self.kind != "official" or not self.credential_store_dir):
            raise ValueError("credential reference requires official adapter and store directory")
        if not self.secret_env and not self.credential_ref:
            raise ValueError("adapter requires secret_env or credential_ref")
        if self.full_mode_verified and not self.full_mode_enabled:
            raise ValueError("full_mode_verified requires enabled permission")
        if self.transport == "websocket":
            supported = (1 << 9) | (1 << 12) | (1 << 25) | (1 << 26) | (1 << 30)
            if self.intents & ~supported:
                raise ValueError("websocket intents must select supported message or interaction events")
            if self.intents & (1 << 9) and not self.full_mode_enabled:
                raise ValueError("guild full-message intent requires full_mode_enabled and platform permission")
        return self


class ModelConfig(DTO):
    endpoint: str | None = None
    model: str | None = None
    api_key_env: str = "QQBOT_MODEL_KEY"
    timeout_seconds: int = Field(default=30, ge=1, le=120)
    context_window: int = Field(default=32768, ge=2048, le=2000000)


class MemoryConfig(DTO):
    openviking: OpenVikingConfig = OpenVikingConfig()


class ExecutionConfig(DTO):
    notify_before_tools: bool = True
    max_active: int = Field(default=8, ge=1, le=32)
    sustained_limit: int = Field(default=2, ge=1, le=31, deprecated=True,
        description="Legacy compatibility field; all Agent roles share max_active without reserved slots")
    foreground_wait_ms: int = Field(default=5000, ge=0, le=60000)
    interactive_budget: RunBudget = RunBudget(
        model_calls=30, tool_calls=80, active_seconds=600, total_wall_seconds=3600,
        output_tokens_per_call=4096,
    )
    budget: RunBudget = RunBudget()
    warm_max: int = Field(default=4, ge=0, le=32)
    warm_idle_max: int = Field(default=2, ge=0, le=32)
    warm_ttl_seconds: int = Field(default=300, ge=1, le=3600)

    @model_validator(mode="after")
    def bounded_capacity(self):
        if self.warm_max and self.warm_idle_max > self.warm_max:
            raise ValueError("idle process limit exceeds resident limit")
        return self


class QQOwnerBinding(DTO):
    app_id: str = Field(min_length=1)
    user_openid: str = Field(min_length=1)
    verified_at: str = Field(min_length=1)


class OwnerConfig(DTO):
    id: str = Field(default="personal-owner", pattern=r"^[a-zA-Z0-9_-]{1,64}$")
    qq_bindings: tuple[QQOwnerBinding, ...] = ()
    preferences: str = Field(
        default="",
        max_length=4000,
        description="Explicit personal preferences such as preferred name, language, and communication style.",
    )


class OrchestrationConfig(DTO):
    mode: Literal["leader_workers"] = "leader_workers"
    pending_inputs_per_owner: int = Field(default=64, ge=1, le=1024)
    pending_root_tasks_per_owner: int = Field(default=5, ge=1, le=100)
    child_parallelism: int = Field(default=2, ge=1, le=8)
    max_task_depth: int = Field(default=2, ge=1, le=4)
    max_nodes_per_root: int = Field(default=7, ge=2, le=64)
    leader_inbox_batch_limit: int = Field(default=16, ge=1, le=64)
    leader_inbox_coalesce_seconds: float = Field(default=2, ge=0, le=60)
    model_request_limit: int = Field(default=8, ge=1, le=32)
    pi_executable: str = "node_modules/.bin/pi"
    github_host: str = "github.com"
    workspace_bindings: dict[str, str] = Field(default_factory=dict)


class ContextConfig(DTO):
    max_request_bytes: int = Field(default=16777216, ge=4096, le=16777216)
    max_tokens: int | None = Field(default=None, ge=256, le=2000000,
        description="pi soft compaction threshold; null reserves 10% of the model window or output headroom, whichever is larger")
    keep_recent_tokens: int | None = Field(default=None, ge=128, le=2000000,
        description="pi recent context retained during compaction; null uses 20% of the model window")
    compaction_output_tokens: int = Field(default=4096, ge=128, le=32768)
    recent_messages: int = Field(default=64, ge=1, le=100)
    topic_idle_seconds: int = Field(default=1800, ge=60, le=86400)
    version: str = "context-v2"

    def limits(self, model_window: int, output_tokens: int) -> dict:
        output = max(output_tokens, self.compaction_output_tokens)
        headroom = output + 1024
        soft = self.max_tokens if self.max_tokens is not None else model_window - max(
            (model_window + 9) // 10, headroom
        )
        recent = self.keep_recent_tokens if self.keep_recent_tokens is not None else min(
            model_window // 5, soft // 2
        )
        if not 0 < recent < soft <= model_window - headroom:
            raise ValueError("context requires recent < soft threshold with output headroom inside model window")
        return {"max_bytes": self.max_request_bytes, "soft_compaction_input_tokens": soft,
                "keep_recent_tokens": recent, "compaction_output_tokens": self.compaction_output_tokens}


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
    owner: OwnerConfig = OwnerConfig()
    orchestration: OrchestrationConfig = OrchestrationConfig()
    context: ContextConfig = ContextConfig()
    internal_socket: str = "var/internal.sock"
    memory: MemoryConfig = MemoryConfig()
    skills: SkillsConfig = SkillsConfig()

    @model_validator(mode="after")
    def distinct(self):
        ids = [a.instance_id for a in self.adapters]
        if len(ids) != len(set(ids)):
            raise ValueError("adapter instance IDs must be unique")
        output = max(
            self.execution.interactive_budget.output_tokens_per_call,
            self.execution.budget.output_tokens_per_call,
        )
        self.context.limits(self.model.context_window, output)
        return self


def load_settings(path: str | Path) -> Settings:
    return Settings.model_validate_json(json.dumps(yaml.safe_load(Path(path).read_text()) or {}), strict=False)


def runtime_config_path(settings: Settings) -> Path:
    # Anchor to the startup file's database, even when the saved database changes.
    return Path(settings.database).resolve().parent / "runtime-config.json"


def load_runtime_settings(settings: Settings) -> Settings:
    path = runtime_config_path(settings)
    if not path.exists():
        return settings
    values = json.loads(path.read_text())
    return Settings.model_validate_json(json.dumps({**settings.model_dump(mode="json"), **values}), strict=False)
