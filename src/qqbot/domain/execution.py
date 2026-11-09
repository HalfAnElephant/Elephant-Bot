"""Host-owned execution contracts; delivery never changes the user's goal."""
from typing import Any, Literal

from pydantic import Field, model_validator

from qqbot.domain.messages import DTO, ConversationRef, PrincipalRef


class TurnInput(DTO):
    conversation: ConversationRef
    principal: PrincipalRef
    text: str = Field(min_length=1, max_length=64000)
    source_event_ids: tuple[str, ...]
    anchor_event_id: str
    reply_to: str | None = None
    timezone: str = "Asia/Shanghai"
    source_revision: int = 0


class TaskGoal(DTO):
    text: str = Field(min_length=1, max_length=64000)
    target_refs: tuple[str, ...] = ()
    constraints: tuple[str, ...] = ()
    requested_actions: tuple[str, ...] = ()
    success_checks: tuple[str, ...] = ()
    requested_outputs: tuple[str, ...] = ()


class RunBudget(DTO):
    model_calls: int = Field(default=60, ge=2, le=200)
    tool_calls: int = Field(default=200, ge=0, le=1000)
    active_seconds: int = Field(default=1800, ge=1, le=86400)
    total_wall_seconds: int = Field(default=7200, ge=1, le=604800)
    output_tokens_per_call: int = Field(default=8192, ge=1, le=32768)
    completion_reserve: int = Field(default=1, ge=1)

    @model_validator(mode="after")
    def reserve_fits(self):
        if self.completion_reserve >= self.model_calls:
            raise ValueError("completion reserve must be smaller than total model budget")
        return self


class ExecutionPlan(DTO):
    runner: Literal["pi"] = "pi"
    initial_delivery: Literal["ATTACHED", "DETACHED"] = "ATTACHED"
    foreground_wait_ms: int = Field(default=5000, ge=0, le=60000)
    capability_hints: tuple[str, ...] = ()
    model_config_ref: str = "configured_default"
    context_config_ref: str = "default"
    budget: RunBudget = RunBudget()
    workspace_ref: str | None = None
    permission_snapshot_ref: str = "current"
    config_version: str = "general-v3"


class TaskOutcome(DTO):
    disposition: Literal["satisfied", "partial", "blocked", "failed", "cancelled"]
    narrative: str = Field(min_length=1, max_length=10000)
    observation_refs: tuple[str, ...] = ()
    effect_refs: tuple[str, ...] = ()
    attachments: tuple[str, ...] = ()
    unresolved_items: tuple[str, ...] = ()


class WaitCondition(DTO):
    capability: str = Field(min_length=1, max_length=128)
    arguments: dict[str, Any]
    path: str = Field(min_length=1, max_length=256, pattern=r"^[A-Za-z0-9_.]+$")
    equals: Any
    interval_seconds: int = Field(default=30, ge=15, le=3600)
