"""Host-validated task intents. Identity, scope and budgets never come from tools."""
from typing import Literal

from pydantic import Field

from qqbot.domain.messages import DTO


class ChildSpec(DTO):
    mode: Literal["work", "consult"] = "work"
    goal: str = Field(min_length=1, max_length=16000)
    delegation_key: str = Field(min_length=1, max_length=128)
    input_refs: tuple[str, ...] = Field(default=(), strict=False)
    success_checks: tuple[str, ...] = Field(default=(), strict=False)
    requested_outputs: tuple[str, ...] = Field(default=(), strict=False)
    rework_of_task_id: str | None = None


class GoalSpec(DTO):
    goal: str = Field(min_length=1, max_length=16000)
    success_checks: tuple[str, ...] = Field(default=(), strict=False)
    requested_outputs: tuple[str, ...] = Field(default=(), strict=False)
    children: tuple[ChildSpec, ...] = Field(min_length=1, max_length=6, strict=False)


class TaskInputSpec(DTO):
    task_ref: str = Field(min_length=1, max_length=128)
    kind: Literal["followup", "answer", "notify"]
    text: str = Field(min_length=1, max_length=16000)
    question_id: str | None = None


class WaitSpec(DTO):
    children: tuple[str, ...] = Field(min_length=1, max_length=6, strict=False)
    predicate: Literal["all", "any"] = "all"


class ProgressSpec(DTO):
    phase: str = Field(min_length=1, max_length=128)
    public_text: str = Field(min_length=1, max_length=2000)
    evidence_refs: tuple[str, ...] = Field(default=(), max_length=20, strict=False)
