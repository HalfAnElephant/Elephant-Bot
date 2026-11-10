import re
from typing import Literal

from pydantic import Field

from qqbot.domain.messages import DTO, Artifact


def validate_user_facing_text(text):
    if re.search(r"<[｜|]{1,2}DSML[｜|]{1,2}\s+calls>\s*<[｜|]{1,2}DSML[｜|]{1,2}\s+invoke\b", text):
        raise ValueError("model emitted tool-call protocol instead of a user-facing response")


class HarnessCapabilities(DTO):
    event_stream: bool = True
    cancel: bool = True
    append_input: bool = False
    session_persistence: bool = True
    exact_session_resume: bool = False
    interactive_questions: bool = False


class Evidence(DTO):
    url: str = Field(min_length=1, max_length=4096)
    retrieved_at: str = Field(min_length=1, max_length=64)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class TaskResult(DTO):
    agent_authored: bool = True
    failure_code: Literal["skill_version_unavailable"] | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    status: Literal["completed", "partial", "blocked", "failed", "waiting_input", "waiting_external"]
    summary: str = Field(min_length=1, max_length=10000)
    artifacts: tuple[Artifact, ...] = Field(default=(), strict=False)
    evidence: tuple[Evidence, ...] = Field(default=(), strict=False)
    limitations: tuple[str, ...] = Field(default=(), strict=False)

    observation_refs: tuple[str, ...] = Field(default=(), strict=False)
    effect_refs: tuple[str, ...] = Field(default=(), strict=False)
