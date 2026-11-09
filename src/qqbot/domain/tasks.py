from typing import Literal

from pydantic import Field

from qqbot.domain.messages import DTO


class HarnessCapabilities(DTO):
    event_stream: bool = True
    cancel: bool = True
    append_input: bool = False
    session_persistence: bool = True
    exact_session_resume: bool = False
    interactive_questions: bool = False


class Artifact(DTO):
    id: str = Field(pattern=r"^[0-9a-f-]{36}$")
    relative_path: str = Field(min_length=1, max_length=512)
    mime_type: str = Field(min_length=1, max_length=128)
    size: int = Field(ge=0, le=20 * 1024 * 1024)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class Evidence(DTO):
    url: str = Field(min_length=1, max_length=4096)
    retrieved_at: str = Field(min_length=1, max_length=64)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class TaskResult(DTO):
    agent_authored: bool = True
    status: Literal["completed", "partial", "blocked", "failed", "waiting_input", "waiting_external"]
    summary: str = Field(min_length=1, max_length=10000)
    artifacts: tuple[Artifact, ...] = Field(default=(), strict=False)
    evidence: tuple[Evidence, ...] = Field(default=(), strict=False)
    limitations: tuple[str, ...] = Field(default=(), strict=False)

    observation_refs: tuple[str, ...] = Field(default=(), strict=False)
    effect_refs: tuple[str, ...] = Field(default=(), strict=False)
