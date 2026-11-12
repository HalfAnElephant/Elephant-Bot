"""Host policy for procedural learning; unrelated to executable Pi skill loading."""
from pydantic import BaseModel, ConfigDict, Field


class SkillsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    enabled: bool = True
    group_enabled: bool = True
    max_per_scope: int = Field(default=100, ge=1, le=1000)
    max_versions_per_skill: int = Field(default=10, ge=1, le=50)
    max_candidates_per_task: int = Field(default=3, ge=1, le=10)
