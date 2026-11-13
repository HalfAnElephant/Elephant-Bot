"""Hermes host policy; no candidate, activation or publication workflow."""
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class HermesConfig(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    create_dir: str | None = None
    write_approval: Literal[False] = False
    inline_shell: Literal[False] = False
    ledger: bool = True
    ledger_max_bytes: int = Field(default=5 * 1024 * 1024, ge=0)
    creation_nudge_interval: int = Field(default=10, ge=0, le=1000)


class SkillsConfig(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)
    enabled: bool = True
    group_enabled: bool = False
    provider: Literal['hermes'] = 'hermes'
    owner_storage_root: str | None = None
    hermes: HermesConfig = HermesConfig()
    background_review_enabled: bool = True

    @model_validator(mode='before')
    @classmethod
    def reject_legacy_candidates(cls, value):
        if isinstance(value, dict) and {'max_per_scope', 'max_versions_per_skill', 'max_candidates_per_task'} & value.keys():
            raise ValueError('Legacy candidate skills settings are retired; migrate to skills.hermes and standard SKILL.md files.')
        return value
