from datetime import datetime
from typing import Literal

from pydantic import Field

from qqbot.domain.messages import DTO


class JudgmentSignals(DTO):
    directed_to_bot: float = Field(default=0, ge=0, le=1, allow_inf_nan=False)
    continues_bot_topic: float = Field(default=0, ge=0, le=1, allow_inf_nan=False)
    invites_participation: float = Field(default=0, ge=0, le=1, allow_inf_nan=False)
    useful_contribution: float = Field(default=0, ge=0, le=1, allow_inf_nan=False)
    private_exchange: float = Field(default=1, ge=0, le=1, allow_inf_nan=False)
    request_kind: Literal["chat", "question", "control", "task", "unclear"] = "unclear"
    abstain: bool = False
    model_id: str = "rule-only"


class SpeechDecision(DTO):
    action: Literal["IGNORE", "WAIT", "SPEAK"]
    reason_codes: tuple[str, ...]
    anchor_message_ids: tuple[str, ...]
    snapshot_seq: int
    policy_version: str = "speech-v1"
    judge_model: str = "rule-only"
    signals: JudgmentSignals | None = None
    wake_at: datetime | None = None
    valid_until: datetime | None = None
    resume_after_seq: int | None = None
    max_rechecks: int = 2
    shadow_action: str | None = None


class ReplyDraft(DTO):
    text_parts: tuple[str, ...] = Field(min_length=1)
    anchor_message_id: str
    attachment_refs: tuple[str, ...] = ()
    memory_ids: tuple[str, ...] = ()
    evidence_refs: tuple[str, ...] = ()
    needs_background: bool = False
