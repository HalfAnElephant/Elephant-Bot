from dataclasses import dataclass, field
from datetime import datetime, timedelta

from qqbot.domain.messages import AdapterCapabilities, InboundEvent, ObservationMode, Surface
from qqbot.domain.social import JudgmentSignals, ParticipationSettings, SpeechDecision, SpeechThresholds


@dataclass
class PolicyState:
    mode: str = "addressed_only"
    authenticated: bool = True
    allowed: bool = True
    muted: bool = False
    paused: bool = False
    duplicate: bool = False
    valid_bot_reply: bool = False
    remaining_budget: int | None = None
    last_bot_at: datetime | None = None
    new_human_messages: int = 0
    unanswered_proactive: int = 0
    recent_bot_ratio: float = 0
    rechecks: int = 0
    social_evaluated: bool = False
    suppressed_until: datetime | None = None


class SpeechPolicy:
    def __init__(self, clock):
        self.clock = clock

    def decide(
        self,
        event: InboundEvent,
        capabilities: AdapterCapabilities,
        state: PolicyState,
        signals: JudgmentSignals | None = None,
        *,
        thresholds: SpeechThresholds | None = None,
        participation: ParticipationSettings | None = None,
    ) -> SpeechDecision:
        now = self.clock.now()
        thresholds = thresholds or SpeechThresholds()
        participation = participation or ParticipationSettings()

        def result(action, reason, blocked_rules=()):
            return SpeechDecision(
                action="IGNORE" if state.mode == "shadow" else action,
                shadow_action=action if state.mode == "shadow" else None,
                reason_codes=(reason,),
                blocked_rules=blocked_rules,
                anchor_message_ids=(event.platform_message_id,),
                snapshot_seq=event.ingest_seq,
                signals=signals,
                judge_model=signals.model_id if signals else "rule-only",
                wake_at=now + timedelta(seconds=1) if action == "WAIT" else None,
                resume_after_seq=event.ingest_seq if action == "WAIT" else None,
                valid_until=now + timedelta(seconds=20),
                max_rechecks=participation.max_rechecks,
            )

        if not state.authenticated or not state.allowed or state.muted:
            return result("IGNORE", "HARD_GATE")
        if event.sender_is_bot or state.duplicate or event.event_type != "message.created":
            return result("IGNORE", "ECHO_OR_DUPLICATE")
        if state.paused or state.mode == "silent" or (state.remaining_budget is not None and state.remaining_budget <= 0):
            return result("IGNORE", "PAUSED_OR_BUDGET")
        if not capabilities.can_reply:
            return result("IGNORE", "NO_SEND_CAPABILITY")
        invited = event.conversation.surface == Surface.PRIVATE or event.mentions_bot or state.valid_bot_reply
        if invited:
            return result("SPEAK", "INVITED")
        if state.mode == "addressed_only":
            return result("IGNORE", "UNADDRESSED")
        if (
            event.observation_mode != ObservationMode.FULL
            or not capabilities.can_observe_unaddressed_messages
        ):
            return result("IGNORE", "SOCIAL_NOT_READY")
        blocked_rules = tuple(
            name for name, blocked in (
                ("SUPPRESSED", state.suppressed_until and now < state.suppressed_until),
                ("COOLDOWN", state.last_bot_at and (now - state.last_bot_at).total_seconds()
                 < participation.cooldown_seconds),
                ("MIN_HUMAN_MESSAGES", state.new_human_messages < participation.min_human_messages),
                ("UNANSWERED_PROACTIVE", participation.require_member_response and state.unanswered_proactive > 0),
                ("RECENT_BOT_RATIO", state.recent_bot_ratio >= participation.max_recent_bot_ratio),
            ) if blocked
        )
        if blocked_rules:
            return result("IGNORE", "PARTICIPATION_LIMIT", blocked_rules)
        if not signals or signals.abstain:
            return result("IGNORE", "JUDGE_ABSTAIN")
        if signals.proactive_allowed < 0.5:
            return result("IGNORE", "PARTICIPATION_PREFERENCE")
        if (
            (signals.invites_participation >= thresholds.invites_participation
             or signals.continues_bot_topic >= thresholds.continues_bot_topic)
            and signals.useful_contribution >= thresholds.useful_contribution
            and signals.private_exchange <= thresholds.private_exchange
        ):
            return result("SPEAK", "USEFUL_INVITATION")
        if (signals.useful_contribution >= thresholds.wait_useful_contribution
                and signals.private_exchange <= thresholds.private_exchange
                and state.rechecks < participation.max_rechecks):
            return result("WAIT", "OTHERS_ARE_FINISHING_TOPIC")
        return result("IGNORE", "LOW_RELEVANCE")


@dataclass
class ParticipationController:
    human_ids: set[str] = field(default_factory=set)
    invited_ids: set[str] = field(default_factory=set)
    reservations: dict[str, str] = field(default_factory=dict)
    sent_ids: set[str] = field(default_factory=set)
    timestamps: dict[str, datetime] = field(default_factory=dict)

    def prune(self, now: datetime, window_seconds=60):
        expired = {key for key, at in self.timestamps.items() if (now - at).total_seconds() >= window_seconds}
        self.human_ids.difference_update(expired)
        self.invited_ids.difference_update(expired)
        self.sent_ids.difference_update(expired)
        for key in expired:
            self.timestamps.pop(key, None)
        # UNKNOWN reservations persist until reconciliation, preventing retry floods.

    def observe(self, event_id: str, invited: bool = False, now: datetime | None = None, *, window_seconds=60):
        if now is not None:
            self.prune(now, window_seconds)
            self.timestamps.setdefault(event_id, now)
        self.human_ids.add(event_id)
        if invited:
            self.invited_ids.add(event_id)

    def available(self, ratio: float = 0.2) -> bool:
        occupied = self.sent_ids | set(self.reservations) | self.invited_ids
        return len(occupied) < len(self.human_ids) * ratio

    def reserve(self, delivery_id: str, now: datetime | None = None):
        if now is not None:
            self.timestamps.setdefault(delivery_id, now)
        self.reservations[delivery_id] = "PENDING"

    def feedback(self, delivery_id: str, state: str):
        if state == "SENT":
            self.sent_ids.add(delivery_id)
            self.reservations.pop(delivery_id, None)
        elif state in {"FAILED", "CANCELLED"}:
            self.reservations.pop(delivery_id, None)
