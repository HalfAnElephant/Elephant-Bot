from collections import deque
from dataclasses import dataclass
from datetime import timedelta

from qqbot.domain.messages import InboundEvent
from qqbot.domain.social import SpeechDecision


@dataclass(frozen=True)
class WindowSnapshot:
    events: tuple[InboundEvent, ...]
    seq: int
    anchor: str | None
    persona_version: str
    policy_version: str
    revocation_version: int


class ConversationWindow:
    def __init__(self, clock, max_messages=40):
        self.clock = clock
        self.events = deque(maxlen=max_messages)
        self.started_at = None
        self.due_at = None
        self.waiting = None
        self.rechecks = 0
        self.generating = False
        self.anchor = None

    def accept(self, event, *, invited=False):
        if self.events and event.ingest_seq <= self.events[-1].ingest_seq:
            return
        self.events.append(event)
        now = self.clock.now()
        if self.started_at is None:
            self.started_at = now
        self.due_at = min(
            self.started_at + timedelta(seconds=2.5), now + timedelta(seconds=0.2 if invited else 0.8)
        )
        if self.anchor is None or invited:
            self.anchor = event.platform_message_id

    def ready(self):
        return self.due_at is not None and self.clock.now() >= self.due_at

    def snapshot(self, persona_version, policy_version, revocation_version):
        if self.generating:
            raise RuntimeError("one generation per conversation")
        self.generating = True
        self.started_at = self.due_at = None
        return WindowSnapshot(
            tuple(self.events),
            self.events[-1].ingest_seq if self.events else 0,
            self.anchor,
            persona_version,
            policy_version,
            revocation_version,
        )

    def finish(self):
        self.generating = False

    def wait(self, decision: SpeechDecision):
        if decision.action != "WAIT" or decision.wake_at is None or decision.resume_after_seq is None:
            raise ValueError("bounded WAIT required")
        self.waiting = decision

    def wake(self):
        decision = self.waiting
        if decision is None:
            return False
        now = self.clock.now()
        seq = self.events[-1].ingest_seq if self.events else 0
        if (
            decision.valid_until
            and now >= decision.valid_until
            or self.rechecks >= decision.max_rechecks
        ):
            self.waiting = None
            return False
        if now >= decision.wake_at or seq > decision.resume_after_seq:
            self.waiting = None
            self.rechecks += 1
            return True
        return False

    def dump_wait(self):
        return {
            "decision": self.waiting.model_dump_json() if self.waiting else None,
            "rechecks": self.rechecks,
        }

    def restore_wait(self, value):
        self.waiting = SpeechDecision.model_validate_json(value["decision"]) if value["decision"] else None
        self.rechecks = value["rechecks"]

    def validate_draft(
        self,
        snapshot,
        *,
        invited,
        current_revocation,
        paused=False,
        recalled=False,
        already_replied=False,
        cancelled=False,
        resolved=False,
    ):
        if (
            paused
            or recalled
            or already_replied
            or cancelled
            or current_revocation != snapshot.revocation_version
        ):
            return False
        if invited:
            return True
        return not resolved and (not self.events or self.events[-1].ingest_seq == snapshot.seq)
