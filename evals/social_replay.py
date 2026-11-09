"""Replay labeled JSONL without calling a provider or transmitting chat history."""

import argparse
import json
import math
from pathlib import Path

from qqbot.clock import FakeClock
from qqbot.domain.messages import AdapterCapabilities, InboundEvent
from qqbot.domain.social import JudgmentSignals
from qqbot.policy import PolicyState, SpeechPolicy


def interval(successes, total):
    if not total:
        return None
    p, z = successes / total, 1.96
    center = (p + z * z / (2 * total)) / (1 + z * z / total)
    half = z * math.sqrt(p * (1 - p) / total + z * z / (4 * total * total)) / (1 + z * z / total)
    return {
        "numerator": successes,
        "denominator": total,
        "rate": p,
        "wilson_95": [max(0, center - half), min(1, center + half)],
    }


def replay(rows):
    splits = {}
    records = []
    for row in rows:
        for key in ("group_id", "fragment_id"):
            identity = (key, row[key])
            if identity in splits and splits[identity] != row["split"]:
                raise ValueError("group or contiguous fragment crosses dataset splits")
            splits[identity] = row["split"]
        label = row["label"]
        if label not in {"must_reply", "may_reply", "should_wait", "should_ignore"}:
            raise ValueError("unknown annotation")
        event = InboundEvent.model_validate_json(json.dumps(row["event"]))
        signals = JudgmentSignals.model_validate(row["signals"]) if row.get("signals") else None
        decision = SpeechPolicy(FakeClock(event.received_at)).decide(
            event,
            AdapterCapabilities.model_validate(row["capabilities"]),
            PolicyState(**row.get("state", {})),
            signals,
        )
        action = decision.shadow_action or decision.action
        records.append((row["split"], label, action))
    test = [(label, action) for split, label, action in records if split == "test"]
    invited = [(label, action) for label, action in test if label == "must_reply"]
    ignored = [(label, action) for label, action in test if label == "should_ignore"]
    speaking = [(label, action) for label, action in test if action == "SPEAK"]
    return {
        "total": len(records),
        "test_total": len(test),
        "must_reply_recall": interval(sum(a == "SPEAK" for _, a in invited), len(invited)),
        "ignore_false_trigger_rate": interval(sum(a == "SPEAK" for _, a in ignored), len(ignored)),
        "speech_precision": interval(
            sum(label in {"must_reply", "may_reply"} for label, _ in speaking), len(speaking)
        ),
        "real_provider_evaluation": False,
        "social_enabled": False,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset", type=Path)
    args = parser.parse_args()
    print(
        json.dumps(
            replay([json.loads(line) for line in args.dataset.read_text().splitlines() if line.strip()]),
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
