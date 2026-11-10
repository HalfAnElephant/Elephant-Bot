"""Labeled policy replay. Provider judging requires an explicit --config."""

import argparse
import asyncio
import hashlib
import json
import math
import os
import re
import subprocess
import time
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

import httpx
from pydantic import TypeAdapter

from qqbot.clock import FakeClock
from qqbot.config import load_settings
from qqbot.domain.messages import AdapterCapabilities, InboundEvent
from qqbot.domain.social import JudgmentSignals, ParticipationSettings, SpeechThresholds
from qqbot.policy import PolicyState, SpeechPolicy
from qqbot.policy.judge import Jev1Judge, JevJudge, StructuredLLMJudge


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
    costs = []
    unjudged = 0
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
        evaluated_at = row.get("context", {}).get("evaluated_at")
        now = datetime.fromisoformat(evaluated_at) if evaluated_at else event.received_at
        decision = SpeechPolicy(FakeClock(now)).decide(
            event,
            AdapterCapabilities.model_validate(row["capabilities"]),
            TypeAdapter(PolicyState).validate_python(row.get("state", {})),
            signals,
            thresholds=SpeechThresholds.model_validate(row.get("thresholds", {})),
            participation=ParticipationSettings.model_validate(row.get("participation", {})),
        )
        action = decision.shadow_action or decision.action
        unjudged += decision.reason_codes == ("JUDGE_ABSTAIN",)
        records.append((row["split"], label, action))
        if row["split"] == "test":
            costs.append((row.get("model_requests"), row.get("decision_ms")))
    test = [(label, action) for split, label, action in records if split == "test"]
    invited = [(label, action) for label, action in test if label == "must_reply"]
    ignored = [(label, action) for label, action in test if label == "should_ignore"]
    speaking = [(label, action) for label, action in test if action == "SPEAK"]
    requests = [value for value, _ in costs if type(value) is int and value >= 0]
    durations = sorted(value for _, value in costs if type(value) in (int, float) and math.isfinite(value) and value >= 0)
    return {
        "model_requests_per_100_messages": 100 * sum(requests) / len(test) if test and len(requests) == len(test) else None,
        "request_measurement_coverage": len(requests),
        "decision_p95_ms": durations[max(0, math.ceil(len(durations) * .95) - 1)] if durations else None,
        "duration_measurement_coverage": len(durations),
        "prohibited_violations": sum(a == "SPEAK" for _, a in ignored),
        "total": len(records),
        "test_total": len(test),
        "unjudged_messages": unjudged,
        "label_action_counts": {label: {action: sum(item == (label, action) for item in test)
            for action in ("SPEAK", "WAIT", "IGNORE")}
            for label in ("must_reply", "may_reply", "should_wait", "should_ignore")},
        "should_wait_recall": interval(sum(label == "should_wait" and action == "WAIT" for label, action in test),
                                        sum(label == "should_wait" for label, _ in test)),
        "must_reply_recall": interval(sum(a == "SPEAK" for _, a in invited), len(invited)),
        "ignore_false_trigger_rate": interval(sum(a == "SPEAK" for _, a in ignored), len(ignored)),
        "speech_precision": interval(
            sum(label in {"must_reply", "may_reply"} for label, _ in speaking), len(speaking)
        ),
        "real_provider_evaluation": False,
        "social_enabled": False,
        "evaluation_kind": "precomputed_signals_policy",
    }



def validate_provider_rows(rows):
    # Validate the complete file before any request, including rows outside a selected split.
    replay(rows)
    conversations, case_ids = {}, set()
    required = {"version", "persona_version", "bot", "participation_policy", "group_participation",
                "target_message_id", "evaluated_at", "messages", "recent_bot_messages"}
    for row in rows:
        if not {"event", "capabilities", "state", "context", "label", "group_id", "fragment_id"} <= set(row):
            raise ValueError("provider cases require event, capabilities, state, context and annotation identities")
        if row.get("data_classification") not in {"synthetic", "deidentified"}:
            raise ValueError("provider replay requires synthetic or deidentified data_classification")
        if row["split"] not in {"development", "test"} or not row.get("case_id") or row["case_id"] in case_ids:
            raise ValueError("cases require unique case_id and development/test split")
        case_ids.add(row["case_id"])
        event = InboundEvent.model_validate_json(json.dumps(row["event"]))
        identity = event.conversation.key
        if identity in conversations and conversations[identity] != row["split"]:
            raise ValueError("conversation crosses dataset splits")
        conversations[identity] = row["split"]
        context = row.get("context", {})
        if set(context) != required or context["version"] != "speech-context-v2":
            raise ValueError("context must use the Core speech-context-v2 fields")
        if (not isinstance(context["bot"], dict) or set(context["bot"]) != {"speaker", "name", "aliases"}
                or context["bot"]["speaker"] != "bot" or not isinstance(context["bot"]["aliases"], list)
                or not all(isinstance(context[key], str) for key in ("participation_policy", "group_participation"))):
            raise ValueError("invalid bot or participation policy context")
        evaluated_at = datetime.fromisoformat(context["evaluated_at"])
        if evaluated_at.tzinfo is None:
            raise ValueError("evaluated_at requires timezone")
        messages, bot_messages = context["messages"], context["recent_bot_messages"]
        if not isinstance(messages, list) or not isinstance(bot_messages, list) or len(messages) > 12 or len(bot_messages) > 4:
            raise ValueError("context message counts exceed Core bounds")
        allowed = {"id", "source", "speaker", "is_bot", "is_self", "at", "text", "mentions", "reply_to", "non_text_types"}
        for message in (*messages, *bot_messages):
            if (not isinstance(message, dict) or set(message) - allowed
                    or not {"id", "source", "speaker", "is_self", "at", "text", "reply_to"} <= set(message)
                    or not isinstance(message["text"], str) or len(message["text"]) > 1000
                    or not isinstance(message["speaker"], str)
                    or not re.fullmatch(r"bot|member_[0-9]+", message["speaker"])):
                raise ValueError("invalid Core message fields or text bounds")
            at = datetime.fromisoformat(message["at"])
            if at.tzinfo is None or at > evaluated_at:
                raise ValueError("context contains future or timezone-free messages")
        target = [message for message in messages if message["id"] == event.platform_message_id]
        if (context["target_message_id"] != event.platform_message_id or len(target) != 1
                or target[0]["source"] != event.event_id or target[0]["text"] != event.text[:1000]
                or target[0]["reply_to"] != event.reply_to):
            raise ValueError("target context does not match the persisted event")
        from qqbot.memory import _safe
        _safe(json.dumps({"event": row["event"], "context": context}, ensure_ascii=False))


class RecordedClient:
    """Record actual HTTP attempts, including Jev1's six concurrent questions."""
    def __init__(self, client, api_key):
        self.client, self.api_key = client, api_key
        self.requests, self.pending = [], set()

    async def post(self, endpoint, **kwargs):
        record = {"started_at": datetime.now(UTC).isoformat(), "usage": None}
        self.requests.append(record)
        started = time.monotonic()
        current = asyncio.current_task()
        self.pending.add(current)
        try:
            response = await self.client.post(endpoint, **kwargs)
            record["http_status"] = response.status_code
            body = response.text[:1048576].replace(self.api_key, "[REDACTED]")
            record["response_truncated"] = len(response.content) > 1048576
            try:
                record["response"] = json.loads(body)
            except ValueError:
                record["response"] = body
            if isinstance(record["response"], dict):
                usage = record["response"].get("usage")
                if isinstance(usage, dict):
                    record["usage"] = {key: value for key, value in usage.items()
                        if type(value) in (int, float) and math.isfinite(value) and value >= 0} or None
            return response
        except BaseException as error:
            record["error_type"] = type(error).__name__
            raise
        finally:
            record["elapsed_ms"] = round((time.monotonic() - started) * 1000, 3)
            self.pending.discard(current)

    async def finish(self):
        if self.pending:
            await asyncio.gather(*self.pending, return_exceptions=True)


def run_metadata(settings, dataset):
    from qqbot.harness.skills import DEFAULT_SKILLS, skill_files, skill_version
    root = Path(__file__).resolve().parents[1]
    revision = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, capture_output=True, text=True, check=False)
    sources = ["evals/social_replay.py", "src/qqbot/policy/judge.py", "src/qqbot/policy/__init__.py",
               "src/qqbot/domain/social.py", "src/qqbot/runtime.py"]
    return {"created_at": datetime.now(UTC).isoformat(), "git_commit": revision.stdout.strip() or None,
        "source_sha256": {path: hashlib.sha256((root / path).read_bytes()).hexdigest() for path in sources},
        "dataset_sha256": hashlib.sha256(dataset.read_bytes()).hexdigest(),
        "judge_config": settings.judge.model_dump(mode="json"),
        "judge_config_sha256": hashlib.sha256(settings.judge.model_dump_json().encode()).hexdigest(),
        "skills_source_version": skill_version(skill_files(DEFAULT_SKILLS)),
        "context_version": "speech-context-v2", "execution_channel": "network",
        "policy_scope": "SpeechPolicy with supplied state; dynamic reservation counters are outside this replay",
        "qq_send_operations": 0}


async def provider_replay(rows, settings, output_directory, metadata, *, client=None, split=None):
    validate_provider_rows(rows)
    config = settings.judge
    if config.kind == "rule":
        raise ValueError("--config must select a jev or structured judge")
    api_key = os.environ.get(config.api_key_env)
    if not api_key:
        raise ValueError("configured judge API key environment variable is missing")
    if client is None:
        async with httpx.AsyncClient() as network:
            return await provider_replay(rows, settings, output_directory, metadata, client=network, split=split)
    provider = StructuredLLMJudge if config.kind == "structured" else (Jev1Judge if config.model == "jev1" else JevJudge)
    judge = provider(api_key, config.model, config.endpoint, client)
    output_directory.mkdir(parents=True, exist_ok=True)
    if (output_directory / "metrics.json").exists():
        raise FileExistsError("metrics.json already exists; use a new output directory")
    evaluated, traces = [], []
    with (output_directory / "trace.jsonl").open("x") as trace_file:
        for row in rows:
            if split is not None and row["split"] != split:
                continue
            recorded = RecordedClient(client, api_key)
            judge.client = recorded
            event = InboundEvent.model_validate_json(json.dumps(row["event"]))
            state = TypeAdapter(PolicyState).validate_python(row.get("state", {}))
            thresholds = SpeechThresholds.model_validate(row.get("thresholds", config.thresholds.model_dump()))
            participation = ParticipationSettings.model_validate(row.get("participation", config.participation.model_dump()))
            policy = SpeechPolicy(FakeClock(datetime.fromisoformat(row["context"]["evaluated_at"])))
            capabilities = AdapterCapabilities.model_validate(row["capabilities"])
            started = time.monotonic()
            decision = policy.decide(event, capabilities, state, thresholds=thresholds, participation=participation)
            signals, status = None, "not_called"
            if decision.reason_codes == ("JUDGE_ABSTAIN",):
                signals = await judge.judge(row["context"])
                await recorded.finish()
                status = "abstained" if signals.abstain else "success"
                decision = policy.decide(event, capabilities, state, signals, thresholds=thresholds, participation=participation)
            elapsed = round((time.monotonic() - started) * 1000, 3)
            trace = {"case_id": row["case_id"], "group_id": row["group_id"], "fragment_id": row["fragment_id"],
                "split": row["split"], "label": row["label"], "data_classification": row["data_classification"],
                "event": row["event"], "context": row["context"], "state": asdict(state),
                "capabilities": capabilities.model_dump(mode="json"),
                "thresholds": thresholds.model_dump(mode="json"), "participation": participation.model_dump(mode="json"),
                "judge_status": status, "signals": signals.model_dump(mode="json") if signals else None,
                "decision": decision.model_dump(mode="json"), "model_requests": len(recorded.requests),
                "decision_ms": elapsed, "provider_requests": recorded.requests, "metadata": metadata}
            trace_file.write(json.dumps(trace, ensure_ascii=False, default=str) + "\n")
            trace_file.flush()
            traces.append(trace)
            evaluated.append({**row, "signals": trace["signals"], "model_requests": trace["model_requests"],
                "decision_ms": elapsed, "thresholds": thresholds.model_dump(), "participation": participation.model_dump()})
    report = replay(evaluated)
    requests = [request for trace in traces for request in trace["provider_requests"]]
    measured_usage = [request["usage"] for request in requests]
    usage_total = None
    if measured_usage and all(isinstance(usage, dict) for usage in measured_usage):
        numeric = set.intersection(*({key for key, value in usage.items() if type(value) in (int, float)}
                                     for usage in measured_usage))
        usage_total = {key: sum(usage[key] for usage in measured_usage) for key in sorted(numeric)} or None
    report.update(real_provider_evaluation=bool(requests) and metadata.get("execution_channel") == "network",
        evaluation_kind="provider_judge_semantic_replay",
        metadata=metadata, model_requests_total=len(requests),
        usage_known_requests=sum(request["usage"] is not None for request in requests),
        usage_unknown_requests=sum(request["usage"] is None for request in requests),
        usage_total=usage_total, estimated_cost=None,
        judge_status_counts={status: sum(trace["judge_status"] == status for trace in traces)
                             for status in ("not_called", "success", "abstained")},
        by_split={name: replay([{**row, "split": "test"} for row in evaluated if row["split"] == name])
                  for name in sorted({row["split"] for row in evaluated})})
    for name, split_report in report["by_split"].items():
        split_requests = sum(trace["model_requests"] for trace in traces if trace["split"] == name)
        split_report.update(evaluation_kind=report["evaluation_kind"], model_requests_total=split_requests,
            real_provider_evaluation=bool(split_requests) and metadata.get("execution_channel") == "network")
    (output_directory / "metrics.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", type=Path)
    parser.add_argument("--config", type=Path, help="explicitly enable real judge endpoint requests using this YAML config")
    parser.add_argument("--output-dir", type=Path, help="new result directory for trace.jsonl and metrics.json")
    parser.add_argument("--split", choices=("development", "test"))
    args = parser.parse_args()
    rows = [json.loads(line) for line in args.dataset.read_text().splitlines() if line.strip()]
    if args.config:
        if args.output_dir is None:
            parser.error("--config requires --output-dir")
        settings = load_settings(args.config)
        report = asyncio.run(provider_replay(rows, settings, args.output_dir,
            run_metadata(settings, args.dataset), split=args.split))
    else:
        if args.output_dir or args.split:
            parser.error("--output-dir and --split require --config")
        report = replay(rows)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
