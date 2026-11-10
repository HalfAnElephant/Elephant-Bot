"""Provider replay wiring uses only mock HTTP, so these tests establish no semantic quality."""
import copy
import json
import runpy
from pathlib import Path

import httpx
import pytest

from qqbot.config import JudgeConfig, Settings
from qqbot.policy.judge import SIGNALS

replay_module = runpy.run_path("evals/social_replay.py")
provider_replay = replay_module["provider_replay"]
validate_provider_rows = replay_module["validate_provider_rows"]
DATASET = Path("evals/fixtures/social-semantic.jsonl")


@pytest.fixture
def rows():
    return [json.loads(line) for line in DATASET.read_text().splitlines()]


@pytest.fixture
def configured(monkeypatch):
    monkeypatch.setenv("REPLAY_TEST_KEY", "replay-key-secret")
    return Settings(judge=JudgeConfig(kind="structured", endpoint="https://judge.invalid", model="mock-model",
        api_key_env="REPLAY_TEST_KEY"))


def scores(**changes):
    return {**dict.fromkeys(SIGNALS, 0.0), "invites_participation": 0.95,
        "useful_contribution": 0.95, "proactive_allowed": 1.0, **changes}


def semantic(rows, split="test", name="public-invitation"):
    return next(row for row in rows if row["case_id"] == split + "-" + name)


async def test_structured_replay_counts_actual_requests_and_keeps_raw_trace(rows, configured, tmp_path):
    row = semantic(rows)
    sent = []
    async def request(packet):
        sent.append(json.loads(packet.content))
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(scores())}}],
            "usage": {"prompt_tokens": 11, "completion_tokens": 7, "total_tokens": 18}})
    async with httpx.AsyncClient(transport=httpx.MockTransport(request)) as client:
        result = await provider_replay([row], configured, tmp_path, {"execution_channel": "mock"}, client=client)
    assert len(sent) == result["model_requests_total"] == 1
    assert result["model_requests_per_100_messages"] == 100
    assert result["usage_known_requests"] == 1 and result["usage_unknown_requests"] == 0
    assert result["usage_total"]["total_tokens"] == 18
    assert result["estimated_cost"] is None and not result["real_provider_evaluation"]
    assert result["judge_status_counts"]["success"] == 1 and result["decision_p95_ms"] >= 0
    trace = json.loads((tmp_path / "trace.jsonl").read_text())
    assert trace["context"] == row["context"] and trace["label"] == row["label"]
    assert trace["provider_requests"][0]["response"]["usage"]["total_tokens"] == 18
    assert trace["signals"]["backend"] == "structured" and trace["decision"]["action"] == "SPEAK"
    assert json.loads((tmp_path / "metrics.json").read_text()) == result
    assert "label" not in json.loads(sent[0]["messages"][1]["content"])
    assert "replay-key-secret" not in (tmp_path / "trace.jsonl").read_text()


async def test_jev1_replay_counts_six_questions_usage_unknown_and_skips_invited(rows, configured, tmp_path):
    configured = configured.model_copy(update={"judge": configured.judge.model_copy(update={"kind": "jev", "model": "jev1"})})
    requests = []
    async def request(packet):
        requests.append(json.loads(packet.content))
        value = {"type": "boolean", "status": "answered", "value": "yes", "distribution": {"yes": 0.9, "no": 0.1}}
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(value)}}]})
    async with httpx.AsyncClient(transport=httpx.MockTransport(request)) as client:
        result = await provider_replay([semantic(rows), semantic(rows, name="explicit-at")], configured,
            tmp_path, {"execution_channel": "mock"}, client=client)
    assert len(requests) == result["model_requests_total"] == 6
    assert result["usage_unknown_requests"] == 6 and result["usage_total"] is None
    assert result["judge_status_counts"] == {"not_called": 1, "success": 1, "abstained": 0}
    traces = [json.loads(line) for line in (tmp_path / "trace.jsonl").read_text().splitlines()]
    assert traces[0]["signals"]["backend"] == "jev1" and len(traces[0]["provider_requests"]) == 6
    assert traces[1]["model_requests"] == 0 and traces[1]["signals"] is None
    assert len({json.loads(packet["messages"][0]["content"])["question"] for packet in requests}) == 6


async def test_legacy_jev_replay_uses_real_parser(rows, configured, tmp_path):
    configured = configured.model_copy(update={"judge": configured.judge.model_copy(update={"kind": "jev", "model": "jev-1.14.0"})})
    requests = []
    def request(packet):
        requests.append(json.loads(packet.content))
        return httpx.Response(200, json={"model": "jev-1.14.0", "answers": {
            name: {"noul": value} for name, value in scores().items()}})
    async with httpx.AsyncClient(transport=httpx.MockTransport(request)) as client:
        report = await provider_replay([semantic(rows)], configured, tmp_path, {}, client=client)
    assert set(requests[0]["questions"]) == set(SIGNALS)
    assert report["judge_status_counts"]["success"] == 1
    assert report["speech_precision"]["numerator"] == 1


@pytest.mark.parametrize("failure", ["timeout", "malformed"])
async def test_provider_failure_abstains_and_counts_circuit_breaker_requests(rows, configured, tmp_path, failure):
    batch = []
    for index in range(4):
        row = copy.deepcopy(semantic(rows))
        row["case_id"] = row["fragment_id"] = "failure-" + str(index)
        row["label"] = "should_ignore"
        batch.append(row)
    def request(packet):
        if failure == "timeout":
            raise httpx.ReadTimeout("fixture", request=packet)
        return httpx.Response(200, json={"choices": []})
    async with httpx.AsyncClient(transport=httpx.MockTransport(request)) as client:
        report = await provider_replay(batch, configured, tmp_path, {}, client=client)
    assert report["model_requests_total"] == 3
    assert report["judge_status_counts"]["abstained"] == 4
    assert report["prohibited_violations"] == 0
    assert report["usage_unknown_requests"] == 3
    traces = [json.loads(line) for line in (tmp_path / "trace.jsonl").read_text().splitlines()]
    assert all(trace["decision"]["action"] == "IGNORE" for trace in traces)
    assert traces[-1]["model_requests"] == 0


@pytest.mark.parametrize("invalid", ["privacy", "split", "conversation", "target", "secret", "future", "context-version"])
async def test_invalid_dataset_is_rejected_before_network(rows, configured, tmp_path, invalid):
    batch = [copy.deepcopy(semantic(rows))]
    if invalid == "privacy":
        del batch[0]["data_classification"]
    elif invalid in {"split", "conversation"}:
        other = copy.deepcopy(batch[0])
        other.update(case_id="other", fragment_id="other", split="development")
        if invalid == "conversation":
            other["group_id"] = "different-alias-for-the-same-conversation"
        batch.append(other)
    elif invalid == "target":
        batch[0]["context"]["messages"][-1]["text"] = "wrong target"
    elif invalid == "secret":
        batch[0]["context"]["participation_policy"] = "api_key=sk-secretsecretsecret"
    elif invalid == "future":
        batch[0]["context"]["messages"][-1]["at"] = "2099-01-01T00:00:00Z"
    else:
        batch[0]["context"]["version"] = "unknown"
    def request(_packet):
        raise AssertionError("invalid data was transmitted")
    async with httpx.AsyncClient(transport=httpx.MockTransport(request)) as client:
        with pytest.raises(ValueError):
            await provider_replay(batch, configured, tmp_path, {}, client=client)
    assert not (tmp_path / "trace.jsonl").exists()


def test_synthetic_dataset_is_partitioned_and_default_replay_is_unjudged(rows):
    validate_provider_rows(rows)
    assert len(rows) == 14
    assert {row["data_classification"] for row in rows} == {"synthetic"}
    assert sum(row["split"] == "development" for row in rows) == 7
    assert sum(row["split"] == "test" for row in rows) == 7
    assert all(row["signals"] is None for row in rows)
    report = replay_module["replay"](rows)
    assert report["unjudged_messages"] == 12
    assert report["model_requests_per_100_messages"] is None and not report["real_provider_evaluation"]


def test_metadata_records_actual_sources_config_and_skill_hashes(configured):
    metadata = replay_module["run_metadata"](configured, DATASET)
    assert len(metadata["dataset_sha256"]) == 64 and len(metadata["skills_source_version"]) == 64
    assert metadata["git_commit"] and metadata["source_sha256"]["src/qqbot/policy/judge.py"]
    assert metadata["judge_config"]["model"] == "mock-model" and metadata["qq_send_operations"] == 0
    assert "replay-key-secret" not in json.dumps(metadata)


async def test_replay_accepts_context_generated_by_core_contract(tmp_path, nap_config, nap_event, clock, rows):
    from qqbot.app import create_app
    from qqbot.domain.messages import ObservationMode

    def no_network(_packet):
        raise AssertionError("context construction made a provider request")
    async with httpx.AsyncClient(transport=httpx.MockTransport(no_network)) as client:
        app = create_app(Settings(database=str(tmp_path / "contract.db"), adapters=(nap_config,)),
            clock=clock, run_background=False, model_client=client)
        core = app.state.core
        try:
            original = core.adapters["nap"].normalize({**nap_event, "message_id": 901,
                "message": [{"type": "text", "data": {"text": "大家一起讨论一下递归吧"}}]})
            original = original.model_copy(update={"observation_mode": ObservationMode.FULL})
            event, _ = core.inbox.accept(original)
            row = copy.deepcopy(semantic(rows))
            row["event"] = event.model_dump(mode="json")
            row["context"] = core.judge_context(event)
            validate_provider_rows([row])
            assert row["context"]["target_message_id"] == event.platform_message_id
        finally:
            core.db.close()


async def test_unselected_private_split_is_also_rejected(rows, configured, tmp_path):
    unsafe = copy.deepcopy(semantic(rows, split="development"))
    del unsafe["data_classification"]
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _packet: pytest.fail("network"))) as client:
        with pytest.raises(ValueError, match="data_classification"):
            await provider_replay([unsafe, semantic(rows)], configured, tmp_path, {}, client=client, split="test")
    assert not (tmp_path / "trace.jsonl").exists()
