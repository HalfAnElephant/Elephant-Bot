import json

import pytest

from qqbot.app import create_app
from qqbot.config import Settings


@pytest.mark.parametrize(
    ("text", "outputs", "checks"),
    [
        ("研究这两种方案，给我报告结论", (), ()),
        ("生成 report.md", ("report.md",), ("artifact",)),
        ("请生成report.md", ("report.md",), ("artifact",)),
        ("生成 `生成记录.md`", ("生成记录.md",), ("artifact",)),
        ("导出到 reports/季度总结.pdf 文件", ("reports/季度总结.pdf",), ("artifact",)),
        ("给我一份 PDF 文件", (), ("artifact",)),
        ("帮我查嘉定今天的天气", (), ("observation",)),
        ("解释天气形成的原理", (), ()),
        ("帮我提交 PR", (), ("effect",)),
        ("看看这个 PR，没问题就 approve", (), ()),
    ],
)
def test_planner_preserves_explicit_output_and_success_contracts(official_config, clock, text, outputs, checks):
    from qqbot.dialogue.planner import plan_turn
    from qqbot.domain import ConversationRef, PrincipalRef, Surface
    from qqbot.domain.execution import TurnInput

    turn = TurnInput(
        conversation=ConversationRef(
            deployment_id="d",
            adapter_instance_id="qq",
            bot_identity_id="bot",
            surface=Surface.GROUP,
            remote_conversation_id="200",
        ),
        principal=PrincipalRef(adapter_instance_id="qq", identity_namespace="qq_app:app", remote_user_id="300"),
        text=text,
        source_event_ids=("e",),
        anchor_event_id="e",
    )
    goal, plan = plan_turn(turn, Settings())
    assert goal.requested_outputs == outputs
    assert goal.success_checks == checks
    assert plan.runner == "pi"


@pytest.mark.parametrize("changes", [
    {"keep_recent_tokens": 30000},
    {"max_tokens": 32000},
    {"max_tokens": 1000, "keep_recent_tokens": 2000},
    {"compaction_output_tokens": 32768},
])
def test_config_rejects_inconsistent_context_budgets(changes):
    from pydantic import ValidationError

    values = Settings().model_dump(mode="json")
    values["context"].update(changes)
    with pytest.raises(ValidationError):
        Settings.model_validate_json(json.dumps(values))


@pytest.mark.parametrize("window", [32768, 1000000])
def test_native_compaction_scales_with_actual_model_window(window):
    from qqbot.config import ModelConfig
    from qqbot.harness.pi import compaction_settings

    settings = Settings(model=ModelConfig(context_window=window))
    output = max(settings.execution.budget.output_tokens_per_call or 0,
                 settings.execution.interactive_budget.output_tokens_per_call or 0,
                 settings.context.compaction_output_tokens)
    limits = settings.context.limits(window, output)
    assert limits["soft_compaction_input_tokens"] == window - max((window + 9) // 10, output + 1024)
    assert limits["keep_recent_tokens"] == window // 5
    native = compaction_settings(window, limits, output)
    assert native["reserveTokens"] == window - limits["soft_compaction_input_tokens"]
    assert native["keepRecentTokens"] == limits["keep_recent_tokens"]


def test_config_allows_explicit_threshold_with_output_headroom():
    values = Settings().model_dump(mode="json")
    values["context"].update(max_tokens=23552, keep_recent_tokens=4000)
    values["execution"]["budget"]["output_tokens_per_call"] = 8192
    settings = Settings.model_validate_json(json.dumps(values))
    assert settings.context.max_tokens + settings.execution.budget.output_tokens_per_call + 1024 == settings.model.context_window


@pytest.mark.parametrize("interactive", [True, False])
def test_status_identifies_single_daemon_pi_configuration(
    tmp_path, official_config, clock, monkeypatch, interactive
):
    from fastapi.testclient import TestClient

    from qqbot.config import ModelConfig

    monkeypatch.setenv("QQBOT_ADMIN_TOKEN", "admin-test-token")
    monkeypatch.setenv("CORE_ONLY_MODEL_KEY", "fixture-only-core-key")
    app = create_app(
        Settings(
            database=str(tmp_path / "core.db"),
            adapters=(official_config,),
            interactive_execution_enabled=interactive,
            model=ModelConfig(
                model="core-metadata",
                endpoint="https://model.invalid/chat",
                api_key_env="CORE_ONLY_MODEL_KEY",
            ),
        ),
        clock=clock,
        run_background=False,
    )
    with TestClient(app) as client:
        response = client.get("/api/v1/status", headers={"Authorization": "Bearer admin-test-token"})
        assert response.status_code == 200
        body = response.json()
        assert body["model_configured"] is True
        assert body["model_execution"] == ("pi" if interactive else "disabled")


def test_ambiguous_displayed_ordinal_does_not_choose_an_invented_target(clock):
    from types import SimpleNamespace

    from qqbot.context.topics import displayed_references, select_topic

    references = displayed_references(
        "2. https://github.com/org/one/pull/52\n2. https://github.com/org/two/pull/99",
        "display-1",
        clock.now().isoformat(),
        (),
    )
    item = {
        "id": "topic-1",
        "kind": "github",
        "entities": [],
        "tokens": [],
        "source_ids": [],
        "last_at": clock.now().isoformat(),
        "displays": [{"source_id": "display-1", "references": references}],
    }
    index = {"active": "topic-1", "items": [item]}
    turn = SimpleNamespace(
        text="回到刚才第二个 PR",
        anchor_event_id="source-2",
        source_event_ids=("source-2",),
        conversation=SimpleNamespace(key="conversation"),
    )
    selected, reference, reason = select_topic(index, turn, clock.now(), 1800)
    assert selected["id"] == "topic-1" and reference is None and reason == "ambiguous_display_ordinal"


def test_rain_followup_does_not_create_a_fictional_location(clock):
    from types import SimpleNamespace

    from qqbot.context.topics import entities, select_topic

    assert entities("那明天会下雨吗") == ("weather", [])
    weather = {
        "id": "weather-1",
        "kind": "weather",
        "entities": ["location:嘉定"],
        "tokens": [],
        "source_ids": ["e1"],
        "displays": [],
        "last_at": clock.now().isoformat(),
    }
    index = {"active": "weather-1", "items": [weather]}
    turn = SimpleNamespace(
        text="那明天会下雨吗",
        anchor_event_id="e2",
        source_event_ids=("e2",),
        conversation=SimpleNamespace(key="conversation"),
    )
    selected, reference, reason = select_topic(index, turn, clock.now(), 1800)
    assert selected["id"] == "weather-1" and selected["entities"] == ["location:嘉定"]
    assert reference is None and reason == "matching_subject"
