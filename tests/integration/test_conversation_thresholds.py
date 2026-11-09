import json

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from qqbot.app import create_app
from qqbot.config import Settings
from qqbot.domain import AdapterCapabilities, ObservationMode
from qqbot.domain.social import JudgmentSignals, ParticipationSettings, SpeechThresholds
from qqbot.permissions import PermissionPolicy
from qqbot.storage.schema import decision_logs

AUTH = {"Authorization": "Bearer threshold-admin"}


@pytest.fixture
def console(tmp_path, nap_config, nap_event, clock, monkeypatch):
    monkeypatch.setenv("QQBOT_ADMIN_TOKEN", "threshold-admin")
    settings = Settings(database=str(tmp_path / "core.db"), adapters=(nap_config,))
    app = create_app(settings, clock=clock, run_background=False)
    core = app.state.core
    event = core.inbox.accept(core.adapters["nap"].normalize(nap_event))[0]
    with TestClient(app) as client:
        yield client, core, event, settings


def test_threshold_override_persists_preserves_state_and_can_follow_defaults(console, clock):
    client, core, event, settings = console
    cid = event.conversation.key
    url = "/api/v1/conversations/" + cid + "/policy"
    defaults = core.settings.judge.thresholds.model_dump(mode="json")
    override = {**defaults, "useful_contribution": 0.9, "private_exchange": 0}
    core.save_state(cid, state_json=json.dumps({"custom": "keep", "wait": {"event_id": "e"}}))
    response = client.patch(url, headers=AUTH, json={"expected_version": 1, "speech_thresholds": override})
    assert response.status_code == 200
    assert response.json()["policy_version"] == 2
    assert response.json()["speech_thresholds"] == response.json()["effective_speech_thresholds"] == override
    assert response.json()["default_speech_thresholds"] == defaults
    assert json.loads(core.state(cid)["state_json"])["custom"] == "keep"
    assert client.patch(url, headers=AUTH, json={"expected_version": 1, "speech_thresholds": None}).status_code == 409
    assert core.policy_view(cid)["speech_thresholds"] == override
    assert client.patch(url, headers=AUTH, json={"expected_version": 2, "mode": "silent"}).status_code == 200
    assert core.policy_view(cid)["speech_thresholds"] == override
    restored = create_app(settings, clock=clock, run_background=False)
    try:
        policy = restored.state.core.policy_view(cid)
        assert policy["speech_thresholds"] == override and policy["mode"] == "silent"
        saved = json.loads(policy["state_json"])
        saved.update(silent_until=clock.now().isoformat(), silent_resume_mode="addressed_only")
        restored.state.core.save_state(cid, state_json=json.dumps(saved))
        restored.state.core.restore_silences()
        assert restored.state.core.policy_view(cid)["speech_thresholds"] == override
    finally:
        restored.state.core.db.close()
    version = core.state(cid)["policy_version"]
    response = client.patch(url, headers=AUTH, json={"expected_version": version, "speech_thresholds": None})
    assert response.status_code == 200 and response.json()["speech_thresholds"] is None
    assert "speech_thresholds" not in json.loads(core.state(cid)["state_json"])
    core.settings = settings.model_copy(update={"judge": settings.judge.model_copy(update={
        "thresholds": SpeechThresholds(useful_contribution=0.66),
    })})
    rows = client.get("/api/v1/conversations", headers=AUTH).json()
    policy = next(row["policy"] for row in rows if row["id"] == cid)
    assert policy["speech_thresholds"] is None
    assert policy["effective_speech_thresholds"]["useful_contribution"] == 0.66
    assert policy["effective_speech_thresholds"] == policy["default_speech_thresholds"]


def test_threshold_patch_rejects_invalid_payloads_private_chats_and_denied_permissions(console, nap_event):
    client, core, event, _ = console
    cid = event.conversation.key
    url = "/api/v1/conversations/" + cid + "/policy"
    defaults = core.settings.judge.thresholds.model_dump(mode="json")
    body = {"expected_version": 1, "speech_thresholds": defaults}
    assert client.patch(url, json=body).status_code == 401
    for patch in ({}, {"mode": None}, {"persona": None},
                  {"mode": "silent", "speech_thresholds": None},
                  {"speech_thresholds": {}},
                  {"speech_thresholds": {**defaults, "useful_contribution": 1.1}},
                  {"speech_thresholds": {**defaults, "useful_contribution": -0.1}},
                  {"speech_thresholds": {**defaults, "useful_contribution": "0.5"}},
                  {"speech_thresholds": {**defaults, "unknown": 0.5}}):
        assert client.patch(url, headers=AUTH, json={"expected_version": 1, **patch}).status_code == 422
    assert core.state(cid)["policy_version"] == 1
    private = core.inbox.accept(core.adapters["nap"].normalize({
        **{key: value for key, value in nap_event.items() if key != "group_id"},
        "message_type": "private", "message_id": 401,
    }))[0]
    assert client.patch("/api/v1/conversations/" + private.conversation.key + "/policy",
                        headers=AUTH, json=body).status_code == 422
    for action in ("conversation.policy.set", "conversation.manage"):
        core.permissions.set_policy(PermissionPolicy.model_validate({"rules": [{
            "id": "deny-threshold", "effect": "deny", "actions": [action],
            "resources": ["conversation:" + cid],
        }]}), expected_version=core.permissions.policy_version, actor="fixture")
        assert client.patch(url, headers=AUTH, json=body).status_code == 403
        assert core.policy_view(cid)["speech_thresholds"] is None


async def test_runtime_uses_group_thresholds_and_reevaluation_logs_effective_defaults(
    tmp_path, nap_config, nap_event, clock,
):
    settings = Settings(database=str(tmp_path / "core.db"), adapters=(nap_config,))
    settings = settings.model_copy(update={"judge": settings.judge.model_copy(update={
        "thresholds": SpeechThresholds(wait_useful_contribution=0.7),
    })})
    app = create_app(settings, clock=clock, run_background=False)
    core = app.state.core

    async def capabilities(target):
        return AdapterCapabilities(can_reply=True, can_observe_unaddressed_messages=True,
                                   observation_mode=ObservationMode.FULL)

    class Judge:
        async def judge(self, context):
            return JudgmentSignals(useful_contribution=0.6, private_exchange=0.1)

    core.adapters["nap"].capabilities = capabilities
    core.judge = Judge()
    try:
        for index in range(3):
            event = core.inbox.accept(core.adapters["nap"].normalize({
                **nap_event, "message_id": 500 + index,
                "message": [{"type": "text", "data": {"text": "human context"}}],
            }).model_copy(update={"observation_mode": ObservationMode.FULL}))[0]
            if index < 2:
                core.inbox.finish(event.event_id)
        cid = event.conversation.key
        core.save_state(cid, mode="social")
        core.set_policy_override(event.conversation, "speech_thresholds", SpeechThresholds(wait_useful_contribution=0.5),
                                   core.access(event), expected_version=2)
        await core.process(event)
        assert core.window(event).waiting is not None
        assert core.policy_view("other-group")["effective_speech_thresholds"]["wait_useful_contribution"] == 0.7
        core.set_policy_override(event.conversation, "speech_thresholds", None, core.access(event), expected_version=3)
        clock.advance(1)
        core.resume_waits()
        await core.process(event)
        assert core.window(event).waiting is None
        with core.db.engine.connect() as c:
            rows = c.execute(select(decision_logs).where(decision_logs.c.event_id == event.event_id)
                             .order_by(decision_logs.c.id)).mappings().all()
        assert [json.loads(row["decision_json"])["action"] for row in rows] == ["WAIT", "IGNORE"]
        assert [json.loads(row["context_json"])["thresholds"]["wait_useful_contribution"]
                for row in rows] == [0.5, 0.7]
    finally:
        await core.stop()
        core.db.close()


def test_participation_override_is_independent_versioned_and_restores_dynamic_defaults(console, clock):
    client, core, event, settings = console
    cid = event.conversation.key
    url = "/api/v1/conversations/" + cid + "/policy"
    defaults = core.settings.judge.participation.model_dump(mode="json")
    custom = {**defaults, "cooldown_seconds": 10, "require_member_response": False,
              "participation_budget_ratio": 0.4}
    thresholds = core.settings.judge.thresholds.model_dump(mode="json")
    assert client.patch(url, headers=AUTH, json={
        "expected_version": 1, "speech_thresholds": thresholds,
    }).status_code == 200
    response = client.patch(url, headers=AUTH, json={"expected_version": 2, "participation": custom})
    assert response.status_code == 200
    assert response.json()["policy_version"] == 3
    assert response.json()["participation"] == response.json()["effective_participation"] == custom
    assert response.json()["default_participation"] == defaults
    assert response.json()["speech_thresholds"] == thresholds
    assert core.participation_for(event.conversation).model_dump(mode="json") == custom
    assert client.patch(url, headers=AUTH, json={"expected_version": 2, "participation": None}).status_code == 409
    for value in ({}, {**custom, "cooldown_seconds": -1}, {**custom, "require_member_response": "false"},
                  {**custom, "activity_max_events": 0}, {**custom, "participation_budget_ratio": 1.01}):
        assert client.patch(url, headers=AUTH, json={"expected_version": 3, "participation": value}).status_code == 422
    assert client.patch(url, headers=AUTH, json={
        "expected_version": 3, "participation": custom, "speech_thresholds": None,
    }).status_code == 422
    restored = create_app(settings, clock=clock, run_background=False)
    try:
        assert restored.state.core.participation_for(event.conversation).model_dump(mode="json") == custom
    finally:
        restored.state.core.db.close()
    response = client.patch(url, headers=AUTH, json={"expected_version": 3, "participation": None})
    assert response.status_code == 200 and response.json()["participation"] is None
    assert response.json()["speech_thresholds"] == thresholds
    core.settings = settings.model_copy(update={"judge": settings.judge.model_copy(update={
        "participation": ParticipationSettings(cooldown_seconds=15),
    })})
    assert core.participation_for(event.conversation).cooldown_seconds == 15
    assert core.policy_view(cid)["effective_participation"]["cooldown_seconds"] == 15
    assert "participation" not in json.loads(core.state(cid)["state_json"])


@pytest.mark.parametrize("override,expected_reason,humans,judge_calls", [
    ({"activity_window_seconds": 120}, "OTHERS_ARE_FINISHING_TOPIC", 3, 1),
    ({"activity_window_seconds": 60}, "PARTICIPATION_LIMIT", 1, 0),
    ({"activity_window_seconds": 120, "participation_budget_ratio": 0}, "SOFT_PARTICIPATION_BUDGET", 3, 0),
    ({"activity_window_seconds": 120, "min_human_messages": 4}, "PARTICIPATION_LIMIT", 3, 0),
])
async def test_runtime_applies_group_participation_window_budget_and_gates(
    tmp_path, nap_config, nap_event, clock, override, expected_reason, humans, judge_calls,
):
    app = create_app(Settings(database=str(tmp_path / "participation.db"), adapters=(nap_config,)),
                     clock=clock, run_background=False)
    core = app.state.core
    calls = []

    async def capabilities(target):
        return AdapterCapabilities(can_reply=True, can_observe_unaddressed_messages=True,
                                   observation_mode=ObservationMode.FULL)

    class Judge:
        async def judge(self, context):
            calls.append(context)
            return JudgmentSignals(useful_contribution=0.6, private_exchange=0.1)

    core.adapters["nap"].capabilities = capabilities
    core.judge = Judge()
    try:
        for index in range(3):
            if index == 2:
                clock.advance(90)
            event = core.inbox.accept(core.adapters["nap"].normalize({
                **nap_event, "message_id": 900 + index, "time": int(clock.now().timestamp()),
                "message": [{"type": "text", "data": {"text": "human context"}}],
            }).model_copy(update={"observation_mode": ObservationMode.FULL}))[0]
            if index < 2:
                core.inbox.finish(event.event_id)
        core.save_state(event.conversation.key, mode="social")
        custom = ParticipationSettings(**override)
        core.set_policy_override(event.conversation, "participation", custom, core.access(event), expected_version=2)
        await core.process(event)
        with core.db.engine.connect() as connection:
            row = connection.execute(select(decision_logs).where(
                decision_logs.c.event_id == event.event_id,
            )).mappings().one()
        context = json.loads(row["context_json"])
        assert context["participation_rules"] == custom.model_dump(mode="json")
        assert context["policy_state"]["new_human_messages"] == humans
        assert json.loads(row["decision_json"])["reason_codes"] == [expected_reason]
        assert len(calls) == judge_calls
    finally:
        await core.stop()
        core.db.close()
