import json

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import delete, insert

from conftest import official_payload
from qqbot.app import create_app
from qqbot.config import Settings
from qqbot.domain.social import JudgmentSignals, SpeechDecision
from qqbot.permissions import PermissionPolicy
from qqbot.storage.schema import decision_logs, decisions, inbox
from qqbot.storage.social_schema import memory_source_retention, social_tombstones

AUTH = {"Authorization": "Bearer decision-log-admin"}


@pytest.fixture
def log_console(tmp_path, official_config, official_event, clock, monkeypatch):
    monkeypatch.setenv("QQBOT_ADMIN_TOKEN", "decision-log-admin")
    app = create_app(Settings(database=str(tmp_path / "core.db"), adapters=(official_config,)),
        clock=clock, run_background=False)
    core = app.state.core
    event = core.inbox.accept(core.adapters["qq"].normalize(official_payload(
        official_event, message_id=1, text="private source text"
    )))[0]
    with TestClient(app) as client:
        yield client, core, event


def append_log(core, event, *, action="IGNORE", context=None):
    decision = SpeechDecision(action=action, reason_codes=("LOW_RELEVANCE",),
        anchor_message_ids=(event.platform_message_id,), snapshot_seq=event.ingest_seq,
        signals=JudgmentSignals(useful_contribution=0.42, model_id="judge-fixture"), judge_model="judge-fixture")
    with core.db.transaction() as c:
        log_id = c.execute(insert(decision_logs).values(event_id=event.event_id,
            conversation_id=event.conversation.key, created_at=core.clock.now().isoformat(),
            decision_json=decision.model_dump_json(), context_json=json.dumps(context or {"legacy": True}),
        )).inserted_primary_key[0]
    return log_id, decision


def test_decision_logs_preserve_reevaluations_and_paginate_by_conversation(log_console, official_event):
    client, core, event = log_console
    cid = event.conversation.key
    url = "/api/v1/conversations/" + cid + "/decision-logs"
    first_id, _ = append_log(core, event, action="WAIT")
    second_id, latest = append_log(core, event, context={
        "thresholds": {"useful_contribution": 0.75}, "judge_status": "success", "judge_duration_ms": 123,
    })
    other = core.inbox.accept(core.adapters["qq"].normalize(official_payload(
        official_event, message_id=2, group_id=201
    )))[0]
    append_log(core, other)
    with core.db.transaction() as c:
        c.execute(insert(decisions).values(event_id=event.event_id, conversation_id=cid,
            created_at=core.clock.now().isoformat(), decision_json=latest.model_dump_json()))
    assert client.get(url).status_code == 401
    response = client.get(url + "?limit=1", headers=AUTH)
    assert response.status_code == 200
    page = response.json()
    assert page["next_before"] == second_id
    row = page["items"][0]
    assert row["id"] == second_id and row["event_id"] == event.event_id
    assert row["message_preview"] == "private source text" and row["inbox_state"] == "PENDING"
    assert row["decision"]["signals"]["useful_contribution"] == 0.42
    assert row["context"]["thresholds"] == {"useful_contribution": 0.75}
    older = client.get(url + "?limit=1&before=" + str(page["next_before"]), headers=AUTH).json()
    assert [item["id"] for item in older["items"]] == [first_id] and older["next_before"] is None
    assert older["items"][0]["context"] == {"legacy": True}
    assert client.get("/api/v1/conversations/" + cid + "/decisions", headers=AUTH).json() == [latest.model_dump(mode="json")]
    for query in ("?limit=0", "?limit=101", "?before=0"):
        assert client.get(url + query, headers=AUTH).status_code == 422
    assert client.get("/api/v1/conversations/missing/decision-logs", headers=AUTH).status_code == 404


@pytest.mark.parametrize("redaction", ["recall", "tombstone", "retention", "missing"])
def test_decision_log_preview_hides_revoked_or_cleaned_sources(log_console, redaction):
    client, core, event = log_console
    append_log(core, event)
    cid = event.conversation.key
    if redaction == "recall":
        core.inbox.accept(event.model_copy(update={
            "event_id": "recall-event", "event_type": "message.recalled", "segments": (),
        }))
    else:
        with core.db.transaction() as c:
            if redaction == "tombstone":
                c.execute(insert(social_tombstones).values(source_id=event.event_id, conversation_id=cid))
            elif redaction == "retention":
                c.execute(insert(memory_source_retention).values(source_id=event.event_id,
                    conversation_id=cid, redacted_at=core.clock.now().isoformat()))
            else:
                c.execute(delete(inbox).where(inbox.c.id == event.event_id))
    response = client.get("/api/v1/conversations/" + cid + "/decision-logs", headers=AUTH)
    assert response.status_code == 200 and response.json()["items"][0]["message_preview"] == ""
    assert "private source text" not in response.text
    assert response.json()["items"][0]["decision"]["signals"]["useful_contribution"] == 0.42


@pytest.mark.parametrize("action", ["history.read", "conversation.manage"])
def test_decision_logs_respect_history_and_conversation_permissions(log_console, action):
    client, core, event = log_console
    append_log(core, event)
    cid = event.conversation.key
    policy = PermissionPolicy.model_validate({"rules": [{
        "id": "deny-history", "effect": "deny", "actions": [action], "resources": ["conversation:" + cid],
    }]})
    core.permissions.set_policy(policy, expected_version=0, actor="fixture")
    assert client.get("/api/v1/conversations/" + cid + "/decision-logs", headers=AUTH).status_code == 403
