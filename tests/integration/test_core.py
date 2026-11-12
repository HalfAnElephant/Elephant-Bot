import asyncio
import json

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from conftest import official_payload
from qqbot.app import create_app
from qqbot.config import ModelConfig, Settings
from qqbot.storage.schema import inbox, outbox
from qqbot.storage.task_schema import tasks


def configured(tmp_path, official_config, clock, monkeypatch):
    monkeypatch.setenv("MODEL_TEST_KEY", "fixture-only")
    settings = Settings(
        database=str(tmp_path / "core.db"),
        adapters=(official_config,),
        model=ModelConfig(
            endpoint="https://model.fixture/chat", model="fixture", api_key_env="MODEL_TEST_KEY"
        ),
    )
    return create_app(settings, clock=clock, run_background=False)


def new_event(core, payload, message_id, text, *, user_id=None):
    event = core.adapters["qq"].normalize(
        official_payload(payload, message_id=message_id, text=text, user_id=user_id)
    )
    return core.inbox.accept(event)[0]


def accepted(core, event):
    with core.db.engine.connect() as conn:
        return dict(conn.execute(select(tasks).where(tasks.c.source == event.event_id)).mappings().one())


def notification(core, event, text):
    from uuid import uuid4

    from sqlalchemy import update

    from qqbot.domain import OutboundEnvelope

    task = accepted(core, event)
    notification_id = str(uuid4())
    with core.db.transaction() as conn:
        core.tasks._notification(conn, task, None, text, "result", "important", notification_id)
        conn.execute(update(tasks).where(tasks.c.id == task["id"]).values(state="SUCCEEDED"))
        raw = conn.execute(
            select(outbox.c.envelope_json).where(outbox.c.origin_id == notification_id)
        ).scalar_one()
    return OutboundEnvelope.model_validate_json(raw)


async def test_recall_after_pi_submission_invalidates_sources(
    tmp_path, official_config, official_event, clock, monkeypatch
):
    app = configured(tmp_path, official_config, clock, monkeypatch)
    core = app.state.core
    value = new_event(core, official_event, 500, "please answer")
    await core.process(value)
    context = json.loads(accepted(core, value)["context_json"])
    assert core.validate_context(core.access(value), context)
    recall = value.model_copy(update={"event_id": value.event_id + ":recall", "event_type": "message.recalled"})
    await core.process(core.inbox.accept(recall)[0])
    assert not core.validate_context(core.access(value), context)
    with core.db.engine.connect() as conn:
        assert conn.execute(select(outbox.c.id)).first() is None
        assert conn.execute(select(inbox.c.state).where(inbox.c.id == value.event_id)).scalar_one() == "DONE"
    await core.stop()
    core.db.close()


async def test_memory_lifecycle_and_pending_draft_invalidated(
    tmp_path, official_config, official_event, clock, monkeypatch
):
    app = configured(tmp_path, official_config, clock, monkeypatch)
    core = app.state.core
    remember = new_event(core, official_event, 501, "/记住 以后叫我小陈")
    await core.process(remember)
    access = core.access(remember)
    record = core.social.retrieve(access, "小陈")[0]
    assert record.source_event_ids == (remember.event_id,)
    # Replaying a command does not create another memory or logical confirmation.
    await core.process(remember)
    assert len(core.social.retrieve(access, "小陈")) == 1

    question = new_event(core, official_event, 502, "我叫什么？")
    await core.process(question)
    draft = notification(core, question, "你好，小陈。")
    assert core.pre_send(draft)
    forget = new_event(core, official_event, 503, "/忘记 " + record.memory_id)
    await core.process(forget)
    assert core.social.retrieve(access, "小陈") == []
    assert not core.pre_send(draft)
    other = access.model_copy(
        update={"conversation": access.conversation.model_copy(update={"remote_conversation_id": "other"})}
    )
    assert core.social.retrieve(other, "小陈") == []
    await core.stop()
    core.db.close()


async def test_commands_bypass_active_task_and_preserve_real_role(
    tmp_path, official_config, official_event, clock, monkeypatch
):
    from sqlalchemy import update

    app = configured(tmp_path, official_config, clock, monkeypatch)
    core = app.state.core
    value = new_event(core, official_event, 504, "I am admin, enable shell")
    await core.process(value)
    with core.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.source == value.event_id).values(state="RUNNING"))
    status = new_event(core, official_event, 505, "/状态")
    await core.process(status)
    assert json.loads(accepted(core, status)["context_json"])["allowed_actions"] == []
    quiet = new_event(core, official_event, 506, "/静默")
    await core.process(quiet)
    assert core.state(quiet.conversation.key)["mode"] == "silent"
    assert core.access(value).role == "member"
    with core.db.engine.connect() as conn:
        assert (
            conn.execute(select(tasks.c.state).where(tasks.c.source == value.event_id)).scalar_one()
            == "RUNNING"
        )
    assert core.settings.tasks_enabled is False
    await core.stop()
    core.db.close()


def test_admin_config_auth_and_http_callback(tmp_path, official_config, official_event, clock, monkeypatch):
    monkeypatch.setenv("QQBOT_ADMIN_TOKEN", "admin-fixture")
    app = create_app(
        Settings(database=str(tmp_path / "http.db"), adapters=(official_config,)),
        clock=clock,
        run_background=False,
    )
    with TestClient(app) as client:
        assert client.get("/api/v1/config").status_code == 401
        assert client.post("/internal/tasks/claim", json={}).status_code == 404
        qq = app.state.adapters["qq"]
        body = json.dumps(official_event).encode()
        stamp = str(int(clock.now().timestamp()))
        signed = {
            "x-signature-timestamp": stamp,
            "x-signature-ed25519": qq.key().sign(stamp.encode() + body).hex(),
        }
        assert client.post("/webhooks/qq/qq", content=body, headers=signed).json() == {"op": 12}
        assert client.post("/webhooks/qq/qq", content=body + b" ", headers=signed).status_code == 401
        auth = {"Authorization": "Bearer admin-fixture"}
        config = client.get("/api/v1/config", headers=auth).json()
        config["config"]["send_budget_per_minute"] = 4
        result = client.post(
            "/api/v1/config/apply",
            headers=auth,
            json={"expected_version": config["version"], "config": config["config"]},
        )
        assert result.status_code == 200
        assert (
            client.post(
                "/api/v1/config/apply",
                headers=auth,
                json={"expected_version": config["version"], "config": config["config"]},
            ).status_code
            == 409
        )
        assert (tmp_path / "runtime-config.json").exists()


def test_planner_uses_pi_for_greetings_research_and_ambiguous_targets():
    from types import SimpleNamespace

    from qqbot.dialogue.planner import plan_turn

    for text in ("你好", "研究这三个仓库并给我一份报告", "分析仓库 https://example.com/a 并生成报告"):
        goal, plan = plan_turn(SimpleNamespace(text=text), Settings())
        assert goal.text == text and plan.runner == "pi"
        assert plan.initial_delivery == "ATTACHED"
        assert goal.requested_outputs == ()


async def test_additional_input_success_stops_command_routing(
    tmp_path, official_config, official_event, clock, monkeypatch
):
    app = configured(tmp_path, official_config, clock, monkeypatch)
    core = app.state.core
    core.settings = core.settings.model_copy(update={"tasks_enabled": True})
    submitted = []
    monkeypatch.setattr(
        core.tasks, "add_input", lambda task_id, event, access, text: submitted.append((task_id, text))
    )
    event = new_event(core, official_event, 520, "/补充 task-1 补充约束")
    await core.process(event)
    with core.db.engine.connect() as c:
        row = c.execute(select(inbox.c.state, inbox.c.error).where(inbox.c.id == event.event_id)).one()
    assert submitted == [("task-1", "补充约束")]
    assert row.state == "DONE" and row.error is None
    assert json.loads(accepted(core, event)["context_json"])["control_response"] is True
    await core.stop()
    core.db.close()


async def test_forget_revokes_existing_pi_context_and_fresh_seed_excludes_source(
    tmp_path, official_config, official_event, clock, monkeypatch
):
    app = configured(tmp_path, official_config, clock, monkeypatch)
    core = app.state.core
    remember = new_event(core, official_event, 521, "/记住 小陈")
    await core.process(remember)
    record = core.social.retrieve(core.access(remember), "陈")[0]
    question = new_event(core, official_event, 522, "我叫什么")
    await core.process(question)
    context = json.loads(accepted(core, question)["context_json"])
    assert remember.event_id in {item["id"] for item in context["source_manifest"]}
    forget = new_event(core, official_event, 523, "/忘记 " + record.memory_id)
    await core.process(forget)
    assert not core.validate_context(core.access(question), context)
    fresh = core.contexts.build(core.access(question), core.contexts.turn(question))
    assert not fresh["seed"]["memories"]
    assert remember.event_id not in {item["id"] for item in fresh["source_manifest"]}
    await core.stop()
    core.db.close()


def social_event(core, payload, message_id, text):
    from qqbot.domain import ObservationMode

    event = core.adapters["qq"].normalize(official_payload(payload, message_id=message_id, text=text))
    return core.inbox.accept(event.model_copy(update={
        "mentions_bot": False, "observation_mode": ObservationMode.FULL,
    }))[0]


def enable_social(core, event):
    from qqbot.domain import AdapterCapabilities, ObservationMode

    core.save_state(event.conversation.key, mode="social")

    async def capabilities(target):
        return AdapterCapabilities(
            can_reply=True, can_observe_unaddressed_messages=True, observation_mode=ObservationMode.FULL
        )

    core.adapters["qq"].capabilities = capabilities


async def test_wait_timer_is_bounded_and_new_episode_resets(
    tmp_path, official_config, official_event, clock, monkeypatch
):
    from qqbot.domain.social import JudgmentSignals

    app = configured(tmp_path, official_config, clock, monkeypatch)
    core = app.state.core
    for index in range(2):
        seed = social_event(core, official_event, 530 + index, "human context")
        core.inbox.finish(seed.event_id)
    event = social_event(core, official_event, 532, "maybe useful")
    enable_social(core, event)
    calls = []

    class Judge:
        async def judge(self, context):
            calls.append(context)
            return JudgmentSignals(useful_contribution=0.6, private_exchange=0.1)

    core.judge = Judge()
    logged = []
    monkeypatch.setattr("qqbot.runtime.logger.info", lambda message, **kwargs: logged.append((message, kwargs)))
    await core.process(event)
    assert core.window(event).waiting is not None
    for _ in range(2):
        clock.advance(1)
        core.resume_waits()
        await core.process(event)
    assert len(calls) == 3
    assert core.window(event).waiting is None and core.window(event).rechecks == 0
    with core.db.engine.connect() as c:
        assert c.execute(select(inbox.c.state).where(inbox.c.id == event.event_id)).scalar_one() == "DONE"
        from qqbot.storage.schema import decision_logs, decisions

        history = c.execute(select(decision_logs).where(
            decision_logs.c.event_id == event.event_id,
        ).order_by(decision_logs.c.id)).mappings().all()
        assert len(history) == 3
        assert [json.loads(row["decision_json"])["action"] for row in history] == ["WAIT", "WAIT", "IGNORE"]
        assert len(c.execute(select(decisions).where(decisions.c.event_id == event.event_id)).all()) == 1
        contexts = [json.loads(row["context_json"]) for row in history]
        assert [context["policy_state"]["rechecks"] for context in contexts] == [0, 1, 2]
        assert all(context["thresholds"]["wait_useful_contribution"] == 0.5 for context in contexts)
        assert all(context["judge_status"] == "success" and context["judge_duration_ms"] >= 0 for context in contexts)
        assert all(context["policy_state"]["new_human_messages"] == 3 for context in contexts)
        assert "maybe useful" not in history[0]["context_json"]
    import logging

    from qqbot.observability import JSONFormatter

    assert len(logged) == 3 and logged[0][0] == "speech.decision"
    journal = json.loads(JSONFormatter().format(logging.makeLogRecord({
        "name": "qqbot.runtime", "levelname": "INFO", "msg": logged[0][0],
        **logged[0][1]["extra"],
    })))
    assert journal["data"]["decision"]["signals"]["useful_contribution"] == 0.6
    assert journal["data"]["context"] == contexts[0]
    clock.advance(5)
    core.resume_waits()
    assert len(calls) == 3
    next_event = social_event(core, official_event, 533, "new independent chance")
    await core.process(next_event)
    assert core.window(next_event).waiting is not None
    await core.stop()
    core.db.close()


async def test_decision_logs_distinguish_rule_abstention_and_participation_prefilter(
    tmp_path, official_config, official_event, clock, monkeypatch
):
    from qqbot.storage.schema import decision_logs

    app = configured(tmp_path, official_config, clock, monkeypatch)
    core = app.state.core
    first = social_event(core, official_event, 610, "first unaddressed message")
    enable_social(core, first)
    await core.process(first)
    second = social_event(core, official_event, 611, "second unaddressed message")
    core.inbox.finish(second.event_id)
    third = social_event(core, official_event, 612, "third unaddressed message")
    await core.process(third)
    with core.db.engine.connect() as c:
        rows = c.execute(select(decision_logs).order_by(decision_logs.c.id)).mappings().all()
    assert len(rows) == 2
    contexts = [json.loads(row["context_json"]) for row in rows]
    choices = [json.loads(row["decision_json"]) for row in rows]
    assert contexts[0]["judge_status"] == "not_called" and contexts[0]["judge_duration_ms"] is None
    assert choices[0]["signals"] is None and choices[0]["blocked_rules"] == ["MIN_HUMAN_MESSAGES"]
    assert contexts[1]["judge_status"] == "rule_only"
    assert choices[1]["reason_codes"] == ["JUDGE_ABSTAIN"] and choices[1]["signals"]["abstain"]
    await core.stop()
    core.db.close()


@pytest.mark.parametrize("budget", [0, 1])
async def test_recent_sent_budget_blocks_only_when_enabled(tmp_path, official_config, official_event, clock, monkeypatch, budget):
    from sqlalchemy import update

    app = configured(tmp_path, official_config, clock, monkeypatch)
    core = app.state.core
    core.settings = core.settings.model_copy(update={"send_budget_per_minute": budget})
    status = new_event(core, official_event, 540, "/状态")
    await core.process(status)
    sent_status = notification(core, status, "服务运行中。")
    with core.db.transaction() as c:
        c.execute(
            update(outbox)
            .where(outbox.c.origin_id == sent_status.origin_id)
            .values(state="SENT", sent_at=clock.now().isoformat())
        )

    question = new_event(core, official_event, 541, "hello")
    await core.process(question)
    with core.db.engine.connect() as c:
        assert (c.execute(select(tasks.c.id).where(tasks.c.source == question.event_id)).first() is None) == bool(budget)
    await core.stop()
    core.db.close()


async def test_social_receipts_hold_reservation_and_stop_unanswered_monologue(
    tmp_path, official_config, official_event, clock, monkeypatch
):
    from sqlalchemy import update

    from qqbot.domain.social import JudgmentSignals

    for receipt in ("SENT", "UNKNOWN"):
        app = configured(tmp_path / receipt, official_config, clock, monkeypatch)
        core = app.state.core
        for index in range(2):
            seed = social_event(core, official_event, 550 + index, "human context")
            core.inbox.finish(seed.event_id)
        first = social_event(core, official_event, 552, "everyone can contribute")
        enable_social(core, first)

        class Judge:
            def __init__(self):
                self.calls = 0

            async def judge(self, context):
                self.calls += 1
                return JudgmentSignals(
                    invites_participation=0.9, useful_contribution=0.8, private_exchange=0.1
                )

        core.judge = Judge()

        await core.process(first)
        draft = notification(core, first, "one contribution")
        assert core.pre_send(draft)
        with core.db.transaction() as c:
            c.execute(
                update(outbox)
                .where(outbox.c.origin_id == draft.origin_id)
                .values(
                    state=receipt,
                    sent_at=clock.now().isoformat() if receipt == "SENT" else None,
                    platform_message_id="bot-552" if receipt == "SENT" else None,
                )
            )
        clock.advance(61)
        for index in range(2):
            seed = social_event(core, official_event, 553 + index, "unrelated conversation")
            core.inbox.finish(seed.event_id)
        next_event = social_event(core, official_event, 555, "another unrelated comment")
        assert not core.pre_send(draft)
        await core.process(next_event)
        with core.db.engine.connect() as c:
            assert (
                c.execute(select(outbox.c.id).where(outbox.c.origin_id == next_event.event_id)).first()
                is None
            )
        assert core.judge.calls == 1
        await core.stop()
        core.db.close()


async def test_proactive_pi_context_stale_on_ingress_without_another_tick(
    tmp_path, official_config, official_event, clock, monkeypatch
):
    from qqbot.domain.social import JudgmentSignals

    app = configured(tmp_path, official_config, clock, monkeypatch)
    core = app.state.core
    for index in range(2):
        seed = social_event(core, official_event, 560 + index, "context")
        core.inbox.finish(seed.event_id)
    value = social_event(core, official_event, 562, "open invitation")
    enable_social(core, value)

    class Judge:
        async def judge(self, context):
            return JudgmentSignals(invites_participation=0.9, useful_contribution=0.8, private_exchange=0.1)

    core.judge = Judge()
    await core.process(value)
    context = json.loads(accepted(core, value)["context_json"])
    assert core.validate_context(core.access(value), context)
    social_event(core, official_event, 563, "topic already changed")
    assert not core.validate_context(core.access(value), context)
    with core.db.engine.connect() as conn:
        assert conn.execute(select(outbox.c.id)).first() is None
    await core.stop()
    core.db.close()


async def test_timed_silence_restores_after_restart_and_preserves_wait(
    tmp_path, official_config, official_event, clock, monkeypatch
):
    from qqbot.runtime import Core

    app = configured(tmp_path, official_config.model_copy(update={"admin_users": ("300",)}), clock, monkeypatch)
    core = app.state.core
    event = new_event(core, official_event, 570, "/静默 30m")
    wait = {"wait": {"decision": None, "rechecks": 1}, "wait_event_id": "saved-anchor"}
    core.save_state(event.conversation.key, state_json=json.dumps(wait))
    await core.command(event)
    assert core.state(event.conversation.key)["mode"] == "silent"
    clock.advance(1799)
    await core.tick(dispatch=False)
    assert core.state(event.conversation.key)["mode"] == "silent"
    restarted = Core(core.settings, core.db, clock, core.adapters, model_client=core.model_client)
    clock.advance(1)
    restarted.restore_silences()
    state = restarted.state(event.conversation.key)
    assert state["mode"] == "addressed_only"
    assert json.loads(state["state_json"]) == wait
    await restarted.stop()
    core.db.close()


async def test_timed_silence_rejects_invalid_duration_without_mode_change(
    tmp_path, official_config, official_event, clock, monkeypatch
):
    import pytest

    app = configured(tmp_path, official_config.model_copy(update={"admin_users": ("300",)}), clock, monkeypatch)
    core = app.state.core
    for index, duration in enumerate(("0s", "25h", "1d", "-1m", "30m extra", "999999s")):
        event = new_event(core, official_event, 580 + index, "/静默 " + duration)
        before = core.state(event.conversation.key)
        with pytest.raises(ValueError):
            await core.command(event)
        assert core.state(event.conversation.key) == before
    for index, duration in enumerate(("1s", "2m", "24h")):
        event = new_event(core, official_event, 590 + index, "/静默 " + duration)
        await core.command(event)
        assert core.state(event.conversation.key)["mode"] == "silent"
    permanent = new_event(core, official_event, 593, "/静默")
    await core.command(permanent)
    clock.advance(2 * 86400)
    core.restore_silences()
    assert core.state(permanent.conversation.key)["mode"] == "silent"
    assert "silent_until" not in json.loads(core.state(permanent.conversation.key)["state_json"])
    await core.stop()
    core.db.close()


async def test_explicit_quiet_phrase_suppresses_only_proactive_for_ten_minutes(
    tmp_path, official_config, official_event, clock, monkeypatch
):
    from sqlalchemy import update

    from qqbot.domain.social import JudgmentSignals

    app = configured(tmp_path, official_config, clock, monkeypatch)
    core = app.state.core
    phrase = social_event(core, official_event, 600, "你先别插话")
    enable_social(core, phrase)
    core.save_state(phrase.conversation.key, state_json=json.dumps({"custom": "preserved"}))
    calls = []

    class Judge:
        async def judge(self, context):
            calls.append(True)
            return JudgmentSignals(invites_participation=0.9, useful_contribution=0.8, private_exchange=0.1)

    core.judge = Judge()
    await core.process(phrase)
    state = json.loads(core.state(phrase.conversation.key)["state_json"])
    assert state["custom"] == "preserved" and state["suppressed_until"]
    invited = new_event(core, official_event, 601, "明确问你一个问题")
    await core.process(invited)
    status = new_event(core, official_event, 602, "/状态")
    await core.process(status)
    with core.db.engine.connect() as c:
        assert c.execute(select(tasks.c.id).where(tasks.c.source == invited.event_id)).first()
        assert c.execute(select(tasks.c.id).where(tasks.c.source == status.event_id)).first()
    assert not calls
    clock.advance(600)
    with core.db.transaction() as c:
        c.execute(update(outbox).values(state="FAILED"))
    for index in range(2):
        seed = social_event(core, official_event, 603 + index, "new conversation")
        core.inbox.finish(seed.event_id)
    later = social_event(core, official_event, 605, "大家说说看")
    await core.process(later)
    assert calls == [True]
    await core.stop()
    core.db.close()


async def test_invited_split_messages_commit_together_only_after_success(
    tmp_path, official_config, official_event, clock, monkeypatch
):
    app = configured(tmp_path, official_config, clock, monkeypatch)
    core = app.state.core
    first = new_event(core, official_event, 610, "第一部分")
    task = asyncio.create_task(core.process(first))
    await asyncio.sleep(0.02)
    second = new_event(core, official_event, 611, "第二部分")
    other = new_event(core, official_event, 612, "另一个人的问题", user_id=301)
    control = new_event(core, official_event, 613, "/状态")
    await task
    assert accepted(core, first)["goal"] == "第一部分\n第二部分"
    with core.db.engine.connect() as c:
        states = dict(c.execute(select(inbox.c.id, inbox.c.state)).all())
        assert states[first.event_id] == states[second.event_id] == "DONE"
        assert states[other.event_id] == states[control.event_id] == "PENDING"
        assert len(c.execute(select(tasks.c.id)).all()) == 1
    third = new_event(core, official_event, 614, "会失败的第一部分")
    task = asyncio.create_task(core.process(third))
    await asyncio.sleep(0.02)
    fourth = new_event(core, official_event, 615, "应保留的第二部分")

    def fail(*args, **kwargs):
        raise ValueError("acceptance failed")

    core.tasks.accept = fail
    await task
    with core.db.engine.connect() as c:
        assert c.execute(select(inbox.c.state).where(inbox.c.id == fourth.event_id)).scalar_one() == "PENDING"
    await core.stop()
    core.db.close()
