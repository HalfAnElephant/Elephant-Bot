import asyncio
import json

import pytest
from sqlalchemy import select, update

from qqbot.app import create_app
from qqbot.config import Settings
from qqbot.permissions import PermissionRule
from qqbot.storage.schema import inbox, outbox
from qqbot.storage.task_schema import tasks


def event(core, payload, message_id, text):
    value = core.adapters["nap"].normalize(
        {
            **payload,
            "message_id": message_id,
            "message": [{"type": "at", "data": {"qq": "100"}}, {"type": "text", "data": {"text": text}}],
        }
    )
    return core.inbox.accept(value)[0]


def core_for(tmp_path, nap_config, clock, **settings):
    core = create_app(
        Settings(database=str(tmp_path / "core.db"), adapters=(nap_config,), **settings),
        clock=clock,
        run_background=False,
    ).state.core
    core.adapters["nap"].account_online = True
    return core


def image_event(core, payload, message_id, url=None):
    value = core.adapters["nap"].normalize({
        **payload, "message_id": message_id,
        "message": [{"type": "image", "data": {"file": "image.png", "url": url}}],
    })
    return core.inbox.accept(value)[0]


async def test_images_survive_image_only_and_merged_turns_without_leaking_urls_to_seed(
    tmp_path, nap_config, nap_event, clock
):
    core = core_for(tmp_path, nap_config, clock)
    picture = image_event(core, nap_event, 1800, "https://example.com/secret-image.png")
    turn = core.contexts.turn(picture)
    assert turn.text == "[图片]"
    context = core.contexts.build(core.access(picture), turn)
    assert context["images"] == [{
        "source": picture.event_id, "message_id": "1800", "url": "https://example.com/secret-image.png",
    }]
    assert "secret-image" not in json.dumps(context["seed"])
    assert core.validate_context(core.access(picture), context)
    text = event(core, nap_event, 1801, "这张图写了什么")
    unresolved = image_event(core, nap_event, 1802)
    merged = core.contexts.turn(picture, (text, unresolved))
    assert merged.text == "[图片]\n这张图写了什么\n[图片]"
    context = core.contexts.build(core.access(picture), merged)
    assert [item["source"] for item in context["images"]] == [picture.event_id, unresolved.event_id]
    assert context["images"][1]["url"] is None
    assert {item["source"] for item in context["images"]}.issubset(
        {item["id"] for item in context["source_manifest"]}
    )
    await core.stop()
    core.db.close()


async def test_reply_restores_image_outside_recent_history_and_respects_revocation(
    tmp_path, nap_config, nap_event, clock
):
    core = core_for(tmp_path, nap_config, clock, context={"recent_messages": 1})
    picture = image_event(core, nap_event, 1810, "https://example.com/old.png")
    later = event(core, nap_event, 1811, "https://github.com/org/repo 的 PR")
    without_reply = core.contexts.build(core.access(later), core.contexts.turn(later))
    assert without_reply["images"] == []
    reply = later.model_copy(update={"reply_to": picture.platform_message_id})
    restored = core.contexts.build(core.access(reply), core.contexts.turn(reply))
    assert restored["images"][0]["source"] == picture.event_id
    assert picture.event_id in {item["source"] for item in restored["seed"]["history"]}
    assert core.validate_context(core.access(reply), restored)
    core.social.revoke_source(core.access(picture), picture.event_id)
    assert not core.validate_context(core.access(reply), restored)
    fresh = core.contexts.build(core.access(reply), core.contexts.turn(reply))
    assert fresh["images"] == []
    await core.stop()
    core.db.close()


async def test_image_history_obeys_source_permissions_and_notice_only(
    tmp_path, nap_config, nap_event, clock
):
    core = core_for(tmp_path, nap_config, clock)
    picture = image_event(core, nap_event, 1820, "https://example.com/denied.png")
    text = event(core, nap_event, 1821, "看看上面的图片")
    context = core.contexts.build(core.access(text), core.contexts.turn(text))
    assert context["images"][0]["source"] == picture.event_id
    notice = core.contexts.build(core.access(text), core.contexts.turn(text), notice_only=True)
    assert notice["images"] == []
    core.permissions.upsert(PermissionRule(
        id="deny-image", effect="deny", actions=("history.source.read",),
        resources=("event:" + picture.event_id,),
    ), expected_version=0, actor="test")
    denied = core.contexts.build(core.access(text), core.contexts.turn(text))
    assert denied["images"] == []
    await core.stop()
    core.db.close()


async def test_process_image_only_private_message_creates_image_task(
    tmp_path, nap_config, nap_event, clock
):
    core = core_for(tmp_path, nap_config, clock)
    payload = {key: value for key, value in nap_event.items() if key != "group_id"}
    payload["message_type"] = "private"
    picture = image_event(core, payload, 1830, "https://example.com/private.png")
    await core.process(picture)
    with core.db.engine.connect() as conn:
        task = conn.execute(select(tasks)).mappings().one()
        received = conn.execute(select(inbox.c.state, inbox.c.error)).one()
    assert task["goal"] == "[图片]"
    assert task["source"] == picture.event_id
    assert received == ("DONE", None)
    context = json.loads(task["context_json"])
    assert context["images"] == [{
        "source": picture.event_id, "message_id": "1830", "url": "https://example.com/private.png",
    }]
    assert core.validate_context(core.access(picture), context)
    await core.stop()
    core.db.close()


async def test_process_coalesces_invited_text_and_image_into_one_task(
    tmp_path, nap_config, nap_event, clock
):
    core = core_for(tmp_path, nap_config, clock)
    text = event(core, nap_event, 1831, "帮我读一下这张截图")
    processing = asyncio.create_task(core.process(text))
    await asyncio.sleep(0.02)
    picture = image_event(core, nap_event, 1832, "https://example.com/screenshot.png")
    await processing
    with core.db.engine.connect() as conn:
        task = conn.execute(select(tasks)).mappings().one()
        received = conn.execute(select(inbox.c.state, inbox.c.error)).all()
    assert task["goal"] == "帮我读一下这张截图\n[图片]"
    assert received == [("DONE", None), ("DONE", None)]
    context = json.loads(task["context_json"])
    assert context["turn"]["source_event_ids"] == [text.event_id, picture.event_id]
    assert context["images"] == [{
        "source": picture.event_id, "message_id": "1832", "url": "https://example.com/screenshot.png",
    }]
    assert core.validate_context(core.access(text), context)
    await core.stop()
    core.db.close()


async def test_normal_and_merged_tool_request_use_same_pi_entrypoint(tmp_path, nap_config, nap_event, clock):
    core = core_for(tmp_path, nap_config.model_copy(update={"allowed_groups": ()}), clock)
    assert not hasattr(core, "generate")
    first = event(core, nap_event, 800, "帮我查询")
    processing = asyncio.create_task(core.process(first))
    await asyncio.sleep(0.02)
    second = event(core, nap_event, 801, "https://github.com/org/repo 的 PR")
    await processing
    with core.db.engine.connect() as conn:
        task = conn.execute(select(tasks)).mappings().one()
        assert conn.execute(select(outbox.c.id)).first() is None
        states = dict(conn.execute(select(inbox.c.id, inbox.c.state)).all())
    assert task["goal"] == "帮我查询\nhttps://github.com/org/repo 的 PR"
    assert task["profile"] == "pi" and task["delivery"] == "ATTACHED"
    assert states[first.event_id] == states[second.event_id] == "DONE"
    context = json.loads(task["context_json"])
    assert context["notify_before_tools"] is True
    assert context["turn"]["source_event_ids"] == [first.event_id, second.event_id]
    assert core.validate_context(core.access(first), context)
    await core.process(first)
    with core.db.engine.connect() as conn:
        assert len(conn.execute(select(tasks.c.id)).all()) == 1
    clock.advance(6)
    core.tasks.detach_due()
    with core.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.delivery)).scalar_one() == "DETACHED"
    await core.stop()
    core.db.close()


async def test_disabled_operation_notice_reaches_task_context(tmp_path, nap_config, nap_event, clock):
    core = core_for(tmp_path, nap_config, clock, execution={"notify_before_tools": False})
    value = event(core, nap_event, 899, "查询现在的 Issue")
    await core.process(value)
    with core.db.engine.connect() as conn:
        context = json.loads(conn.execute(select(tasks.c.context_json)).scalar_one())
    assert context["notify_before_tools"] is False
    await core.stop()
    core.db.close()


async def test_disabled_interactive_has_no_direct_model_fallback(tmp_path, nap_config, nap_event, clock):
    core = core_for(tmp_path, nap_config, clock, interactive_execution_enabled=False)
    value = event(core, nap_event, 802, "你好")
    await core.process(value)
    with core.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.id)).first() is None
        assert conn.execute(select(outbox.c.id)).first() is None
        assert conn.execute(select(inbox.c.error)).scalar_one() == "ValueError"
    await core.stop()
    core.db.close()


async def test_context_delivered_assistant_scope_and_permission_revocation(
    tmp_path, nap_config, nap_event, clock
):
    core = core_for(tmp_path, nap_config, clock)
    first = event(core, nap_event, 803, "之前的问题")
    with core.db.transaction() as conn:
        sent = core.outbox.enqueue_text(
            conn, target=first.conversation, text="已送达的回答", origin_kind="chat", origin_id=first.event_id
        )[0]
        conn.execute(
            update(outbox).where(outbox.c.id == sent).values(state="SENT", sent_at=clock.now().isoformat())
        )
        core.outbox.enqueue_text(
            conn, target=first.conversation, text="未送达的草稿", origin_kind="chat", origin_id="pending"
        )
    second = event(core, nap_event, 804, "继续")
    access = core.access(second)
    context = core.contexts.build(access, core.contexts.turn(second))
    texts = [item["text"] for item in context["seed"]["history"]]
    assert "已送达的回答" in texts and "未送达的草稿" not in texts
    history = context["seed"]["history"]
    bot_reply = next(item for item in history if item["text"] == "已送达的回答")
    member_message = next(item for item in history if item["source"] == second.event_id)
    assert bot_reply["speaker"] == "bot"
    assert member_message["speaker"].startswith("member_") and member_message["speaker"] != "bot"
    assert member_message["message_id"] == second.platform_message_id
    assert member_message["reply_to"] == second.reply_to
    assert core.validate_context(access, context)
    core.permissions.upsert(
        PermissionRule(
            id="revoke-source",
            effect="deny",
            actions=("history.source.read",),
            resources=("event:" + first.event_id,),
        ),
        expected_version=0,
        actor="test",
    )
    assert not core.validate_context(access, context)
    fresh = core.contexts.build(access, core.contexts.turn(second))
    assert first.event_id not in {item["id"] for item in fresh["source_manifest"]}
    assert fresh["key"] != context["key"]
    other = access.model_copy(
        update={"conversation": access.conversation.model_copy(update={"remote_conversation_id": "other"})}
    )
    assert not core.validate_context(other, fresh)
    await core.stop()
    core.db.close()


async def test_context_topic_epoch_and_recalled_anchor(tmp_path, nap_config, nap_event, clock):
    core = core_for(tmp_path, nap_config, clock)
    first = event(core, nap_event, 805, "你好")
    access = core.access(first)
    original = core.contexts.build(access, core.contexts.turn(first))
    second = event(core, nap_event, 806, "继续说")
    continued = core.contexts.build(core.access(second), core.contexts.turn(second))
    assert original["key"] == continued["key"]
    clock.advance(1801)
    third = event(core, nap_event, 807, "新的话题")
    fresh = core.contexts.build(core.access(third), core.contexts.turn(third))
    assert fresh["topic"] != original["topic"]
    core.social.revoke_source(access, first.event_id)
    assert not core.validate_context(access, original)
    with pytest.raises(ValueError, match="unavailable or revoked"):
        core.contexts.build(access, core.contexts.turn(first))
    await core.stop()
    core.db.close()


async def test_member_cancel_happens_before_pi_fact_explanation(tmp_path, nap_config, nap_event, clock):
    core = core_for(tmp_path, nap_config, clock)
    first = event(core, nap_event, 808, "你好")
    await core.process(first)
    with core.db.engine.connect() as conn:
        original = conn.execute(select(tasks)).mappings().one()
    cancel = event(core, nap_event, 809, "/取消 " + original["public_id"])
    await core.process(cancel)
    with core.db.engine.connect() as conn:
        original = conn.execute(select(tasks).where(tasks.c.id == original["id"])).mappings().one()
        explanation = conn.execute(select(tasks).where(tasks.c.source == cancel.event_id)).mappings().one()
        assert conn.execute(select(inbox.c.state).where(inbox.c.id == cancel.event_id)).scalar_one() == "DONE"
    assert original["state"] == "CANCELLED"
    context = json.loads(explanation["context_json"])
    assert context["allowed_actions"] == [] and context["control_response"] is True
    assert json.loads(explanation["plan_json"])["budget"]["tool_calls"] == 0
    assert core.validate_context(core.access(cancel), context)
    assert not hasattr(core, "generate")
    await core.stop()
    core.db.close()


@pytest.mark.parametrize("receipt", ["SENT", "FAILED", "UNKNOWN", "PENDING"])
async def test_only_confirmed_delivery_keeps_warm_context_epoch(
    tmp_path, nap_config, nap_event, clock, receipt
):
    from uuid import uuid4

    core = core_for(tmp_path, nap_config, clock)
    first = event(core, nap_event, 810, "你好")
    await core.process(first)
    with core.db.engine.connect() as conn:
        task = dict(conn.execute(select(tasks).where(tasks.c.source == first.event_id)).mappings().one())
    original = json.loads(task["context_json"])
    notification_id = str(uuid4())
    with core.db.transaction() as conn:
        core.tasks._notification(conn, task, None, "之前生成的回答", "result", "important", notification_id)
        conn.execute(update(tasks).where(tasks.c.id == task["id"]).values(state="SUCCEEDED"))
        conn.execute(
            update(outbox)
            .where(outbox.c.origin_id == notification_id)
            .values(state=receipt, sent_at=clock.now().isoformat() if receipt == "SENT" else None)
        )
    second = event(core, nap_event, 811, "接着说")
    next_context = core.contexts.build(core.access(second), core.contexts.turn(second))
    assert (next_context["key"] == original["key"]) == (receipt == "SENT")
    history_text = [item["text"] for item in next_context["seed"]["history"]]
    assert ("之前生成的回答" in history_text) == (receipt == "SENT")
    await core.stop()
    core.db.close()


async def test_seed_source_closure_survives_history_trimming_and_revokes(
    tmp_path, nap_config, nap_event, clock
):
    core = core_for(tmp_path, nap_config, clock)
    first = event(core, nap_event, 812, "最初的资料")
    original = core.contexts.build(core.access(first), core.contexts.turn(first))
    for index in range(core.settings.context.recent_messages + 1):
        later = event(core, nap_event, 813 + index, "接着说")
    refreshed = core.contexts.build(core.access(later), core.contexts.turn(later))
    assert refreshed["key"] == original["key"]
    assert first.event_id not in {item["source"] for item in refreshed["seed"]["history"]}
    assert first.event_id in {item["id"] for item in refreshed["source_manifest"]}
    core.social.revoke_source(core.access(first), first.event_id)
    assert not core.validate_context(core.access(later), refreshed)
    latest = core.contexts.build(core.access(later), core.contexts.turn(later))
    assert latest["key"] != refreshed["key"]
    assert first.event_id not in {item["id"] for item in latest["source_manifest"]}
    await core.stop()
    core.db.close()


async def test_topic_references_weather_followup_and_return_to_displayed_pr(
    tmp_path, nap_config, nap_event, clock
):
    from uuid import uuid4

    core = core_for(tmp_path, nap_config, clock)
    first = event(core, nap_event, 840, "列出 https://github.com/org/repo 的 PR")
    await core.process(first)
    with core.db.engine.connect() as conn:
        task = dict(conn.execute(select(tasks).where(tasks.c.source == first.event_id)).mappings().one())
    original = json.loads(task["context_json"])
    notification_id = str(uuid4())
    body = (
        "1. [第一个修复](https://github.com/org/repo/pull/41)\n2. [第二个修复](https://github.com/org/repo/pull/52)\n"
        + "大段 PR diff " * 200
        + "\n3. [最后一个修复](https://github.com/org/repo/pull/99)"
    )
    with core.db.transaction() as conn:
        core.tasks._notification(conn, task, None, body, "result", "important", notification_id)
        conn.execute(update(tasks).where(tasks.c.id == task["id"]).values(state="SUCCEEDED"))
        conn.execute(
            update(outbox)
            .where(outbox.c.origin_id == notification_id)
            .values(state="SENT", sent_at=clock.now().isoformat(), platform_message_id="pr-list-message")
        )
    weather = event(core, nap_event, 841, "帮我查嘉定今天的天气")
    weather_context = core.contexts.build(core.access(weather), core.contexts.turn(weather))
    assert weather_context["topic"] != original["topic"]
    assert weather_context["seed"]["topic"]["entities"] == ["location:嘉定"]
    assert not any("大段 PR diff" in item["text"] for item in weather_context["seed"]["history"])
    tomorrow = event(core, nap_event, 842, "那明天呢")
    tomorrow_context = core.contexts.build(core.access(tomorrow), core.contexts.turn(tomorrow))
    assert tomorrow_context["topic"] == weather_context["topic"]
    assert "location:嘉定" in tomorrow_context["seed"]["topic"]["entities"]
    tomorrow_task = core.submit_turn(tomorrow)
    assert json.loads(tomorrow_task["goal_json"])["success_checks"] == ["observation"]
    assert json.loads(tomorrow_task["plan_json"])["capability_hints"] == []
    back = event(core, nap_event, 843, "回到刚才第二个 PR，帮我看一下")
    returned = core.contexts.build(core.access(back), core.contexts.turn(back))
    assert returned["topic"] == original["topic"]
    assert returned["selected_reference"]["resource_id"] == "pr:github.com/org/repo#52"
    assert returned["selected_reference"]["ordinal"] == 2
    returned_task = core.submit_turn(back)
    assert json.loads(returned_task["goal_json"])["target_refs"] == ["pr:github.com/org/repo#52"]
    assert returned["selected_reference"]["observed_at"] == clock.now().isoformat()
    assert returned["selected_reference"]["source_kind"] == "delivered_reference"
    assert core.validate_context(core.access(back), returned)
    assert returned["limits"]["soft_compaction_input_tokens"] == 23552
    assert "hard_input_tokens" not in returned["limits"]
    assert returned["model_context_window"] == 32768
    # Reply-to returns the actual prior topic even while weather is most recent.
    reply = event(core, nap_event, 844, "继续这个")
    reply = reply.model_copy(update={"reply_to": "pr-list-message"})
    replied = core.contexts.build(core.access(reply), core.contexts.turn(reply))
    assert replied["topic"] == original["topic"]
    await core.stop()
    core.db.close()


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
def test_planner_preserves_explicit_output_and_success_contracts(nap_config, clock, text, outputs, checks):
    from qqbot.dialogue.planner import plan_turn
    from qqbot.domain import ConversationRef, PrincipalRef, Surface
    from qqbot.domain.execution import TurnInput

    turn = TurnInput(
        conversation=ConversationRef(
            deployment_id="d",
            adapter_instance_id="nap",
            bot_identity_id="100",
            surface=Surface.GROUP,
            remote_conversation_id="200",
        ),
        principal=PrincipalRef(adapter_instance_id="nap", identity_namespace="qq", remote_user_id="300"),
        text=text,
        source_event_ids=("e",),
        anchor_event_id="e",
    )
    goal, plan = plan_turn(turn, Settings())
    assert goal.requested_outputs == outputs
    assert goal.success_checks == checks
    assert plan.runner == "pi"


async def test_background_command_preserves_file_contract(tmp_path, nap_config, nap_event, clock):
    core = core_for(tmp_path, nap_config, clock, tasks_enabled=True)
    request = event(core, nap_event, 850, "/研究 生成 reports/结论.pdf 文件")
    await core.process(request)
    with core.db.engine.connect() as conn:
        task = conn.execute(select(tasks).where(tasks.c.source == request.event_id)).mappings().one()
    goal = json.loads(task["goal_json"])
    assert goal["text"] == "生成 reports/结论.pdf 文件"
    assert goal["requested_outputs"] == ["reports/结论.pdf"]
    assert goal["success_checks"] == ["artifact"]
    assert task["delivery"] == "DETACHED"
    await core.stop()
    core.db.close()


def test_admin_create_task_uses_planner_context_and_idempotent_source(
    tmp_path, nap_config, nap_event, clock, monkeypatch
):
    from fastapi.testclient import TestClient

    monkeypatch.setenv("QQBOT_ADMIN_TOKEN", "admin-test-token")
    app = create_app(
        Settings(database=str(tmp_path / "core.db"), adapters=(nap_config,), tasks_enabled=True),
        clock=clock,
        run_background=False,
    )
    core = app.state.core
    source = event(core, nap_event, 860, "建立会话")
    auth = {"Authorization": "Bearer admin-test-token", "Idempotency-Key": "file-goal"}
    body = {"conversation_id": source.conversation.key, "goal": "生成 reports/报告.pdf 文件"}
    with TestClient(app) as client:
        first = client.post("/api/v1/tasks", headers=auth, json=body)
        assert first.status_code == 200
        assert client.post("/api/v1/tasks", headers=auth, json=body).json() == first.json()
        with core.db.engine.connect() as conn:
            task = conn.execute(select(tasks)).mappings().one()
            assert conn.execute(select(outbox.c.id)).first() is None
        goal, context = json.loads(task["goal_json"]), json.loads(task["context_json"])
        assert goal["requested_outputs"] == ["reports/报告.pdf"] and goal["success_checks"] == ["artifact"]
        assert context["invited"] is True and context["turn"]["source_revision"] > 0
        assert json.loads(task["permission_json"])["role"] == "owner"
        assert core.validate_context(core.tasks._access_for(task), context)


async def test_memory_source_permission_invalidates_old_seed(tmp_path, nap_config, nap_event, clock):
    core = core_for(tmp_path, nap_config, clock)
    remember = event(core, nap_event, 861, "/记住 小陈")
    await core.process(remember)
    request = event(core, nap_event, 862, "小陈")
    access = core.access(request)
    context = core.contexts.build(access, core.contexts.turn(request))
    assert context["seed"]["memories"]
    source = next(item for item in context["source_manifest"] if item["id"] == remember.event_id)
    assert "memory.source.read" in source["actions"]
    core.permissions.upsert(
        PermissionRule(
            id="deny-memory-source",
            effect="deny",
            actions=("memory.source.read",),
            resources=("event:" + remember.event_id,),
        ),
        expected_version=0,
        actor="test",
    )
    assert not core.validate_context(access, context)
    fresh = core.contexts.build(access, core.contexts.turn(request))
    assert not fresh["seed"]["memories"]
    assert fresh["key"] != context["key"]
    await core.stop()
    core.db.close()


def test_admin_retry_preserves_goal_contract_with_fresh_context(
    tmp_path, nap_config, nap_event, clock, monkeypatch
):
    from fastapi.testclient import TestClient

    monkeypatch.setenv("QQBOT_ADMIN_TOKEN", "admin-test-token")
    app = create_app(
        Settings(database=str(tmp_path / "core.db"), adapters=(nap_config,), tasks_enabled=True),
        clock=clock,
        run_background=False,
    )
    core = app.state.core
    source = event(core, nap_event, 863, "建立会话")
    auth = {"Authorization": "Bearer admin-test-token", "Idempotency-Key": "first-file-goal"}
    body = {"conversation_id": source.conversation.key, "goal": "生成 reports/报告.pdf 文件"}
    with TestClient(app) as client:
        first = client.post("/api/v1/tasks", headers=auth, json=body).json()
        with core.db.transaction() as conn:
            original = dict(
                conn.execute(select(tasks).where(tasks.c.public_id == first["public_id"])).mappings().one()
            )
            conn.execute(update(tasks).where(tasks.c.id == original["id"]).values(state="FAILED"))
        response = client.post(
            "/api/v1/tasks/" + first["public_id"] + "/retry",
            headers={**auth, "Idempotency-Key": "retry-file-goal"},
        )
        assert response.status_code == 200
        with core.db.engine.connect() as conn:
            retried = (
                conn.execute(select(tasks).where(tasks.c.public_id == response.json()["public_id"]))
                .mappings()
                .one()
            )
        assert retried["retry_of_task_id"] == original["id"]
        assert retried["goal_json"] == original["goal_json"]
        assert retried["plan_json"] == original["plan_json"]
        context = json.loads(retried["context_json"])
        assert (
            context["turn"]["anchor_event_id"]
            != json.loads(original["context_json"])["turn"]["anchor_event_id"]
        )
        assert context["invited"] is True and context["key"] != json.loads(original["context_json"])["key"]
        assert core.validate_context(core.tasks._access_for(retried), context)


@pytest.mark.parametrize("characters", [9000, 65000])
async def test_oversized_input_queues_bounded_pi_explanation_and_delivers(
    tmp_path, nap_config, nap_event, clock, characters
):
    from qqbot.domain import InboundEvent, SendReceipt

    core = core_for(tmp_path, nap_config, clock, context={"max_request_bytes": 8192})
    marker = "ORIGINAL_OVERSIZED_INPUT_"
    request = event(core, nap_event, 870, marker + "x" * characters)
    await core.process(request)
    with core.db.engine.connect() as conn:
        task = dict(conn.execute(select(tasks)).mappings().one())
        raw_source = conn.execute(
            select(inbox.c.event_json).where(inbox.c.id == request.event_id)
        ).scalar_one()
        assert (
            conn.execute(select(inbox.c.state).where(inbox.c.id == request.event_id)).scalar_one() == "DONE"
        )
        assert conn.execute(select(outbox.c.id)).first() is None
    context = json.loads(task["context_json"])
    assert task["profile"] == "pi"
    assert context["control_facts"]["reason"] == "input_over_budget"
    assert context["control_facts"]["original_goal_started"] is False
    assert context["control_facts"]["received_characters"] == len(request.text)
    assert context["allowed_actions"] == [] and context["control_response"] is True
    assert context["seed"]["history"] == context["seed"]["memories"] == []
    original_persona = json.loads(core.personas.snapshot(request.conversation.key)[0])
    assert context["seed"]["persona"]["core"] == {
        key: original_persona["core"][key] for key in ("name", "boundaries")
    }
    assert len(json.dumps(context["seed"], ensure_ascii=False).encode()) + len(
        context["turn"]["text"].encode()
    ) <= core.settings.context.max_request_bytes // 2
    assert [item["id"] for item in context["source_manifest"]] == [request.event_id]
    assert marker in InboundEvent.model_validate_json(raw_source).text
    assert marker not in task["event_json"] + task["goal"] + task["context_json"]
    assert core.validate_context(core.access(request), context)
    claim = core.tasks.claim("fixture-worker", protocol_version=2)
    assert marker not in json.dumps(claim, ensure_ascii=False)
    assert claim["plan"]["budget"]["model_calls"] == 2 and claim["plan"]["budget"]["tool_calls"] == 0
    # Simulate the authenticated pi result, then exercise the actual Outbox delivery gate.
    narrative = "这次内容加上必要上下文超过了当前输入预算，还没有执行原始请求。请缩短内容或分段发送。"
    core.tasks.submit(
        claim["run_id"],
        claim["fencing_token"],
        "fixture-worker",
        {"status": "completed", "summary": narrative},
    )
    sent = []

    async def send(envelope):
        sent.append(envelope.text)
        return SendReceipt(state="SENT", platform_message_id="notice-message")

    core.adapters["nap"].websocket = object()
    core.adapters["nap"].send = send
    await core.outbox.dispatch_once(core.adapters, pre_send=core.pre_send)
    assert sent == [narrative]
    await core.stop()
    core.db.close()


async def test_oversized_merged_turn_is_one_notice_and_keeps_both_sources(
    tmp_path, nap_config, nap_event, clock
):
    core = core_for(tmp_path, nap_config, clock, context={"max_request_bytes": 8192})
    first = event(core, nap_event, 871, "first-private-part " + "a" * 4500)
    processing = asyncio.create_task(core.process(first))
    await asyncio.sleep(0.02)
    second = event(core, nap_event, 872, "second-private-part " + "b" * 4500)
    await processing
    with core.db.engine.connect() as conn:
        task = conn.execute(select(tasks)).mappings().one()
        states = dict(conn.execute(select(inbox.c.id, inbox.c.state)).all())
    context = json.loads(task["context_json"])
    assert states[first.event_id] == states[second.event_id] == "DONE"
    assert set(context["turn"]["source_event_ids"]) == {first.event_id, second.event_id}
    assert context["control_facts"]["received_characters"] == len(first.text) + len(second.text) + 1
    assert "first-private-part" not in task["goal"] + task["context_json"]
    assert "second-private-part" not in task["goal"] + task["context_json"]
    await core.process(first)
    with core.db.engine.connect() as conn:
        assert len(conn.execute(select(tasks.c.id)).all()) == 1
    await core.stop()
    core.db.close()


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
    output = max(settings.execution.budget.output_tokens_per_call,
                 settings.execution.interactive_budget.output_tokens_per_call,
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
    settings = Settings.model_validate_json(json.dumps(values))
    assert settings.context.max_tokens + settings.execution.budget.output_tokens_per_call + 1024 == settings.model.context_window


@pytest.mark.parametrize("interactive", [True, False])
def test_status_identifies_worker_execution_without_claiming_core_key_proves_model(
    tmp_path, nap_config, clock, monkeypatch, interactive
):
    from fastapi.testclient import TestClient

    from qqbot.config import ModelConfig

    monkeypatch.setenv("QQBOT_ADMIN_TOKEN", "admin-test-token")
    monkeypatch.setenv("CORE_ONLY_MODEL_KEY", "fixture-only-core-key")
    app = create_app(
        Settings(
            database=str(tmp_path / "core.db"),
            adapters=(nap_config,),
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
        assert body["model_configured"] is None
        assert body["model_execution"] == ("worker_pi" if interactive else "disabled")


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
        principal=SimpleNamespace(key="requester"),
    )
    selected, reference, reason = select_topic(index, turn, clock.now(), 1800)
    assert selected["id"] == "topic-1" and reference is None and reason == "ambiguous_display_ordinal"


@pytest.mark.parametrize(
    ("interactive", "background_enabled", "accepted"),
    [
        (False, True, True),
        (True, False, False),
    ],
)
async def test_natural_background_uses_background_switch_independently(
    tmp_path, nap_config, nap_event, clock, interactive, background_enabled, accepted
):
    core = core_for(
        tmp_path,
        nap_config,
        clock,
        interactive_execution_enabled=interactive,
        tasks_enabled=background_enabled,
    )
    request = event(core, nap_event, 873, "这件事后台慢慢做，帮我整理资料")
    await core.process(request)
    with core.db.engine.connect() as conn:
        rows = conn.execute(select(tasks)).mappings().all()
        state = conn.execute(select(inbox.c.state).where(inbox.c.id == request.event_id)).scalar_one()
    assert len(rows) == int(accepted)
    if accepted:
        assert rows[0]["delivery"] == "DETACHED" and state == "DONE"
        assert json.loads(rows[0]["plan_json"])["budget"] == core.settings.execution.budget.model_dump()
    else:
        assert state == "FAILED"
        with pytest.raises(ValueError, match="后台执行未启用"):
            core.submit_turn(request, background=True)
    await core.stop()
    core.db.close()


async def test_background_only_still_cancels_immediately_and_queues_control_explanation(
    tmp_path, nap_config, nap_event, clock
):
    core = core_for(tmp_path, nap_config, clock, interactive_execution_enabled=False, tasks_enabled=True)
    request = event(core, nap_event, 874, "后台慢慢做")
    await core.process(request)
    with core.db.engine.connect() as conn:
        original = conn.execute(select(tasks)).mappings().one()
    cancel = event(core, nap_event, 875, "/取消 " + original["public_id"])
    await core.process(cancel)
    with core.db.engine.connect() as conn:
        assert (
            conn.execute(select(tasks.c.state).where(tasks.c.id == original["id"])).scalar_one()
            == "CANCELLED"
        )
        notice = conn.execute(select(tasks).where(tasks.c.source == cancel.event_id)).mappings().one()
    assert notice["delivery"] == "ATTACHED"
    assert json.loads(notice["context_json"])["control_response"] is True
    ordinary = event(core, nap_event, 876, "你好")
    with pytest.raises(ValueError, match="交互执行未启用"):
        core.submit_turn(ordinary)
    await core.stop()
    core.db.close()


@pytest.mark.parametrize(
    ("execution", "expected"),
    [
        ({"max_active": 4, "warm_max": 0}, {"max_resident": 5, "max_idle": 0, "ttl": 300}),
        (
            {"max_active": 4, "warm_max": 6, "warm_idle_max": 3, "warm_ttl_seconds": 90},
            {"max_resident": 6, "max_idle": 3, "ttl": 90},
        ),
        (
            {"max_active": 6, "warm_max": 2, "warm_idle_max": 1},
            {"max_resident": 7, "max_idle": 1, "ttl": 300},
        ),
    ],
)
async def test_resolved_pool_limits_reach_context_and_core_resident_gate(
    tmp_path, nap_config, nap_event, clock, execution, expected
):
    from qqbot.config import ExecutionConfig

    core = core_for(tmp_path, nap_config, clock, execution=ExecutionConfig(**execution))
    request = event(core, nap_event, 877, "你好")
    context = core.context_for(request)
    assert context["pool"] == expected
    assert core.tasks.max_resident == expected["max_resident"]
    assert context["pool"]["max_resident"] >= core.settings.execution.max_active
    await core.stop()
    core.db.close()


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
        principal=SimpleNamespace(key="requester"),
    )
    selected, reference, reason = select_topic(index, turn, clock.now(), 1800)
    assert selected["id"] == "weather-1" and selected["entities"] == ["location:嘉定"]
    assert reference is None and reason == "matching_subject"


async def test_python_documentation_history_does_not_enter_github_topic(tmp_path, nap_config, nap_event, clock):
    from qqbot.context.topics import entities

    core = core_for(tmp_path, nap_config, clock)
    research = event(core, nap_event, 1800, "请阅读 https://docs.python.org/3/library/asyncio-task.html 并整理文档")
    assert entities(research.text) == ("general", [])
    question = event(core, nap_event, 1801, "看下 yourtj hub 有哪些 PR")
    context = core.contexts.build(core.access(question), core.contexts.turn(question))
    assert not any("docs.python.org" in item["text"] for item in context["seed"]["history"])
    assert entities("查看 https://github.com/org/repo 的 PR")[1] == ["repo:github.com/org/repo"]
    assert entities("查看 https://git.example.com/org/repo/pull/1")[0] == "github"
    await core.stop()
    core.db.close()


async def test_one_million_window_keeps_relevant_large_history(tmp_path, nap_config, nap_event, clock):
    from qqbot.config import ModelConfig

    core = core_for(tmp_path, nap_config, clock, model=ModelConfig(context_window=1000000))
    earlier = event(core, nap_event, 1802, "https://github.com/org/repo 的 PR 背景：" + "相关细节。" * 5000)
    current = event(core, nap_event, 1803, "继续查看 https://github.com/org/repo 的 PR")
    context = core.contexts.build(core.access(current), core.contexts.turn(current))
    assert any(item["text"] == earlier.text for item in context["seed"]["history"])
    assert context["limits"]["soft_compaction_input_tokens"] == 900000
    assert context["limits"]["keep_recent_tokens"] == 200000
    assert context["limits"]["max_bytes"] == 16 * 1024 * 1024
    await core.stop()
    core.db.close()


async def test_expired_failure_notice_delivers_but_original_and_cancelled_results_do_not(
    tmp_path, nap_config, nap_event, clock
):
    from datetime import timedelta

    from qqbot.domain import OutboundEnvelope, SendReceipt
    from qqbot.storage.task_schema import notifications

    core = core_for(tmp_path, nap_config, clock)
    request = event(core, nap_event, 1900, "查看 PR")
    task = core.submit_turn(request)
    original = core.tasks.claim("worker")
    with core.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.id == task["id"]).values(
            deadline=(clock.now() - timedelta(seconds=1)).isoformat(), stop_reason="deadline"))
    core.tasks.submit(original["run_id"], original["fencing_token"], "worker",
                      {"status": "failed", "agent_authored": False, "summary": "Execution timed out."})
    notice = core.tasks.claim("worker")
    summary = "任务超时，尚未取得完整结果。"
    result = core.tasks.submit(notice["run_id"], notice["fencing_token"], "worker",
                               {"status": "failed", "summary": summary})
    assert result["state"] == "EXPIRED"
    with core.db.engine.connect() as conn:
        envelope = OutboundEnvelope.model_validate_json(conn.execute(select(outbox.c.envelope_json)).scalar_one())
    assert core.pre_send(envelope)
    with core.db.transaction() as conn:
        conn.execute(update(notifications).where(notifications.c.id == envelope.origin_id).values(run_id=original["run_id"]))
    assert not core.pre_send(envelope)
    with core.db.transaction() as conn:
        conn.execute(update(notifications).where(notifications.c.id == envelope.origin_id).values(run_id=notice["run_id"]))
        conn.execute(update(tasks).where(tasks.c.id == task["id"]).values(state="CANCELLED"))
    assert not core.pre_send(envelope)
    with core.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.id == task["id"]).values(state="EXPIRED"))
    sent = []

    async def send(envelope):
        sent.append(envelope.text)
        return SendReceipt(state="SENT", platform_message_id="expired-failure-notice")

    core.adapters["nap"].websocket = object()
    core.adapters["nap"].send = send
    await core.outbox.dispatch_once(core.adapters, pre_send=core.pre_send)
    assert sent == [summary]
    await core.stop()
    core.db.close()


async def test_historical_reference_consumes_summary_source_index_with_fresh_authorization(
    tmp_path, nap_config, nap_event, clock,
):
    from qqbot.config import ContextConfig
    from qqbot.memory.maintenance import MemoryMaintenance

    core = core_for(tmp_path, nap_config, clock, context=ContextConfig(recent_messages=2))
    previous = event(core, nap_event, 9100, '原来的目录是 /srv/old-project')
    maintenance = MemoryMaintenance(core.social, enabled=True)
    await maintenance.maintenance_once(core.access(previous))
    for index in range(4):
        event(core, nap_event, 9101 + index, '新话题' + str(index))
    current = event(core, nap_event, 9110, '上次给你的目录是什么')
    context = core.context_for(current)
    assert previous.event_id in {row['source'] for row in context['seed']['history']}
    assert previous.event_id in {row['id'] for row in context['source_manifest']}
    assert core.validate_context(core.access(current), context)
    core.social.revoke_source(core.access(previous), previous.event_id)
    assert not core.validate_context(core.access(current), context)
    next_context = core.context_for(current)
    assert previous.event_id not in {row['source'] for row in next_context['seed']['history']}
    await core.stop()
    core.db.close()
