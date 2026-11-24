"""Host controls resolve exact task identity before optional wording selection."""
import asyncio
import json
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import insert, select, update

from elephant.app import create_app
from elephant.config import ModelConfig, OwnerConfig, QQOwnerBinding, Settings
from elephant.domain import OutboundEnvelope, stable_id
from elephant.permissions import PermissionRule
from elephant.providers import ModelCapacity
from elephant.storage.agent_schema import dialogue_inputs, leader_inbox
from elephant.storage.schema import audit_events, outbox
from elephant.storage.task_schema import inputs, model_requests, notifications, runs, tasks, waits


@pytest.fixture
async def control_runtime(tmp_path, official_config, official_event, clock):
    from conftest import official_payload
    second = official_config.model_copy(update={"instance_id": "qq-other", "app_id": "other-app", "bot_id": "other-bot"})
    settings = Settings(database=str(tmp_path / "controls.db"), adapters=(official_config, second),
        execution={"max_active": 1}, owner=OwnerConfig(qq_bindings=(QQOwnerBinding(
            app_id="app", user_openid="300", verified_at=clock.now().isoformat()),)))
    core = create_app(settings, clock=clock, run_background=False).state.core
    prepare = AsyncMock(wraps=core.knowledge.prepare)
    core.knowledge.prepare = prepare

    def event(text="检查项目", *, private=True, addressed=True, subject="300", adapter="qq", reply_to=None):
        identifier = str(uuid4())
        raw = official_payload(official_event, private=private, text=text, user_id=subject, message_id=identifier)
        raw["id"] = identifier
        normalized = core.adapters[adapter].normalize(raw)
        return normalized.model_copy(update={"mentions_bot": addressed, "reply_to": reply_to})

    def root(source=None, *, title=None, goal="检查项目", children=1):
        source = source or event(goal)
        with core.db.transaction() as conn:
            dialogue = core.sessions.dialogue(conn, source, new_title=title)
            result = core.tasks.graph.submit(conn, dialogue, source, core.access(source), {
                "goal": goal, "children": [{"goal": goal + str(index), "delegation_key": str(index)} for index in range(children)]},
                str(uuid4()), core.sessions.binding(), {})
        return result

    async def receive(source):
        accepted, _ = core.inbox.accept(source)
        await core.process(accepted)
        return accepted

    def response(source):
        with core.db.engine.connect() as conn:
            raw = conn.execute(select(outbox.c.envelope_json).where(outbox.c.origin_id == source.event_id)).scalar_one_or_none()
        return json.loads(raw)["text"] if raw else None

    def no_model_input():
        with core.db.engine.connect() as conn:
            assert not conn.execute(select(dialogue_inputs)).first()
            assert not conn.execute(select(model_requests)).first()
        prepare.assert_not_awaited()

    def notice(task_id, *, purpose="progress", run_id=None):
        identifier, message = str(uuid4()), str(uuid4())
        with core.db.transaction() as conn:
            task = conn.execute(select(tasks).where(tasks.c.id == task_id)).mappings().one()
            core.tasks._notification(conn, task, run_id, "已登记任务事实", purpose, "important", identifier)
            conn.execute(update(outbox).where(outbox.c.origin_id == identifier).values(state="SENT", platform_message_id=message))
        return message

    def question(child_id, name):
        with core.db.transaction() as conn:
            conn.execute(update(tasks).where(tasks.c.id == child_id).values(state="WAITING_INPUT"))
            conn.execute(insert(waits).values(id=name, task_id=child_id, run_id="fixture-" + name,
                revision=1, kind="input", notification_id=name, checkpoint="awaiting authentic owner answer", state="WAITING",
                condition_json="{}", created_at=clock.now().isoformat(), delivered_at=clock.now().isoformat(),
                expires_at=(clock.now() + timedelta(minutes=10)).isoformat()))
            child = conn.execute(select(tasks).where(tasks.c.id == child_id)).mappings().one()
            core.tasks.graph.event(conn, child, "needs_input", {"question_id": name, "public_text": "请选择参数"},
                event_id="question-" + name, wake=True)

    def question_notice(*, sent=True):
        claim = core.sessions.claim("question-leader")
        assert claim["purpose"] == "question"
        core.leader.complete(claim["run_id"], claim["fencing_token"], "question-leader",
            {"status": "completed", "summary": "请选择待答问题的参数"}, stopped=True, quiescent=False,
            receipt={"kind": "turn_finished", "session_id": claim["session_id"], "run_id": claim["run_id"],
                     "session_revision": "question-fixture", "input_ids": claim["source_ids"], "inflight": 0, "tools_paired": True})
        message = str(uuid4())
        with core.db.transaction() as conn:
            notice = conn.execute(select(notifications).where(notifications.c.run_id == claim["run_id"])).mappings().one()
            if sent:
                conn.execute(update(outbox).where(outbox.c.origin_id == notice["id"])
                    .values(state="SENT", platform_message_id=message))
        return message

    yield SimpleNamespace(core=core, event=event, root=root, receive=receive, response=response,
        no_model_input=no_model_input, notice=notice, question=question, question_notice=question_notice, prepare=prepare)
    await core.stop()
    core.db.close()


def task_state(runtime, identifier):
    with runtime.core.db.engine.connect() as conn:
        return conn.execute(select(tasks.c.state).where(tasks.c.id == identifier)).scalar_one()


def child_rows(runtime, root_id):
    with runtime.core.db.engine.connect() as conn:
        return [dict(row) for row in conn.execute(select(tasks).where(tasks.c.parent_task_id == root_id)).mappings()]


async def test_host_control_expression_does_not_block_ingress_and_selects_one_body(control_runtime, monkeypatch):
    r = control_runtime
    r.root()
    r.core.settings = r.core.settings.model_copy(update={"model": ModelConfig(
        endpoint="https://api.deepseek.com/chat/completions", model="deepseek-chat")})
    monkeypatch.setenv("ELEPHANT_MODEL_KEY", "test-key")
    r.core.scheduler = SimpleNamespace(capacity=ModelCapacity(total=2), close=AsyncMock())
    release = asyncio.Event()
    calls = []

    async def slow_post(url, **kwargs):
        calls.append(kwargs["json"])
        await release.wait()
        count = len(json.loads(kwargs["json"]["messages"][1]["content"]))
        return httpx.Response(200, json={"choices": [{"message": {
            "content": json.dumps({"choices": [1] * count})}}]},
            request=httpx.Request("POST", url))

    monkeypatch.setattr(r.core.model_client, "post", slow_post)
    status, same_dialogue, other = (r.event("任务状态"), r.event("任务状态"),
        r.event("/help", adapter="qq-other"))
    for source in (status, same_dialogue, other):
        r.core.inbox.accept(source)
    await r.core.tick(dispatch=False)
    assert status.event_id in r.core._control_expressions
    assert r.response(status) is None
    assert "/tasks" in r.response(other)
    assert r.response(same_dialogue) is None
    release.set()
    await asyncio.gather(*r.core._control_expressions.values())
    await r.core.tick(dispatch=False)
    await asyncio.gather(*r.core._control_expressions.values())
    assert "检查项目" in r.response(same_dialogue)
    assert r.response(status).startswith("目前正在处理。")
    assert "检查项目" in r.response(status)
    with r.core.db.engine.connect() as conn:
        rows = conn.execute(select(outbox).where(outbox.c.origin_id == status.event_id)).mappings().all()
        receipt = json.loads(conn.execute(select(audit_events.c.detail).where(
            audit_events.c.id == stable_id("host-task-control", status.event_id))).scalar_one())
    assert len(rows) == 1 and len(calls) == 2
    assert receipt["response"] == r.response(status)
    assert r.core.pre_send(OutboundEnvelope.model_validate_json(rows[0]["envelope_json"]))


async def test_interrupted_host_expression_replays_fallback_once(control_runtime, monkeypatch):
    r = control_runtime
    r.root()
    r.core.settings = r.core.settings.model_copy(update={"model": ModelConfig(
        endpoint="https://api.deepseek.com/chat/completions", model="deepseek-chat")})
    monkeypatch.setenv("ELEPHANT_MODEL_KEY", "test-key")
    r.core.scheduler = SimpleNamespace(capacity=ModelCapacity(total=2), close=AsyncMock())
    started = asyncio.Event()

    async def hanging_post(_url, **_kwargs):
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(r.core.model_client, "post", hanging_post)
    status = await r.receive(r.event("任务状态"))
    task = r.core._control_expressions[status.event_id]
    await started.wait()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert r.response(status) is None
    await r.core.process(status)
    assert r.response(status).startswith("正在处理。")
    with r.core.db.engine.connect() as conn:
        assert len(conn.execute(select(outbox).where(outbox.c.origin_id == status.event_id)).all()) == 1


@pytest.mark.parametrize("change", ["silent", "revoked"])
async def test_invalidated_host_expression_prevents_delivery(control_runtime, monkeypatch, change):
    r = control_runtime
    r.root()
    r.core.settings = r.core.settings.model_copy(update={"model": ModelConfig(
        endpoint="https://api.deepseek.com/chat/completions", model="deepseek-chat")})
    monkeypatch.setenv("ELEPHANT_MODEL_KEY", "test-key")
    r.core.scheduler = SimpleNamespace(capacity=ModelCapacity(total=2), close=AsyncMock())
    release = asyncio.Event()

    async def slow_post(url, **_kwargs):
        await release.wait()
        return httpx.Response(200, json={"choices": [{"message": {"content": '{"choices":[0,0,0]}'}}]},
            request=httpx.Request("POST", url))

    monkeypatch.setattr(r.core.model_client, "post", slow_post)
    status = await r.receive(r.event("任务状态"))
    if change == "silent":
        r.core.save_state(status.conversation.key, mode="silent")
    else:
        monkeypatch.setattr(r.core, "allowed", lambda _event: False)
    release.set()
    await asyncio.gather(*r.core._control_expressions.values())
    assert r.response(status) is None


async def test_cancel_expression_drops_stale_stop_request(control_runtime, monkeypatch):
    r = control_runtime
    root = r.root()
    assert r.core.tasks.claim("worker") is not None
    r.core.settings = r.core.settings.model_copy(update={"model": ModelConfig(
        endpoint="https://api.deepseek.com/chat/completions", model="deepseek-chat")})
    monkeypatch.setenv("ELEPHANT_MODEL_KEY", "test-key")
    r.core.scheduler = SimpleNamespace(capacity=ModelCapacity(total=2), close=AsyncMock())
    release = asyncio.Event()

    async def slow_post(url, **_kwargs):
        await release.wait()
        return httpx.Response(200, json={"choices": [{"message": {"content": '{"choices":[1]}'}}]},
            request=httpx.Request("POST", url))

    monkeypatch.setattr(r.core.model_client, "post", slow_post)
    cancel = await r.receive(r.event("取消这个任务"))
    assert task_state(r, root["task_id"]) == "CANCEL_REQUESTED"
    with r.core.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.id == root["task_id"]).values(state="CANCELLED"))
    release.set()
    await asyncio.gather(*r.core._control_expressions.values())
    assert r.response(cancel) is None


async def test_full_slots_ambiguous_cancel_selects_then_exact_cancel_only_targets_one(control_runtime):
    r = control_runtime
    first, second = r.root(goal="第一项"), r.root(goal="第二项")
    busy = r.core.tasks.claim("busy-worker")
    assert busy is not None
    with r.core.db.engine.connect() as conn:
        busy_root = conn.execute(select(tasks.c.root_task_id).where(tasks.c.id == busy["task_id"])).scalar_one()
    assert busy_root in {first["task_id"], second["task_id"]}
    target = next(root for root in (first, second) if root["task_id"] != busy_root)
    source = await r.receive(r.event("取消刚才那个"))
    answer = r.response(source)
    assert "第一项" in answer and "第二项" in answer and "哪个目标" in answer
    assert first["public_id"] not in answer and second["public_id"] not in answer
    assert task_state(r, first["task_id"]) == task_state(r, second["task_id"]) == "WAITING_CHILDREN"
    await r.receive(r.event("取消 " + target["public_id"]))
    assert task_state(r, target["task_id"]) == "CANCELLED"
    assert task_state(r, busy_root) == "WAITING_CHILDREN"
    r.no_model_input()


@pytest.mark.parametrize("command", ["/cancel", "/取消"])
async def test_cancel_without_id_and_status_remain_host_controls_at_full_capacity(control_runtime, command):
    r = control_runtime
    root = r.root()
    r.core.tasks.claim("busy-worker")
    status = await r.receive(r.event("查看 " + root["public_id"] + " 的进度"))
    assert "正在处理" in r.response(status) and "检查项目" in r.response(status)
    assert root["public_id"] not in r.response(status) and "WAITING_CHILDREN" not in r.response(status)
    cancel = await r.receive(r.event(command))
    assert r.response(cancel) == "正在停止。" and task_state(r, root["task_id"]) == "CANCEL_REQUESTED"
    r.no_model_input()


async def test_control_replay_keeps_target_after_focus_and_active_roots_change(control_runtime):
    r = control_runtime
    first = r.root()
    source = await r.receive(r.event("取消刚才那个"))
    assert task_state(r, first["task_id"]) == "CANCELLED"
    second = r.root(title="新对话")
    await r.core.process(source)
    assert task_state(r, second["task_id"]) == "WAITING_CHILDREN"
    with r.core.db.engine.connect() as conn:
        receipt = json.loads(conn.execute(select(audit_events.c.detail).where(audit_events.c.kind == "host.task.control")).scalar_one())
    assert receipt["task_id"] == first["task_id"] and receipt["actor"] == source.principal.key
    assert receipt["conversation"] == source.conversation.key and receipt["event_sha256"]
    r.no_model_input()


async def test_conflicting_quote_and_task_number_require_a_new_instruction(control_runtime):
    r = control_runtime
    first = r.root(goal="被引用的任务")
    message = r.notice(first["task_id"])
    second = r.root(title="当前对话", goal="当前任务")
    conflict = await r.receive(r.event("取消 " + second["public_id"], reply_to=message))
    assert "无法对应" in r.response(conflict)
    assert task_state(r, first["task_id"]) == task_state(r, second["task_id"]) == "WAITING_CHILDREN"
    await r.receive(r.event("取消刚才那个", reply_to=message))
    assert task_state(r, first["task_id"]) == "CANCELLED"
    assert task_state(r, second["task_id"]) == "WAITING_CHILDREN"
    await r.receive(r.event("取消 " + second["public_id"]))
    assert task_state(r, second["task_id"]) == "CANCELLED"
    r.no_model_input()


@pytest.mark.parametrize("text", ["不要取消刚才那个", "别取消任务", "请解释‘取消刚才那个’这句话", "“取消刚才那个”", "取消刚才那个之前先解释一下"])
async def test_negative_quoted_and_compound_chat_stays_with_leader(control_runtime, text):
    r = control_runtime
    root = r.root()
    await r.receive(r.event(text))
    assert task_state(r, root["task_id"]) == "WAITING_CHILDREN"
    with r.core.db.engine.connect() as conn:
        assert len(conn.execute(select(dialogue_inputs)).all()) == 1
        assert not conn.execute(select(audit_events).where(audit_events.c.kind == "host.task.control")).first()
    r.prepare.assert_awaited_once()


async def test_unaddressed_group_natural_control_is_ignored_while_slash_stays_explicit(control_runtime):
    r = control_runtime
    root = r.root(r.event(private=False))
    await r.receive(r.event("取消 " + root["public_id"], private=False, addressed=False))
    assert task_state(r, root["task_id"]) == "WAITING_CHILDREN"
    await r.receive(r.event("/取消 " + root["public_id"], private=False, addressed=False))
    assert task_state(r, root["task_id"]) == "CANCELLED"
    r.no_model_input()


async def test_foreign_group_reply_and_cross_app_id_cannot_supply_owner_input(control_runtime):
    r = control_runtime
    root = r.root(r.event(private=False))
    child = child_rows(r, root["task_id"])[0]
    r.question(child["id"], "owner-question")
    message = r.notice(root["task_id"])
    await r.receive(r.event("参数是被伪造的", private=False, subject="301", reply_to=message))
    await r.receive(r.event("补充 " + child["public_id"] + " 跨应用答案", private=False, adapter="qq-other"))
    with r.core.db.engine.connect() as conn:
        assert not conn.execute(select(inputs)).first()
        assert conn.execute(select(waits.c.state)).scalar_one() == "WAITING"
    r.no_model_input()


async def test_foreign_group_reply_cannot_be_overridden_by_own_valid_task_id(control_runtime):
    r = control_runtime
    original = r.root(r.event(private=False))
    other = r.root(r.event(private=False, subject="301"))
    message = r.notice(original["task_id"])
    await r.receive(r.event("补充 " + other["public_id"] + " 这条消息归哪个任务", private=False,
                          subject="301", reply_to=message))
    with r.core.db.engine.connect() as conn:
        assert not conn.execute(select(inputs)).first()
    r.no_model_input()


async def test_group_replies_to_host_status_use_its_original_task_and_requester(control_runtime):
    r = control_runtime
    event = r.event(private=False)
    target = r.root(event)
    status = await r.receive(r.event("查看 " + target["public_id"], private=False))
    with r.core.db.transaction() as conn:
        conn.execute(update(outbox).where(outbox.c.origin_id == status.event_id).values(
            state="SENT", platform_message_id="task-status"))
    other = r.root(r.event(private=False, subject="301"))
    await r.receive(r.event("补充 " + other["public_id"] + " 只看上周", private=False,
                          addressed=False, subject="301", reply_to="task-status"))
    r.root(event, title="切换后的对话")
    source = await r.receive(r.event("保留上周的记录", private=False, addressed=False, reply_to="task-status"))
    replay, created = r.core.inbox.accept(source.model_copy(update={"mentions_bot": True}))
    assert not created and replay == source
    with r.core.db.engine.connect() as conn:
        row = conn.execute(select(inputs).where(inputs.c.task_id == target["task_id"])).mappings().one()
        assert row["source_event_id"] == source.event_id
        assert not conn.execute(select(inputs).where(inputs.c.task_id == other["task_id"])).first()
    r.no_model_input()


async def test_mention_upgrade_during_context_preparation_preserves_the_newer_event(control_runtime):
    r = control_runtime
    chat = await r.receive(r.event("/状态", private=False))
    with r.core.db.transaction() as conn:
        conn.execute(update(outbox).where(outbox.c.origin_id == chat.event_id).values(
            state="SENT", platform_message_id="bot-chat"))

    async def upgrade(event, context):
        r.core.inbox.accept(event.model_copy(update={"mentions_bot": True}))
        return context

    r.prepare.side_effect = upgrade
    source = await r.receive(r.event("今天天气怎么样", private=False, addressed=False, reply_to="bot-chat"))
    pending, = r.core.inbox.pending()
    assert pending.event_id == source.event_id and pending.mentions_bot
    assert pending.ingest_seq > source.ingest_seq
    await r.core.tick(dispatch=False)
    with r.core.db.engine.connect() as conn:
        assert conn.execute(select(dialogue_inputs.c.event_json)).scalar_one() == pending.model_dump_json()
    assert not r.core.inbox.pending()


async def test_active_notification_reply_is_idempotent_and_closed_reply_stays_bound(control_runtime):
    r = control_runtime
    root = r.root()
    message = r.notice(root["task_id"])
    source = await r.receive(r.event("只检查最近一周，保留问号？", reply_to=message))
    await r.core.process(source)
    with r.core.db.engine.connect() as conn:
        item = conn.execute(select(inputs).where(inputs.c.task_id == root["task_id"])).mappings().one()
        assert item["source_id"] == source.event_id and item["text"] == source.text
        assert item["kind"] == "followup" and item["actor"] == source.principal.key
    r.no_model_input()
    with r.core.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.root_task_id == root["task_id"]).values(state="SUCCEEDED"))
    other = r.root(goal="另一项任务")
    closed = await r.receive(r.event("接下来讨论另一种方案", reply_to=message))
    assert "另发一条请求" in r.response(closed)
    r.no_model_input()
    unrelated = await r.receive(r.event("顺便帮我看看今天的天气。"))
    with r.core.db.engine.connect() as conn:
        assert conn.execute(select(dialogue_inputs.c.id)).scalar_one() == unrelated.event_id
        assert not conn.execute(select(inputs).where(inputs.c.task_id == other["task_id"])).first()


async def test_recalled_task_notice_cannot_route_a_new_reply(control_runtime):
    r = control_runtime
    root = r.root()
    notice = r.notice(root["task_id"])
    r.core.inbox.accept(r.event().model_copy(update={"event_type": "message.recalled", "platform_message_id": notice}))
    reply = await r.receive(r.event("只看最近一周", reply_to=notice))
    assert "没有找到对应内容" in r.response(reply)
    with r.core.db.engine.connect() as conn:
        assert not conn.execute(select(inputs)).first()
    r.no_model_input()


@pytest.mark.parametrize(("withdrawn", "quoted"), [(None, False), (None, True), ("answer", False), ("selection", False)])
async def test_multiple_open_questions_require_child_selection_and_preserve_exact_answer(
        control_runtime, withdrawn, quoted):
    r = control_runtime
    root = r.root(children=2)
    first, second = child_rows(r, root["task_id"])
    r.question(first["id"], "first-question");r.question(second["id"], "second-question")
    message = r.question_notice()
    source = await r.receive(r.event("这是给待答问题的答案", reply_to=message))
    assert "检查项目0" in r.response(source) and "检查项目1" in r.response(source)
    assert first["public_id"] not in r.response(source) and second["public_id"] not in r.response(source)
    with r.core.db.engine.connect() as conn:
        assert not conn.execute(select(inputs)).first()

    def recall(event):
        r.core.inbox.accept(event.model_copy(update={"event_id": "recall-" + event.event_id,
            "event_type": "message.recalled"}))

    if quoted:
        with r.core.db.transaction() as conn:
            conn.execute(update(outbox).where(outbox.c.origin_id == source.event_id)
                .values(state="SENT", platform_message_id="selection-menu"))
    choice = r.event("选择 1", reply_to="selection-menu" if quoted else None)
    if withdrawn:
        recall(source if withdrawn == "answer" else choice)
    selected = await r.receive(choice)
    if withdrawn:
        with r.core.db.engine.connect() as conn:
            assert not conn.execute(select(inputs)).first()
            assert set(conn.execute(select(waits.c.state)).scalars()) == {"WAITING"}
            envelope = OutboundEnvelope.model_validate_json(conn.scalar(select(outbox.c.envelope_json)
                .where(outbox.c.origin_id == selected.event_id)))
            assert "请重新发送内容" in envelope.text
        assert r.core.pre_send(envelope) == (withdrawn == "answer")
        assert r.core.controls.route(selected) == r.response(selected)
        r.no_model_input()
        return
    with r.core.db.engine.connect() as conn:
        rows = conn.execute(select(inputs)).mappings().all()
        assert len(rows) == 1 and rows[0]["task_id"] == first["id"]
        assert rows[0]["question_id"] == "first-question" and rows[0]["text"] == source.text
        assert rows[0]["actor"] == source.principal.key and rows[0]["source_event_id"] == source.event_id
        assert conn.execute(select(waits.c.state).where(waits.c.id == "second-question")).scalar_one() == "WAITING"
        target = conn.execute(select(tasks).where(tasks.c.id == first["id"])).mappings().one()
        assert {item["id"] for item in json.loads(target["context_json"])["source_manifest"]} == {
            selected.event_id, source.event_id}
    recall(selected)
    assert not r.core.tasks.graph.handoff_valid(target)
    assert r.core.tasks.claim("worker") is None
    r.no_model_input()


@pytest.mark.parametrize("legacy", [False, True])
async def test_leader_question_notice_binds_its_actual_wait_over_other_open_questions(control_runtime, legacy):
    r = control_runtime
    root = r.root(children=2)
    first, second = child_rows(r, root["task_id"])
    r.question(first["id"], "bound-question")
    claim = r.core.sessions.claim("question-leader")
    assert claim["purpose"] == "question"
    r.core.leader.complete(claim["run_id"], claim["fencing_token"], "question-leader",
        {"status": "completed", "summary": "请确认第一项的参数"}, stopped=True, quiescent=False,
        receipt={"kind": "turn_finished", "session_id": claim["session_id"], "run_id": claim["run_id"],
                 "session_revision": "question-fixture", "input_ids": claim["source_ids"], "inflight": 0, "tools_paired": True})
    with r.core.db.transaction() as conn:
        notice = conn.execute(select(notifications).where(notifications.c.run_id == claim["run_id"])).mappings().one()
        conn.execute(update(outbox).where(outbox.c.origin_id == notice["id"]).values(state="SENT", platform_message_id="leader-question-message"))
        if legacy:
            conn.execute(update(waits).where(waits.c.id == "bound-question").values(notification_id="bound-question"))
            claim.pop("question_ids")
            conn.execute(update(runs).where(runs.c.id == claim["run_id"]).values(request_json=json.dumps(claim)))
    r.question(second["id"], "new-unrelated-question")
    await r.receive(r.event("确认第一项参数", reply_to="leader-question-message"))
    with r.core.db.engine.connect() as conn:
        answered = conn.execute(select(inputs).where(inputs.c.question_id == "bound-question")).mappings().one()
        assert answered["task_id"] == first["id"]
        assert not conn.execute(select(inputs).where(inputs.c.task_id == second["id"])).first()
        assert conn.execute(select(waits.c.state).where(waits.c.id == "new-unrelated-question")).scalar_one() == "WAITING"
    r.no_model_input()


async def test_question_remains_deliverable_after_an_unrelated_worker_revision(control_runtime):
    r = control_runtime
    root = r.root(children=2)
    first, second = child_rows(r, root["task_id"])
    r.question(first["id"], "relayed-question")
    r.question_notice(sent=False)
    with r.core.db.engine.connect() as conn:
        notice_id = conn.scalar(select(notifications.c.id).where(notifications.c.purpose == "question"))
        assert conn.scalar(select(waits.c.notification_id).where(waits.c.id == "relayed-question")) == notice_id
        envelope = OutboundEnvelope.model_validate_json(conn.scalar(select(outbox.c.envelope_json)
            .where(outbox.c.origin_id == notice_id)))
    revised = await r.receive(r.event("补充 " + second["public_id"] + " 只看今年"))
    assert r.response(revised) == "已收到补充内容。"
    assert r.core.pre_send(envelope)
    answer = await r.receive(r.event("回答 " + first["public_id"] + " 参数 A"))
    assert r.response(answer) == "已收到补充内容。"
    assert not r.core.pre_send(envelope)


async def test_question_relay_and_worker_failure_keep_separate_bindings(control_runtime):
    r = control_runtime
    root = r.root(children=2)
    first, second = child_rows(r, root["task_id"])
    r.question(first["id"], "answer-during-escalation")
    worker = r.core.tasks.claim("worker")
    assert worker["task_id"] == second["id"]
    r.core.tasks.submit(worker["run_id"], worker["fencing_token"], "worker",
        {"status": "failed", "summary": "无法访问数据", "agent_authored": False})
    message = r.question_notice()
    answer = await r.receive(r.event("参数 A", reply_to=message))
    with r.core.db.engine.connect() as conn:
        row = conn.execute(select(inputs).where(inputs.c.source_id == answer.event_id)).mappings().one()
        assert row["task_id"] == first["id"] and row["question_id"] == "answer-during-escalation"
        assert conn.scalar(select(leader_inbox.c.handled_at).where(
            leader_inbox.c.resume_reason == "needs_attention")) is None
    assert r.core.sessions.claim("leader")["purpose"] == "attention"


@pytest.mark.parametrize("action", ["task.read", "task.cancel", "task.input"])
async def test_control_permissions_are_rechecked_without_model_admission(control_runtime, action):
    r = control_runtime
    root = r.root()
    r.core.permissions.upsert(PermissionRule(id="deny-control", effect="deny", actions=(action,),
        resources=("task:" + root["task_id"],)), expected_version=0, actor="test")
    text = {"task.read": "查看 ", "task.cancel": "取消 ", "task.input": "补充 "}[action] + root["public_id"]
    if action == "task.input":text += " 新内容"
    await r.receive(r.event(text))
    assert task_state(r, root["task_id"]) == "WAITING_CHILDREN"
    with r.core.db.engine.connect() as conn:
        assert not conn.execute(select(inputs)).first()
    r.no_model_input()


async def test_natural_control_uses_current_dialogue_without_touching_another_active_root(control_runtime):
    r = control_runtime
    first = r.root(title="第一对话")
    second = r.root(title="第二对话")
    await r.receive(r.event("取消刚才那个"))
    assert task_state(r, first["task_id"]) == "WAITING_CHILDREN"
    assert task_state(r, second["task_id"]) == "CANCELLED"
    r.no_model_input()


async def test_lost_cancel_receipt_retries_the_bound_target_without_retargeting(control_runtime, monkeypatch):
    r = control_runtime
    first = r.root()
    original_reply = r.core.reply

    def lost_reply(*args, **kwargs):
        raise RuntimeError("simulated response loss after committed cancellation")

    monkeypatch.setattr(r.core, "reply", lost_reply)
    source = await r.receive(r.event("取消刚才那个"))
    assert task_state(r, first["task_id"]) == "CANCELLED" and r.response(source) is None
    assert r.core.inbox.pending() == [source]
    second = r.root(title="随后创建的新任务")
    monkeypatch.setattr(r.core, "reply", original_reply)
    await r.core.tick(dispatch=False)
    assert r.response(source) == "已取消。"
    assert not r.core.inbox.pending()
    assert task_state(r, second["task_id"]) == "WAITING_CHILDREN"
    r.no_model_input()


@pytest.mark.parametrize("replay", [False, True])
async def test_qq_channel_cancel_recalled_before_commit_does_not_cancel(control_runtime, clock, monkeypatch, replay):
    from contracts.test_adapters import signed

    r = control_runtime
    qq = r.core.adapters["qq"]
    message = {"id": "channel-task", "channel_id": "channel", "author": {"id": "300"},
        "timestamp": clock.now().isoformat(), "content": "检查项目"}
    root = r.root(source=qq.normalize({"op": 0, "t": "AT_MESSAGE_CREATE", "d": message}))
    cancellation = {**message, "id": "channel-cancel", "content": "取消 " + root["public_id"]}
    created = {"op": 0, "t": "AT_MESSAGE_CREATE", "d": cancellation}
    recalled = {"op": 0, "t": "PUBLIC_MESSAGE_DELETE",
        "d": {"message": cancellation, "op_user": {"id": "300"}}}
    assert qq.callback(*signed(qq, created, clock)) == {"op": 12}
    if replay:
        original_cancel = r.core.tasks.cancel

        def interrupted_cancel(*args, **kwargs):
            original_cancel(*args, **kwargs)
            raise RuntimeError("cancel interrupted before commit")

        with monkeypatch.context() as patch:
            patch.setattr(r.core.tasks, "cancel", interrupted_cancel)
            with pytest.raises(RuntimeError, match="persisted task control needs replay"):
                await r.core.tick(dispatch=False)
        assert task_state(r, root["task_id"]) == "WAITING_CHILDREN"
        assert r.core.inbox.pending()
    assert qq.callback(*signed(qq, recalled, clock)) == {"op": 12}
    assert r.core.inbox.pending()[0].event_type == "message.recalled"
    await r.core.tick(dispatch=False)
    await r.core.tick(dispatch=False)
    assert task_state(r, root["task_id"]) == "WAITING_CHILDREN"
    assert not r.core.inbox.pending()
    r.no_model_input()


async def test_replayed_control_revalidates_permissions_and_original_actor_digest(control_runtime):
    r = control_runtime
    first = r.root()
    source = await r.receive(r.event("取消刚才那个"))
    r.core.permissions.upsert(PermissionRule(id="revoke-cancel", effect="deny", actions=("task.cancel",),
        resources=("task:" + first["task_id"],)), expected_version=0, actor="test")
    with pytest.raises(PermissionError):
        r.core.controls.route(source)
    foreign = source.model_copy(update={"principal": source.principal.model_copy(update={"remote_user_id": "301"})})
    with pytest.raises(ValueError, match="conflicting task-control"):
        r.core.controls.route(foreign)
    changed = source.model_copy(update={"reply_to": "another-notification"})
    with pytest.raises(ValueError, match="conflicting task-control"):
        r.core.controls.route(changed)
    r.no_model_input()


async def test_lost_answer_receipt_reuses_exact_question_even_after_task_closes(control_runtime, monkeypatch):
    r = control_runtime
    root = r.root()
    child = child_rows(r, root["task_id"])[0]
    r.question(child["id"], "durable-answer-question")
    message = r.question_notice()
    original_reply = r.core.reply

    def lost_reply(*args, **kwargs):
        raise RuntimeError("simulated response loss after committed answer")

    monkeypatch.setattr(r.core, "reply", lost_reply)
    source = await r.receive(r.event("原始答案？", reply_to=message))
    assert r.response(source) is None
    assert r.core.inbox.pending() == [source]
    with r.core.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.root_task_id == root["task_id"]).values(state="SUCCEEDED"))
    r.root(title="随后对话")
    monkeypatch.setattr(r.core, "reply", original_reply)
    await r.core.tick(dispatch=False)
    assert r.response(source) == "已收到补充内容。"
    with r.core.db.engine.connect() as conn:
        saved = conn.execute(select(inputs)).mappings().all()
        assert len(saved) == 1 and saved[0]["question_id"] == "durable-answer-question"
        assert saved[0]["source_id"] == source.event_id and saved[0]["text"] == "原始答案？"
    r.no_model_input()


async def test_progress_reply_is_followup_and_does_not_answer_or_wake_open_question(control_runtime):
    r = control_runtime
    root = r.root()
    child = child_rows(r, root["task_id"])[0]
    r.question(child["id"], "still-awaiting-answer")
    message = r.notice(root["task_id"], purpose="progress")
    source = await r.receive(r.event("补充一条目标约束", reply_to=message))
    with r.core.db.engine.connect() as conn:
        root_input = conn.execute(select(inputs).where(inputs.c.task_id == root["task_id"])).mappings().one()
        assert root_input["source_id"] == source.event_id and root_input["kind"] == "followup"
        assert root_input["question_id"] is None
        assert not conn.execute(select(inputs).where(inputs.c.kind == "answer")).first()
        assert conn.execute(select(waits.c.state).where(waits.c.id == "still-awaiting-answer")).scalar_one() == "WAITING"
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == child["id"])).scalar_one() == "WAITING_INPUT"
    r.no_model_input()


async def test_obsolete_question_reply_cannot_turn_into_a_sibling_revision(control_runtime):
    r = control_runtime
    root = r.root(children=2)
    first, second = child_rows(r, root["task_id"])
    r.question(first["id"], "old-question")
    message = r.question_notice()
    with r.core.db.transaction() as conn:
        conn.execute(update(waits).where(waits.c.id == "old-question").values(state="CONSUMED"))
    r.question(second["id"], "new-question")
    sent = await r.receive(r.event("确认", reply_to=message))
    assert "这个问题已结束" in r.response(sent)
    with r.core.db.engine.connect() as conn:
        assert not conn.execute(select(inputs)).first()
        assert conn.execute(select(waits.c.state).where(waits.c.id == "new-question")).scalar_one() == "WAITING"
    r.no_model_input()


@pytest.mark.parametrize("outcome", ["relay", "answered", "cancelled"])
async def test_nested_worker_question_keeps_its_binding_across_a_revision(control_runtime, outcome):
    r = control_runtime
    r.root()
    parent = r.core.tasks.claim("parent")
    identity = (parent["run_id"], parent["fencing_token"], "parent")
    nested = r.core.tasks.task_operation(*identity, {"operation": "tasks.spawn", "call_id": "spawn-nested",
        "arguments": {"goal": "确认标准", "delegation_key": "nested"}})
    r.core.tasks.task_operation(*identity, {"operation": "tasks.wait", "call_id": "wait-nested",
        "arguments": {"children": [nested["task_id"]], "predicate": "all"}})
    r.core.tasks.submit(*identity, {"status": "waiting_children", "summary": "等待检查结果"}, receipt={
        "kind": "run_yielded", "session_id": parent["session_id"], "run_id": parent["run_id"],
        "session_revision": "parent-wait", "input_ids": parent["context"]["source_input_ids"],
        "inflight": 0, "tools_paired": True})
    worker = r.core.tasks.claim("nested")
    assert worker["task_id"] == nested["task_id"]
    question = "请确认以北京时间还是 UTC 统计？"
    r.core.tasks.ask(worker["run_id"], worker["fencing_token"], "nested", {
        "question_id": "nested-question", "text": question, "checkpoint": "等待时区确认"})
    await r.receive(r.event("补充 " + nested["public_id"] + " 只统计今年"))
    response = "回答 " + nested["public_id"] + " 用北京时间"
    if outcome == "cancelled":
        await r.receive(r.event("取消 " + nested["public_id"]))
        assert r.core.sessions.claim("leader") is None
        return
    if outcome == "answered":
        await r.receive(r.event(response))
        replay = await r.receive(r.event(response))
        assert "这个问题已结束" in r.response(replay)
        assert r.core.sessions.claim("leader") is None
    else:
        message = r.question_notice()
        with r.core.db.engine.connect() as conn:
            claim = json.loads(conn.execute(select(runs.c.request_json).where(runs.c.purpose == "question")).scalar_one())
        assert question in claim["goal"]
        await r.receive(r.event(response, reply_to=message))
    with r.core.db.engine.connect() as conn:
        answer = conn.execute(select(inputs).where(inputs.c.question_id == "nested-question")).mappings().one()
    assert answer["task_id"] == nested["task_id"] and answer["text"] == "用北京时间"


async def test_worker_followup_preserves_wait_and_resumes_after_exact_answer(control_runtime):
    r = control_runtime
    root = r.root(children=2)
    worker = r.core.tasks.claim("worker")
    identity = (worker["run_id"], worker["fencing_token"], "worker")
    target = next(row for row in child_rows(r, root["task_id"]) if row["id"] == worker["task_id"])
    followup = await r.receive(r.event("追加 " + target["public_id"] + "：保留最近一周"))
    r.core.tasks.ask(*identity, {"question_id": "exact-question", "text": "选择参数", "checkpoint": "等待参数"})
    receipt = {"kind": "run_yielded", "run_id": worker["run_id"], "session_id": worker["session_id"],
        "session_revision": "wait-leaf", "input_ids": worker["context"]["source_input_ids"],
        "inflight": 0, "tools_paired": True}
    result = r.core.tasks.submit(*identity, {"status": "waiting_input", "summary": "等待参数"}, receipt=receipt)
    assert result["state"] == "WAITING_INPUT"
    with r.core.db.engine.connect() as conn:
        row = conn.execute(select(inputs)).mappings().one()
        assert row["task_id"] == target["id"] and row["question_id"] is None and row["state"] == "PENDING"
        assert row["source_event_id"] == followup.event_id

    await r.receive(r.event("回答 " + target["public_id"] + " 参数 A"))
    # Claims are fair across workers, so the untouched sibling can be admitted first.
    continuation = r.core.tasks.claim("worker")
    if continuation["task_id"] != target["id"]:
        assert continuation["continuation"]["inputs"] == []
        r.core.tasks.submit(continuation["run_id"], continuation["fencing_token"], "worker",
            {"status": "failed", "summary": "测试结束", "agent_authored": False})
        continuation = r.core.tasks.claim("worker")
    assert continuation["task_id"] == target["id"]
    assert [row["text"] for row in continuation["continuation"]["inputs"]] == ["保留最近一周", "参数 A"]
    stale_receipt = {**receipt, "input_ids": continuation["context"]["source_input_ids"]}
    with pytest.raises(PermissionError, match="stale run"):
        r.core.tasks.session_receipt(*identity, stale_receipt)
    with r.core.db.engine.connect() as conn:
        assert all(value is None for value in conn.execute(select(inputs.c.consumed_at)).scalars())
    r.no_model_input()


@pytest.mark.parametrize("acknowledged", [False, True])
async def test_recalled_task_input_is_fenced_at_poll_and_session_receipt(control_runtime, acknowledged):
    r = control_runtime
    root = r.root()
    worker = r.core.tasks.claim("worker")
    identity = (worker["run_id"], worker["fencing_token"], "worker")
    event = await r.receive(r.event("补充 " + root["public_id"] + " 最近一周"))
    pending = r.core.tasks.take_inputs(*identity)
    assert [row["text"] for row in pending] == ["最近一周"]
    receipt = {"kind": "input_consumed", "run_id": worker["run_id"], "session_id": worker["session_id"],
        "session_revision": "warm-leaf", "input_ids": [pending[0]["id"]]}
    if acknowledged:
        r.core.tasks.session_receipt(*identity, receipt)
    else:
        with r.core.db.transaction() as conn:
            conn.execute(update(inputs).where(inputs.c.source_event_id == event.event_id).values(source_event_id=None))
    r.core.inbox.accept(event.model_copy(update={"event_id": "recall-" + event.event_id, "event_type": "message.recalled"}))
    assert r.core.tasks.take_inputs(*identity) == []
    with pytest.raises(PermissionError, match="input context"):
        r.core.tasks.session_receipt(*identity, receipt)
    assert r.core.tasks.control(*identity)["state"] == "CANCEL_REQUESTED"
    with r.core.db.engine.connect() as conn:
        consumed = conn.execute(select(inputs.c.consumed_at).where(inputs.c.id == pending[0]["id"])).scalar_one()
    assert bool(consumed) is acknowledged


async def test_recalled_task_input_rotates_the_leader_session_after_finalize(control_runtime):
    r = control_runtime
    root = r.root()
    worker = r.core.tasks.claim("worker")
    r.core.tasks.submit(worker["run_id"], worker["fencing_token"], "worker",
        {"status": "completed", "summary": "核对完成"}, receipt={
            "kind": "turn_finished", "run_id": worker["run_id"], "session_id": worker["session_id"],
            "session_revision": "worker-leaf", "input_ids": worker["context"]["source_input_ids"],
            "inflight": 0, "tools_paired": True})
    revision = await r.receive(r.event("补充 " + root["public_id"] + " 按周汇总"))
    claim = r.core.sessions.claim("leader")
    assert claim["purpose"] == "finalize" and claim["task_input_ids"]
    identity = (claim["run_id"], claim["fencing_token"], "leader")
    await r.core.leader.invoke("bind-process", identity, {"session_id": claim["session_id"],
        "session_file": "finalizer-fixture.jsonl", "process_id": "leader-fixture", "epoch": 1, "generation": 1})
    r.core.leader.complete(*identity, {"status": "completed", "summary": "汇总完成"}, stopped=False, quiescent=True,
        process={"id": "leader-fixture", "epoch": 1, "generation": 1}, receipt={
            "kind": "turn_finished", "run_id": claim["run_id"], "session_id": claim["session_id"],
            "session_revision": "leader-leaf", "input_ids": claim["context"]["source_input_ids"],
            "inflight": 0, "tools_paired": True})

    r.core.inbox.accept(revision.model_copy(update={"event_id": "recall-" + revision.event_id, "event_type": "message.recalled"}))
    fresh = await r.receive(r.event("另一个问题"))
    assert r.core.sessions.claim("leader") is None
    next_turn = r.core.sessions.claim("leader")
    assert next_turn["source_ids"] == [fresh.event_id]
    assert next_turn["session_id"] != claim["session_id"]


@pytest.mark.parametrize(("state", "expected"), [
    ("QUEUED", "已收到，尚未开始"), ("RUNNING", "正在处理"),
    ("WAITING_INPUT", "等待你补充信息"), ("WAITING_EXTERNAL", "等待外部结果"),
    ("SUCCEEDED", "已完成"), ("PARTIAL", "已完成一部分"),
    ("BLOCKED", "暂时无法继续"), ("FAILED", "这次未能完成"),
    ("EXPIRED", "已超时停止"), ("NEEDS_RECONCILIATION", "正在核实处理结果"),
])
async def test_status_uses_natural_progress_and_preserves_result(control_runtime, state, expected):
    r = control_runtime
    root = r.root(goal="整理本周记录")
    with r.core.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.id == root["task_id"]).values(state=state,
            result_json=json.dumps({"status": "partial", "summary": "已整理三条记录"})))
    source = await r.receive(r.event("/任务 " + root["public_id"]))
    answer = r.response(source)
    assert expected in answer and "整理本周记录" in answer and "已整理三条记录" in answer
    assert root["public_id"] not in answer and state not in answer and "子任务" not in answer
    r.no_model_input()


async def test_help_service_status_and_dialogues_hide_internal_details(control_runtime):
    r = control_runtime
    r.root(title="研究资料")
    for text in ("/帮助", "/状态", "/对话", "/任务", "/切换对话 不存在的名称"):
        answer = await r.core.command(r.event(text))
        assert answer
        assert all(word not in answer for word in ("Leader", "Worker", "排队", "编号", "WAITING_CHILDREN", "T-"))
    assert await r.core.command(r.event("/对话")) == "研究资料"
    assert await r.core.command(r.event("/切换对话 研究资料")) == "当前对话：研究资料"
    with r.core.db.engine.connect() as conn:
        from elephant.storage.agent_schema import dialogues
        identifier = conn.execute(select(dialogues.c.id)).scalar_one()
    assert await r.core.command(r.event("/切换对话 " + identifier)) == "当前对话：研究资料"


@pytest.mark.parametrize(("english", "legacy"), [
    ("/help", "/帮助"),
    ("/status", "/状态"),
    ("/tasks", "/任务"),
    ("/project", "/项目"),
    ("/dialogs", "/对话"),
])
async def test_legacy_read_commands_share_the_english_host_route(control_runtime, english, legacy):
    r = control_runtime
    r.root(title="研究资料")
    english_event = await r.receive(r.event(english))
    legacy_event = await r.receive(r.event(legacy))
    assert r.response(english_event)
    assert r.response(legacy_event) == r.response(english_event)
    r.no_model_input()


@pytest.mark.parametrize(("new", "switch", "mute", "resume", "mode"), [
    ("/new", "/switch", "/mute", "/resume", "/mode"),
    ("/新对话", "/切换对话", "/静默", "/恢复", "/模式"),
])
async def test_english_and_legacy_session_commands_remain_host_controls(
        control_runtime, new, switch, mute, resume, mode):
    r = control_runtime
    r.root(title="研究资料")
    created = await r.receive(r.event(new + " 第二个对话"))
    assert r.response(created) == "当前对话：第二个对话"
    switched = await r.receive(r.event(switch + " 研究资料"))
    assert r.response(switched) == "当前对话：研究资料"
    stopped = await r.receive(r.event(mute + " 30m"))
    assert r.core.state(stopped.conversation.key)["mode"] == "silent"
    resumed = await r.receive(r.event(resume))
    assert r.core.state(resumed.conversation.key)["mode"] == "addressed_only"
    changed = await r.receive(r.event(mode + " silent"))
    assert r.core.state(changed.conversation.key)["mode"] == "silent"
    r.no_model_input()


async def test_legacy_slash_prefix_is_not_a_host_command(control_runtime):
    r = control_runtime
    await r.receive(r.event("/状态说明"))
    r.core.knowledge.prepare.assert_awaited_once()


@pytest.mark.parametrize("command", ["/followup", "/补充"])
async def test_english_and_legacy_followup_keep_the_explicit_target(control_runtime, command):
    r = control_runtime
    root = r.root()
    source = await r.receive(r.event(command + " " + root["public_id"] + " 只看最近一周"))
    assert r.response(source) == "已收到补充内容。"
    with r.core.db.engine.connect() as conn:
        stored = conn.execute(select(inputs).where(inputs.c.task_id == root["task_id"])).mappings().one()
    assert stored["source_id"] == source.event_id and stored["text"] == "只看最近一周"
    r.no_model_input()


@pytest.mark.parametrize(("error", "expected"), [
    (PermissionError("task:T-private belongs to another subject"), "无法操作"),
    (KeyError("T-internal"), "没有找到"),
    (ValueError("capacity_full: /var/lib/elephant/private"), "暂时无法处理"),
])
async def test_rejected_private_input_hides_internal_errors(control_runtime, monkeypatch, error, expected):
    r = control_runtime
    def reject(_event):
        raise error
    monkeypatch.setattr(r.core.controls, "route", reject)
    source = await r.receive(r.event("进度怎么样"))
    answer = r.response(source)
    assert expected in answer
    assert all(value not in answer for value in ("T-", "capacity_full", "/var/lib", "subject"))
    r.no_model_input()


async def test_invalid_silence_duration_keeps_actionable_hint(control_runtime):
    r = control_runtime
    source = await r.receive(r.event("/静默 明天"))
    assert r.response(source) == "静默时长用 s/m/h，例如 30m"
    r.no_model_input()


async def test_answer_after_old_execution_deadline_remains_bound_to_open_question(control_runtime):
    r = control_runtime
    root = r.root(children=2)
    first, second = child_rows(r, root["task_id"])
    r.question(first["id"], "long-question")
    with r.core.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.root_task_id == root["task_id"]).values(
            deadline=(r.core.clock.now() - timedelta(hours=1)).isoformat()))
        conn.execute(update(waits).where(waits.c.id == "long-question").values(
            expires_at=(r.core.clock.now() + timedelta(hours=20)).isoformat()))
    await r.receive(r.event("回答 " + first["public_id"] + " 继续"))
    with r.core.db.engine.connect() as conn:
        row = conn.execute(select(inputs)).mappings().one()
        assert row["task_id"] == first["id"] and row["question_id"] == "long-question"
        assert row["kind"] == "answer" and row["text"] == "继续"
        assert not conn.execute(select(inputs).where(inputs.c.task_id == second["id"])).first()
        assert conn.execute(select(waits.c.state).where(waits.c.id == "long-question")).scalar_one() == "CONSUMED"


async def test_second_reply_to_combined_questions_is_not_auto_rebound(control_runtime):
    r = control_runtime
    root = r.root(children=2)
    first, second = child_rows(r, root["task_id"])
    r.question(first["id"], "first-question")
    r.question(second["id"], "second-question")
    message = r.question_notice()
    await r.receive(r.event("回答 " + first["public_id"] + " 参数甲", reply_to=message))
    with r.core.db.engine.connect() as conn:
        assert conn.scalar(select(inputs.c.question_id)) == "first-question"
    await r.receive(r.event("补充说明：第一项的参数甲要保留", reply_to=message))
    with r.core.db.engine.connect() as conn:
        assert conn.scalar(select(waits.c.state).where(waits.c.id == "second-question")) == "WAITING"


@pytest.mark.parametrize("legacy", [False, True])
async def test_question_relay_send_starts_answer_window(control_runtime, legacy):
    from elephant.domain import SendReceipt

    r = control_runtime
    r.root()
    worker = r.core.tasks.claim("worker")
    ident = worker["run_id"], worker["fencing_token"], "worker"
    r.core.tasks.ask(*ident, {"question_id": "review-real-wait", "text": "请确认参数", "checkpoint": "waiting"})
    r.core.tasks.submit(*ident, {"status": "waiting_input", "summary": "等待参数"}, receipt={
        "kind": "run_yielded", "run_id": worker["run_id"], "session_id": worker["session_id"],
        "session_revision": "review-yield", "input_ids": worker["context"]["source_input_ids"],
        "inflight": 0, "tools_paired": True})
    r.question_notice(sent=False)
    message = "worker-question-relay"
    with r.core.db.transaction() as conn:
        delivery_id = conn.scalar(select(outbox.c.id).join(notifications, notifications.c.id == outbox.c.origin_id)
            .where(notifications.c.purpose == "question"))
        if legacy:
            conn.execute(update(waits).where(waits.c.id == "review-real-wait")
                .values(notification_id="review-real-wait"))
        envelope = OutboundEnvelope.model_validate_json(conn.scalar(select(outbox.c.envelope_json)
            .where(outbox.c.id == delivery_id)))
    assert r.core.pre_send(envelope)
    r.core.outbox._receipt(delivery_id, SendReceipt(state="SENT", platform_message_id=message))
    with r.core.db.engine.connect() as conn:
        wait = conn.execute(select(waits).where(waits.c.id == "review-real-wait")).mappings().one()
        assert wait["delivered_at"] == r.core.clock.now().isoformat()
        assert wait["expires_at"] == (r.core.clock.now() + timedelta(hours=1)).isoformat()
