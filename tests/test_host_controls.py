"""Host controls resolve exact task identity without model capacity or guessed answers."""
import json
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import insert, select, update

from qqbot.app import create_app
from qqbot.config import OwnerConfig, QQOwnerBinding, Settings
from qqbot.permissions import PermissionRule
from qqbot.storage.agent_schema import dialogue_inputs
from qqbot.storage.schema import audit_events, outbox
from qqbot.storage.task_schema import inputs, model_requests, notifications, tasks, waits


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

    def question_notice():
        claim = core.sessions.claim("question-leader")
        assert claim["purpose"] == "question"
        core.leader.complete(claim["run_id"], claim["fencing_token"], "question-leader",
            {"status": "completed", "summary": "请选择待答问题的参数"}, stopped=True, quiescent=False,
            receipt={"kind": "turn_finished", "session_id": claim["session_id"], "run_id": claim["run_id"],
                     "session_revision": "question-fixture", "input_ids": claim["source_ids"], "inflight": 0, "tools_paired": True})
        message = str(uuid4())
        with core.db.transaction() as conn:
            notice = conn.execute(select(notifications).where(notifications.c.run_id == claim["run_id"])).mappings().one()
            conn.execute(update(outbox).where(outbox.c.origin_id == notice["id"]).values(state="SENT", platform_message_id=message))
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


async def test_cancel_without_id_and_status_remain_host_controls_at_full_capacity(control_runtime):
    r = control_runtime
    root = r.root()
    r.core.tasks.claim("busy-worker")
    status = await r.receive(r.event("查看 " + root["public_id"] + " 的进度"))
    assert "正在处理" in r.response(status) and "检查项目" in r.response(status)
    assert root["public_id"] not in r.response(status) and "WAITING_CHILDREN" not in r.response(status)
    cancel = await r.receive(r.event("/取消"))
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


async def test_notification_reference_precedes_current_dialogue_and_explicit_id_precedes_reference(control_runtime):
    r = control_runtime
    first = r.root(goal="被引用的任务")
    message = r.notice(first["task_id"])
    second = r.root(title="当前对话", goal="当前任务")
    await r.receive(r.event("取消刚才那个", reply_to=message))
    assert task_state(r, first["task_id"]) == "CANCELLED"
    assert task_state(r, second["task_id"]) == "WAITING_CHILDREN"
    await r.receive(r.event("取消 " + second["public_id"], reply_to=message))
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


async def test_active_notification_reply_becomes_bound_input_and_closed_reply_becomes_new_turn(control_runtime):
    r = control_runtime
    root = r.root()
    message = r.notice(root["task_id"])
    source = await r.receive(r.event("只检查最近一周，保留问号？", reply_to=message))
    with r.core.db.engine.connect() as conn:
        item = conn.execute(select(inputs).where(inputs.c.task_id == root["task_id"])).mappings().one()
        assert item["source_id"] == source.event_id and item["text"] == source.text
        assert item["kind"] == "followup" and item["actor"] == source.principal.key
    r.no_model_input()
    with r.core.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.root_task_id == root["task_id"]).values(state="SUCCEEDED"))
    await r.receive(r.event("接下来讨论另一种方案", reply_to=message))
    with r.core.db.engine.connect() as conn:
        assert len(conn.execute(select(dialogue_inputs)).all()) == 1


async def test_multiple_open_questions_require_child_selection_and_preserve_exact_answer(control_runtime):
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
    answer = await r.receive(r.event("补充 " + first["public_id"] + " 选择A？"))
    with r.core.db.engine.connect() as conn:
        rows = conn.execute(select(inputs)).mappings().all()
        assert len(rows) == 1 and rows[0]["task_id"] == first["id"]
        assert rows[0]["question_id"] == "first-question" and rows[0]["text"] == "选择A？"
        assert rows[0]["actor"] == answer.principal.key
        assert conn.execute(select(waits.c.state).where(waits.c.id == "second-question")).scalar_one() == "WAITING"
    r.no_model_input()


async def test_leader_question_notice_binds_its_actual_wait_over_other_open_questions(control_runtime):
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
    r.question(second["id"], "new-unrelated-question")
    await r.receive(r.event("确认第一项参数", reply_to="leader-question-message"))
    with r.core.db.engine.connect() as conn:
        answered = conn.execute(select(inputs).where(inputs.c.question_id == "bound-question")).mappings().one()
        assert answered["task_id"] == first["id"]
        assert not conn.execute(select(inputs).where(inputs.c.task_id == second["id"])).first()
        assert conn.execute(select(waits.c.state).where(waits.c.id == "new-unrelated-question")).scalar_one() == "WAITING"
    r.no_model_input()


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
    original_cancel = r.core.tasks.cancel

    def lost_reply(reference, access):
        original_cancel(reference, access)
        raise RuntimeError("simulated response loss after committed cancellation")

    monkeypatch.setattr(r.core.tasks, "cancel", lost_reply)
    source = await r.receive(r.event("取消刚才那个"))
    assert task_state(r, first["task_id"]) == "CANCELLED" and r.response(source) is None
    second = r.root(title="随后创建的新任务")
    monkeypatch.setattr(r.core.tasks, "cancel", original_cancel)
    await r.core.process(source)
    assert r.response(source) == "已取消。"
    assert task_state(r, second["task_id"]) == "WAITING_CHILDREN"
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
    original_add = r.core.tasks.add_input

    def lost_reply(*args, **kwargs):
        original_add(*args, **kwargs)
        raise RuntimeError("simulated response loss after committed answer")

    monkeypatch.setattr(r.core.tasks, "add_input", lost_reply)
    source = await r.receive(r.event("原始答案？", reply_to=message))
    assert r.response(source) is None
    with r.core.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.root_task_id == root["task_id"]).values(state="SUCCEEDED"))
    r.root(title="随后对话")
    monkeypatch.setattr(r.core.tasks, "add_input", original_add)
    await r.core.process(source)
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
        from qqbot.storage.agent_schema import dialogues
        identifier = conn.execute(select(dialogues.c.id)).scalar_one()
    assert await r.core.command(r.event("/切换对话 " + identifier)) == "当前对话：研究资料"


@pytest.mark.parametrize(("error", "expected"), [
    (PermissionError("task:T-private belongs to another subject"), "无法操作"),
    (KeyError("T-internal"), "没有找到"),
    (ValueError("capacity_full: /var/lib/qqbot/private"), "暂时无法处理"),
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
