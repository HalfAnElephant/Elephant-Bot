import hashlib
import json

import pytest
from sqlalchemy import select

from qqbot.app import create_app
from qqbot.config import Settings
from qqbot.storage.schema import inbox, outbox
from qqbot.storage.task_schema import inputs, tasks


@pytest.fixture
async def core(tmp_path, nap_config, clock):
    value = create_app(Settings(database=str(tmp_path / "core.db"), adapters=(nap_config,)),
                       clock=clock, run_background=False).state.core
    value.adapters["nap"].account_online = True
    yield value
    await value.stop()
    value.db.close()


def incoming(core, packet, number, text, *, mention=True, sender=300, reply_to=None):
    message = [{"type": "text", "data": {"text": text}}]
    if mention:
        message.insert(0, {"type": "at", "data": {"qq": "100"}})
    if reply_to:
        message.insert(0, {"type": "reply", "data": {"id": reply_to}})
    event = core.adapters["nap"].normalize({**packet, "message_id": number, "message": message,
        "user_id": sender, "time": int(core.clock.now().timestamp())})
    return core.inbox.accept(event)[0]


async def begin(core, packet, number=2800, text="查 PR"):
    request = incoming(core, packet, number, text)
    await core.process(request)
    return request, core.tasks.claim("worker")


async def test_natural_filter_and_correction_steer_original_task(core, nap_event):
    _, claim = await begin(core, nap_event)
    for number, text in [(2801, "只看我创建的"), (2802, "仓库改成 org/other")]:
        supplement = incoming(core, nap_event, number, text, mention=False)
        await core.process(supplement)
        pending = core.tasks.take_inputs(claim["run_id"], claim["fencing_token"], "worker")
        assert [item["text"] for item in pending] == [text]
        assert core.tasks.acknowledge_input(claim["run_id"], claim["fencing_token"], "worker",
            {"input_id": pending[0]["id"]})["accepted"]
    with core.db.engine.connect() as conn:
        assert len(conn.execute(select(tasks)).all()) == 1
        assert set(conn.execute(select(inputs.c.state)).scalars()) == {"STEERED"}
    independent = incoming(core, nap_event, 2803, "我想买相机")
    await core.process(independent)
    with core.db.engine.connect() as conn:
        goals = conn.execute(select(tasks.c.goal).order_by(tasks.c.created_at, tasks.c.source)).scalars().all()
    assert set(goals) == {"查 PR", "我想买相机"}


@pytest.mark.parametrize("finish_timing", ["before_input", "during_input", "after_input"])
async def test_finish_boundary_preserves_supplement(core, nap_event, monkeypatch, finish_timing):
    request, claim = await begin(core, nap_event)
    result = {"status": "completed", "summary": "已检查现有列表。"}
    if finish_timing == "before_input":
        core.tasks.submit(claim["run_id"], claim["fencing_token"], "worker", result)
    elif finish_timing == "during_input":
        original = core.tasks.add_input

        def finish_then_input(*args, **kwargs):
            core.tasks.submit(claim["run_id"], claim["fencing_token"], "worker", result)
            return original(*args, **kwargs)

        monkeypatch.setattr(core.tasks, "add_input", finish_then_input)
    supplement = incoming(core, nap_event, 2810, "只看我创建的", mention=False)
    await core.process(supplement)
    if finish_timing == "after_input":
        assert core.tasks.submit(claim["run_id"], claim["fencing_token"], "worker", result)["state"] == "QUEUED"
    resumed = core.tasks.claim("worker")
    if finish_timing == "after_input":
        assert resumed["task_id"] == claim["task_id"]
        assert [item["text"] for item in resumed["continuation"]["inputs"]] == ["只看我创建的"]
    else:
        assert resumed["task_id"] != claim["task_id"]
        assert resumed["context"]["continuation_of_task_id"] == claim["task_id"]
        assert "查 PR" in resumed["goal"] and "只看我创建的" in resumed["goal"]
        assert request.event_id in {source["id"] for source in resumed["context"]["source_manifest"]}
    with core.db.engine.connect() as conn:
        assert conn.execute(select(inbox.c.state).where(inbox.c.id == supplement.event_id)).scalar_one() == "DONE"


async def test_recent_terminal_continuation_keeps_revocation_boundary(core, nap_event):
    request, claim = await begin(core, nap_event)
    core.tasks.submit(claim["run_id"], claim["fencing_token"], "worker",
        {"status": "completed", "summary": "已检查。"})
    core.social.revoke_source(core.access(request), request.event_id)
    supplement = incoming(core, nap_event, 2811, "只看我创建的", mention=False)
    await core.process(supplement)
    with core.db.engine.connect() as conn:
        assert len(conn.execute(select(tasks)).all()) == 1
        state = conn.execute(select(inbox.c.state, inbox.c.error).where(inbox.c.id == supplement.event_id)).one()
    assert state.state == "FAILED" and state.error == "PermissionError"


async def test_multiple_continue_candidates_require_selection(core, nap_event):
    _, first = await begin(core, nap_event, 2820)
    second_request, second = await begin(core, nap_event, 2821)
    vague = incoming(core, nap_event, 2822, "继续", mention=False)
    await core.process(vague)
    with core.db.engine.connect() as conn:
        assert conn.execute(select(inputs.c.id)).first() is None
        assert len(conn.execute(select(tasks)).all()) == 2
        text = conn.execute(select(outbox.c.envelope_json).where(outbox.c.origin_id == vague.event_id)).scalar_one()
    assert "任务编号" in text
    with core.db.engine.connect() as conn:
        public_id = conn.execute(select(tasks.c.public_id).where(tasks.c.id == first["task_id"])).scalar_one()
    chosen = incoming(core, nap_event, 2823, f"继续 {public_id}", mention=False)
    await core.process(chosen)
    assert len(core.tasks.take_inputs(first["run_id"], first["fencing_token"], "worker")) == 1
    assert core.tasks.take_inputs(second["run_id"], second["fencing_token"], "worker") == []
    quoted = incoming(core, nap_event, 2824, "只看我创建的", mention=False, reply_to=second_request.platform_message_id)
    await core.process(quoted)
    pending = core.tasks.take_inputs(second["run_id"], second["fencing_token"], "worker")
    assert [item["text"] for item in pending] == ["只看我创建的"]


async def test_natural_cancel_and_other_speaker_do_not_steer(core, nap_event):
    _, claim = await begin(core, nap_event)
    other = incoming(core, nap_event, 2830, "只看我创建的", mention=False, sender=301)
    await core.process(other)
    with core.db.engine.connect() as conn:
        assert conn.execute(select(inputs.c.id)).first() is None
    cancel = incoming(core, nap_event, 2831, "不用查了", mention=False)
    await core.process(cancel)
    assert core.tasks.control(claim["run_id"], claim["fencing_token"], "worker")["state"] == "CANCEL_REQUESTED"


async def test_topic_short_new_goals_do_not_inherit_weather(core, nap_event):
    weather = incoming(core, nap_event, 2840, "嘉定今天的天气")
    initial = core.contexts.build(core.access(weather), core.contexts.turn(weather))
    tomorrow = incoming(core, nap_event, 2841, "那明天呢")
    followup = core.contexts.build(core.access(tomorrow), core.contexts.turn(tomorrow))
    assert initial["topic"] == followup["topic"]
    assert "location:嘉定" in followup["seed"]["topic"]["entities"]
    for number, text in [(2842, "我想买相机"), (2843, "帮我看看服务器日志")]:
        message = incoming(core, nap_event, number, text)
        context = core.contexts.build(core.access(message), core.contexts.turn(message))
        assert context["topic"] != initial["topic"]
        assert "location:嘉定" not in context["seed"]["topic"]["entities"]


@pytest.mark.parametrize("before_settle", [False, True])
async def test_natural_external_wait_supplement_keeps_waiting_for_observation(core, nap_event, before_settle):
    _, claim = await begin(core, nap_event)
    core.tasks.ask(claim["run_id"], claim["fencing_token"], "worker", {
        "kind": "external", "question_id": "router-external", "text": "等待外部检查。",
        "checkpoint": "No changes", "condition": {"capability": "github.issue.read", "arguments": {},
            "path": "state", "equals": "closed"}})
    result = {"status": "waiting_external", "summary": "等待外部检查。"}
    if not before_settle:
        core.tasks.submit(claim["run_id"], claim["fencing_token"], "worker", result)
    supplement = incoming(core, nap_event, 2850, "只看我创建的", mention=False)
    await core.process(supplement)
    if before_settle:
        assert core.tasks.submit(claim["run_id"], claim["fencing_token"], "worker", result)["state"] == "WAITING_EXTERNAL"
    with core.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.state)).scalar_one() == "WAITING_EXTERNAL"
        assert conn.execute(select(inputs.c.text)).scalar_one() == "只看我创建的"
    assert core.tasks.observe_waiter({"wait_id": "router-external", "revision": 1,
        "source_id": "external:" + hashlib.sha256(b"router-externalclosed").hexdigest(), "observation": "closed"})["woken"]
    resumed = core.tasks.claim("worker")
    assert resumed["task_id"] == claim["task_id"]
    assert {item["text"] for item in resumed["continuation"]["inputs"]} == {"只看我创建的", "closed"}


async def test_explicit_reply_restores_prior_topic_after_switch(core, nap_event):
    from sqlalchemy import update

    weather = incoming(core, nap_event, 2860, "嘉定今天的天气")
    task = core.submit_turn(weather)
    original_topic = json.loads(task["context_json"])["topic"]
    with core.db.transaction() as conn:
        core.tasks._notification(conn, task, None, "嘉定今天晴。", "progress", "important", "weather-reply")
        conn.execute(update(outbox).where(outbox.c.origin_id == "weather-reply").values(
            state="SENT", platform_message_id="weather-message", sent_at=core.clock.now().isoformat()))
    camera = incoming(core, nap_event, 2861, "我想买相机")
    assert core.contexts.build(core.access(camera), core.contexts.turn(camera))["topic"] != original_topic
    reference = incoming(core, nap_event, 2862, "那明天呢", reply_to="weather-message")
    context = core.contexts.build(core.access(reference), core.contexts.turn(reference))
    assert context["topic"] == original_topic
    assert "location:嘉定" in context["seed"]["topic"]["entities"]


@pytest.mark.parametrize("surface", ["private", "group"])
@pytest.mark.parametrize("control", ["取消", "先停一下", "换个话题，帮我看看相机"])
async def test_waiting_input_routes_controls_before_answers(core, nap_event, surface, control):
    packet = {**nap_event, "message_type": surface}
    if surface == "private":
        packet.pop("group_id")
    _, claim = await begin(core, packet)
    core.tasks.ask(claim["run_id"], claim["fencing_token"], "worker", {
        "question_id": "control-question", "text": "哪个仓库？", "checkpoint": "No writes"})
    core.tasks.submit(claim["run_id"], claim["fencing_token"], "worker", {
        "status": "waiting_input", "summary": "等待用户回答。"})
    control_event = incoming(core, packet, 2870, control, mention=surface == "group")
    await core.process(control_event)
    with core.db.engine.connect() as conn:
        assert conn.execute(select(inputs.c.id).where(inputs.c.task_id == claim["task_id"])).first() is None
        state = conn.execute(select(tasks.c.state).where(tasks.c.id == claim["task_id"])).scalar_one()
        created = conn.execute(select(tasks).where(tasks.c.source == control_event.event_id)).mappings().one()
        assert conn.execute(select(inbox.c.state).where(inbox.c.id == control_event.event_id)).scalar_one() == "DONE"
    if control.startswith("换个话题"):
        assert state == "WAITING_INPUT" and created["goal"] == control
    else:
        assert state == "CANCELLED" and json.loads(created["context_json"])["control_response"]


async def test_waiting_input_explicit_target_and_multiple_cancel_candidates_do_not_guess(core, nap_event):
    from qqbot.storage.task_schema import waits

    claims = []
    for index in range(2):
        _, claim = await begin(core, nap_event, 2880 + index,
            text="查 PR" if index == 0 else "换个话题，查 PR")
        core.tasks.ask(claim["run_id"], claim["fencing_token"], "worker", {
            "question_id": f"cancel-question-{index}", "text": "哪个仓库？", "checkpoint": "No writes"})
        core.tasks.submit(claim["run_id"], claim["fencing_token"], "worker", {
            "status": "waiting_input", "summary": "等待用户回答。"})
        claims.append(claim)
    vague = incoming(core, nap_event, 2882, "取消")
    await core.process(vague)
    with core.db.engine.connect() as conn:
        assert set(conn.execute(select(tasks.c.state)).scalars()) == {"WAITING_INPUT"}
        assert conn.execute(select(inputs.c.id)).first() is None
        assert "任务编号" in conn.execute(select(outbox.c.envelope_json).where(outbox.c.origin_id == vague.event_id)).scalar_one()
        public_id = conn.execute(select(tasks.c.public_id).where(tasks.c.id == claims[1]["task_id"])).scalar_one()
    explicit = incoming(core, nap_event, 2883, f"取消 {public_id}")
    await core.process(explicit)
    with core.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == claims[0]["task_id"])).scalar_one() == "WAITING_INPUT"
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == claims[1]["task_id"])).scalar_one() == "CANCELLED"
        assert set(conn.execute(select(waits.c.state)).scalars()) == {"WAITING"}
        assert conn.execute(select(inputs.c.id)).first() is None


@pytest.mark.parametrize("outcome", ["partial", "failed_notice", "partial_verified", "verified"])
async def test_terminal_natural_continue_preserves_unresolved_ledger_and_skill_version(core, nap_event, outcome):
    from qqbot.capabilities import Action, Capability, CapabilityRegistry, ToolContext, ToolGateway
    from qqbot.harness.gateway import RemoteLedger

    verified = outcome in {"verified", "partial_verified"}
    fresh = outcome == "verified"
    request, original = await begin(core, nap_event)
    core.tasks.bind_skills(original["run_id"], original["fencing_token"], "worker", {"skill_version": "a" * 64})
    dispatched = []
    registry = CapabilityRegistry()

    async def execute(arguments, context):
        dispatched.append(arguments)
        return {"url": "https://github.com/org/repo/issues/1"}

    registry.register(Capability("fixture.create", "Create issue"), execute,
        lambda arguments, context: Action("github.issue.create", ("repo:github.com/org/repo",), "write"))

    def gateway(claim):
        async def call(operation, payload):
            assert operation == "ledger"
            return await core.tasks.ledger(claim["run_id"], claim["fencing_token"], "worker", payload)

        def authority(context):
            return core.tasks.authorize(claim["run_id"], claim["fencing_token"], "worker", {"action": "task.execute"})["allowed"]

        def permission(context, action):
            return core.tasks.authorize(claim["run_id"], claim["fencing_token"], "worker",
                {"action": action.name, "resources": action.resources, "effect": action.effect})["allowed"]

        return ToolGateway(registry, authority=authority, permission=permission, ledger=RemoteLedger(call),
            verify=(lambda context, action, arguments, data: ("VERIFIED", data)) if verified else None)

    def context(claim):
        return ToolContext(claim["task_id"], claim["run_id"], claim["fencing_token"], core.access(request), "/tmp")

    arguments = {"title": "same write"}
    first = await gateway(original).invoke("fixture.create", arguments, context(original), operation_id="before-continuation")
    assert first.status == ("verified" if verified else "unknown") and len(dispatched) == 1
    if outcome == "failed_notice":
        core.tasks.submit(original["run_id"], original["fencing_token"], "worker", {
            "status": "failed", "summary": "执行中断。", "agent_authored": False})
        notice = core.tasks.claim("worker")
        assert notice["notification_status"] == "failed"
        core.tasks.submit(notice["run_id"], notice["fencing_token"], "worker", {
            "status": "failed", "summary": "执行已经中断，写入结果尚未核验。"})
    else:
        core.tasks.submit(original["run_id"], original["fencing_token"], "worker", {
            "status": "completed" if fresh else "partial", "summary": "已核验。" if verified else "写入结果尚未核验。"})
    continuation = incoming(core, nap_event, 2890, "继续", mention=False)
    await core.process(continuation)
    resumed = core.tasks.claim("worker")
    assert resumed["task_id"] != original["task_id"]
    assert "继续" in resumed["goal"]
    recorded = await core.tasks.ledger(resumed["run_id"], resumed["fencing_token"], "worker", {"operation": "list"})
    with core.db.engine.connect() as conn:
        retry_of = conn.execute(select(tasks.c.retry_of_task_id).where(tasks.c.id == resumed["task_id"])).scalar_one()
    if fresh:
        assert retry_of is None and recorded == []
    else:
        assert retry_of == original["task_id"]
        assert resumed["context"]["skill_version"] == "a" * 64
        expected = "VERIFIED" if verified else "UNKNOWN"
        assert [(row["id"], row["state"]) for row in recorded] == [("before-continuation", expected)]
    repeated = await gateway(resumed).invoke("fixture.create", arguments, context(resumed), operation_id="after-continuation")
    assert repeated.status == ("verified" if verified else "unknown")
    assert len(dispatched) == (2 if fresh else 1)


@pytest.mark.parametrize("surface", ["private", "group"])
async def test_pause_asks_for_confirmation_without_cancelling_or_answering_wait(core, nap_event, surface):
    packet = {**nap_event, "message_type": surface}
    if surface == "private":
        packet.pop("group_id")
    _, claim = await begin(core, packet)
    core.tasks.ask(claim["run_id"], claim["fencing_token"], "worker", {
        "question_id": "pause-question", "text": "哪个仓库？", "checkpoint": "No writes"})
    core.tasks.submit(claim["run_id"], claim["fencing_token"], "worker", {
        "status": "waiting_input", "summary": "等待用户回答。"})
    pause = incoming(core, packet, 2891, "暂停", mention=surface == "group")
    await core.process(pause)
    with core.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.state)).scalar_one() == "WAITING_INPUT"
        assert conn.execute(select(inputs.c.id)).first() is None
        text = conn.execute(select(outbox.c.envelope_json).where(outbox.c.origin_id == pause.event_id)).scalar_one()
    assert "请确认" in text


@pytest.mark.parametrize("text", ["继续", "取消", "只看我创建的"])
async def test_unanchored_control_does_not_create_empty_goal_task(core, nap_event, text):
    value = incoming(core, nap_event, 2892, text)
    await core.process(value)
    with core.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.id)).first() is None
        assert conn.execute(select(inputs.c.id)).first() is None
        assert "任务编号" in conn.execute(select(outbox.c.envelope_json).where(outbox.c.origin_id == value.event_id)).scalar_one()


@pytest.mark.parametrize("surface", ["private", "group"])
@pytest.mark.parametrize("new_request", ["我想买相机", "帮我看看服务器日志"])
async def test_independent_request_bypasses_unanswered_question(core, nap_event, surface, new_request):
    packet = {**nap_event, "message_type": surface}
    if surface == "private":
        packet.pop("group_id")
    _, claim = await begin(core, packet)
    core.tasks.ask(claim["run_id"], claim["fencing_token"], "worker", {
        "question_id": "independent-question", "text": "哪个仓库？", "checkpoint": "No writes"})
    core.tasks.submit(claim["run_id"], claim["fencing_token"], "worker", {
        "status": "waiting_input", "summary": "等待用户回答。"})
    message = incoming(core, packet, 2900, new_request, mention=surface == "group")
    await core.process(message)
    with core.db.engine.connect() as conn:
        assert conn.execute(select(inputs.c.id)).first() is None
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == claim["task_id"])).scalar_one() == "WAITING_INPUT"
        assert conn.execute(select(tasks.c.goal).where(tasks.c.source == message.event_id)).scalar_one() == new_request


@pytest.mark.parametrize("surface", ["private", "group"])
@pytest.mark.parametrize("answer", ["org/repo", "我想用 org/repo", "请你用 org/repo", "中午吃什么？"])
async def test_wait_answer_and_ambiguous_question_remain_distinct(core, nap_event, surface, answer):
    packet = {**nap_event, "message_type": surface}
    if surface == "private":
        packet.pop("group_id")
    _, claim = await begin(core, packet)
    core.tasks.ask(claim["run_id"], claim["fencing_token"], "worker", {
        "question_id": "answer-question", "text": "哪个仓库？", "checkpoint": "No writes"})
    core.tasks.submit(claim["run_id"], claim["fencing_token"], "worker", {
        "status": "waiting_input", "summary": "等待用户回答。"})
    message = incoming(core, packet, 2901, answer, mention=surface == "group")
    await core.process(message)
    with core.db.engine.connect() as conn:
        if answer.endswith("？"):
            assert conn.execute(select(inputs.c.id)).first() is None
            assert conn.execute(select(tasks.c.state)).scalar_one() == "WAITING_INPUT"
            assert "新问题" in conn.execute(select(outbox.c.envelope_json).where(outbox.c.origin_id == message.event_id)).scalar_one()
        else:
            assert conn.execute(select(inputs.c.text)).scalar_one() == answer
            assert conn.execute(select(tasks.c.state)).scalar_one() == "QUEUED"


async def test_generic_request_with_explicit_task_id_still_answers_wait(core, nap_event):
    _, claim = await begin(core, nap_event)
    core.tasks.ask(claim["run_id"], claim["fencing_token"], "worker", {
        "question_id": "targeted-question", "text": "哪个仓库？", "checkpoint": "No writes"})
    core.tasks.submit(claim["run_id"], claim["fencing_token"], "worker", {
        "status": "waiting_input", "summary": "等待用户回答。"})
    with core.db.engine.connect() as conn:
        public_id = conn.execute(select(tasks.c.public_id)).scalar_one()
    message = incoming(core, nap_event, 2902, f"{public_id} 帮我用 org/repo", mention=False)
    await core.process(message)
    with core.db.engine.connect() as conn:
        assert conn.execute(select(inputs.c.text)).scalar_one() == message.text
        assert conn.execute(select(tasks.c.state)).scalar_one() == "QUEUED"
