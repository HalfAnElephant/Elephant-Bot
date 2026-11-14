"""Progress is derived from receipts and numeric choices retain their original scope."""
import json
from uuid import uuid4

import pytest
from sqlalchemy import select, update
from test_agent_runtime import root_goal
from test_agent_runtime import runtime as runtime  # noqa: PLC0414
from test_host_controls import child_rows, task_state
from test_host_controls import control_runtime as control_runtime  # noqa: PLC0414
from test_project_scopes import project_core as project_core  # noqa: PLC0414

from qqbot.storage.agent_schema import agent_events, leader_inbox
from qqbot.storage.task_schema import inputs, runs, tasks


def report(runtime, claim, call_id, *, phase="checking", evidence=("observation",), text="正在核实资料"):
    return runtime.service.task_operation(claim["run_id"], claim["fencing_token"], "worker", {
        "operation": "tasks.report_progress", "call_id": call_id,
        "arguments": {"phase": phase, "evidence_refs": list(evidence), "public_text": text}})


def observe(runtime, claim, reference="observation"):
    return runtime.service.observation(claim["run_id"], claim["fencing_token"], "worker", {
        "id": reference, "data": {"checked": 1}, "sha256": "a" * 64,
        "retrieved_at": runtime.clock.now().isoformat()})


def view(runtime, root):
    with runtime.db.engine.connect() as conn:
        task = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
        return runtime.service.graph.status(conn, task, access=runtime.access)


def test_projection_has_receipts_waiting_budget_delivery_and_effective_time(runtime):
    root = root_goal(runtime)
    claim = runtime.service.claim("worker")
    observe(runtime, claim)
    report(runtime, claim, "fact")
    first = view(runtime, root)["progress"]
    assert first["verified_counts"]["observation"] == 1
    assert first["current_action"] == "正在执行"
    assert first["waiting_for"][0]["kind"] == "children"
    assert first["budget"]["scope"] == "tree_usage"
    assert not first["delivery"]["execution_complete"]
    runtime.clock.advance(20)
    runtime.service.renew(claim["run_id"], claim["fencing_token"], "worker")
    after = view(runtime, root)["progress"]
    assert after["last_effective_progress_at"] == first["last_effective_progress_at"]


def test_same_facts_reworded_or_reordered_do_not_wake_again(runtime):
    root_goal(runtime)
    claim = runtime.service.claim("worker")
    observe(runtime, claim, "a")
    observe(runtime, claim, "b")
    assert report(runtime, claim, "first", evidence=("a", "b"))["accepted"]
    assert report(runtime, claim, "second", evidence=("b", "a"), text="换一种说法") == {
        "accepted": False, "reason": "coalesced"}
    report(runtime, claim, "other-phase", phase="verification", evidence=("a",))
    assert not report(runtime, claim, "old-phase-again", evidence=("a",))["accepted"]
    with runtime.db.engine.connect() as conn:
        assert len(conn.execute(select(agent_events).where(agent_events.c.kind == "milestone")).all()) == 2


def test_same_phase_new_facts_merge_only_pending_milestones(runtime):
    root_goal(runtime)
    claim = runtime.service.claim("worker")
    observe(runtime, claim, "a")
    report(runtime, claim, "first", evidence=("a",))
    observe(runtime, claim, "b")
    report(runtime, claim, "second", evidence=("b",))
    with runtime.db.engine.connect() as conn:
        pending = conn.execute(select(leader_inbox).where(leader_inbox.c.handled_at.is_(None))).mappings().all()
        assert len(pending) == 1 and pending[0]["event_id"] == "second"
        payload = json.loads(conn.execute(select(agent_events.c.payload_json).where(agent_events.c.id == "second")).scalar_one())
        assert set(payload["evidence_refs"]) == {"a", "b"}
        old = conn.execute(select(leader_inbox.c.output_ref).where(leader_inbox.c.event_id == "first")).scalar_one()
        assert old == "SUPERSEDED"
    assert not report(runtime, claim, "combined-again", evidence=("a", "b"))["accepted"]


def test_progress_accepts_registered_artifact_but_rejects_path_claim(runtime):
    root_goal(runtime)
    claim = runtime.service.claim("worker")
    artifact_id = str(uuid4())
    artifact = {"id": artifact_id, "relative_path": "outputs/answer.txt", "mime_type": "text/plain",
                "size": 4, "sha256": "a" * 64}
    with runtime.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.id == claim["task_id"]).values(
            result_json=json.dumps({"status": "partial", "summary": "文件已登记", "artifacts": [artifact]})))
    with pytest.raises(PermissionError, match="unverified"):
        report(runtime, claim, "forged-path", evidence=("outputs/answer.txt",))
    assert report(runtime, claim, "actual-artifact", evidence=("artifact:" + artifact_id,))["accepted"]
    assert not report(runtime, claim, "artifact-alias", evidence=(artifact_id,))["accepted"]


def test_memory_replan_hides_old_progress_details(runtime):
    root = root_goal(runtime)
    claim = runtime.service.claim("worker")
    observe(runtime, claim)
    report(runtime, claim, "old", text="不能泄露的旧事实")
    with runtime.db.transaction() as conn:
        task = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
        context = json.loads(task["context_json"]) | {"memory_replan_pending": {"operation": "forget"}}
        conn.execute(update(tasks).where(tasks.c.id == root["task_id"]).values(context_json=json.dumps(context)))
    snapshot = view(runtime, root)
    assert snapshot["last_progress"] is None
    assert snapshot["progress"]["verified_completed"] == []
    assert snapshot["progress"]["last_effective_progress_at"] is None
    assert "不能泄露" not in json.dumps(snapshot, ensure_ascii=False)


def test_new_goal_revision_can_register_a_new_milestone(runtime):
    root = root_goal(runtime)
    claim = runtime.service.claim("worker")
    observe(runtime, claim)
    report(runtime, claim, "revision-zero")
    with runtime.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.root_task_id == root["task_id"]).values(goal_revision=1))
        conn.execute(update(runs).where(runs.c.id == claim["run_id"]).values(basis_goal_revision=1))
    assert report(runtime, claim, "revision-one")["accepted"]


async def test_numeric_cancel_keeps_snapshot_when_activity_order_changes(control_runtime):
    runtime = control_runtime
    first = runtime.root(goal="第一件事")
    second = runtime.root(goal="第二件事")
    source = await runtime.receive(runtime.event("取消刚才那个"))
    answer = runtime.response(source)
    assert "第一件事" in answer and "第二件事" in answer
    selected = next(line.split(".", 1)[0] for line in answer.splitlines() if "第二件事" in line)
    third = runtime.root(goal="第三件事")
    choice = await runtime.receive(runtime.event("选择" + selected))
    assert runtime.response(choice) == "已取消。"
    assert task_state(runtime, second["task_id"]) == "CANCELLED"
    assert task_state(runtime, first["task_id"]) == task_state(runtime, third["task_id"]) == "WAITING_CHILDREN"
    await runtime.core.process(choice)
    assert task_state(runtime, first["task_id"]) == "WAITING_CHILDREN"
    runtime.no_model_input()


async def test_numeric_question_choice_preserves_original_answer(control_runtime):
    runtime = control_runtime
    root = runtime.root(children=2)
    first, second = child_rows(runtime, root["task_id"])
    runtime.question(first["id"], "question-a")
    runtime.question(second["id"], "question-b")
    message = runtime.question_notice()
    source = await runtime.receive(runtime.event("请使用方案A", reply_to=message))
    answer = runtime.response(source)
    assert "1." in answer and "2." in answer
    selected = next(line.split(".", 1)[0] for line in answer.splitlines() if first["goal"] in line)
    choice = await runtime.receive(runtime.event("选" + selected))
    with runtime.core.db.engine.connect() as conn:
        rows = conn.execute(select(inputs)).mappings().all()
    assert len(rows) == 1 and rows[0]["task_id"] == first["id"]
    assert rows[0]["question_id"] == "question-a" and rows[0]["text"] == "请使用方案A"
    assert rows[0]["actor"] == choice.principal.key


async def test_numeric_selection_expires_and_never_follows_new_dialogue(control_runtime):
    runtime = control_runtime
    first = runtime.root(goal="第一件事")
    runtime.root(goal="第二件事")
    await runtime.receive(runtime.event("取消刚才那个"))
    runtime.core.clock.advance(301)
    expired = await runtime.receive(runtime.event("1"))
    assert "已过期" in runtime.response(expired)
    assert task_state(runtime, first["task_id"]) == "WAITING_CHILDREN"
    await runtime.receive(runtime.event("取消刚才那个"))
    runtime.root(goal="另一对话", title="新对话")
    switched = await runtime.receive(runtime.event("1"))
    assert "对话已切换" in runtime.response(switched)
    assert task_state(runtime, first["task_id"]) == "WAITING_CHILDREN"


@pytest.mark.parametrize(("state", "expected"), [
    ("CANCELLED", "已取消"), ("BLOCKED", "暂时无法继续"), ("FAILED", "这次未能完成"),
    ("PARTIAL", "已完成一部分"), ("EXPIRED", "已超时停止"),
])
def test_terminal_root_never_reports_waiting_for_verification(runtime, state, expected):
    root = root_goal(runtime)
    with runtime.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.parent_task_id == root["task_id"]).values(state="CANCELLED"))
        conn.execute(update(tasks).where(tasks.c.id == root["task_id"]).values(state=state))
    assert view(runtime, root)["progress"]["current_action"] == expected


@pytest.mark.parametrize("age", [0, 301])
async def test_numeric_reply_to_real_question_ignores_unrelated_cancel_selection(control_runtime, age):
    runtime = control_runtime
    first = runtime.root(goal="第一件事")
    second = runtime.root(goal="第二件事")
    await runtime.receive(runtime.event("取消刚才那个"))
    runtime.core.clock.advance(age)
    child = child_rows(runtime, first["task_id"])[0]
    runtime.question(child["id"], "actual-question")
    message = runtime.question_notice()
    source = await runtime.receive(runtime.event("2", reply_to=message))
    assert runtime.response(source) == "已收到补充内容。"
    assert task_state(runtime, first["task_id"]) == "WAITING_CHILDREN"
    assert task_state(runtime, second["task_id"]) == "WAITING_CHILDREN"
    with runtime.core.db.engine.connect() as conn:
        rows = conn.execute(select(inputs)).mappings().all()
    assert len(rows) == 1 and rows[0]["task_id"] == child["id"]
    assert rows[0]["question_id"] == "actual-question" and rows[0]["text"] == "2"


def control_deliveries(runtime, source):
    from qqbot.domain import OutboundEnvelope
    from qqbot.storage.schema import outbox
    with runtime.core.db.engine.connect() as conn:
        return [OutboundEnvelope.model_validate_json(raw) for raw in conn.execute(select(outbox.c.envelope_json).where(
            outbox.c.origin_id == source.event_id).order_by(outbox.c.part_no)).scalars()]


async def test_control_delivery_accepts_unchanged_facts_and_ignores_budget_heartbeat(control_runtime):
    runtime = control_runtime
    root = runtime.root(goal="检查旧项目")
    claim = runtime.core.tasks.claim("worker")
    progress_at = runtime.core.clock.now().isoformat()
    runtime.core.tasks.observation(claim["run_id"], claim["fencing_token"], "worker", {
        "id": "status-evidence", "data": {"checked": True}, "sha256": "a" * 64, "retrieved_at": progress_at})
    source = await runtime.receive(runtime.event("/任务 " + root["public_id"]))
    envelopes = control_deliveries(runtime, source)
    assert progress_at in "\n".join(item.text for item in envelopes)
    assert envelopes and all(runtime.core.controls.delivery_valid(item) is True for item in envelopes)
    runtime.core.clock.advance(5)
    runtime.core.tasks.renew(claim["run_id"], claim["fencing_token"], "worker")
    with runtime.core.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.root_task_id == root["task_id"]).values(
            budget_json=json.dumps({"active_seconds": 5, "model_calls": 1, "tool_calls": 1})))
    assert all(runtime.core.controls.delivery_valid(item) is True for item in envelopes)
    unrelated = envelopes[0].model_copy(update={"origin_id": "ordinary-chat"})
    assert runtime.core.controls.delivery_valid(unrelated) is None


@pytest.mark.parametrize("kind", ["status", "selection", "list"])
async def test_control_delivery_rejects_goal_revision_change(control_runtime, kind):
    runtime = control_runtime
    root = runtime.root(goal="旧项目私有标题")
    if kind == "selection":
        runtime.root(goal="另一个目标")
        text = "取消刚才那个"
    else:
        text = "/任务" if kind == "list" else "/任务 " + root["public_id"]
    source = await runtime.receive(runtime.event(text))
    envelopes = control_deliveries(runtime, source)
    assert all(runtime.core.controls.delivery_valid(item) is True for item in envelopes)
    with runtime.core.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.id == root["task_id"]).values(goal_revision=1, goal="新目标"))
    assert all(runtime.core.controls.delivery_valid(item) is False for item in envelopes)


@pytest.mark.parametrize("markdown", [False, True])
async def test_control_delivery_matches_rendered_markdown_and_plain_bubbles(control_runtime, markdown):
    runtime = control_runtime
    runtime.core.outbox.markdown_adapters = frozenset({"qq"}) if markdown else frozenset()
    root = runtime.root(goal="**项目标题**")
    with runtime.core.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.id == root["task_id"]).values(result_json=json.dumps({
            "status": "partial", "summary": "**第一段**|||第二段 [文档](https://example.com/docs)"})))
    source = await runtime.receive(runtime.event("/任务 " + root["public_id"]))
    envelopes = control_deliveries(runtime, source)
    assert len(envelopes) == 1 if markdown else len(envelopes) >= 2
    assert all(runtime.core.controls.delivery_valid(item) is True for item in envelopes)
    assert runtime.core.controls.delivery_valid(envelopes[0].model_copy(update={"text": "被替换的旧正文"})) is False
    assert runtime.core.controls.delivery_valid(envelopes[0].model_copy(update={"part_no": 100})) is False


async def test_control_status_body_and_fingerprint_share_one_transaction(control_runtime, monkeypatch):
    runtime = control_runtime
    root = runtime.root(goal="原目标")
    original = runtime.core.controls._status_response
    status = runtime.core.tasks.graph.status
    freeze = runtime.core.controls._freeze_delivery
    rendered, reads, frozen = [], [], []

    def read(conn, *args, **kwargs):
        reads.append((id(conn), conn.connection.driver_connection.in_transaction))
        return status(conn, *args, **kwargs)

    def capture(conn, *args, **kwargs):
        frozen.append((id(conn), conn.connection.driver_connection.in_transaction))
        return freeze(conn, *args, **kwargs)

    def render(view):
        response = original(view)
        rendered.append(view["goal_revision"])
        return response

    monkeypatch.setattr(runtime.core.controls, "_status_response", render)
    monkeypatch.setattr(runtime.core.tasks.graph, "status", read)
    monkeypatch.setattr(runtime.core.controls, "_freeze_delivery", capture)
    source = await runtime.receive(runtime.event("/任务 " + root["public_id"]))
    assert rendered == [0]
    assert reads == frozen and len(reads) == 1 and reads[0][1] is True
    envelope = control_deliveries(runtime, source)[0]
    assert runtime.core.controls.delivery_valid(envelope) is True
    with runtime.core.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.id == root["task_id"]).values(goal_revision=1))
    assert runtime.core.controls.delivery_valid(envelope) is False


def test_control_old_body_stays_invalid_after_memory_replan_completes(project_core):
    from test_project_scopes import host_event

    from qqbot.domain import OutboundEnvelope
    from qqbot.storage.memory_sync_schema import memory_owners
    from qqbot.storage.schema import outbox
    core = project_core
    root = core.enqueue_admin_task("旧记忆下的目标", idempotency_key="control-old-memory", workspace_id="alpha")
    source, _ = core.inbox.accept(host_event(core, "control-memory-status", "/任务 " + root["public_id"]))
    response = core.controls.command(source, "/任务", root["public_id"])
    core.reply(source, response)
    with core.db.engine.connect() as conn:
        envelope = OutboundEnvelope.model_validate_json(conn.execute(select(outbox.c.envelope_json).where(
            outbox.c.origin_id == source.event_id).order_by(outbox.c.part_no).limit(1)).scalar_one())
        task = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
    assert core.controls.delivery_valid(envelope) is True
    scope = core.knowledge.scope(source, workspace_id="alpha")
    original_context = json.loads(task["context_json"])
    with core.db.transaction() as conn:
        conn.execute(update(memory_owners).where(memory_owners.c.id == scope.key).values(epoch=1))
        conn.execute(update(tasks).where(tasks.c.id == root["task_id"]).values(context_json=json.dumps(
            original_context | {"memory_replan_pending": {"operation": "forget"}})))
    assert core.controls.delivery_valid(envelope) is False
    with core.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.id == root["task_id"]).values(goal_revision=1,
            context_json=json.dumps(original_context | {"memory_epoch": 1, "memory_replan": {"operation": "forget"}})))
    assert core.controls.delivery_valid(envelope) is False


async def test_control_cancel_confirmation_survives_business_context_revocation(control_runtime):
    runtime = control_runtime
    root = runtime.root(goal="可取消的目标")
    source = await runtime.receive(runtime.event("取消 " + root["public_id"]))
    envelope = control_deliveries(runtime, source)[0]
    with runtime.core.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.id == root["task_id"]).values(context_json=json.dumps({
            "source_manifest": [{"kind": "inbox", "id": "revoked-business-source", "digest": "a" * 64}]})))
    assert runtime.core.controls.delivery_valid(envelope) is True
