"""Active duplicate dispatches reuse a durable goal without another Worker."""
from uuid import uuid4

import pytest
from sqlalchemy import select, update
from test_agent_runtime import runtime as runtime  # noqa: PLC0414 - Register the shared pytest fixture.

from qqbot.domain.messages import TextSegment
from qqbot.runtime.task_graph import TaskGraph
from qqbot.storage.task_schema import tasks


def submit(r, key, *, spec=None, event=None, context=None, new_dialogue=None):
    event = event or r.event
    access = r.access.model_copy(update={"principal": event.principal, "conversation": event.conversation})
    with r.db.transaction() as conn:
        dialogue = r.sessions.dialogue(conn, event, new_title=new_dialogue)
        return r.service.graph.submit(conn, dialogue, event, access,
            spec or {"goal": "检查项目", "children": [{"goal": "检查配置", "delegation_key": key}]},
            key, r.sessions.binding(), context or {})


def task_count(r):
    with r.db.engine.connect() as conn:
        return len(conn.execute(select(tasks)).all())


def test_same_original_request_reuses_work_despite_model_rephrasing_and_new_source(runtime):
    r = runtime
    first = submit(r, "first")
    event = r.event.model_copy(update={"event_id": "second-input", "platform_message_id": "second-message"})
    rewritten = {"goal": "审查项目", "children": [{"goal": "核实配置文件", "delegation_key": "changed"}]}
    second = submit(r, "second", spec=rewritten, event=event)
    assert second["task_id"] == first["task_id"] and second["reused"] is True
    assert task_count(r) == 2
    r.service.graph = TaskGraph(r.service, r.service.graph.settings)
    with r.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.id == first["task_id"]).values(state="SUCCEEDED"))
    replayed = submit(r, "second", spec=rewritten, event=event)
    assert replayed["task_id"] == first["task_id"] and replayed["reused"]
    with pytest.raises(ValueError, match="dispatch key"):
        submit(r, "second", event=event)
    with r.db.engine.connect() as conn:
        task, intent = r.service.graph.dispatch_receipt(conn, r.sessions.settings.owner.id, "second")
        assert task["id"] == first["task_id"] and intent["goal"] == "审查项目"


def test_same_event_equivalent_goal_ignores_only_delegation_identity(runtime):
    r = runtime
    first = submit(r, "first")
    second = submit(r, "second")
    assert second["task_id"] == first["task_id"] and second["reused"]
    assert task_count(r) == 2


def test_same_event_distinct_goals_create_independent_work(runtime):
    r = runtime
    first = submit(r, "first")
    second = submit(r, "second", spec={"goal": "检查另一个项目",
        "children": [{"goal": "检查另一个项目的配置", "delegation_key": "second"}]})
    assert second["task_id"] != first["task_id"] and task_count(r) == 4


def test_changed_original_request_does_not_merge_when_model_omits_new_constraints(runtime):
    r = runtime
    first = submit(r, "first")
    event = r.event.model_copy(update={"event_id": "other", "segments": (TextSegment(text="请检查项目，只看最近一周"),)})
    second = submit(r, "second", event=event)
    assert second["task_id"] != first["task_id"] and task_count(r) == 4


@pytest.mark.parametrize("difference", ["goal", "checks", "outputs", "child", "image", "reply", "revision", "dialogue", "conversation", "principal", "memory"])
def test_distinct_constraints_context_or_scope_do_not_merge(runtime, difference):
    r = runtime
    first = submit(r, "first")
    spec = {"goal": "检查项目", "children": [{"goal": "检查配置", "delegation_key": "second"}]}
    event = r.event.model_copy(update={"event_id": str(uuid4()), "segments": (TextSegment(text="检查项目并采用新的约束"),)})
    context = {}
    new_dialogue = None
    if difference == "goal":
        spec["goal"] = "只检查最近一周"
    elif difference == "checks":
        spec["success_checks"] = ["observation"]
    elif difference == "outputs":
        spec["requested_outputs"] = ["report.txt"]
    elif difference == "child":
        spec["children"][0]["goal"] = "只检查测试配置"
    elif difference == "image":
        context["images"] = [{"source": "new", "url": "https://example.com/new.png"}]
    elif difference == "reply":
        event = event.model_copy(update={"reply_to": "different-reference"})
    elif difference == "revision":
        with r.db.transaction() as conn:
            conn.execute(update(tasks).where(tasks.c.id == first["task_id"]).values(goal_revision=1))
    elif difference == "dialogue":
        new_dialogue = "另一对话"
    elif difference == "conversation":
        event = event.model_copy(update={"conversation": event.conversation.model_copy(update={"remote_conversation_id": "other"})})
    elif difference == "principal":
        event = event.model_copy(update={"principal": event.principal.model_copy(update={"remote_user_id": "other"})})
    elif difference == "memory":
        context["memory_epoch"] = 2
    second = submit(r, "second", spec=spec, event=event, context=context, new_dialogue=new_dialogue)
    assert second["task_id"] != first["task_id"] and task_count(r) == 4


def test_same_image_with_new_event_identity_reuses_active_goal(runtime):
    r = runtime
    first = submit(r, "first", context={"images": [{"source": "first", "message_id": "one", "url": "https://example.com/a.png"}]})
    second = submit(r, "second", context={"images": [{"source": "second", "message_id": "two", "url": "https://example.com/a.png"}]})
    assert second["task_id"] == first["task_id"]


@pytest.mark.parametrize("state", ["SUCCEEDED", "FAILED", "CANCELLED", "CANCEL_REQUESTED", "NEEDS_RECONCILIATION"])
def test_closed_or_stopping_goal_allows_new_dispatch(runtime, state):
    r = runtime
    first = submit(r, "first")
    with r.db.transaction() as conn:
        conn.execute(update(tasks).where(tasks.c.id == first["task_id"]).values(state=state))
    assert submit(r, "second")["task_id"] != first["task_id"]


def test_duplicate_reuse_precedes_capacity_check_and_preserves_original_key_conflicts(runtime):
    r = runtime
    settings = r.service.graph.settings
    r.service.graph.settings = settings.model_copy(update={"orchestration":
        settings.orchestration.model_copy(update={"pending_root_tasks_per_owner": 1})})
    first = submit(r, "first")
    assert submit(r, "second")["task_id"] == first["task_id"]
    with pytest.raises(ValueError, match="dispatch key"):
        submit(r, "first", spec={"goal": "其他目标", "children": [{"goal": "其他工作", "delegation_key": "first"}]})
