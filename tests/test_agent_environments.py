"""Environment choices survive retries and cannot be escalated by sandbox children."""
import json

import pytest
from sqlalchemy import select, update
from test_agent_runtime import root_goal
from test_agent_runtime import runtime as runtime  # noqa: PLC0414

from qqbot.domain.execution import ExecutionPlan
from qqbot.storage.agent_schema import dialogue_inputs
from qqbot.storage.task_schema import tasks


def test_defaults_and_nested_environment_boundaries(runtime):
    result = root_goal(runtime)
    assert result["environment"] == "host"
    assert result["children"][0]["environment"] == "sandbox"
    with runtime.db.transaction() as conn:
        root = conn.execute(select(tasks).where(tasks.c.id == result["task_id"])).mappings().one()
        sandbox = conn.execute(select(tasks).where(tasks.c.parent_task_id == root["id"])).mappings().one()
        with pytest.raises(PermissionError, match="cannot delegate host"):
            runtime.service.graph.spawn(conn, sandbox,
                {"goal": "host escalation", "delegation_key": "denied", "environment": "host"}, runtime.access)
        host = runtime.service.graph.spawn(conn, root,
            {"goal": "host work", "delegation_key": "host", "environment": "host"}, runtime.access)
        parent = conn.execute(select(tasks).where(tasks.c.id == host["task_id"])).mappings().one()
        for environment in ("host", "sandbox"):
            child = runtime.service.graph.spawn(conn, parent,
                {"goal": environment, "delegation_key": environment, "environment": environment}, runtime.access)
            plan = conn.execute(select(tasks.c.plan_json).where(tasks.c.id == child["task_id"])).scalar_one()
            assert json.loads(plan)["environment"] == environment


def test_child_replay_preserves_accepted_environment_after_default_changes(runtime):
    result = root_goal(runtime)
    settings = runtime.service.graph.settings
    runtime.service.graph.settings = settings.model_copy(update={
        "execution": settings.execution.model_copy(update={"worker_environment": "host"})})
    with runtime.db.transaction() as conn:
        root = conn.execute(select(tasks).where(tasks.c.id == result["task_id"])).mappings().one()
        replay = runtime.service.graph.spawn(conn, root,
            {"goal": "检查部分 0", "delegation_key": "0"}, runtime.access)
        assert replay["environment"] == "sandbox"
        assert replay["task_id"] == result["children"][0]["id"]
        sandbox = conn.execute(select(tasks).where(tasks.c.id == replay["task_id"])).mappings().one()
        with pytest.raises(PermissionError, match="cannot delegate host"):
            runtime.service.graph.spawn(conn, sandbox,
                {"goal": "default escalation", "delegation_key": "denied"}, runtime.access)


def test_legacy_intents_normalize_optional_environment(runtime):
    result = root_goal(runtime)
    with runtime.db.transaction() as conn:
        for row in conn.execute(select(tasks)).mappings().all():
            context = json.loads(row["context_json"])
            if "dispatch_spec" in context:
                for child in context["dispatch_spec"]["children"]:
                    child.pop("environment", None)
            if "child_spec" in context:
                context["child_spec"].pop("environment", None)
            conn.execute(update(tasks).where(tasks.c.id == row["id"]).values(context_json=json.dumps(context)))
    assert root_goal(runtime)["task_id"] == result["task_id"]
    with runtime.db.transaction() as conn:
        root = conn.execute(select(tasks).where(tasks.c.id == result["task_id"])).mappings().one()
        assert runtime.service.graph.spawn(conn, root,
            {"goal": "检查部分 0", "delegation_key": "0"}, runtime.access)["environment"] == "sandbox"
    assert ExecutionPlan.model_validate_json('{}').environment == "sandbox"


def test_leader_input_persists_environment_before_claim(runtime):
    runtime.sessions.enqueue(runtime.event, runtime.access)
    settings = runtime.sessions.settings
    runtime.sessions.settings = settings.model_copy(update={
        "execution": settings.execution.model_copy(update={"leader_environment": "sandbox"})})
    claim = runtime.sessions.claim("leader")
    assert claim["plan"]["environment"] == "host"
    assert claim["context"]["environment"] == "host"
    with runtime.db.engine.connect() as conn:
        context = json.loads(conn.execute(select(dialogue_inputs.c.context_json)).scalar_one())
        assert context["leader_turn"]["plan"]["environment"] == "host"


def test_environment_is_exposed_in_prompt():
    from qqbot.runtime.prompting import assemble_prompt

    assert "本轮实际执行环境：host" in assemble_prompt("leader", environment="host")
    assert "本轮实际执行环境：sandbox" in assemble_prompt("worker", environment="sandbox")


def test_legacy_accepted_leader_turn_keeps_sandbox(runtime):
    runtime.sessions.enqueue(runtime.event, runtime.access)
    with runtime.db.transaction() as conn:
        row = conn.execute(select(dialogue_inputs)).mappings().one()
        context = json.loads(row["context_json"])
        context["leader_turn"]["plan"].pop("environment")
        conn.execute(update(dialogue_inputs).where(dialogue_inputs.c.id == row["id"])
            .values(context_json=json.dumps(context)))
    claim = runtime.sessions.claim("leader")
    assert claim["plan"]["environment"] == "sandbox"


def test_configured_worker_default_is_persisted(runtime):
    settings = runtime.service.graph.settings
    runtime.service.graph.settings = settings.model_copy(update={
        "execution": settings.execution.model_copy(update={"worker_environment": "host"})})
    result = root_goal(runtime)
    assert result["children"][0]["environment"] == "host"
    runtime.service.graph.settings = settings
    assert runtime.service.claim("worker")["plan"]["environment"] == "host"


def test_finalization_preserves_root_leader_environment(runtime):
    from test_agent_runtime import worker_complete

    root_goal(runtime)
    worker_complete(runtime)
    settings = runtime.sessions.settings
    runtime.sessions.settings = settings.model_copy(update={
        "execution": settings.execution.model_copy(update={"leader_environment": "sandbox"})})
    claim = runtime.sessions.claim("leader")
    assert claim["purpose"] == "finalize"
    assert claim["plan"]["environment"] == "host"
