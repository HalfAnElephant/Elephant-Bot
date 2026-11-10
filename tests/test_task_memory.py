import json

import pytest
import test_tasks_control
from sqlalchemy import insert, select, update

from qqbot.domain.messages import TextSegment
from qqbot.memory import SocialService
from qqbot.memory.history import HistoryMemoryService
from qqbot.storage.schema import inbox
from qqbot.storage.task_schema import inputs, runs, tasks

setup = test_tasks_control.setup


def archive(service, event, seq=1):
    with service.db.transaction() as conn:
        conn.execute(insert(inbox).values(id=event.event_id, conversation_id=event.conversation.key,
            event_type=event.event_type, message_id=event.platform_message_id,
            principal_id=event.principal.key, seq=seq, event_json=event.model_dump_json(), state="DONE"))


async def test_worker_history_lease_and_source_closure(setup):
    service, event, access, _ = setup
    source = event.model_copy(update={"segments": (TextSegment(text="历史路径 /tmp/archive"),)})
    archive(service, source)
    service.history_memory = HistoryMemoryService(SocialService(service.db, service.clock, permissions=service.permissions))
    service.accept(event, access, "找原路径")
    claim = service.claim("worker")
    args = (claim["run_id"], claim["fencing_token"], "worker")
    result = await service.memory_query(*args, {"operation": "history.search", "arguments": {"query": "路径"}})
    assert result["messages"][0]["text"] == source.text
    with service.db.engine.connect() as conn:
        context = json.loads(conn.execute(select(tasks.c.context_json)).scalar_one())
    assert context["source_manifest"][0]["id"] == event.event_id
    with pytest.raises(PermissionError):
        await service.memory_query(args[0], args[1] + 1, args[2], {"operation": "history.read", "arguments": {"source_ids": ["e"]}})
    service.history_memory.social.revoke_source(access, event.event_id)
    result = await service.memory_query(*args, {"operation": "history.read", "arguments": {"source_ids": ["e"]}})
    assert result["messages"] == []


async def test_worker_history_rechecks_lease_after_embedding(setup):
    service, event, access, _ = setup
    archive(service, event)
    social = SocialService(service.db, service.clock, permissions=service.permissions)
    history = HistoryMemoryService(social)
    service.history_memory = history
    service.accept(event, access, "查记忆")
    claim = service.claim("worker")
    async def invalidate(_access, _query):
        with service.db.transaction() as conn:
            conn.execute(update(runs).where(runs.c.id == claim["run_id"]).values(slot_reserved=0))
        return {}
    history.query_options = invalidate
    with pytest.raises(PermissionError):
        await service.memory_query(claim["run_id"], claim["fencing_token"], "worker",
            {"operation": "memory.search", "arguments": {"query": "之前的偏好"}})


def test_claim_rebuilds_consumed_user_decisions_without_duplicate_storage(setup):
    service, event, access, clock = setup
    archive(service, event)
    task = service.accept(event, access, "按用户要求调整")
    update_event = event.model_copy(update={"event_id": "u2", "platform_message_id": "m2",
        "segments": (TextSegment(text="改成只看我创建的"),)})
    archive(service, update_event, 2)
    with service.db.transaction() as conn:
        conn.execute(insert(inputs).values(id="decision", task_id=task["id"], source_id="u2",
            text=update_event.text, state="CONSUMED", created_at=clock.now().isoformat()))
    claim = service.claim("worker")
    assert claim["continuation"]["inputs"] == []
    assert claim["task_state"]["user_updates"] == [{"id": "decision", "text": update_event.text,
        "source_id": "u2", "state": "CONSUMED", "created_at": clock.now().isoformat()}]
    assert claim["task_state"]["goal"]["text"] == "按用户要求调整"


def test_cold_restore_rejects_revoked_consumed_update(setup, tmp_path):
    from qqbot.harness.gateway import RunGateway
    service, event, access, clock = setup
    archive(service, event)
    task = service.accept(event, access, "调整")
    update_event = event.model_copy(update={"event_id": "u2", "platform_message_id": "m2",
        "segments": (TextSegment(text="已经撤回的条件"),)})
    archive(service, update_event, 2)
    with service.db.transaction() as conn:
        conn.execute(insert(inputs).values(id="old", task_id=task["id"], source_id="u2",
            text=update_event.text, state="CONSUMED", created_at=clock.now().isoformat()))
    SocialService(service.db, clock).revoke_source(access, "u2")
    claim = service.claim("worker")
    assert claim["task_state"]["user_updates"] == []
    gateway = RunGateway(tmp_path, "m", "key", "https://invalid", call=None)
    with pytest.raises(PermissionError, match="revoked"):
        gateway.bind(claim)
    with pytest.raises(PermissionError, match="revoked"):
        service.authorize(claim["run_id"], claim["fencing_token"], "worker", {"action": "model.request"})


def test_task_state_byte_gate_preserves_archived_inputs(tmp_path):
    from qqbot.harness.gateway import RunGateway
    gateway = RunGateway(tmp_path, "m", "key", "https://invalid", call=None)
    claim = {"context": {"limits": {"max_bytes": 200}},
        "goal": "g", "task_state": {"user_updates": [{"text": "x" * 500}]}}
    with pytest.raises(ValueError, match="byte budget"):
        gateway.bind(claim)
    assert gateway.revoked and len(claim["task_state"]["user_updates"][0]["text"]) == 500


async def test_tool_observations_record_latency_skill_and_failure_without_arguments(tmp_path):
    import hashlib

    import httpx

    from qqbot.harness.gateway import RunGateway
    from qqbot.harness.skills import SkillSnapshot
    observations, events = [], []
    async def core(operation, payload):
        if operation == "authorize":
            return {"allowed": True}
        if operation == "reserve-budget":
            return {"accepted": True}
        if operation == "observation":
            observations.append(payload)
            return {"recorded": True}
        raise AssertionError(operation)
    directory = tmp_path / "skills"
    (directory / "test").mkdir(parents=True)
    (directory / "test" / "SKILL.md").write_text("skill body")
    snapshot = SkillSnapshot("version", directory, {"test/SKILL.md": hashlib.sha256(b"skill body").hexdigest()})
    gateway = RunGateway(tmp_path, "m", "key", "https://invalid", call=core)
    gateway.skills = snapshot
    gateway.bind({"task_id": "task", "run_id": "run", "fencing_token": 1, "goal": "read",
        "permission_snapshot": {"principal": {}}, "context": {}})
    gateway.event_sink = events.append
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://local") as client:
        headers = {"Authorization": "Bearer " + gateway.tool_token}
        result = await client.post("/tools/read", headers=headers,
            json={"call_id": "read", "arguments": {"path": "/skills/test/SKILL.md"}})
        assert result.status_code == 200
        result = await client.post("/tools/capability_invoke", headers=headers,
            json={"call_id": "bad", "arguments": {"name": "missing", "arguments": {"secret": "never-record"}}})
        assert result.status_code == 422
    observed = observations[0]["data"]
    assert observed["capability"] == "skills.read" and observed["elapsed_ms"] >= 0
    assert observed["skill_name"] == "test" and observed["skill_path"] == "/skills/test/SKILL.md"
    assert events[-1]["type"] == "host_tool_finished" and events[-1]["status"] == "KeyError"
    assert events[-1]["skill_version"] == "version" and events[-1]["elapsed_ms"] >= 0
    assert "never-record" not in json.dumps(events) and "skill body" not in json.dumps(observations)


def test_equal_timestamp_user_updates_keep_ingress_order(setup):
    service, event, access, clock = setup
    archive(service, event)
    task = service.accept(event, access, '按最新条件执行')
    for seq, identity, text in [(2, 'z-first', '范围设为一周'), (3, 'a-last', '改成一个月')]:
        source = event.model_copy(update={'event_id': identity, 'platform_message_id': identity,
                                         'segments': (TextSegment(text=text),)})
        archive(service, source, seq)
        with service.db.transaction() as conn:
            conn.execute(insert(inputs).values(id=identity, task_id=task['id'], source_id=identity,
                text=text, state='CONSUMED', created_at=clock.now().isoformat()))
    claim = service.claim('worker')
    assert [item['text'] for item in claim['task_state']['user_updates']] == ['范围设为一周', '改成一个月']
