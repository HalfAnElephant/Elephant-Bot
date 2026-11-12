"""Revoked user context is closed before creating another model attempt."""
import json

from sqlalchemy import func, insert, select
from test_leader_core import make_core

from qqbot.contracts.memory import MemoryScope
from qqbot.storage.agent_schema import dialogue_inputs
from qqbot.storage.memory_sync_schema import memory_owners
from qqbot.storage.schema import outbox
from qqbot.storage.task_schema import model_requests, processes, runs


async def test_invalid_memory_epoch_closes_once_before_claim_and_allows_other_input(
    tmp_path, official_config, official_event, clock,
):
    core = make_core(tmp_path, official_config, clock)
    try:
        event = core.adapters["qq"].normalize(official_event)
        access = core.access(event)
        scope = MemoryScope(owner_id=core.settings.owner.id, workspace_id=event.conversation.key, audience="group")
        with core.db.transaction() as conn:
            conn.execute(insert(memory_owners).values(id=scope.key, scope_json=scope.model_dump_json(),
                binding_json="{}", binding_hash="fixture", remote_identity="fixture", epoch=2, state="READY",
                updated_at=clock.now().isoformat()))
        stale = {"memory_scope": scope.model_dump(), "memory_epoch": 1,
                 "retrieved_data": [{"content": "REVOKED_PRIVATE_FACT"}]}
        assert core.validate_context(access, stale) is False
        assert core.tasks.context_validator.__self__ is core
        accepted = core.sessions.enqueue(event, access, context=stale)
        with core.db.engine.connect() as conn:
            before = json.loads(conn.execute(select(dialogue_inputs.c.context_json)).scalar_one())
        for _ in range(20):
            assert core.sessions.claim("worker") is None
        with core.db.engine.connect() as conn:
            source = conn.execute(select(dialogue_inputs)).mappings().one()
            closed = json.loads(source["context_json"])
            assert source["state"] == "HANDLED" and source["handled_at"]
            assert source["claimed_attempt_id"] is None
            assert closed == {**before, "output_ref": "CONTEXT_REVOKED"}
            assert not conn.execute(select(outbox)).first()
            for table in (runs, processes, model_requests):
                assert conn.execute(select(func.count()).select_from(table)).scalar_one() == 0
        current = {"memory_scope": scope.model_dump(), "memory_epoch": 2, "retrieved_data": []}
        assert core.validate_context(access, current) is True
        followup = event.model_copy(update={"event_id": "current-valid-input"})
        core.sessions.enqueue(followup, access, context=current)
        claim = core.sessions.claim("worker")
        assert claim["source_ids"] == [followup.event_id]
        assert claim["session_id"] == accepted["session_id"]
        assert claim["context"]["memory_epoch"] == 2
        assert core.sessions.claim("second-writer") is None
        with core.db.engine.connect() as conn:
            assert conn.execute(select(func.count()).select_from(runs)).scalar_one() == 1
            assert not conn.execute(select(processes)).first()
            assert not conn.execute(select(model_requests)).first()
    finally:
        await core.stop()
        core.db.close()
