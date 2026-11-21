"""Memory failures and mutations preserve the real Core/Leader admission boundary."""
import json
import logging

import pytest
from conftest import official_payload
from sqlalchemy import select
from test_openviking import memory, seeded  # noqa: F401

from elephant.app import create_app
from elephant.config import Settings
from elephant.contracts.memory import MemoryDeletion
from elephant.domain import Surface
from elephant.runtime.knowledge import KnowledgeRuntime
from elephant.storage.agent_schema import dialogue_inputs
from elephant.storage.memory_sync_schema import memory_owners


@pytest.fixture
async def memory_core(memory, official_config, official_event, clock):  # noqa: F811
    service, remote, scope, _ = memory
    _, _, uri = await seeded(memory)
    settings = Settings(database=service.db.path, adapters=(official_config,),
        owner={"id": scope.owner_id, "qq_bindings": ({"app_id": "app", "user_openid": "300",
            "verified_at": clock.now().isoformat()},)},
        memory={"openviking": service.config})
    core = create_app(settings, clock=clock, run_background=False).state.core
    await core.openviking.close()
    core.openviking = service
    core.knowledge = KnowledgeRuntime(core)
    event = core.adapters["qq"].normalize(official_payload(
        official_event, private=True, text="请根据当前资料回答"))
    event, _ = core.inbox.accept(event)
    try:
        yield core, remote, scope, event, uri
    finally:
        await core.stop()
        core.db.close()


@pytest.mark.parametrize("state", ["offline", "restored"])
async def test_memory_unavailability_still_admits_an_unenriched_user_turn(memory_core, state):
    core, remote, scope, event, _ = memory_core
    if state == "offline":
        remote.offline = True
    else:
        core.openviking.restore_barriers(scope, core.openviking.export_barriers(scope))
    before = len(remote.calls)
    await core.process(event)
    claim = core.sessions.claim("contract-leader")
    assert claim is not None and claim["purpose"] == "user"
    assert claim["context"]["memory_status"] == ("degraded" if state == "offline" else "blocked")
    assert claim["context"]["retrieved_data"] == []
    assert all(method == "GET" or path == "/api/v1/search/find" for method, path, _ in remote.calls[before:])


async def test_forget_between_enrichment_and_claim_fences_the_queued_context(memory_core, caplog, monkeypatch):
    core, _, scope, event, uri = memory_core
    logger = logging.getLogger("elephant.observability.timing")
    monkeypatch.setattr(logger, "handlers", [caplog.handler])
    monkeypatch.setattr(logger, "propagate", False)
    with caplog.at_level(logging.INFO, logger="elephant.observability.timing"):
        await core.process(event)
    trace = [record for record in caplog.records if record.name == "elephant.observability.timing"]
    assert [record.msg for record in trace] == [
        "ingress.memory_search", "ingress.memory_read", "ingress.dialogue_admission",
    ]
    assert all(record.data["event_id"] == event.event_id for record in trace)
    assert trace[1].data["index"] == 1
    with core.db.engine.connect() as conn:
        saved = conn.execute(select(dialogue_inputs)).mappings().one()
    context = json.loads(saved["context_json"])
    assert context["retrieved_data"][0]["uri"] == uri
    receipt = await core.openviking.forget(MemoryDeletion(
        scope=scope, uris=(uri,), idempotency_key="contract-forget"))
    assert receipt.memory_epoch > context["memory_epoch"]
    assert not core.validate_context(core.access(event), context)
    assert core.sessions.claim("contract-leader") is None
    with core.db.engine.connect() as conn:
        saved = conn.execute(select(dialogue_inputs)).mappings().one()
    assert json.loads(saved["context_json"])["output_ref"] == "CONTEXT_REVOKED" and saved["handled_at"] is not None


async def test_group_ingress_cannot_initialize_or_recall_the_private_binding(memory_core):
    core, remote, scope, event, _ = memory_core
    event = event.model_copy(update={"event_id": "contract-group", "conversation": event.conversation.model_copy(
        update={"surface": Surface.GROUP, "remote_conversation_id": "200"}), "mentions_bot": True})
    event, _ = core.inbox.accept(event)
    before = len(remote.calls)
    assert core.knowledge.scope(event) is None
    await core.process(event)
    claim = core.sessions.claim("contract-leader")
    assert claim is not None and "memory_scope" not in claim["context"]
    assert len(remote.calls) == before
    with core.db.engine.connect() as conn:
        assert conn.execute(select(memory_owners.c.id)).scalars().all() == [scope.key]


async def test_restored_remote_fact_cannot_reenter_a_new_core_turn(memory_core):
    core, remote, scope, event, uri = memory_core
    receipt = await core.openviking.forget(MemoryDeletion(
        scope=scope, uris=(uri,), idempotency_key="contract-restore-forget"))
    assert (await core.openviking.poll(receipt.operation_id)).completed
    core.openviking.restore_barriers(scope, core.openviking.export_barriers(scope))
    assert (await core.openviking.reconcile_restore(scope))["state"] == "READY"
    remote.files[uri] = "synthetic forgotten fact restored by an external backup"
    await core.process(event)
    claim = core.sessions.claim("contract-leader")
    assert claim is not None and claim["context"]["retrieved_data"] == []
    assert claim["context"]["memory_epoch"] == receipt.memory_epoch
    assert "synthetic forgotten fact" not in json.dumps(claim)
