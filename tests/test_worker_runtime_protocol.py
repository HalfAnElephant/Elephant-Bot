"""Worker request validation and leased-call checks on the production transport."""
import asyncio
from types import SimpleNamespace

import httpx
import pytest
from pydantic import ValidationError
from sqlalchemy import select
from test_agent_runtime import prepare_conditional_review
from test_agent_runtime import runtime as protocol_runtime  # noqa: F401 - pytest fixture
from test_scheduler_supervision import scheduler_for

from elephant.app import create_app
from elephant.config import Settings
from elephant.jobs.protocol import WorkerRequest
from elephant.jobs.worker import Spool, Worker
from elephant.runtime.scheduler import RuntimeClient
from elephant.storage.task_schema import sessions


def test_worker_request_validation_contract():
    default = WorkerRequest(worker_id='fixture')
    assert (default.protocol_version, default.wait_ms, default.stopped, default.quiescent) == (1, 0, True, False)
    for invalid in ({'worker_id': ''}, {'worker_id': 'x' * 129}, {'wait_ms': 10_001},
                    {'fencing_token': 0}, {'seq': 0}, {'event_id': 'x' * 129}, {'unexpected': True}):
        with pytest.raises(ValidationError):
            WorkerRequest.model_validate({'worker_id': 'fixture', **invalid})


async def test_runtime_client_enforces_current_worker_and_fence(protocol_runtime):  # noqa: F811
    runtime = protocol_runtime
    service = runtime.service
    service.accept(runtime.event, runtime.access, 'fixture task')
    worker_id = 'protocol-worker'
    claim = service.claim(worker_id)
    assert claim is not None
    body = {'worker_id': worker_id, 'run_id': claim['run_id'], 'fencing_token': claim['fencing_token']}

    direct = RuntimeClient(runtime.leader.core, runtime.leader)

    async def call(operation, body, status):
        response = await direct.post('/internal/tasks/' + operation, json=body)
        assert response.status_code == status, response.text
        return response.json()

    control = await call('control', body, 200)
    assert control['state'] == 'RUNNING'
    authorized = await call('authorize', body | {'payload': {'action': 'task.execute'}}, 200)
    assert authorized['allowed'] is True
    await call('control', body | {'fencing_token': claim['fencing_token'] + 1}, 409)
    await call('authorize', body | {'worker_id': 'wrong-worker',
        'payload': {'action': 'task.execute'}}, 409)


@pytest.mark.parametrize("records", [("event",), ("result",), ("event", "result")])
async def test_recovered_review_spool_does_not_block_admission(protocol_runtime, records):  # noqa: F811
    runtime = protocol_runtime
    core = runtime.leader.core
    core.knowledge = SimpleNamespace(recover_mutations=lambda: None)
    core.recovery = SimpleNamespace(epoch="fixture")
    await prepare_conditional_review(runtime)
    review = runtime.leader.claim_review("daemon")
    run_id = review["run_id"]
    identity = {"run_id": run_id, "fencing_token": review["fencing_token"]}
    spool = Spool(runtime.path / "spool")
    worker = Worker(RuntimeClient(core, runtime.leader), "daemon", spool, None)
    paths = []
    if "event" in records:
        paths.append(spool.save({**identity, "seq": 1, "event_id": "late-event",
            "payload": {"kind": "failed"}}, run_id=run_id, seq=1))
    if "result" in records:
        paths.append(spool.save({**identity, "stopped": True, "quiescent": False,
            "result": {"status": "failed", "summary": "Interrupted review.", "agent_authored": False}}))
    original = {path: path.read_bytes() for path in paths}

    runtime.clock.advance(31)
    runtime.service.stop_verifier = lambda *_: False
    assert runtime.leader.recover()[0]["state"] == "NEEDS_RECONCILIATION"
    assert not (await worker.call("recovery-status", **identity))["archive_spool"]
    await worker.replay()
    assert all(path.read_bytes() == original[path] for path in paths)

    runtime.service.stop_verifier = lambda *_: True
    assert runtime.leader.recover()[0]["state"] == "RECOVERED"
    for override in ({"worker_id": "other"}, {"fencing_token": identity["fencing_token"] + 1}):
        with pytest.raises(httpx.HTTPStatusError) as failure:
            await worker.call("recovery-status", **(identity | override))
        assert failure.value.response.status_code == 409
    with pytest.raises(httpx.HTTPStatusError) as failure:
        await worker.call("renew", **identity)
    assert failure.value.response.status_code == 409

    runtime.sessions.enqueue(runtime.event.model_copy(update={"event_id": "new-after-review"}), runtime.access)
    admitted = []

    async def record_claim(claim):
        admitted.append(claim["role"])

    worker.once = record_claim
    scheduler = scheduler_for(None)
    scheduler.worker = worker
    await scheduler.step()
    await asyncio.gather(*scheduler.active)
    assert admitted[0] == "leader"
    await worker.replay()
    archive = runtime.path / "spool-reconciled"
    for path in paths:
        assert not path.exists()
        assert (archive / path.relative_to(spool.directory)).read_bytes() == original[path]
    assert len(list(archive.rglob("*.json"))) == len(paths)


async def test_late_stop_report_preserves_new_session_writer(protocol_runtime):  # noqa: F811
    runtime = protocol_runtime
    leader = runtime.leader
    runtime.sessions.enqueue(runtime.event, runtime.access)
    old = runtime.sessions.claim("daemon")

    async def bind(claim, process_id):
        payload = {"process_id": process_id, "session_id": claim["session_id"], "epoch": 1, "generation": 1,
            "session_file": str(runtime.path / "session.jsonl")}
        assert await leader.invoke("bind-process", (claim["run_id"], claim["fencing_token"], "daemon"), payload)

    await bind(old, "old-process")
    runtime.clock.advance(31)
    assert leader.recover()[0]["state"] == "RECOVERED"
    replacement = runtime.sessions.claim("daemon")
    assert replacement["session_id"] == old["session_id"]
    await bind(replacement, "new-process")

    runtime.service.process_stopped("daemon", {"process_id": "old-process", "epoch": 1, "generation": 1, "stopped": True})
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(sessions.c.active_run_id)).scalar_one() == replacement["run_id"]


async def test_worker_operations_are_not_public_http_routes(tmp_path, clock):
    settings = Settings(database=str(tmp_path / 'no-worker-listener.db'))
    app = create_app(settings, clock=clock, run_background=False)
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url='http://test') as client:
            response = await client.post('/internal/tasks/claim', json={'worker_id': 'fixture'})
            assert response.status_code == 404
    finally:
        await app.state.core.stop()
        app.state.core.db.close()
