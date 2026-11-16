"""Worker request validation and leased-call checks on the production transport."""
import httpx
import pytest
from pydantic import ValidationError
from test_agent_runtime import runtime as protocol_runtime  # noqa: F401 - pytest fixture

from qqbot.app import create_app
from qqbot.config import Settings
from qqbot.jobs.protocol import WorkerRequest
from qqbot.runtime.scheduler import RuntimeClient


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
