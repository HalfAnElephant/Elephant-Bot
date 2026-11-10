import json

import httpx
from sqlalchemy import select

from qqbot.observability.diagnostics import configuration_status, model_identity, report_worker
from qqbot.providers.metering import MeteredClient, call_scope
from qqbot.storage.schema import audit_events


async def test_core_model_metering_counts_missing_usage_and_failure(db, clock):
    def handle(request):
        if request.url.path == '/fail':
            return httpx.Response(503)
        return httpx.Response(200, json={'usage': {'prompt_tokens': 12, 'completion_tokens': 3}})
    raw = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    client = MeteredClient(raw, db, clock).for_phase('judge')
    with call_scope(message_id='source', conversation='group', principal='person'):
        await client.post('https://test/ok', json={'model': 'test', 'messages': ['private']})
        async with client.stream('POST', 'https://test/fail', json={'model': 'test'}) as response:
            await response.aread()
    with db.engine.connect() as c:
        records = [json.loads(value) for value in c.execute(select(audit_events.c.detail)).scalars()]
    assert len(records) == 2
    assert records[0]['usage']['input_tokens'] == 12
    assert records[1]['usage']['input_tokens'] is None
    assert records[1]['usage']['coverage'] == 'unknown'
    assert all(row['message_id'] == 'source' and row['phase'] == 'judge' for row in records)
    assert 'private' not in json.dumps(records)
    await client.aclose()


def test_worker_config_reports_mismatch_staleness_and_updates(db, clock):
    from types import SimpleNamespace

    from qqbot.config import Settings
    service = SimpleNamespace(db=db, clock=clock)
    core = SimpleNamespace(db=db, clock=clock, settings=Settings())
    old = model_identity('old-model', 'https://old', 2048)
    report = {'model_identity': old, 'skill_version': 'a' * 64, 'loaded_skill_versions': ['a' * 64]}
    report_worker(service, 'worker', report)
    result = configuration_status(core, core.settings)
    assert result['restart_components'] == ['Worker']
    assert not result['workers'][0]['model_matches']
    clock.advance(91)
    assert configuration_status(core, core.settings)['workers'][0]['stale']
    report_worker(service, 'worker', report)
    assert len(configuration_status(core, core.settings)['workers']) == 1
    assert not configuration_status(core, core.settings)['workers'][0]['stale']


async def test_stream_usage_survives_incremental_reads(db, clock):
    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'{"choices": [], "usage": '
            yield b'{"prompt_tokens": 9, "completion_tokens": 2}}'
    raw = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, stream=Stream())))
    client = MeteredClient(raw, db, clock).for_phase('expression')
    async with client.stream('POST', 'https://test', json={'model': 'test'}) as response:
        async for _ in response.aiter_bytes():
            pass
    with db.engine.connect() as c:
        result = json.loads(c.execute(select(audit_events.c.detail)).scalar_one())
    assert result['usage']['input_tokens'] == 9
    assert result['usage']['output_tokens'] == 2
    assert result['queue_ms'] >= 0
    await client.aclose()
