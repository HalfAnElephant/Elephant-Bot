import json
import os
import sys
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import select

from qqbot.domain import AccessContext, ConversationRef, InboundEvent, PrincipalRef, Surface
from qqbot.harness.skills import SkillStore, SkillVersionUnavailable, publish_skills
from qqbot.jobs.service import TaskService
from qqbot.outbox.service import Outbox
from qqbot.storage.task_schema import tasks


def create_skill(directory, name='camera', content='CAMERA_V1'):
    path = directory / name
    path.mkdir(parents=True, exist_ok=True)
    (path / 'SKILL.md').write_text(f'---\nname: {name}\ndescription: Help select a camera.\n---\n{content}\n')
    return path


def test_skill_snapshot_pins_all_resources_and_survives_source_update(tmp_path):
    source, versions = tmp_path / 'source', tmp_path / 'versions'
    skill = create_skill(source)
    (skill / 'scripts').mkdir()
    (skill / 'scripts' / 'build.py').write_text('print("version-one")')
    store = SkillStore(source, versions)
    first = store.select()
    (skill / 'SKILL.md').write_text('new version')
    (skill / 'scripts' / 'build.py').write_text('print("version-two")')
    second = store.select()
    assert first.version != second.version
    recovered = SkillStore(source, versions).select(first.version)
    assert 'CAMERA_V1' in recovered.read('/skills/camera/SKILL.md')['text']
    assert 'version-one' in recovered.read('/skills/camera/scripts/build.py')['text']
    assert store.select().version == second.version
    for path in ('/skills/../outside', '/etc/passwd', '/skills/absent.md'):
        with pytest.raises(ValueError):
            recovered.read(path)


def test_missing_or_changed_pinned_skill_version_never_falls_back_to_current(tmp_path):
    source = tmp_path / 'source'
    create_skill(source)
    store = SkillStore(source, tmp_path / 'versions')
    snapshot = store.select()
    resource = snapshot.directory / 'camera' / 'SKILL.md'
    resource.chmod(0o644)
    resource.write_text('changed after snapshot')
    with pytest.raises(SkillVersionUnavailable, match='changed'):
        snapshot.read('/skills/camera/SKILL.md')
    with pytest.raises(SkillVersionUnavailable, match=snapshot.version):
        store.select(snapshot.version)
    with pytest.raises(SkillVersionUnavailable, match='a' * 64):
        store.select('a' * 64)


@pytest.mark.parametrize('link_kind', ['file', 'directory', 'hardlink'])
def test_skill_publishing_rejects_resource_links(tmp_path, link_kind):
    source = tmp_path / 'source'
    create_skill(source)
    outside = tmp_path / 'outside'
    outside.mkdir()
    (outside / 'private').write_text('secret')
    link = source / 'camera' / 'linked'
    if link_kind == 'hardlink':
        os.link(outside / 'private', link)
    else:
        link.symlink_to(outside if link_kind == 'directory' else outside / 'private')
    with pytest.raises((SkillVersionUnavailable, OSError)):
        publish_skills(source, tmp_path / 'versions')


def test_task_skill_version_binding_is_durable_and_idempotent(db, clock):
    service = TaskService(db, clock, Outbox(db, clock))
    conversation = ConversationRef(deployment_id='test', adapter_instance_id='a', bot_identity_id='b',
        surface=Surface.PRIVATE, remote_conversation_id='u')
    principal = PrincipalRef(adapter_instance_id='a', identity_namespace='qq', remote_user_id='u')
    event = InboundEvent(event_id='version-task', event_type='message.created', conversation=conversation,
        principal=principal, platform_message_id='m', occurred_at=clock.now(), received_at=clock.now(), trace_id='t')
    task = service.accept(event, AccessContext(principal=principal, conversation=conversation), 'Use skills')
    claim = service.claim('worker')
    identity = claim['run_id'], claim['fencing_token'], 'worker'
    assert service.bind_skills(*identity, {'skill_version': 'a' * 64}) == {'skill_version': 'a' * 64}
    assert service.bind_skills(*identity, {'skill_version': 'a' * 64}) == {'skill_version': 'a' * 64}
    with pytest.raises(PermissionError, match='already fixed'):
        service.bind_skills(*identity, {'skill_version': 'b' * 64})
    with pytest.raises(ValueError):
        service.bind_skills(*identity, {'skill_version': '../current'})
    with db.engine.connect() as conn:
        context = json.loads(conn.execute(select(tasks.c.context_json).where(tasks.c.id == task['id'])).scalar_one())
    assert context['skill_version'] == 'a' * 64
    service.submit(*identity, {'status': 'completed', 'summary': 'Done'})
    with pytest.raises(PermissionError):
        service.bind_skills(*identity, {'skill_version': 'a' * 64})
    retried = service.retry(task['public_id'], event.model_copy(update={'event_id': 'retry-version'}),
                            AccessContext(principal=principal, conversation=conversation))
    assert json.loads(retried['context_json'])['skill_version'] == 'a' * 64


def test_manifest_requires_version_for_start_and_accepts_old_stop_manifest():
    from datetime import UTC, datetime, timedelta

    from qqbot.harness.launcher import systemd_argv, validate_manifest

    task, run, session = str(uuid4()), str(uuid4()), str(uuid4())
    policy = {'approved_models': ['fixture'], 'proxy_host': '10.203.0.1',
              'proxy_port_min': 30000, 'proxy_port_max': 30999}
    manifest = {'schema_version': 2, 'task_id': task, 'run_id': run, 'session_id': session,
        'process_id': run, 'process_epoch': 1, 'profile': 'pi', 'model': 'fixture',
        'proxy_url': 'http://10.203.0.1:30001', 'process_token': 't' * 32,
        'model_context_window': 32768, 'output_tokens': 4096, 'soft_input_tokens': 24000,
        'keep_recent_tokens': 2000, 'expires_at': (datetime.now(UTC) + timedelta(minutes=5)).isoformat()}
    assert validate_manifest(manifest, task, run, policy, check_deadline=False)
    with pytest.raises(ValueError, match='schema'):
        validate_manifest(manifest, task, run, policy)
    for version in ('../outside', 'A' * 64):
        with pytest.raises(ValueError, match='skill version'):
            validate_manifest({**manifest, 'skill_version': version}, task, run, policy)
    updated = {**manifest, 'skill_version': 'a' * 64}
    assert validate_manifest(updated, task, run, policy)
    arguments = systemd_argv(task, run, policy, general=True, skill_version='a' * 64)
    mounts = next(value for value in arguments if value.startswith('--property=BindReadOnlyPaths='))
    assert '/opt/qqbot/skillsets/' + 'a' * 64 + ':/skills' in mounts


async def test_pool_retires_old_idle_skills_and_preserves_busy_versions():
    from qqbot.harness.pool import SessionPool

    created = []

    class Runner:
        quiescent_verified = True
        stopped_verified = False

        async def bind(self, claim):
            self.version = claim['context']['skill_version']

        async def stop(self):
            self.stopped_verified = True
            return True

    def factory(*_arguments):
        runner = Runner()
        created.append(runner)
        return runner

    pool = SessionPool(factory, max_resident=3, max_idle=3)
    old = {'task_id': 'old', 'context': {'key': 'same-topic', 'skill_version': 'a' * 64}}
    new = {'task_id': 'new', 'context': {'key': 'same-topic', 'skill_version': 'b' * 64}}
    first = await pool.acquire(old)
    await pool.retire_skills('b' * 64)
    assert not first.stopped_verified
    second = await pool.acquire(new)
    assert second is not first
    await pool.release(first, True)
    assert first.stopped_verified
    await pool.release(second, True)
    assert not second.stopped_verified
    await pool.retire_skills('c' * 64)
    assert second.stopped_verified
    recovered = await pool.acquire(old)
    assert recovered is not first and recovered.version == 'a' * 64
    await pool.release(recovered, True)
    assert recovered.stopped_verified
    await pool.close()


def test_published_skill_store_is_outside_maintained_source(tmp_path):
    source = tmp_path / 'source'
    create_skill(source)
    with pytest.raises(ValueError, match='outside'):
        publish_skills(source, source / 'versions')


@pytest.mark.parametrize('notice_queued', [False, True])
async def test_missing_fixed_version_sends_one_host_failure_without_running_again(db, clock, tmp_path, notice_queued):
    import httpx
    from fastapi import FastAPI

    from qqbot.jobs.api import internal_router
    from qqbot.jobs.worker import Spool, Worker
    from qqbot.storage.schema import outbox

    conversation = ConversationRef(deployment_id='test', adapter_instance_id='a', bot_identity_id='b',
        surface=Surface.PRIVATE, remote_conversation_id='u')
    principal = PrincipalRef(adapter_instance_id='a', identity_namespace='qq', remote_user_id='u')
    event = InboundEvent(event_id='missing-version', event_type='message.created', conversation=conversation,
        principal=principal, platform_message_id='m', occurred_at=clock.now(), received_at=clock.now(), trace_id='t')
    service = TaskService(db, clock, Outbox(db, clock))
    task = service.accept(event, AccessContext(principal=principal, conversation=conversation),
                          'Resume the work', context={'skill_version': 'a' * 64})
    if notice_queued:
        claim = service.claim('prior-worker')
        service.submit(claim['run_id'], claim['fencing_token'], 'prior-worker',
                       {'status': 'failed', 'agent_authored': False, 'summary': 'Original runner failed'})
    app = FastAPI()
    app.include_router(internal_router(service, 't' * 32))
    launched = []
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://core',
        headers={'Authorization': 'Bearer ' + 't' * 32}) as client:
        worker = Worker(client, 'w', Spool(tmp_path / 'spool'),
            lambda claim: launched.append(claim), skills=SkillStore(tmp_path / 'source', tmp_path / 'versions'))
        assert await worker.once()
        assert await worker.once() is False
    assert launched == []
    with db.engine.connect() as conn:
        row = conn.execute(select(tasks).where(tasks.c.id == task['id'])).mappings().one()
        deliveries = conn.execute(select(outbox.c.envelope_json)).scalars().all()
    assert row['state'] == 'FAILED'
    assert bool(row['failure_notice_json']) == notice_queued
    assert json.loads(row['context_json'])['skill_version'] == 'a' * 64
    assert len(deliveries) == 1 and 'a' * 64 in json.loads(deliveries[0])['text']


async def test_cancelled_pi_reader_with_full_queue_can_finish(tmp_path):
    import asyncio
    from types import SimpleNamespace

    from qqbot.harness.pi import PiRunnerAdapter

    runner = PiRunnerAdapter(sys.executable, tmp_path)
    stdout = asyncio.StreamReader()
    runner.process = SimpleNamespace(stdout=stdout)
    for _ in range(runner.queue.maxsize):
        runner.queue.put_nowait({'type': 'buffered'})
    reader = asyncio.create_task(runner._stdout())
    await asyncio.sleep(0)
    reader.cancel()
    await asyncio.wait_for(asyncio.gather(reader, return_exceptions=True), 1)


async def test_real_worker_keeps_waiting_task_skill_version_after_update_and_restart(db, clock, tmp_path, monkeypatch):
    from types import SimpleNamespace

    import httpx
    from fastapi import FastAPI
    from test_pi_bridge import completion_sse

    from qqbot.harness.gateway import RunGateway
    from qqbot.harness.pool import SessionPool
    from qqbot.jobs import worker as worker_module
    from qqbot.jobs.api import internal_router
    from qqbot.jobs.worker import GeneralRun, Spool, Worker
    from qqbot.providers import ModelCapacity

    source = tmp_path / 'maintained'
    skill = create_skill(source, content='WORKER_SKILL_V1_SENTINEL')
    service = TaskService(db, clock, Outbox(db, clock))
    service.archive_root.mkdir()
    conversation = ConversationRef(deployment_id='test', adapter_instance_id='a', bot_identity_id='b',
        surface=Surface.PRIVATE, remote_conversation_id='u')
    principal = PrincipalRef(adapter_instance_id='a', identity_namespace='qq', remote_user_id='u')
    event = InboundEvent(event_id='first-task', event_type='message.created', conversation=conversation,
        principal=principal, platform_message_id='m', occurred_at=clock.now(), received_at=clock.now(), trace_id='t')
    access = AccessContext(principal=principal, conversation=conversation)
    first = service.accept(event, access, 'first waiting task', context={'key': 'shared-topic'})
    requests, gateways = [], []

    def gateway_factory(*args, **kwargs):
        counts = {}
        gateway = None

        def upstream(request):
            payload = json.loads(request.content)
            run = gateway.claim['run_id']
            counts[run] = counts.get(run, 0) + 1
            requests.append((gateway.claim['task_id'], gateway.skills.version, payload))
            if counts[run] == 1:
                name, arguments = 'read', {'path': '/skills/camera/SKILL.md'}
            elif gateway.claim['task_id'] == first['id'] and not gateway.claim.get('continuation', {}).get('inputs'):
                if counts[run] == 2:
                    name, arguments = 'task_ask_user', {'text': '继续吗？', 'checkpoint': 'Skill read, awaiting reply'}
                else:
                    return httpx.Response(200, headers={'content-type': 'text/event-stream'},
                        text=completion_sse({'role': 'assistant', 'content': ''}))
            else:
                return httpx.Response(200, headers={'content-type': 'text/event-stream'},
                    text=completion_sse({'role': 'assistant', 'content': '已按固定版本完成。'}))
            message = {'role': 'assistant', 'content': None, 'tool_calls': [{'id': f'call-{counts[run]}',
                'type': 'function', 'function': {'name': name, 'arguments': json.dumps(arguments)}}]}
            return httpx.Response(200, headers={'content-type': 'text/event-stream'}, text=completion_sse(message))

        gateway = RunGateway(*args, **kwargs, transport=httpx.MockTransport(upstream))
        gateways.append(gateway)
        return gateway

    monkeypatch.setattr(worker_module, 'RunGateway', gateway_factory)
    args = SimpleNamespace(workspace=str(tmp_path / 'runners'), archive=str(service.archive_root),
        spool=str(tmp_path / 'state' / 'spool'), pi=str(Path(__file__).resolve().parents[1] / 'node_modules/.bin/pi'),
        skills_dir=str(source), model='fixture', model_endpoint='https://model.invalid/chat',
        worker_id='worker', runner_mode='local', model_context_window=32768)
    app = FastAPI()
    app.include_router(internal_router(service, 't' * 32))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://core',
        headers={'Authorization': 'Bearer ' + 't' * 32}) as client:
        store = SkillStore(source, tmp_path / 'state' / 'skillsets')
        capacity = ModelCapacity(5, 1)
        pool = SessionPool(lambda claim, process, session: GeneralRun(args, claim, client, process, session, capacity),
                           max_resident=5, max_idle=2)
        worker = Worker(client, 'worker', Spool(args.spool), None, pool=pool, skills=store)
        try:
            assert await worker.once()
            assert service.get(first['public_id'], access)['state'] == 'WAITING_INPUT'
            with db.engine.connect() as conn:
                context = conn.execute(select(tasks.c.context_json).where(tasks.c.id == first['id'])).scalar_one()
            old_version = json.loads(context)['skill_version']
            (skill / 'SKILL.md').write_text('---\nname: camera\ndescription: Help select a camera.\n---\nWORKER_SKILL_V2_SENTINEL\n')
            second_event = event.model_copy(update={'event_id': 'second-task', 'platform_message_id': 'm2'})
            second = service.accept(second_event, access, 'second task', context={'key': 'shared-topic'})
            assert await worker.once()
            with db.engine.connect() as conn:
                context = conn.execute(select(tasks.c.context_json).where(tasks.c.id == second['id'])).scalar_one()
            new_version = json.loads(context)['skill_version']
            assert new_version != old_version
            answer = event.model_copy(update={'event_id': 'answer', 'platform_message_id': 'answer'})
            from qqbot.ingress.service import Inbox

            answer, _ = Inbox(db).accept(answer)
            question = service.question_for(answer)
            service.add_input(first['public_id'], answer, access, '继续', question['id'])
            # Rebuild supervisor and pool to exercise durable task version recovery.
            await pool.close()
            pool = SessionPool(lambda claim, process, session: GeneralRun(args, claim, client, process, session, capacity),
                               max_resident=5, max_idle=2)
            worker = Worker(client, 'worker', Spool(args.spool), None, pool=pool,
                            skills=SkillStore(source, tmp_path / 'state' / 'skillsets'))
            assert await worker.once()
            assert service.get(first['public_id'], access)['state'] == 'SUCCEEDED'
        finally:
            await pool.close()
    first_requests = [request for task, version, request in requests if task == first['id']]
    second_requests = [request for task, version, request in requests if task == second['id']]
    assert 'WORKER_SKILL_V1_SENTINEL' in str(first_requests[-1]['messages'])
    assert 'WORKER_SKILL_V2_SENTINEL' not in str(first_requests[-1]['messages'])
    assert 'WORKER_SKILL_V2_SENTINEL' in str(second_requests[-1]['messages'])
    assert len({version for task, version, request in requests if task == first['id']}) == 1
    assert len(gateways) == 3
