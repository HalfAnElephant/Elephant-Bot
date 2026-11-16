"""Schedule tools follow the same authenticated Leader entry as ordinary user turns."""
from datetime import timedelta
from unittest.mock import AsyncMock

import httpx
import pytest
from test_leader_core import make_core

from qqbot.harness.gateway import RunGateway
from qqbot.runtime.tools import native_tools
from qqbot.storage.task_schema import runs


def test_schedule_schemas_are_only_available_to_user_leader():
    assert {'schedules.create', 'schedules.list', 'schedules.get', 'schedules.update', 'schedules.cancel'} <= set(
        native_tools('leader', 'user'))
    for role, purpose in [('worker', 'work'), ('leader', 'progress'), ('leader', 'finalize'), ('skill_review', 'skill_review')]:
        assert not any(name.startswith('schedules.') for name in native_tools(role, purpose))


async def test_schedule_native_tool_reaches_host_operation(tmp_path):
    call = AsyncMock(return_value={'allowed': True, 'id': 'saved-schedule'})
    gateway = RunGateway(tmp_path, 'model', '', 'https://model.invalid', call=call)
    try:
        gateway.bind({'task_id': 'turn', 'run_id': 'run', 'fencing_token': 1, 'role': 'leader', 'purpose': 'user',
            'goal': '明天检查一次', 'permission_snapshot': {'principal': {}}, 'context': {}})
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url='http://test') as client:
            response = await client.post('/tools/schedules_create', headers={'Authorization': 'Bearer ' + gateway.tool_token},
                json={'call_id': 'create-once', 'arguments': {'goal': '检查一次', 'run_at': '2030-01-01T09:00:00+08:00'}})
        assert response.status_code == 200, response.text
        assert response.json()['id'] == 'saved-schedule'
        arguments = next(item.args[1] for item in call.await_args_list if item.args[0] == 'schedules')
        assert arguments['operation'] == 'create' and arguments['call_id'] == 'create-once'
    finally:
        gateway.revoke()


async def test_live_leader_schedule_controls_persist_and_worker_role_is_rejected(
        tmp_path, official_config, official_event, clock):
    from conftest import official_payload
    core = make_core(tmp_path, official_config, clock)
    try:
        event = core.adapters['qq'].normalize(official_payload(official_event, private=True,
            text='明天检查一次仓库', message_id='schedule-user'))
        event, _ = core.inbox.accept(event)
        await core.process(event)
        claim = core.sessions.claim('schedule-leader')
        identity = (claim['run_id'], claim['fencing_token'], 'schedule-leader')
        payload = {'operation': 'create', 'call_id': 'schedule-call', 'arguments': {'goal': '检查一次仓库',
            'run_at': (clock.now() + timedelta(days=1)).isoformat()}}
        first = await core.leader.invoke('schedules', identity, payload)
        assert await core.leader.invoke('schedules', identity, payload) == first
        assert len(core.schedules.list(event)) == 1
        with core.db.transaction() as conn:
            conn.execute(runs.update().where(runs.c.id == claim['run_id']).values(role='skill_review'))
        with pytest.raises(PermissionError, match='real Leader user turn'):
            await core.leader.invoke('schedules', identity, payload)
    finally:
        await core.stop()
        core.db.close()


async def test_mail_draft_from_authenticated_leader_persists_filters_and_goal(
        tmp_path, official_config, official_event, clock):
    from conftest import official_payload

    from qqbot.runtime.schedules import ScheduleService

    core = make_core(tmp_path, official_config, clock)
    try:
        text = "在 demo 邮箱收件箱收到 example.org 发来的周报，且不是旧稿时，总结为三条并整理为 summary.md。"
        event = core.adapters['qq'].normalize(official_payload(official_event, private=True,
            text=text, message_id='mail-draft-user'))
        event, _ = core.inbox.accept(event)
        await core.process(event)
        claim = core.sessions.claim('mail-draft-leader')
        identity = (claim['run_id'], claim['fencing_token'], 'mail-draft-leader')
        # Model output is scripted; the authenticated Leader, persistence and matcher are real.
        args = {
            'goal': '将周报总结为三条，保存到 summary.md；不发送邮件。',
            'success_checks': ['artifact'],
            'requested_outputs': ['summary.md'],
            'enabled': False,
            'mail': {
                'account_id': 'demo', 'folder': 'INBOX',
                'all': [
                    {'field': 'sender_domain', 'op': 'equals', 'value': 'example.org'},
                    {'field': 'subject', 'op': 'contains', 'value': '周报'},
                ],
                'none': [{'field': 'body', 'op': 'contains', 'value': '旧稿'}],
            },
        }
        payload = {'operation': 'create', 'call_id': 'mail-draft', 'arguments': args}
        result = await core.leader.invoke('schedules', identity, payload)
        stored = ScheduleService(core).get(event, result['id'])
        assert await core.leader.invoke('schedules', identity, payload) == result
        assert not stored['enabled']
        assert stored['next_fire_at'] is None
        assert stored['spec']['goal'] == args['goal']
        assert stored['spec']['requested_outputs'] == args['requested_outputs']
        from qqbot.runtime.schedules import ScheduleSpec
        trigger = ScheduleSpec.model_validate(stored['spec']).mail
        assert trigger is not None
        assert trigger.account_id == 'demo'
        assert trigger.matches(sender_address='office@example.org', subject='本周周报', body='新版本')
        assert not trigger.matches(sender_address='office@example.org', subject='本周周报', body='旧稿')
        assert not trigger.matches(sender_address='office@elsewhere.example', subject='本周周报', body='新版本')
        assert core.schedules.once()
        assert ScheduleService(core).get(event, result['id'])['last_task_id'] is None
        with pytest.raises(ValueError, match='require enabled=false'):
            await core.leader.invoke('schedules', identity, payload | {'call_id': 'activate',
                'arguments': args | {'enabled': True}})
    finally:
        await core.stop()
        core.db.close()


async def test_synthetic_mail_trigger_bound_to_owner_persists_once(
        tmp_path, official_config, official_event, clock, monkeypatch):
    import json

    from conftest import official_payload

    from qqbot.runtime.mail_source import MailBinding, SyntheticMailSource, SyntheticMessage
    from qqbot.runtime.schedules import ScheduleService
    from qqbot.storage.task_schema import tasks

    core = make_core(tmp_path, official_config, clock)
    try:
        event = core.adapters['qq'].normalize(official_payload(official_event, private=True,
            text='demo 邮箱收到 example.org 的周报就运行仅本地汇总，不发送邮件。', message_id='mail-trigger-user'))
        event, _ = core.inbox.accept(event)
        await core.process(event)
        claim = core.sessions.claim('mail-leader')
        identity = (claim['run_id'], claim['fencing_token'], 'mail-leader')
        workspace = claim['context']['workspace_id']
        source = SyntheticMailSource(MailBinding(core.sessions.owner(event), event.principal.key,
            event.conversation.key, workspace, 'demo', 'INBOX'), baseline=1)
        core.schedules = ScheduleService(core, synthetic_mail=source)
        goal = '仅本地记录任务完成，不发送邮件。'
        spec = {'goal': goal, 'mail': {'account_id': 'demo', 'all': [
            {'field': 'sender_domain', 'op': 'equals', 'value': 'example.org'},
            {'field': 'subject', 'op': 'contains', 'value': '周报'},
        ]}}
        with pytest.raises(PermissionError, match='authenticated scope'):
            await core.leader.invoke('schedules', identity, {'operation': 'create', 'call_id': 'wrong-account',
                'arguments': spec | {'mail': {'account_id': 'other'}}})
        source.messages.append(SyntheticMessage(2, 'sender@example.org', '旧周报', 'before creation'))
        payload = {'operation': 'create', 'call_id': 'bound-account', 'arguments': spec}
        item = await core.leader.invoke('schedules', identity, payload)
        source.messages.extend([
            SyntheticMessage(4, 'sender@example.org', '周报', 'UNTRUSTED_REWRITE_MARKER'),
            SyntheticMessage(3, 'other@elsewhere.example', '周报', 'excluded'),
        ])
        submit = core.tasks.graph.submit

        def fail_after_submit(*args, **kwargs):
            submit(*args, **kwargs)
            raise RuntimeError('synthetic failure after submission')

        with monkeypatch.context() as patch:
            patch.setattr(core.tasks.graph, 'submit', fail_after_submit)
            with pytest.raises(RuntimeError, match='synthetic failure'):
                core.schedules.scan_synthetic_mail()
        assert core.schedules.get(event, item['id'])['last_task_id'] is None
        with core.db.engine.connect() as conn:
            assert not conn.execute(tasks.select().where(tasks.c.kind == 'goal')).first()
        core.schedules.scan_synthetic_mail()
        restarted = ScheduleService(core, synthetic_mail=source)
        restarted.scan_synthetic_mail()
        receipt = restarted.get(event, item['id'])
        assert receipt['last_task_id']
        with core.db.engine.connect() as conn:
            rows = conn.execute(tasks.select().where(tasks.c.kind == 'goal')).mappings().all()
        assert len(rows) == 1
        assert rows[0]['id'] == receipt['last_task_id']
        assert rows[0]['requester'] == event.principal.key
        assert goal in str(dict(rows[0]))
        assert 'UNTRUSTED_REWRITE_MARKER' not in str(dict(rows[0]))
        context = json.loads(rows[0]['context_json'])
        assert core.validate_context(core.access(event), context)
        source.enabled = False
        assert not core.validate_context(core.access(event), context)
        with pytest.raises(PermissionError, match='authenticated scope'):
            await core.leader.invoke('schedules', identity, payload)
        source.messages.append(SyntheticMessage(5, 'sender@example.org', '周报', 'after disabled'))
        restarted.scan_synthetic_mail()
        assert restarted.get(event, item['id']) == receipt
        source.enabled = True
        restarted.update(event, item['id'], {'enabled': False}, expected_revision=1)
        assert not core.validate_context(core.access(event), context)
        source.messages.append(SyntheticMessage(6, 'sender@example.org', '周报', 'while paused'))
        restarted.update(event, item['id'], {'enabled': True}, expected_revision=2)
        assert not core.validate_context(core.access(event), context)
        restarted.scan_synthetic_mail()
        assert restarted.get(event, item['id'])['last_task_id'] == receipt['last_task_id']
        source.messages.append(SyntheticMessage(7, 'sender@example.org', '周报', 'after resume'))
        restarted.scan_synthetic_mail()
        assert restarted.get(event, item['id'])['last_task_id'] != receipt['last_task_id']
    finally:
        await core.stop()
        core.db.close()


@pytest.mark.parametrize(('subject', 'requirement', 'filename', 'content'), [
    ('周报', '仅生成三句本地摘要，不外发。', 'summary.md', '一。二。三。'),
    ('工单', '仅在本地归档工单编号，不外发。', 'tickets.txt', 'TICKET-001\n'),
])
async def test_synthetic_mail_runs_worker_and_archives_local_output(
        tmp_path, official_config, official_event, clock, subject, requirement, filename, content):
    import json
    from types import SimpleNamespace

    from conftest import official_payload

    from qqbot.domain.tasks import TaskResult
    from qqbot.jobs.worker import Spool, Worker
    from qqbot.runtime.mail_source import MailBinding, SyntheticMailSource, SyntheticMessage
    from qqbot.runtime.scheduler import RuntimeClient
    from qqbot.runtime.schedules import ScheduleService
    from qqbot.storage.task_schema import tasks

    core = make_core(tmp_path, official_config, clock)
    try:
        event = core.adapters['qq'].normalize(official_payload(official_event, private=True,
            text=f'demo 邮箱收到 example.org 的{subject}时，{requirement}', message_id='mail-worker-user'))
        event, _ = core.inbox.accept(event)
        await core.process(event)
        leader = core.sessions.claim('fixture-leader')
        identity = (leader['run_id'], leader['fencing_token'], 'fixture-leader')
        workspace_id = leader['context']['workspace_id']
        source = SyntheticMailSource(MailBinding(core.sessions.owner(event), event.principal.key,
            event.conversation.key, workspace_id, 'demo', 'INBOX'), baseline=0)
        core.schedules = ScheduleService(core, synthetic_mail=source)
        output = 'outputs/' + filename
        arguments = {'goal': requirement, 'success_checks': ['artifact'], 'requested_outputs': [output],
            'mail': {'account_id': 'demo', 'all': [
                {'field': 'sender_domain', 'op': 'equals', 'value': 'example.org'},
                {'field': 'subject', 'op': 'contains', 'value': subject},
            ]}}
        # Scripted tool reply, not a real model's interpretation of the Chinese prompt.
        with pytest.raises(ValueError, match='account_id'):
            await core.leader.invoke('schedules', identity, {'operation': 'create', 'call_id': 'missing-account',
                'arguments': arguments | {'mail': {'all': arguments['mail']['all']}}})
        assert core.schedules.list(event) == []
        created = await core.leader.invoke('schedules', identity, {'operation': 'create',
            'call_id': 'mail-local-output', 'arguments': arguments})
        source.messages.extend([
            SyntheticMessage(1, 'sender@elsewhere.example', subject, '不符发件域'),
            SyntheticMessage(2, 'sender@example.org', subject, 'NOT_HARNESS_INPUT'),
        ])
        core.schedules.scan_synthetic_mail()
        root = core.schedules.get(event, created['id'])['last_task_id']
        assert root
        directory = tmp_path / 'fixture-worker'
        (directory / 'outputs').mkdir(parents=True)
        archive_dir = core.tasks.archive_root
        archive_dir.mkdir(parents=True, exist_ok=True)
        seen = []

        class SyntheticModelHarness:
            stopped_verified = True
            gateway = SimpleNamespace(workspace=directory, artifacts=[], evidence=[])
            archive = archive_dir

            async def run(self, goal):
                seen.append(goal)
                assert goal == requirement
                (directory / output).write_text(content, encoding='utf-8')
                return TaskResult(status='completed', summary='已生成本地文件。')

            async def stop(self):
                return True

        def fixture_factory(claim):
            assert claim['role'] == 'worker'
            assert claim['goal_spec']['requested_outputs'] == [output]
            assert 'NOT_HARNESS_INPUT' not in str(claim['context'])
            harness = SyntheticModelHarness()
            harness.last_receipt = {
                'kind': 'turn_finished', 'run_id': claim['run_id'], 'session_id': claim['session_id'],
                'session_revision': 'test-harness', 'input_ids': ['task:' + claim['task_id']],
                'inflight': 0, 'tools_paired': True,
            }
            return harness

        worker = Worker(RuntimeClient(core, core.leader), 'fixture-worker', Spool(tmp_path / 'spool'), fixture_factory)
        try:
            assert await worker.once()
        except httpx.HTTPStatusError as error:
            pytest.fail(error.response.text)
        assert seen == [requirement]
        with core.db.engine.connect() as conn:
            child = conn.execute(tasks.select().where(tasks.c.root_task_id == root,
                tasks.c.id != root)).mappings().one()
        assert child['state'] == 'SUCCEEDED'
        artifact = json.loads(child['result_json'])['artifacts'][0]
        assert (archive_dir / artifact['id']).read_text(encoding='utf-8') == content
        assert list((tmp_path / 'spool').glob('*.json')) == []
    finally:
        await core.stop()
        core.db.close()
