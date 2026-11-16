from types import SimpleNamespace

import pytest
from sqlalchemy import select
from test_host_controls import control_runtime as control_runtime  # noqa: PLC0414
from test_mail_runtime import claim, rule
from test_mail_runtime import mail as mail  # noqa: PLC0414

from elephant.capabilities.gateway import CapabilityRegistry, standard_registry
from elephant.capabilities.types import Action, Capability
from elephant.jobs.waiters import ExternalWaiters
from elephant.permissions import PermissionPolicy, PermissionRule
from elephant.storage.task_schema import inputs, tasks, waits


def waiting_mail_task(mail, grants):
    schedule = rule(mail, mail_permissions=grants)
    mail.mailbox.append(1, body='PRIVATE_MAIL_BODY')
    mail.core.mail.triggers.once()
    worker, identity = claim(mail)
    mail.core.tasks.ask(*identity, {'question_id': 'mail-web-wait', 'kind': 'external',
        'text': '等待外部状态', 'checkpoint': '保留原任务，不重复业务写入',
        'condition': {'capability': 'web.fetch',
            'arguments': {'url': 'https://allowed.example/status?message=PRIVATE_MAIL_BODY'},
            'path': 'status', 'equals': 'completed', 'interval_seconds': 15}})
    mail.core.tasks.submit(*identity, {'status': 'waiting_external', 'summary': '等待外部状态'},
        receipt={'kind': 'run_yielded', 'session_id': worker['context']['session_id'],
            'run_id': worker['run_id'], 'session_revision': 'mail-external-wait',
            'input_ids': worker['context']['source_input_ids'], 'inflight': 0, 'tools_paired': True})
    return schedule, worker['task_id']


def web_grant(*hosts):
    return {'id': 'web-observation', 'effect': 'allow', 'actions': ['web.fetch'],
            'resources': ['domain:' + host for host in hosts]}


def observer(mail, registry, authorizations):
    async def call(operation, payload):
        if operation == 'waiters':
            return mail.core.tasks.external_waiters()
        if operation == 'wait-authorize':
            authorizations.append(payload)
            return mail.core.tasks.authorize_waiter(payload)
        if operation == 'wait-observed':
            return mail.core.tasks.observe_waiter(payload)
        raise AssertionError('unexpected waiter host operation')

    return ExternalWaiters(call, registry)


def pending_web_registry(executed):
    registry = CapabilityRegistry()

    async def fetch(arguments, context):
        executed.append(arguments['url'])
        return {'status': 'pending'}

    registry.register(Capability('web.fetch', 'Read external status'), fetch,
        lambda arguments, context: Action('web.fetch', ('domain:allowed.example',)))
    return registry


def assert_still_waiting(mail, task_id):
    with mail.core.db.engine.connect() as conn:
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == task_id)).scalar_one() == 'WAITING_EXTERNAL'
        assert conn.execute(select(waits.c.state).where(waits.c.id == 'mail-web-wait')).scalar_one() == 'WAITING'
        assert not conn.execute(select(inputs.c.id).where(inputs.c.question_id == 'mail-web-wait')).first()


async def test_mail_waiter_with_empty_grants_never_contacts_external_host(mail):
    _, task_id = waiting_mail_task(mail, [])
    executed, authorized = [], []
    await observer(mail, pending_web_registry(executed), authorized).once()
    assert executed == [] and authorized[0]['action'] == 'web.fetch'
    assert_still_waiting(mail, task_id)


async def test_mail_waiter_rechecks_current_rule_grant_on_each_poll(mail):
    schedule, task_id = waiting_mail_task(mail, [web_grant('allowed.example')])
    executed, authorized = [], []
    waiter = observer(mail, pending_web_registry(executed), authorized)
    await waiter.once()
    assert len(executed) == 1
    mail.core.schedules.update(mail.event, schedule['id'], {'mail_permissions': []}, expected_revision=1)
    waiter.next_poll.clear()
    await waiter.once()
    assert len(executed) == 1 and len(authorized) == 2
    assert_still_waiting(mail, task_id)


@pytest.mark.parametrize('restriction', ['rule', 'global'])
async def test_mail_waiter_redirect_rechecks_rule_and_global_domain_scope(mail, monkeypatch, restriction):
    from elephant.harness import public_web

    hosts = ('allowed.example',) if restriction == 'rule' else ('allowed.example', 'denied.example')
    _, task_id = waiting_mail_task(mail, [web_grant(*hosts)])
    if restriction == 'global':
        mail.core.permissions.initial_policy = PermissionPolicy(preset='custom', default_effect='allow', rules=(
            PermissionRule(id='block-redirect', effect='deny', actions=('web.fetch',),
                resources=('domain:denied.example',)),))
    contacted, resolved, authorized = [], [], []

    def addresses(host, port, **kwargs):
        resolved.append(host)
        return []

    class RedirectConnection:
        def __init__(self, host, port, addresses, **kwargs):
            self.host = host
            self.sock = SimpleNamespace(settimeout=lambda value: None, shutdown=lambda value: None)
            self.timeout = 10

        def remaining(self):
            return 10

        def request(self, method, path, **kwargs):
            contacted.append((self.host, method, path))

        def getresponse(self):
            return SimpleNamespace(status=302, getheader=lambda name: 'https://denied.example/collect', close=lambda: None)

        def close(self):
            pass

    monkeypatch.setattr(public_web, 'public_addresses', addresses)
    monkeypatch.setattr(public_web, 'PinnedHTTPS', RedirectConnection)
    await observer(mail, standard_registry(), authorized).once()
    assert len(contacted) == 1 and contacted[0][0] == 'allowed.example'
    assert resolved == ['allowed.example']
    assert [payload['resources'] for payload in authorized] == [
        ('domain:allowed.example',), ('domain:allowed.example',), ('domain:denied.example',)]
    assert_still_waiting(mail, task_id)
