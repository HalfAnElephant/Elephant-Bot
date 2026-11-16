import base64
import json
import smtplib
import ssl
from concurrent.futures import ThreadPoolExecutor
from email import message_from_bytes
from email.policy import default
from threading import Event
from types import SimpleNamespace

import pytest
from sqlalchemy import select, update

from elephant.domain import ConversationRef, InboundEvent, PrincipalRef, Surface, TextSegment
from elephant.mail.sending import MailSender, SMTPTransport
from elephant.storage.mail_send_schema import mail_outgoing


@pytest.fixture
def outgoing(db, clock):
    mail_outgoing.create(db.engine, checkfirst=True)
    event = InboundEvent(event_id='mail-owner-request', event_type='message.created',
        conversation=ConversationRef(deployment_id='test', adapter_instance_id='qq', bot_identity_id='bot',
            surface=Surface.PRIVATE, remote_conversation_id='300'),
        principal=PrincipalRef(adapter_instance_id='qq', identity_namespace='qq_app:app', remote_user_id='300'),
        platform_message_id='mail-owner-request', occurred_at=clock.now(), received_at=clock.now(),
        segments=(TextSegment(text='发送邮件'),), trace_id='send-test')
    core = SimpleNamespace(db=db, clock=clock, allowed=lambda event: True,
        access=lambda event: event,
        sessions=SimpleNamespace(owner=lambda event: 'owner' if event.principal.remote_user_id == '300' else None,
            validate_workspace=lambda event, workspace: workspace),
        permissions=SimpleNamespace(require=lambda *args: None))

    class Accounts:
        def __init__(self):
            self.row = {'id': 'account', 'revision': 1, 'credential_ref': 'credential', 'enabled': True}

        def resolve(self, request, workspace, account_id, *, require_ready=False, conn=None):
            if (request.principal != event.principal or request.conversation != event.conversation
                    or workspace != 'personal' or account_id not in {'account', 'alias'}):
                raise PermissionError('mail account outside scope')
            if require_ready and not self.row['enabled']:
                raise PermissionError('mail account disabled')
            return dict(self.row)

        def spec(self, row):
            return SimpleNamespace(address='owner@example.org', username='owner@example.org',
                smtp_host='smtp.example.org', smtp_port=465, smtp_security='ssl')

        def credential(self, row):
            return 'TOP_SECRET_PASSWORD'

        def messages(self, row, folder, *, uid):
            return {'uidvalidity': 7, 'messages': [{'uid': uid, 'sender_address': 'sender@example.org',
                'reply_to': ['reply@example.org'], 'subject': '中文通知', 'message_id': '<original@example.org>',
                'references': ['<ancestor@example.org>']}]}

    class Transport:
        def __init__(self):
            self.calls = 0
            self.sent = []

        def send(self, spec, secret, wire, sender, recipients, authorize):
            authorize()
            self.calls += 1
            self.sent.append((wire, sender, recipients))
            return {'state': 'ACCEPTED', 'smtp_code': 250, 'accepted_recipients': recipients,
                    'rejected_recipients': {}}

    accounts, transport = Accounts(), Transport()
    sender = MailSender(core, accounts, transport)
    return SimpleNamespace(core=core, event=event, accounts=accounts, transport=transport, sender=sender)


def prepare(runtime, arguments=None, *, key='send-once', scope='root-task'):
    return runtime.sender.prepare(runtime.event, 'personal', 'account', arguments or {
        'to': ['recipient@example.org'], 'subject': '中文主题', 'body': '中文正文'},
        idempotency_key=key, scope_id=scope)


def send(runtime, draft, authorize=lambda: None):
    return runtime.sender.send(runtime.event, 'personal', draft['id'],
        expected_digest=draft['digest'], authorize=authorize)


def test_frozen_mime_idempotency_reply_and_accepted_delivery_are_distinct(outgoing):
    r = outgoing
    arguments = {'cc': ['cc@example.org'], 'bcc': ['hidden@example.org'], 'body': '中文回复',
        'reply': {'folder': 'INBOX', 'uid': 42, 'uidvalidity': 7},
        'attachments': [{'filename': '报告.txt', 'mime_type': 'text/plain',
                         'data_base64': base64.b64encode('附件'.encode()).decode()}]}
    draft = prepare(r, arguments)
    assert prepare(r, arguments)['id'] == draft['id']
    assert prepare(r, arguments, key='worker-retry')['id'] == draft['id']
    with pytest.raises(ValueError, match='different content'):
        prepare(r, arguments | {'body': 'changed'})

    def unavailable_original(*args, **kwargs):
        raise LookupError('original message has been removed')

    r.accounts.messages = unavailable_original
    assert prepare(r, arguments)['id'] == draft['id']
    assert draft['state'] == 'DRAFT'
    assert draft['account_revision'] == 1
    assert draft['transport_snapshot'] == {'smtp_host': 'smtp.example.org', 'smtp_port': 465,
        'smtp_security': 'ssl', 'username': 'owner@example.org', 'sender_address': 'owner@example.org'}
    assert draft['content']['to'] == ['reply@example.org']
    assert draft['content']['subject'] == 'Re: 中文通知'
    receipt = send(r, draft)
    assert receipt['state'] == 'ACCEPTED' and receipt['delivery_status'] == 'unconfirmed'
    assert send(r, draft) == receipt and r.transport.calls == 1
    wire, sender, recipients = r.transport.sent[0]
    assert wire.isascii()
    message = message_from_bytes(wire, policy=default)
    assert sender == 'owner@example.org'
    assert set(recipients) == {'reply@example.org', 'cc@example.org', 'hidden@example.org'}
    assert message['Bcc'] is None
    assert str(message['Subject']) == 'Re: 中文通知'
    assert str(message['Message-ID']) == receipt['message_id']
    assert str(message['In-Reply-To']) == '<original@example.org>'
    assert str(message['References']) == '<ancestor@example.org> <original@example.org>'
    assert message.get_body(preferencelist=('plain',)).get_content().strip() == '中文回复'
    attachment = next(message.iter_attachments())
    assert attachment.get_filename() == '报告.txt' and attachment.get_payload(decode=True) == '附件'.encode()
    assert 'TOP_SECRET_PASSWORD' not in json.dumps(receipt)
    assert 'data_base64' not in json.dumps(receipt)


def test_frozen_digest_scope_and_changed_account_are_rechecked(outgoing):
    r = outgoing
    draft = prepare(r)
    with pytest.raises(ValueError, match='digest'):
        r.sender.send(r.event, 'personal', draft['id'], expected_digest='different', authorize=lambda: None)
    other = r.event.model_copy(update={'conversation': r.event.conversation.model_copy(
        update={'remote_conversation_id': 'other'})})
    with pytest.raises(PermissionError, match='scope'):
        r.sender.get(other, 'personal', draft['id'])
    r.accounts.row['revision'] = 2
    with pytest.raises(PermissionError, match='changed'):
        send(r, draft)
    assert r.transport.calls == 0
    assert r.sender.get(r.event, 'personal', draft['id'])['state'] == 'DRAFT'
    with pytest.raises(ValueError, match='new creation key'):
        prepare(r)
    replacement = prepare(r, key='changed-account')
    assert replacement['id'] != draft['id']
    assert send(r, replacement)['state'] == 'ACCEPTED'


def test_concurrent_send_claims_the_draft_once(outgoing):
    r = outgoing
    draft = prepare(r)
    entered, release = Event(), Event()
    original = r.transport.send

    def blocked(*args):
        entered.set()
        assert release.wait(5)
        return original(*args)

    r.transport.send = blocked
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(send, r, draft)
        try:
            assert entered.wait(5)
            second = send(r, draft)
            assert second['state'] == 'DISPATCHING'
        finally:
            release.set()
        assert first.result()['state'] == 'ACCEPTED'
    assert r.transport.calls == 1


def test_uncertain_send_and_recovered_dispatch_are_never_resent(outgoing):
    r = outgoing
    draft = prepare(r)

    def uncertain(*args):
        r.transport.calls += 1
        raise TimeoutError('server error containing TOP_SECRET_PASSWORD')

    r.transport.send = uncertain
    receipt = send(r, draft)
    assert receipt['state'] == 'UNKNOWN'
    assert 'TOP_SECRET_PASSWORD' not in json.dumps(receipt)
    assert send(r, draft) == receipt and r.transport.calls == 1
    with r.core.db.transaction() as conn:
        conn.execute(update(mail_outgoing).where(mail_outgoing.c.id == draft['id']).values(state='DISPATCHING'))
    restarted = MailSender(r.core, r.accounts, r.transport)
    assert restarted.recover() == 1
    assert restarted.recover() == 0
    r.sender = restarted
    assert send(r, draft)['state'] == 'UNKNOWN' and r.transport.calls == 1


@pytest.mark.parametrize('arguments', [
    {'to': ['recipient@example.org'], 'subject': 'hello\r\nBcc: evil@example.org'},
    {'to': ['recipient@example.org\r\nRCPT TO:evil@example.org']},
    {'to': ['first@example.org,second@example.org']},
    {'to': ['bad@@example.org']},
    {'to': []},
    {'to': ['recipient@example.org'], 'attachments': [
        {'filename': '../password', 'mime_type': 'text/plain', 'data_base64': 'YQ=='}]},
    {'to': ['recipient@example.org'], 'attachments': [
        {'filename': 'file.txt', 'mime_type': 'text/plain', 'data_base64': 'not base64'}]},
    {'reply': {'folder': 'INBOX', 'uid': 42, 'uidvalidity': 8}},
])
def test_untrusted_mail_arguments_fail_before_dispatch(outgoing, arguments):
    with pytest.raises(ValueError):
        prepare(outgoing, arguments)
    assert outgoing.transport.calls == 0
    with outgoing.core.db.engine.connect() as conn:
        assert not conn.execute(select(mail_outgoing)).first()


@pytest.mark.parametrize('outcome,expected', [('accept', 'PARTIAL_ACCEPTED'), ('timeout', 'UNKNOWN'),
                                          ('reject', 'FAILED')])
@pytest.mark.parametrize('security', ['ssl', 'starttls'])
def test_smtp_partial_recipients_and_data_results_are_recorded_without_server_text(
        monkeypatch, outcome, expected, security):
    calls = []

    class SMTP:
        def __init__(self, host, port, *, timeout, context=None):
            if security == 'ssl':
                assert context.check_hostname and context.verify_mode == ssl.CERT_REQUIRED

        def ehlo(self):
            return 250, b'hello'

        def login(self, username, secret):
            assert secret == 'TOP_SECRET_PASSWORD'
            assert security == 'ssl' or 'starttls' in calls
            calls.append('login')

        def starttls(self, *, context):
            assert context.check_hostname and context.verify_mode == ssl.CERT_REQUIRED
            calls.append('starttls')

        def mail(self, sender):
            return 250, b'ok'

        def rcpt(self, recipient):
            return (550, b'private server explanation') if recipient.startswith('rejected') else (250, b'ok')

        def data(self, wire):
            calls.append('data')
            if outcome == 'timeout':
                raise TimeoutError('TOP_SECRET_PASSWORD')
            if outcome == 'reject':
                raise smtplib.SMTPDataError(550, b'TOP_SECRET_PASSWORD')
            return 250, b'queued'

        def close(self):
            calls.append('close')

    monkeypatch.setattr('elephant.mail.sending.smtplib.SMTP_SSL', SMTP)
    monkeypatch.setattr('elephant.mail.sending.smtplib.SMTP', SMTP)
    result = SMTPTransport().send(SimpleNamespace(smtp_host='smtp.example.org', smtp_port=465,
        smtp_security=security, username='user'), 'TOP_SECRET_PASSWORD', b'message', 'owner@example.org',
        ['accepted@example.org', 'rejected@example.org'], lambda: calls.append('authorize'))
    assert result['state'] == expected
    assert result['rejected_recipients'] == {'rejected@example.org': 550}
    assert calls[calls.index('data') - 1] == 'authorize'
    assert 'TOP_SECRET_PASSWORD' not in json.dumps(result)


@pytest.mark.parametrize('revocation', ['disabled', 'deleted'])
def test_smtp_revocation_after_recipient_checks_prevents_data(outgoing, monkeypatch, revocation):
    r = outgoing
    draft = prepare(r)
    calls = []

    class SMTP:
        def __init__(self, *args, **kwargs):
            pass

        def ehlo(self):
            return 250, b'ok'

        def login(self, *args):
            pass

        def mail(self, sender):
            return 250, b'ok'

        def rcpt(self, recipient):
            if revocation == 'deleted':
                def deleted(*args, **kwargs):
                    raise LookupError('mail account unavailable')
                r.accounts.resolve = deleted
            else:
                r.accounts.row['enabled'] = False
            return 250, b'ok'

        def data(self, wire):
            calls.append('data')

        def close(self):
            pass

    monkeypatch.setattr('elephant.mail.sending.smtplib.SMTP_SSL', SMTP)
    r.sender.transport = SMTPTransport()
    with pytest.raises(PermissionError, match='disabled|no longer available'):
        send(r, draft)
    assert calls == []
    assert r.sender.get(r.event, 'personal', draft['id'])['state'] == 'FAILED'


def test_delivery_reconciliation_requires_admin_and_operator_evidence(outgoing):
    r = outgoing
    draft = prepare(r)
    send(r, draft)
    with pytest.raises(PermissionError, match='administrator'):
        r.sender.resolve_delivery(r.event, 'personal', draft['id'], state='delivered', evidence='recipient checked')
    admin = r.event.model_copy(update={'principal': r.event.principal.model_copy(
        update={'identity_namespace': 'admin_api'})})
    with pytest.raises(ValueError, match='SMTP acceptance'):
        r.sender.resolve_delivery(admin, 'personal', draft['id'], state='not_sent', evidence='recipient has not seen it yet')
    receipt = r.sender.resolve_delivery(admin, 'personal', draft['id'], state='delivered',
        evidence='Operator checked this Message-ID in the recipient mailbox.')
    assert receipt['state'] == 'ACCEPTED' and receipt['delivery_status'] == 'delivered'
    assert r.transport.calls == 1


def test_missing_credential_never_claims_an_smtp_dispatch(outgoing):
    r = outgoing
    draft = prepare(r)

    def missing(row):
        raise ValueError('credential is unavailable')

    r.accounts.credential = missing
    with pytest.raises(ValueError, match='credential'):
        send(r, draft)
    assert r.sender.get(r.event, 'personal', draft['id'])['state'] == 'DRAFT'
    assert r.transport.calls == 0


@pytest.mark.parametrize('state', ['UNKNOWN', 'DISPATCHING'])
def test_unresolved_scope_blocks_changed_content_until_operator_reconciliation(outgoing, state):
    r = outgoing
    first = prepare(r)
    changed = prepare(r, {'to': ['recipient@example.org'], 'subject': 'updated', 'body': 'changed'}, key='changed')
    with r.core.db.transaction() as conn:
        conn.execute(update(mail_outgoing).where(mail_outgoing.c.id == first['id']).values(state=state))
    with pytest.raises(ValueError, match=first['id']):
        send(r, changed)
    assert send(r, first)['state'] == state
    assert r.transport.calls == 0
    independent = prepare(r, key='separate-user-message', scope='another-authorized-user-request')
    assert send(r, independent)['state'] == 'ACCEPTED'
    r.sender.recover()
    admin = r.event.model_copy(update={'principal': r.event.principal.model_copy(
        update={'identity_namespace': 'admin_api'})})
    resolved = r.sender.resolve_delivery(admin, 'personal', first['id'], state='not_sent',
        evidence='Provider logs confirm this Message-ID was never accepted.')
    assert resolved['state'] == 'FAILED' and resolved['delivery_status'] == 'not_sent'
    assert send(r, changed)['state'] == 'ACCEPTED'
    assert r.transport.calls == 2


@pytest.mark.parametrize('when', ['before_read', 'during_read'])
def test_reply_preparation_checks_read_authority_before_and_after_network(outgoing, when):
    r = outgoing
    allowed = when != 'before_read'
    observed = []
    messages = r.accounts.messages

    def require(access, action, resource):
        if action == 'mail.read':
            assert resource == 'mail-account:account'
            if not allowed:
                raise PermissionError('mail read revoked')

    def read(*args, **kwargs):
        nonlocal allowed
        observed.append('read')
        result = messages(*args, **kwargs)
        allowed = False
        return result

    r.core.permissions.require = require
    r.accounts.messages = read
    with pytest.raises(PermissionError, match='read revoked'):
        r.sender.prepare(r.event, 'personal', 'alias', {'body': 'reply',
            'reply': {'folder': 'INBOX', 'uid': 42, 'uidvalidity': 7}},
            idempotency_key='read-authorized-reply', scope_id='root-task')
    assert len(observed) == (1 if when == 'during_read' else 0)
    with r.core.db.engine.connect() as conn:
        assert not conn.execute(select(mail_outgoing)).first()


def test_reply_preparation_replay_cannot_bypass_revoked_read_authority(outgoing):
    r = outgoing
    arguments = {'body': 'reply', 'reply': {'folder': 'INBOX', 'uid': 42, 'uidvalidity': 7}}
    prepare(r, arguments)

    def require(access, action, resource):
        if action == 'mail.read':
            raise PermissionError('mail read revoked')

    r.core.permissions.require = require
    with pytest.raises(PermissionError, match='read revoked'):
        prepare(r, arguments)


def test_new_mail_sends_and_returns_status_without_incoming_read_authority(outgoing):
    r = outgoing
    actions = []

    def require(access, action, resource):
        actions.append(action)
        if action == 'mail.read':
            raise PermissionError('incoming mail read denied')

    r.core.permissions.require = require
    draft = prepare(r)
    receipt = send(r, draft)
    assert receipt['state'] == 'ACCEPTED' and r.transport.calls == 1
    assert r.sender.get(r.event, 'personal', draft['id']) == receipt
    assert 'mail.send' in actions and 'mail.status' in actions and 'mail.read' not in actions


def test_retry_confirmed_failure_preserves_wire_and_previous_outcome(outgoing):
    r = outgoing
    draft = prepare(r)
    accepted_send = r.transport.send
    attempts = []

    def rejected(spec, secret, wire, sender, recipients, authorize):
        authorize()
        attempts.append(wire)
        return {'state': 'FAILED', 'error': 'smtp_data_rejected', 'smtp_code': 550}

    r.transport.send = rejected
    assert send(r, draft)['state'] == 'FAILED'
    assert prepare(r)['state'] == 'FAILED'
    retried = r.sender.retry(r.event, 'personal', draft['id'], expected_digest=draft['digest'], authorize=lambda: None)
    assert retried['state'] == 'DRAFT' and retried['result']['retry_count'] == 1
    assert retried['result']['previous_result']['state'] == 'FAILED'
    assert retried['result']['previous_result']['smtp_code'] == 550
    assert retried['message_id'] == draft['message_id'] and retried['digest'] == draft['digest']
    assert retried['content'] == draft['content'] and r.transport.calls == 0
    assert len(attempts) == 1
    r.transport.send = accepted_send
    receipt = send(r, retried)
    assert receipt['state'] == 'ACCEPTED' and receipt['result']['retry_count'] == 1
    assert receipt['result']['previous_result'] == retried['result']['previous_result']
    assert r.transport.sent[0][0] == attempts[0]


@pytest.mark.parametrize('state', ['DRAFT', 'UNKNOWN', 'DISPATCHING', 'ACCEPTED', 'PARTIAL_ACCEPTED'])
def test_retry_refuses_every_state_that_is_not_a_confirmed_failure(outgoing, state):
    r = outgoing
    draft = prepare(r)
    with r.core.db.transaction() as conn:
        conn.execute(update(mail_outgoing).where(mail_outgoing.c.id == draft['id']).values(state=state))
    with pytest.raises(ValueError, match='confirmed FAILED'):
        r.sender.retry(r.event, 'personal', draft['id'], expected_digest=draft['digest'], authorize=lambda: None)
    assert r.sender.get(r.event, 'personal', draft['id'])['state'] == state
    assert r.transport.calls == 0


@pytest.mark.parametrize('failure', ['scope', 'digest', 'revision', 'credential', 'disabled', 'authority'])
def test_retry_rechecks_current_scope_digest_account_and_authority(outgoing, failure):
    r = outgoing
    draft = prepare(r)
    with r.core.db.transaction() as conn:
        conn.execute(update(mail_outgoing).where(mail_outgoing.c.id == draft['id']).values(state='FAILED'))
    event, digest = r.event, draft['digest']
    if failure == 'scope':
        event = event.model_copy(update={'conversation': event.conversation.model_copy(
            update={'remote_conversation_id': 'another'})})
    if failure == 'digest':
        digest = 'different-digest'
    if failure == 'revision':
        r.accounts.row['revision'] += 1
    if failure == 'credential':
        r.accounts.row['credential_ref'] = 'changed-reference'
    if failure == 'disabled':
        r.accounts.row['enabled'] = False

    def authorize():
        if failure == 'authority':
            raise PermissionError('run authority revoked')

    with pytest.raises((PermissionError, ValueError)):
        r.sender.retry(event, 'personal', draft['id'], expected_digest=digest, authorize=authorize)
    assert r.sender.get(r.event, 'personal', draft['id'])['state'] == 'FAILED'
    assert r.transport.calls == 0


def test_retry_after_operator_not_sent_is_atomic_and_keeps_history_on_recovery(outgoing):
    r = outgoing
    draft = prepare(r)
    with r.core.db.transaction() as conn:
        conn.execute(update(mail_outgoing).where(mail_outgoing.c.id == draft['id']).values(state='UNKNOWN'))
    admin = r.event.model_copy(update={'principal': r.event.principal.model_copy(
        update={'identity_namespace': 'admin_api'})})
    resolved = r.sender.resolve_delivery(admin, 'personal', draft['id'], state='not_sent',
        evidence='Provider logs confirm that the frozen Message-ID was never accepted.')
    assert resolved['state'] == 'FAILED'

    def retry_once():
        try:
            return r.sender.retry(r.event, 'personal', draft['id'],
                expected_digest=draft['digest'], authorize=lambda: None)
        except ValueError:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        attempts = [pool.submit(retry_once) for _ in range(2)]
        receipts = [attempt.result() for attempt in attempts]
    assert sum(receipt is not None for receipt in receipts) == 1
    retried = next(receipt for receipt in receipts if receipt is not None)
    assert retried['result']['retry_count'] == 1
    assert retried['result']['previous_result']['state'] == 'FAILED'
    assert retried['result']['previous_result']['reconciliation_evidence'] == resolved['result']['reconciliation_evidence']
    with r.core.db.transaction() as conn:
        conn.execute(update(mail_outgoing).where(mail_outgoing.c.id == draft['id']).values(state='DISPATCHING'))
    assert r.sender.recover() == 1
    recovered = r.sender.get(r.event, 'personal', draft['id'])
    assert recovered['state'] == 'UNKNOWN' and recovered['result']['retry_count'] == 1
    assert recovered['result']['previous_result'] == retried['result']['previous_result']
    assert r.transport.calls == 0


def test_smtp_unicode_authentication_failure_is_definite_before_data(monkeypatch):
    sent = []

    class SMTP:
        def __init__(self, *args, **kwargs):
            pass

        def ehlo(self):
            return 250, b'ok'

        def login(self, username, secret):
            raise UnicodeEncodeError('ascii', secret, 0, 1, 'unsupported authentication text')

        def data(self, wire):
            sent.append(wire)

        def close(self):
            pass

    monkeypatch.setattr('elephant.mail.sending.smtplib.SMTP_SSL', SMTP)
    receipt = SMTPTransport().send(SimpleNamespace(smtp_host='smtp.example.org', smtp_port=465,
        smtp_security='ssl', username='user'), '密码', b'message', 'owner@example.org',
        ['recipient@example.org'], lambda: None)
    assert receipt['state'] == 'FAILED' and not sent
    assert '密码' not in json.dumps(receipt, ensure_ascii=False)
