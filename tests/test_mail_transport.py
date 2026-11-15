import base64
import re

import pytest

from qqbot.mail import transport as mail_transport
from qqbot.mail.models import MailAccountSpec
from qqbot.mail.transport import MailTransport, MailTransportError, _parts, _structure

BODY = base64.b64encode('本周中文周报。'.encode())
ATTACHMENT = base64.b64encode(b'attachment-bytes')
STRUCTURE = (b'(("TEXT" "PLAIN" ("CHARSET" "UTF-8") NIL NIL "BASE64" ' + str(len(BODY)).encode()
    + b' 1 NIL NIL)("APPLICATION" "PDF" NIL NIL NIL "BASE64" ' + str(len(ATTACHMENT)).encode()
    + b' NIL ("ATTACHMENT" ("FILENAME*" "utf-8\'\'%E6%8A%A5%E5%91%8A.pdf"))) "MIXED")')
HEADER = (b'From: =?utf-8?b?5Y+R5Lu25Lq6?= <sender@example.org>\r\nTo: reader@example.org\r\n'
    b'Reply-To: replies@example.org\r\nSubject: =?utf-8?b?5ZGo5oql?=\r\n'
    b'Message-ID: <weekly@example.org>\r\nReferences: <prior@example.org>\r\n\r\n')


class IMAPServer:
    def __init__(self, *args, **kwargs):
        self.calls = []
        self.validity = 42
        self.identifiers = [1, 2, 4]

    def starttls(self, **kwargs):
        self.calls.append(('starttls', kwargs))

    def login(self, username, secret):
        self.calls.append(('login', username))
        return 'OK', []

    def select(self, folder, readonly=False):
        self.calls.append(('select', folder, readonly))
        return 'OK', [b'3']

    def response(self, code):
        return code, [str(self.validity if code == 'UIDVALIDITY' else 5).encode()]

    def uid(self, command, *arguments):
        self.calls.append((command, *arguments))
        if command == 'SEARCH':
            return 'OK', [b' '.join(str(uid).encode() for uid in self.identifiers)]
        uid, request = arguments
        if request == '(UID BODYSTRUCTURE)':
            return 'OK', [uid.encode() + b' (UID ' + uid.encode() + b' BODYSTRUCTURE ' + STRUCTURE + b')']
        part = re.search(r'BODY\.PEEK\[([^]]+)\]', request).group(1)
        size = int(re.search(r'<0\.(\d+)>', request).group(1))
        data = {'HEADER': HEADER, '1': BODY, '2': ATTACHMENT}[part][:size]
        return 'OK', [(b'1 (BODY[] {' + str(len(data)).encode() + b'}', data), b')']

    def logout(self):
        self.calls.append(('logout',))


@pytest.fixture
def protocol(monkeypatch):
    server = IMAPServer()
    monkeypatch.setattr(mail_transport.imaplib, 'IMAP4_SSL', lambda *args, **kwargs: server)
    return MailTransport(), MailAccountSpec(provider='qq', address='reader@qq.com'), server


def test_readonly_mime_messages_and_on_demand_attachment(protocol):
    transport, spec, server = protocol
    result = transport.messages(spec, 'secret', 'INBOX', limit=2)
    assert result['uidvalidity'] == 42 and result['scanned_through'] == 2
    assert [message['uid'] for message in result['messages']] == [1, 2]
    message = result['messages'][0]
    assert message['subject'] == '周报' and message['body'] == '本周中文周报。'
    assert message['body_complete'] and message['sender_address'] == 'sender@example.org'
    assert message['reply_to'] == ['replies@example.org']
    assert message['references'] == ['<prior@example.org>']
    assert message['attachments'][0]['filename'] == '报告.pdf'
    assert not any('BODY.PEEK[2]' in str(call) for call in server.calls)
    assert all(call[2] is True for call in server.calls if call[0] == 'select')
    attachment = transport.attachment(spec, 'secret', 'INBOX', 1, '2', 42)
    assert attachment == {'filename': '报告.pdf', 'mime_type': 'application/pdf', 'data': b'attachment-bytes'}
    assert not any('STORE' in str(call) or 'BODY[' in str(call) for call in server.calls)


def test_pagination_and_unicode_search_do_not_skip_messages(protocol):
    transport, spec, server = protocol
    first = transport.messages(spec, 'secret', 'INBOX', limit=1)
    second = transport.messages(spec, 'secret', 'INBOX', after_uid=first['scanned_through'], limit=1)
    third = transport.messages(spec, 'secret', 'INBOX', after_uid=second['scanned_through'], limit=1)
    assert [first['messages'][0]['uid'], second['messages'][0]['uid'], third['messages'][0]['uid']] == [1, 2, 4]
    assert third['scanned_through'] == 4
    transport.messages(spec, 'secret', 'INBOX', query='周报')
    assert any(call[0] == 'SEARCH' and call[1:3] == ('CHARSET', 'UTF-8') for call in server.calls)
    assert transport.messages(spec, 'secret', 'INBOX', uid=2)['messages'][0]['uid'] == 2


def test_scan_freezes_select_uid_upper_bound_when_new_mail_arrives(protocol):
    transport, spec, server = protocol
    server.identifiers = [1, 2, 4, 5, 6]
    result = transport.messages(spec, 'secret', 'INBOX', after_uid=2, limit=50)
    assert [message['uid'] for message in result['messages']] == [4]
    assert result['highest_uid'] == result['scanned_through'] == 4
    assert next(call for call in server.calls if call[0] == 'SEARCH') == ('SEARCH', 'UID', '3:4')
    before = len(server.calls)
    empty = transport.messages(spec, 'secret', 'INBOX', after_uid=7)
    assert empty['messages'] == [] and empty['scanned_through'] == 7
    assert not any(call[0] == 'SEARCH' for call in server.calls[before:])


@pytest.mark.parametrize('query', [123, {}, '', ' ', 'x' * 4097, 'x\r\nLOGOUT'])
def test_search_query_is_bounded_and_rejects_command_injection(protocol, query):
    transport, spec, server = protocol
    with pytest.raises(ValueError):
        transport.messages(spec, 'secret', 'INBOX', query=query)
    assert server.calls == []


def test_list_folders_decodes_modified_utf7_and_escaped_literals(protocol, monkeypatch):
    transport, spec, server = protocol
    monkeypatch.setattr(server, 'list', lambda: ('OK', [b'(\\HasNoChildren) "/" "INBOX"',
        b'(\\Noselect) "/" "&ZTZO9nux- &- Projects"',
        b'() NIL "A\\\"B"', (b'() "/" {5}', b'Notes')]), raising=False)
    result = transport.folders(spec, 'secret')
    assert result['folders'] == [
        {'name': 'INBOX', 'delimiter': '/', 'selectable': True},
        {'name': '收件箱 & Projects', 'delimiter': '/', 'selectable': False},
        {'name': 'A"B', 'delimiter': None, 'selectable': True},
        {'name': 'Notes', 'delimiter': '/', 'selectable': True},
    ]
    assert not any(call[0] in {'FETCH', 'STORE', 'select'} for call in server.calls)


def test_uidvalidity_changes_and_attachment_path_injection_are_rejected(protocol):
    transport, spec, server = protocol
    with pytest.raises(MailTransportError, match='UIDVALIDITY'):
        transport.attachment(spec, 'secret', 'INBOX', 1, '2', 99)
    assert not any(call[0] == 'FETCH' for call in server.calls)
    with pytest.raises(ValueError):
        transport.attachment(spec, 'secret', 'INBOX', 1, '2] BODY[]', 42)
    with pytest.raises(ValueError):
        transport.messages(spec, 'secret', 'INBOX\r\nLOGOUT')


def test_unicode_folder_uses_imap_modified_utf7(protocol):
    transport, spec, server = protocol
    transport.mailbox(spec, 'secret', '收件箱 & Projects')
    folder = next(call[1] for call in server.calls if call[0] == 'select')
    assert folder == '"&ZTZO9nux- &- Projects"'


def test_body_limits_are_marked_unknown_and_attachment_limits_stop_before_fetch(protocol, monkeypatch):
    transport, spec, server = protocol
    monkeypatch.setattr(mail_transport, 'MAX_BODY_BYTES', 4)
    assert not transport.messages(spec, 'secret', 'INBOX', uid=1)['messages'][0]['body_complete']
    monkeypatch.setattr(mail_transport, 'MAX_ATTACHMENT_BYTES', 2)
    with pytest.raises(MailTransportError, match='size limit'):
        transport.attachment(spec, 'secret', 'INBOX', 1, '2', 42)
    assert not any('BODY.PEEK[2]' in str(call) for call in server.calls)


def test_nested_structure_quoted_escapes_and_literals():
    wire = b'1 (BODYSTRUCTURE (("TEXT" "HTML" NIL NIL NIL "7BIT" 12 1 NIL NIL)'
    wire += b'(("APPLICATION" "PDF" NIL NIL NIL "BASE64" 4 NIL ("ATTACHMENT" ("FILENAME" "a\\\"b.pdf")))'
    wire += b'("APPLICATION" "OCTET-STREAM" NIL NIL NIL "BASE64" 4 NIL ("ATTACHMENT" ("FILENAME" {5}\r\nx.pdf))) "MIXED") "MIXED"))'
    parts = _parts(_structure(wire))
    assert [part['part'] for part in parts] == ['1', '2.1', '2.2']
    assert parts[1]['filename'] == 'a"b.pdf' and parts[2]['filename'] == 'x.pdf'
    for bad in (b'BODYSTRUCTURE (', b'BODYSTRUCTURE ("bad', b'BODYSTRUCTURE {5}\r\nx'):
        with pytest.raises(MailTransportError):
            _structure(bad)
    calendar = _parts(_structure(b'BODYSTRUCTURE ("TEXT" "CALENDAR" NIL NIL NIL "7BIT" 12 1 NIL NIL)'))
    assert calendar[0]['attachment'] and calendar[0]['mime_type'] == 'text/calendar'


def test_html_body_strips_scripts_and_styles(protocol, monkeypatch):
    transport, spec, _server = protocol
    html = b'<style>bad-style</style><p>Hello &amp; world</p><script>bad-script</script>'
    monkeypatch.setattr(__import__(__name__), 'BODY', html)
    monkeypatch.setattr(__import__(__name__), 'STRUCTURE',
        b'("TEXT" "HTML" ("CHARSET" "UTF-8") NIL NIL "7BIT" ' + str(len(html)).encode() + b' 1 NIL NIL)')
    message = transport.messages(spec, 'secret', 'INBOX', uid=1)['messages'][0]
    assert 'Hello & world' in message['body']
    assert 'bad-script' not in message['body'] and 'bad-style' not in message['body']


@pytest.mark.parametrize(('charset', 'content'), [('unknown-mail-charset', b'visible text'), ('UTF-8', b'visible\xfftext')])
def test_uncertain_body_decoding_cannot_confirm_exclusion(protocol, monkeypatch, charset, content):
    from qqbot.runtime.mail_filters import MailTrigger

    transport, spec, _server = protocol
    monkeypatch.setattr(__import__(__name__), 'BODY', content)
    monkeypatch.setattr(__import__(__name__), 'STRUCTURE',
        b'("TEXT" "PLAIN" ("CHARSET" "' + charset.encode() + b'") NIL NIL "7BIT" '
        + str(len(content)).encode() + b' 1 NIL NIL)')
    message = transport.messages(spec, 'secret', 'INBOX', uid=1)['messages'][0]
    assert not message['body_complete'] and 'visible' in message['body']
    trigger = MailTrigger(account_id='test', none=({'field': 'body', 'op': 'contains', 'value': 'excluded'},))
    assert not trigger.matches(sender_address=message['sender_address'], subject=message['subject'],
        body=message['body'], body_complete=message['body_complete'])


def test_account_test_only_authenticates_and_tls_precedes_login(protocol, monkeypatch):
    transport, spec, _server = protocol
    calls = []

    class SMTPServer:
        def __init__(self, *args, **kwargs):
            calls.append('connect')
            assert kwargs['context'].check_hostname

        def login(self, username, secret):
            calls.append('login')

        def quit(self):
            calls.append('quit')

    monkeypatch.setattr(mail_transport.smtplib, 'SMTP_SSL', SMTPServer)
    assert transport.test(spec, 'secret')['sent'] is False
    assert calls == ['connect', 'login', 'quit']


def test_protocol_failures_do_not_expose_credentials(protocol, monkeypatch):
    transport, spec, server = protocol

    def fail(username, secret):
        raise mail_transport.imaplib.IMAP4.error('server echoed ' + secret)

    monkeypatch.setattr(server, 'login', fail)
    with pytest.raises(MailTransportError) as error:
        transport.mailbox(spec, 'very-secret-password', 'INBOX')
    assert 'very-secret-password' not in str(error.value)


def test_starttls_happens_before_authentication(protocol, monkeypatch):
    transport, spec, server = protocol
    spec = spec.model_copy(update={'imap_security': 'starttls', 'imap_port': 143,
                                   'smtp_security': 'starttls', 'smtp_port': 587})
    monkeypatch.setattr(mail_transport.imaplib, 'IMAP4', lambda *args, **kwargs: server)
    smtp_calls = []

    class SMTPServer:
        def __init__(self, *args, **kwargs):
            smtp_calls.append('connect')

        def ehlo(self):
            smtp_calls.append('ehlo')

        def starttls(self, **kwargs):
            smtp_calls.append('starttls')
            assert kwargs['context'].check_hostname

        def login(self, username, secret):
            smtp_calls.append('login')

        def quit(self):
            smtp_calls.append('quit')

    monkeypatch.setattr(mail_transport.smtplib, 'SMTP', SMTPServer)
    transport.test(spec, 'secret')
    assert [call[0] for call in server.calls] == ['starttls', 'login', 'logout']
    assert smtp_calls == ['connect', 'ehlo', 'starttls', 'ehlo', 'login', 'quit']
