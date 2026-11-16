"""Bounded, read-only IMAP reads and TLS connection tests using the standard library."""
import base64
import imaplib
import re
import smtplib
import ssl
from contextlib import contextmanager
from email import policy
from email.parser import BytesHeaderParser, BytesParser
from email.utils import getaddresses
from html.parser import HTMLParser

TIMEOUT = 20
MAX_BODY_BYTES = 256 * 1024
MAX_ATTACHMENT_BYTES = 20 * 1024 * 1024
MAX_HEADER_BYTES = 128 * 1024


class MailTransportError(ValueError):
    """A sanitized protocol failure safe to expose to an authenticated caller."""


class _HTMLText(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
        self.hidden = 0

    def handle_starttag(self, tag, attrs):
        if tag in {'script', 'style', 'head'}:
            self.hidden += 1
        if tag in {'p', 'br', 'div', 'li', 'tr'} and not self.hidden:
            self.parts.append('\n')

    def handle_endtag(self, tag):
        if tag in {'script', 'style', 'head'} and self.hidden:
            self.hidden -= 1

    def handle_data(self, data):
        if not self.hidden:
            self.parts.append(data)


def _quote(value):
    if not isinstance(value, str) or not value or any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError('invalid mailbox or search value')
    return '"' + value.replace('\\', '\\\\').replace('"', '\\"') + '"'


def _mailbox_name(folder):
    _quote(folder)
    pieces, encoded = [], []

    def flush():
        if encoded:
            value = ''.join(encoded).encode('utf-16-be')
            pieces.append('&' + base64.b64encode(value).decode().rstrip('=').replace('/', ',') + '-')
            encoded.clear()

    for char in folder:
        if ' ' <= char <= '~':
            flush()
            pieces.append('&-' if char == '&' else char)
        else:
            encoded.append(char)
    flush()
    return _quote(''.join(pieces))


def _decode_mailbox(value):
    def decode(match):
        encoded = match.group(1)
        if not encoded:
            return '&'
        raw = encoded.replace(',', '/')
        try:
            return base64.b64decode(raw + '=' * (-len(raw) % 4), validate=True).decode('utf-16-be')
        except (ValueError, UnicodeError):
            raise MailTransportError('IMAP folder encoding is invalid') from None

    return re.sub(r'&([A-Za-z0-9+,]*)-', decode, value)


def _structure(data):
    """Parse IMAP nested lists, escaped quoted strings and length-delimited literals."""
    if len(data) > 512 * 1024:
        raise MailTransportError('IMAP message structure exceeds the size limit')
    position = data.upper().find(b'BODYSTRUCTURE ')
    if position < 0:
        raise MailTransportError('IMAP message structure is unavailable')
    position += len(b'BODYSTRUCTURE ')

    def parse(depth=0):
        nonlocal position
        if depth > 64:
            raise MailTransportError('IMAP message structure is too deeply nested')
        while position < len(data) and data[position:position + 1] in b' \t\r\n':
            position += 1
        if position >= len(data):
            raise MailTransportError('IMAP message structure is incomplete')
        char = data[position:position + 1]
        position += 1
        if char == b'(':
            result = []
            while True:
                while position < len(data) and data[position:position + 1] in b' \t\r\n':
                    position += 1
                if position >= len(data):
                    raise MailTransportError('IMAP message structure is incomplete')
                if data[position:position + 1] == b')':
                    position += 1
                    return result
                result.append(parse(depth + 1))
        if char == b'"':
            value = bytearray()
            while position < len(data):
                char = data[position:position + 1]
                position += 1
                if char == b'"':
                    return value.decode('utf-8', 'replace')
                if char == b'\\':
                    char = data[position:position + 1]
                    position += 1
                value.extend(char)
            raise MailTransportError('IMAP quoted value is incomplete')
        if char == b'{':
            end = data.find(b'}\r\n', position)
            if end < 0 or not data[position:end].isdigit():
                raise MailTransportError('IMAP literal is invalid')
            size = int(data[position:end])
            position = end + 3
            value = data[position:position + size]
            if len(value) != size:
                raise MailTransportError('IMAP literal is incomplete')
            position += size
            return value.decode('utf-8', 'replace')
        start = position - 1
        while position < len(data) and data[position:position + 1] not in b' ()\t\r\n':
            position += 1
        value = data[start:position].decode('ascii', 'replace')
        return None if value.upper() == 'NIL' else value

    parsed = parse()
    if not isinstance(parsed, list):
        raise MailTransportError('IMAP message structure is invalid')
    return parsed


def _parameters(values):
    if not isinstance(values, list):
        return {}
    return {str(values[index]).lower(): str(values[index + 1])
            for index in range(0, len(values) - 1, 2)}


def _parts(node, prefix=''):
    if not node:
        raise MailTransportError('IMAP message part is invalid')
    if isinstance(node[0], list):
        count = next((index for index, value in enumerate(node) if not isinstance(value, list)), len(node))
        disposition = node[count + 2] if len(node) > count + 2 else None
        if isinstance(disposition, list) and disposition and str(disposition[0]).lower() == 'attachment':
            parameters = _parameters(disposition[1]) if len(disposition) > 1 else {}
            return [{'part': prefix or '1', 'mime_type': 'multipart/' + str(node[count]).lower(),
                     'size': sum(item['size'] for child in node[:count] for item in _parts(child)),
                     'filename': parameters.get('filename'), 'encoding': '7bit', 'charset': 'utf-8',
                     'attachment': True}]
        children = []
        for index, child in enumerate(node[:count], 1):
            children.extend(_parts(child, f'{prefix}.{index}' if prefix else str(index)))
        return children
    if len(node) < 7:
        raise MailTransportError('IMAP message part is incomplete')
    mime = (str(node[0]) + '/' + str(node[1])).lower()
    parameters = _parameters(node[2])
    disposition_index = 9 if mime.startswith('text/') else 11 if mime == 'message/rfc822' else 8
    disposition = node[disposition_index] if len(node) > disposition_index else None
    disposition_name = str(disposition[0]).lower() if isinstance(disposition, list) and disposition else ''
    disposition_parameters = _parameters(disposition[1]) if isinstance(disposition, list) and len(disposition) > 1 else {}
    header = BytesHeaderParser(policy=policy.default).parsebytes(
        ('Content-Type: ' + mime + ''.join('; ' + key + '="' + value.replace('"', '\\"') + '"'
            for key, value in parameters.items()) + '\r\nContent-Disposition: ' + (disposition_name or 'inline')
         + ''.join('; ' + key + '="' + value.replace('"', '\\"') + '"'
            for key, value in disposition_parameters.items()) + '\r\n\r\n').encode('utf-8'))
    filename = header.get_filename()
    try:
        size = int(node[6])
        if size < 0:
            raise ValueError
    except (TypeError, ValueError):
        raise MailTransportError('IMAP message part size is invalid') from None
    return [{'part': prefix or '1', 'mime_type': mime, 'size': size,
             'filename': filename, 'encoding': str(node[5]), 'charset': header.get_content_charset() or 'utf-8',
             'attachment': bool(filename or disposition_name == 'attachment'
                                or mime not in {'text/plain', 'text/html'})}]


def _wire(data):
    return b' '.join(part[0] + b'\r\n' + part[1] if isinstance(part, tuple) else part
                     for part in data if isinstance(part, (bytes, tuple)))


def _literal(data, maximum):
    literals = [part[1] for part in data if isinstance(part, tuple) and isinstance(part[1], bytes)]
    if not literals:
        raise MailTransportError('IMAP message data is unavailable')
    value = b''.join(literals)
    if len(value) > maximum:
        raise MailTransportError('IMAP response exceeds the size limit')
    return value


def _addresses(header, field):
    return [address for _, address in getaddresses(header.get_all(field, []), strict=True) if address]


class MailTransport:
    @contextmanager
    def imap(self, spec, secret):
        connection = None
        try:
            context = ssl.create_default_context()
            if spec.imap_security == 'ssl':
                connection = imaplib.IMAP4_SSL(spec.imap_host, spec.imap_port, ssl_context=context, timeout=TIMEOUT)
            else:
                connection = imaplib.IMAP4(spec.imap_host, spec.imap_port, timeout=TIMEOUT)
                connection.starttls(ssl_context=context)
            connection.login(spec.username, secret)
            yield connection
        except MailTransportError:
            raise
        except (OSError, imaplib.IMAP4.error, UnicodeError, ValueError, TypeError, IndexError):
            raise MailTransportError('IMAP operation failed; verify account settings and authorization') from None
        finally:
            if connection is not None:
                try:
                    connection.logout()
                except (OSError, imaplib.IMAP4.error):
                    pass

    @contextmanager
    def smtp(self, spec, secret):
        connection = None
        try:
            context = ssl.create_default_context()
            if spec.smtp_security == 'ssl':
                connection = smtplib.SMTP_SSL(spec.smtp_host, spec.smtp_port, timeout=TIMEOUT, context=context)
            else:
                connection = smtplib.SMTP(spec.smtp_host, spec.smtp_port, timeout=TIMEOUT)
                connection.ehlo()
                connection.starttls(context=context)
                connection.ehlo()
            connection.login(spec.username, secret)
            yield connection
        except (OSError, smtplib.SMTPException, UnicodeError, ValueError):
            raise MailTransportError('SMTP operation failed; verify account settings and authorization') from None
        finally:
            if connection is not None:
                try:
                    connection.quit()
                except (OSError, smtplib.SMTPException):
                    connection.close()

    @staticmethod
    def _ok(result):
        status, data = result
        if status != 'OK':
            raise MailTransportError('IMAP server rejected the operation')
        return data

    def _select(self, connection, folder):
        self._ok(connection.select(_mailbox_name(folder), readonly=True))
        validity = connection.response('UIDVALIDITY')[1]
        next_uid = connection.response('UIDNEXT')[1]
        try:
            uidvalidity = int(validity[0])
            highest_uid = int(next_uid[0]) - 1
        except (TypeError, ValueError, IndexError):
            raise MailTransportError('IMAP server did not provide stable UID metadata') from None
        if uidvalidity < 1 or highest_uid < 0:
            raise MailTransportError('IMAP server returned invalid UID metadata')
        return {'uidvalidity': uidvalidity, 'highest_uid': highest_uid}

    def test(self, spec, secret):
        with self.imap(spec, secret):
            pass
        with self.smtp(spec, secret):
            pass
        return {'imap': 'authenticated', 'smtp': 'authenticated', 'sent': False}

    def mailbox(self, spec, secret, folder):
        with self.imap(spec, secret) as connection:
            return self._select(connection, folder)

    def folders(self, spec, secret):
        with self.imap(spec, secret) as connection:
            data = self._ok(connection.list())
            result = []
            for entry in data:
                if entry is None:
                    continue
                values = _structure(b'BODYSTRUCTURE (' + _wire([entry]) + b')')
                if len(values) != 3 or not isinstance(values[0], list) or not isinstance(values[2], str):
                    raise MailTransportError('IMAP folder metadata is invalid')
                result.append({'name': _decode_mailbox(values[2]), 'delimiter': values[1],
                               'selectable': '\\noselect' not in {str(flag).lower() for flag in values[0]}})
            return {'folders': result}

    def _message(self, connection, uid):
        metadata = self._ok(connection.uid('FETCH', str(uid), '(UID BODYSTRUCTURE)'))
        parts = _parts(_structure(_wire(metadata)))
        raw_header = _literal(self._ok(connection.uid('FETCH', str(uid),
            f'(BODY.PEEK[HEADER]<0.{MAX_HEADER_BYTES}>)')), MAX_HEADER_BYTES)
        header = BytesHeaderParser(policy=policy.default).parsebytes(raw_header)
        bodies, complete = [], len(raw_header) < MAX_HEADER_BYTES
        text_parts = [part for part in parts if not part['attachment'] and part['mime_type'] == 'text/plain']
        if not text_parts:
            text_parts = [part for part in parts if not part['attachment'] and part['mime_type'] == 'text/html']
        remaining = MAX_BODY_BYTES
        for part in text_parts:
            if remaining <= 0:
                complete = False
                break
            encoded = _literal(self._ok(connection.uid('FETCH', str(uid),
                f'(BODY.PEEK[{part["part"]}]<0.{remaining}>)')), remaining)
            complete = complete and len(encoded) >= part['size']
            remaining -= len(encoded)
            message = BytesParser(policy=policy.default).parsebytes(
                (f'Content-Type: {part["mime_type"]}; charset="{part["charset"]}"\r\n'
                 f'Content-Transfer-Encoding: {part["encoding"]}\r\n\r\n').encode() + encoded)
            decoded = message.get_payload(decode=True) or b''
            try:
                body = decoded.decode(part['charset'])
            except UnicodeDecodeError:
                complete = False
                body = decoded.decode(part['charset'], 'replace')
            except LookupError:
                complete = False
                body = decoded.decode('utf-8', 'replace')
            if part['mime_type'] == 'text/html':
                parser = _HTMLText()
                parser.feed(body)
                body = ''.join(parser.parts)
            bodies.append(body)
        senders = _addresses(header, 'from')
        return {'uid': uid, 'sender_address': senders[0] if len(senders) == 1 else '',
                'to': _addresses(header, 'to'), 'cc': _addresses(header, 'cc'),
                'reply_to': _addresses(header, 'reply-to'), 'subject': str(header.get('subject', '')),
                'body': '\n'.join(bodies), 'body_complete': complete,
                'message_id': str(header.get('message-id', '')), 'in_reply_to': str(header.get('in-reply-to', '')),
                'references': re.findall(r'<[^<>\s]+>', str(header.get('references', ''))),
                'date': str(header.get('date', '')),
                'attachments': [{key: part[key] for key in ('part', 'filename', 'mime_type', 'size')}
                                for part in parts if part['attachment']]}

    def messages(self, spec, secret, folder, *, after_uid=0, limit=50, query=None, uid=None):
        if type(after_uid) is not int or after_uid < 0 or type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError('invalid mail pagination')
        if uid is not None and (type(uid) is not int or uid < 1):
            raise ValueError('invalid mail UID')
        if query is not None and (not isinstance(query, str) or not query.strip() or len(query) > 4096):
            raise ValueError('invalid mail search query')
        if query is not None:
            _quote(query)
        with self.imap(spec, secret) as connection:
            state = self._select(connection, folder)
            if (uid is None and after_uid >= state['highest_uid']) or (uid is not None and uid > state['highest_uid']):
                return state | {'scanned_through': after_uid, 'messages': []}
            criteria = ['UID', str(uid) if uid else f'{after_uid + 1}:{state["highest_uid"]}']
            if query:
                criteria.extend(['TEXT', _quote(query)])
            charset = 'UTF-8' if query and not query.isascii() else None
            arguments = [value.encode('utf-8') if charset else value for value in criteria]
            found = self._ok(connection.uid('SEARCH', *(['CHARSET', charset] if charset else []), *arguments))
            identifiers = sorted({int(value) for value in b' '.join(found or []).split()})
            identifiers = [value for value in identifiers if value <= state['highest_uid']
                           and (value == uid if uid else value > after_uid)]
            selected = identifiers[:limit]
            messages = [self._message(connection, value) for value in selected]
            scanned = selected[-1] if len(identifiers) > limit else max(after_uid, state['highest_uid'],
                                                                       selected[-1] if selected else 0)
            return state | {'scanned_through': scanned, 'messages': messages}

    def attachment(self, spec, secret, folder, uid, part, uidvalidity):
        if type(uid) is not int or uid < 1 or not isinstance(part, str) or not re.fullmatch(r'[1-9]\d*(?:\.[1-9]\d*)*', part):
            raise ValueError('invalid attachment reference')
        if type(uidvalidity) is not int or uidvalidity < 1:
            raise ValueError('invalid mailbox UIDVALIDITY')
        with self.imap(spec, secret) as connection:
            state = self._select(connection, folder)
            if state['uidvalidity'] != uidvalidity:
                raise MailTransportError('mailbox UIDVALIDITY changed; refresh the message before reading attachments')
            metadata = self._ok(connection.uid('FETCH', str(uid), '(UID BODYSTRUCTURE)'))
            selected = next((item for item in _parts(_structure(_wire(metadata)))
                             if item['part'] == part and item['attachment']), None)
            if selected is None:
                raise LookupError('mail attachment unavailable')
            if selected['size'] > MAX_ATTACHMENT_BYTES:
                raise MailTransportError('mail attachment exceeds the size limit')
            raw = _literal(self._ok(connection.uid('FETCH', str(uid),
                f'(BODY.PEEK[{part}]<0.{MAX_ATTACHMENT_BYTES + 1}>)')), MAX_ATTACHMENT_BYTES)
            if len(raw) < selected['size']:
                raise MailTransportError('mail attachment is incomplete')
            message = BytesParser(policy=policy.default).parsebytes(
                ('Content-Transfer-Encoding: ' + selected['encoding'] + '\r\n\r\n').encode() + raw)
            content = message.get_payload(decode=True) or b''
            return {'filename': selected['filename'] or 'attachment', 'mime_type': selected['mime_type'], 'data': content}
