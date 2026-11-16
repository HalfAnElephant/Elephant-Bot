"""Scoped account management with encrypted credentials kept outside SQLite."""
import re
from contextlib import nullcontext
from pathlib import Path
from uuid import UUID, uuid4

from pydantic import ValidationError
from sqlalchemy import delete, insert, or_, select, update

from qqbot.adapters.qq_official.credential_store import load_credential, remove_credential, save_credential
from qqbot.domain import InboundEvent
from qqbot.mail.models import MailAccountSpec
from qqbot.mail.transport import MailTransport, MailTransportError
from qqbot.storage.mail_schema import mail_accounts

SETUP_PATH = '/api/v1/mail'


def _validated_spec(values):
    try:
        return MailAccountSpec.model_validate(values)
    except ValidationError:
        raise ValueError('invalid mailbox connection parameters') from None


class MailAccounts:
    def __init__(self, core, transport=None):
        self.core, self.db = core, core.db
        self.transport = transport or MailTransport()
        self.credential_dir = Path(self.db.path).parent / 'mail-credentials'

    def _identity(self, event, workspace_id, *, write=False):
        owner = self.core.sessions.owner(event)
        if not owner or not self.core.allowed(event):
            raise PermissionError('mail requires an authenticated active owner')
        self.core.sessions.validate_workspace(event, workspace_id)
        self.core.permissions.require(self.core.access(event), 'task.create' if write else 'task.read',
                                      'conversation:' + event.conversation.key)
        return owner

    def resolve(self, event, workspace_id, account_id, *, require_ready=False, conn=None):
        owner = self._identity(event, workspace_id)
        if not isinstance(account_id, str) or not account_id or len(account_id) > 128:
            raise ValueError('invalid mail account identifier')
        with (nullcontext(conn) if conn is not None else self.db.engine.connect()) as connection:
            row = connection.execute(select(mail_accounts).where(mail_accounts.c.owner_id == owner,
                mail_accounts.c.principal == event.principal.key,
                mail_accounts.c.conversation == event.conversation.key,
                mail_accounts.c.workspace_id == workspace_id,
                or_(mail_accounts.c.id == account_id, mail_accounts.c.account_id == account_id))).mappings().first()
        if row is None:
            raise LookupError('mail account unavailable in this authenticated scope')
        if require_ready and (not row['enabled'] or not row['credential_ref']):
            raise PermissionError('mail account is disabled or requires protected credential setup')
        return row

    @staticmethod
    def spec(row):
        return MailAccountSpec.model_validate_json(row['spec_json'])

    def credential(self, row):
        if not row['enabled'] or not row['credential_ref']:
            raise PermissionError('mail account is disabled or requires protected credential setup')
        return load_credential(self.credential_dir, row['credential_ref'], 'mail:' + row['id'])

    def view(self, row):
        source = InboundEvent.model_validate_json(row['event_json'])
        return {key: row[key] for key in ('id', 'account_id', 'revision', 'mailbox_generation', 'workspace_id',
                'credential_ref', 'last_test_at', 'last_error', 'created_at', 'updated_at')} | {
            'spec': self.spec(row).model_dump(mode='json'), 'enabled': bool(row['enabled']),
            'ready': bool(row['enabled'] and row['credential_ref']),
            'tested': row['tested_revision'] == row['revision'],
            'scope_label': ' / '.join((source.conversation.adapter_instance_id, source.conversation.surface,
                source.conversation.remote_conversation_id, row['workspace_id'])),
            'setup_path': SETUP_PATH + '?account_id=' + row['id'],
            'provider_guidance': {
                'icloud': 'Enable two-factor authentication and create an Apple app-specific password.',
                'qq': 'Enable IMAP/SMTP in QQ Mail settings and generate a mail authorization code.',
                '163': 'Enable IMAP/SMTP in 163 Mail settings and generate a client authorization code.',
                'custom': 'Enable IMAP/SMTP and obtain the provider-required app password or authorization code.',
            }[self.spec(row).provider] + ' Submit it only through the protected setup page.'}

    @staticmethod
    def _revision(arguments):
        revision = arguments.get('expected_revision')
        if type(revision) is not int or revision < 1:
            raise ValueError('expected_revision must be a positive integer')
        return revision

    def invoke(self, event, workspace_id, operation, arguments):
        if not isinstance(operation, str) or not operation:
            raise ValueError('invalid mail account operation')
        operation = operation.removeprefix('accounts.')
        fields = {'list': set(), 'get': {'account_id'}, 'create': {'account_id', 'spec', 'enabled'},
                  'update': {'account_id', 'expected_revision', 'changes'},
                  'test': {'account_id'}, 'disable': {'account_id', 'expected_revision'},
                  'delete': {'account_id', 'expected_revision'}}
        if operation not in fields or not isinstance(arguments, dict) or set(arguments) - fields[operation]:
            raise ValueError('invalid mail account operation or arguments')
        owner = self._identity(event, workspace_id, write=operation not in {'list', 'get'})
        if operation == 'list':
            with self.db.engine.connect() as conn:
                rows = conn.execute(select(mail_accounts).where(mail_accounts.c.owner_id == owner,
                    mail_accounts.c.principal == event.principal.key,
                    mail_accounts.c.conversation == event.conversation.key,
                    mail_accounts.c.workspace_id == workspace_id).order_by(mail_accounts.c.created_at)).mappings()
                return {'accounts': [self.view(row) for row in rows]}
        account_id = arguments.get('account_id')
        if operation == 'create':
            if not isinstance(account_id, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.@+\-]{0,127}', account_id):
                raise ValueError('mail account identifier must contain letters, digits or ._@+-')
            try:
                UUID(account_id)
            except ValueError:
                pass
            else:
                raise ValueError('mail account alias cannot be a UUID')
            spec = _validated_spec(arguments.get('spec'))
            enabled = arguments.get('enabled', True)
            if type(enabled) is not bool:
                raise ValueError('enabled must be a boolean')
            now = self.core.clock.now().isoformat()
            with self.db.transaction() as conn:
                old = conn.execute(select(mail_accounts).where(mail_accounts.c.owner_id == owner,
                    mail_accounts.c.principal == event.principal.key, mail_accounts.c.conversation == event.conversation.key,
                    mail_accounts.c.workspace_id == workspace_id, mail_accounts.c.account_id == account_id)).mappings().first()
                if old:
                    if self.spec(old) != spec or bool(old['enabled']) != enabled:
                        raise ValueError('mail account identifier already has different settings')
                    return self.view(old)
                identifier = str(uuid4())
                conn.execute(insert(mail_accounts).values(id=identifier, account_id=account_id, owner_id=owner,
                    principal=event.principal.key, conversation=event.conversation.key, workspace_id=workspace_id,
                    event_json=event.model_dump_json(), spec_json=spec.model_dump_json(), revision=1,
                    enabled=int(enabled), created_at=now, updated_at=now))
                return self.view(self.resolve(event, workspace_id, identifier, conn=conn))
        if operation == 'get':
            return self.view(self.resolve(event, workspace_id, account_id))
        if operation == 'test':
            row = self.resolve(event, workspace_id, account_id, require_ready=True)
            try:
                result = self.transport.test(self.spec(row), self.credential(row))
            except (MailTransportError, ValueError):
                with self.db.transaction() as conn:
                    conn.execute(update(mail_accounts).where(mail_accounts.c.id == row['id'],
                        mail_accounts.c.revision == row['revision']).values(last_error='mail_connection_failed',
                        tested_revision=None, last_test_at=self.core.clock.now().isoformat()))
                raise MailTransportError('mail connection test failed; verify the provider settings and authorization') from None
            with self.db.transaction() as conn:
                current = self.resolve(event, workspace_id, account_id, require_ready=True, conn=conn)
                if current['revision'] != row['revision']:
                    raise ValueError('mail account changed during the connection test')
                conn.execute(update(mail_accounts).where(mail_accounts.c.id == row['id']).values(
                    tested_revision=row['revision'], last_error=None, last_test_at=self.core.clock.now().isoformat()))
            return {'account_id': row['id'], 'status': result}
        old_reference = None
        with self.db.transaction() as conn:
            row = self.resolve(event, workspace_id, account_id, conn=conn)
            if row['revision'] != self._revision(arguments):
                raise ValueError('mail account revision conflict')
            if operation == 'delete':
                old_reference = row['credential_ref']
                conn.execute(delete(mail_accounts).where(mail_accounts.c.id == row['id']))
                result = {'id': row['id'], 'deleted': True}
            else:
                changes = arguments.get('changes', {}) if operation == 'update' else {'enabled': False}
                if not isinstance(changes, dict) or set(changes) - {'spec', 'enabled'}:
                    raise ValueError('mail account changes support spec and enabled only')
                enabled = changes.get('enabled', bool(row['enabled']))
                if type(enabled) is not bool:
                    raise ValueError('enabled must be a boolean')
                current_spec = self.spec(row)
                spec_changes = changes.get('spec', {})
                if not isinstance(spec_changes, dict):
                    raise ValueError('mail account spec changes must be an object')
                spec_values = current_spec.model_dump() | spec_changes
                if 'provider' in spec_changes and spec_changes['provider'] != current_spec.provider:
                    for key in ('imap_host', 'smtp_host', 'smtp_port', 'smtp_security'):
                        if key not in spec_changes:
                            spec_values.pop(key, None)
                spec = _validated_spec(spec_values)
                invalidate = spec != current_spec
                identity_fields = ('provider', 'address', 'username', 'imap_host', 'imap_port', 'imap_security')
                mailbox_changed = any(getattr(spec, key) != getattr(current_spec, key) for key in identity_fields)
                if invalidate:
                    old_reference = row['credential_ref']
                conn.execute(update(mail_accounts).where(mail_accounts.c.id == row['id']).values(
                    spec_json=spec.model_dump_json(), enabled=int(enabled), revision=row['revision'] + 1,
                    mailbox_generation=row['mailbox_generation'] + int(mailbox_changed),
                    credential_ref=None if invalidate else row['credential_ref'], tested_revision=None,
                    last_error=None, updated_at=self.core.clock.now().isoformat()))
                result = self.view(self.resolve(event, workspace_id, row['id'], conn=conn))
        if old_reference:
            remove_credential(self.credential_dir, old_reference)
        return result

    def admin_row(self, identifier, *, conn=None):
        with (nullcontext(conn) if conn is not None else self.db.engine.connect()) as connection:
            row = connection.execute(select(mail_accounts).where(mail_accounts.c.id == identifier,
                mail_accounts.c.owner_id == self.core.settings.owner.id)).mappings().first()
        if row is None:
            raise LookupError('mail account unavailable')
        return row

    def set_credential(self, identifier, secret, *, expected_revision):
        if not isinstance(secret, str) or not secret or len(secret) > 4096 or '\x00' in secret:
            raise ValueError('invalid mail credential')
        reference, old_reference = None, None
        try:
            with self.db.transaction() as conn:
                row = self.admin_row(identifier, conn=conn)
                if type(expected_revision) is not int or row['revision'] != expected_revision:
                    raise ValueError('mail account revision conflict')
                reference = save_credential(self.credential_dir, 'mail:' + identifier, secret)
                old_reference = row['credential_ref']
                conn.execute(update(mail_accounts).where(mail_accounts.c.id == identifier).values(
                    credential_ref=reference, revision=row['revision'] + 1, tested_revision=None, last_error=None,
                    updated_at=self.core.clock.now().isoformat()))
                result = self.view(self.admin_row(identifier, conn=conn))
        except BaseException:
            if reference:
                remove_credential(self.credential_dir, reference)
            raise
        if old_reference:
            remove_credential(self.credential_dir, old_reference)
        return result

    def mailbox(self, row, folder):
        return self.transport.mailbox(self.spec(row), self.credential(row), folder)

    def folders(self, row):
        return self.transport.folders(self.spec(row), self.credential(row))

    def messages(self, row, folder, **arguments):
        return self.transport.messages(self.spec(row), self.credential(row), folder, **arguments)

    def attachment(self, row, folder, uid, part, uidvalidity):
        return self.transport.attachment(self.spec(row), self.credential(row), folder, uid, part, uidvalidity)
