"""Public setup shell with authenticated, owner-only mail account APIs."""
import asyncio
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationError
from sqlalchemy import select

from qqbot.domain import ConversationRef, InboundEvent, PrincipalRef, Surface
from qqbot.domain.messages import TextSegment
from qqbot.harness.public_web import bounded_json
from qqbot.mail.models import MailAccountSpec
from qqbot.permissions import PermissionSubject
from qqbot.storage.agent_schema import dialogue_inputs, dialogues
from qqbot.storage.mail_schema import mail_accounts
from qqbot.storage.schema import inbox


class AccountCreate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    account_id: str
    workspace_id: str = 'personal'
    scope_event_id: str | None = Field(default=None, min_length=1, max_length=256)
    spec: MailAccountSpec
    enabled: bool = True


class AccountUpdate(BaseModel):
    model_config = ConfigDict(extra='forbid')
    expected_revision: int = Field(ge=1)
    changes: dict


class CredentialSubmission(BaseModel):
    model_config = ConfigDict(extra='forbid')
    expected_revision: int = Field(ge=1)
    secret: SecretStr


def build_mail_router(core, admin):
    router = APIRouter(prefix='/api/v1/mail')

    @router.get('')
    def setup_page():
        return FileResponse(Path(__file__).with_name('mail.html'), headers={'Cache-Control': 'no-store'})

    def authorize(request: Request):
        subject = PermissionSubject(adapter_instance_id='admin_api', identity_namespace='admin_api', user_id='owner')
        core.permissions.require(subject, 'admin.' + request.method.lower(), 'http:' + request.url.path)

    protected = APIRouter(prefix='/accounts', dependencies=[Depends(admin), Depends(authorize)])

    def event():
        now = core.clock.now()
        return InboundEvent(event_id=str(uuid4()), event_type='message.created',
            principal=PrincipalRef(adapter_instance_id='admin_api', identity_namespace='admin_api',
                remote_user_id=core.settings.owner.id),
            conversation=ConversationRef(deployment_id=core.settings.deployment_id, adapter_instance_id='admin_api',
                bot_identity_id='host', surface=Surface.PRIVATE, remote_conversation_id=core.settings.owner.id),
            platform_message_id='admin-mail-account', received_at=now, occurred_at=now,
            segments=(TextSegment(text='Manage mailbox configuration'),), trace_id=str(uuid4()))

    def account_event(identifier):
        row = core.mail.accounts.admin_row(identifier)
        return row, InboundEvent.model_validate_json(row['event_json'])

    @protected.get('/scopes')
    def list_scopes():
        with core.db.engine.connect() as conn:
            rows = conn.execute(select(dialogue_inputs.c.event_json, dialogues.c.workspace_id,
                dialogues.c.title).join(dialogues, dialogues.c.id == dialogue_inputs.c.dialogue_id).join(inbox,
                inbox.c.id == dialogue_inputs.c.id).where(dialogues.c.owner_id == core.settings.owner.id,
                dialogues.c.state == 'ACTIVE', inbox.c.event_type == 'message.created')
                .order_by(dialogue_inputs.c.created_at.desc())).mappings()
            scopes = {}
            for row in rows:
                source = InboundEvent.model_validate_json(row['event_json'])
                if (source.principal.identity_namespace == 'admin_api'
                        or core.sessions.owner(source) != core.settings.owner.id or not core.allowed(source)):
                    continue
                try:
                    core.sessions.validate_workspace(source, row['workspace_id'])
                except (PermissionError, ValueError):
                    continue
                key = (source.principal.key, source.conversation.key, row['workspace_id'])
                scopes.setdefault(key, {'scope_event_id': source.event_id, 'workspace_id': row['workspace_id'],
                    'label': source.conversation.adapter_instance_id + ' / ' + row['title'] + ' / ' + row['workspace_id']})
            return {'scopes': list(scopes.values())}

    @protected.get('')
    def list_accounts():
        with core.db.engine.connect() as conn:
            rows = conn.execute(select(mail_accounts).where(
                mail_accounts.c.owner_id == core.settings.owner.id).order_by(mail_accounts.c.created_at)).mappings()
            return {'accounts': [core.mail.accounts.view(row) for row in rows]}

    @protected.post('')
    def create_account(body: AccountCreate):
        source = event()
        if body.scope_event_id:
            with core.db.engine.connect() as conn:
                raw = conn.execute(select(dialogue_inputs.c.event_json).join(dialogues,
                    dialogues.c.id == dialogue_inputs.c.dialogue_id).where(
                    dialogue_inputs.c.id == body.scope_event_id, dialogues.c.owner_id == core.settings.owner.id,
                    dialogues.c.workspace_id == body.workspace_id, dialogues.c.state == 'ACTIVE')).scalar_one_or_none()
            if raw is None:
                raise HTTPException(403, 'mail account scope is unavailable')
            source = InboundEvent.model_validate_json(raw)
            if core.sessions.owner(source) != core.settings.owner.id or not core.allowed(source):
                raise HTTPException(403, 'mail account scope is no longer authorized')
        return core.mail.accounts.invoke(source, body.workspace_id, 'create', {
            'account_id': body.account_id, 'spec': body.spec.model_dump(mode='json'), 'enabled': body.enabled})

    @protected.get('/{identifier}')
    def get_account(identifier: str):
        return core.mail.accounts.view(core.mail.accounts.admin_row(identifier))

    @protected.patch('/{identifier}')
    def update_account(identifier: str, body: AccountUpdate):
        row, source = account_event(identifier)
        return core.mail.accounts.invoke(source, row['workspace_id'], 'update', {
            'account_id': identifier, 'expected_revision': body.expected_revision, 'changes': body.changes})

    @protected.delete('/{identifier}')
    def delete_account(identifier: str, expected_revision: int):
        row, source = account_event(identifier)
        return core.mail.accounts.invoke(source, row['workspace_id'], 'delete', {
            'account_id': identifier, 'expected_revision': expected_revision})

    @protected.post('/{identifier}/test')
    def test_account(identifier: str):
        row, source = account_event(identifier)
        return core.mail.accounts.invoke(source, row['workspace_id'], 'test', {'account_id': identifier})

    @protected.post('/{identifier}/credential')
    async def submit_credential(identifier: str, request: Request):
        try:
            body = CredentialSubmission.model_validate(await bounded_json(request, 16384))
        except ValidationError:
            raise HTTPException(422, 'invalid credential submission') from None
        return await asyncio.to_thread(core.mail.accounts.set_credential, identifier, body.secret.get_secret_value(),
            expected_revision=body.expected_revision)

    router.include_router(protected)
    return router
