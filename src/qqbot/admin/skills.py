"""Owner-authenticated inspection, export and guarded rollback of Hermes files."""
import asyncio
import json

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import Response
from pydantic import BaseModel, ConfigDict, Field

from qqbot.domain import AccessContext, ConversationRef, PrincipalRef, Surface
from qqbot.permissions import PermissionSubject


class SkillRollback(BaseModel):
    model_config = ConfigDict(extra='forbid')
    entry_id: str = Field(min_length=1, max_length=128)
    expected_latest_id: str = Field(min_length=1, max_length=128)
    workspace_id: str = Field(default='personal', min_length=1, max_length=128)


def build_skills_router(core, authenticate):
    subject = PermissionSubject(adapter_instance_id='admin_api', identity_namespace='admin_api', user_id='owner')

    def authorize_admin(request: Request):
        core.permissions.require(subject, 'admin.' + request.method.lower(), 'http:' + request.url.path)

    router = APIRouter(prefix='/api/v1/skills', dependencies=[Depends(authenticate), Depends(authorize_admin)])

    def adapter(workspace_id, *, write=False):
        conversation = ConversationRef(deployment_id=core.settings.deployment_id, adapter_instance_id='admin_api',
            bot_identity_id='host', surface=Surface.PRIVATE, remote_conversation_id=core.settings.owner.id)
        principal = PrincipalRef(adapter_instance_id='admin_api', identity_namespace='admin_api',
            remote_user_id=core.settings.owner.id)
        access = AccessContext(conversation=conversation, principal=principal, role='admin')
        action = 'skills.write' if write else 'skills.read'
        resource = 'conversation:' + conversation.key
        core.permissions.require(subject, action, resource)
        core.permissions.require(access, action, resource)
        provider = core.skills_for(access, workspace_id=workspace_id)
        if provider is None:
            raise HTTPException(409, 'skills are disabled for this owner')
        return provider

    @router.get('')
    async def list_skills(workspace_id: str = 'personal'):
        result = await asyncio.to_thread(adapter(workspace_id).skills_list)
        adapter(workspace_id)
        return result

    @router.get('/read')
    async def read_skill(name: str, file_path: str | None = None, workspace_id: str = 'personal'):
        arguments = {'name': name}
        if file_path is not None:
            arguments['file_path'] = file_path
        result = await asyncio.to_thread(adapter(workspace_id).skill_view, arguments)
        adapter(workspace_id)
        if not result.get('success'):
            raise HTTPException(404, 'skill or supporting file unavailable')
        return result

    @router.get('/history')
    async def history(workspace_id: str = 'personal', skill: str | None = None, limit: int = Query(default=50, ge=1, le=1000)):
        result = await asyncio.to_thread(adapter(workspace_id).ledger, skill=skill, limit=limit)
        adapter(workspace_id)
        return result

    @router.get('/export')
    async def export(workspace_id: str = 'personal'):
        content = await asyncio.to_thread(adapter(workspace_id).export)
        adapter(workspace_id)
        return Response(content, media_type='application/zip', headers={
            'Content-Disposition': 'attachment; filename="elephant-skills.zip"', 'Cache-Control': 'no-store'})

    @router.post('/rollback')
    async def rollback(body: SkillRollback):
        provider = adapter(body.workspace_id, write=True)
        ok, message = await asyncio.to_thread(provider.rollback, body.entry_id,
                                              expected_latest_id=body.expected_latest_id)
        if not ok:
            raise HTTPException(409, message)
        core.audit('skills.rollback', body.entry_id, 'admin_api:owner',
            json.dumps({'workspace_id': body.workspace_id, 'expected_latest_id': body.expected_latest_id}))
        return {'success': True, 'entry_id': body.entry_id, 'message': message}

    return router
