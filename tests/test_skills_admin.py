"""Real native skill files remain scoped, exportable and guarded against stale rollback."""
import io
import zipfile

from fastapi.testclient import TestClient

from qqbot.app import create_app
from qqbot.config import Settings
from qqbot.permissions import PermissionRule

CONTENT = '---\nname: sample\ndescription: Use when checking a workflow.\n---\nRead the original source.\n'


def test_skill_management_reads_exports_and_rejects_stale_rollback(tmp_path, clock, monkeypatch):
    monkeypatch.setenv('QQBOT_ADMIN_TOKEN', 'skill-admin')
    app = create_app(Settings(database=str(tmp_path / 'core.db')), clock=clock, run_background=False)
    core = app.state.core
    headers = {'Authorization': 'Bearer skill-admin'}
    with TestClient(app) as client:
        assert client.get('/api/v1/skills').status_code == 401
        assert client.get('/api/v1/skills', headers=headers).json()['skills'] == []
        from qqbot.domain import AccessContext, ConversationRef, PrincipalRef, Surface
        access = AccessContext(conversation=ConversationRef(deployment_id=core.settings.deployment_id,
            adapter_instance_id='admin_api', bot_identity_id='host', surface=Surface.PRIVATE,
            remote_conversation_id=core.settings.owner.id), principal=PrincipalRef(adapter_instance_id='admin_api',
            identity_namespace='admin_api', remote_user_id=core.settings.owner.id), role='admin')
        provider = core.skills_for(access)
        assert provider.skill_manage({'operations': [{'action': 'create', 'name': 'sample', 'content': CONTENT}]})['success']
        assert provider.skill_manage({'operations': [{'action': 'patch', 'name': 'sample',
            'old_string': 'original source', 'new_string': 'current source'}]})['success']
        history = client.get('/api/v1/skills/history', params={'skill': 'sample'}, headers=headers).json()
        patch = next(entry for entry in history if entry['action'] == 'patch')
        body = {'entry_id': patch['id'], 'expected_latest_id': history[0]['id']}
        response = client.post('/api/v1/skills/rollback', json=body, headers=headers)
        assert response.status_code == 200, response.text
        assert client.post('/api/v1/skills/rollback', json=body, headers=headers).status_code == 409
        read = client.get('/api/v1/skills/read', params={'name': 'sample'}, headers=headers)
        assert read.status_code == 200 and read.json()['content'] == CONTENT
        response = client.get('/api/v1/skills/export', headers=headers)
        assert response.status_code == 200 and response.headers['content-type'] == 'application/zip'
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            assert archive.namelist() == ['skills/sample/SKILL.md']
            assert archive.read('skills/sample/SKILL.md').decode() == CONTENT
        assert client.get('/api/v1/skills/read', params={'name': '../outside'}, headers=headers).status_code == 404
        core.permissions.upsert(PermissionRule(id='deny-skills', effect='deny', actions=('skills.read',), resources=('*',)),
            expected_version=0, actor='fixture')
        assert client.get('/api/v1/skills/export', headers=headers).status_code == 403


def test_empty_skill_export_is_valid_and_link_poisoning_is_rejected(tmp_path):
    import pytest

    from qqbot.adapters.hermes_skills import HermesSkills
    provider = HermesSkills(tmp_path / 'owner')
    with zipfile.ZipFile(io.BytesIO(provider.export())) as archive:
        assert archive.namelist() == []
    (provider.root / 'skills' / 'outside').symlink_to(tmp_path)
    with pytest.raises(PermissionError, match='links'):
        provider.export()


def test_skill_export_rejects_fifo_without_waiting_for_a_writer(tmp_path):
    import os

    import pytest

    from qqbot.adapters.hermes_skills import HermesSkills
    provider = HermesSkills(tmp_path / 'owner')
    (provider.root / 'skills').mkdir(parents=True)
    os.mkfifo(provider.root / 'skills' / 'pipe')
    with pytest.raises(PermissionError, match='regular'):
        provider.export()
