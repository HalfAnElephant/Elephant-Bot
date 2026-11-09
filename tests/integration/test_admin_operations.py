import hashlib
import json
from uuid import uuid4

from fastapi.testclient import TestClient
from sqlalchemy import update

from qqbot.app import create_app
from qqbot.config import Settings
from qqbot.domain import AccessContext
from qqbot.storage.task_schema import tasks


def test_config_rollback_and_artifact_download_boundary(tmp_path, nap_config, nap_event, clock, monkeypatch):
    monkeypatch.setenv('QQBOT_ADMIN_TOKEN', 'admin-test-token')
    app = create_app(Settings(database=str(tmp_path/'core.db'), adapters=(nap_config,)), clock=clock, run_background=False)
    core = app.state.core
    event = core.inbox.accept(core.adapters['nap'].normalize(nap_event))[0]
    access = AccessContext(principal=event.principal, conversation=event.conversation, role='owner')
    task = core.tasks.accept(event, access, 'download fixture')
    core.tasks.archive_root.mkdir()
    content = b'verified report'
    artifact_id = str(uuid4())
    digest = hashlib.sha256(content).hexdigest()
    path = core.tasks.archive_root/artifact_id
    path.write_bytes(content)
    result = {'status':'completed', 'summary':'complete', 'artifacts':[{'id':artifact_id, 'relative_path':'outputs/report.md', 'mime_type':'text/markdown','size':len(content),'sha256':digest}]}
    with core.db.transaction() as c:
        c.execute(update(tasks).where(tasks.c.id==task['id']).values(state='SUCCEEDED',result_json=json.dumps(result)))
    auth = {'Authorization':'Bearer admin-test-token'}
    with TestClient(app) as client:
        first = client.get('/api/v1/config', headers=auth).json()
        candidate = {**first['config'], 'send_budget_per_minute':3}
        response = client.post('/api/v1/config/apply',headers=auth,json={'expected_version':first['version'],'config':candidate})
        assert response.status_code == 200
        assert first['version'] in client.get('/api/v1/config/versions',headers=auth).json()
        original = client.get('/api/v1/config/versions/'+first['version'],headers=auth).json()
        restored = client.post('/api/v1/config/apply',headers=auth,json={'expected_version':response.json()['version'],'config':original})
        assert restored.status_code == 200
        assert core.settings.send_budget_per_minute == first['config']['send_budget_per_minute']
        url = '/api/v1/artifacts/'+task['public_id']+'/'+digest+'/download'
        assert client.get(url).status_code == 401
        assert client.get(url,headers=auth).content == content
        path.unlink()
        path.symlink_to(tmp_path/'core.db')
        assert client.get(url,headers=auth).status_code == 409
        clock.advance(31*86400)
        assert client.get(url,headers=auth).status_code == 410


def test_unknown_delivery_resolution_does_not_resend(tmp_path, nap_config, nap_event, clock, monkeypatch):
    from sqlalchemy import select

    from qqbot.storage.schema import outbox
    monkeypatch.setenv('QQBOT_ADMIN_TOKEN', 'admin-test-token')
    app = create_app(Settings(database=str(tmp_path/'core.db'), adapters=(nap_config,)), clock=clock, run_background=False)
    core = app.state.core
    event = core.inbox.accept(core.adapters['nap'].normalize(nap_event))[0]
    with core.db.transaction() as c:
        delivery_id = core.outbox.enqueue_text(c,target=event.conversation,text='unknown receipt',origin_kind='chat',origin_id=event.event_id)[0]
        c.execute(update(outbox).where(outbox.c.id==delivery_id).values(state='UNKNOWN',attempts=1))
    auth = {'Authorization':'Bearer admin-test-token'}
    with TestClient(app) as client:
        url='/api/v1/deliveries/'+delivery_id+'/resolve'
        body={'state':'SENT','evidence':'operator confirmed the exact message in QQ','platform_message_id':'123'}
        assert client.post(url,json=body).status_code == 401
        assert client.post(url,json=body,headers=auth).json() == {'state':'SENT','resent':False}
        assert client.post(url,json=body,headers=auth).status_code == 409
        with core.db.engine.connect() as c:
            row=c.execute(select(outbox).where(outbox.c.id==delivery_id)).mappings().one()
            assert row['attempts']==1 and row['state']=='SENT'
