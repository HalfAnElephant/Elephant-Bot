import hashlib
import json
from uuid import uuid4

from fastapi.testclient import TestClient
from sqlalchemy import update

from qqbot.app import create_app
from qqbot.config import Settings
from qqbot.domain import AccessContext
from qqbot.storage.task_schema import tasks


def test_social_mode_save_restart_and_timed_silence_without_replay_registration(
    tmp_path, nap_config, nap_event, clock, monkeypatch
):
    monkeypatch.setenv("QQBOT_ADMIN_TOKEN", "admin-test-token")
    settings = Settings(database=str(tmp_path / "core.db"), adapters=(nap_config,))
    auth = {"Authorization": "Bearer admin-test-token"}
    app = create_app(settings, clock=clock, run_background=False)
    core = app.state.core
    event = core.inbox.accept(core.adapters["nap"].normalize(nap_event))[0]
    cid = event.conversation.key
    url = "/api/v1/conversations/" + cid + "/policy"
    with TestClient(app) as client:
        body = {"expected_version": 1, "mode": "social"}
        assert client.patch(url, json=body).status_code == 401
        response = client.patch(url, headers=auth, json=body)
        assert response.status_code == 200
        assert response.json()["mode"] == "social" and response.json()["policy_version"] == 2
        assert core.settings.social_evaluated_conversations == ()
        assert client.patch(url, headers=auth, json=body).status_code == 409
        assert client.patch(url, headers=auth, json={
            "expected_version": 2, "mode": "unknown",
        }).status_code == 422
        assert core.state(cid)["mode"] == "social" and core.state(cid)["policy_version"] == 2

    app = create_app(settings, clock=clock, run_background=False)
    with TestClient(app) as client:
        core = app.state.core
        saved = next(row for row in client.get("/api/v1/conversations", headers=auth).json() if row["id"] == cid)
        assert saved["policy"]["mode"] == "social" and saved["policy"]["policy_version"] == 2
        silence = {"silent_until": clock.now().isoformat(), "silent_resume_mode": "social"}
        core.save_state(cid, mode="silent", state_json=json.dumps(silence), expected_version=2)
        core.restore_silences()
        assert core.state(cid)["mode"] == "social" and core.state(cid)["policy_version"] == 4


def test_social_mode_still_requires_full_observation(tmp_path, official_config, official_event, clock, monkeypatch):
    monkeypatch.setenv("QQBOT_ADMIN_TOKEN", "admin-test-token")
    app = create_app(Settings(database=str(tmp_path / "core.db"), adapters=(official_config,)),
        clock=clock, run_background=False)
    core = app.state.core
    event = core.inbox.accept(core.adapters["qq"].normalize(official_event))[0]
    with TestClient(app) as client:
        response = client.patch("/api/v1/conversations/" + event.conversation.key + "/policy",
            headers={"Authorization": "Bearer admin-test-token"}, json={"expected_version": 1, "mode": "social"})
        assert response.status_code == 422 and "全量观察能力" in response.json()["detail"]
        assert core.state(event.conversation.key)["mode"] == "addressed_only"
        assert core.state(event.conversation.key)["policy_version"] == 1


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
        assert client.get(url,headers=auth).headers["Content-Disposition"].endswith("report.md")
        result["artifacts"][0]["relative_path"] = "outputs/报告.pdf"
        with core.db.transaction() as c:
            c.execute(update(tasks).where(tasks.c.id == task["id"]).values(result_json=json.dumps(result)))
        assert client.get(url,headers=auth).headers["Content-Disposition"].endswith("%E6%8A%A5%E5%91%8A.pdf")
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


def test_pi_model_change_requires_restart_without_partial_apply(tmp_path, nap_config, clock, monkeypatch):
    monkeypatch.setenv("QQBOT_ADMIN_TOKEN", "admin-test-token")
    app = create_app(
        Settings(database=str(tmp_path / "core.db"), adapters=(nap_config,)),
        clock=clock,
        run_background=False,
    )
    auth = {"Authorization": "Bearer admin-test-token"}
    with TestClient(app) as client:
        first = client.get("/api/v1/config", headers=auth).json()
        candidate = {
            **first["config"],
            "model": {**first["config"]["model"], "model": "another-model"},
            "send_budget_per_minute": 3,
        }
        response = client.post(
            "/api/v1/config/apply",
            headers=auth,
            json={"expected_version": first["version"], "config": candidate},
        )
        assert response.status_code == 409 and "restarting Core and Worker" in response.json()["detail"]
        assert client.get("/api/v1/config", headers=auth).json() == first
        assert not (tmp_path / "runtime-config.json").exists()
        budget_only = {**first["config"], "send_budget_per_minute": 3}
        response = client.post(
            "/api/v1/config/apply",
            headers=auth,
            json={"expected_version": first["version"], "config": budget_only},
        )
        assert response.status_code == 200
        applied = client.get("/api/v1/config", headers=auth).json()
        assert applied["config"]["send_budget_per_minute"] == 3
        assert applied["config"]["model"] == first["config"]["model"]


def test_confirmed_question_delivery_updates_wait_and_task_view(tmp_path, nap_config, nap_event, clock, monkeypatch):
    from sqlalchemy import select

    from qqbot.storage.schema import outbox
    from qqbot.storage.task_schema import waits
    monkeypatch.setenv("QQBOT_ADMIN_TOKEN", "admin-test-token")
    app = create_app(Settings(database=str(tmp_path / "core.db"), adapters=(nap_config,)),
        clock=clock, run_background=False)
    core = app.state.core
    event = core.inbox.accept(core.adapters["nap"].normalize(nap_event))[0]
    access = AccessContext(principal=event.principal, conversation=event.conversation)
    task = core.tasks.accept(event, access, "question")
    claim = core.tasks.claim("w")
    core.tasks.ask(claim["run_id"], 1, "w", {"question_id": "q", "text": "Which target?", "checkpoint": "No actions"})
    core.tasks.submit(claim["run_id"], 1, "w", {"status": "waiting_input", "summary": "Waiting"})
    with core.db.transaction() as c:
        delivery_id = c.execute(select(outbox.c.id).where(outbox.c.task_id == task["id"])).scalar_one()
        c.execute(update(outbox).where(outbox.c.id == delivery_id).values(state="UNKNOWN"))
    auth = {"Authorization": "Bearer admin-test-token"}
    with TestClient(app) as client:
        view = client.get("/api/v1/tasks/" + task["public_id"], headers=auth).json()
        assert view["deliveries"] == [{"id": delivery_id, "state": "UNKNOWN"}]
        response = client.post("/api/v1/deliveries/" + delivery_id + "/resolve", headers=auth,
            json={"state": "SENT", "evidence": "Confirmed exact question", "platform_message_id": "confirmed-question"})
        assert response.status_code == 200
    with core.db.engine.connect() as c:
        wait = c.execute(select(waits)).mappings().one()
        assert wait["delivered_at"] == clock.now().isoformat() and wait["expires_at"]
