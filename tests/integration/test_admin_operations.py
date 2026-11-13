import hashlib
import json
from uuid import uuid4

from fastapi.testclient import TestClient
from sqlalchemy import select, update

from qqbot.app import create_app
from qqbot.config import Settings
from qqbot.domain import AccessContext, InboundEvent
from qqbot.storage.task_schema import tasks


def test_conversation_policy_only_allows_addressed_or_silent(tmp_path, official_config, official_event, clock, monkeypatch):
    monkeypatch.setenv("QQBOT_ADMIN_TOKEN", "admin-test-token")
    app = create_app(Settings(database=str(tmp_path / "core.db"), adapters=(official_config,)),
                     clock=clock, run_background=False)
    core = app.state.core
    event = core.inbox.accept(core.adapters["qq"].normalize(official_event))[0]
    cid = event.conversation.key
    url = "/api/v1/conversations/" + cid + "/policy"
    auth = {"Authorization": "Bearer admin-test-token"}
    with TestClient(app) as client:
        assert client.patch(url, json={"expected_version": 1, "mode": "silent"}).status_code == 401
        assert client.patch(url, headers=auth, json={"expected_version": 1, "mode": "social"}).status_code == 422
        assert core.state(cid)["mode"] == "addressed_only"
        response = client.patch(url, headers=auth, json={"expected_version": 1, "mode": "silent"})
        assert response.status_code == 200
        assert response.json()["mode"] == "silent" and response.json()["policy_version"] == 2
        response = client.patch(url, headers=auth, json={"expected_version": 2, "mode": "addressed_only"})
        assert response.status_code == 200
        assert response.json()["mode"] == "addressed_only" and response.json()["policy_version"] == 3
        assert client.patch(url, headers=auth, json={"expected_version": 3, "mode": "shadow"}).status_code == 422
        assert core.state(cid)["mode"] == "addressed_only" and core.state(cid)["policy_version"] == 3


def test_config_rollback_and_artifact_download_boundary(tmp_path, official_config, official_event, clock, monkeypatch):
    monkeypatch.setenv('QQBOT_ADMIN_TOKEN', 'admin-test-token')
    settings = Settings(database=str(tmp_path/'core.db'), adapters=(official_config,), owner={
        "qq_bindings": ({"app_id": official_config.app_id, "user_openid": "300",
                         "verified_at": clock.now().isoformat()},),
    })
    app = create_app(settings, clock=clock, run_background=False)
    core = app.state.core
    event = core.inbox.accept(core.adapters['qq'].normalize(official_event))[0]
    access = core.access(event)
    with core.db.transaction() as c:
        dialogue = core.sessions.dialogue(c, event)
        task_view = core.tasks.graph.submit(c, dialogue, event, access, {
            "goal": "download fixture", "success_checks": [], "requested_outputs": [],
            "children": [{"mode": "work", "goal": "download fixture", "delegation_key": "primary",
                          "success_checks": [], "requested_outputs": []}],
        }, "artifact-download-test", core.sessions.binding(), {})
        task = c.execute(select(tasks).where(tasks.c.public_id == task_view["public_id"])).mappings().one()
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
        assert restored.status_code == 200, restored.text
        assert core.settings.send_budget_per_minute == first['config']['send_budget_per_minute']
        url = '/api/v1/artifacts/'+task['id']+'/'+digest+'/download'
        assert client.get(url).status_code == 401
        assert client.get(url,headers=auth).content == content
        assert client.get('/api/v1/artifacts/'+task['public_id']+'/'+digest+'/download', headers=auth).content == content
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


def test_unknown_delivery_resolution_does_not_resend(tmp_path, official_config, official_event, clock, monkeypatch):
    from sqlalchemy import select

    from qqbot.storage.schema import outbox
    monkeypatch.setenv('QQBOT_ADMIN_TOKEN', 'admin-test-token')
    app = create_app(Settings(database=str(tmp_path/'core.db'), adapters=(official_config,)), clock=clock, run_background=False)
    core = app.state.core
    event = core.inbox.accept(core.adapters['qq'].normalize(official_event))[0]
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


def test_pi_model_change_requires_restart_without_partial_apply(tmp_path, official_config, clock, monkeypatch):
    monkeypatch.setenv("QQBOT_ADMIN_TOKEN", "admin-test-token")
    app = create_app(
        Settings(database=str(tmp_path / "core.db"), adapters=(official_config,)),
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


def test_admin_task_uses_host_owner_and_idempotency(tmp_path, clock, monkeypatch):
    monkeypatch.setenv("QQBOT_ADMIN_TOKEN", "admin-test-token")
    app = create_app(Settings(database=str(tmp_path / "core.db"), tasks_enabled=True),
                     clock=clock, run_background=False)
    auth = {"Authorization": "Bearer admin-test-token"}
    with TestClient(app) as client:
        created = client.post("/api/v1/tasks", headers={**auth, "Idempotency-Key": "console-task-1"},
                              json={"goal": "Review the current project status"})
        assert created.status_code == 200
        task = created.json()
        assert task["kind"] == "goal" and task["public_id"]
        duplicate = client.post("/api/v1/tasks", headers={**auth, "Idempotency-Key": "console-task-1"},
                                json={"goal": "Review the current project status"})
        assert duplicate.status_code == 200 and duplicate.json()["public_id"] == task["public_id"]
        with app.state.core.db.engine.connect() as conn:
            row = conn.execute(select(tasks).where(tasks.c.public_id == task["public_id"])).mappings().one()
        event = InboundEvent.model_validate_json(row["event_json"])
        assert row["owner_id"] == app.state.core.settings.owner.id
        assert event.conversation.adapter_instance_id == "admin_api"
        assert client.get("/api/v1/tasks", headers=auth).json()[0]["public_id"] == task["public_id"]
        assert client.post("/api/v1/tasks", headers={**auth, "Idempotency-Key": "console-task-2"},
                           json={"conversation_id": "fake-qq", "goal": "Not accepted"}).status_code == 422


def test_admin_can_read_and_cancel_owner_qq_task_by_id_or_public_id(
    tmp_path, official_config, official_event, clock, monkeypatch,
):
    from sqlalchemy import select

    monkeypatch.setenv("QQBOT_ADMIN_TOKEN", "admin-test-token")
    settings = Settings(database=str(tmp_path / "core.db"), adapters=(official_config,), owner={
        "qq_bindings": ({"app_id": official_config.app_id, "user_openid": "300",
                         "verified_at": clock.now().isoformat()},),
    })
    app = create_app(settings, clock=clock, run_background=False)
    core = app.state.core
    event = core.inbox.accept(core.adapters["qq"].normalize(official_event))[0]
    access = core.access(event)
    with core.db.transaction() as conn:
        dialogue = core.sessions.dialogue(conn, event)
        created = core.tasks.graph.submit(conn, dialogue, event, access, {
            "goal": "owner QQ task", "success_checks": [], "requested_outputs": [],
            "children": [{"mode": "work", "goal": "owner QQ task", "delegation_key": "primary",
                          "success_checks": [], "requested_outputs": []}],
        }, "owner-qq-task-test", core.sessions.binding(), {})
        row = conn.execute(select(tasks).where(tasks.c.public_id == created["public_id"])).mappings().one()

    auth = {"Authorization": "Bearer admin-test-token"}
    with TestClient(app) as client:
        listed = client.get("/api/v1/tasks", headers=auth)
        assert listed.status_code == 200
        assert any(item["public_id"] == row["public_id"] for item in listed.json())
        for task_ref in (row["id"], row["public_id"]):
            response = client.get("/api/v1/tasks/" + task_ref, headers=auth)
            assert response.status_code == 200
            assert response.json()["task_id"] == row["id"]
        cancelled = client.post("/api/v1/tasks/" + row["id"] + "/cancel", headers=auth)
        assert cancelled.status_code == 200
        assert cancelled.json()["state"] == "CANCELLED"


def test_memory_restore_reconcile_is_authenticated_and_uses_private_owner_scope(
    tmp_path, clock, monkeypatch,
):
    monkeypatch.setenv("QQBOT_ADMIN_TOKEN", "admin-test-token")
    app = create_app(Settings(database=str(tmp_path / "core.db")), clock=clock, run_background=False)
    core = app.state.core
    seen = []

    async def reconcile(scope):
        seen.append(scope)
        return {"state": "READY"}

    monkeypatch.setattr(core.openviking, "reconcile_restore", reconcile)
    original_check = core.permissions.check
    checked = []

    def check(subject, action, resource, *args, **kwargs):
        if action == "memory.write":
            checked.append((action, resource))
        return original_check(subject, action, resource, *args, **kwargs)

    monkeypatch.setattr(core.permissions, "check", check)
    with TestClient(app) as client:
        assert client.post("/api/v1/memory/reconcile").status_code == 401
        response = client.post("/api/v1/memory/reconcile", headers={
            "Authorization": "Bearer admin-test-token",
        }, json={"owner_id": "another-owner", "audience": "group"})
        assert response.status_code == 200 and response.json() == {"state": "READY"}

    assert len(seen) == 1
    assert seen[0].owner_id == core.settings.owner.id
    assert seen[0].workspace_id == "personal" and seen[0].audience == "private"
    assert checked == [("memory.write", "memory:" + seen[0].key)]


def test_confirmed_question_delivery_updates_wait_without_exposing_non_owner_task(
    tmp_path, official_config, official_event, clock, monkeypatch,
):
    from sqlalchemy import select

    from qqbot.storage.schema import outbox
    from qqbot.storage.task_schema import waits
    monkeypatch.setenv("QQBOT_ADMIN_TOKEN", "admin-test-token")
    app = create_app(Settings(database=str(tmp_path / "core.db"), adapters=(official_config,)),
        clock=clock, run_background=False)
    core = app.state.core
    event = core.inbox.accept(core.adapters["qq"].normalize(official_event))[0]
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
        assert client.get("/api/v1/tasks/" + task["public_id"], headers=auth).status_code == 404
        response = client.post("/api/v1/deliveries/" + delivery_id + "/resolve", headers=auth,
            json={"state": "SENT", "evidence": "Confirmed exact question", "platform_message_id": "confirmed-question"})
        assert response.status_code == 200
    with core.db.engine.connect() as c:
        wait = c.execute(select(waits)).mappings().one()
        assert wait["delivered_at"] == clock.now().isoformat() and wait["expires_at"]
