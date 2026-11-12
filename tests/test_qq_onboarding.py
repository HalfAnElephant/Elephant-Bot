import asyncio
import base64
import json
import os
from types import SimpleNamespace

import httpx
import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from fastapi import FastAPI, HTTPException, Request

from qqbot.adapters.qq_official.credential_store import load_credential, save_credential
from qqbot.adapters.qq_official.onboarding import Onboarding
from qqbot.admin.qq_onboarding import install_qq_onboarding
from qqbot.config import Settings
from qqbot.configuration import ConfigurationStore, version


def encrypted(key, secret="private-client-secret"):
    nonce = os.urandom(12)
    return base64.b64encode(nonce + AESGCM(key).encrypt(nonce, secret.encode(), None)).decode()


def protocol(status=2, tamper=False):
    keys = {}
    def handler(request):
        payload = json.loads(request.content)
        if request.url.path.endswith("create_bind_task"):
            task = str(len(keys) + 1)
            keys[task] = base64.b64decode(payload["key"])
            return httpx.Response(200, json={"retcode":0,"data":{"task_id":task}})
        if request.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token":"hidden-access-token"})
        task = payload["task_id"]
        return httpx.Response(200, json={"retcode":0,"data":{"status":status,
            "bot_appid":"123" + task,"user_openid":"scanner-is-not-admin",
            "bot_encrypt_secret":"bad" if tamper else encrypted(keys[task])}})
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def await_state(session, expected):
    for _ in range(100):
        if session.state == expected:
            return
        await asyncio.sleep(.001)
    assert session.state == expected


@pytest.mark.asyncio
async def test_sessions_are_owned_and_concurrent_and_redacted():
    async with protocol() as client:
        service = Onboarding(client=client)
        one = await service.create("owner", "first")
        two = await service.create("owner", "second")
        await await_state(one, "ready")
        await await_state(two, "ready")
        assert one.app_id != two.app_id
        assert one.key == b""
        assert "private-client-secret" not in json.dumps(one.public())
        assert "scanner-is-not-admin" not in json.dumps(one.public())
        with pytest.raises(LookupError):
            service.get(one.id, "other-owner")
        with pytest.raises(ValueError):
            await service.create("owner", "first")
        await service.cancel(one.id, "owner")
        assert one.secret is None
        assert two.state == "ready"
        await service.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("status,tamper,state", [(3,False,"expired"),(2,True,"failed")])
async def test_platform_expiration_and_bad_crypto(status,tamper,state):
    async with protocol(status,tamper) as client:
        service = Onboarding(client=client)
        session = await service.create("owner", "one")
        await await_state(session,state)
        assert session.secret is None and session.key == b""
        await service.close()


@pytest.mark.asyncio
async def test_completed_but_unconfirmed_credentials_expire_without_reads():
    async with protocol() as client:
        service = Onboarding(client=client,timeout=.03)
        session = await service.create("owner", "one")
        await await_state(session,"ready")
        await asyncio.sleep(.04)
        assert session.state == "timed_out" and session.secret is None
        await service.close()


@pytest.mark.asyncio
async def test_cancel_while_request_in_flight_never_finishes_binding():
    entered, release = asyncio.Event(), asyncio.Event()
    async def handler(request):
        entered.set()
        await release.wait()
        return httpx.Response(200,json={"retcode":0,"data":{"task_id":"task"}})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        service = Onboarding(client=client)
        session = await service.create("owner", "one")
        await entered.wait()
        await service.cancel(session.id,"owner")
        release.set()
        await asyncio.sleep(.001)
        assert session.state == "cancelled" and not session.key
        await service.close()


def test_encrypted_store_is_app_bound_private_and_has_no_plaintext(tmp_path, monkeypatch):
    monkeypatch.setenv("QQBOT_CREDENTIAL_KEY",base64.b64encode(os.urandom(32)).decode())
    directory=tmp_path / "private"
    reference=save_credential(directory,"app","private-client-secret")
    path=directory / (reference + ".json")
    assert path.stat().st_mode & 0o777 == 0o600
    assert directory.stat().st_mode & 0o777 == 0o700
    assert "private-client-secret" not in path.read_text()
    assert load_credential(directory,reference,"app") == "private-client-secret"
    with pytest.raises(ValueError): load_credential(directory,reference,"other")
    with pytest.raises(ValueError): load_credential(directory,"../escape","app")
    monkeypatch.delenv("QQBOT_CREDENTIAL_KEY")
    with pytest.raises(ValueError): load_credential(directory,reference,"app")


@pytest.mark.asyncio
async def test_admin_routes_two_phase_apply_and_auth(tmp_path, monkeypatch):
    monkeypatch.setenv("QQBOT_CREDENTIAL_KEY",base64.b64encode(os.urandom(32)).decode())
    settings=Settings(database=str(tmp_path / "core.db"))
    core=SimpleNamespace(config_store=ConfigurationStore(settings),permissions=SimpleNamespace(require=lambda *a:None))
    def auth(request:Request):
        if request.headers.get("Authorization") != "Bearer admin": raise HTTPException(401)
    app=FastAPI()
    async with protocol() as official:
        service=install_qq_onboarding(app,core,auth,client=official)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url="http://test") as client:
            route="/api/v1/qq/onboarding/sessions"
            assert (await client.post(route,json={"instance_id":"qq"})).status_code==401
            headers={"Authorization":"Bearer admin"}
            response=await client.post(route,headers=headers,json={"instance_id":"qq"})
            session=service.sessions[response.json()["session_id"]]
            await await_state(session,"ready")
            assert not core.config_store.read().adapters
            saved=await client.post(route+"/"+session.id+"/apply",headers=headers,
                json={"expected_version":version(settings),"expected_app_id":session.app_id})
            assert saved.status_code==200,saved.text
            assert "private-client-secret" not in saved.text
            cfg=core.config_store.read().adapters[0]
            assert cfg.secret()=="private-client-secret"
            assert cfg.admin_users==() and cfg.allowed_users==()
            assert cfg.enabled is False
            again=await client.post(route+"/"+session.id+"/apply",headers=headers,
                json={"expected_version":version(settings),"expected_app_id":session.app_id})
            assert again.status_code==409
        await service.close()


@pytest.mark.asyncio
async def test_missing_key_prevents_external_request(tmp_path, monkeypatch):
    monkeypatch.delenv("QQBOT_CREDENTIAL_KEY",raising=False)
    settings=Settings(database=str(tmp_path / "core.db"))
    core=SimpleNamespace(config_store=ConfigurationStore(settings),permissions=SimpleNamespace(require=lambda *a:None))
    def handler(request):
        pytest.fail("no external request allowed without encryption key")
    app=FastAPI()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as official:
        service=install_qq_onboarding(app,core,lambda:None,client=official)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url="http://test") as client:
            result=await client.post("/api/v1/qq/onboarding/sessions",json={"instance_id":"qq"})
            assert result.status_code==503
        await service.close()


@pytest.mark.asyncio
async def test_cancellation_during_validation_prevents_persistence(tmp_path, monkeypatch):
    monkeypatch.setenv("QQBOT_CREDENTIAL_KEY",base64.b64encode(os.urandom(32)).decode())
    started,release=asyncio.Event(),asyncio.Event()
    async def handler(request):
        started.set()
        await release.wait()
        return httpx.Response(200,json={"access_token":"token"})
    settings=Settings(database=str(tmp_path / "core.db"))
    core=SimpleNamespace(config_store=ConfigurationStore(settings),permissions=SimpleNamespace(require=lambda *a:None))
    app=FastAPI()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as official:
        service=install_qq_onboarding(app,core,lambda:None,client=official)
        import hashlib

        from qqbot.adapters.qq_official.onboarding import Session
        session=Session("id",hashlib.sha256(b"").hexdigest(),"qq",service.now(),service.now()+300,
                        state="ready",app_id="123",secret="secret")
        service.sessions[session.id]=session
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url="http://test") as client:
            route="/api/v1/qq/onboarding/sessions/id"
            apply=asyncio.create_task(client.post(route+"/apply",json={
                "expected_version":version(settings),"expected_app_id":"123"}))
            await started.wait()
            assert (await client.delete(route)).status_code==200
            release.set()
            assert (await apply).status_code==409
            assert not core.config_store.read().adapters
            assert not (tmp_path / "qq-credentials").exists()
        await service.close()


def test_credential_file_symlinks_and_public_permissions_rejected(tmp_path,monkeypatch):
    monkeypatch.setenv("QQBOT_CREDENTIAL_KEY",base64.b64encode(os.urandom(32)).decode())
    directory=tmp_path/"store"
    reference=save_credential(directory,"app","secret")
    path=directory/(reference+".json")
    path.chmod(0o644)
    with pytest.raises(ValueError): load_credential(directory,reference,"app")
    path.chmod(0o600)
    alias="a"*32
    (directory/(alias+".json")).symlink_to(path)
    with pytest.raises(ValueError): load_credential(directory,alias,"app")


@pytest.mark.asyncio
async def test_qr_is_authenticated_and_local_svg(tmp_path,monkeypatch):
    monkeypatch.setenv("QQBOT_CREDENTIAL_KEY",base64.b64encode(os.urandom(32)).decode())
    settings=Settings(database=str(tmp_path/"core.db"))
    core=SimpleNamespace(config_store=ConfigurationStore(settings),permissions=SimpleNamespace(require=lambda *a:None))
    def auth(request:Request):
        if request.headers.get("Authorization")!="Bearer admin": raise HTTPException(401)
    app=FastAPI()
    async with protocol(status=1) as official:
        service=install_qq_onboarding(app,core,auth,client=official)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url="http://test") as client:
            headers={"Authorization":"Bearer admin"}
            response=await client.post("/api/v1/qq/onboarding/sessions",headers=headers,json={"instance_id":"qq"})
            session=service.sessions[response.json()["session_id"]]
            await await_state(session,"waiting")
            route="/api/v1/qq/onboarding/sessions/"+session.id+"/qr"
            assert (await client.get(route)).status_code==401
            svg=await client.get(route,headers=headers)
            assert svg.status_code==200 and "image/svg+xml" in svg.headers["Content-Type"]
            assert "<svg" in svg.text and "<script" not in svg.text
            assert base64.b64encode(session.key).decode() not in svg.text
            await client.delete(route.removesuffix("/qr"),headers=headers)
            assert (await client.get(route,headers=headers)).status_code==409
        await service.close()


@pytest.mark.asyncio
async def test_stale_config_no_write_and_permission_deny(tmp_path,monkeypatch):
    monkeypatch.setenv("QQBOT_CREDENTIAL_KEY",base64.b64encode(os.urandom(32)).decode())
    settings=Settings(database=str(tmp_path/"core.db"))
    core=SimpleNamespace(config_store=ConfigurationStore(settings),permissions=SimpleNamespace(require=lambda *a:None))
    app=FastAPI()
    async with protocol() as official:
        service=install_qq_onboarding(app,core,lambda:None,client=official)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url="http://test") as client:
            route="/api/v1/qq/onboarding/sessions"
            response=await client.post(route,json={"instance_id":"qq"})
            session=service.sessions[response.json()["session_id"]]
            await await_state(session,"ready")
            result=await client.post(route+"/"+session.id+"/apply",json={
                "expected_version":"stale","expected_app_id":session.app_id})
            assert result.status_code==409
            assert not (tmp_path/"qq-credentials").exists()
            assert session.state=="ready"
            def deny(*args): raise HTTPException(403)
            core.permissions.require=deny
            assert (await client.get(route+"/"+session.id)).status_code==403
        await service.close()


@pytest.mark.asyncio
async def test_portal_create_and_poll_request_json_headers():
    calls = []
    def handler(request):
        calls.append(request.url.path)
        assert request.headers["accept"] == "application/json"
        assert request.headers["content-type"] == "application/json"
        assert "ElephantBot" in request.headers["user-agent"]
        data = {"task_id": "header-test"} if request.url.path.endswith("create_bind_task") else {"status": 3}
        return httpx.Response(200, json={"retcode": 0, "data": data})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        service = Onboarding(client=client)
        session = await service.create("owner", "headers")
        await await_state(session, "expired")
        assert calls == ["/lite/create_bind_task", "/lite/poll_bind_result"]
        await service.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled,running,expected", [(True, False, 409), (False, True, 409), (False, False, 200)])
async def test_rotation_requires_stopped_disabled_same_identity(tmp_path, monkeypatch, enabled, running, expected):
    from qqbot.config import AdapterConfig
    monkeypatch.setenv("QQBOT_CREDENTIAL_KEY", base64.b64encode(os.urandom(32)).decode())
    cfg = AdapterConfig(instance_id="qq", kind="official", bot_id="1231", app_id="1231",
                        secret_env="OLD_QQ_SECRET", enabled=enabled, streaming_enabled=True)
    settings = Settings(database=str(tmp_path / "core.db"), adapters=(cfg,))
    core = SimpleNamespace(config_store=ConfigurationStore(settings),
        permissions=SimpleNamespace(require=lambda *a: None), adapters={"qq": object()} if running else {})
    app = FastAPI()
    async with protocol() as official:
        service = install_qq_onboarding(app, core, lambda: None, client=official)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            route = "/api/v1/qq/onboarding/sessions"
            assert (await client.post(route, json={"instance_id": "qq"})).status_code == 409
            result = await client.post(route, json={"instance_id": "qq", "replace_app_id": "1231"})
            assert result.status_code == expected
            if expected == 200:
                session = service.sessions[result.json()["session_id"]]
                await await_state(session, "ready")
                saved = await client.post(route + "/" + session.id + "/apply", json={
                    "expected_version": version(settings), "expected_app_id": session.app_id})
                assert saved.status_code == 200, saved.text
                replacement = core.config_store.read().adapters[0]
                assert replacement.secret() == "private-client-secret"
                assert replacement.streaming_enabled and not replacement.enabled
                assert replacement.bot_id == "1231" and replacement.secret_env == ""
                assert "private-client-secret" not in saved.text
        await service.close()


@pytest.mark.asyncio
async def test_rotation_rejects_different_scanned_app(tmp_path, monkeypatch):
    from qqbot.config import AdapterConfig
    monkeypatch.setenv("QQBOT_CREDENTIAL_KEY", base64.b64encode(os.urandom(32)).decode())
    cfg = AdapterConfig(instance_id="qq", kind="official", bot_id="old", app_id="old",
                        secret_env="OLD_QQ_SECRET", enabled=False)
    settings = Settings(database=str(tmp_path / "core.db"), adapters=(cfg,))
    core = SimpleNamespace(config_store=ConfigurationStore(settings), permissions=SimpleNamespace(require=lambda *a: None))
    app = FastAPI()
    async with protocol() as official:
        service = install_qq_onboarding(app, core, lambda: None, client=official)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            route = "/api/v1/qq/onboarding/sessions"
            result = await client.post(route, json={"instance_id": "qq", "replace_app_id": "old"})
            session = service.sessions[result.json()["session_id"]]
            await await_state(session, "ready")
            saved = await client.post(route + "/" + session.id + "/apply", json={
                "expected_version": version(settings), "expected_app_id": session.app_id})
            assert saved.status_code == 409
            assert core.config_store.read().adapters[0].app_id == "old"
            assert not (tmp_path / "qq-credentials").exists()
        await service.close()
