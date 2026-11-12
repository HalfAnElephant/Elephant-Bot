import json
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient
from pydantic import ValidationError

from qqbot.adapters.qq_official.menus import (
    MenuClient,
    QQMenu,
    QQMenuAPIError,
    QQMenuItem,
    QQMenuUpdate,
    QQPanel,
    QQPanelCreate,
    QQPanelItem,
    QQPanelTargets,
    QQPanelUpdate,
    router,
)
from qqbot.permissions import PermissionService


def adapter(handler):
    async def token():
        return "test-secret-token"
    return SimpleNamespace(access_token=token, token="test-secret-token",
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        config=SimpleNamespace(api_base="https://api.bot.qq.com", app_id="app", kind="official"))


def test_menu_and_panel_schema_validation():
    menu = QQMenuUpdate(menu=QQMenu(items=(QQMenuItem(name="帮助", type="send_message", send_message="/help"),)))
    assert menu.model_dump(mode="json")["menu"]["items"][0]["send_message"] == "/help"
    with pytest.raises(ValidationError):
        QQMenuItem(name="六个汉字过长", type="link", link="https://example.com")
    with pytest.raises(ValidationError):
        QQMenuItem(name="link", type="link", link="javascript:alert(1)")
    with pytest.raises(ValidationError):
        QQMenuItem(name="menu", type="menu", send_message="wrong")
    with pytest.raises(ValidationError):
        QQPanelCreate(scope="channel", target_type="specific", group_openids=("group",), panel=QQPanel())
    with pytest.raises(ValidationError):
        QQPanelCreate(scope="group", target_type="all", group_openids=("group",), panel=QQPanel())
    with pytest.raises(ValidationError):
        QQPanelCreate(scope="c2c", target_type="specific", group_openids=("group",), panel=QQPanel())
    with pytest.raises(ValidationError):
        QQPanelTargets(op="add", user_openids=("user",), group_openids=("group",))
    with pytest.raises(ValidationError):
        QQPanelTargets(op="add", user_openids=("user", "user"))
    with pytest.raises(ValidationError):
        QQPanelItem(name="help", type="command", link="https://example.com")


@pytest.mark.asyncio
async def test_menu_panel_transport_uses_documented_endpoints():
    requests = []
    def respond(request):
        requests.append(request)
        return httpx.Response(200, json={"scope": "group", "target_type": "specific", "panel_id": "p"})
    bot = adapter(respond)
    client = MenuClient(bot)
    await client.get_menu()
    await client.update_menu(QQMenuUpdate(menu=QQMenu()))
    await client.list_panels("c2c", cursor="cursor", limit=50)
    await client.create_panel(QQPanelCreate(scope="group", panel=QQPanel()))
    await client.get_panel("p/id")
    await client.update_panel("p", QQPanelUpdate(panel=QQPanel(items=(QQPanelItem(name="/help", type="command"),))))
    await client.delete_panel("p")
    await client.update_targets("p", QQPanelTargets(op="add", group_openids=("group",)))
    assert [(r.method, r.url.path) for r in requests] == [
        ("GET", "/v2/menu"), ("PUT", "/v2/menu"), ("GET", "/v2/panels"),
        ("POST", "/v2/panels"), ("GET", "/v2/panels/p/id"), ("PUT", "/v2/panels/p"),
        ("DELETE", "/v2/panels/p"), ("GET", "/v2/panels/p"), ("PUT", "/v2/panels/p/target")]
    assert requests[4].url.raw_path == b"/v2/panels/p%2Fid"
    assert requests[2].url.params["limit"] == "50"
    assert json.loads(requests[1].content) == {"menu": {"items": []}}
    assert json.loads(requests[-1].content) == {"op": "add", "user_openids": [], "group_openids": ["group"]}
    assert all(r.headers["Authorization"] == "QQBot test-secret-token" for r in requests)
    await bot.client.aclose()


@pytest.mark.asyncio
async def test_targets_reject_global_or_wrong_scope_before_write():
    requests = []
    def respond(request):
        requests.append(request)
        return httpx.Response(200, json={"scope": "group", "target_type": "all"})
    bot = adapter(respond)
    with pytest.raises(ValueError):
        await MenuClient(bot).update_targets("p", QQPanelTargets(op="add", group_openids=("g",)))
    assert [r.method for r in requests] == ["GET"]
    await bot.client.aclose()


@pytest.mark.asyncio
async def test_no_auto_retry_of_uncertain_create():
    requests = []
    def respond(request):
        requests.append(request)
        raise httpx.ReadTimeout("secret body", request=request)
    bot = adapter(respond)
    with pytest.raises(QQMenuAPIError, match="OUTCOME_UNKNOWN") as caught:
        await MenuClient(bot).create_panel(QQPanelCreate(scope="c2c", panel=QQPanel()))
    assert "secret" not in str(caught.value)
    assert len(requests) == 1
    await bot.client.aclose()


def test_menu_routes_are_authenticated_and_invalid_scope_never_calls_remote():
    requests = []
    def respond(request):
        requests.append(request)
        return httpx.Response(200, json={"version": 1})
    bot = adapter(respond)
    app = FastAPI()
    def authenticate(request: Request):
        if request.headers.get("authorization") != "Bearer test":
            raise HTTPException(401)
    app.include_router(router({"qq": bot}, authenticate, PermissionService()))
    headers = {"authorization": "Bearer test"}
    client = TestClient(app)
    assert client.get("/api/v1/qq/qq/menu").status_code == 401
    assert client.get("/api/v1/qq/qq/menu", headers=headers).json() == {"version": 1}
    assert client.get("/api/v1/qq/qq/panels?scope=unknown", headers=headers).status_code == 422
    assert len(requests) == 1
    assert client.put("/api/v1/qq/qq/menu", headers=headers, json={"menu": {"items": []}}).status_code == 200
    assert client.get("/api/v1/qq/missing/menu", headers=headers).status_code == 404


def test_permission_denial_prevents_remote_write():
    app = FastAPI()
    class Deny:
        def check(self, *args):
            return SimpleNamespace(allowed=False)
    bot = adapter(lambda request: pytest.fail("must not call remote"))
    app.include_router(router({"qq": bot}, lambda: None, Deny()))
    response = TestClient(app).put("/api/v1/qq/qq/menu", json={"menu": {"items": []}})
    assert response.status_code == 403
