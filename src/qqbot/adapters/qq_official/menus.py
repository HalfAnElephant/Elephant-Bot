"""Explicit administrator-driven QQ menus/panels; never synchronized at startup.

Schemas verified 2026-11-12 against bot.q.qq.com/wiki/develop/api-v2/autogen/api/
(v2_menu.put, v2_panels.post, v2_panels_panel_id.put and *_target.put).
QQ menu button text is a message from the user, not an authorization shortcut.
"""
from typing import Literal
from urllib.parse import quote, urlsplit

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field, model_validator

from qqbot.permissions import PermissionSubject

Scope = Literal["c2c", "group", "channel", "dm"]


class MenuDTO(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _display_length(text):
    # Conservative interpretation of official "Chinese characters count as two".
    return sum(1 if ord(char) < 128 else 2 for char in text)


def _https(url):
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("link must be an HTTPS URL without credentials")
    return url


class QQMenuSwitch(MenuDTO):
    switch_id: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_-]+$")
    default: bool = False


class QQSubMenuItem(MenuDTO):
    name: str = Field(min_length=1, max_length=14)
    type: Literal["send_message", "link"]
    send_message: str | None = Field(default=None, min_length=1, max_length=1024)
    link: str | None = Field(default=None, max_length=2048)

    @model_validator(mode="after")
    def valid_content(self):
        if _display_length(self.name) > 14:
            raise ValueError("submenu label exceeds 14 display characters")
        if self.type == "link":
            if not self.link or self.send_message is not None:
                raise ValueError("link item requires only link")
            _https(self.link)
        elif not self.send_message or self.link is not None:
            raise ValueError("send_message item requires only send_message")
        return self


class QQMenuItem(MenuDTO):
    name: str = Field(min_length=1, max_length=10)
    type: Literal["switch", "send_message", "link", "menu"]
    sub_menu_items: tuple[QQSubMenuItem, ...] = Field(default=(), max_length=5)
    send_message: str | None = Field(default=None, min_length=1, max_length=1024)
    link: str | None = Field(default=None, max_length=2048)
    switch: QQMenuSwitch | None = None

    @model_validator(mode="after")
    def valid_content(self):
        if _display_length(self.name) > 10:
            raise ValueError("menu label exceeds 10 display characters")
        fields = {"menu": bool(self.sub_menu_items), "send_message": self.send_message is not None,
                  "link": self.link is not None, "switch": self.switch is not None}
        if not fields[self.type] or sum(fields.values()) != 1:
            raise ValueError("menu content does not match type")
        if self.link:
            _https(self.link)
        return self


class QQMenu(MenuDTO):
    items: tuple[QQMenuItem, ...] = Field(default=(), max_length=10)


class QQMenuUpdate(MenuDTO):
    menu: QQMenu


class QQPanelItem(MenuDTO):
    name: str = Field(min_length=1, max_length=14)
    desc: str | None = Field(default=None, max_length=30)
    type: Literal["command", "link"]
    only_admin: bool = False
    link: str | None = Field(default=None, max_length=2048)

    @model_validator(mode="after")
    def valid_content(self):
        if _display_length(self.name) > 14 or _display_length(self.desc or "") > 30:
            raise ValueError("panel label/description exceeds display character limit")
        if self.type == "link":
            if not self.link:
                raise ValueError("panel link required")
            _https(self.link)
        elif self.link is not None:
            raise ValueError("command item cannot contain a link")
        return self


class QQPanel(MenuDTO):
    items: tuple[QQPanelItem, ...] = Field(default=(), max_length=20)
    remark: str | None = Field(default=None, max_length=255)
    version: int | None = Field(default=None, ge=0)


class QQPanelCreate(MenuDTO):
    scope: Scope
    target_type: Literal["all", "specific"] = "all"
    user_openids: tuple[str, ...] = Field(default=(), max_length=20)
    group_openids: tuple[str, ...] = Field(default=(), max_length=20)
    panel: QQPanel

    @model_validator(mode="after")
    def target_matches_scope(self):
        ids = self.user_openids + self.group_openids
        if any(not value or len(value) > 256 for value in ids):
            raise ValueError("invalid target openid")
        if len(set(ids)) != len(ids):
            raise ValueError("duplicate target openid")
        if self.scope in {"channel", "dm"} and self.target_type != "all":
            raise ValueError("channel/dm support global panels only")
        if self.target_type == "all" and ids:
            raise ValueError("global panel cannot specify targets")
        if self.target_type == "specific":
            valid = self.user_openids if self.scope == "c2c" else self.group_openids
            wrong = self.group_openids if self.scope == "c2c" else self.user_openids
            if not valid or wrong:
                raise ValueError("specific panel requires targets of the matching scope")
        return self


class QQPanelUpdate(MenuDTO):
    panel: QQPanel


class QQPanelTargets(MenuDTO):
    op: Literal["add", "del"]
    user_openids: tuple[str, ...] = Field(default=(), max_length=20)
    group_openids: tuple[str, ...] = Field(default=(), max_length=20)

    @model_validator(mode="after")
    def one_target_kind(self):
        ids = self.user_openids + self.group_openids
        if bool(self.user_openids) == bool(self.group_openids):
            raise ValueError("exactly one non-empty target list is required")
        if any(not value or len(value) > 256 for value in ids) or len(set(ids)) != len(ids):
            raise ValueError("invalid or duplicate target openid")
        return self


class QQMenuAPIError(Exception):
    """Sanitized errors; raw upstream bodies and authorization headers are not logged."""
    def __init__(self, state, status_code=None):
        self.state, self.status_code = state, status_code
        super().__init__(state)


class MenuClient:
    def __init__(self, adapter):
        self.adapter = adapter

    async def _request(self, method, path, *, body=None, params=None):
        try:
            token = await self.adapter.access_token()
        except (httpx.HTTPError, ValueError, KeyError) as exc:
            raise QQMenuAPIError("TOKEN_UNAVAILABLE") from exc
        try:
            response = await self.adapter.client.request(
                method, self.adapter.config.api_base.rstrip("/") + path,
                headers={"Authorization": "QQBot " + token,
                         "X-Union-Appid": str(self.adapter.config.app_id)},
                json=body.model_dump(mode="json", exclude_none=True) if body is not None else None,
                params=params,
            )
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout) as exc:
            raise QQMenuAPIError("NOT_CONNECTED") from exc
        except httpx.HTTPError as exc:
            # Writes may already have taken effect. Caller must reconcile before retry.
            raise QQMenuAPIError("OUTCOME_UNKNOWN") from exc
        if response.status_code == 401:
            self.adapter.token = None
        if not response.is_success:
            raise QQMenuAPIError("RATE_LIMITED" if response.status_code == 429 else "REJECTED",
                                 response.status_code)
        try:
            data = response.json() if response.content else {}
        except ValueError as exc:
            raise QQMenuAPIError("OUTCOME_UNKNOWN") from exc
        if not isinstance(data, dict):
            raise QQMenuAPIError("OUTCOME_UNKNOWN")
        return data

    @staticmethod
    def _id(panel_id):
        if not isinstance(panel_id, str) or not panel_id or len(panel_id) > 256:
            raise ValueError("invalid panel id")
        return quote(panel_id, safe="")

    async def get_menu(self):
        return await self._request("GET", "/v2/menu")

    async def update_menu(self, body: QQMenuUpdate):
        return await self._request("PUT", "/v2/menu", body=body)

    async def list_panels(self, scope: Scope, *, cursor="", limit=20):
        if scope not in {"c2c", "group", "channel", "dm"} or not 1 <= limit <= 50:
            raise ValueError("invalid panel list arguments")
        return await self._request("GET", "/v2/panels", params={"scope": scope, "cursor": cursor, "limit": limit})

    async def create_panel(self, body: QQPanelCreate):
        return await self._request("POST", "/v2/panels", body=body)

    async def get_panel(self, panel_id):
        return await self._request("GET", "/v2/panels/" + self._id(panel_id))

    async def update_panel(self, panel_id, body: QQPanelUpdate):
        return await self._request("PUT", "/v2/panels/" + self._id(panel_id), body=body)

    async def delete_panel(self, panel_id):
        return await self._request("DELETE", "/v2/panels/" + self._id(panel_id))

    async def update_targets(self, panel_id, body: QQPanelTargets):
        # Query scope first rather than trusting a client-supplied scope assertion.
        existing = await self.get_panel(panel_id)
        if existing.get("target_type") != "specific" or existing.get("scope") not in {"c2c", "group"}:
            raise ValueError("panel is not a targeted C2C/group panel")
        if (existing["scope"] == "c2c") != bool(body.user_openids):
            raise ValueError("target kind does not match remote panel scope")
        return await self._request("PUT", "/v2/panels/" + self._id(panel_id) + "/target", body=body)


def router(adapters, authenticate, permissions):
    """Mount once from app.py; authentication + existing admin permissions are mandatory."""
    admin_subject = PermissionSubject(adapter_instance_id="admin_api", identity_namespace="admin_api", user_id="owner")

    def authorize(request: Request):
        if not permissions.check(admin_subject, "admin." + request.method.lower(), "http:" + request.url.path).allowed:
            raise HTTPException(403, "permission denied")

    api = APIRouter(prefix="/api/v1/qq/{instance_id}", dependencies=[Depends(authenticate), Depends(authorize)])

    def client(instance_id):
        adapter = adapters.get(instance_id)
        if adapter is None or adapter.config.kind != "official":
            raise HTTPException(404, "official adapter not found")
        return MenuClient(adapter)

    async def call(awaitable):
        try:
            return await awaitable
        except ValueError as exc:
            raise HTTPException(422, "invalid menu or panel target") from exc
        except QQMenuAPIError as exc:
            raise HTTPException(429 if exc.state == "RATE_LIMITED" else 502, detail={
                "state": exc.state, "upstream_status": exc.status_code,
                "retry": "reconcile before retry" if exc.state == "OUTCOME_UNKNOWN" else "manual",
            }) from exc

    @api.get("/menu")
    async def get_menu(instance_id: str):
        return await call(client(instance_id).get_menu())

    @api.put("/menu")
    async def update_menu(instance_id: str, body: QQMenuUpdate):
        return await call(client(instance_id).update_menu(body))

    @api.get("/panels")
    async def list_panels(instance_id: str, scope: Scope, cursor: str = Query(default="", max_length=1024),
                          limit: int = Query(default=20, ge=1, le=50)):
        return await call(client(instance_id).list_panels(scope, cursor=cursor, limit=limit))

    @api.post("/panels")
    async def create_panel(instance_id: str, body: QQPanelCreate):
        return await call(client(instance_id).create_panel(body))

    @api.get("/panels/{panel_id}")
    async def get_panel(instance_id: str, panel_id: str):
        return await call(client(instance_id).get_panel(panel_id))

    @api.put("/panels/{panel_id}")
    async def update_panel(instance_id: str, panel_id: str, body: QQPanelUpdate):
        return await call(client(instance_id).update_panel(panel_id, body))

    @api.delete("/panels/{panel_id}")
    async def delete_panel(instance_id: str, panel_id: str):
        return await call(client(instance_id).delete_panel(panel_id))

    @api.put("/panels/{panel_id}/target")
    async def update_targets(instance_id: str, panel_id: str, body: QQPanelTargets):
        return await call(client(instance_id).update_targets(panel_id, body))

    return api
