"""Official QQ callbacks: validate signed events before using this module.

Opaque token lookup authorizes only a finite, scoped workflow action. Never parse
button_data as a shell command, LLM instruction, task approval, or role grant.
"""
import asyncio
import hashlib
import json
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal
from urllib.parse import quote

import httpx
from sqlalchemy import insert, select, update

from qqbot.domain import ConversationRef, PrincipalRef, Surface, stable_id
from qqbot.storage.interaction_schema import qq_interaction_events, qq_interaction_tokens

Action = Literal["status", "help", "choice", "page", "cancel_prompt", "cancel_task", "dismiss"]
ALLOWED_ACTIONS = frozenset({"status", "help", "choice", "page", "cancel_prompt", "cancel_task", "dismiss"})


@dataclass(frozen=True)
class QQInteraction:
    id: str
    type: int
    conversation: ConversationRef
    principal: PrincipalRef
    occurred_at: datetime
    button_id: str
    button_data: str

    @property
    def key(self):
        return stable_id(self.conversation.adapter_instance_id, self.conversation.bot_identity_id,
                         self.principal.identity_namespace, self.id)

    @property
    def needs_ack(self):
        return self.type in {11, 12}


@dataclass(frozen=True)
class InteractionDecision:
    event_key: str
    code: int
    action: Action | None = None
    duplicate: bool = False
    payload: dict | None = None


def _identifier(value, name):
    if not isinstance(value, str) or not value or len(value) > 256:
        raise ValueError("invalid interaction " + name)
    return value


def parse_interaction(payload, *, deployment_id, instance_id, app_id, bot_id):
    """Parse a verified INTERACTION_CREATE; require the receiving application's ID."""
    if payload.get("t") != "INTERACTION_CREATE" or payload.get("op") != 0:
        raise ValueError("not an interaction event")
    data = payload.get("d")
    if not isinstance(data, dict) or str(data.get("application_id", "")) != str(app_id):
        raise ValueError("interaction application mismatch")
    kind = data.get("type")
    if not isinstance(kind, int) or isinstance(kind, bool):
        raise ValueError("invalid interaction type")  # noqa: TRY004 - callback validation boundary
    scene = data.get("scene")
    inner = data.get("data") or {}
    if not isinstance(inner, dict) or inner.get("type", kind) != kind:
        raise ValueError("interaction type mismatch")
    resolved = inner.get("resolved") or {}
    if not isinstance(resolved, dict):
        raise ValueError("invalid resolved interaction")  # noqa: TRY004 - callback validation boundary
    if scene == "c2c":
        surface, user = Surface.PRIVATE, data.get("user_openid")
        remote = user
    elif scene == "group":
        surface, user = Surface.GROUP, data.get("group_member_openid")
        remote = data.get("group_openid")
    elif scene == "guild":
        surface, user = Surface.GUILD_CHANNEL, resolved.get("user_id")
        remote = data.get("channel_id")
    else:
        raise ValueError("unsupported interaction scene")
    user = _identifier(user, "user")
    remote = _identifier(remote, "conversation")
    stamp = datetime.fromisoformat(data["timestamp"])
    if stamp.tzinfo is None:
        raise ValueError("timezone required")
    button_data = resolved.get("button_data", "")
    button_id = resolved.get("button_id", "")
    if (not isinstance(button_data, str) or len(button_data) > 2048
            or not isinstance(button_id, str) or len(button_id) > 128):
        raise ValueError("invalid button payload")
    return QQInteraction(
        id=_identifier(data.get("id"), "id"), type=kind,
        conversation=ConversationRef(deployment_id=deployment_id, adapter_instance_id=instance_id,
                                     bot_identity_id=bot_id, surface=surface,
                                     remote_conversation_id=remote),
        principal=PrincipalRef(adapter_instance_id=instance_id, identity_namespace="qq_app:" + str(app_id),
                               remote_user_id=user),
        occurred_at=stamp.astimezone(UTC), button_id=button_id, button_data=button_data,
    )


class InteractionStore:
    """Database transaction serializes token consumption and group exclusion.

    Accepted actions persist as PENDING. Caller claims before dispatch; crashes
    after claim deliberately remain RUNNING rather than replaying effects.
    """
    def __init__(self, db, clock):
        self.db, self.clock = db, clock

    def issue(self, target, principal, action: Action, *, ttl_seconds=300, group_key=None, payload=None):
        from qqbot.domain.interactions import WorkflowButton
        payload = payload or {}
        if action not in {"status", "help"}:
            WorkflowButton.model_validate_json(json.dumps({"label": "action", "action": action, "payload": payload}))
        elif payload:
            raise ValueError("read-only status/help takes no payload")
        if action not in ALLOWED_ACTIONS or not 1 <= ttl_seconds <= 3600:
            raise ValueError("invalid action or token lifetime")
        if target.adapter_instance_id != principal.adapter_instance_id:
            raise ValueError("button identity mismatch")
        if group_key is not None and (not isinstance(group_key, str) or not 1 <= len(group_key) <= 128):
            raise ValueError("invalid mutually exclusive group")
        token = "qbi_" + secrets.token_urlsafe(24)
        with self.db.transaction() as c:
            c.execute(insert(qq_interaction_tokens).values(
                token_hash=hashlib.sha256(token.encode()).hexdigest(), conversation_key=target.key,
                principal_key=principal.key, action=action, payload_json=json.dumps(payload),
                group_key=stable_id(target.key, principal.key, group_key) if group_key else None,
                expires_at=(self.clock.now() + timedelta(seconds=ttl_seconds)).isoformat(),
            ))
        return token

    def consume(self, event: QQInteraction):
        now = self.clock.now()
        with self.db.transaction() as c:
            old = c.execute(select(qq_interaction_events).where(
                qq_interaction_events.c.event_key == event.key)).mappings().first()
            if old:
                if old["conversation_key"] != event.conversation.key or old["principal_key"] != event.principal.key:
                    return InteractionDecision(event.key, 4, duplicate=True)
                # An ACK retry must not renew the original short-lived capability.
                original = c.execute(select(qq_interaction_tokens).where(
                    qq_interaction_tokens.c.token_hash == hashlib.sha256(event.button_data.encode()).hexdigest(),
                    qq_interaction_tokens.c.consumed_by == event.key,
                )).mappings().first()
                live = original and datetime.fromisoformat(original["expires_at"]) > now
                if old["state"] == "PENDING" and (not live or (now - event.occurred_at).total_seconds() > 300):
                    c.execute(update(qq_interaction_events).where(qq_interaction_events.c.event_key == event.key)
                              .values(state="FAILED"))
                    return InteractionDecision(event.key, 3, duplicate=True)
                return InteractionDecision(event.key, old["ack_code"], action=old["action"], duplicate=True,
                                           payload=json.loads(old["details"] or "{}"))
            token = None
            code, action, payload = 4, None, {}
            # Delayed/forwarded events cannot refresh expired capabilities.
            if event.type == 11 and -30 <= (now - event.occurred_at).total_seconds() <= 300:
                token = c.execute(select(qq_interaction_tokens).where(
                    qq_interaction_tokens.c.token_hash == hashlib.sha256(event.button_data.encode()).hexdigest()
                )).mappings().first()
            if token and token["conversation_key"] == event.conversation.key and token["principal_key"] == event.principal.key:
                if token["consumed_by"]:
                    code = 3
                elif datetime.fromisoformat(token["expires_at"]) > now and token["action"] in ALLOWED_ACTIONS:
                    code, action = 0, token["action"]
                    payload = json.loads(token["payload_json"])
                    criterion = qq_interaction_tokens.c.token_hash == token["token_hash"]
                    if token["group_key"]:
                        criterion = qq_interaction_tokens.c.group_key == token["group_key"]
                    c.execute(update(qq_interaction_tokens).where(criterion).values(consumed_by=event.key))
            c.execute(insert(qq_interaction_events).values(
                event_key=event.key, interaction_id=event.id, conversation_key=event.conversation.key,
                principal_key=event.principal.key, action=action, ack_code=code,
                state="PENDING" if action else "REJECTED", created_at=now.isoformat(),
                details=json.dumps(payload),
            ))
            return InteractionDecision(event.key, code, action, payload=payload)

    def claim(self, event_key):
        with self.db.transaction() as c:
            result = c.execute(update(qq_interaction_events).where(
                qq_interaction_events.c.event_key == event_key, qq_interaction_events.c.state == "PENDING"
            ).values(state="RUNNING"))
            return result.rowcount == 1

    def finish(self, event_key, *, failed=False):
        with self.db.transaction() as c:
            c.execute(update(qq_interaction_events).where(
                qq_interaction_events.c.event_key == event_key, qq_interaction_events.c.state == "RUNNING"
            ).values(state="FAILED" if failed else "DONE"))

    def revoke(self, token):
        with self.db.transaction() as c:
            c.execute(update(qq_interaction_tokens).where(
                qq_interaction_tokens.c.token_hash == hashlib.sha256(token.encode()).hexdigest(),
                qq_interaction_tokens.c.consumed_by.is_(None),
            ).values(consumed_by="revoked"))


async def acknowledge_interaction(adapter, event: QQInteraction, code: int):
    """One bounded ACK attempt; callers must not retry uncertain outcomes."""
    if not event.needs_ack:
        return "NOT_REQUIRED"
    if code not in range(6):
        raise ValueError("invalid ACK code")
    try:
        async with asyncio.timeout(2.5):
            token = await adapter.access_token()
            response = await adapter.client.put(
                adapter.config.api_base.rstrip("/") + "/interactions/" + quote(event.id, safe=""),
                headers={"Authorization": "QQBot " + token, "X-Union-Appid": str(adapter.config.app_id)},
                json={"code": code}, timeout=2,
            )
            return "ACKED" if response.is_success else "REJECTED"
    except (httpx.HTTPError, ValueError, KeyError, TimeoutError):
        return "UNKNOWN"


def action_keyboard(store, target, principal, *, actions=("status", "help"), ttl_seconds=300, group_key=None):
    """Issue safe application-owned buttons. The caller still sends through Outbox."""
    from qqbot.domain.interactions import (
        QQButton,
        QQButtonAction,
        QQButtonPermission,
        QQButtonRender,
        QQButtonRow,
        QQKeyboard,
        QQKeyboardContent,
    )
    if not actions or len(actions) > 2 or len(set(actions)) != len(actions) or any(
        action not in {"help", "status"} for action in actions
    ):
        raise ValueError("only unique help/status buttons are supported")
    labels = {"status": "查看状态", "help": "使用帮助"}
    buttons = []
    for action in actions:
        token = store.issue(target, principal, action, ttl_seconds=ttl_seconds, group_key=group_key)
        buttons.append(QQButton(
            id=action, render_data=QQButtonRender(label=labels[action]),
            action=QQButtonAction(type=1, data=token, permission=QQButtonPermission(
                type=0, specify_user_ids=(principal.remote_user_id,))),
            group_id=group_key,
        ))
    return QQKeyboard(content=QQKeyboardContent(rows=(QQButtonRow(buttons=tuple(buttons)),)))


def router(adapters, store, authenticate, permissions):
    """Authenticated backend integration point; issuing does not send a QQ message."""
    from fastapi import APIRouter, Depends, HTTPException, Request
    from pydantic import BaseModel, ConfigDict, Field, field_validator

    from qqbot.permissions import PermissionSubject

    class ButtonRequest(BaseModel):
        model_config = ConfigDict(extra="forbid")
        target: ConversationRef
        principal: PrincipalRef
        actions: list[Literal["help", "status"]] = Field(default_factory=lambda: ["status", "help"],
                                                       min_length=1, max_length=2)
        workflow_buttons: list[dict] | None = Field(default=None, min_length=1, max_length=5)
        ttl_seconds: int = Field(default=300, ge=1, le=3600)
        group_key: str | None = Field(default=None, min_length=1, max_length=128)

        @field_validator("target", mode="before")
        @classmethod
        def parse_target(cls, value):
            import json
            return value if isinstance(value, ConversationRef) else ConversationRef.model_validate_json(json.dumps(value))

    subject = PermissionSubject(adapter_instance_id="admin_api", identity_namespace="admin_api", user_id="owner")

    def authorize(request: Request):
        if not permissions.check(subject, "admin.post", "http:" + request.url.path).allowed:
            raise HTTPException(403, "permission denied")

    api = APIRouter(dependencies=[Depends(authenticate), Depends(authorize)])

    @api.post("/api/v1/qq/{instance_id}/buttons")
    def issue(instance_id: str, body: ButtonRequest):
        adapter = adapters.get(instance_id)
        if adapter is None or adapter.config.kind != "official":
            raise HTTPException(404, "official adapter not found")
        if (body.target.adapter_instance_id != instance_id or body.principal.adapter_instance_id != instance_id
                or body.target.deployment_id != adapter.deployment_id
                or body.target.bot_identity_id != adapter.config.bot_id
                or body.principal.identity_namespace != "qq_app:" + str(adapter.config.app_id)
                or (body.target.surface == Surface.PRIVATE
                    and body.target.remote_conversation_id != body.principal.remote_user_id)):
            raise HTTPException(422, "button identity mismatch")
        if not permissions.check(PermissionSubject.from_refs(body.target, body.principal),
                                 "notification.send", "conversation:" + body.target.key).allowed:
            raise HTTPException(403, "target denied")
        try:
            if body.workflow_buttons is not None:
                keyboard = workflow_keyboard(store, body.target, body.principal, body.workflow_buttons,
                                             ttl_seconds=body.ttl_seconds, group_key=body.group_key)
            else:
                keyboard = action_keyboard(store, body.target, body.principal, actions=tuple(body.actions),
                                           ttl_seconds=body.ttl_seconds, group_key=body.group_key)
        except ValueError as exc:
            raise HTTPException(422, "invalid button request") from exc
        return {"keyboard": keyboard.to_payload(), "expires_in": body.ttl_seconds}

    return api


def workflow_keyboard(store, target, principal, buttons, *, ttl_seconds=300, group_key=None, internal=False):
    """Issue bounded workflow buttons; direct cancellation is internal confirmation only."""
    from qqbot.domain.interactions import (
        QQButton,
        QQButtonAction,
        QQButtonPermission,
        QQButtonRender,
        QQButtonRow,
        QQKeyboard,
        QQKeyboardContent,
        WorkflowButton,
    )
    if not 1 <= len(buttons) <= 5:
        raise ValueError("workflow requires 1..5 buttons")
    parsed = [b if isinstance(b, WorkflowButton) else WorkflowButton.model_validate_json(json.dumps(b))
              for b in buttons]
    if not internal and any(b.action == "cancel_task" for b in parsed):
        raise ValueError("direct cancellation requires server confirmation")
    rendered = []
    for index, button in enumerate(parsed):
        token = store.issue(target, principal, button.action, payload=button.payload.model_dump(mode="json"),
                            ttl_seconds=ttl_seconds, group_key=group_key)
        rendered.append(QQButton(id="workflow_" + str(index), render_data=QQButtonRender(label=button.label),
            action=QQButtonAction(type=1, data=token, permission=QQButtonPermission(
                type=0, specify_user_ids=(principal.remote_user_id,))), group_id=group_key))
    return QQKeyboard(content=QQKeyboardContent(rows=(QQButtonRow(buttons=tuple(rendered)),)))
