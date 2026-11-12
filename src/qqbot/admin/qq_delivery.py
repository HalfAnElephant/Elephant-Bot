"""Admin-authenticated rich sends enter the durable outbox, never bypass it."""
import json
from datetime import timedelta
from typing import Literal
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import select

from qqbot.adapters.qq_official.adapter import OfficialAdapter
from qqbot.domain import ConversationRef, OutboundEnvelope, ReplyContext
from qqbot.domain.official import OfficialContent
from qqbot.permissions import PermissionSubject
from qqbot.storage.schema import conversations, inbox


class RichSendRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    conversation_id: str
    request_id: str = Field(min_length=1, max_length=128)
    text: str = Field(default="", max_length=10000)
    content_format: Literal["text", "markdown"] = "text"
    official_content: OfficialContent | None = None
    reply_to_event_id: str | None = None
    allow_proactive: bool = False

    @model_validator(mode="after")
    def nonempty(self):
        if not self.text.strip() and not (self.official_content and (
            self.official_content.media or self.official_content.template_markdown or self.official_content.card
        )):
            raise ValueError("empty message content")
        return self

    @field_validator("official_content", mode="before")
    @classmethod
    def parse_content(cls, value):
        return value if value is None or isinstance(value, OfficialContent) else OfficialContent.model_validate_json(json.dumps(value))


def router(core, authenticate):
    subject = PermissionSubject(adapter_instance_id="admin_api", identity_namespace="admin_api", user_id="owner")

    def authorize(request: Request):
        core.permissions.require(subject, "admin." + request.method.lower(), "http:" + request.url.path)

    api = APIRouter(dependencies=[Depends(authenticate), Depends(authorize)])

    @api.post("/api/v1/qq/{instance_id}/messages")
    async def enqueue(instance_id: str, body: RichSendRequest):
        adapter = core.adapters.get(instance_id)
        if not isinstance(adapter, OfficialAdapter):
            raise HTTPException(404, "official adapter not found")
        with core.db.engine.connect() as c:
            raw = c.execute(select(conversations.c.ref_json).where(conversations.c.id == body.conversation_id)).scalar()
            if raw is None:
                raise HTTPException(404, "known conversation required")
            target = ConversationRef.model_validate_json(raw)
            if target.adapter_instance_id != instance_id:
                raise HTTPException(422, "target adapter mismatch")
            reply = None
            if body.reply_to_event_id:
                event_json = c.execute(select(inbox.c.event_json).where(
                    inbox.c.id == body.reply_to_event_id, inbox.c.conversation_id == target.key)).scalar()
                if event_json is None:
                    raise HTTPException(422, "reply event does not belong to target")
                from qqbot.domain import InboundEvent
                event = InboundEvent.model_validate_json(event_json)
                reply = event.reply_context
                if reply:
                    reply = ReplyContext.model_validate_json(reply.model_dump_json()).model_copy(update={"msg_seq": 1})
        envelope = OutboundEnvelope(delivery_id=str(uuid4()), target=target, text=body.text,
            content_format=body.content_format, official_content=body.official_content,
            reply_context=reply, origin_kind="command", origin_id="admin-rich:" + body.request_id,
            expires_at=core.clock.now() + timedelta(minutes=5), allow_proactive=body.allow_proactive)
        check = await adapter.validate_delivery(envelope)
        if not check.allowed:
            raise HTTPException(409, check.reason)
        with core.db.transaction() as c:
            delivery_id = core.outbox.enqueue(c, envelope)
        return {"delivery_id": delivery_id, "state": "QUEUED", "delivered": False}

    return api
