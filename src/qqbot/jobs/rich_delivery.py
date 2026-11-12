"""Task-scoped QQ delivery. Worker input cannot select identity or audience."""
import json
from datetime import timedelta
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import func, insert, select

from qqbot.adapters.qq_official.adapter import OfficialAdapter
from qqbot.adapters.qq_official.local_limits import MAX_FILE_BYTES
from qqbot.domain import InboundEvent, OutboundEnvelope, stable_id
from qqbot.domain.interactions import WorkflowButton
from qqbot.domain.official import OfficialContent
from qqbot.storage.schema import outbox
from qqbot.storage.task_schema import notifications


class RichMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: str = Field(min_length=1, max_length=128)
    text: str = Field(default="", max_length=10000)
    content_format: Literal["text", "markdown"] = "text"
    official_content: OfficialContent | None = None

    @field_validator("official_content", mode="before")
    @classmethod
    def content(cls, value):
        return value if value is None or isinstance(value, OfficialContent) else OfficialContent.model_validate_json(json.dumps(value))

    @model_validator(mode="after")
    def nonempty(self):
        if not self.text.strip() and not (self.official_content and (
            self.official_content.media or self.official_content.card or self.official_content.template_markdown
        )):
            raise ValueError("empty message")
        if self.content_format == "text" and self.official_content is None:
            from qqbot.outbox.presentation import MESSAGE_LIMIT
            if len(self.text) > MESSAGE_LIMIT:
                raise ValueError("plain text exceeds atomic message limit; use Markdown or separate request IDs")
        return self


class RichStatus(BaseModel):
    model_config = ConfigDict(extra="forbid")
    delivery_id: str = Field(min_length=1, max_length=128)


def _authorize(service, args):
    service.authorize(*args, {"action": "notification.send"})
    with service.db.engine.connect() as c:
        _run, task = service._lease(c, *args)
    event = InboundEvent.model_validate_json(task["event_json"])
    service.permissions.require(service._access_for(task), "notification.send", "conversation:" + task["conversation"])
    return task, event


def _status(row):
    return {"delivery_id": row["id"], "state": row["state"], "delivered": row["state"] == "SENT",
            "receipt": json.loads(row["receipt_json"]) if row["receipt_json"] else None}


async def rich_send(service, args, payload):
    body = RichMessage.model_validate(payload)
    task, event = _authorize(service, args)
    adapter = getattr(service, "qq_adapters", {}).get(event.conversation.adapter_instance_id)
    if not isinstance(adapter, OfficialAdapter):
        raise ValueError("official QQ adapter unavailable for this conversation")  # noqa: TRY004
    notification_id = stable_id("qq-rich", task["id"], body.request_id)
    canonical = body.model_dump_json()
    with service.db.engine.connect() as c:
        existing = c.execute(select(notifications).where(notifications.c.id == notification_id)).mappings().first()
        if existing:
            if existing["body"] != canonical:
                raise ValueError("request_id already used with different content")
            row = c.execute(select(outbox).where(outbox.c.origin_id == notification_id)).mappings().one()
            return _status(row)
    envelope = OutboundEnvelope(delivery_id=str(uuid4()), target=event.conversation, text=body.text,
        content_format=body.content_format, official_content=body.official_content, plain_text=True,
        reply_context=event.reply_context, origin_kind="task", origin_id=notification_id,
        requester=event.principal, parent_task_id=task["id"], purpose="status",
        expires_at=service.clock.now() + timedelta(minutes=5))
    check = await adapter.validate_delivery(envelope.model_copy(update={"reply_context":
        event.reply_context.model_copy(update={"msg_seq": 1}) if event.reply_context else None}))
    if not check.allowed:
        raise ValueError(check.reason)
    # Adapter validation may await; renew the authority check before committing the send intent.
    _authorize(service, args)
    with service.db.transaction() as c:
        existing = c.execute(select(notifications).where(notifications.c.id == notification_id)).mappings().first()
        if existing:
            if existing["body"] != canonical:
                raise ValueError("request_id already used with different content")
            return _status(c.execute(select(outbox).where(outbox.c.origin_id == notification_id)).mappings().one())
        sequence = (c.execute(select(func.max(notifications.c.sequence)).where(
            notifications.c.task_id == task["id"])).scalar_one() or 0) + 1
        c.execute(insert(notifications).values(id=notification_id, task_id=task["id"], run_id=args[0],
            sequence=sequence, purpose="status", importance="important", body=canonical,
            created_at=service.clock.now().isoformat(), state="ACCEPTED"))
        delivery_id = service.outbox.enqueue(c, envelope.model_copy(update={"notification_sequence": sequence}))
        row = c.execute(select(outbox).where(outbox.c.id == delivery_id)).mappings().one()
        return _status(row)


def rich_status(service, args, payload):
    body = RichStatus.model_validate(payload)
    task, _event = _authorize(service, args)
    with service.db.engine.connect() as c:
        row = c.execute(select(outbox).where(outbox.c.id == body.delivery_id,
            outbox.c.task_id == task["id"])).mappings().first()
        if row is None:
            raise ValueError("delivery does not belong to this task")
        return _status(row)


class AgentButton(WorkflowButton):
    action: Literal["choice", "page", "cancel_prompt", "dismiss"]


class RichButtons(BaseModel):
    model_config = ConfigDict(extra="forbid")
    buttons: list[AgentButton] = Field(min_length=1, max_length=5)

    @field_validator("buttons", mode="before")
    @classmethod
    def parse_buttons(cls, value):
        if not isinstance(value, list):
            raise ValueError("buttons must be a list")  # noqa: TRY004
        return [b if isinstance(b, AgentButton) else AgentButton.model_validate_json(json.dumps(b)) for b in value]
    ttl_seconds: int = Field(default=300, ge=30, le=900)


def rich_buttons(service, args, payload):
    from qqbot.adapters.qq_official.interactions import InteractionStore, workflow_keyboard
    body = RichButtons.model_validate(payload)
    task, event = _authorize(service, args)
    if not isinstance(getattr(service, "qq_adapters", {}).get(event.conversation.adapter_instance_id), OfficialAdapter):
        raise ValueError("official QQ adapter unavailable")  # noqa: TRY004
    buttons = [button.model_dump(mode="json") for button in body.buttons]
    for button in buttons:
        data = button.get("payload", {})
        if data.get("task_id") not in {None, task["public_id"]}:
            raise ValueError("button task must be this task")
        if button.get("action") == "choice":
            data = {**data, "task_id": task["public_id"]}
            button["payload"] = data
    keyboard = workflow_keyboard(InteractionStore(service.db, service.clock), event.conversation,
        event.principal, buttons, ttl_seconds=body.ttl_seconds, group_key=str(uuid4())).to_payload()
    return {"binding_created": True, "keyboard": keyboard,
            "sent": False, "expires_in_seconds": body.ttl_seconds}


class LocalFileMessage(BaseModel):
    """Bounded stream metadata; no file contents in JSON or model output."""
    model_config = ConfigDict(extra="forbid")
    request_id: str = Field(min_length=1, max_length=128)
    filename: str = Field(min_length=1, max_length=255)
    media_type: Literal["image", "audio", "video", "file"]
    mime_type: str = Field(min_length=1, max_length=128)
    size: int = Field(gt=0, le=MAX_FILE_BYTES, strict=True)
    text: str = Field(default="", max_length=1800)


async def local_file_send(service, args, payload):
    raise ValueError("Base64 file transport removed; use authenticated qq-file-upload streaming endpoint")


async def local_file_upload(service, args, payload, content):
    import asyncio
    import tempfile

    from qqbot.adapters.qq_official.local_limits import limit_for
    from qqbot.adapters.qq_official.local_media import LocalMediaStore, sniff_media_file
    body = LocalFileMessage.model_validate(payload)
    task, event = _authorize(service, args)
    adapter = getattr(service, "qq_adapters", {}).get(event.conversation.adapter_instance_id)
    if not isinstance(adapter, OfficialAdapter):
        raise ValueError("official QQ adapter unavailable for this conversation")  # noqa: TRY004
    from qqbot.domain import Surface
    if event.conversation.surface not in {Surface.PRIVATE, Surface.GROUP}:
        raise ValueError("official channel local-file size limit is not verified; use a public image URL")
    limit = limit_for(body.media_type)
    if body.size > limit:
        raise ValueError("file exceeds official QQ media size limit")
    store = LocalMediaStore(service.db, service.clock)
    size = 0
    async def disk(function, *values, **kwargs):
        pending = asyncio.create_task(asyncio.to_thread(function, *values, **kwargs))
        try:
            return await asyncio.shield(pending)
        except asyncio.CancelledError:
            # A file cannot be closed while its background disk operation still owns it.
            finished = asyncio.gather(pending, return_exceptions=True)
            while not finished.done():
                try:
                    await asyncio.shield(finished)
                except asyncio.CancelledError:
                    continue
            raise
    # Unnamed private temporary file; cancellation/invalid length always closes it.
    with tempfile.TemporaryFile(mode="w+b") as staged:
        async for chunk in content:
            _authorize(service, args)
            size += len(chunk)
            if size > body.size or size > limit:
                raise ValueError("file stream exceeds declared or official limit")
            for offset in range(0, len(chunk), 65536):
                await disk(staged.write, chunk[offset:offset + 65536])
        if size != body.size:
            raise ValueError("file stream length does not match metadata")
        _authorize(service, args)
        await disk(staged.seek, 0)
        if await disk(sniff_media_file, staged, body.filename, body.media_type) != body.mime_type:
            raise ValueError("declared MIME does not match file content")
        await disk(staged.seek, 0)
        _authorize(service, args)
        await disk(store.cleanup)
        metadata = await disk(store.put_file, staged, task_id=task["id"],
            conversation_id=event.conversation.key, principal_key=event.principal.key,
            request_id=body.request_id, filename=body.filename, media_type=body.media_type)
    if metadata["mime_type"] != body.mime_type:
        raise ValueError("declared MIME does not match file content")
    _authorize(service, args)
    return await rich_send(service, args, {"request_id": body.request_id, "text": body.text,
        "official_content": {"media": {"type": body.media_type, "local_ref": metadata["id"],
                                       "filename": metadata["filename"]}}})
