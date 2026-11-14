"""Host-only credentials for a proven inbound attachment on QQ's private CDN."""

import hashlib
from urllib.parse import urlsplit

from sqlalchemy import select

from qqbot.domain import InboundEvent, PrincipalRef
from qqbot.storage.schema import inbox

from .adapter import OfficialAdapter


async def inbound_media_headers(core, claim, attachment) -> dict:
    try:
        url = attachment.get("url")
        parsed = urlsplit(url) if isinstance(url, str) else None
        trusted = (parsed is not None and parsed.hostname == "multimedia.nt.qq.com.cn"
            and ((parsed.scheme == "https" and parsed.port in {None, 443})
                 or (parsed.scheme == "http" and parsed.port in {None, 80}))
            and not parsed.username and not parsed.password)
    except ValueError:
        trusted = False
    if not trusted:
        return {}
    with core.db.engine.connect() as connection:
        raw = connection.execute(select(inbox.c.event_json).where(
            inbox.c.id == attachment.get("source"))).scalar_one_or_none()
    if raw is None:
        raise PermissionError("QQ media requires a durably accepted source")
    try:
        source = InboundEvent.model_validate_json(raw)
        principal = PrincipalRef.model_validate(claim["permission_snapshot"]["principal"])
        context = claim["context"]
        if (source.event_type != "message.created" or source.principal != principal
                or source.conversation.key != context.get("scope")):
            raise PermissionError("QQ media source does not match the run identity")
        if claim.get("event_json"):
            origin = InboundEvent.model_validate_json(claim["event_json"])
            if origin.principal != source.principal or origin.conversation != source.conversation:
                raise PermissionError("QQ media source does not match the run conversation")
    except (ValueError, TypeError, KeyError):
        raise PermissionError("QQ media requires a valid run identity") from None
    manifest = {"kind": "inbox", "id": source.event_id, "digest": hashlib.sha256(raw.encode()).hexdigest()}
    if manifest not in context.get("source_manifest", ()):
        raise PermissionError("QQ media source is absent from the run's verified context")
    if not any(segment.type in {"audio", "file"} and segment.source_url == url
            and segment.attachment_ref == attachment.get("attachment_ref") for segment in source.segments):
        raise PermissionError("QQ media URL does not belong to the accepted attachment")
    adapter = core.adapters.get(source.conversation.adapter_instance_id)
    if not isinstance(adapter, OfficialAdapter):
        raise PermissionError("QQ media source has no official adapter")
    access = core.access(source)

    def authorize():
        if not core.allowed(source) or not core.validate_context(access, context):
            raise PermissionError("QQ media source authorization was revoked")

    authorize()
    token = await adapter.access_token()
    authorize()
    return {"Authorization": "QQBot " + token}
