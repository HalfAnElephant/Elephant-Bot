"""Encode typed messages; never drop an unsupported component silently."""
import json

from qqbot.domain import Surface
from qqbot.domain.interactions import QQKeyboard


def message_body(envelope, *, markdown_enabled=True, rich_enabled=True):
    guild = envelope.target.surface in {Surface.GUILD_CHANNEL, Surface.GUILD_PRIVATE}
    rich = envelope.official_content
    if (rich is not None or envelope.content_format == "markdown") and not rich_enabled:
        raise ValueError("RICH_MESSAGES_DISABLED")
    if guild and rich and rich.media and rich.media.local_ref:
        raise ValueError("LOCAL_MEDIA_LIMIT_UNVERIFIED")
    # Channel permissions and upload contracts are deliberately distinct.
    if guild and rich and (rich.card or (rich.media and rich.media.type != "image")):
        raise ValueError("UNSUPPORTED_SURFACE")
    body = {"content": envelope.text}
    if not guild:
        body["msg_type"] = 0
    if envelope.content_format == "markdown":
        if not markdown_enabled:
            raise ValueError("MARKDOWN_DISABLED")
        body = {"msg_type": 2, "markdown": {"content": envelope.text}}
    if rich:
        if envelope.content_format == "markdown" and (rich.media or rich.card):
            raise ValueError("INCOMPATIBLE_CONTENT")
        if rich.card:
            body = {"msg_type": 8, "card": {"type": "tuwen", "content": rich.card.model_dump()}}
        if rich.template_markdown:
            if not markdown_enabled:
                raise ValueError("MARKDOWN_DISABLED")
            body = {"msg_type": 2, "markdown": {
                "custom_template_id": rich.template_markdown.template_id,
                "params": [p.model_dump(mode="json") for p in rich.template_markdown.params],
            }}
        if rich.keyboard:
            body["keyboard"] = QQKeyboard.model_validate_json(json.dumps(rich.keyboard)).to_payload()
        if rich.reference:
            body["message_reference"] = rich.reference.model_dump()
        if rich.media:
            if guild:
                if rich.media.url is not None:
                    body["image"] = rich.media.url
            else:
                body["msg_type"] = 7
    if guild:
        body.pop("msg_type", None)
    return body
