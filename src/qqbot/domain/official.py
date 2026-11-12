"""Portable rich-content contracts, persisted with the outbound envelope.

URLs here are delivery inputs, never authority to download arbitrary resources.
QQ's upload endpoint fetches public media; local file/credential URLs are rejected.
"""
import ipaddress
import json
from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class RichDTO(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)


def public_https_url(value: str) -> str:
    parsed = urlsplit(value)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("media requires a public HTTPS URL without credentials")
    host = parsed.hostname.lower().rstrip(".")
    if host in {"localhost", "metadata.google.internal"} or host.endswith((".localhost", ".local", ".internal")):
        raise ValueError("local media addresses are not allowed")
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        if "." not in host:
            raise ValueError("media requires a public host") from None
    else:
        if not address.is_global:
            raise ValueError("private media addresses are not allowed")
    if parsed.port not in (None, 443):
        raise ValueError("media URL must use HTTPS default port")
    return value


class OfficialMedia(RichDTO):
    type: Literal["image", "audio", "video", "file"]
    url: str | None = Field(default=None, min_length=1, max_length=8192)
    local_ref: str | None = Field(default=None, pattern=r"^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$")
    filename: str | None = Field(default=None, max_length=255)

    @field_validator("url")
    @classmethod
    def valid_url(cls, value):
        return public_https_url(value) if value is not None else None

    @model_validator(mode="after")
    def exactly_one_source(self):
        if (self.url is None) == (self.local_ref is None):
            raise ValueError("select exactly one HTTPS URL or scoped local reference")
        return self


class MarkdownParameter(RichDTO):
    key: str = Field(min_length=1, max_length=128)
    values: tuple[str, ...] = Field(min_length=1, max_length=20)


class TemplateMarkdown(RichDTO):
    template_id: str = Field(min_length=1, max_length=128)
    params: tuple[MarkdownParameter, ...] = Field(default=(), max_length=100)


class MessageReference(RichDTO):
    message_id: str = Field(min_length=1, max_length=256)
    ignore_get_message_error: bool = True


class TuwenCard(RichDTO):
    title: str = Field(min_length=1, max_length=50)
    description: str = Field(min_length=1, max_length=200)
    url: str = Field(min_length=1, max_length=1024)
    pic_url: str = Field(min_length=1, max_length=1024)

    @field_validator("url", "pic_url")
    @classmethod
    def valid_url(cls, value):
        return public_https_url(value)


class OfficialContent(RichDTO):
    """Payload variants supported by the QQ renderer; no arbitrary request fields."""
    media: OfficialMedia | None = None
    card: TuwenCard | None = None
    template_markdown: TemplateMarkdown | None = None
    # Keyboard is validated by the interaction module before it reaches this DTO.
    keyboard: dict | None = None
    reference: MessageReference | None = None

    @model_validator(mode="after")
    def validate_content(self):
        if sum(value is not None for value in (self.media, self.card, self.template_markdown)) > 1:
            raise ValueError("select one media, card or template body")
        if self.card and self.keyboard:
            raise ValueError("card must be sent separately from keyboards")
        if self.media and (self.template_markdown or self.keyboard):
            raise ValueError("media must be sent separately from Markdown/keyboards")
        if self.keyboard is not None:
            from qqbot.domain.interactions import QQKeyboard
            QQKeyboard.model_validate_json(json.dumps(self.keyboard))
        return self
