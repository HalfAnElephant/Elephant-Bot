import json
from datetime import UTC, datetime
from enum import StrEnum
from hashlib import sha256
from typing import Annotated, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, field_validator


class DTO(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)


class Surface(StrEnum):
    PRIVATE = "private"
    GROUP = "group"
    GUILD_CHANNEL = "guild_channel"


class ObservationMode(StrEnum):
    FULL = "full"
    MENTIONS_ONLY = "mentions_only"
    PARTIAL = "partial"
    UNKNOWN = "unknown"


def stable_id(*parts: str) -> str:
    return sha256(json.dumps(parts, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()


class ConversationRef(DTO):
    deployment_id: str
    adapter_instance_id: str
    bot_identity_id: str
    surface: Surface
    remote_conversation_id: str

    @property
    def key(self) -> str:
        return stable_id(*[str(v) for v in self.model_dump().values()])


class PrincipalRef(DTO):
    adapter_instance_id: str
    identity_namespace: str
    remote_user_id: str

    @property
    def key(self) -> str:
        return stable_id(self.adapter_instance_id, self.identity_namespace, self.remote_user_id)


class TextSegment(DTO):
    type: Literal["text"] = "text"
    text: str


class MentionSegment(DTO):
    type: Literal["mention"] = "mention"
    remote_user_id: str


class MediaSegment(DTO):
    type: Literal["image", "audio", "file"]
    attachment_ref: str
    mime_type: str | None = None
    size: int | None = Field(default=None, ge=0)
    sha256: str | None = None


class FaceSegment(DTO):
    type: Literal["face"] = "face"
    face_id: str


class ReplySegment(DTO):
    type: Literal["reply"] = "reply"
    platform_message_id: str


class UnknownSegment(DTO):
    type: Literal["unknown"] = "unknown"
    original_type: str
    digest: str


Segment = Annotated[
    TextSegment | MentionSegment | MediaSegment | FaceSegment | ReplySegment | UnknownSegment,
    Field(discriminator="type"),
]


class ReplyContext(DTO):
    platform_message_id: str
    expires_at: datetime | None = None
    event_id: str | None = None
    max_replies: int = Field(default=5, ge=1)
    msg_seq: int | None = Field(default=None, ge=1)


class InboundEvent(DTO):
    schema_version: Literal[1] = 1
    event_id: str
    platform_event_id: str | None = None
    event_type: Literal[
        "message.created", "message.recalled", "member.changed", "mute.changed", "connection.changed"
    ]
    conversation: ConversationRef
    principal: PrincipalRef
    platform_message_id: str
    occurred_at: datetime
    received_at: datetime
    segments: tuple[Segment, ...] = ()
    reply_to: str | None = None
    mentions_bot: bool = False
    sender_is_bot: bool = False
    observation_mode: ObservationMode = ObservationMode.UNKNOWN
    reply_context: ReplyContext | None = None
    trace_id: str
    ingest_seq: int = 0

    @field_validator("occurred_at", "received_at")
    @classmethod
    def utc_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("timezone required")
        return value.astimezone(UTC)

    @property
    def text(self) -> str:
        return "".join(s.text for s in self.segments if isinstance(s, TextSegment))


class AdapterCapabilities(DTO):
    can_observe_unaddressed_messages: bool = False
    can_reply: bool = False
    can_initiate_message: bool = False
    supports_reply_reference: bool = False
    supports_mention: bool = False
    supports_images: bool = False
    supports_files: bool = False
    supports_recall: bool = False
    supports_message_lookup: bool = False
    supports_history_fetch: bool = False
    observation_mode: ObservationMode = ObservationMode.UNKNOWN
    delivery_constraints: tuple[str, ...] = ()


class OutboundEnvelope(DTO):
    delivery_id: str
    target: ConversationRef
    text: str = Field(min_length=1, max_length=10000)
    reply_context: ReplyContext | None = None
    origin_kind: Literal["chat", "command", "task"] = "chat"
    origin_id: str
    part_no: int = 0
    content_version: int = 1
    expires_at: datetime
    allow_proactive: bool = False
    context_version: int | None = None
    persona_version: str | None = None
    policy_version: int | None = None
    anchor_message_id: str | None = None
    requester: PrincipalRef | None = None


class DeliveryCheck(DTO):
    allowed: bool
    reason: str
    wait_for_trigger: bool = False


class SendReceipt(DTO):
    state: Literal["SENT", "RETRYABLE", "FAILED", "UNKNOWN", "WAITING_FOR_TRIGGER"]
    platform_message_id: str | None = None
    error_code: str | None = None


class AccessContext(DTO):
    principal: PrincipalRef
    conversation: ConversationRef
    role: Literal["member", "admin", "owner"] = "member"
    policy_version: int = 1


class PlatformAdapter(Protocol):
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def capabilities(self, target: ConversationRef) -> AdapterCapabilities: ...
    async def validate_delivery(self, envelope: OutboundEnvelope) -> DeliveryCheck: ...
    async def send(self, envelope: OutboundEnvelope) -> SendReceipt: ...
