"""Authenticated owner administration of durable schedules."""
from uuid import uuid4

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from qqbot.domain import ConversationRef, InboundEvent, PrincipalRef, Surface
from qqbot.domain.messages import TextSegment
from qqbot.permissions import PermissionSubject
from qqbot.runtime.schedules import ScheduleSpec


class ScheduleUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expected_revision: int = Field(ge=1)
    changes: dict


def build_schedule_router(core, admin):
    def authorize(request: Request):
        subject = PermissionSubject(adapter_instance_id="admin_api", identity_namespace="admin_api", user_id="owner")
        if not core.permissions.check(subject, "admin." + request.method.lower(), "http:" + request.url.path).allowed:
            raise HTTPException(403, "permission denied")

    router = APIRouter(prefix="/api/v1/schedules", dependencies=[Depends(admin), Depends(authorize)])

    def event():
        now = core.clock.now()
        return InboundEvent(event_id=str(uuid4()), event_type="message.created",
            conversation=ConversationRef(deployment_id=core.settings.deployment_id,
                adapter_instance_id="admin_api", bot_identity_id="host", surface=Surface.PRIVATE,
                remote_conversation_id=core.settings.owner.id),
            principal=PrincipalRef(adapter_instance_id="admin_api", identity_namespace="admin_api",
                remote_user_id=core.settings.owner.id),
            platform_message_id="admin-schedule", occurred_at=now, received_at=now,
            segments=(TextSegment(text="管理定时任务"),), trace_id=str(uuid4()))

    @router.post("")
    def create(body: ScheduleSpec, idempotency_key: str = Header(alias="Idempotency-Key")):
        return core.schedules.create(event(), body, idempotency_key=idempotency_key)

    @router.get("")
    def list_schedules():
        return core.schedules.list(event())

    @router.get("/{schedule_id}")
    def get(schedule_id: str):
        return core.schedules.get(event(), schedule_id)

    @router.patch("/{schedule_id}")
    def update(schedule_id: str, body: ScheduleUpdate):
        return core.schedules.update(event(), schedule_id, body.changes, expected_revision=body.expected_revision)

    @router.delete("/{schedule_id}")
    def delete(schedule_id: str, expected_revision: int):
        return core.schedules.delete(event(), schedule_id, expected_revision=expected_revision)

    return router
