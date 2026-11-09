"""Offline, operator-curated private profile import; never enqueues platform messages."""

import argparse
import json
from datetime import datetime
from pathlib import Path

from pydantic import Field
from sqlalchemy import insert, select

from qqbot.clock import Clock
from qqbot.config import load_settings
from qqbot.domain import DTO, AccessContext, InboundEvent, Surface, stable_id
from qqbot.memory import MemoryRecord, SocialService, _safe
from qqbot.storage.db import Database
from qqbot.storage.schema import inbox
from qqbot.storage.social_schema import social_records, social_tombstones


class Entry(DTO):
    key: str = Field(min_length=1, max_length=100)
    text: str = Field(min_length=1, max_length=2000)
    excerpt: str = Field(min_length=1, max_length=4000)


class Profile(DTO):
    source: str = Field(min_length=1, max_length=1000)
    observed_at: datetime
    entries: tuple[Entry, ...] = Field(min_length=1, max_length=20)


def import_profile(db, clock, access, profile):
    if (
        access.role not in {"owner", "admin"}
        or access.conversation.surface != Surface.PRIVATE
        or access.conversation.remote_conversation_id != access.principal.remote_user_id
        or access.conversation.adapter_instance_id != access.principal.adapter_instance_id
    ):
        raise PermissionError("import requires the authorized subject's private conversation")
    if profile.observed_at.tzinfo is None or profile.observed_at > clock.now():
        raise ValueError("invalid source observation time")
    if len({entry.key for entry in profile.entries}) != len(profile.entries):
        raise ValueError("duplicate entry keys")
    service = SocialService(db, clock)
    records = []
    for entry in profile.entries:
        source_id = "legacy-profile:" + stable_id(access.conversation.key, profile.source, entry.key)
        provenance = {
            "source": profile.source,
            "excerpt": entry.excerpt,
            "observed_at": profile.observed_at.isoformat(),
            "original_message_verified": False,
        }
        record = MemoryRecord(
            memory_id=stable_id("koishi-profile-v1", source_id),
            scope_type="principal_in_conversation",
            scope_id=access.conversation.key,
            subject_ref=access.principal.key,
            predicate="legacy_profile:" + entry.key,
            value_json=json.dumps({"value": entry.text, "provenance": provenance}, ensure_ascii=False),
            text=entry.text,
            source_event_ids=(source_id,),
            source_kind="inference",
            confidence=0.8,
            status="active",
            sensitivity="private",
            valid_from=clock.now(),
            created_at=clock.now(),
            last_verified_at=profile.observed_at,
            extraction_model="curated-legacy-profile",
            prompt_version="koishi-import-v1",
        )
        _safe(record.text + record.value_json)
        records.append(record)
    result = {"inserted": 0, "existing": 0, "revoked": 0}
    with db.transaction() as conn:
        for record in records:
            if conn.execute(
                select(social_tombstones.c.source_id).where(
                    social_tombstones.c.source_id.in_(record.source_event_ids)
                )
            ).first():
                result["revoked"] += 1
                continue
            raw = conn.execute(
                select(social_records.c.record_json).where(social_records.c.id == record.memory_id)
            ).scalar_one_or_none()
            if raw:
                old = MemoryRecord.model_validate_json(raw)
                if old.status == "deleted":
                    result["revoked"] += 1
                    continue
                if not service._same_content(old, record):
                    raise ValueError("existing import differs; use explicit memory correction")
                result["existing"] += 1
                continue
            conn.execute(
                insert(social_records).values(
                    id=record.memory_id,
                    conversation_id=access.conversation.key,
                    scope_type=record.scope_type,
                    scope_id=record.scope_id,
                    subject_ref=record.subject_ref,
                    status=record.status,
                    record_json=record.model_dump_json(),
                )
            )
            result["inserted"] += 1
        if result["inserted"]:
            service._bump(conn, access.conversation.key)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--event-id", required=True, help="Existing private event identifying the subject")
    args = parser.parse_args()
    settings = load_settings(args.config)
    profile = Profile.model_validate_json(Path(args.manifest).read_text())
    db = Database(settings.database)
    try:
        with db.engine.connect() as conn:
            raw = conn.execute(select(inbox.c.event_json).where(inbox.c.id == args.event_id)).scalar_one()
        event = InboundEvent.model_validate_json(raw)
        adapter = next(
            a for a in settings.adapters if a.instance_id == event.conversation.adapter_instance_id
        )
        if (
            event.sender_is_bot
            or event.event_type != "message.created"
            or event.conversation.deployment_id != settings.deployment_id
            or event.conversation.bot_identity_id != adapter.bot_id
            or event.principal.remote_user_id not in adapter.admin_users
            or event.principal.remote_user_id not in adapter.allowed_users
        ):
            raise PermissionError("subject is not the configured allowed administrator")
        access = AccessContext(principal=event.principal, conversation=event.conversation, role="admin")
        print(json.dumps(import_profile(db, Clock(), access, profile)))
    finally:
        db.close()


if __name__ == "__main__":
    main()
