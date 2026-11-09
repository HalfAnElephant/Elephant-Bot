import json

import pytest
from sqlalchemy import insert

from qqbot.clock import FakeClock
from qqbot.domain.messages import (
    AccessContext,
    ConversationRef,
    InboundEvent,
    ObservationMode,
    PrincipalRef,
    Surface,
    TextSegment,
)
from qqbot.memory import MemoryRecord, SocialService
from qqbot.storage.db import Database
from qqbot.storage.schema import inbox, metadata


@pytest.fixture
def setup(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    metadata.create_all(db.engine)
    clock = FakeClock()
    svc = SocialService(db, clock)
    conv = ConversationRef(
        deployment_id="d",
        adapter_instance_id="a",
        bot_identity_id="b",
        surface=Surface.GROUP,
        remote_conversation_id="g",
    )
    principal = PrincipalRef(adapter_instance_id="a", identity_namespace="qq", remote_user_id="u")
    access = AccessContext(principal=principal, conversation=conv, role="admin")

    def event(event_id, text="我想叫小陈", user="u"):
        p = principal.model_copy(update={"remote_user_id": user})
        e = InboundEvent(
            event_id=event_id,
            event_type="message.created",
            conversation=conv,
            principal=p,
            platform_message_id=event_id,
            occurred_at=clock.now(),
            received_at=clock.now(),
            segments=(TextSegment(text=text),),
            trace_id=event_id,
            observation_mode=ObservationMode.FULL,
        )
        with db.transaction() as conn:
            conn.execute(
                insert(inbox).values(
                    id=event_id,
                    conversation_id=conv.key,
                    seq=len(event_id),
                    event_json=e.model_dump_json(),
                    state="DONE",
                )
            )
        return e

    def record(source="e", value="小陈", **updates):
        return MemoryRecord(
            scope_type="principal_in_conversation",
            scope_id=conv.key,
            subject_ref=principal.key,
            predicate="name",
            value_json=json.dumps(value),
            text=value,
            source_event_ids=(source,),
            source_kind="user_explicit",
            confidence=1.0,
            valid_from=clock.now(),
            created_at=clock.now(),
            last_verified_at=clock.now(),
            **updates,
        )

    yield svc, access, event, record, clock
    db.close()


def test_scope_correct_forget_and_tombstone(setup):
    svc, access, event, record, _clock = setup
    event("e")
    r = svc.candidate(access, record(), explicit=True)
    svc.commit(access, r.memory_id, authorized=True)
    assert svc.retrieve(access, "陈")[0].memory_id == r.memory_id
    other = access.model_copy(
        update={"conversation": access.conversation.model_copy(update={"remote_conversation_id": "other"})}
    )
    assert svc.retrieve(other, "陈") == []
    event("ee", "改叫小李")
    new = svc.correct(access, r.memory_id, record("ee", "小李"))
    assert svc.retrieve(access, "李")[0].supersedes_id == r.memory_id
    assert svc.retrieve(access, "陈") == []
    svc.store_embedding(access, new.memory_id, (1.0, 0.0), "model-a", "v1")
    assert svc.retrieve(access, "missing", embedding=(1.0, 0.0), model_id="model-b", version="v1") == []
    assert svc.retrieve(access, "missing", embedding=(1.0, 0.0), model_id="model-a", version="v1")
    version = svc.version(access)
    svc.forget(access, new.memory_id)
    assert svc.version(access) > version
    assert svc.retrieve(access, "李") == []
    with pytest.raises(ValueError, match="revoked"):
        svc.candidate(access, record("ee", "小李"), explicit=True)


def test_evidence_permissions_revocation(setup):
    svc, access, event, record, _clock = setup
    with pytest.raises(ValueError, match="missing"):
        svc.candidate(access, record(), explicit=True)
    event("e")
    r = svc.candidate(access, record(), explicit=True)
    with pytest.raises(PermissionError):
        svc.commit(access, r.memory_id)
    svc.commit(access, r.memory_id, authorized=True)
    other = access.model_copy(
        update={"principal": access.principal.model_copy(update={"remote_user_id": "other"})}
    )
    assert svc.retrieve(other, "陈") == []
    svc.revoke_source(access, "e")
    assert svc.retrieve(access, "陈") == []


def test_jargon_short_words_and_repetition(setup):
    svc, access, event, _record, _clock = setup
    event("e", "发车")
    event("ee", "发车", "v")
    event("eee", "发车")
    assert svc.jargon(access, "车", "activity", "活动", ["e", "ee", "eee"]).status == "candidate"
    event("eeee", "明天发车", "v")
    event("eeeee", "周末也发车")
    entry, _mid = svc.learn_jargon(
        access, "车", "activity", "活动", ["e", "eeee", "eeeee"], authorized=True, allow_output=True
    )
    assert entry.status == "active"
    assert svc.jargon_entries(access, "车", for_output=True)[0].definition == "活动"
    svc.revoke_source(access, "e")
    assert svc.jargon_entries(access, "车") == []


async def test_embedding_missing_key_and_real_contract():
    import httpx

    from qqbot.memory.embedding import HTTPEmbedding

    assert await HTTPEmbedding("https://example.invalid", "m", "v").embed("中文") is None

    def handler(request):
        return httpx.Response(200, json={"data": [{"embedding": [1.0, 0.0]}]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = HTTPEmbedding("https://example.invalid", "m", "v", "key", dimensions=2, client=client)
        assert await provider.embed("中文") == (1.0, 0.0)
        provider.dimensions = 3
        assert await provider.embed("中文") is None


def test_replay_safe_ids_and_global_revocation_epoch(setup):
    svc, access, event, record, _clock = setup
    event("e")
    r = record().model_copy(update={"memory_id": "stable-id"})
    svc.candidate(access, r, explicit=True)
    svc.commit(access, r.memory_id, authorized=True)
    assert svc.candidate(access, r, explicit=True).status == "active"
    with pytest.raises(ValueError, match="conflicts"):
        svc.candidate(access, r.model_copy(update={"text": "different"}), explicit=True)
    event("ee", "新称呼")
    replacement = record("ee", "新名").model_copy(update={"memory_id": "stable-correction"})
    new = svc.correct(access, r.memory_id, replacement)
    assert svc.correct(access, r.memory_id, replacement).memory_id == new.memory_id
    other = access.model_copy(
        update={"conversation": access.conversation.model_copy(update={"remote_conversation_id": "other"})}
    )
    before = svc.version(other)
    svc.forget(access, new.memory_id)
    assert svc.version(other) > before
    svc.forget(access, new.memory_id)
