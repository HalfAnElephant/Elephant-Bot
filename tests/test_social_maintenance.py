import json

import httpx
import pytest
from sqlalchemy import select, update

from qqbot.config import MemoryConfig
from qqbot.domain.messages import MediaSegment
from qqbot.memory.embedding import HTTPEmbedding
from qqbot.memory.extraction import HTTPCandidateExtractor
from qqbot.memory.maintenance import MemoryMaintenance
from qqbot.storage.schema import inbox
from qqbot.storage.social_schema import memory_deletions, social_index_jobs, social_records

pytest_plugins = ("test_social_memory",)


def provider(body):
    return httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, json={"choices": [{"message": {"content": json.dumps(body)}}]}
            )
        )
    )


async def test_disabled_extractor_and_indexes_mark_skipped(setup):
    svc, access, event, record, _clock = setup
    event("e")
    r = svc.candidate(access, record(), explicit=True)
    svc.commit(access, r.memory_id, authorized=True)
    maintenance = MemoryMaintenance(svc)
    report = await maintenance.maintenance_once(access, auto_extract=True)
    assert report.state == "SKIPPED" and report.skipped_indexes == 1
    with svc.db.engine.connect() as conn:
        assert conn.execute(select(social_index_jobs.c.state)).scalar_one() == "SKIPPED"
    extractor = HTTPCandidateExtractor("https://example.invalid", "m", enabled=True)
    assert (await extractor.extract([])).state == "SKIPPED"


async def test_incremental_summary_delete_and_no_revival(setup):
    svc, access, event, record, _clock = setup
    event("e", "称呼是小陈")
    r = svc.candidate(access, record(), explicit=True)
    svc.commit(access, r.memory_id, authorized=True)
    maintenance = MemoryMaintenance(svc, enabled=True)
    report = await maintenance.maintenance_once(access)
    assert report.summary_seq == 1
    assert "小陈" in maintenance.summary(access)
    assert (await maintenance.maintenance_once(access)).summary_seq == 1
    svc.forget(access, r.memory_id)
    assert maintenance.summary(access) == ""
    with svc.db.engine.connect() as conn:
        raw = conn.execute(select(social_records.c.record_json)).scalar_one()
        assert "小陈" not in raw and json.loads(raw)["value_json"] == "null"
        assert conn.execute(select(memory_deletions.c.memory_id)).scalar_one() == r.memory_id
    event("ee", "新的独立话题")
    await maintenance.maintenance_once(access)
    assert "小陈" not in maintenance.summary(access)
    assert "新的独立话题" in maintenance.summary(access)


async def test_provider_suggestions_require_review_and_reject_foreign_evidence(setup):
    svc, access, event, _record, _clock = setup
    event("e")
    suggestion = {
        "scope_type": "principal_in_conversation",
        "subject_ref": access.principal.key,
        "predicate": "name",
        "value_json": json.dumps("小陈"),
        "text": "小陈",
        "source_event_ids": ["e"],
        "confidence": 0.9,
        "should_persist": True,
    }
    async with provider(
        {"suggestions": [suggestion, {**suggestion, "source_event_ids": ["foreign"]}]}
    ) as client:
        extractor = HTTPCandidateExtractor("https://example.invalid", "m", "key", enabled=True, client=client)
        maintenance = MemoryMaintenance(svc, extractor, enabled=True)
        report = await maintenance.maintenance_once(access, auto_extract=True)
    assert report.candidates == 1 and report.rejected == 1
    assert svc.retrieve(access, "陈") == []
    with svc.db.engine.connect() as conn:
        assert conn.execute(select(social_records.c.status)).scalar_one() == "candidate"
    report = await maintenance.maintenance_once(access, auto_extract=True)
    assert report.candidates == 0


async def test_provider_failure_keeps_checkpoint_retryable(setup):
    svc, access, event, _record, _clock = setup
    event("e")
    async with provider({"wrong": []}) as client:
        extractor = HTTPCandidateExtractor("https://example.invalid", "m", "key", enabled=True, client=client)
        maintenance = MemoryMaintenance(svc, extractor, enabled=True)
        assert (await maintenance.maintenance_once(access, auto_extract=True)).extraction_state == "FAILED"
    async with provider({"suggestions": []}) as client:
        extractor.client = client
        assert (await maintenance.maintenance_once(access, auto_extract=True)).extraction_state == "DONE"


async def test_embedding_index_and_delete_during_http(setup):
    svc, access, event, record, _clock = setup
    event("e")
    r = svc.candidate(access, record(), explicit=True)
    svc.commit(access, r.memory_id, authorized=True)

    def handler(request):
        svc.forget(access, r.memory_id)
        return httpx.Response(200, json={"data": [{"embedding": [1.0, 0.0]}]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        embedding = HTTPEmbedding("https://example.invalid", "m", "v", "key", client=client)
        maintenance = MemoryMaintenance(svc, embedding=embedding, enabled=True)
        report = await maintenance.maintenance_once(access)
        assert report.indexed == 0 and report.skipped_indexes == 1
    assert svc.retrieve(access, "陈") == []


async def test_retention_preserves_minimal_evidence_for_active_fact(setup):
    svc, access, event, record, clock = setup
    event("e", "小陈" + "x" * 400)
    event("ee", "无长期事实的过期消息")
    r = svc.candidate(access, record(), explicit=True)
    svc.commit(access, r.memory_id, authorized=True)
    clock.advance(31 * 86400)
    maintenance = MemoryMaintenance(svc, enabled=True, raw_retention_days=30)
    report = await maintenance.maintenance_once(access)
    assert report.expired_sources == 2
    assert svc.retrieve(access, "陈")
    with svc.db.engine.connect() as conn:
        rows = conn.execute(select(inbox.c.id, inbox.c.event_json)).all()
    raw = {key: json.loads(value) for key, value in rows}
    assert len(raw["e"]["segments"][0]["text"]) == 300
    assert raw["ee"]["segments"] == []
    assert (await maintenance.maintenance_once(access)).expired_sources == 0


async def test_legacy_deleted_body_is_purged(setup):
    svc, access, event, record, _clock = setup
    event("e")
    r = svc.candidate(access, record(), explicit=True)
    with svc.db.transaction() as conn:
        conn.execute(
            update(social_records)
            .where(social_records.c.id == r.memory_id)
            .values(
                status="deleted", record_json=r.model_copy(update={"status": "deleted"}).model_dump_json()
            )
        )
    maintenance = MemoryMaintenance(svc, enabled=True)
    assert (await maintenance.maintenance_once(access)).purged_records == 1
    with svc.db.engine.connect() as conn:
        assert "小陈" not in conn.execute(select(social_records.c.record_json)).scalar_one()
    assert maintenance.summary(access) == ""
    assert (await maintenance.maintenance_once(access)).purged_records == 0


async def test_missing_embedding_key_requeue_and_success(setup):
    svc, access, event, record, _clock = setup
    event("e")
    r = svc.candidate(access, record(), explicit=True)
    svc.commit(access, r.memory_id, authorized=True)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"data": [{"embedding": [1.0, 0.0]}]})
        )
    ) as client:
        embedding = HTTPEmbedding("https://example.invalid", "model", "v1", client=client)
        maintenance = MemoryMaintenance(svc, embedding=embedding, enabled=True)
        assert (await maintenance.maintenance_once(access)).skipped_indexes == 1
        embedding.api_key = "configured-key"
        maintenance.requeue_embeddings(access)
        assert (await maintenance.maintenance_once(access)).indexed == 1
    assert svc.retrieve(access, "no-match", embedding=(1.0, 0.0), model_id="model", version="v1")


async def test_source_recall_during_extraction_rejects_late_suggestion(setup):
    svc, access, event, _record, _clock = setup
    event("e")
    suggestion = {
        "scope_type": "principal_in_conversation",
        "subject_ref": access.principal.key,
        "predicate": "name",
        "value_json": json.dumps("小陈"),
        "text": "小陈",
        "source_event_ids": ["e"],
        "confidence": 0.9,
        "should_persist": True,
    }

    def handler(request):
        svc.revoke_source(access, "e")
        return httpx.Response(
            200, json={"choices": [{"message": {"content": json.dumps({"suggestions": [suggestion]})}}]}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        extractor = HTTPCandidateExtractor("https://example.invalid", "m", "key", enabled=True, client=client)
        maintenance = MemoryMaintenance(svc, extractor, enabled=True)
        report = await maintenance.maintenance_once(access, auto_extract=True)
        assert report.candidates == 0 and report.rejected == 1
        assert maintenance.summary(access) == ""


@pytest.mark.parametrize("age_days", [31, 365])
async def test_default_retention_preserves_old_originals_during_all_maintenance(setup, age_days):
    svc, access, event, record, clock = setup
    original = event("e", "小陈" + "x" * 400)
    original = original.model_copy(
        update={"segments": (*original.segments, MediaSegment(type="image", attachment_ref="archive/image"))}
    )
    event("ee", "没有长期事实的原始消息")
    with svc.db.transaction() as conn:
        conn.execute(update(inbox).where(inbox.c.id == "e").values(event_json=original.model_dump_json()))
        before = dict(conn.execute(select(inbox.c.id, inbox.c.event_json)).all())
    r = svc.candidate(access, record(), explicit=True)
    svc.commit(access, r.memory_id, authorized=True)
    clock.advance(age_days * 86400)
    async with (
        provider({"suggestions": []}) as extraction_client,
        httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, json={"data": [{"embedding": [1.0, 0.0]}]})
            )
        ) as embedding_client,
    ):
        maintenance = MemoryMaintenance(
            svc,
            extractor=HTTPCandidateExtractor(
                "https://example.invalid", "m", "key", enabled=True, client=extraction_client
            ),
            embedding=HTTPEmbedding("https://example.invalid", "m", "v", "key", client=embedding_client),
            enabled=True,
        )
        report = await maintenance.maintenance_once(access, auto_extract=True)
    assert report.expired_sources == 0
    assert report.extraction_state == "DONE" and report.indexed == 1
    assert "小陈" in maintenance.summary(access)
    with svc.db.engine.connect() as conn:
        assert dict(conn.execute(select(inbox.c.id, inbox.c.event_json)).all()) == before
    svc.revoke_source(access, "e")
    assert maintenance.summary(access) == ""
    assert svc.retrieve(access, "陈") == []
    with svc.db.engine.connect() as conn:
        assert conn.execute(select(social_index_jobs.c.id)).first() is None


def test_retention_config_default_and_legacy_number():
    assert MemoryConfig().raw_retention_days is None
    assert MemoryConfig.model_validate({"raw_retention_days": None}).raw_retention_days is None
    assert MemoryConfig.model_validate({"raw_retention_days": 30}).raw_retention_days == 30
    for days in (0, -1, 3651):
        with pytest.raises(ValueError):
            MemoryConfig(raw_retention_days=days)
