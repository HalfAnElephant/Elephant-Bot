import json

from sqlalchemy import insert, select

from qqbot.domain.messages import AccessContext
from qqbot.memory.extraction import ExtractionResult, JargonSuggestion, MemorySuggestion
from qqbot.memory.history import HistoryMemoryService
from qqbot.memory.maintenance import MemoryMaintenance
from qqbot.permissions import PermissionPolicy, PermissionRule, PermissionService
from qqbot.storage.schema import inbox
from qqbot.storage.social_schema import social_records

pytest_plugins = ("test_social_memory",)


async def test_history_chinese_pagination_original_and_tombstone(setup):
    social, access, event, _, _ = setup
    first = event("e", "旧路径 /archive/a")
    event("ee", "新路径 /archive/b")
    service = HistoryMemoryService(social)
    result = await service.query(access, "history.search", {"query": "路径", "limit": 1})
    assert result["messages"][0]["source_id"] == "ee"
    result = await service.query(access, "history.search", {
        "query": "路径", "before_seq": result["before_seq"], "limit": 1})
    assert result["messages"][0]["text"] == first.text
    assert (await service.query(access, "history.read", {"source_ids": ["e"]}))["messages"]
    social.revoke_source(access, "e")
    assert not (await service.query(access, "history.read", {"source_ids": ["e"]}))["messages"]
    other = access.model_copy(update={"conversation": access.conversation.model_copy(update={"remote_conversation_id": "other"})})
    assert not (await service.query(other, "history.search", {"query": "路径"}))["messages"]


async def test_history_read_rechecks_permission_and_recall(setup):
    social, access, event, _, _clock = setup
    original = event("e", "路径 abc")
    service = HistoryMemoryService(social)
    social.permissions = service.permissions = PermissionService(policy=PermissionPolicy(rules=(PermissionRule(
        id="deny-source", actions=("history.source.read",), resources=("event:e",), effect="deny"),)))
    assert not (await service.query(access, "history.read", {"source_ids": ["e"]}))["messages"]
    social.permissions = service.permissions = PermissionService()
    recalled = original.model_copy(update={"event_id": "recall", "event_type": "message.recalled"})
    with social.db.transaction() as conn:
        conn.execute(insert(inbox).values(id="recall", conversation_id=access.conversation.key, seq=2,
            event_type="message.recalled", message_id="e", event_json=recalled.model_dump_json(), state="DONE"))
    assert not (await service.query(access, "history.read", {"source_ids": ["e"]}))["messages"]


class Embedding:
    api_key = "key"
    model = "m"
    version = "v1"
    dimensions = 2

    def __init__(self):
        self.calls = 0

    async def embed(self, _text):
        self.calls += 1
        return (1.0, 0.0)


async def test_query_embedding_hybrid_synonym_cache_scope_version_and_fallback(setup):
    social, access, event, record, _ = setup
    event("e", "我最近很高兴")
    memory = social.candidate(access, record(value="高兴"), explicit=True)
    social.commit(access, memory.memory_id, authorized=True)
    social.store_embedding(access, memory.memory_id, (1.0, 0.0), "m", "v1")
    embedding = Embedding()
    service = HistoryMemoryService(social, embedding)
    assert social.retrieve(access, "感觉非常开心") == []
    result = await service.query(access, "memory.search", {"query": "感觉非常开心"})
    assert result["retrieval"] == "hybrid" and result["records"][0]["memory_id"] == memory.memory_id
    await service.query_options(access, "感觉非常开心")
    assert embedding.calls == 1
    other = access.model_copy(update={"principal": access.principal.model_copy(update={"remote_user_id": "other"})})
    assert not (await service.query(other, "memory.search", {"query": "感觉非常开心"}))["records"]
    assert embedding.calls == 2
    embedding.version = "v2"
    assert not (await service.query(access, "memory.search", {"query": "感觉非常开心"}))["records"]
    assert embedding.calls == 3
    social.forget(access, memory.memory_id)
    assert not (await service.query(access, "memory.get", {"memory_id": memory.memory_id}))["records"]
    async def unavailable(_text):
        raise TimeoutError
    embedding.embed = unavailable
    assert await service.query_options(access, "新的未缓存查询") == {}
    assert await service.query_options(access, "你好") == {}


class Extractor:
    model = "fake"

    def __init__(self, result):
        self.result = result

    async def extract(self, _events):
        return self.result


def suggestion(event, **changes):
    return MemorySuggestion(scope_type="principal_in_conversation", subject_ref=event.principal.key,
        predicate="preference", value_json=json.dumps(event.text), text=event.text,
        source_event_ids=(event.event_id,), confidence=0.95, should_persist=True,
        self_report_quote=event.text, low_risk=True, **changes)


async def test_mixed_subjects_publish_and_correction_remains_retrievable(setup):
    social, access, event, _, _ = setup
    first, second = event("e", "我喜欢猫", user="a"), event("ee", "我喜欢狗", user="b")
    bad = suggestion(first).model_copy(update={"subject_ref": second.principal.key})
    extractor = Extractor(ExtractionResult(state="DONE", suggestions=(bad, suggestion(first), suggestion(second))))
    maintenance = MemoryMaintenance(social, extractor, enabled=True)
    report = await maintenance.maintenance_once(access, auto_extract=True)
    assert report.candidates == 2 and report.rejected == 1
    for source in (first, second):
        identity = AccessContext(principal=source.principal, conversation=source.conversation)
        records = social.retrieve(identity)
        assert len(records) == 1 and records[0].text == source.text
        assert records[0].status == "active"
    assert not social.retrieve(access)
    assert (await maintenance.maintenance_once(access, auto_extract=True)).candidates == 0


async def test_self_report_publication_can_be_disabled_and_inference_stays_candidate(setup):
    social, access, event, _, _ = setup
    source = event("e", "我喜欢猫")
    extractor = Extractor(ExtractionResult(state="DONE", suggestions=(suggestion(source),)))
    maintenance = MemoryMaintenance(social, extractor, enabled=True, auto_publish_self_reports=False)
    await maintenance.maintenance_once(access, auto_extract=True)
    assert not social.retrieve(access)
    with social.db.engine.connect() as conn:
        assert conn.execute(select(social_records.c.status)).scalar_one() == "candidate"


async def test_jargon_miner_independent_contexts_scope_and_revocation(setup):
    social, access, event, _, _ = setup
    event("e", "象站就是我们的内网", user="a")
    event("ee", "去象站交作业", user="b")
    event("eee", "象站登录好了", user="a")
    jargon = JargonSuggestion(term="象站", sense_id="intranet", definition="本群内网",
        source_event_ids=("e", "ee", "eee"), confidence=0.95)
    extractor = Extractor(ExtractionResult(state="DONE", jargon=(jargon,)))
    maintenance = MemoryMaintenance(social, extractor, enabled=True)
    report = await maintenance.maintenance_once(access, auto_extract=True)
    assert report.candidates == 1
    entries = social.jargon_entries(access, "象站")
    assert len(entries) == 1 and entries[0].definition == "本群内网"
    assert social.jargon_entries(access, "象站", for_output=True) == []
    other = access.model_copy(update={"conversation": access.conversation.model_copy(update={"remote_conversation_id": "other"})})
    assert social.jargon_entries(other, "象站") == []
    social.revoke_source(access, "ee")
    assert social.jargon_entries(access, "象站") == []


async def test_jargon_miner_rejects_single_speaker_and_absent_term(setup):
    social, access, event, _, _ = setup
    event("e", "象站是内网")
    event("ee", "去象站")
    event("eee", "象站登录")
    jargon = JargonSuggestion(term="象站", sense_id="intranet", definition="内网",
        source_event_ids=("e", "ee", "eee"), confidence=0.95)
    extractor = Extractor(ExtractionResult(state="DONE", jargon=(jargon,
        jargon.model_copy(update={"term": "从未出现"}))))
    report = await MemoryMaintenance(social, extractor, enabled=True).maintenance_once(access, auto_extract=True)
    assert report.rejected == 2 and not social.jargon_entries(access, "象站")


async def test_query_embedding_checks_permissions_before_network_or_cache(setup):
    social, access, _, _, _ = setup
    embedding = Embedding()
    service = HistoryMemoryService(social, embedding)
    await service.query_options(access, "合法的初次查询")
    assert embedding.calls == 1
    for action in ("memory.read", "model.request"):
        service.permissions = PermissionService(policy=PermissionPolicy(rules=(PermissionRule(
            id="deny", actions=(action,), resources=("*",), effect="deny"),)))
        try:
            await service.query_options(access, "合法的初次查询")
        except PermissionError:
            pass
        else:
            raise AssertionError("denied query accessed embedding cache")
        assert embedding.calls == 1


async def test_model_facing_memory_tools_omit_vectors_and_keep_evidence(setup):
    social, access, event, record, _clock = setup
    event('e')
    memory = social.candidate(access, record(), explicit=True)
    social.commit(access, memory.memory_id, authorized=True)
    vector = (0.125,) * 512
    social.store_embedding(access, memory.memory_id, vector, 'fixture-encoder', 'fixture-version')
    stored = social.visible(access)[0]
    expected = stored.model_dump(mode='json', exclude={'embedding'})
    service = HistoryMemoryService(social)
    for operation, arguments in [('memory.search', {'query': '小陈'}),
                                 ('memory.get', {'memory_id': memory.memory_id})]:
        result = await service.query(access, operation, arguments)
        assert result['records'] == [expected]
        assert 'embedding' not in result['records'][0]
        assert result['source_ids'] == ['e']
    assert social.visible(access)[0].embedding == vector
    with social.db.engine.connect() as conn:
        saved = json.loads(conn.execute(select(social_records.c.record_json)).scalar_one())
    assert saved['embedding'] == list(vector)
