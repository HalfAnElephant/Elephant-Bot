import hashlib
import json

import pytest

from qqbot.capabilities import ToolContext, ToolGateway, standard_registry
from qqbot.harness import public_web


class DeepSeekFixture:
    def __init__(self):
        self.calls = []
        self.results = {"results": [{"title": "Weather", "url": "https://weather.example/today",
                                    "summary": "晴天", "content": "今日晴天。"}]}
        self.page = {"title": "Article", "content": "# Article\n\n正文"}

    async def search(self, query):
        self.calls.append(("search", query))
        return self.results

    async def fetch(self, url):
        self.calls.append(("fetch", url))
        return self.page


@pytest.fixture
def context(tmp_path):
    return ToolContext("task", "run", 1, {}, str(tmp_path))


async def test_search_preserves_structured_results_and_evidence(context):
    upstream, checked = DeepSeekFixture(), []

    def permission(_context, action):
        checked.append(action.resources)
        return True

    gateway = ToolGateway(standard_registry(deepseek=upstream), authority=lambda _context: True,
                          permission=permission, ledger=None)
    result = await gateway.invoke("web.search", {"query": "上海天气"}, context)
    assert upstream.calls == [("search", "上海天气")]
    assert checked == [("domain:api.deepseek.com",)]
    assert result.data["results"] == upstream.results["results"]
    assert json.loads(result.data["text"]) == upstream.results["results"]
    assert result.data["sha256"] == hashlib.sha256(result.data["text"].encode()).hexdigest()
    assert result.data["retrieved_at"] and result.data["query"] == "上海天气"
    assert result.data["url"] == "https://api.deepseek.com/v1/tools/search"


@pytest.mark.parametrize("query", ["", "x" * 501, " " * 3, "x" * 129])
async def test_search_rejects_invalid_or_overlong_queries_without_truncation(context, query):
    upstream = DeepSeekFixture()
    registry = standard_registry(deepseek=upstream)
    assert registry.describe("web.search")["parameters"]["properties"]["query"]["maxLength"] == 500
    execute = registry.entries["web.search"][1]
    with pytest.raises(ValueError, match="query"):
        await execute({"query": query}, context)
    assert not upstream.calls


async def test_search_accepts_unicode_query_at_upstream_limit(context):
    upstream = DeepSeekFixture()
    execute = standard_registry(deepseek=upstream).entries["web.search"][1]
    query = "天气" * 64
    await execute({"query": query}, context)
    assert upstream.calls == [("search", query)]


async def test_empty_search_is_a_successful_empty_observation(context):
    upstream = DeepSeekFixture()
    upstream.results = {"results": []}
    execute = standard_registry(deepseek=upstream).entries["web.search"][1]
    result = await execute({"query": "no matches"}, context)
    assert result["results"] == [] and result["text"] == "[]"


async def test_fetch_preflights_redirects_and_keeps_final_url(context, monkeypatch):
    upstream, checked = DeepSeekFixture(), []

    def fetch(url, *, authorize_url):
        authorize_url(url)
        authorize_url("https://final.example/article")
        return {"url": "https://final.example/article", "text": "<html>raw</html>",
                "retrieved_at": "2026-11-14T00:00:00+00:00", "sha256": "old"}

    def permission(_context, action):
        checked.append(action.resources[0])
        return True

    monkeypatch.setattr(public_web, "fetch_public", fetch)
    registry = standard_registry(deepseek=upstream)
    assert registry.describe("web.fetch")["timeout_seconds"] == 60
    gateway = ToolGateway(registry, authority=lambda _context: True, permission=permission, ledger=None)
    result = await gateway.invoke("web.fetch", {"url": "https://first.example/article"}, context)
    assert upstream.calls == [("fetch", "https://final.example/article")]
    assert checked == ["domain:first.example", "domain:first.example", "domain:final.example", "domain:final.example"]
    assert result.data["text"] == upstream.page["content"]
    assert result.data["title"] == upstream.page["title"]
    assert result.data["url"] == "https://final.example/article"
    assert result.data["retrieved_at"]
    assert result.data["sha256"] == hashlib.sha256(upstream.page["content"].encode()).hexdigest()


async def test_fetch_denied_redirect_never_reaches_upstream(context, monkeypatch):
    upstream = DeepSeekFixture()

    def fetch(url, *, authorize_url):
        authorize_url(url)
        authorize_url("https://denied.example/article")
        raise AssertionError("redirect authorization should fail")

    monkeypatch.setattr(public_web, "fetch_public", fetch)
    gateway = ToolGateway(standard_registry(deepseek=upstream), authority=lambda _context: True,
        permission=lambda _context, action: action.resources == ("domain:allowed.example",), ledger=None)
    with pytest.raises(PermissionError, match="denied"):
        await gateway.invoke("web.fetch", {"url": "https://allowed.example/article"}, context)
    assert not upstream.calls


async def test_fetch_rechecks_authority_after_preflight(context, monkeypatch):
    upstream, active = DeepSeekFixture(), [True]

    def fetch(url, *, authorize_url):
        authorize_url(url)
        active[0] = False
        return {"url": url, "text": "raw", "retrieved_at": "now", "sha256": "old"}

    monkeypatch.setattr(public_web, "fetch_public", fetch)
    gateway = ToolGateway(standard_registry(deepseek=upstream), authority=lambda _context: active[0],
                          permission=lambda _context, action: True, ledger=None)
    with pytest.raises(PermissionError, match="denied"):
        await gateway.invoke("web.fetch", {"url": "https://allowed.example/article"}, context)
    assert not upstream.calls


@pytest.mark.parametrize("url", ["http://example.com", "https://user:pass@example.com",
                                "https://example.com:444", "file:///etc/passwd"])
async def test_fetch_preserves_public_https_url_restrictions(context, url):
    upstream = DeepSeekFixture()
    gateway = ToolGateway(standard_registry(deepseek=upstream), authority=lambda _context: True,
                          permission=lambda _context, action: True, ledger=None)
    with pytest.raises(ValueError):
        await gateway.invoke("web.fetch", {"url": url}, context)
    assert not upstream.calls


async def test_fetch_preserves_private_address_rejection(context, monkeypatch):
    upstream = DeepSeekFixture()
    looked_up = []

    def addresses(host, port, *, timeout=None):
        looked_up.append(host)
        raise ValueError("only public unicast addresses are allowed")

    monkeypatch.setattr(public_web, "public_addresses", addresses)
    gateway = ToolGateway(standard_registry(deepseek=upstream), authority=lambda _context: True,
                          permission=lambda _context, action: True, ledger=None)
    with pytest.raises(ValueError, match="public unicast"):
        await gateway.invoke("web.fetch", {"url": "https://127.0.0.1/private"}, context)
    assert looked_up == ["127.0.0.1"] and not upstream.calls


@pytest.mark.parametrize("response", [None, [], {}, {"results": "invalid"}, {"results": ["invalid"]}])
async def test_search_rejects_malformed_upstream_data(context, response):
    upstream = DeepSeekFixture()
    upstream.results = response
    execute = standard_registry(deepseek=upstream).entries["web.search"][1]
    with pytest.raises(ValueError, match="invalid search results"):
        await execute({"query": "example"}, context)


@pytest.mark.parametrize("response", [None, [], {}, {"title": "title", "content": []}])
async def test_fetch_rejects_malformed_upstream_data(context, monkeypatch, response):
    upstream = DeepSeekFixture()
    upstream.page = response
    monkeypatch.setattr(public_web, "fetch_public", lambda *args, **kwargs: {"url": "https://example.com"})
    execute = standard_registry(deepseek=upstream).entries["web.fetch"][1]
    with pytest.raises(ValueError, match="invalid fetched page"):
        await execute({"url": "https://example.com"}, context)


async def test_registry_without_deepseek_keeps_existing_search_and_fetch(context, monkeypatch):
    requested = []

    def fetch(url, *, authorize_url=None):
        requested.append(url)
        return {"url": url, "text": "existing", "retrieved_at": "now", "sha256": "existing-hash"}

    monkeypatch.setattr(public_web, "fetch_public", fetch)
    registry = standard_registry()
    search = registry.entries["web.search"][1]
    fetch = registry.entries["web.fetch"][1]
    query = "x" * 500
    assert (await search({"query": query}, context))["text"] == "existing"
    assert (await fetch({"url": "https://example.com"}, context))["sha256"] == "existing-hash"
    assert requested == ["https://cn.bing.com/search?format=rss&q=" + query, "https://example.com"]
    assert registry.entries["web.search"][2]({"query": query}, context).resources == ("domain:cn.bing.com",)
