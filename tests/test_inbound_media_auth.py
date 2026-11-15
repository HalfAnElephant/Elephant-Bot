"""QQ private-CDN credentials stay bound to an authorized durable attachment."""

import copy
from types import SimpleNamespace
from urllib.parse import urlsplit

import httpx
import pytest

from qqbot.adapters.qq_official.media_download import inbound_media_headers
from qqbot.harness import images
from qqbot.permissions import PermissionRule

CDN = "multimedia.nt.qq.com.cn"
URL = "https://" + CDN + "/download?attachment=signed"


@pytest.fixture
async def accepted_media(request, tmp_path, official_config, official_event, clock):
    from conftest import official_payload
    from test_leader_core import make_core

    kind = getattr(request, "param", "audio")
    core = make_core(tmp_path, official_config, clock)
    adapter = core.adapters["qq"]
    source = adapter.normalize(official_payload(official_event, private=True, text="读取附件",
        attachments=[{"url": URL, "content_type": "voice" if kind == "audio" else "application/pdf",
            "filename": "voice.silk" if kind == "audio" else "report.pdf"}]))
    source, _ = core.inbox.accept(source)
    context = core.context_for(source)
    attachment, = context["files"]
    claim = {"context": context, "permission_snapshot": {"principal": source.principal.model_dump(mode="json")},
        "event_json": source.model_dump_json()}
    state = SimpleNamespace(core=core, adapter=adapter, source=source, attachment=attachment, claim=claim,
        token_requests=[], revoke_after_token=False)

    def revoke_source():
        core.permissions.upsert(PermissionRule(id="deny-media", effect="deny", actions=("history.source.read",),
            resources=("event:" + source.event_id,)), expected_version=core.permissions.policy_version, actor="test")

    async def token(request):
        assert str(request.url) == official_config.token_url
        state.token_requests.append(request)
        if state.revoke_after_token:
            revoke_source()
        return httpx.Response(200, json={"access_token": "host-only-media-token", "expires_in": 7200})

    state.revoke_source = revoke_source
    await adapter.client.aclose()
    adapter.client = httpx.AsyncClient(transport=httpx.MockTransport(token))
    try:
        yield state
    finally:
        await core.stop()
        core.db.close()


@pytest.mark.parametrize("accepted_media", ["audio", "file"], indirect=True)
async def test_private_cdn_uses_its_real_official_adapter_token(accepted_media):
    state = accepted_media
    headers = await inbound_media_headers(state.core, state.claim, state.attachment)
    assert headers == {"Authorization": "QQBot host-only-media-token"}
    assert len(state.token_requests) == 1
    assert "host-only-media-token" not in str(state.claim)
    assert "host-only-media-token" not in str(state.attachment)


async def test_worker_claim_without_event_json_uses_frozen_identity_and_scope(accepted_media):
    state = accepted_media
    claim = {key: value for key, value in state.claim.items() if key != "event_json"}
    assert await inbound_media_headers(state.core, claim, state.attachment) == {
        "Authorization": "QQBot host-only-media-token"}


async def test_cdn_credentials_are_not_issued_for_other_media_kinds(accepted_media):
    state = accepted_media
    segment = next(item for item in state.source.segments if item.type == "audio")
    source = state.source.model_copy(update={"event_id": "image-source", "platform_message_id": "image-message",
        "segments": (segment.model_copy(update={"type": "image"}),)})
    source, _ = state.core.inbox.accept(source)
    claim = {"context": state.core.context_for(source), "permission_snapshot": state.claim["permission_snapshot"],
        "event_json": source.model_dump_json()}
    attachment = {"source": source.event_id, "url": URL, "attachment_ref": segment.attachment_ref}
    with pytest.raises(PermissionError, match="accepted attachment"):
        await inbound_media_headers(state.core, claim, attachment)
    assert not state.token_requests


@pytest.mark.parametrize("url", [
    "https://public.example/file", "https://multimedia.nt.qq.com.cn.evil.example/file",
    "http://multimedia.nt.qq.com.cn:8080/file", "https://multimedia.nt.qq.com.cn:444/file",
    "https://attacker@multimedia.nt.qq.com.cn/file", "https://multimedia.nt.qq.com.cn@evil.example/file",
    "https://multimedia.nt.qq.com.cn:invalid/file", "https://[broken", None,
])
async def test_untrusted_urls_never_access_core_or_token(url):
    assert await inbound_media_headers(SimpleNamespace(), {}, {"url": url}) == {}


@pytest.mark.parametrize("changed", [
    {"source": "missing-source"}, {"url": URL + "&other=true"}, {"attachment_ref": "other-attachment"},
])
async def test_private_cdn_requires_the_exact_durable_attachment(accepted_media, changed):
    state = accepted_media
    with pytest.raises(PermissionError):
        await inbound_media_headers(state.core, state.claim, {**state.attachment, **changed})
    assert not state.token_requests


@pytest.mark.parametrize("changed", ["principal", "conversation", "origin", "manifest", "missing_manifest"])
async def test_source_identity_and_context_proof_are_checked_before_token(accepted_media, changed):
    state = accepted_media
    claim = copy.deepcopy(state.claim)
    if changed == "principal":
        claim["permission_snapshot"]["principal"]["remote_user_id"] = "another-user"
    elif changed == "conversation":
        claim["context"]["scope"] = "another-conversation"
    elif changed == "origin":
        claim["event_json"] = state.source.model_copy(update={"principal":
            state.source.principal.model_copy(update={"remote_user_id": "another-user"})}).model_dump_json()
    elif changed == "manifest":
        claim["context"]["source_manifest"][0]["digest"] = "0" * 64
    else:
        claim["context"]["source_manifest"] = []
    with pytest.raises(PermissionError):
        await inbound_media_headers(state.core, claim, state.attachment)
    assert not state.token_requests


async def test_recalled_source_cannot_obtain_cdn_credentials(accepted_media):
    state = accepted_media
    state.core.inbox.accept(state.source.model_copy(update={"event_id": "recall-media", "event_type": "message.recalled"}))
    with pytest.raises(PermissionError, match="revoked"):
        await inbound_media_headers(state.core, state.claim, state.attachment)
    assert not state.token_requests


@pytest.mark.parametrize("during_token", [False, True])
async def test_source_permission_is_rechecked_after_token_wait(accepted_media, during_token):
    state = accepted_media
    state.revoke_after_token = during_token
    if not during_token:
        state.revoke_source()
    with pytest.raises(PermissionError, match="revoked"):
        await inbound_media_headers(state.core, state.claim, state.attachment)
    assert len(state.token_requests) == int(during_token)


def mock_cdn(monkeypatch, *, redirect=None):
    events, requests = [], []

    def addresses(host, port, *, timeout=None):
        events.append(("dns", host))
        return [(2, 1, 6, "", ("8.8.8.8", port))]

    class Connection:
        def __init__(self, host, port, addresses, *, deadline=None):
            self.host = host
            self.sock = SimpleNamespace(settimeout=lambda _: None)

        def remaining(self):
            return 10

        def request(self, method, path, headers):
            events.append(("request", self.host))
            requests.append((self.host, dict(headers)))

        def getresponse(self):
            payload = bytearray(b"audio original")

            def read1(limit):
                chunk = bytes(payload[:limit])
                del payload[:limit]
                return chunk

            return SimpleNamespace(status=302 if redirect and self.host == CDN else 200,
                getheader=lambda name, default=None: redirect if name == "Location" else default,
                read1=read1, close=lambda: None)

        def close(self):
            pass

    monkeypatch.setattr(images, "public_addresses", addresses)
    monkeypatch.setattr(images, "PinnedHTTPS", Connection)
    return events, requests


@pytest.mark.parametrize("redirect", [None, "https://other.example/audio"])
async def test_download_headers_are_selected_after_each_hops_dns_without_redirect_leaks(
        accepted_media, monkeypatch, redirect):
    state = accepted_media
    credentials = await inbound_media_headers(state.core, state.claim, state.attachment)
    events, requests = mock_cdn(monkeypatch, redirect=redirect)

    def headers_for_url(url):
        host = urlsplit(url).hostname
        events.append(("headers", host))
        return credentials if host == CDN else {}

    assert images.fetch_public_bytes(URL, 1024, headers_for_url=headers_for_url) == b"audio original"
    assert requests[0][1]["Authorization"] == "QQBot host-only-media-token"
    assert events[:3] == [("dns", CDN), ("headers", CDN), ("request", CDN)]
    if redirect:
        assert "Authorization" not in requests[1][1]
        assert events[3:] == [("dns", "other.example"), ("headers", "other.example"), ("request", "other.example")]
    else:
        assert len(requests) == 1


async def test_official_http_cdn_source_is_upgraded_before_credentials_are_sent(accepted_media, monkeypatch):
    state = accepted_media
    url = URL.replace("https:", "http:")
    source = state.source.model_copy(update={"event_id": "http-media", "platform_message_id": "http-message",
        "segments": tuple(segment.model_copy(update={"source_url": url}) if segment.type == "audio" else segment
                          for segment in state.source.segments)})
    source, _ = state.core.inbox.accept(source)
    context = state.core.context_for(source)
    attachment = next(item for item in context["files"] if item["source"] == source.event_id)
    claim = {**state.claim, "context": context, "event_json": source.model_dump_json()}
    headers = await inbound_media_headers(state.core, claim, attachment)
    _events, requests = mock_cdn(monkeypatch)

    def protected_headers(target):
        assert target.startswith("https://" + CDN)
        return headers

    assert images.fetch_public_bytes(url, 1024, headers_for_url=protected_headers) == b"audio original"
    assert requests[0][1]["Authorization"] == "QQBot host-only-media-token"


def test_credentials_are_not_requested_before_url_and_dns_validation(monkeypatch):
    def rejected_dns(*args, **kwargs):
        raise ValueError("private address rejected")

    monkeypatch.setattr(images, "public_addresses", rejected_dns)
    for url in ("https://127.0.0.1/audio", "https://user@multimedia.nt.qq.com.cn/audio"):
        with pytest.raises(ValueError):
            images.fetch_public_bytes(url, 1024, headers_for_url=lambda _: pytest.fail("credentials requested early"))


def test_default_download_contract_sends_no_authorization(monkeypatch):
    _events, requests = mock_cdn(monkeypatch)
    assert images.fetch_public_bytes(URL, 1024) == b"audio original"
    assert "Authorization" not in requests[0][1]
