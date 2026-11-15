import base64
from http.client import BadStatusLine
from types import SimpleNamespace

import httpx
import pytest

from qqbot.harness import images
from qqbot.harness.gateway import RunGateway

PNG = b"\x89PNG\r\n\x1a\nfixture"
DATA = "data:image/png;base64," + base64.b64encode(PNG).decode()


@pytest.fixture
def download(monkeypatch):
    state = SimpleNamespace(urls=[], addresses=[], data=PNG, status=200, location=None, encoding="identity")

    class Connection:
        def __init__(self, host, port, addresses, *, deadline=None):
            self.host = host
            self.deadline = deadline
            self.sock = SimpleNamespace(settimeout=lambda value: None)

        def remaining(self):
            import time
            return self.deadline - time.monotonic()

        def request(self, method, path, headers):
            state.urls.append((self.host, path))

        def getresponse(self):
            remaining = bytearray(state.data)

            def read1(limit):
                chunk = remaining[:limit]
                del remaining[:limit]
                return bytes(chunk)

            return SimpleNamespace(status=state.status, read1=read1, getheader=lambda key, default=None: {
                "Location": state.location, "Content-Encoding": state.encoding,
            }.get(key, default), close=lambda: None)

        def close(self):
            pass

    def addresses(host, port, *, timeout=None):
        state.addresses.append(host)
        if host in {"localhost", "127.0.0.1"}:
            raise ValueError("private address")
        return []

    monkeypatch.setattr(images, "PinnedHTTPS", Connection)
    monkeypatch.setattr(images, "public_addresses", addresses)
    return state


def test_fetch_image_sniffs_bytes_and_upgrades_qq_http(download):
    assert images.fetch_image("http://gchat.qpic.cn/a?k=v", 1024) == DATA
    assert download.urls == [("gchat.qpic.cn", "/a?k=v")]


@pytest.mark.parametrize("url", ["file:///etc/passwd", "http://other.example/a", "https://localhost/a",
                                  "https://user:password@cdn.example/a", "http://user:password@gchat.qpic.cn/a", "https://cdn.example:444/a"])
def test_fetch_image_rejects_unsafe_sources(download, url):
    with pytest.raises(ValueError):
        images.fetch_image(url, 1024)
    assert not download.urls


def test_fetch_image_revalidates_redirect(download):
    download.status, download.location = 302, "https://127.0.0.1/private"
    with pytest.raises(ValueError):
        images.fetch_image("https://cdn.example/a", 1024)
    assert len(download.urls) == 1


@pytest.mark.parametrize("data,limit", [(b"<html>not an image</html>", 1024), (PNG, 2)])
def test_fetch_image_bounds_size_and_format(download, data, limit):
    download.data = data
    with pytest.raises(ValueError):
        images.fetch_image("https://cdn.example/a", limit)


@pytest.fixture
def gateway(tmp_path):
    state = SimpleNamespace(requests=[], allowed=True)

    async def core(operation, payload):
        if operation == "authorize":
            if not state.allowed:
                raise PermissionError("revoked")
            return {"allowed": True}
        return {"accepted": True, "remaining": 10}

    def upstream(request):
        import json
        state.requests.append(json.loads(request.content))
        return httpx.Response(200, json={"choices": [{"message": {"content": "seen"}}]})

    state.gateway = RunGateway(tmp_path, "offline", "", "https://model.invalid", call=core,
                               transport=httpx.MockTransport(upstream))
    state.claim = {"task_id": "task", "run_id": "run", "fencing_token": 1, "goal": "看图",
                   "permission_snapshot": {"principal": {}}, "context": {"images": [
                       {"source": "e", "message_id": "42", "url": "https://cdn.example/image"}]}}
    state.gateway.bind(state.claim)
    return state


async def test_gateway_images_reach_provider_cached_and_rotate(gateway, monkeypatch):
    state, loads = gateway, []
    monkeypatch.setattr("qqbot.harness.gateway.fetch_image", lambda url, limit: loads.append(url) or DATA)
    g = state.gateway
    body = {"model": "offline", "messages": [{"role": "system", "content": "trusted"},
            {"role": "user", "content": "look"}, {"role": "assistant", "tool_calls": [{"id": "tool"}]},
            {"role": "tool", "tool_call_id": "tool", "content": "observed"}]}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=g.app), base_url="http://gateway") as client:
        for _ in range(2):
            response = await client.post("/v1/chat/completions", json=body,
                                         headers={"Authorization": "Bearer " + g.model_token})
            assert response.status_code == 200, response.text
        assert len(loads) == 1
        messages = state.requests[-1]["messages"]
        assert messages[0] == body["messages"][0]
        assert messages[1]["content"][-1] == {"type": "image_url", "image_url": {"url": DATA}}
        assert messages[-2:] == body["messages"][-2:]
        assert "https://cdn.example" not in str(messages)
        g.revoke()
        g.bind({**state.claim, "run_id": "next", "context": {}})
        response = await client.post("/v1/chat/completions", json=body,
                                     headers={"Authorization": "Bearer " + g.model_token})
        assert response.status_code == 200
        assert state.requests[-1]["messages"] == body["messages"]


@pytest.mark.parametrize("error", [ValueError("expired"), BadStatusLine("invalid")])
async def test_gateway_failure_is_explicit_and_authorization_precedes_download(gateway, monkeypatch, error):
    loads = []

    def failed(url, limit):
        loads.append(url)
        raise error

    monkeypatch.setattr("qqbot.harness.gateway.fetch_image", failed)
    body = {"messages": [{"role": "user", "content": "look"}]}
    result = await gateway.gateway.with_images(body)
    assert "未能读取" in str(result)
    assert "image_url" not in str(result)
    gateway.allowed = False
    with pytest.raises(PermissionError):
        await gateway.gateway.with_images(body)
    assert len(loads) == 1


async def test_cached_images_yield_to_growing_tool_history(gateway, monkeypatch):
    g = gateway.gateway
    g.provider.max_bytes = 20000
    data = "data:image/png;base64," + "A" * 9000
    monkeypatch.setattr("qqbot.harness.gateway.fetch_image", lambda url, limit: data)
    first = await g.with_images({"messages": [{"role": "user", "content": "look"}]})
    assert "image_url" in str(first)
    later = await g.with_images({"messages": [{"role": "user", "content": "x" * 15000}]})
    assert "image_url" not in str(later)
    assert "未提交" in str(later)
    assert "image_url" in str(g.image_content)


@pytest.mark.parametrize("messages", [None, ["invalid"], [{"role": ["invalid"]}],
                                      [{"role": "tool", "tool_call_id": "orphan"}]])
async def test_invalid_image_request_rejected_before_download(gateway, monkeypatch, messages):
    def unexpected(*args):
        raise AssertionError("invalid requests must not download images")
    monkeypatch.setattr("qqbot.harness.gateway.fetch_image", unexpected)
    g = gateway.gateway
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=g.app), base_url="http://gateway") as client:
        response = await client.post("/v1/chat/completions", json={"model": "offline", "messages": messages},
                                     headers={"Authorization": "Bearer " + g.model_token})
    assert response.status_code == 422
    assert not gateway.requests


async def test_original_files_reach_workspaces_cached_across_worker_handoff(gateway, monkeypatch, tmp_path):
    loads = []
    def load(url, limit):
        loads.append(url)
        return b'ORIGINAL_FILE_BYTES'
    monkeypatch.setattr('qqbot.harness.gateway.fetch_public_bytes', load)
    claim = {**gateway.claim, 'context': {'scope': 'conversation', 'workspace_id': 'alpha', 'files': [
        {'source': 'original-event', 'message_id': '43', 'attachment_ref': 'file-ref',
         'filename': 'slides.pptx', 'url': 'https://cdn.example/file'}]}}
    gateway.gateway.revoke()
    gateway.gateway.bind(claim)
    body = {'messages': [{'role': 'system', 'content': 'trusted'}, {'role': 'user', 'content': 'Read file'}]}
    first = await gateway.gateway.with_attachments(body)
    second = await gateway.gateway.with_attachments(body)
    assert loads == ['https://cdn.example/file']
    assert first == second and first['messages'][0] == body['messages'][0]
    assert 'original-event' in str(first) and 'slides.pptx' in str(first)
    assert 'ORIGINAL_FILE_BYTES' not in str(first)
    assert 'https://cdn.example/file' not in str(first)
    original = list(tmp_path.glob('attachment-*'))
    assert len(original) == 1 and original[0].read_bytes() == b'ORIGINAL_FILE_BYTES'
    # A new Worker workspace reuses the authorized original after its URL expires.
    work = tmp_path / 'worker'
    work.mkdir()
    gateway.gateway.revoke()
    gateway.gateway.workspace = work
    gateway.gateway.bind({**claim, 'task_id': 'worker', 'run_id': 'worker-run'})
    def expired(*args):
        raise AssertionError('cached original must survive an expired URL')
    monkeypatch.setattr('qqbot.harness.gateway.fetch_public_bytes', expired)
    await gateway.gateway.with_attachments(body)
    assert len(list(work.glob('attachment-*'))) == 1
    gateway.allowed = False
    with pytest.raises(PermissionError):
        await gateway.gateway.with_attachments(body)


async def test_document_download_failure_is_explicit(gateway, monkeypatch):
    def failed(*args):
        raise ValueError('expired')
    monkeypatch.setattr('qqbot.harness.gateway.fetch_public_bytes', failed)
    gateway.gateway.revoke()
    gateway.gateway.bind({**gateway.claim, 'context': {'files': [
        {'source': 'e', 'message_id': '1', 'filename': 'original.pdf', 'url': 'https://cdn.example/file'}]}})
    body = {'messages': [{'role': 'user', 'content': 'Read'}]}
    result = await gateway.gateway.with_attachments(body)
    assert '未能保存' in str(result) and '不得猜测' in str(result)
