import hashlib
import json
from types import SimpleNamespace

import httpx
import pytest

from qqbot.adapters.qq_official.local_upload import LocalUploadError, upload_local
from qqbot.domain import DeliveryCheck, Surface


@pytest.fixture
def target():
    return SimpleNamespace(target=SimpleNamespace(surface=Surface.PRIVATE, remote_conversation_id="openid"))


def setup(handler, *, allowed=lambda: True):
    async def validate(envelope):
        return DeliveryCheck(allowed=allowed(), reason="REPLY_EXPIRED_OR_EXHAUSTED")
    async def token():
        return "secret-token"
    return SimpleNamespace(config=SimpleNamespace(api_base="https://api.sgroup.qq.com", app_id="123"),
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)), validate_delivery=validate,
        access_token=token, token="secret-token")


def plan(parts=None):
    return {"upload_id": "upload", "block_size": 3, "parts": parts or [
        {"index": 1, "presigned_url": "https://bucket-123.cos.ap-guangzhou.myqcloud.com/p1?signature=secret"},
        {"index": 2, "presigned_url": "https://bucket-123.cos.ap-guangzhou.tencentcos.cn/p2?signature=secret"}]}


@pytest.mark.parametrize("surface,scope", [(Surface.PRIVATE, "users"), (Surface.GROUP, "groups")])
async def test_chunked_upload_exact_protocol_no_auto_send(target, surface, scope):
    target.target.surface = surface
    posts, puts = [], []
    def api(request):
        body = json.loads(request.content); posts.append((request.url.path, body))
        assert request.headers["authorization"] == "QQBot secret-token"
        assert request.url.path.startswith(f"/v2/{scope}/openid/")
        if request.url.path.endswith("upload_prepare"):
            return httpx.Response(200, json=plan())
        return httpx.Response(200, json={"file_info": "media-token"} if request.url.path.endswith("files") else {})
    def cos(request):
        assert "authorization" not in request.headers and "cookie" not in request.headers
        puts.append(request.content)
        return httpx.Response(200)
    adapter = setup(api)
    async with httpx.AsyncClient(transport=httpx.MockTransport(cos), headers={"Authorization": "must-not-forward"},
                                 cookies={"secret": "cookie"}) as cos_client:
        info = await upload_local(adapter, target, b"abcdef", "file.txt", "file", "text/plain", cos_client=cos_client)
    assert info == "media-token" and puts == [b"abc", b"def"]
    assert posts[0][1] == {"file_type": 4, "file_name": "file.txt", "file_size": 6,
        "md5": hashlib.md5(b"abcdef").hexdigest(), "sha1": hashlib.sha1(b"abcdef").hexdigest(),
        "md5_10m": hashlib.md5(b"abcdef").hexdigest()}
    assert posts[-1][1] == {"upload_id": "upload"}
    assert posts[1][1] == {"upload_id": "upload", "part_index": 1, "block_size": 3,
                            "md5": hashlib.md5(b"abc").hexdigest()}
    assert all("srv_send_msg" not in body and "file_data" not in body for _, body in posts)
    await adapter.client.aclose()


@pytest.mark.parametrize("url", ["http://bucket-1.cos.ap-a.myqcloud.com/p", "https://evil.com/p",
    "https://myqcloud.com.evil.com/p", "https://127.0.0.1/p", "https://bucket-1.cos.ap-a.myqcloud.com:444/p",
    "https://user:secret@bucket-1.cos.ap-a.myqcloud.com/p", "https://bucket-1.cos.ap-a.myqcloud.com/p#x"])
async def test_every_presigned_host_validated_before_any_transfer(target, url):
    value = plan(); value["parts"][1]["presigned_url"] = url
    adapter = setup(lambda request: httpx.Response(200, json=value))
    def cos(request):
        pytest.fail("no bytes must leave when any plan URL is unsafe")
    async with httpx.AsyncClient(transport=httpx.MockTransport(cos)) as cos_client:
        with pytest.raises(LocalUploadError, match="LOCAL_UPLOAD_UNSAFE_HOST") as exc:
            await upload_local(adapter, target, b"abcdef", "a.txt", "file", "text/plain", cos_client=cos_client)
        assert "secret" not in str(exc.value)
    await adapter.client.aclose()


@pytest.mark.parametrize("value", [
    {"upload_id": "u", "block_size": 3, "parts": []},
    {"upload_id": "u", "block_size": True, "parts": []},
    plan([{"index": 1, "presigned_url": "https://bucket-1.cos.ap-a.myqcloud.com/p"}] * 2),
    {"upload_id": "u", "block_size": 1, "parts": [{}] * 65},
])
async def test_malformed_upload_plan_rejected(target, value):
    adapter = setup(lambda request: httpx.Response(200, json=value))
    with pytest.raises(LocalUploadError, match="INVALID_PLAN"):
        await upload_local(adapter, target, b"abcdef", "a.txt", "file", "text/plain")
    await adapter.client.aclose()


async def test_revoke_after_part_stops_finish_and_completion(target):
    enabled = True
    posts = []
    def api(request):
        posts.append(request.url.path)
        return httpx.Response(200, json=plan())
    def cos(request):
        nonlocal enabled
        enabled = False
        return httpx.Response(200)
    adapter = setup(api)
    async with httpx.AsyncClient(transport=httpx.MockTransport(cos)) as cos_client:
        with pytest.raises(LocalUploadError, match="AUTHORIZATION_CHANGED"):
            await upload_local(adapter, target, b"abcdef", "a.txt", "file", "text/plain", lambda: enabled,
                               cos_client=cos_client)
    assert len(posts) == 1
    await adapter.client.aclose()


async def test_expiry_after_token_blocks_prepare(target):
    enabled = True
    def api(request):
        pytest.fail("expired context cannot upload")
    adapter = setup(api, allowed=lambda: enabled)
    async def token():
        nonlocal enabled
        enabled = False
        return "secret"
    adapter.access_token = token
    with pytest.raises(LocalUploadError, match="REPLY_EXPIRED_OR_EXHAUSTED"):
        await upload_local(adapter, target, b"abcdef", "a.txt", "file", "text/plain")
    await adapter.client.aclose()


@pytest.mark.parametrize("status,code", [(403, 40093002), (403, 40093001), (401, 1)])
async def test_business_error_sanitized(target, status, code):
    adapter = setup(lambda request: httpx.Response(status, json={"code": code, "message": "secret"}))
    with pytest.raises(LocalUploadError) as exc:
        await upload_local(adapter, target, b"abcdef", "a.txt", "file", "text/plain")
    assert "secret" not in str(exc.value)
    assert exc.value.retryable == (code != 40093002)
    await adapter.client.aclose()


async def test_redirect_from_cos_never_followed(target):
    posts = []
    def api(request):
        posts.append(request.url.path)
        return httpx.Response(200, json=plan())
    def cos(request):
        assert request.url.host.endswith("myqcloud.com")
        return httpx.Response(307, headers={"location": "https://evil.com/capture"})
    adapter = setup(api)
    async with httpx.AsyncClient(transport=httpx.MockTransport(cos), follow_redirects=True) as cos_client:
        with pytest.raises(LocalUploadError, match="PART_REJECTED"):
            await upload_local(adapter, target, b"abcdef", "a.txt", "file", "text/plain", cos_client=cos_client)
    assert len(posts) == 1
    await adapter.client.aclose()


async def test_cancellation_propagates_and_no_completion(target):
    import asyncio
    posts = []
    def api(request):
        posts.append(request.url.path)
        return httpx.Response(200, json=plan())
    async def cos(request):
        raise asyncio.CancelledError
    adapter = setup(api)
    async with httpx.AsyncClient(transport=httpx.MockTransport(cos)) as cos_client:
        with pytest.raises(asyncio.CancelledError):
            await upload_local(adapter, target, b"abcdef", "a.txt", "file", "text/plain", cos_client=cos_client)
    assert len(posts) == 1
    await adapter.client.aclose()


@pytest.mark.parametrize("payload,filename", [(b"", "a.txt"), (SimpleNamespace(seek=lambda *args: None, tell=lambda: 100 * 1024 * 1024 + 1), "a.txt"),
                                             (b"x", "../secret"), (b"x", "secret\nfile")])
async def test_bounds_before_network(target, payload, filename):
    def api(request):
        pytest.fail("invalid local file must not reach network")
    adapter = setup(api)
    with pytest.raises(LocalUploadError):
        await upload_local(adapter, target, payload, filename, "file", "text/plain")
    await adapter.client.aclose()


async def test_large_response_rejected_without_echo(target):
    adapter = setup(lambda request: httpx.Response(200, content=b"secret" * 20000))
    with pytest.raises(LocalUploadError, match="INVALID_RESPONSE") as exc:
        await upload_local(adapter, target, b"abc", "a.txt", "file", "text/plain")
    assert "secret" not in str(exc.value)
    await adapter.client.aclose()


async def test_server_block_larger_than_small_file_does_not_allocate_block(target):
    blocks, finish = [], []
    def api(request):
        if request.url.path.endswith("upload_prepare"):
            return httpx.Response(200, json={"upload_id": "big-block", "block_size": 64 * 1024 * 1024,
                "parts": [{"index": 1, "block_size": 64 * 1024 * 1024,
                           "presigned_url": "https://bucket-123.cos.ap-guangzhou.myqcloud.com/p"}]})
        if request.url.path.endswith("upload_part_finish"):
            finish.append(json.loads(request.content))
        return httpx.Response(200, json={"file_info": "info"} if request.url.path.endswith("files") else {})
    def cos(request):
        blocks.append(request.content)
        assert request.headers["content-length"] == "3"
        return httpx.Response(200)
    adapter = setup(api)
    async with httpx.AsyncClient(transport=httpx.MockTransport(cos)) as cos_client:
        assert await upload_local(adapter, target, b"abc", "a.txt", "file", "text/plain", cos_client=cos_client) == "info"
    assert blocks == [b"abc"] and finish[0]["block_size"] == 3
    await adapter.client.aclose()


@pytest.mark.parametrize("kind,limit", [("image", 31457280), ("audio", 20971520),
                                       ("video", 104857600), ("file", 104857600)])
def test_verified_official_limit_boundaries(kind, limit):
    from qqbot.adapters.qq_official.local_limits import limit_for, validate_size
    assert limit_for(kind) == limit
    validate_size(limit, kind)
    validate_size(limit - 1, kind)
    with pytest.raises(ValueError): validate_size(limit + 1, kind)
    with pytest.raises(ValueError): validate_size(0, kind)


async def test_official_max_file_streams_in_bounded_reads(target):
    from qqbot.adapters.qq_official.local_limits import MAX_FILE_BYTES
    from qqbot.adapters.qq_official.local_upload import IO_CHUNK
    class Source:
        offset = 0
        max_read = 0
        def seek(self, offset, whence=0):
            self.offset = MAX_FILE_BYTES + offset if whence == 2 else offset
        def tell(self): return self.offset
        def read(self, size):
            assert 0 < size <= IO_CHUNK
            self.max_read = max(self.max_read, size)
            count = min(size, MAX_FILE_BYTES - self.offset)
            self.offset += count
            return b"x" * count
    source = Source()
    sent = 0
    class Transport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            nonlocal sent
            assert int(request.headers["content-length"]) == MAX_FILE_BYTES
            async for chunk in request.stream:
                assert len(chunk) <= IO_CHUNK
                sent += len(chunk)
            return httpx.Response(200)
    def api(request):
        if request.url.path.endswith("upload_prepare"):
            assert json.loads(request.content)["file_size"] == MAX_FILE_BYTES
            return httpx.Response(200, json={"upload_id": "large", "block_size": MAX_FILE_BYTES,
                "parts": [{"index": 1, "presigned_url": "https://bucket-123.cos.ap-guangzhou.myqcloud.com/a"}]})
        return httpx.Response(200, json={"file_info": "info"} if request.url.path.endswith("files") else {})
    adapter = setup(api)
    async with httpx.AsyncClient(transport=Transport()) as cos:
        assert await upload_local(adapter, target, source, "large.txt", "file", "text/plain", cos_client=cos) == "info"
    assert sent == MAX_FILE_BYTES and source.max_read == IO_CHUNK
    await adapter.client.aclose()


@pytest.mark.parametrize("fail_read", [False, True])
async def test_cancel_waits_for_inflight_file_read_before_caller_closes(target, fail_read):
    import asyncio
    import io
    import threading
    entered, release, completed = threading.Event(), threading.Event(), threading.Event()
    class SlowSource(io.BytesIO):
        def read(self, length=-1):
            entered.set()
            assert release.wait(5), "test failed to release read"
            assert not self.closed
            try:
                if fail_read:
                    raise OSError("read failed while cancelled")
                return super().read(length)
            finally:
                completed.set()
    source = SlowSource(b"safe")
    calls = []
    def api(request):
        calls.append(request)
        pytest.fail("cancel during initial hash must not upload")
    adapter = setup(api)
    async def run():
        try:
            await upload_local(adapter, target, source, "a.txt", "file", "text/plain")
        finally:
            assert completed.is_set(), "caller closed source while read still running"
            source.close()
    task = asyncio.create_task(run())
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        await asyncio.sleep(.01)
        task.cancel()  # Repeated cancellation must not bypass descriptor ownership.
        await asyncio.sleep(.01)
        assert not task.done() and not source.closed
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert source.closed and not calls
    finally:
        release.set()
        if not task.done():
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        await adapter.client.aclose()
