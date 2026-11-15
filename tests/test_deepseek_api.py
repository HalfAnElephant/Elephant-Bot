import json

import httpx
import pytest

from qqbot.adapters.deepseek import DeepSeekAPI, DeepSeekError


@pytest.mark.parametrize("endpoint", [
    "https://api.deepseek.com/chat/completions", "https://api.deepseek.com:443/v1/chat/completions",
])
def test_official_model_reuses_its_credential(endpoint):
    api = DeepSeekAPI.from_model(endpoint, "dedicated-key")
    assert api.key == "dedicated-key" and len(api.account) == 64
    assert DeepSeekAPI.from_model(endpoint, "dedicated-key").account == api.account
    assert DeepSeekAPI.from_model(endpoint, "") is None


@pytest.mark.parametrize("endpoint", [
    None, "https://model.example/chat/completions", "http://api.deepseek.com/chat/completions",
    "https://api.deepseek.com.evil/chat/completions", "https://someone@api.deepseek.com/chat/completions",
    "https://api.deepseek.com:8080/chat/completions", "https://[broken", "https://api.deepseek.com:bad",
])
def test_other_endpoints_never_receive_deepseek_tools(endpoint):
    assert DeepSeekAPI.from_model(endpoint, "dedicated-key") is None


async def test_tool_endpoints_and_upload_contract():
    seen = []
    async def receive(request):
        seen.append(request)
        assert request.headers["authorization"] == "Bearer dedicated-key"
        if request.url.path == "/v1/tools/asr":
            return httpx.Response(200, json={"text": "hello", "duration_ms": 100, "request_id": "req"})
        return httpx.Response(200, json={"results": [], "id": "file-api-fixture"})
    api = DeepSeekAPI("dedicated-key", transport=httpx.MockTransport(receive))
    await api.search("weather")
    await api.fetch("https://example.com")
    await api.upload(b"original", "report.pdf", "text_recognition")
    await api.files("text_recognition", "file-api-previous")
    assert (await api.transcribe(b"wave", "voice.wav"))["text"] == "hello"
    assert json.loads(seen[0].content) == {"query": "weather", "need_content": True}
    assert json.loads(seen[1].content) == {"url": "https://example.com"}
    multipart = seen[2].content
    assert multipart.index(b'name="purpose"') < multipart.index(b'name="file"')
    assert b"text_recognition" in multipart and b"2592000" in multipart and b"original" in multipart
    assert b"Content-Type: application/pdf" in multipart
    assert seen[2].extensions["timeout"]["read"] == 260
    assert seen[3].url.params["after"] == "file-api-previous"
    assert b'filename="voice.wav"' in seen[4].content


@pytest.mark.parametrize("response", [
    httpx.Response(302, headers={"location": "https://untrusted.example"}),
    httpx.Response(429, text="sensitive upstream error"),
    httpx.Response(200, content=b"broken JSON"), httpx.Response(200, json=[]),
    httpx.Response(200, json={"error": "sensitive upstream error"}),
    httpx.Response(200, content=b"x" * (2 * 1024 * 1024 + 1)),
])
@pytest.mark.parametrize("operation", ["search", "ocr"])
async def test_errors_are_bounded_sanitized_and_never_retried(response, operation):
    calls = []
    def receive(request):
        calls.append(request)
        return response
    api = DeepSeekAPI("dedicated-key", transport=httpx.MockTransport(receive))
    with pytest.raises(DeepSeekError) as error:
        if operation == "ocr":
            await api.ocr(b"original", "report.pdf")
        else:
            await api.search("weather")
    assert len(calls) == 1
    assert "sensitive" not in str(error.value) and "dedicated-key" not in str(error.value)


async def test_connection_failure_does_not_expose_request_or_credentials():
    def receive(request):
        raise httpx.ReadTimeout("dedicated-key in a bad exception", request=request)
    api = DeepSeekAPI("dedicated-key", transport=httpx.MockTransport(receive))
    with pytest.raises(DeepSeekError, match="connection failed") as error:
        await api.fetch("https://example.com")
    assert error.value.status_code is None and "dedicated-key" not in str(error.value)


@pytest.mark.parametrize("result", [
    {"duration_ms": 100}, {"text": 12, "duration_ms": 100},
    {"text": "x" * 10001, "duration_ms": 100}, {"text": "x", "duration_ms": True},
    {"text": "x", "duration_ms": 120001},
])
async def test_asr_response_validation(result):
    api = DeepSeekAPI("key", transport=httpx.MockTransport(lambda _: httpx.Response(200, json=result)))
    with pytest.raises(DeepSeekError, match="transcript"):
        await api.transcribe(b"wave", "voice.wav")


@pytest.mark.parametrize("filename,mime_type", [
    ("report.pdf", "application/pdf"), ("scan.png", "image/png"),
])
@pytest.mark.parametrize("text,finish_reason", [("recognized text", "stop"), ("", "stop"), ("partial", "length")])
async def test_ocr_contract_and_normalized_pages(filename, mime_type, text, finish_reason):
    seen = []
    def receive(request):
        seen.append(request)
        return httpx.Response(200, json={"id": "upstream-id", "purpose": "text_recognition", "page_count": 1,
            "pages": [{"index": 0, "text": text, "finish_reason": finish_reason, "untrusted_extra": "discard"}]})
    api = DeepSeekAPI("dedicated-key", transport=httpx.MockTransport(receive))
    assert await api.ocr(b"original", filename) == {
        "purpose": "text_recognition", "page_count": 1,
        "pages": [{"index": 0, "text": text, "finish_reason": finish_reason}]}
    assert len(seen) == 1
    request = seen[0]
    assert request.method == "POST" and request.url.path == "/v1/tools/ocr"
    assert request.headers["authorization"] == "Bearer dedicated-key"
    assert request.content.index(b'name="purpose"') < request.content.index(b'name="file"')
    assert b"text_recognition" in request.content and b"original" in request.content
    assert ("Content-Type: " + mime_type).encode() in request.content
    assert request.extensions["timeout"]["read"] == 220


@pytest.mark.parametrize("changes", [
    {"purpose": "rich_layout_recognition"}, {"purpose": None},
    {"page_count": True}, {"page_count": 1.0}, {"page_count": 0}, {"page_count": 1001},
    {"page_count": 2}, {"pages": None}, {"pages": {}}, {"pages": []},
    {"pages": [None]}, {"pages": [{"text": "text", "finish_reason": "stop"}]},
    {"pages": [{"index": True, "text": "text", "finish_reason": "stop"}]},
    {"pages": [{"index": 0.0, "text": "text", "finish_reason": "stop"}]},
    {"pages": [{"index": 1, "text": "text", "finish_reason": "stop"}]},
    {"pages": [{"index": 0, "text": 1, "finish_reason": "stop"}]},
    {"pages": [{"index": 0, "finish_reason": "stop"}]},
    {"pages": [{"index": 0, "text": "text", "finish_reason": "error"}]},
    {"pages": [{"index": 0, "text": "text"}]},
    {"page_count": 2, "pages": [
        {"index": 0, "text": "one", "finish_reason": "stop"},
        {"index": 0, "text": "two", "finish_reason": "stop"}]},
])
async def test_ocr_response_validation(changes):
    result = {"purpose": "text_recognition", "page_count": 1,
        "pages": [{"index": 0, "text": "text", "finish_reason": "stop"}], **changes}
    calls = []
    def receive(request):
        calls.append(request)
        return httpx.Response(200, json=result)
    api = DeepSeekAPI("key", transport=httpx.MockTransport(receive))
    with pytest.raises(DeepSeekError, match="OCR"):
        await api.ocr(b"original", "report.pdf")
    assert len(calls) == 1


async def test_ocr_preserves_sequential_pages_and_partial_finish_reason():
    pages = [{"index": 0, "text": "", "finish_reason": "stop"},
             {"index": 1, "text": "partial page", "finish_reason": "length"}]
    result = {"purpose": "text_recognition", "page_count": 2, "pages": pages}
    api = DeepSeekAPI("key", transport=httpx.MockTransport(lambda _: httpx.Response(200, json=result)))
    assert await api.ocr(b"original", "report.pdf") == result
