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
async def test_errors_are_bounded_sanitized_and_never_retried(response):
    calls = []
    def receive(request):
        calls.append(request)
        return response
    api = DeepSeekAPI("dedicated-key", transport=httpx.MockTransport(receive))
    with pytest.raises(DeepSeekError) as error:
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
