"""Voice input through the normal model endpoint with durable, scoped ASR results."""

import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from qqbot.harness import audio
from qqbot.harness.gateway import RunGateway

from test_provider_gateway import FakeCore

VOICE = b"fixture original QQ audio"
TRANSCRIPT = "帮我查询今天上海的天气"


class VoiceCore(FakeCore):
    deny_audio = False

    async def __call__(self, operation, payload):
        if operation == "authorize" and payload["action"] == "audio.transcribe" and self.deny_audio:
            self.calls.append((operation, payload))
            return {"allowed": False}
        return await super().__call__(operation, payload)


@pytest.fixture
def voice_host(tmp_path, monkeypatch):
    state = SimpleNamespace(core=VoiceCore(), downloads=[], conversions=[], asr_requests=[], model_requests=[],
        asr_status=200, asr_result={"text": TRANSCRIPT, "duration_ms": 1200, "request_id": "asr-fixture"},
        deny_after_decode=False, revoke_during_asr=False)

    async def upstream(request):
        if request.url.path == "/v1/tools/asr":
            state.asr_requests.append(request.content)
            if state.revoke_during_asr:
                state.core.allowed = False
            return httpx.Response(state.asr_status, json=state.asr_result)
        assert request.url.path == "/chat/completions"
        state.model_requests.append(json.loads(request.content))
        return httpx.Response(200, json={"choices": [{"index": 0, "finish_reason": "stop",
            "message": {"role": "assistant", "content": "收到你的口述请求。"}}],
            "usage": {"prompt_tokens": 100, "completion_tokens": 10}})

    def download(url, limit):
        state.downloads.append(url)
        assert limit >= len(VOICE)
        return VOICE

    async def convert(api, data, filename=None, *, before_transcribe=None):
        assert data == VOICE and filename == "voice.silk"
        state.conversions.append(data)
        if state.deny_after_decode:
            state.core.deny_audio = True
        if before_transcribe is not None:
            await before_transcribe()
        return await api.transcribe(b"normalized WAV fixture", "voice.wav")

    def make(*, attachment=None, conversation="private", workspace_id="personal", principal="alice",
             key="fixture-key", endpoint="https://api.deepseek.com/chat/completions"):
        workspace = tmp_path / ("workspace-" + str(len(state.model_requests)))
        workspace.mkdir(exist_ok=True)
        gateway = RunGateway(workspace, "deepseek-flash", key, endpoint, call=state.core,
            transport=httpx.MockTransport(upstream), content_root=tmp_path / "content")
        file = {"source": "voice-source", "message_id": "message", "kind": "audio", "filename": "voice.silk",
            "mime_type": "audio/silk", "url": "https://cdn.example/voice.silk", "size": len(VOICE),
            "attachment_ref": "voice-attachment", "quoted": False, "is_current_input": True}
        file.update(attachment or {})
        gateway.bind({"task_id": "voice-task", "run_id": "voice-run", "fencing_token": 1,
            "role": "leader", "purpose": "user", "goal": "[用户发送了语音]",
            "permission_snapshot": {"principal": principal},
            "context": {"scope": conversation, "workspace_id": workspace_id, "files": [file]}})
        return gateway

    monkeypatch.setattr("qqbot.harness.gateway.fetch_public_bytes", download)
    monkeypatch.setattr(audio, "transcribe_audio", convert)
    state.make = make
    return state


async def send_voice(gateway):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app, raise_app_exceptions=False),
            base_url="http://gateway") as client:
        return await client.post("/v1/chat/completions", headers={"Authorization": "Bearer " + gateway.model_token},
            json={"model": gateway.model, "messages": [{"role": "user", "content": "[用户发送了语音]"}]})


def prompt(state):
    return json.dumps(state.model_requests[-1]["messages"], ensure_ascii=False)


async def test_model_entry_transcribes_voice_and_preserves_original(voice_host):
    gateway = voice_host.make()
    response = await send_voice(gateway)
    assert response.status_code == 200
    assert len(voice_host.asr_requests) == len(voice_host.model_requests) == 1
    assert TRANSCRIPT in prompt(voice_host)
    assert "本轮当前用户直接发送的语音转写" in prompt(voice_host)
    saved, = Path(gateway.workspace).glob("attachment-*")
    assert saved.read_bytes() == VOICE
    assert not gateway.inflight and not gateway.provider.active


async def test_transcript_cache_is_reused_across_model_calls_and_restart(voice_host):
    gateway = voice_host.make()
    assert (await send_voice(gateway)).status_code == 200
    assert (await send_voice(gateway)).status_code == 200
    restarted = voice_host.make(attachment={"url": "https://expired.example/voice"})
    assert (await send_voice(restarted)).status_code == 200
    assert len(voice_host.asr_requests) == len(voice_host.conversions) == len(voice_host.downloads) == 1
    assert len(voice_host.model_requests) == 3 and TRANSCRIPT in prompt(voice_host)


@pytest.mark.parametrize("changed", [{"principal": "bob"}, {"conversation": "other-private"},
                                      {"workspace_id": "project"}, {"key": "other-account"}])
async def test_transcript_cache_is_isolated_by_source_scope_and_account(voice_host, changed):
    assert (await send_voice(voice_host.make())).status_code == 200
    assert (await send_voice(voice_host.make(**changed))).status_code == 200
    assert len(voice_host.asr_requests) == len(voice_host.conversions) == 2


@pytest.mark.parametrize("attachment", [{"quoted": True}, {"is_current_input": False},
                                      {"quoted": True, "is_current_input": False}])
async def test_historical_and_quoted_voice_is_only_reference_material(voice_host, attachment):
    assert (await send_voice(voice_host.make(attachment=attachment))).status_code == 200
    assert TRANSCRIPT in prompt(voice_host)
    assert "历史或引用语音的转写，仅作资料" in prompt(voice_host)
    assert "本轮当前用户直接发送的语音转写" not in prompt(voice_host)


async def test_platform_transcript_works_without_deepseek_or_downloading(voice_host):
    gateway = voice_host.make(attachment={"transcript": TRANSCRIPT},
        endpoint="https://model.invalid/chat/completions")
    assert (await send_voice(gateway)).status_code == 200
    assert not voice_host.asr_requests and not voice_host.downloads and not voice_host.conversions
    assert TRANSCRIPT in prompt(voice_host)
    authorization = [payload for operation, payload in voice_host.core.calls
                     if operation == "authorize" and payload["action"] == "audio.transcribe"]
    assert authorization and all("event:voice-source" in item["resources"] for item in authorization)


@pytest.mark.parametrize("failure", ["unavailable", "silence"])
async def test_asr_failures_reach_model_as_explicit_unread_audio(voice_host, failure):
    if failure == "unavailable":
        voice_host.asr_status, voice_host.asr_result = 503, {"error": {"message": "unavailable"}}
        expected = "此语音未能转写"
    else:
        voice_host.asr_result = {"text": "", "duration_ms": 1000}
        expected = "此语音未识别出可用文字"
    gateway = voice_host.make()
    assert (await send_voice(gateway)).status_code == 200
    assert expected in prompt(voice_host) and "不得猜测内容" in prompt(voice_host)
    assert TRANSCRIPT not in prompt(voice_host)
    saved, = Path(gateway.workspace).glob("attachment-*")
    assert saved.read_bytes() == VOICE


async def test_audio_source_revocation_before_request_blocks_every_external_call(voice_host):
    gateway = voice_host.make()
    voice_host.core.allowed = False
    assert (await send_voice(gateway)).status_code == 403
    assert not voice_host.model_requests and not voice_host.asr_requests and not voice_host.downloads


async def test_source_revoked_during_asr_is_never_sent_to_model_or_cached(voice_host):
    gateway = voice_host.make()
    voice_host.revoke_during_asr = True
    assert (await send_voice(gateway)).status_code >= 400
    assert len(voice_host.asr_requests) == 1 and not voice_host.model_requests
    assert not list(gateway.content_root.glob("inbound/*/asr-*.json"))


async def test_source_revoked_during_decode_is_never_uploaded(voice_host):
    gateway = voice_host.make()
    voice_host.deny_after_decode = True
    assert (await send_voice(gateway)).status_code >= 400
    assert len(voice_host.conversions) == 1
    assert not voice_host.asr_requests and not voice_host.model_requests


async def test_audio_permission_is_rechecked_when_reusing_cached_prompt(voice_host):
    gateway = voice_host.make()
    assert (await send_voice(gateway)).status_code == 200
    voice_host.core.deny_audio = True
    assert (await send_voice(gateway)).status_code >= 400
    assert len(voice_host.model_requests) == len(voice_host.asr_requests) == 1


@pytest.mark.parametrize("platform", [False, True])
async def test_completion_after_business_budget_exhaustion_keeps_voice_input(voice_host, platform):
    gateway = voice_host.make(attachment={"transcript": TRANSCRIPT} if platform else {})
    gateway.budget_exhausted()
    gateway.provider.business_exhausted = True
    assert (await send_voice(gateway)).status_code == 200
    assert TRANSCRIPT in prompt(voice_host)
    reservations = [payload for operation, payload in voice_host.core.calls if operation == "reserve-budget"]
    assert reservations[0]["kind"] == "completion"
