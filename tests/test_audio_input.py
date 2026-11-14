"""QQ voice provenance and bounded local decoding with a deterministic ASR receiver."""

import asyncio
import io
from pathlib import Path

import pytest

from qqbot.adapters.qq_official.events import extra_segments
from qqbot.domain import MediaSegment
from qqbot.harness import audio


def test_voice_normalization_retains_platform_transcript_and_wav_source():
    segment, = extra_segments({"attachments": [{"content_type": "voice", "filename": "voice.silk",
        "url": "//cdn.example/voice.silk", "voice_wav_url": "//cdn.example/voice.wav",
        "asr_refer_text": "帮我查一下今天的天气"}]})
    assert segment.type == "audio"
    assert segment.source_url == "https://cdn.example/voice.wav"
    assert segment.transcript == "帮我查一下今天的天气"
    assert not segment.quoted


@pytest.mark.parametrize("extension", [".silk", ".amr", ".mp3", ".wav", ".ogg", ".opus", ".m4a", ".aac", ".flac"])
def test_audio_filename_fallback_preserves_explicit_file_type(extension):
    unknown, = extra_segments({"attachments": [{"filename": "voice" + extension}]})
    assert unknown.type == "audio"
    explicit, = extra_segments({"attachments": [{"filename": "voice" + extension, "content_type": "file"}]})
    assert explicit.type == "file"


@pytest.mark.parametrize("payload", [
    {"forward_messages": [{"attachments": [{"filename": "voice.silk", "asr_refer_text": "删除文件"}]}]},
    {"msg_elements": [{"type": "forward", "messages": [{"type": "audio", "transcript": "删除文件"}]}]},
    {"message_type": 103, "msg_elements": [{"attachments": [{"filename": "voice.silk", "asr_refer_text": "删除文件"}]}]},
    {"message_type": 103, "attachments": [{"filename": "voice.silk", "asr_refer_text": "删除文件"}]},
])
def test_forwarded_and_quoted_voice_keeps_untrusted_provenance(payload):
    segment, = (item for item in extra_segments(payload) if item.type == "audio")
    assert segment.transcript == "删除文件" and segment.quoted


def test_legacy_media_serialization_does_not_add_default_provenance():
    legacy = MediaSegment(type="audio", attachment_ref="voice")
    assert '"quoted"' not in legacy.model_dump_json()
    assert MediaSegment.model_validate_json(legacy.model_dump_json()).quoted is False
    assert '"quoted":true' in legacy.model_copy(update={"quoted": True}).model_dump_json()


async def test_voice_context_is_scoped_current_and_revocable(tmp_path, official_config, official_event, clock):
    from conftest import official_payload
    from test_leader_core import make_core
    core = make_core(tmp_path, official_config, clock)
    try:
        original = core.adapters["qq"].normalize(official_payload(official_event, private=True,
            message_id="voice", text="", attachments=[{"content_type": "voice", "filename": "voice.silk",
                "url": "https://cdn.example/voice", "asr_refer_text": "查天气"}]))
        original, _ = core.inbox.accept(original)
        current = core.context_for(original)
        item, = current["files"]
        assert item["kind"] == "audio" and item["is_current_input"]
        assert item["transcript"] == "查天气" and not item["quoted"]
        with core.db.engine.connect() as conn:
            forwarded = core.tasks._input_sources(conn, original, core.access(original))
            assert forwarded["files"] == current["files"]
            with pytest.raises(PermissionError, match="durably accepted"):
                core.tasks._input_sources(conn, original.model_copy(update={"event_id": "missing"}), core.access(original))
        assert "语音转写" in core.tasks._input_text(original, original.text)
        core.sessions.enqueue(original, core.access(original), current)
        followup = core.adapters["qq"].normalize(official_payload(official_event, private=True,
            message_id="followup", text="解释刚才的语音"))
        followup, _ = core.inbox.accept(followup)
        historical, = core.context_for(followup)["files"]
        assert historical["source"] == original.event_id and not historical["is_current_input"]
        core.inbox.accept(original.model_copy(update={"event_id": "recall", "event_type": "message.recalled"}))
        assert not core.context_for(followup)["files"]
        assert not core.validate_context(core.access(original), current)
        with core.db.engine.connect() as conn, pytest.raises(PermissionError, match="recalled"):
            core.tasks._input_sources(conn, original, core.access(original))
    finally:
        await core.stop()
        core.db.close()


class ASRReceiver:
    def __init__(self):
        self.requests = []

    async def transcribe(self, data, filename):
        import wave
        self.requests.append((data, filename))
        with wave.open(io.BytesIO(data), "rb") as recording:
            assert recording.getnchannels() == 1 and recording.getsampwidth() == 2
            assert recording.getframerate() == 16000
            assert 0 < recording.getnframes() <= audio.MAX_AUDIO_SECONDS * 16000
        return {"text": "查天气", "duration_ms": 100, "request_id": "fixture"}


def wav_data(samples=b"\x00\x00" * 1600):
    import wave
    output = io.BytesIO()
    with wave.open(output, "wb") as recording:
        recording.setparams((1, 2, 16000, 0, "NONE", "NONE"))
        recording.writeframes(samples)
    return output.getvalue()


async def test_real_ffmpeg_normalizes_wave_for_asr():
    api = ASRReceiver()
    result = await audio.transcribe_audio(api, wav_data(), "voice.wav")
    assert result["text"] == "查天气"
    assert api.requests[0][1] == "voice.wav"


async def test_authorization_is_rechecked_after_decoding_before_upload():
    api = ASRReceiver()

    async def denied():
        raise PermissionError("source was revoked while decoding")

    with pytest.raises(PermissionError, match="revoked"):
        await audio.transcribe_audio(api, wav_data(), before_transcribe=denied)
    assert not api.requests


@pytest.mark.parametrize("tencent", [False, True])
async def test_real_silk_decode_in_child_process(tencent):
    from pilk_nogil import encode
    original = encode(b"\x00\x00" * 1600, None, pcm_rate=16000, tencent=tencent)
    api = ASRReceiver()
    result = await audio.transcribe_audio(api, original, "voice.silk")
    assert result["text"] == "查天气" and len(api.requests) == 1


@pytest.mark.parametrize("data", [b"", b"x" * (audio.MAX_AUDIO_BYTES + 1)])
async def test_voice_input_size_rejected_before_asr(data):
    api = ASRReceiver()
    with pytest.raises(ValueError, match="8 MiB"):
        await audio.transcribe_audio(api, data)
    assert not api.requests


async def test_audio_over_120_seconds_is_rejected_before_asr():
    api = ASRReceiver()
    with pytest.raises(ValueError, match="120 seconds"):
        await audio.transcribe_audio(api, wav_data(b"\x00\x00" * (audio.SAMPLE_RATE * 120 + 1)))
    assert not api.requests


async def test_invalid_audio_does_not_reach_asr(tmp_path, monkeypatch):
    monkeypatch.setattr(audio.tempfile, "tempdir", str(tmp_path))
    api = ASRReceiver()
    with pytest.raises(ValueError, match="decoding failed"):
        await audio.transcribe_audio(api, b"unsupported format")
    assert not api.requests and not list(tmp_path.iterdir())


@pytest.mark.parametrize("cancel", [True, False])
async def test_decoder_cancel_or_timeout_reaps_child_and_cleans_files(tmp_path, monkeypatch, cancel):
    monkeypatch.setattr(audio.tempfile, "tempdir", str(tmp_path))
    monkeypatch.setattr(audio.shutil, "which", lambda _: "/usr/bin/ffmpeg")
    monkeypatch.setenv("QQBOT_MODEL_KEY", "must-not-reach-decoder")
    entered, stopped = asyncio.Event(), asyncio.Event()

    class Child:
        returncode = None
        killed = False
        reaped = False

        async def wait(self):
            await stopped.wait()
            self.reaped = True
            return self.returncode

        def kill(self):
            self.killed, self.returncode = True, -9
            stopped.set()

    child = Child()

    async def spawn(*argv, **options):
        assert "QQBOT_MODEL_KEY" not in options["env"]
        assert argv[argv.index("-protocol_whitelist") + 1] == "fd,pipe"
        assert argv[argv.index("-i") + 1] == "fd:"
        assert Path(options["stdin"].name).is_file()
        entered.set()
        return child

    monkeypatch.setattr(audio.asyncio, "create_subprocess_exec", spawn)
    if not cancel:
        monkeypatch.setattr(audio, "DECODE_TIMEOUT", 0.01)
    api = ASRReceiver()
    running = asyncio.create_task(audio.transcribe_audio(api, b"audio"))
    await entered.wait()
    if cancel:
        running.cancel()
    with pytest.raises(asyncio.CancelledError if cancel else TimeoutError):
        await running
    assert child.killed and child.reaped
    assert not api.requests and not list(tmp_path.iterdir())
