"""Bounded QQ voice decoding before DeepSeek's complete-audio transcription API."""

import asyncio
import io
import os
import shutil
import sys
import tempfile
import wave
from pathlib import Path

MAX_AUDIO_BYTES = 8 * 1024 * 1024
MAX_AUDIO_SECONDS = 120
SAMPLE_RATE = 16000
DECODE_TIMEOUT = 20
MAX_PCM_BYTES = MAX_AUDIO_SECONDS * SAMPLE_RATE * 2

SILK_DECODER = """import resource, sys
resource.setrlimit(resource.RLIMIT_CPU, (15, 15))
limit = int(sys.argv[3])
resource.setrlimit(resource.RLIMIT_FSIZE, (limit, limit))
from pilk_nogil import decode
decode(sys.argv[1], sys.argv[2], pcm_rate=16000)
"""


async def _run_decoder(argv, *, stdin=None, stdout=None):
    starting = asyncio.create_task(asyncio.create_subprocess_exec(*argv,
        stdin=stdin if stdin is not None else asyncio.subprocess.DEVNULL,
        stdout=stdout if stdout is not None else asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL, env={"PATH": os.defpath, "LANG": "C.UTF-8"}))
    waiting = None
    try:
        process = await asyncio.shield(starting)
        waiting = asyncio.create_task(process.wait())
        code = await asyncio.wait_for(asyncio.shield(waiting), DECODE_TIMEOUT)
    except BaseException:
        while not starting.done():
            try:
                await asyncio.shield(starting)
            except asyncio.CancelledError:
                pass
        process = starting.result()
        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
        waiting = waiting or asyncio.create_task(process.wait())
        while not waiting.done():
            try:
                await asyncio.shield(waiting)
            except asyncio.CancelledError:
                pass
        waiting.result()
        raise
    if code:
        raise ValueError("audio decoding failed")


async def transcribe_audio(api, data: bytes, filename: str | None = None, *, before_transcribe=None) -> dict:
    if not isinstance(data, bytes) or not data or len(data) > MAX_AUDIO_BYTES:
        raise ValueError("voice input must contain 1 byte to 8 MiB of audio")
    with tempfile.TemporaryDirectory(prefix="qqbot-audio-") as directory:
        source, pcm = Path(directory) / "input", Path(directory) / "audio.pcm"
        source.write_bytes(data)
        if data.startswith((b"#!SILK_V3", b"\x02#!SILK_V3")):
            await _run_decoder([sys.executable, "-I", "-c", SILK_DECODER, str(source), str(pcm),
                                str(MAX_PCM_BYTES + SAMPLE_RATE * 2)])
        else:
            ffmpeg = shutil.which("ffmpeg")
            if ffmpeg is None:
                raise ValueError("ffmpeg is required to decode audio")
            with source.open("rb") as incoming, pcm.open("xb") as outgoing:
                # Descriptors permit seeking while excluding network and arbitrary file URLs.
                await _run_decoder([ffmpeg, "-nostdin", "-v", "error", "-protocol_whitelist", "fd,pipe",
                    "-i", "fd:", "-map", "0:a:0", "-vn", "-t", str(MAX_AUDIO_SECONDS + 1),
                    "-ac", "1", "-ar", str(SAMPLE_RATE), "-codec:a", "pcm_s16le", "-f", "s16le",
                    "-fs", str(MAX_PCM_BYTES + SAMPLE_RATE * 2), "pipe:1"], stdin=incoming, stdout=outgoing)
        if pcm.stat().st_size > MAX_PCM_BYTES:
            raise ValueError("voice input exceeds 120 seconds")
        samples = pcm.read_bytes()
        if not samples or len(samples) % 2:
            raise ValueError("audio decoding produced no complete samples")
    with io.BytesIO() as output:
        with wave.open(output, "wb") as recording:
            recording.setparams((1, 2, SAMPLE_RATE, 0, "NONE", "NONE"))
            recording.writeframes(samples)
        if before_transcribe is not None:
            await before_transcribe()
        return await api.transcribe(output.getvalue(), "voice.wav")
