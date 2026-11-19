"""Decode QQ voice and send bounded segments to DeepSeek ASR."""

import asyncio
import io
import os
import shutil
import sys
import tempfile
import wave
from contextlib import nullcontext
from pathlib import Path
from typing import BinaryIO

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


async def transcribe_audio(api, data: bytes | BinaryIO, filename: str | None = None, *, before_transcribe=None) -> dict:
    with tempfile.TemporaryDirectory(prefix="elephant-audio-") as directory:
        source, pcm = Path(directory) / "input", Path(directory) / "audio.pcm"
        pcm_limit = shutil.disk_usage(directory).free // 2
        if isinstance(data, bytes):
            source.write_bytes(data)
        with source.open("rb") if isinstance(data, bytes) else nullcontext(data) as incoming:
            header = incoming.read(10)
            incoming.seek(0)
            if not header:
                raise ValueError("voice input is empty")
            if header.startswith((b"#!SILK_V3", b"\x02#!SILK_V3")):
                if not isinstance(data, bytes):
                    with source.open("xb") as output:
                        shutil.copyfileobj(incoming, output)
                await _run_decoder([sys.executable, "-I", "-c", SILK_DECODER, str(source), str(pcm), str(pcm_limit)])
            else:
                ffmpeg = shutil.which("ffmpeg")
                if ffmpeg is None:
                    raise ValueError("ffmpeg is required to decode audio")
                with pcm.open("xb") as outgoing:
                    # Descriptors permit seeking while excluding network and arbitrary file URLs.
                    os.lseek(incoming.fileno(), 0, os.SEEK_SET)
                    await _run_decoder([ffmpeg, "-nostdin", "-v", "error", "-protocol_whitelist", "fd,pipe",
                        "-i", "fd:", "-map", "0:a:0", "-vn", "-ac", "1", "-ar", str(SAMPLE_RATE),
                        "-codec:a", "pcm_s16le", "-f", "s16le", "-fs", str(pcm_limit), "pipe:1"],
                        stdin=incoming, stdout=outgoing)
        size = pcm.stat().st_size
        if size >= pcm_limit:
            raise ValueError("insufficient temporary disk space for decoded voice")
        if not size or size % 2:
            raise ValueError("audio decoding produced no complete samples")
        with pcm.open("rb") as decoded:
            first, duration, text = None, 0, ""
            while samples := decoded.read(MAX_PCM_BYTES):
                with io.BytesIO() as output:
                    with wave.open(output, "wb") as recording:
                        recording.setparams((1, 2, SAMPLE_RATE, 0, "NONE", "NONE"))
                        recording.writeframes(samples)
                    if before_transcribe is not None:
                        await before_transcribe()
                    response = await api.transcribe(output.getvalue(), "voice.wav")
                if first is None:
                    first = response
                duration += response["duration_ms"]
                text = (text + ("\n" if text else "") + response["text"])[:10001]
            if size <= MAX_PCM_BYTES:
                return first
            if len(text) > 10000:
                text = text[:9950] + "\n[转写过长，已截断。]"
            return {"text": text, "duration_ms": duration}
