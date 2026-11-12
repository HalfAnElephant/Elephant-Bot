"""Bounded local-media transport, using official prepare/part/complete protocol.

Source: tencent-connect/qqbot-agent-sdk commit 6163b5dc979a2f12379b1916805009075008c3c3.
No base64 file_data fallback: official bot-docs still marks it unsupported.
COS hosts: https://cloud.tencent.com/document/product/436/6224 and /436/67768.
This module receives authorized bytes or an immutable seekable file, never opens a path.
Per-type limits: qqbot-nodejs 429a0f1b798f3b0fcef3679e207dbc0469a1df2d.
"""
import asyncio
import hashlib
import json
import re
from dataclasses import dataclass
from typing import BinaryIO
from urllib.parse import quote, urlsplit

import httpx

from qqbot.domain import Surface

from .local_limits import MAX_FILE_BYTES, validate_size

MAX_BYTES = MAX_FILE_BYTES  # Compatibility alias; per-type limits are authoritative.
MAX_PARTS = 1024  # Bounded response/CPU guard, not a platform media-size limit.
IO_CHUNK = 1024 * 1024
MAX_SERVER_BLOCK = 1024 * 1024 * 1024  # Metadata only; allocations stay bounded by MAX_BYTES.
_COS_HOST = re.compile(r"[a-z0-9][a-z0-9-]*-[0-9]+\.cos\.[a-z0-9-]+\.(?:myqcloud\.com|tencentcos\.cn)\Z")


@dataclass
class LocalUploadError(Exception):
    code: str
    retryable: bool = False

    def __str__(self):
        return self.code  # Never include bearer tokens, presigned URLs or raw response bodies.


def _cos_url(value):
    if not isinstance(value, str) or len(value) > 8192 or any(ord(c) < 33 for c in value):
        raise LocalUploadError("LOCAL_UPLOAD_UNSAFE_HOST")
    try:
        url = urlsplit(value)
        if (url.scheme != "https" or url.username or url.password or url.fragment
                or url.port not in (None, 443) or not _COS_HOST.fullmatch(url.hostname or "")):
            raise ValueError
    except ValueError:
        raise LocalUploadError("LOCAL_UPLOAD_UNSAFE_HOST") from None
    return value


def _positive(value, maximum):
    if type(value) is not int or not 1 <= value <= maximum:
        raise LocalUploadError("LOCAL_UPLOAD_INVALID_PLAN")
    return value


def _plan(raw, size):
    upload_id = raw.get("upload_id")
    if not isinstance(upload_id, str) or not 1 <= len(upload_id) <= 512:
        raise LocalUploadError("LOCAL_UPLOAD_INVALID_PLAN")
    block = _positive(raw.get("block_size"), MAX_SERVER_BLOCK)
    count = (size + block - 1) // block
    parts = raw.get("parts")
    if not isinstance(parts, list) or not 1 <= len(parts) == count <= MAX_PARTS:
        raise LocalUploadError("LOCAL_UPLOAD_INVALID_PLAN")
    validated = {}
    for part in parts:
        if not isinstance(part, dict):
            raise LocalUploadError("LOCAL_UPLOAD_INVALID_PLAN")
        index = _positive(part.get("index"), count)
        actual = min(block, size - (index - 1) * block)
        override = part.get("block_size", 0)
        if type(override) is not int or override not in (0, actual, block) or index in validated:
            raise LocalUploadError("LOCAL_UPLOAD_INVALID_PLAN")
        validated[index] = (_cos_url(part.get("presigned_url")), (index - 1) * block, actual)
    return upload_id, [validated[i] for i in range(1, count + 1)]


async def _json_post(client, url, body, headers):
    async with client.stream("POST", url, json=body, headers=headers, timeout=30, follow_redirects=False) as response:
        content = bytearray()
        async for chunk in response.aiter_bytes():
            if len(content) + len(chunk) > 1024 * 1024:
                raise LocalUploadError("LOCAL_UPLOAD_INVALID_RESPONSE")
            content.extend(chunk)
        try:
            raw = json.loads(content)
        except (ValueError, UnicodeDecodeError):
            raw = None
        if response.status_code == 401:
            raise LocalUploadError("LOCAL_UPLOAD_TOKEN_REJECTED", True)
        if response.status_code == 429 or response.status_code >= 500:
            raise LocalUploadError("LOCAL_UPLOAD_UNAVAILABLE", True)
        code = raw.get("code") if isinstance(raw, dict) else None
        if not response.is_success or (type(code) is int and code != 0):
            if type(code) is int and code == 40093002:
                raise LocalUploadError("LOCAL_UPLOAD_DAILY_LIMIT")
            if type(code) is int and code == 40093001:
                raise LocalUploadError("LOCAL_UPLOAD_PART_RETRY", True)
            raise LocalUploadError("LOCAL_UPLOAD_REJECTED")
        if not isinstance(raw, dict):
            raise LocalUploadError("LOCAL_UPLOAD_INVALID_RESPONSE")
        return raw


async def _file_io(function, *args):
    """Keep the caller's descriptor alive until an in-flight thread has stopped.

    Cancellation must not let the adapter close a file that a worker still reads.
    Drain the one local I/O operation, including repeated cancellation, then
    propagate cancellation without resuming any network operation.
    """
    worker = asyncio.create_task(asyncio.to_thread(function, *args))
    try:
        return await asyncio.shield(worker)
    except asyncio.CancelledError:
        while not worker.done():
            try:
                await asyncio.shield(worker)
            except asyncio.CancelledError:
                continue
            except Exception:  # noqa: BLE001 - Preserve cancellation after draining local I/O.
                break
        if not worker.cancelled():
            worker.exception()  # Retrieve any thread failure; cancellation remains authoritative.
        raise


async def upload_local(adapter, envelope, content: bytes | BinaryIO, filename: str, media_type: str,
                       mime_type: str, authorize=None, *, cos_client=None) -> str:
    """Upload bytes/stream only; return file_info, never send a message.

    Stream ownership stays with the caller. Hashing and PUT bodies use bounded
    chunks even when the server chooses one part as large as the entire file.

    Channel images use multipart on the message endpoint, handled by the adapter.
    The optional COS client is for isolated transport testing; production creates
    a fresh client without QQ headers, cookies, auth, proxies or redirects.
    Errors are sanitized and leave no local temporary files. No remote abort API
    is documented; incomplete server uploads are left to platform expiry.
    """
    try:
        if isinstance(content, bytes):
            size = len(content)
        else:
            def stream_size():
                content.seek(0, 2)
                size = content.tell()
                content.seek(0)
                return size
            size = await _file_io(stream_size)
        validate_size(size, media_type)
    except (ValueError, TypeError, AttributeError, OSError):
        raise LocalUploadError("LOCAL_UPLOAD_SIZE_LIMIT") from None

    def read_at(offset, length):
        if isinstance(content, bytes):
            return content[offset:offset + length]
        content.seek(offset)
        chunk = content.read(length)
        if not isinstance(chunk, bytes) or len(chunk) != length:
            raise LocalUploadError("LOCAL_UPLOAD_SOURCE_CHANGED")
        return chunk

    def part_md5(offset, length):
        digest = hashlib.md5()
        for pos in range(offset, offset + length, IO_CHUNK):
            digest.update(read_at(pos, min(IO_CHUNK, offset + length - pos)))
        return digest.hexdigest()

    async def part_stream(offset, length):
        for pos in range(offset, offset + length, IO_CHUNK):
            await allowed()
            yield await _file_io(read_at, pos, min(IO_CHUNK, offset + length - pos))
    if (not isinstance(filename, str) or not 1 <= len(filename.encode("utf-8")) <= 255
            or any(c in filename for c in "/\\") or any(ord(c) < 32 for c in filename)):
        raise LocalUploadError("LOCAL_UPLOAD_INVALID_FILENAME")
    if media_type not in {"image", "video", "audio", "file"} or not isinstance(mime_type, str):
        raise LocalUploadError("LOCAL_UPLOAD_INVALID_TYPE")
    if envelope.target.surface not in {Surface.PRIVATE, Surface.GROUP}:
        raise LocalUploadError("LOCAL_UPLOAD_UNSUPPORTED_SURFACE")

    async def allowed():
        try:
            if authorize is not None and not authorize():
                raise LocalUploadError("AUTHORIZATION_CHANGED")
        except LocalUploadError:
            raise
        except Exception:  # noqa: BLE001 - Authorization must fail closed.
            raise LocalUploadError("AUTHORIZATION_CHANGED") from None
        check = await adapter.validate_delivery(envelope)
        if not check.allowed:
            raise LocalUploadError(check.reason)

    async def perform(cos):
        await allowed()
        token = await adapter.access_token()
        await allowed()
        scope = "users" if envelope.target.surface == Surface.PRIVATE else "groups"
        base = adapter.config.api_base.rstrip("/") + "/v2/" + scope + "/" + quote(
            envelope.target.remote_conversation_id, safe="")
        headers = {"Authorization": "QQBot " + token, "X-Union-Appid": str(adapter.config.app_id)}
        def hashes():
            md5, sha1, prefix = hashlib.md5(), hashlib.sha1(), hashlib.md5()
            for offset in range(0, size, IO_CHUNK):
                chunk = read_at(offset, min(IO_CHUNK, size - offset))
                md5.update(chunk); sha1.update(chunk)
                if offset < 10_002_432:
                    prefix.update(chunk[:10_002_432 - offset])
            return {"md5": md5.hexdigest(), "sha1": sha1.hexdigest(), "md5_10m": prefix.hexdigest()}
        digest = await _file_io(hashes)
        await allowed()
        raw = await _json_post(adapter.client, base + "/upload_prepare", {
            "file_type": {"image": 1, "video": 2, "audio": 3, "file": 4}[media_type],
            "file_name": filename, "file_size": size, **digest}, headers)
        upload_id, parts = _plan(raw, size)  # Validate every URL and boundary before any byte is uploaded.
        for index, (url, offset, length) in enumerate(parts, start=1):
            await allowed()
            digest = await _file_io(part_md5, offset, length)
            await allowed()
            # Construct directly, not build_request: never inherit cookies/auth headers from a client.
            request = httpx.Request("PUT", url, content=part_stream(offset, length), headers={"Content-Length": str(length)})
            response = await cos.send(request, auth=None, follow_redirects=False, stream=True)
            try:
                if not response.is_success:
                    raise LocalUploadError("LOCAL_UPLOAD_PART_REJECTED", response.status_code >= 500)
            finally:
                await response.aclose()
            await allowed()
            await _json_post(adapter.client, base + "/upload_part_finish", {
                "upload_id": upload_id, "part_index": index, "block_size": length,
                "md5": digest}, headers)
        await allowed()
        complete = await _json_post(adapter.client, base + "/files", {"upload_id": upload_id}, headers)
        info = complete.get("file_info")
        if not isinstance(info, str) or not 1 <= len(info) <= 16384:
            raise LocalUploadError("LOCAL_UPLOAD_MISSING_FILE_INFO")
        return info

    try:
        async with asyncio.timeout(600):
            if cos_client is not None:
                return await perform(cos_client)
            async with httpx.AsyncClient(timeout=30, follow_redirects=False, trust_env=False) as cos:
                return await perform(cos)
    except LocalUploadError as exc:
        if exc.code == "LOCAL_UPLOAD_TOKEN_REJECTED":
            adapter.token = None
        raise
    except (httpx.HTTPError, TimeoutError):
        raise LocalUploadError("LOCAL_UPLOAD_UNAVAILABLE", True) from None
