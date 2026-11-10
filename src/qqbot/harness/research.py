"""Trusted, bounded public web fetch and task-scoped capability service."""

import asyncio
import hashlib
import hmac
import http.client
import ipaddress
import json
import os
import secrets
import socket
import ssl
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from urllib.parse import quote, urljoin, urlsplit
from uuid import uuid4

import httpx
from fastapi import FastAPI, Header, HTTPException, Request

from qqbot.providers import ModelCapacity

MAX_PAGE = 1024 * 1024


def public_addresses(host, port):
    addresses = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    if not addresses or len(addresses) > 16:
        raise ValueError('public DNS response is empty or exceeds address limit')
    for item in addresses:
        address = ipaddress.ip_address(item[4][0])
        if not address.is_global or address.is_multicast or address.is_reserved:
            raise ValueError('only public unicast addresses are allowed')
    return addresses


class PinnedHTTPS(http.client.HTTPSConnection):
    def __init__(self, host, port, addresses):
        super().__init__(host, port, timeout=10, context=ssl.create_default_context())
        self.addresses = addresses

    def connect(self):
        last_error = None
        for family, kind, protocol, _, address in self.addresses:
            connection = socket.socket(family, kind, protocol)
            connection.settimeout(self.timeout)
            try:
                connection.connect(address)
                self.sock = self._context.wrap_socket(connection, server_hostname=self.host)
                return
            except OSError as error:
                connection.close()
                last_error = error
        raise last_error or OSError("no public address")


def fetch_public(url):
    for _ in range(4):
        parsed = urlsplit(url)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError("only public HTTPS URLs without credentials are allowed")
        if parsed.port not in {None, 443}:
            raise ValueError("only HTTPS port 443 is allowed")
        connection = PinnedHTTPS(parsed.hostname, 443, public_addresses(parsed.hostname, 443))
        try:
            connection.request(
                "GET",
                (parsed.path or "/") + ("?" + parsed.query if parsed.query else ""),
                headers={"User-Agent": "ElephantResearch/0.1", "Accept-Encoding": "identity"},
            )
            response = connection.getresponse()
            if response.status in {301, 302, 303, 307, 308}:
                location = response.getheader("Location")
                if not location:
                    raise ValueError("redirect without location")
                url = urljoin(url, location)
                continue
            if response.status != 200:
                raise ValueError("public source returned HTTP " + str(response.status))
            media_type = response.getheader("Content-Type", "").split(";")[0].lower()
            if media_type not in {
                "text/plain",
                "text/html",
                "application/json",
                "text/markdown",
                "application/xml",
                "text/xml",
            }:
                raise ValueError("unsupported public source type")
            if response.getheader("Content-Encoding", "identity") != "identity":
                raise ValueError("compressed responses are disabled")
            content = response.read(MAX_PAGE + 1)
            if len(content) > MAX_PAGE:
                raise ValueError("public source exceeds size limit")
            text = content.decode("utf-8", errors="replace")
            return {
                "url": url,
                "retrieved_at": datetime.now(UTC).isoformat(),
                "sha256": hashlib.sha256(content).hexdigest(),
                "text": text,
            }
        finally:
            connection.close()
    raise ValueError("too many redirects")


class ResearchCapabilities:
    """One run, separate model/tool token, revocable supervisor-side budgets."""

    def __init__(
        self,
        workspace,
        model,
        upstream_key,
        *,
        upstream="https://api.openai.com/v1/chat/completions",
        active=lambda: True,
        transport=None,
        authority=None,
        capacity=None,
        background=False,
    ):
        self.workspace = Path(workspace)
        self.model, self.upstream_key, self.upstream = model, upstream_key, upstream
        self.upstream_session = str(uuid4())
        self.active = active
        self.authority = authority
        self.capacity, self.background = capacity or ModelCapacity(), background
        self.tool_token, self.model_token = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
        self.tool_calls = self.model_calls = 0
        self.revoked = False
        self.evidence = []
        self.event_sink = None
        self.artifacts = []
        self.lock = asyncio.Lock()
        self.model_lock = asyncio.Lock()
        self.transport = transport
        self.app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
        self._routes()

    def revoke(self):
        self.revoked = True

    async def authorize(self, authorization, token):
        if self.revoked or not self.active() or not hmac.compare_digest(authorization, "Bearer " + token):
            raise HTTPException(403, "run capability expired or revoked")
        if self.authority and not await self.authority():
            self.revoke()
            raise HTTPException(403, "core rejected run authority")

    def _routes(self):
        @self.app.post("/tools/{name}")
        async def tool(name: str, request: Request, authorization: str = Header(default="")):
            await self.authorize(authorization, self.tool_token)
            content = await bounded_json(request, MAX_PAGE)
            async with self.lock:
                if self.tool_calls >= 60:
                    raise HTTPException(429, "tool budget exhausted")
                self.tool_calls += 1
            try:
                if name in {"search", "fetch"}:
                    if name == "search":
                        query = content.get("query")
                        if not isinstance(query, str) or not 1 <= len(query) <= 500:
                            raise ValueError("search query length must be 1..500")
                        url = "https://cn.bing.com/search?format=rss&q=" + quote(query)
                    else:
                        url = content.get("url")
                        if not isinstance(url, str) or len(url) > 4096:
                            raise ValueError("invalid URL")
                    result = await asyncio.wait_for(asyncio.to_thread(fetch_public, url), 15)
                    await self.authorize(authorization, self.tool_token)
                    self.evidence.append({key: result[key] for key in ("url", "retrieved_at", "sha256")})
                    if self.event_sink:
                        self.event_sink({"type": "source_retrieved", "source": self.evidence[-1]})
                    return result
                if name == "write":
                    relative, text = content.get("path"), content.get("text")
                    if not isinstance(relative, str) or not isinstance(text, str):
                        raise ValueError("path and text are required")
                    path = PurePosixPath(relative)
                    if (
                        len(path.parts) != 2
                        or path.parts[0] != "outputs"
                        or path.suffix not in {".md", ".txt", ".json"}
                    ):
                        raise ValueError("use outputs/name.md, .txt or .json")
                    data = text.encode()
                    if len(data) > MAX_PAGE or len(self.artifacts) >= 20:
                        raise ValueError("artifact budget exceeded")
                    directory = self.workspace / "outputs"
                    directory.mkdir(mode=0o700, exist_ok=True)
                    root_fd = os.open(self.workspace, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                    try:
                        directory_fd = os.open(
                            "outputs", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=root_fd
                        )
                        try:
                            fd = os.open(
                                path.name,
                                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                                0o600,
                                dir_fd=directory_fd,
                            )
                            with os.fdopen(fd, "wb") as output:
                                output.write(data)
                                output.flush()
                                os.fsync(output.fileno())
                        finally:
                            os.close(directory_fd)
                    finally:
                        os.close(root_fd)
                    self.artifacts.append(str(path))
                    return {"path": str(path), "bytes": len(data)}
                raise ValueError("unsupported research tool")
            except (ValueError, OSError, TimeoutError, http.client.HTTPException) as error:
                raise HTTPException(422, str(error)) from error

        @self.app.post("/v1/chat/completions")
        async def model(request: Request, authorization: str = Header(default="")):
            await self.authorize(authorization, self.model_token)
            if not self.upstream_key and self.transport is None:
                raise HTTPException(503, "dedicated model credential is not configured")
            body = await bounded_json(request, 2 * MAX_PAGE)
            if body.get("model") != self.model or not isinstance(body.get("messages"), list):
                raise HTTPException(422, "unapproved model or messages")
            async with self.lock:
                if self.model_calls >= 30:
                    raise HTTPException(429, "model budget exhausted")
                self.model_calls += 1
            # Buffering bounds the response and permits revocation before returning it.
            payload = {
                key: body[key]
                for key in ("model", "messages", "tools", "tool_choice", "temperature")
                if key in body
            }
            payload.update(stream=False, max_tokens=4096)
            async with self.model_lock, self.capacity.slot(self.background):
                await self.authorize(authorization, self.model_token)
                async with (
                    httpx.AsyncClient(
                        transport=self.transport, timeout=60, follow_redirects=False, trust_env=False
                    ) as client,
                    client.stream(
                        "POST",
                        self.upstream,
                        json=payload,
                        headers={
                            "Authorization": "Bearer " + self.upstream_key,
                            "User-Agent": "ElephantBot/0.1",
                            "x-opencode-session": self.upstream_session,
                        },
                    ) as response,
                ):
                    if response.status_code != 200:
                        raise HTTPException(502, "model upstream failed")
                    output = bytearray()
                    async for chunk in response.aiter_bytes():
                        output.extend(chunk)
                        if len(output) > 2 * MAX_PAGE:
                            raise HTTPException(502, "model response exceeded limit")
            await self.authorize(authorization, self.model_token)
            result = json.loads(output)
            if body.get("stream"):
                from fastapi.responses import StreamingResponse

                message = result["choices"][0]["message"]
                if message.get("tool_calls"):
                    message["tool_calls"] = [
                        {"index": index, **call} for index, call in enumerate(message["tool_calls"])
                    ]
                chunk = {
                    "id": result.get("id", "proxy"),
                    "object": "chat.completion.chunk",
                    "created": result.get("created", 0),
                    "model": self.model,
                    "choices": [{"index": 0, "delta": message, "finish_reason": None}],
                }
                finish = {
                    **chunk,
                    "choices": [
                        {
                            "index": 0,
                            "delta": {},
                            "finish_reason": result["choices"][0].get("finish_reason", "stop"),
                        }
                    ],
                    "usage": result.get("usage"),
                }

                async def stream():
                    yield "data: " + json.dumps(chunk) + "\n\n"
                    yield "data: " + json.dumps(finish) + "\n\n"
                    yield "data: [DONE]\n\n"

                return StreamingResponse(stream(), media_type="text/event-stream")
            return result


async def bounded_json(request, limit):
    content = bytearray()
    async for chunk in request.stream():
        content.extend(chunk)
        if len(content) > limit:
            raise HTTPException(413, "request exceeded limit")
    try:
        result = json.loads(content)
        if not isinstance(result, dict):
            raise TypeError("object required")
        return result
    except (ValueError, TypeError) as error:
        raise HTTPException(422, "invalid JSON object") from error
