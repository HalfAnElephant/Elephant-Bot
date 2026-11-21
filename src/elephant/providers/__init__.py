"""Bounded OpenAI-compatible transport. Pi owns retries and execution history."""
import asyncio
import codecs
import hashlib
import inspect
import json
import logging
import math
import re
import time
from contextlib import asynccontextmanager
from urllib.parse import urlsplit
from uuid import uuid4

import anyio
import httpx
from fastapi import HTTPException
from fastapi.responses import StreamingResponse

from elephant.observability.timing import record_timing

COMPLETION_INSTRUCTION = (
    "执行预算已到收尾边界。停止调用业务工具，仅根据已有工具观察和已核实效果，"
    "沿用当前宿主人格和表达风格，用 QQ 纯文本自然说清你做了什么、哪里还没弄清。"
    "不要宣称未验证的操作成功，也不要编造结果。"
)
logger = logging.getLogger(__name__)


def output_limit_parameter(endpoint: str) -> str:
    """Use the current limit field on OpenAI's own endpoint; keep compatibility elsewhere."""
    parts = urlsplit(endpoint)
    if parts.scheme == "https" and parts.hostname == "api.openai.com" and parts.port in {None, 443}:
        return "max_completion_tokens"
    return "max_tokens"


def estimate_tokens(value):
    # Conservative UTF-8 estimate includes tool schemas without an additional tokenizer dependency.
    return math.ceil(len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()) / 2)


def pi_request_tokens(payload):
    """Use Pi's text/image estimate on the final OpenAI messages and tool schema."""
    def chars(value):
        if not isinstance(value, str):
            value = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        return len(value.encode("utf-16-le", errors="surrogatepass")) // 2

    tokens = 0
    for message in payload["messages"]:
        content = message.get("content") or ""
        if isinstance(content, list):
            length = sum(4800 if isinstance(part, dict) and part.get("type") == "image_url"
                         else chars(part.get("text") or "") if isinstance(part, dict) and part.get("type") == "text"
                         else chars(part) for part in content)
        else:
            length = chars(content)
        length += chars(message.get("reasoning_content") or "")
        for call in message.get("tool_calls") or ():
            function = call.get("function")
            length += chars(function.get("name") or "") + chars(function.get("arguments") or "") if isinstance(
                function, dict) else chars(call)
        tokens += (length + 3) // 4
    tools = payload.get("tools")
    return tokens + ((chars(json.dumps(tools, ensure_ascii=False, separators=(",", ":"))) + 3) // 4 if tools else 0)


def validate_transactions(messages):
    pending = set()
    for message in messages:
        if not isinstance(message, dict) or message.get("role") not in ("system", "developer", "user", "assistant", "tool"):
            raise ValueError("invalid model message")
        role = message["role"]
        if role == "tool":
            identity = message.get("tool_call_id")
            if not isinstance(identity, str) or identity not in pending:
                raise ValueError("orphan or duplicate tool result")
            pending.remove(identity)
        else:
            if pending:
                raise ValueError("incomplete tool transaction")
            calls = message.get("tool_calls") or []
            if not isinstance(calls, list) or any(not isinstance(call, dict) for call in calls):
                raise ValueError("invalid tool calls")
            ids = [call.get("id") for call in calls]
            if calls and (role != "assistant" or any(not isinstance(i, str) or not i for i in ids)
                          or len(set(ids)) != len(ids)):
                raise ValueError("invalid tool call identities")
            pending.update(ids)
    if pending:
        raise ValueError("tool results missing before model request")


def normalize_usage(raw):
    usage = {"coverage": "unknown", "raw": raw, "input_tokens": None, "output_tokens": None,
             "uncached_input_tokens": None, "cached_read_tokens": None, "cached_write_tokens": None,
             "reasoning_tokens": None, "cost": None}
    if not isinstance(raw, dict):
        return usage
    def count(value):
        return value if type(value) is int and value >= 0 else None
    prompt = raw.get("prompt_tokens_details")
    completion = raw.get("completion_tokens_details")
    prompt = prompt if isinstance(prompt, dict) else {}
    completion = completion if isinstance(completion, dict) else {}
    total = count(raw.get("prompt_tokens", raw.get("input_tokens")))
    cached = count(prompt.get("cached_tokens", raw.get("prompt_cache_hit_tokens")))
    creation = count(raw.get("cache_creation_input_tokens"))
    uncached = count(raw.get("prompt_cache_miss_tokens"))
    if "cache_read_input_tokens" in raw:
        cached = count(raw.get("cache_read_input_tokens"))
        uncached = count(raw.get("input_tokens"))
        counters = [uncached, creation, cached]
        # Anthropic's input count excludes cache reads and cache creation.
        total = sum(value for value in counters if value is not None) if all(
            value is not None for value in (uncached, cached)
        ) else None
    elif total is None and uncached is not None and cached is not None:
        total = uncached + cached
    if uncached is None and total is not None and cached is not None and cached <= total:
        uncached = total - cached
    usage.update(input_tokens=total, output_tokens=count(raw.get("completion_tokens", raw.get("output_tokens"))),
                 uncached_input_tokens=uncached, cached_read_tokens=cached, cached_write_tokens=creation,
                 reasoning_tokens=count(completion.get("reasoning_tokens")))
    if any(usage[key] is not None for key in ("input_tokens", "output_tokens", "cached_read_tokens", "cached_write_tokens")):
        usage["coverage"] = "reported"
    return usage


class ModelCapacity:
    def __init__(self, total=4, reserved=0):
        if total < 1 or not 0 <= reserved < total:
            raise ValueError("invalid model capacity reservation")
        self.total = asyncio.Semaphore(total)
        self.background = asyncio.Semaphore(total - reserved)

    @property
    def maintenance_available(self):
        return not (self.background.locked() or self.total.locked())

    @asynccontextmanager
    async def slot(self, background):
        if not background:
            async with self.total:
                yield
            return
        async with self.background, self.total:
            yield


class _FirstBodyByte(httpx.AsyncByteStream):
    def __init__(self, stream, timings):
        self.stream, self.timings = stream, timings

    async def __aiter__(self):
        async for chunk in self.stream:
            if chunk and "first_body_byte" not in self.timings:
                self.timings["first_body_byte"] = time.monotonic_ns()
            yield chunk

    async def aclose(self):
        await self.stream.aclose()


class ProviderAdapter:
    def __init__(self, model, endpoint, key, *, authorize, reserve, record, transport=None,
                 capacity=None, background=False, max_bytes=16777216,
                 output_tokens=None, model_window=32768,
                 compaction_output=4096, stream_supported=True, on_exhausted=None,
                 guard=None, upstream_failure=None, request_succeeded=None, before_compaction_request=None,
                 maintenance=False, event_id=None, run_id=None, reasoning_effort=None):
        self.model, self.endpoint, self.key = model, endpoint, key
        self.upstream_session = str(uuid4())
        self.authorize, self.reserve, self.record = authorize, reserve, record
        self.transport, self.capacity, self.background = transport, capacity or ModelCapacity(), background
        self.max_bytes, self.output_tokens = max_bytes, output_tokens
        self.model_window, self.compaction_output = model_window, compaction_output
        self.stream_supported, self.on_exhausted = stream_supported, on_exhausted
        self.guard, self.upstream_failure, self.request_succeeded = guard, upstream_failure, request_succeeded
        self.before_compaction_request, self.maintenance = before_compaction_request, maintenance
        self.request_kind = "business"
        self.request_count = self.compaction_count = self.completion_count = 0
        self.business_exhausted = False
        self.active = 0
        self.last_usage = self.ttft = None
        self.event_id, self.run_id = event_id, run_id
        self.reasoning_effort = reasoning_effort

    async def _exhausted(self):
        self.business_exhausted = True
        if self.on_exhausted:
            result = self.on_exhausted()
            if inspect.isawaitable(result):
                await result

    def _check_size(self, payload):
        if len(json.dumps(payload, ensure_ascii=False).encode()) > self.max_bytes:
            raise HTTPException(413, "request exceeds application byte limit")

    def request_slot(self):
        if self.maintenance and not self.capacity.maintenance_available:
            raise HTTPException(409, "maintenance yielded model capacity")
        return self.capacity.slot(self.background)

    async def authorize_request(self, kind):
        await self.authorize("model.request")
        if kind == "compaction" and self.before_compaction_request:
            await self.before_compaction_request()

    async def complete(self, body, *, kind=None, document_tokens=0):
        await self.authorize("model.request")
        if not isinstance(body, dict) or body.get("model") != self.model or not isinstance(body.get("messages"), list):
            raise HTTPException(422, "unapproved model or messages")
        if kind is None:
            kind = "completion" if self.business_exhausted and self.request_kind == "business" else self.request_kind
        if kind not in {"business", "compaction", "completion", "notification"}:
            raise HTTPException(422, "invalid trusted model request kind")
        if kind == "completion" and self.completion_count >= 1:
            raise HTTPException(429, "per-run completion budget exhausted")
        output = self.compaction_output if kind == "compaction" else self.output_tokens
        payload = {key: body[key] for key in ("model", "messages", "tools", "tool_choice", "temperature",
                   "parallel_tool_calls", "reasoning_effort") if key in body}
        if self.reasoning_effort is not None:
            payload["reasoning_effort"] = self.reasoning_effort
        try:
            validate_transactions(payload["messages"])
        except ValueError as error:
            raise HTTPException(422, str(error)) from error
        payload["stream"] = bool(body.get("stream"))
        if output is not None:
            payload[output_limit_parameter(self.endpoint)] = output
        if payload["stream"]:
            payload["stream_options"] = {"include_usage": True}
        if self.guard:
            self.guard(payload, kind, document_tokens)
        self._check_size(payload)
        if body.get("stream") and not self.stream_supported:
            raise HTTPException(422, "configured provider does not support streaming")
        if not self.key and self.transport is None:
            raise HTTPException(503, "dedicated model credential is not configured")
        request_id = str(uuid4())
        try:
            reservation = await self.reserve(request_id, kind)
        except (ValueError, httpx.HTTPStatusError) as error:
            detail = str(error)
            if isinstance(error, httpx.HTTPStatusError):
                detail = error.response.text
            if kind != "business" or "budget exhausted" not in detail:
                raise
            await self._exhausted()
            kind = "completion"
            if self.completion_count:
                raise HTTPException(429, "per-run completion budget exhausted") from error
            request_id = str(uuid4())
            reservation = await self.reserve(request_id, kind)
        if kind == "business" and isinstance(reservation, dict) and reservation.get("remaining") == 0:
            await self._exhausted()
        if kind == "completion" or (kind == "business" and self.business_exhausted):
            # Keep the protocol schema for tool history while explicitly disabling further calls.
            payload.pop("parallel_tool_calls", None)
            if payload.get("tools"):
                payload["tool_choice"] = "none"
            else:
                payload.pop("tool_choice", None)
            payload["messages"] = [{"role": "system", "content": COMPLETION_INSTRUCTION}, *payload["messages"]]
            if self.guard:
                self.guard(payload, kind, document_tokens)
            self._check_size(payload)
        self.request_count += 1
        self.compaction_count += kind == "compaction"
        self.completion_count += kind == "completion"
        headers = {"Authorization": "Bearer " + self.key, "User-Agent": "ElephantBot/0.1",
                   "x-opencode-session": self.upstream_session}
        self.active += 1
        if payload["stream"]:
            return StreamingResponse(self._stream(payload, headers, request_id, kind), media_type="text/event-stream")
        started, raw = time.monotonic(), None
        try:
            async with self._upstream(payload, headers, request_id, kind, {}) as response:
                if response.status_code != 200:
                    error = await self._upstream_error(response)
                    if self.upstream_failure:
                        self.upstream_failure(payload, kind, error["code"], response.status_code)
                    raise HTTPException(response.status_code, error["message"])
                content = bytearray()
                async for chunk in response.aiter_bytes():
                    content.extend(chunk)
                    if len(content) > 2 * 1024 * 1024:
                        raise HTTPException(502, "model response exceeded limit")
                try:
                    result = json.loads(content)
                except (ValueError, UnicodeDecodeError) as error:
                    raise HTTPException(502, "invalid model response") from error
                if not isinstance(result, dict):
                    raise HTTPException(502, "invalid model response")
                raw = result.get("usage")
                await self.authorize("model.request")
                if self.request_succeeded:
                    self.request_succeeded(kind)
                return result
        finally:
            try:
                await self._record_safely(request_id, raw, started, None, payload)
            finally:
                self.active -= 1

    @asynccontextmanager
    async def _upstream(self, payload, headers, request_id, kind, timings):
        started = timings["waiting_for_slot"] = time.monotonic_ns()

        async def trace(name, _info):
            if name.endswith(".send_request_headers.started"):
                timings.setdefault("request_send_started", time.monotonic_ns())
            elif name.endswith(".send_request_body.complete"):
                timings.setdefault("request_sent", time.monotonic_ns())

        try:
            async with self.request_slot():
                timings["slot_acquired"] = time.monotonic_ns()
                async with httpx.AsyncClient(
                    transport=self.transport, timeout=60, trust_env=False, follow_redirects=False
                ) as client:
                    await self.authorize_request(kind)
                    timings["request_dispatched"] = time.monotonic_ns()
                    async with client.stream("POST", self.endpoint, json=payload, headers=headers,
                                             extensions={"trace": trace}) as response:
                        timings["response_headers"] = time.monotonic_ns()
                        response.stream = _FirstBodyByte(response.stream, timings)
                        yield response
        finally:
            record_timing("model.timing", event_id=self.event_id, run_id=self.run_id, request_id=request_id,
                          started_ns=started, offsets_ms={
                              stage: (stamp - started) / 1_000_000 for stage, stamp in timings.items()
                          })

    async def _upstream_error(self, response):
        content = bytearray()
        async for chunk in response.aiter_bytes():
            content.extend(chunk[:65536 - len(content)])
            if len(content) >= 65536:
                break
        text = content.decode(errors="replace")
        overflow = response.status_code == 413 or re.search(
            r"context[_ ](?:length|window)[_ ]exceeded|maximum context length|prompt (?:is )?too long|"
            r"request_too_large|exceeds.{0,40}context (?:window|length)", text, re.IGNORECASE
        )
        if overflow:
            return {"message": "maximum context length exceeded", "type": "invalid_request_error",
                    "code": "context_length_exceeded"}
        return {"message": f"model upstream failed (HTTP {response.status_code})",
                "type": "api_error", "code": "upstream_error"}

    async def _record_safely(self, request_id, raw, started, ttft, payload):
        # A client closing after [DONE] must not cancel the host's usage commit.
        try:
            with anyio.fail_after(5, shield=True):
                await self._record(request_id, raw, started, ttft, payload)
        except Exception:
            logger.exception("model usage commit failed", extra={"request_id": request_id})

    async def _record(self, request_id, raw, started, ttft, payload):
        usage = normalize_usage(raw)
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
        prefix = {"messages": [message for message in payload["messages"] if message["role"] in {"system", "developer"}],
                  "tools": payload.get("tools", [])}
        usage.update(request_bytes=len(encoded), input_tokens_estimated=estimate_tokens(payload),
            tools_bytes=len(json.dumps(prefix["tools"], ensure_ascii=False).encode()),
            request_sha256=hashlib.sha256(encoded).hexdigest(),
            stable_prefix_sha256=hashlib.sha256(json.dumps(prefix, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest())
        usage.update(elapsed_ms=round((time.monotonic() - started) * 1000, 2),
                     ttft_ms=round(ttft * 1000, 2) if ttft is not None else None,
                     provider_model=self.model)
        if "reasoning_effort" in payload:
            usage["requested_reasoning_effort"] = payload["reasoning_effort"]
        self.last_usage = usage
        await self.record(request_id, usage)

    async def _stream(self, payload, headers, request_id, kind):
        started, first, raw, total, pending = time.monotonic(), None, None, 0, ""
        failed = False
        decoder = codecs.getincrementaldecoder("utf-8")()
        timings = {}
        try:
            async with self._upstream(payload, headers, request_id, kind, timings) as response:
                if response.status_code != 200:
                    error = await self._upstream_error(response)
                    if self.upstream_failure:
                        self.upstream_failure(payload, kind, error["code"], response.status_code)
                    yield ("data: " + json.dumps({"error": error}) + "\n\n").encode()
                    return
                if "text/event-stream" not in response.headers.get("content-type", ""):
                    raise RuntimeError("upstream did not provide configured streaming protocol")
                async for chunk in response.aiter_bytes():
                    await self.authorize("model.request")
                    total += len(chunk)
                    if total > 2 * 1024 * 1024:
                        raise RuntimeError("stream response exceeded limit")
                    pending += decoder.decode(chunk)
                    normalized = pending.replace("\r\n", "\n")
                    while "\n\n" in normalized:
                        frame, normalized = normalized.split("\n\n", 1)
                        data = "\n".join(line[5:].lstrip(" ") for line in frame.splitlines() if line.startswith("data:"))
                        if data and data != "[DONE]":
                            event = json.loads(data)
                            if isinstance(event, dict):
                                if isinstance(event.get("error"), dict):
                                    failed = True
                                    if self.upstream_failure:
                                        code = event["error"].get("code", "upstream_error")
                                        self.upstream_failure(payload, kind, code, 200)
                                if event.get("usage") is not None:
                                    raw = event["usage"]
                                for choice in event.get("choices", []):
                                    delta = choice.get("delta", {}) if isinstance(choice, dict) else {}
                                    if first is None and isinstance(delta, dict) and any(
                                        delta.get(key) for key in ("content", "reasoning_content", "tool_calls")
                                    ):
                                        timings["first_effective_delta"] = time.monotonic_ns()
                                        first = time.monotonic() - started
                                        self.ttft = first
                    pending = normalized
                    yield chunk
                pending += decoder.decode(b"", final=True)
                if pending.strip():
                    raise RuntimeError("truncated upstream event")
                if self.request_succeeded and not failed:
                    self.request_succeeded(kind)
        except (httpx.TransportError, TimeoutError):
            if self.upstream_failure:
                self.upstream_failure(payload, kind, "upstream_transport_error", 503)
            raise
        finally:
            try:
                await self._record_safely(request_id, raw, started, first, payload)
            finally:
                self.active -= 1
