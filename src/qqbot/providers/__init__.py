"""Bounded OpenAI-compatible transport. Pi owns retries and execution history."""
import asyncio
import codecs
import hashlib
import inspect
import json
import math
import time
from contextlib import asynccontextmanager
from uuid import uuid4

import httpx
from fastapi import HTTPException
from fastapi.responses import StreamingResponse

COMPLETION_INSTRUCTION = (
    "执行预算已到收尾边界。停止调用业务工具，仅根据已有工具观察和已核实效果，"
    "自然说明已完成、未完成和未知事项。不要宣称未验证的操作成功，也不要编造结果。"
)


def estimate_tokens(value):
    # Conservative UTF-8 estimate includes tool schemas without an additional tokenizer dependency.
    return math.ceil(len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()) / 2)


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
    def __init__(self, total=4, reserved=1):
        if not 1 <= reserved < total:
            raise ValueError("new interactions need a reserved model slot")
        self.total = asyncio.Semaphore(total)
        self.sustained = asyncio.Semaphore(total - reserved)

    @asynccontextmanager
    async def slot(self, background):
        if background:
            await self.sustained.acquire()
        try:
            async with self.total:
                yield
        finally:
            if background:
                self.sustained.release()


class ProviderAdapter:
    def __init__(self, model, endpoint, key, *, authorize, reserve, record, transport=None,
                 capacity=None, background=False, hard_tokens=8000, max_bytes=262144,
                 output_tokens=600, model_window=32768, compaction_tokens=16000,
                 compaction_output=1000, stream_supported=True, on_exhausted=None):
        self.model, self.endpoint, self.key = model, endpoint, key
        self.upstream_session = str(uuid4())
        self.authorize, self.reserve, self.record = authorize, reserve, record
        self.transport, self.capacity, self.background = transport, capacity or ModelCapacity(), background
        self.hard_tokens, self.max_bytes, self.output_tokens = hard_tokens, max_bytes, output_tokens
        self.model_window, self.compaction_tokens, self.compaction_output = model_window, compaction_tokens, compaction_output
        self.stream_supported, self.on_exhausted = stream_supported, on_exhausted
        self.request_kind = "business"
        self.request_count = self.compaction_count = self.completion_count = 0
        self.business_exhausted = False
        self.active = 0
        self.last_usage = self.ttft = None

    async def _exhausted(self):
        self.business_exhausted = True
        if self.on_exhausted:
            result = self.on_exhausted()
            if inspect.isawaitable(result):
                await result

    def _check_budget(self, payload, limit, output):
        if len(json.dumps(payload, ensure_ascii=False).encode()) > self.max_bytes or estimate_tokens(payload) > min(
            limit, self.model_window - output
        ):
            raise HTTPException(413, "context exceeds application budget; bounded compaction or new epoch required")

    async def complete(self, body):
        await self.authorize("model.request")
        if not isinstance(body, dict) or body.get("model") != self.model or not isinstance(body.get("messages"), list):
            raise HTTPException(422, "unapproved model or messages")
        kind = "completion" if self.business_exhausted and self.request_kind == "business" else self.request_kind
        if kind not in {"business", "compaction", "completion", "notification"}:
            raise HTTPException(422, "invalid trusted model request kind")
        if kind == "compaction" and self.compaction_count >= 2:
            raise HTTPException(429, "per-run compaction budget exhausted")
        if kind == "completion" and self.completion_count >= 1:
            raise HTTPException(429, "per-run completion budget exhausted")
        output = self.compaction_output if kind == "compaction" else self.output_tokens
        limit = self.compaction_tokens if kind == "compaction" else self.hard_tokens
        payload = {key: body[key] for key in ("model", "messages", "tools", "tool_choice", "temperature",
                   "parallel_tool_calls", "reasoning_effort") if key in body}
        try:
            validate_transactions(payload["messages"])
        except ValueError as error:
            raise HTTPException(422, str(error)) from error
        self._check_budget(payload, limit, output)
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
            payload = {key: value for key, value in payload.items() if key not in {"tools", "tool_choice", "parallel_tool_calls"}}
            payload["messages"] = [{"role": "system", "content": COMPLETION_INSTRUCTION}, *payload["messages"]]
        payload.update(max_tokens=output, stream=bool(body.get("stream")))
        if payload["stream"]:
            payload["stream_options"] = {"include_usage": True}
        self._check_budget(payload, limit, output)
        self.request_count += 1
        self.compaction_count += kind == "compaction"
        self.completion_count += kind == "completion"
        headers = {"Authorization": "Bearer " + self.key, "User-Agent": "ElephantBot/0.1",
                   "x-opencode-session": self.upstream_session}
        self.active += 1
        if payload["stream"]:
            return StreamingResponse(self._stream(payload, headers, request_id), media_type="text/event-stream")
        started, raw = time.monotonic(), None
        try:
            async with self.capacity.slot(self.background), httpx.AsyncClient(
                transport=self.transport, timeout=60, trust_env=False, follow_redirects=False) as client:
                await self.authorize("model.request")
                async with client.stream("POST", self.endpoint, json=payload, headers=headers) as response:
                    if response.status_code != 200:
                        raise HTTPException(502, "model upstream failed")
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
                return result
        finally:
            self.active -= 1
            await self._record(request_id, raw, started, None, payload)

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
        self.last_usage = usage
        await self.record(request_id, usage)

    async def _stream(self, payload, headers, request_id):
        started, first, raw, total, pending = time.monotonic(), None, None, 0, ""
        decoder = codecs.getincrementaldecoder("utf-8")()
        try:
            async with self.capacity.slot(self.background), httpx.AsyncClient(
                transport=self.transport, timeout=60, trust_env=False, follow_redirects=False) as client:
                await self.authorize("model.request")
                async with client.stream("POST", self.endpoint, json=payload, headers=headers) as response:
                    if response.status_code != 200 or "text/event-stream" not in response.headers.get("content-type", ""):
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
                                    if event.get("usage") is not None:
                                        raw = event["usage"]
                                    for choice in event.get("choices", []):
                                        delta = choice.get("delta", {}) if isinstance(choice, dict) else {}
                                        if first is None and isinstance(delta, dict) and any(
                                            delta.get(key) for key in ("content", "reasoning_content", "tool_calls")
                                        ):
                                            first = time.monotonic() - started
                                            self.ttft = first
                        pending = normalized
                        yield chunk
                    pending += decoder.decode(b"", final=True)
                    if pending.strip():
                        raise RuntimeError("truncated upstream event")
        finally:
            self.active -= 1
            await self._record(request_id, raw, started, first, payload)
