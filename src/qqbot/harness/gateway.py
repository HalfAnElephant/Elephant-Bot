import asyncio

"""Per-process binding broker and per-run model/tool gateway, owned by the Worker."""
import hashlib
import hmac
import http.client
import json
import os
import re
import secrets
from contextlib import asynccontextmanager
from dataclasses import asdict
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import StreamingResponse

from qqbot.adapters.hermes_skills import GUIDANCE as SKILLS_GUIDANCE
from qqbot.capabilities.gateway import ToolGateway, standard_registry
from qqbot.capabilities.git import GitService
from qqbot.capabilities.process import ProcessService
from qqbot.capabilities.types import Action, Capability, ToolContext
from qqbot.harness.images import MAX_IMAGE_BYTES, MAX_IMAGES, fetch_image
from qqbot.harness.public_web import bounded_json
from qqbot.providers import ProviderAdapter, validate_transactions
from qqbot.runtime.prompting import assemble_prompt
from qqbot.runtime.tools import native_tools

MAX_TOOL_RESULT_BYTES = 64 * 1024
MAX_CONTENT_BYTES = 20 * 1024 * 1024

GENERAL_PROMPT = assemble_prompt("worker", skills_guidance=SKILLS_GUIDANCE)


class RemoteLedger:
    def __init__(self, call):
        self.call = call

    async def prepare(self, operation_id, context, action, arguments_hash, arguments):
        return await self.call("ledger", {"operation": "prepare", "operation_id": operation_id,
            "action": asdict(action), "arguments_hash": arguments_hash, "arguments": arguments})

    async def transition(self, operation_id, expected, state, result):
        response = await self.call("ledger", {"operation": "transition", "operation_id": operation_id,
            "expected": expected, "state": state, "result": result})
        return response["changed"]


class RunGateway:
    def __init__(self, workspace, model, key, endpoint, *, call, capacity=None, transport=None,
                 github=None, processes=None, model_window=32768, content_root=None, github_host="github.com",
                 icloud=None, upload=None):
        self.workspace = Path(workspace)
        self.model, self.key, self.endpoint = model, key, endpoint
        self.upstream_session = str(uuid4())
        self.call, self.capacity, self.transport = call, capacity, transport
        self.model_window = model_window
        self.github_host = github_host
        self.content_root = Path(content_root) if content_root else self.workspace.parent / ".tool-content"
        self.registry = standard_registry(github=github)
        from qqbot.capabilities.skills import register_skills
        register_skills(self.registry, self.call)
        from qqbot.capabilities.qq import register_qq
        register_qq(self.registry, self.call, workspace=lambda: self.workspace, upload=upload)
        self.icloud = icloud
        if self.icloud is None and os.environ.get("ICLOUD_CALENDAR_CONFIG"):
            from qqbot.capabilities.icloud import ICloudCalendarService
            self.icloud = ICloudCalendarService.from_file(os.environ["ICLOUD_CALENDAR_CONFIG"])
        if self.icloud:
            self.icloud.register(self.registry)
        GitService(credentials=getattr(github, "credentials", None) or (lambda _host: {})).register(self.registry)
        self.processes = processes or ProcessService()
        self.processes.register(self.registry)
        from qqbot.adapters.execution.jobs import register_remote_jobs
        register_remote_jobs(self.registry, self.call)
        self.registry.register(Capability("task.extend_budget", "Extend numeric execution budget within the same goal and total ceiling, with a reason",
            {"type": "object", "properties": {"reason": {"type": "string"}, "model_calls": {"type": "integer"},
                "tool_calls": {"type": "integer"}, "active_seconds": {"type": "integer"}}, "required": ["reason"]}),
            lambda arguments, context: self.call("extend-budget", arguments),
            lambda a, c: Action("task.budget.extend", ("task:" + c.task_id,), "write"))
        self.registry.register(Capability("operation.list", "Read this task's durable external operation history"),
            self.list_operations, lambda a, c: Action("task.operations.read", ("task:" + c.task_id,)))
        self.registry.register(Capability("operation.reconcile", "Read back an UNKNOWN operation; never dispatch it again",
            {"type": "object", "properties": {"operation_id": {"type": "string"}}, "required": ["operation_id"]}),
            self.reconcile_operation, lambda a, c: Action("task.operations.read", ("task:" + c.task_id,)))
        self.process_token = secrets.token_urlsafe(32)
        self.binding = None
        self.generation = 0
        self.revoked = True
        self.inflight = 0
        self.request_tasks = set()
        self.waiting = False
        self.event_sink = None
        self.app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
        self._routes()

    def bind(self, claim):
        if not self.revoked or self.inflight:
            raise RuntimeError("cannot rotate an active binding")
        self.generation += 1
        self.claim = claim
        self.binding = str(uuid4())
        self.tool_token, self.model_token = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
        self.revoked, self.waiting = False, False
        self.outcome, self.evidence, self.artifacts = None, [], []
        # Previously accepted tasks retain their original notification policy.
        self.notify_before_tools = claim.get("context", {}).get("notify_before_tools", False)
        self.progress_announced = False
        self.observations, self.effects, self.content = {}, {}, {}
        self.image_content = None
        self.image_key = None
        self.image_lock = asyncio.Lock()
        self.business_exhausted = False
        self.system_prompt = assemble_prompt(claim.get("role", "worker"),
            purpose=claim.get("purpose", "work"), preferences=claim.get("context", {}).get("preferences", ""),
            skills_guidance=SKILLS_GUIDANCE)
        if self.notify_before_tools:
            self.system_prompt += "\n开始业务工具前必须先单独调用 task_notify，确认宿主接受进度通知后再继续。"
        self.native_tools = native_tools(claim.get("role", "worker"), claim.get("purpose", "work"))
        self.wait_kind = None
        self.session_receipt = None
        budget = claim.get("plan", {}).get("budget", {})
        limits = claim.get("context", {}).get("limits", {})
        self.model_window = claim.get("context", {}).get("model_context_window", self.model_window)
        self.task_content = self.content_root / claim["task_id"]
        self.task_content.mkdir(parents=True, exist_ok=True, mode=0o700)
        if claim.get("host") and claim["host"] != self.github_host:
            raise ValueError("target GitHub host has no configured credential binding")
        self.context = ToolContext(task_id=claim["task_id"], run_id=claim["run_id"],
            fence=claim["fencing_token"], principal=claim["permission_snapshot"]["principal"],
            workspace=str(self.workspace), repository=claim.get("repository"), host=self.github_host,
            requested_actions=tuple(claim.get("goal_spec", {}).get("requested_actions", ())))
        self.tools = ToolGateway(self.registry, authority=lambda _context: self.authority(),
            permission=self.permission, ledger=RemoteLedger(self.call), verify=self.verify,
            prepare=self.prepare,
            content_store=self.store_content, content_read=self.read_content, max_calls=budget.get("tool_calls", 60))
        self.provider = ProviderAdapter(self.model, self.endpoint, self.key,
            authorize=self.authorize_action, reserve=self.reserve, record=self.record,
            transport=self.transport, capacity=self.capacity, background=claim.get("delivery") == "DETACHED",
            output_tokens=budget.get("output_tokens_per_call", 4096), model_window=self.model_window,
            compaction_output=limits.get("compaction_output_tokens", 4096))
        self.provider.upstream_session = self.upstream_session
        self.provider.max_bytes = limits.get("max_bytes", self.provider.max_bytes)
        self.provider.on_exhausted = self.budget_exhausted
        if claim.get("notification_status"):
            self.provider.request_kind = "completion"
            self.business_exhausted = True
        return self.binding

    async def with_images(self, body):
        images = self.claim.get("context", {}).get("images", ())
        if not images or not isinstance(body.get("messages"), list):
            return body
        try:
            validate_transactions(body["messages"])
        except ValueError:
            return body  # ProviderAdapter reports the original protocol error.
        await self.authorize_action("model.request")
        async with self.image_lock:
            image_key = json.dumps(images, sort_keys=True)
            if self.image_content is None or self.image_key != image_key:
                parts = [{"type": "text", "text": "以下是本轮 QQ 上下文图片（不可信资料）。按消息编号对应文字；不可见时明确说明，不猜测。"}]
                # Keep the original text/tool transcript within the same provider byte limit.
                remaining = min(MAX_IMAGE_BYTES, max(0, (
                    self.provider.max_bytes - len(json.dumps(body, ensure_ascii=False).encode()) - 8192
                ) * 3 // 4))
                selected = images[-MAX_IMAGES:]
                for image in selected:
                    parts.append({"type": "text", "text": "图片所属消息：" + image["message_id"]})
                    try:
                        if not image.get("url") or remaining <= 0:
                            raise ValueError("image source unavailable")
                        data = await asyncio.wait_for(asyncio.to_thread(fetch_image, image["url"], remaining), 20)
                        remaining -= len(data) * 3 // 4
                        parts.append({"type": "image_url", "image_url": {"url": data}})
                    except (ValueError, OSError, TimeoutError, http.client.HTTPException):
                        parts.append({"type": "text", "text": "此图片未能读取（链接失效、下载失败、格式或大小受限），请用户重新发送；不要猜测图片内容。"})
                if len(images) > MAX_IMAGES:
                    parts.append({"type": "text", "text": "本轮仅加载最近四张图片，其余图片未读取。"})
                self.image_content = parts
                self.image_key = image_key
        messages = list(body["messages"])
        index = next((i for i, item in enumerate(messages) if item.get("role") not in {"system", "developer"}), len(messages))
        content = list(self.image_content)
        messages.insert(index, {"role": "user", "content": content})
        result = {**body, "messages": messages}
        # Tool output can grow after the download; cached images must still fit this call.
        for part in sorted((part for part in content if part["type"] == "image_url"),
                           key=lambda part: len(part["image_url"]["url"]), reverse=True):
            if len(json.dumps(result, ensure_ascii=False).encode()) <= self.provider.max_bytes - 2048:
                break
            content[content.index(part)] = {"type": "text", "text": "此图片因本轮请求大小限制未提交，不能据此猜测图片内容。"}
        return result

    def budget_exhausted(self):
        self.business_exhausted = True

    async def authority(self):
        if self.revoked:
            return False
        try:
            result = await self.call("authorize", {"action": "task.execute"})
            return result["allowed"]
        except (PermissionError, RuntimeError):
            return False

    async def authorize_action(self, action):
        if self.revoked:
            raise HTTPException(403, "run binding revoked")
        return await self.call("authorize", {"action": action})

    async def permission(self, context, action):
        if self.waiting or self.business_exhausted:
            return False
        result = await self.call("authorize", {"action": action.name, "resources": action.resources,
            "effect": action.effect})
        return result["allowed"]

    async def reserve(self, request_id, kind):
        return await self.call("reserve-budget", {"request_id": request_id, "kind": kind})

    async def record(self, request_id, usage):
        await self.call("usage", {"request_id": request_id, "usage": usage})

    async def prepare(self, context, action, arguments):
        if action.name == "workspace.job.start":
            return await self.call("jobs", {"operation": "prepare", "arguments": arguments})
        if action.name == "workspace.job.cancel":
            receipt = await self.call("jobs", {"operation": "inspect", "arguments": {"job_id": arguments["job_id"]}})
            return {"job_id": receipt["job_id"], "invocation_id": receipt["invocation_id"]}
        if self.icloud and action.name.startswith("icloud."):
            return await self.icloud.prepare(context, action, arguments)
        prepare = getattr(getattr(self.registry, "verify", None), "prepare", None)
        return await prepare(context, action, arguments) if prepare else {}

    async def verify(self, context, action, arguments, data):
        if action.name in {"workspace.job.start", "workspace.job.cancel"}:
            if action.name == "workspace.job.start":
                receipt = await self.call("jobs", {"operation": "lookup", "arguments": {"job_key": arguments["job_key"]}})
                if receipt.get("invocation_id"):
                    return "VERIFIED", {**receipt, "verified_effect": "supervised_job_dispatch",
                        "execution_succeeded": receipt["state"] == "EXITED" and receipt.get("exit_code") == 0}
            else:
                receipt = await self.call("jobs", {"operation": "inspect", "arguments": {"job_id": arguments["job_id"]}})
                if receipt["state"] in {"EXITED", "FAILED", "CANCELLED"}:
                    return "VERIFIED", {**receipt, "verified_effect": "job_stopped"}
            return "UNKNOWN", receipt
        if self.icloud and action.name.startswith("icloud."):
            return await self.icloud.verify(context, action, arguments, data)
        if action.name == "notification.send" and isinstance(data, dict) and data.get("binding_created"):
            return "VERIFIED", data
        if action.name == "notification.send" and isinstance(data, dict) and data.get("delivery_id"):
            # Verification is of durable enqueue only, never a claim of remote delivery.
            status = await self.call("qq-status", {"delivery_id": data["delivery_id"]})
            return "VERIFIED", {**status, "verified_effect": "durable_enqueue"}
        if action.name == "task.budget.extend" and data.get("accepted"):
            return "VERIFIED", data
        if action.name.startswith("workspace.") and isinstance(data, dict) and data.get("path"):
            path = self.workspace / data["path"]
            if path.is_file() and not path.is_symlink():
                if data["path"].startswith("outputs/") or data["path"] in self.claim.get("goal_spec", {}).get("requested_outputs", []):
                    self.artifacts.append(data["path"])
                return "VERIFIED", data
        verifier = getattr(self.registry, "verify", None)
        if verifier:
            return await verifier(context, action, arguments, data)
        return "UNKNOWN", data

    async def store_content(self, context, content):
        digest = hashlib.sha256(content).hexdigest()
        destination = self.task_content / digest
        if not destination.exists():
            if sum(path.stat().st_size for path in self.task_content.iterdir()) + len(content) > MAX_CONTENT_BYTES:
                raise ValueError("task content archive budget exhausted")
            with destination.open("xb") as output:
                output.write(content)
                output.flush()
                os.fsync(output.fileno())
        return "content:" + digest

    async def read_content(self, context, reference):
        if not isinstance(reference, str) or not re.fullmatch(r"content:[0-9a-f]{64}", reference):
            raise ValueError("invalid content reference; use a returned content_ref in content:<64 hex characters> format")
        destination = self.task_content / reference[8:]
        try:
            with os.fdopen(os.open(destination, os.O_RDONLY | os.O_NOFOLLOW), "rb") as file:
                content = file.read(MAX_CONTENT_BYTES + 1)
        except FileNotFoundError as error:
            raise ValueError("content reference is unavailable for this task; use a returned content_ref") from error
        if len(content) > MAX_CONTENT_BYTES or hashlib.sha256(content).hexdigest() != reference[8:]:
            raise ValueError("content archive hash mismatch")
        return content.decode()

    def safe_error(self, error):
        message = str(error)
        for secret in (self.key, self.model_token, self.tool_token, self.process_token):
            if secret:
                message = message.replace(secret, "[REDACTED]")
        return re.sub(r"(?i)\bBearer\s+[A-Za-z0-9._~+/-]+=*", "Bearer [REDACTED]", message)[:1000]

    async def list_operations(self, arguments, context):
        return await self.call("ledger", {"operation": "list"})

    async def reconcile_operation(self, arguments, context):
        rows = await self.call("ledger", {"operation": "read", "operation_id": arguments["operation_id"]})
        if len(rows) != 1:
            raise ValueError("unknown operation")
        record = rows[0]
        if record["state"] != "UNKNOWN":
            return {"state": record["state"], "result": record["result"]}
        action = Action(**{**record["action"], "resources": tuple(record["action"]["resources"])})
        if not await self.permission(context, action):
            raise PermissionError("operation readback is no longer authorized")
        verification = await self.verify(context, action, record["arguments"], record["result"])
        if verification and verification[0] != "UNKNOWN":
            state, result = verification
            await self.call("ledger", {"operation": "transition", "operation_id": record["id"],
                "expected": "UNKNOWN", "state": state, "result": result})
            self.effects[record["id"]] = state.lower()
            return {"state": state, "result": result}
        return {"state": "UNKNOWN", "result": record["result"]}

    async def authenticate(self, header, token):
        if not hmac.compare_digest(header, "Bearer " + token) or not await self.authority():
            raise HTTPException(403, "run capability expired or revoked")

    async def drain(self):
        if self.inflight or self.provider.active:
            return False
        if self.processes.active_run(self.context):
            return False
        await self.processes.close_run(self.context)
        self.revoked = True
        return True

    def revoke(self):
        self.revoked = True

    @asynccontextmanager
    async def request_scope(self, authorization, kind):
        task, generation = asyncio.current_task(), self.generation
        token = self.process_token if kind == "binding" else self.model_token if kind == "model" else self.tool_token
        self.inflight += 1
        self.request_tasks.add(task)
        try:
            await self.authenticate(authorization, token)
            if generation != self.generation:
                raise HTTPException(403, "binding changed while authenticating")
            yield generation
        finally:
            self.inflight -= 1
            self.request_tasks.discard(task)

    async def stop_requests(self):
        self.revoke()
        pending = {task for task in self.request_tasks if task is not asyncio.current_task() and not task.done()}
        for task in pending:
            task.cancel()
        if pending:
            _, pending = await asyncio.wait(pending, timeout=10)
        return not pending and self.inflight == 0 and getattr(getattr(self, "provider", None), "active", 0) == 0

    def _routes(self):
        @self.app.post("/binding")
        async def binding(request: Request, authorization: str = Header(default="")):
            async with self.request_scope(authorization, "binding") as generation:
                packet = await bounded_json(request, 1024)
                if packet.get("binding_id") != self.binding or not await self.authority():
                    raise HTTPException(403, "binding no longer active")
                if generation != self.generation or self.revoked:
                    raise HTTPException(403, "binding no longer active")
                context = self.claim.get("context", {})
                return {"generation": self.generation, "model": self.model, "model_token": self.model_token,
                    "tool_token": self.tool_token, "system_prompt": self.system_prompt,
                    "native_tools": list(self.native_tools.values()), "role": self.claim.get("role", "worker"),
                    "repository": self.context.repository, "github_host": self.context.host,
                    "model_window": self.model_window, "output_tokens": max(self.provider.output_tokens, self.provider.compaction_output),
                    "session_id": self.claim.get("session_id"), "run_id": self.claim["run_id"],
                    "input_ids": context.get("source_input_ids", []),
                    "source_kind": "user" if self.claim.get("role") == "leader" and self.claim.get("purpose") == "user" else "relay",
                    "goal": self.claim["goal"], "goal_spec": self.claim.get("goal_spec", {}),
                    "budget": self.claim.get("plan", {}).get("budget", {}),
                    "retrieved_data": context.get("retrieved_data", []),
                    "memory_status": context.get("memory_status", "disabled"),
                    "current_time": context.get("now"), "timezone": context.get("timezone", "Asia/Shanghai"), "continuation": self.claim.get("continuation", {})}

        @self.app.post("/session-receipt")
        async def session_receipt(request: Request, authorization: str = Header(default="")):
            async with self.request_scope(authorization, "tool"):
                await self.authenticate(authorization, self.tool_token)
                body = await bounded_json(request, 256 * 1024)
                body["inflight"] = max(0, self.inflight - 1)
                if body.get("kind") in {"turn_finished", "run_yielded"}:
                    body["result"] = {"status": self.wait_kind or (self.outcome or {}).get("status", "completed"),
                        "summary": body.get("summary") or "等待条件已持久化。", "agent_authored": True,
                        "observation_refs": list(self.observations), "effect_refs": list(self.effects),
                        "limitations": (self.outcome or {}).get("unresolved_items", [])}
                    body["attachments"] = list(self.artifacts)
                    self.session_receipt = body
                return await self.call("session-receipt", body)

        @self.app.post("/mode")
        async def mode(request: Request, authorization: str = Header(default="")):
            async with self.request_scope(authorization, "tool"):
                await self.authenticate(authorization, self.tool_token)
                body = await bounded_json(request, 1024)
                if body.get("kind") not in {"business", "compaction", "completion"}:
                    raise HTTPException(422, "invalid trusted request kind")
                self.provider.request_kind = body["kind"]
                return {"accepted": True}

        @self.app.post("/v1/chat/completions")
        async def model(request: Request, authorization: str = Header(default="")):
            scope = self.request_scope(authorization, "model")
            generation = await scope.__aenter__()
            streaming = False
            try:
                provider = self.provider
                body = await bounded_json(request, provider.max_bytes)
                await self.authenticate(authorization, self.model_token)
                body = await self.with_images(body)
                if generation != self.generation or self.revoked:
                    raise HTTPException(403, "binding changed while loading images")
                response = await provider.complete(body)
                if isinstance(response, StreamingResponse):
                    source = response.body_iterator
                    async def stream():
                        try:
                            async for chunk in source:
                                yield chunk
                        finally:
                            try:
                                await source.aclose()
                            finally:
                                await scope.__aexit__(None, None, None)
                    response.body_iterator = stream()
                    streaming = True
                return response
            finally:
                if not streaming:
                    await scope.__aexit__(None, None, None)

        @self.app.post("/tools/{name}")
        async def tool(name: str, request: Request, authorization: str = Header(default="")):
            async with self.request_scope(authorization, "tool") as generation:
                body = await bounded_json(request, 1024 * 1024)
                await self.authenticate(authorization, self.tool_token)
                args, call_id = body.get("arguments", {}), body.get("call_id")
                if not isinstance(call_id, str) or not 1 <= len(call_id) <= 200 or not isinstance(args, dict):
                    raise HTTPException(422, "tool call identity and arguments required")
                operation_id = hashlib.sha256((self.claim["run_id"] + ":" + call_id).encode()).hexdigest()
                try:
                    await self.reserve(operation_id, "tool")
                    logical_name = args.get("name") if name == "capability_invoke" else name.replace("tasks_", "tasks.", 1)
                    logical_args = args.get("arguments", {}) if name == "capability_invoke" else args
                    if self.claim.get("role") == "skill_review" and logical_name not in {"skills_list", "skill_view", "skill_manage"}:
                        raise PermissionError("skill review only supports Hermes tools")
                    if logical_name in self.native_tools:
                        if logical_name.startswith("memory_"):
                            result = await self.call("memory", {"operation": logical_name, "arguments": logical_args, "call_id": call_id})
                            if result.get("end_turn"):
                                self.waiting, self.wait_kind = True, "completed"
                            return result
                        if logical_name.startswith("tasks."):
                            result = await self.call("tasks", {"operation": logical_name, "arguments": logical_args, "call_id": call_id})
                            if isinstance(result, dict) and result.get("waiting"):
                                self.waiting, self.wait_kind = True, "waiting_children"
                            return result
                        await self.authorize_action("skills.write" if logical_name == "skill_manage" else "skills.read")
                        return await self.call("skills", {"operation": logical_name, "arguments": logical_args, "call_id": call_id})
                    if name == "task_notify":
                        notice = {**args, "notification_id": operation_id}
                        if self.notify_before_tools and not self.progress_announced:
                            notice["importance"] = "important"
                        result = await self.call("notify", notice)
                        if result.get("accepted"):
                            self.progress_announced = True
                        return result
                    if name in {"task_ask_user", "task_wait_external"}:
                        result = await self.call("ask", {**args, "question_id": operation_id,
                            "kind": "input" if name == "task_ask_user" else "external"})
                        self.waiting = True
                        self.wait_kind = "waiting_input" if name == "task_ask_user" else "waiting_external"
                        return result
                    if self.waiting:
                        raise PermissionError("wait intent blocks subsequent tools")
                    if name == "task_outcome":
                        status = args.get("status")
                        if status not in {"completed", "partial", "blocked", "failed"}:
                            raise ValueError("invalid goal outcome")
                        attachments = args.get("attachments", [])
                        if not isinstance(attachments, list) or len(attachments) > 20 or any(
                                not isinstance(path, str) or not 1 <= len(path) <= 1024 for path in attachments):
                            raise ValueError("invalid artifact paths")
                        self.outcome = args
                        self.artifacts.extend(path for path in attachments if path not in self.artifacts)
                        return {"accepted": True}
                    if self.notify_before_tools and not self.progress_announced:
                        raise ValueError("先单独调用 task_notify，简短告知用户准备做什么，提示被接受后再调用工具。")
                    if name == "content_read":
                        text = await self.read_content(self.context, args["reference"])
                        offset, limit = args.get("offset", 0), args.get("limit", 16000)
                        if type(offset) is not int or type(limit) is not int or offset < 0 or not 1 <= limit <= 65536:
                            raise ValueError("invalid content page")
                        return {"text": text[offset:offset + limit], "next_offset": offset + limit if len(text) > offset + limit else None}
                    logical = {"capability_search": "capability.search", "capability_invoke": "capability.invoke"}.get(name)
                    if not logical:
                        raise ValueError("unknown bridge tool")
                    result = await self.tools.invoke(logical, args, self.context, operation_id=operation_id)
                    if generation != self.generation or self.revoked:
                        raise PermissionError("late response from revoked generation")
                    encoded = asdict(result)
                    if result.retrieved_at:
                        observation = "observation:" + operation_id
                        self.observations[observation] = encoded
                        await self.call("observation", {"id": observation, "retrieved_at": result.retrieved_at,
                            "sha256": hashlib.sha256(json.dumps(encoded, ensure_ascii=False, sort_keys=True).encode()).hexdigest(),
                            "data": {"status": result.status, "content_ref": result.content_ref, "operation_id": result.operation_id}})
                        encoded["observation_refs"] = [observation]
                    if result.operation_id:
                        self.effects[result.operation_id] = result.status
                    if isinstance(result.data, dict) and all(k in result.data for k in ("url", "retrieved_at", "sha256")):
                        self.evidence.append({k: result.data[k] for k in ("url", "retrieved_at", "sha256")})
                    full_output = json.dumps(encoded, ensure_ascii=False).encode()
                    if len(full_output) > MAX_TOOL_RESULT_BYTES:
                        reference = await self.store_content(self.context, full_output)
                        encoded["data"] = {"preview": full_output[:16384].decode(errors="replace"),
                                           "note": "Read the full result using content_ref"}
                        encoded["content_ref"] = reference
                        encoded["truncated"] = True
                    return encoded
                except (KeyError, ValueError, PermissionError) as error:
                    raise HTTPException(422, self.safe_error(error)) from error
