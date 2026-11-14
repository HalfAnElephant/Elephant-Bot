"""Small discovery catalog and single execution/permission gate."""
import asyncio
import hashlib
import inspect
import json
import os
import re
import stat
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from urllib.parse import quote, urlsplit
from uuid import uuid4

from .github import GitHubCLI, classify
from .types import Action, Capability, ToolResult


async def resolve(value):
    return await value if inspect.isawaitable(value) else value


class CapabilityRegistry:
    def __init__(self):
        self.entries = {}

    def register(self, capability, execute, action):
        if capability.name in self.entries:
            raise ValueError("duplicate capability")
        self.entries[capability.name] = (capability, execute, action)

    def search(self, query="", limit=None):
        if not isinstance(query, str) or len(query) > 500:
            raise ValueError("capability query must be a string of at most 500 characters")
        if limit is not None and (type(limit) is not int or limit < 1):
            raise ValueError("capability result limit must be a positive integer")
        query = query.casefold().strip()
        if not query:
            return [asdict(entry[0]) for entry in self.entries.values()][:limit]
        aliases = {
            "gh.exec": "github gh pr pull request repo repository issue review approve merge ci workflow release project list search 仓库 拉取请求 合并请求 审批 批准 工单 持续集成",
            "git.transport": "git clone fetch push repository repo 克隆 拉取 推送 仓库",
            "web.search": "web search internet weather 搜索 联网 查询 天气 预报",
            "web.fetch": "web fetch url link page 网页 网站 抓取 链接",
            "workspace": "workspace file read write edit 文件 工作区 读取 写入 编辑",
            "process": "process command shell bash terminal execute test pytest 命令 终端 执行 运行 测试 进程",
        }
        words = set(re.findall(r"[a-z0-9]+", query))
        chinese = {part[index:index + 2] for part in re.findall(r"[\u4e00-\u9fff]+", query)
                   for index in range(max(1, len(part) - 1))}
        ranked = []
        for capability, _, _ in self.entries.values():
            hints = aliases.get(capability.name, aliases.get(capability.name.split(".")[0], ""))
            text = (capability.name + " " + capability.description + " " + hints).casefold()
            indexed_words = set(re.findall(r"[a-z0-9]+", text))
            score = len(words & indexed_words)
            score += sum(term in text for term in chinese)
            if query == capability.name.casefold():
                score += 100
            if score:
                ranked.append((score, asdict(capability)))
        ranked.sort(key=lambda item: item[0], reverse=True)
        return [capability for _, capability in ranked][:limit]

    def describe(self, name):
        return asdict(self.entries[name][0])


class ToolGateway:
    def __init__(self, registry, *, authority, permission, ledger, verify=None, content_store=None,
                 content_read=None, max_calls=60, prepare=None):
        self.registry, self.authority, self.permission = registry, authority, permission
        self.ledger, self.verify = ledger, verify
        self.prepare = prepare or getattr(verify, "prepare", None)
        self.content_store, self.content_read = content_store, content_read
        self.max_calls, self.calls = max_calls, 0
        self.lock = asyncio.Lock()

    async def invoke(self, name, arguments, context, *, operation_id=None):
        if not isinstance(arguments, dict) or len(json.dumps(arguments).encode()) > 1048576:
            raise ValueError("invalid or oversized capability arguments")
        if not await resolve(self.authority(context)):
            raise PermissionError("run authority expired or revoked")
        if name == "capability.search":
            return ToolResult("ok", self.registry.search(arguments.get("query", "")))
        if name == "capability.describe":
            return ToolResult("ok", self.registry.describe(arguments["name"]))
        if name == "capability.invoke":
            return await self.invoke(arguments["name"], arguments.get("arguments", {}), context,
                                     operation_id=operation_id)
        capability, execute, classifier = self.registry.entries[name]
        arguments = dict(arguments)
        if arguments.get("workspace_ref") not in {None, context.workspace}:
            raise PermissionError("workspace reference does not match host binding")
        if "stdin_ref" in arguments:
            if not self.content_read:
                raise ValueError("content reference reader unavailable")
            arguments["stdin"] = await resolve(self.content_read(context, arguments.pop("stdin_ref")))
        action = classifier(arguments, context)
        decision = await resolve(self.permission(context, action))
        if not (decision if isinstance(decision, bool) else getattr(decision, "allowed", False)):
            raise PermissionError("capability action denied by current policy")
        async with self.lock:
            if self.calls >= self.max_calls:
                raise ValueError("tool call budget exhausted")
            self.calls += 1
        operation_id = operation_id or str(uuid4())
        side_effect = action.effect != "read"
        if side_effect:
            digest = hashlib.sha256(json.dumps({"name": name, "arguments": arguments,
                "action": asdict(action)}, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
            if self.prepare:
                arguments["_preconditions"] = await resolve(self.prepare(context, action, arguments))
            record = await self.ledger.prepare(operation_id, context, action, digest, arguments)
            recorded_operation_id = record.get("operation_id", operation_id)
            semantic_duplicate = recorded_operation_id != operation_id
            operation_id = recorded_operation_id
            if semantic_duplicate or record["state"] != "PREPARED":
                return ToolResult(record["state"].lower(), record.get("result"), operation_id=operation_id,
                                  error="existing operation requires reconciliation before any repeat")
            if not await self.ledger.transition(operation_id, "PREPARED", "DISPATCHED", {}):
                return ToolResult("unknown", operation_id=operation_id, error="operation already claimed")
        try:
            if not await resolve(self.authority(context)):
                raise PermissionError("run authority revoked before dispatch")
            if name in {"web.fetch", "web.search"}:
                loop = asyncio.get_running_loop()

                async def authorize_url(url):
                    redirected = Action(name, ("domain:" + (urlsplit(url).hostname or ""),))
                    decision = await resolve(self.permission(context, redirected))
                    allowed = decision if isinstance(decision, bool) else getattr(decision, "allowed", False)
                    if not await resolve(self.authority(context)) or not allowed:
                        raise PermissionError("public URL denied by current policy")

                arguments["_authorize_url"] = lambda url: asyncio.run_coroutine_threadsafe(
                    authorize_url(url), loop).result(timeout=15)
            data = await asyncio.wait_for(resolve(execute(arguments, context)), capability.timeout_seconds)
            state = "ok"
            if side_effect:
                state = "UNKNOWN"
                if self.verify:
                    verification = await resolve(self.verify(context, action, arguments, data))
                    if verification:
                        state, data = verification
                        if state not in {"VERIFIED", "PARTIAL", "FAILED", "UNKNOWN"}:
                            raise ValueError("invalid verification state")
                await self.ledger.transition(operation_id, "DISPATCHED", state, data)
                state = state.lower()
        except BaseException as error:
            if side_effect:
                await self.ledger.transition(operation_id, "DISPATCHED", "UNKNOWN", {"error": str(error)})
            raise
        if side_effect and state == "verified":
            effect_refs = (operation_id,)
        else:
            effect_refs = ()
        encoded = json.dumps(data, ensure_ascii=False).encode()
        result = ToolResult(state, data, operation_id=operation_id if side_effect else None,
                            retrieved_at=datetime.now(UTC).isoformat(), effect_refs=effect_refs)
        if self.content_store:
            result.content_ref = await resolve(self.content_store(context, encoded))
            result.observation_refs = (result.content_ref,)
        if len(encoded) > capability.max_output_bytes:
            result.data = {"preview": encoded[:capability.max_output_bytes].decode(errors="replace")}
            result.truncated = True
        return result


def workspace_parent(context, relative):
    path = PurePosixPath(relative)
    host_path = getattr(context, "environment", "sandbox") == "host" and path.is_absolute()
    if host_path:
        path = PurePosixPath(Path(relative).resolve())
    if (path.is_absolute() and not host_path or not path.parts
            or any(part in {"..", ".git"} for part in path.parts) and not host_path):
        raise ValueError("path must be a workspace-relative file outside .git")
    directory = os.open("/" if host_path else context.workspace, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in path.parts[1:-1] if host_path else path.parts[:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
            os.close(directory)
            directory = child
        return directory, path.name
    except BaseException:
        os.close(directory)
        raise


def workspace_file(context, relative, flags, mode=0o600):
    directory, name = workspace_parent(context, relative)
    try:
        fd = os.open(name, flags | os.O_NOFOLLOW | os.O_NONBLOCK, mode, dir_fd=directory)
        if not stat.S_ISREG(os.fstat(fd).st_mode) or os.fstat(fd).st_nlink != 1:
            os.close(fd)
            raise ValueError("workspace file must be a regular file without hardlinks")
        return fd
    finally:
        os.close(directory)


def standard_registry(*, github=None, execution=None, deepseek=None):
    from qqbot.adapters.execution.world import LocalExecution
    execution = execution or LocalExecution()
    registry = CapabilityRegistry()
    cli = github or GitHubCLI()
    from .verification import GitHubVerifier
    registry.verify = GitHubVerifier(cli)
    async def web(arguments, context):
        from qqbot.harness.public_web import fetch_public
        if "url" in arguments:
            url = arguments["url"]
            if not isinstance(url, str) or len(url) > 4096:
                raise ValueError("invalid public URL")
            page = await asyncio.to_thread(fetch_public, url, authorize_url=arguments.get("_authorize_url"))
            if deepseek is None:
                return page
            # shortcut: preflight checks local redirects, recheck remote hops when DeepSeek exposes its redirect chain.
            if arguments.get("_authorize_url"):
                await asyncio.to_thread(arguments["_authorize_url"], page["url"])
            parsed = await deepseek.fetch(page["url"])
            if not isinstance(parsed, dict) or not all(isinstance(parsed.get(key), str) for key in ("title", "content")):
                raise ValueError("DeepSeek returned an invalid fetched page")
            text = parsed["content"]
            return {**page, "text": text, "title": parsed["title"], "retrieved_at": datetime.now(UTC).isoformat(),
                    "sha256": hashlib.sha256(text.encode()).hexdigest()}
        else:
            query = arguments["query"]
            if not isinstance(query, str) or not 1 <= len(query) <= 500:
                raise ValueError("search query must be 1..500 characters")
            url = "https://cn.bing.com/search?format=rss&q=" + quote(query)
            if deepseek is None:
                return await asyncio.to_thread(fetch_public, url, authorize_url=arguments.get("_authorize_url"))
            if not query.strip() or len(query) > 128:
                raise ValueError("DeepSeek search requires a nonblank query of at most 128 characters; shorten the query")
            parsed = await deepseek.search(query)
            if (not isinstance(parsed, dict) or not isinstance(parsed.get("results"), list)
                    or any(not isinstance(item, dict) for item in parsed["results"])):
                raise ValueError("DeepSeek returned invalid search results")
            text = json.dumps(parsed["results"], ensure_ascii=False)
            return {"url": "https://api.deepseek.com/v1/tools/search", "query": query,
                    "retrieved_at": datetime.now(UTC).isoformat(), "sha256": hashlib.sha256(text.encode()).hexdigest(),
                    "text": text, "results": parsed["results"]}
    registry.register(Capability("web.fetch", "Fetch public HTTPS source", {"url": {"type": "string"}},
                                timeout_seconds=60 if deepseek else 30), web,
                      lambda a, c: Action("web.fetch", ("domain:" + (urlsplit(a["url"]).hostname or ""),)))
    registry.register(Capability("web.search", "Search fresh public web sources" +
                                ("; DeepSeek queries must contain at most 128 characters" if deepseek else ""),
                                {"query": {"type": "string"}}),
                      web, lambda a, c: Action("web.search", ("domain:api.deepseek.com" if deepseek else "domain:cn.bing.com",)))
    async def read(arguments, context):
        offset, limit = arguments.get("offset", 0), arguments.get("limit", 8192)
        if type(offset) is not int or type(limit) is not int or offset < 0 or not 1 <= limit <= 65536:
            raise ValueError("invalid file page")
        content = await execution.read_file(context, arguments["path"], offset, limit + 1)
        return {"text": content[:limit].decode(errors="replace"),
                "next_offset": offset + limit if len(content) > limit else None}
    async def write(arguments, context):
        content = arguments["text"].encode()
        if len(content) > 1048576:
            raise ValueError("file exceeds write budget")
        await execution.write_file(context, arguments["path"], content)
        return {"path": arguments["path"], "bytes": len(content)}
    for name, handler, effect in (("read", read, "read"), ("write", write, "write")):
        registry.register(Capability("workspace." + name,
            "File " + name + "; workspace-relative paths, or absolute host paths in host environment"), handler,
            lambda a, c, n=name, e=effect: Action("workspace.file." + n,
                                                ("workspace:" + c.workspace, "file:" + a["path"]), e))
    registry.register(Capability("gh.exec", "Execute installed GitHub CLI with literal argv/stdin",
                                {"argv": {"type": "array"}, "stdin": {"type": "string"}}, timeout_seconds=120),
                      cli, lambda a, c: classify(a["argv"], c, a.get("stdin", "")))
    schemas = {
        "web.fetch": {"url": {"type": "string", "maxLength": 4096}},
        "web.search": {"query": {"type": "string", "minLength": 1, "maxLength": 500}},
        "workspace.read": {"path": {"type": "string"}, "offset": {"type": "integer", "minimum": 0},
                           "limit": {"type": "integer", "minimum": 1, "maximum": 65536}},
        "workspace.write": {"path": {"type": "string"}, "text": {"type": "string"}},
        "gh.exec": {"argv": {"type": "array", "items": {"type": "string"}, "minItems": 1},
                    "stdin": {"type": "string"}, "stdin_ref": {"type": "string"},
                    "reviewed_head_sha": {"type": "string"},
                    "timeout_ms": {"type": "integer", "minimum": 1, "maximum": 120000}},
    }
    from dataclasses import replace
    for name, (capability, execute, classifier) in registry.entries.items():
        required = {"web.fetch": ["url"], "web.search": ["query"], "workspace.read": ["path"],
                    "workspace.write": ["path", "text"], "gh.exec": ["argv"]}[name]
        registry.entries[name] = (replace(capability, parameters={"type": "object", "properties": schemas[name],
            "required": required, "additionalProperties": False}), execute, classifier)
    return registry
