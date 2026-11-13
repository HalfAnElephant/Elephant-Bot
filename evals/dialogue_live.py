"""Opt-in real DeepSeek/Pi dialogue acceptance. Run from the repository root.

Only the model uses the network. QQ delivery and business observations are synthetic.
The JSON report contains metrics and synthetic replies, never session files or credentials.
"""

import asyncio
import json
import os
import re
import shlex
import shutil
import sys
import tempfile
import time
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import httpx
from sqlalchemy import select

from qqbot.app import create_app
from qqbot.capabilities.gateway import CapabilityRegistry
from qqbot.capabilities.types import Action, Capability
from qqbot.config import Settings
from qqbot.harness.gateway import RunGateway
from qqbot.harness.pi import PiRunnerAdapter
from qqbot.runtime.scheduler import RuntimeClient
from qqbot.storage.agent_schema import dialogue_inputs
from qqbot.storage.schema import outbox
from qqbot.storage.task_schema import tasks

MODEL = "deepseek-flash"
ENDPOINT = "https://api.deepseek.com/chat/completions"
REPORT = Path("test-results/dialogue-live.json")
CASE_TIMEOUT = 240

# SessionManager writes a paired, completed synthetic transcript in the actual Pi format.
LEGACY_HISTORY = r"""
import {readFileSync} from 'node:fs';
const body = JSON.parse(readFileSync(0, 'utf8'));
const {SessionManager} = await import(body.module);
const session = SessionManager.open(body.path);
const usage = {input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2,
  cost: {input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0}};
for (let index = 0; index < 6; index++) {
  const id = 'synthetic-legacy-' + index;
  session.appendMessage({role: 'user', content: [{type: 'text', text: '查询过去的合成测试资料'}], timestamp: Date.now()});
  session.appendMessage({role: 'assistant', content: [{type: 'toolCall', id,
    name: 'capability_invoke', arguments: {name: 'archive.lookup', arguments: {index}}}],
    api: 'openai-completions', provider: 'qqbot', model: 'deepseek-flash',
    stopReason: 'toolUse', usage, timestamp: Date.now()});
  session.appendMessage({role: 'toolResult', toolCallId: id, toolName: 'capability_invoke',
    content: [{type: 'text', text: JSON.stringify({status: 'ok', data: {archived: true, index}})}],
    isError: false, timestamp: Date.now()});
  session.appendMessage({role: 'assistant', content: [{type: 'text', text: '过去的合成资料已查到。'}],
    api: 'openai-completions', provider: 'qqbot', model: 'deepseek-flash',
    stopReason: 'stop', usage, timestamp: Date.now()});
}
"""


def require(condition, message):
    if not condition:
        raise AssertionError(message)


class LiveModelTransport(httpx.AsyncBaseTransport):
    """Allow only the official model endpoint and explicitly disable thinking for this suite."""

    def __init__(self, report):
        self.report = report
        self.transport = httpx.AsyncHTTPTransport(retries=0)

    async def handle_async_request(self, request):
        require(str(request.url) == ENDPOINT, "unexpected external model endpoint")
        body = json.loads(await request.aread())
        require(body.get("model") == MODEL, "unexpected model")
        body["reasoning_effort"] = "none"
        schemas = {tool["function"]["name"]: tool["function"]["parameters"]
                   for tool in body.get("tools", [])}
        self.report["requests"].append({
            "communication_schemas": {name: "user_message" in schema.get("required", [])
                                      for name, schema in schemas.items()
                                      if name in {"capability_search", "capability_invoke", "content_read"}},
            "has_output_limit": "max_tokens" in body or "max_completion_tokens" in body,
        })
        forwarded = httpx.Request(request.method, request.url,
                                 headers={key: value for key, value in request.headers.items()
                                          if key.lower() != "content-length"},
                                 content=json.dumps(body).encode(), extensions=request.extensions)
        response = await self.transport.handle_async_request(forwarded)
        self.report["requests"][-1]["http_status"] = response.status_code
        return response

    async def aclose(self):
        # ProviderAdapter creates a client for each request. The suite owns this shared transport.
        pass


class DialogueSuite:
    def __init__(self, root, report):
        self.root, self.report = root, report
        self.current = None
        self.seeded = False
        self.transport = LiveModelTransport(report)

    def registry(self):
        registry = CapabilityRegistry()
        definitions = [
            ("weather.resolve", "Resolve 嘉定 Jiading 天气 预报 weather forecast location; returns the forecast lookup ID.",
             {"query": {"type": "string"}}, ["query"]),
            ("weather.forecast", "Read today's synthetic 天气 weather after weather.resolve, using its location_id.",
             {"location_id": {"type": "string"}}, ["location_id"]),
            ("github.resolve", "Resolve yourtjhub YourTJ-Hub GitHub 仓库 repository before counting issues.",
             {"query": {"type": "string"}}, ["query"]),
            ("github.issues", "Count current synthetic GitHub issues after github.resolve using repository_id.",
             {"repository_id": {"type": "string"}}, ["repository_id"]),
        ]
        for name, description, properties, required in definitions:
            async def execute(arguments, context, name=name):
                return await self.business(name, arguments)
            registry.register(Capability(name, description, {"type": "object", "properties": properties,
                                                           "required": required, "additionalProperties": False}),
                              execute, lambda arguments, context: Action("fixture.read", ()))
        return registry

    async def business(self, name, arguments):
        case = self.current
        case["business_calls"].append(name)
        if name == "weather.resolve":
            return {"location_id": "synthetic-jiading", "next": "weather.forecast"}
        if name == "github.resolve":
            return {"repository_id": "synthetic-yourtj", "next": "github.issues"}
        if name == "weather.forecast":
            require(arguments.get("location_id") == "synthetic-jiading", "forecast requires resolved location")
            require("weather.resolve" in case["business_calls"], "forecast resolution was skipped")
            data = {"location": "嘉定", "temperature_c": 18, "condition": "小雨", "synthetic": True}
        else:
            require(arguments.get("repository_id") == "synthetic-yourtj", "issues require resolved repository")
            require("github.resolve" in case["business_calls"], "repository resolution was skipped")
            data = {"repository": "YourTongji/YourTJ-Hub", "open_issues": 17, "synthetic": True}
        case["business_completed_seconds"] = self.elapsed()
        return data

    def elapsed(self):
        return round(time.perf_counter() - self.current["started"], 3)

    def receiver(self, request):
        if request.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "synthetic-token", "expires_in": 7200})
        require(request.url.path.endswith("/messages"), "unexpected QQ receiver operation")
        packet = json.loads(request.content)
        self.current["received"].append({"seconds": self.elapsed(), "body": packet})
        return httpx.Response(200, json={"id": "synthetic-" + uuid4().hex})

    async def seed_history(self, runner, original_start):
        if not self.seeded and runner.gateway.claim.get("role") == "leader":
            runner.session_path.parent.mkdir(parents=True, exist_ok=True)
            module = Path("node_modules/.bin/pi").resolve().parents[1] / "index.js"
            process = await asyncio.create_subprocess_exec(
                "node", "--input-type=module", "-e", LEGACY_HISTORY,
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            await asyncio.wait_for(process.communicate(json.dumps({"module": module.as_uri(),
                                                                  "path": str(runner.session_path)}).encode()), 20)
            require(process.returncode == 0, "native synthetic history construction failed")
            self.seeded = True
        return await original_start(runner)

    async def case(self, app, label, text):
        core = app.state.core
        case = {"label": label, "input": text, "started": time.perf_counter(), "received": [],
                "claims": [], "business_calls": [], "business_completed_seconds": None, "runner_failed": False,
                "passed": False}
        self.current = case
        self.report["cases"].append(case)
        packet = {"op": 0, "id": uuid4().hex, "t": "C2C_MESSAGE_CREATE", "d": {
            "id": uuid4().hex, "author": {"user_openid": "synthetic-user"}, "content": text,
            "timestamp": core.clock.now().isoformat()}}
        raw, stamp = json.dumps(packet).encode(), str(int(time.time()))
        signed = core.adapters["live-eval"].key().sign(stamp.encode() + raw).hex()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://fixture") as inbound:
            response = await inbound.post("/webhooks/qq/live-eval", content=raw, headers={
                "x-signature-timestamp": stamp, "x-signature-ed25519": signed})
        require(response.status_code == 200 and response.json() == {"op": 12}, "signed QQ ingress failed")
        deadline = time.monotonic() + CASE_TIMEOUT
        while time.monotonic() < deadline:
            await asyncio.sleep(0.1)
            if app.state.core_task.done():
                raise AssertionError("Core loop stopped")
            require(not case["runner_failed"], "Runner failed before completing the dialogue case")
            with core.db.engine.connect() as conn:
                deliveries = [dict(row) for row in conn.execute(select(outbox)).mappings()
                              if json.loads(row["envelope_json"]).get("anchor_message_id") == packet["d"]["id"]]
                pending = conn.execute(select(dialogue_inputs.c.id).where(
                    dialogue_inputs.c.handled_at.is_(None))).first()
                roots = [dict(row) for row in conn.execute(select(tasks).where(tasks.c.kind == "goal")).mappings()]
                children = conn.execute(select(tasks.c.id).where(tasks.c.kind != "goal")).all()
            sent = [(row, json.loads(row["envelope_json"])) for row in deliveries if row["state"] == "SENT"]
            finals = [envelope for _, envelope in sent if envelope.get("purpose") == "result"]
            final_rows = [row for row in deliveries if json.loads(row["envelope_json"]).get("purpose") == "result"]
            if finals and final_rows and all(row["state"] == "SENT" for row in final_rows) and not pending:
                case["duration_seconds"] = self.elapsed()
                case["first_reply_seconds"] = case["received"][0]["seconds"]
                case["replies"] = [{"purpose": envelope.get("purpose"), "text": envelope["text"],
                                    "sent_at": row["sent_at"]} for row, envelope in sent]
                worker_claims = [claim for claim in case["claims"] if claim["role"] == "worker"]
                if label == "webpage":
                    require(roots and children, "webpage did not persist a delegated goal and child")
                    require(not worker_claims, "dispatch-only case unexpectedly ran a Worker")
                    case["dispatch_observed"] = True
                    case["worker_result_verified"] = False
                else:
                    require(not worker_claims and not roots, "short dialogue unexpectedly delegated")
                    if label in {"hello", "available"}:
                        require(not case["business_calls"], "greeting unexpectedly invoked business tools")
                        require(not any(envelope.get("purpose") == "progress" for _, envelope in sent),
                                "greeting unexpectedly sent progress")
                    if label in {"weather", "issues_legacy"}:
                        require(case["business_completed_seconds"] is not None, "business query did not complete")
                        progress = [(row, envelope) for row, envelope in sent if envelope.get("purpose") == "progress"]
                        require(progress, "query had no autonomous progress delivery")
                        final_time = min(row["sent_at"] for row, envelope in sent if envelope.get("purpose") == "result")
                        require(any(row["sent_at"] < final_time for row, _ in progress), "progress did not precede final delivery")
                        number = r"(?<!\d)18(?!\d)|十八" if label == "weather" else r"(?<!\d)17(?!\d)|十七"
                        require(re.search(number, "\n".join(item["text"] for item in finals)),
                                "final reply omitted the observed fixture result")
                case["passed"] = True
                return
        raise AssertionError("dialogue case timed out")

    async def run(self, cases=None):
        os.environ["DIALOGUE_EVAL_QQ_SECRET"] = "synthetic-official-signature-secret"
        node = shutil.which("node")
        require(node is not None, "Node installation is required")
        executable = self.root / "pi-command"
        # Pi's isolated PATH omits Actions setup-node directories. Pin the installed Node explicitly.
        executable.write_text("#!/bin/sh\nexec " + shlex.quote(node) + " "
                              + shlex.quote(str(Path("node_modules/.bin/pi").resolve())) + ' "$@"\n')
        executable.chmod(0o700)
        settings = Settings.model_validate({
            "database": str(self.root / "core.db"), "internal_socket": str(self.root / "internal.sock"),
            "orchestration": {"pi_executable": str(executable)},
            "model": {"model": MODEL, "endpoint": ENDPOINT, "api_key_env": "DEEPSEEK_API_KEY", "context_window": 1000000},
            "adapters": [{"instance_id": "live-eval", "kind": "official", "bot_id": "synthetic-bot",
                          "app_id": "synthetic-app", "secret_env": "DIALOGUE_EVAL_QQ_SECRET", "proactive_verified": True,
                          "c2c_reply_window_seconds": 3600}],
            "owner": {"id": "dialogue-live", "qq_bindings": [{"app_id": "synthetic-app", "user_openid": "synthetic-user",
                                                             "verified_at": "2026-01-01T00:00:00Z"}]},
            "skills": {"enabled": False, "background_review_enabled": False, "owner_storage_root": str(self.root / "hermes")},
            "memory": {"openviking": {"private_enabled": False, "group_enabled": False}},
        }, strict=False)
        original_gateway, original_start, original_invoke = RunGateway.__init__, PiRunnerAdapter.start, RuntimeClient.invoke

        def gateway_init(gateway, *args, **kwargs):
            original_gateway(gateway, *args, **kwargs)
            gateway.registry.entries = self.registry().entries
            gateway.transport = self.transport

        async def start(runner):
            return await self.seed_history(runner, original_start)

        async def invoke(client, operation, body):
            if operation == "claim" and self.current and self.current["label"] == "webpage":
                # Observe durable dispatch and the real Leader confirmation without buying a long Worker run.
                with patch.object(client.service, "claim", return_value=None):
                    result = await original_invoke(client, operation, body)
            else:
                result = await original_invoke(client, operation, body)
            if operation == "claim" and result and self.current:
                self.current["claims"].append({"role": result.get("role"), "purpose": result.get("purpose"),
                                               "session_id": result.get("session_id"), "seconds": self.elapsed()})
            if operation == "result" and self.current and not body.result.agent_authored:
                self.current["runner_failed"] = True
            return result

        with patch.object(RunGateway, "__init__", gateway_init), patch.object(PiRunnerAdapter, "start", start), \
                patch.object(RuntimeClient, "invoke", invoke):
            async with httpx.AsyncClient(transport=httpx.MockTransport(self.receiver)) as receiver:
                app = create_app(settings, official_client=receiver)
                async with app.router.lifespan_context(app):
                    for label, text in cases or [("hello", "Hello～"), ("available", "在吗？"),
                                                ("weather", "查看一下今天嘉定的天气"),
                                                ("issues_legacy", "查看一下 yourtjhub 现在有多少 Issue"),
                                                ("webpage", "帮我写一个网页，里面用 svg 描绘一个秦始皇骑北极熊的动画")]:
                        await self.case(app, label, text)
                    sessions = {claim["session_id"] for case in self.report["cases"] for claim in case["claims"]
                                if claim["role"] == "leader"}
                    require(self.seeded and len(sessions) == 1, "cases did not reuse the seeded native Session")
                    require(self.report["requests"], "no real model requests were observed")
                    expected = {"capability_search", "capability_invoke", "content_read"}
                    for request in self.report["requests"]:
                        schemas = request["communication_schemas"]
                        require(set(schemas) == expected and all(schemas.values()),
                                "communication schemas missing or lost required choice")


async def main():
    report = {"model": MODEL, "endpoint": ENDPOINT, "thinking": "disabled via reasoning_effort=none at test transport",
              "synthetic_business": True, "legacy_paired_turns": 6, "cases": [], "requests": [], "passed": False}
    suite = None
    try:
        require(bool(os.environ.get("DEEPSEEK_API_KEY")), "DEEPSEEK_API_KEY is required")
        require(Path("node_modules/.bin/pi").is_file(), "locked Pi installation is required")
        with tempfile.TemporaryDirectory(prefix="elephant-dialogue-live-") as directory:
            suite = DialogueSuite(Path(directory), report)
            await asyncio.wait_for(suite.run(), timeout=720)
        report["passed"] = True
    except Exception as error:  # noqa: BLE001 — persist a sanitized failure for every failed live run.
        # HTTP/provider exceptions may contain headers or body. Keep those outside CI artifacts.
        report["failure"] = str(error) if isinstance(error, AssertionError) else type(error).__name__
        if isinstance(error, httpx.HTTPStatusError):
            report["failure_http_status"] = error.response.status_code
    finally:
        if suite:
            await suite.transport.transport.aclose()
        for case in report["cases"]:
            started = case.pop("started", None)
            if started is not None:
                case.setdefault("duration_seconds", round(time.perf_counter() - started, 3))
            case.pop("received", None)
        REPORT.parent.mkdir(parents=True, exist_ok=True)
        REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(f"Dialogue live acceptance {'passed' if report['passed'] else 'failed'}. Report {REPORT}")
    return 0 if report["passed"] else 1


async def self_check():
    """Check fixtures and native history construction without credentials or any HTTP request."""
    from types import SimpleNamespace

    from qqbot.adapters.pi.sessions import load_entries, unfinished_tools

    with tempfile.TemporaryDirectory(prefix="elephant-dialogue-self-check-") as directory:
        root = Path(directory)
        suite = DialogueSuite(root, {"requests": [], "cases": []})
        try:
            suite.current = {"started": time.perf_counter(), "business_calls": []}
            registry = suite.registry()
            require(registry.search("天气"), "weather fixture is undiscoverable")
            require(registry.search("GitHub Issue"), "GitHub fixture is undiscoverable")
            await suite.business("weather.resolve", {"query": "嘉定"})
            await suite.business("weather.forecast", {"location_id": "synthetic-jiading"})
            await suite.business("github.resolve", {"query": "yourtjhub"})
            data = await suite.business("github.issues", {"repository_id": "synthetic-yourtj"})
            require(data["open_issues"] == 17, "fixture result changed")
            runner = SimpleNamespace(session_path=root / "sessions" / "synthetic.jsonl",
                                     gateway=SimpleNamespace(claim={"role": "leader"}))
            async def no_start(runner):
                return None
            await suite.seed_history(runner, no_start)
            entries = load_entries(runner.session_path)
            calls = [part for entry in entries for part in entry.get("message", {}).get("content", [])
                     if isinstance(part, dict) and part.get("type") == "toolCall"]
            require(len(calls) == 6 and all("user_message" not in call["arguments"] for call in calls),
                    "legacy fixture no longer reproduces missing communication choice")
            require(not unfinished_tools(runner.session_path), "native history contains unpaired tools")
            suite.seeded = False
            await suite.transport.transport.aclose()
            def model_fixture(request):
                body = json.loads(request.content)
                require(body["reasoning_effort"] == "none", "test transport did not disable thinking")
                chunk = {"id": "synthetic-completion", "object": "chat.completion.chunk", "model": MODEL,
                         "created": 1, "choices": [{"index": 0, "delta": {"role": "assistant", "content": "在呀～"},
                                                    "finish_reason": None}]}
                final = {**chunk, "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
                         "usage": {"prompt_tokens": 100, "completion_tokens": 3}}
                content = "data: " + json.dumps(chunk) + "\n\ndata: " + json.dumps(final) + "\n\ndata: [DONE]\n\n"
                return httpx.Response(200, headers={"content-type": "text/event-stream"}, text=content)
            suite.transport.transport = httpx.MockTransport(model_fixture)
            with patch.dict(os.environ, {"DEEPSEEK_API_KEY": "synthetic-offline-key"}):
                await asyncio.wait_for(suite.run(cases=[("hello", "Hello～")]), timeout=30)
            require(suite.report["cases"][0]["passed"], "offline signed ingress to final delivery failed")
        finally:
            await suite.transport.transport.aclose()
    print("Dialogue live offline self-check passed")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-check"]:
        asyncio.run(self_check())
    else:
        require(not sys.argv[1:], "unsupported command arguments")
        raise SystemExit(asyncio.run(main()))
