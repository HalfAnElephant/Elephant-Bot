"""Opt-in real DeepSeek/Pi dialogue acceptance. Run from the repository root.

Only the model uses the network. QQ delivery and business observations are synthetic.
The JSON report contains metrics and synthetic replies, never session files or credentials.
"""

import asyncio
import base64
import hashlib
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

from elephant.app import create_app
from elephant.capabilities.gateway import CapabilityRegistry
from elephant.capabilities.types import Action, Capability
from elephant.config import Settings
from elephant.harness.gateway import RunGateway
from elephant.harness.pi import PiRunnerAdapter
from elephant.runtime.scheduler import RuntimeClient
from elephant.storage.agent_schema import dialogue_inputs
from elephant.storage.mail_schema import mail_accounts
from elephant.storage.mail_trigger_schema import mail_rule_receipts
from elephant.storage.schedule_schema import schedules
from elephant.storage.schema import outbox
from elephant.storage.task_schema import tasks

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

    def __init__(self, report, *, gateway=None, transport=None):
        self.report = report
        self.gateway = gateway
        self.transport = transport if transport is not None else httpx.AsyncHTTPTransport(retries=0)

    async def handle_async_request(self, request):
        require(str(request.url) == ENDPOINT, "unexpected external model endpoint")
        body = json.loads(await request.aread())
        require(body.get("model") == MODEL, "unexpected model")
        body["reasoning_effort"] = "none"
        schemas = {tool["function"]["name"]: tool["function"]["parameters"]
                   for tool in body.get("tools", [])}
        self.report["requests"].append({
            "role": self.gateway.claim["role"] if self.gateway is not None else "leader",
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


class SyntheticMailbox:
    """Only mailbox protocol observations are fixtures; Agent decisions and files are real."""
    def __init__(self):
        self.messages_received = []

    def mailbox(self, spec, secret, folder):
        require(folder == "INBOX" and secret == "synthetic-app-password", "unexpected synthetic mailbox binding")
        return {"uidvalidity": 713, "highest_uid": len(self.messages_received)}

    def test(self, spec, secret):
        self.mailbox(spec, secret, "INBOX")
        return {"imap": "authenticated", "smtp": "authenticated", "sent": False}

    def folders(self, spec, secret):
        self.mailbox(spec, secret, "INBOX")
        return {"folders": [{"name": "INBOX", "delimiter": "/", "selectable": True}]}

    def messages(self, spec, secret, folder, *, after_uid=0, limit=50, query=None, uid=None):
        snapshot = self.mailbox(spec, secret, folder)
        selected = [dict(message) for message in self.messages_received
                    if message["uid"] <= snapshot["highest_uid"]
                    and (message["uid"] == uid if uid is not None else message["uid"] > after_uid)
                    and (query is None or query in message["body"])]
        scanned = selected[limit - 1]["uid"] if len(selected) > limit else max(after_uid, snapshot["highest_uid"])
        return {**snapshot, "scanned_through": scanned, "messages": selected[:limit]}


async def prepare_synthetic_mail_account(app, account):
    """Prepare the local-only execution fixture without conflating storage and enablement."""
    headers = {"Authorization": "Bearer synthetic-mail-admin"}
    path = "/api/v1/mail/accounts/" + account["id"]
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://fixture") as client:
        if not account["credential_ref"]:
            response = await client.post(path + "/credential", headers=headers,
                json={"expected_revision": account["revision"], "secret": "synthetic-app-password"})
            require(response.status_code == 200, f"protected synthetic credential save returned HTTP {response.status_code}")
            account = response.json()
            require(account["credential_ref"], "protected synthetic credential save returned no reference")
        if not account["enabled"]:
            response = await client.patch(path, headers=headers,
                json={"expected_revision": account["revision"], "changes": {"enabled": True}})
            require(response.status_code == 200, f"synthetic account enable returned HTTP {response.status_code}")
            account = response.json()
    require(account["ready"], "synthetic account is not ready for rule evaluation after setup")
    return account


class DialogueSuite:
    def __init__(self, root, report):
        self.root, self.report = root, report
        self.current = None
        self.seeded = False
        self.transport = LiveModelTransport(report)
        self.mailbox = SyntheticMailbox()

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
        with core.db.engine.connect() as conn:
            prior_schedules = {row.id for row in conn.execute(select(schedules.c.id))}
            prior_accounts = {row.id for row in conn.execute(select(mail_accounts.c.id))}
            prior_roots = {row.id for row in conn.execute(select(tasks.c.id).where(tasks.c.kind == "goal"))}
        case = {"label": label, "input": text, "started": time.perf_counter(), "received": [],
                "claims": [], "business_calls": [], "business_completed_seconds": None, "runner_failed": False,
                "mail_operations": [], "passed": False}
        self.current = case
        self.report["cases"].append(case)
        packet = {"op": 0, "id": uuid4().hex, "t": "C2C_MESSAGE_CREATE", "d": {
            "id": uuid4().hex, "author": {"user_openid": "synthetic-user"}, "content": text,
            "timestamp": core.clock.now().isoformat()}}
        source = core.adapters["live-eval"].normalize(packet)
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
                final_text = "\n".join(item["text"] for item in finals)
                worker_claims = [claim for claim in case["claims"] if claim["role"] == "worker"]
                if label == "webpage":
                    require(roots and children, "webpage did not persist a delegated goal and child")
                    require(not worker_claims, "dispatch-only case unexpectedly ran a Worker")
                    case["dispatch_observed"] = True
                    case["worker_result_verified"] = False
                else:
                    require(not worker_claims and {row["id"] for row in roots} == prior_roots,
                            "short dialogue unexpectedly delegated")
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
                    if label.startswith("mail_"):
                        with core.db.engine.connect() as conn:
                            saved = [dict(row) for row in conn.execute(select(schedules)).mappings()
                                     if row["id"] not in prior_schedules]
                            accounts = [dict(row) for row in conn.execute(select(mail_accounts)).mappings()]
                        if label == "mail_draft":
                            from elephant.runtime.schedules import ScheduleSpec

                            require(len(saved) == 1, "mail request did not persist exactly one draft")
                            row = saved[0]
                            spec = ScheduleSpec.model_validate_json(row["spec_json"])
                            require(not row["enabled"] and not spec.enabled and row["next_fire_at"] is None,
                                    "mail draft was enabled or scheduled as a timer")
                            identifiers = {"weekly-demo", *(row["id"] for row in accounts if row["account_id"] == "weekly-demo")}
                            require(spec.mail and spec.mail.account_id in identifiers and spec.mail.folder == "INBOX",
                                    "mail draft changed the user's account or folder")
                            mail = {"sender_address": "office@example.org", "subject": "周报", "body": "新稿"}
                            require(spec.mail.matches(**mail), "mail draft does not match the requested weekly report")
                            require(not spec.mail.matches(**{**mail, "sender_address": "office@elsewhere.org"})
                                    and not spec.mail.matches(**{**mail, "subject": "通知"})
                                    and not spec.mail.matches(**{**mail, "body": "旧稿"}),
                                    "mail draft lost the requested sender, subject or exclusion")
                            require(any(Path(path).name == "summary.md" for path in spec.requested_outputs)
                                    and re.search(r"三|3", spec.goal),
                                    "mail draft lost the requested output or summary requirement")
                            require(re.search(r"(?:未|没(?:有)?)启用|禁用|停用|不.*扫描|不会.*扫描", final_text),
                                    "mail draft reply omitted its inactive state")
                            case["mail_draft"] = {"enabled": False, "account_id": spec.mail.account_id,
                                                  "folder": spec.mail.folder, "spec": spec.model_dump(mode="json")}
                        else:
                            require(not saved, "mail clarification unexpectedly saved an invented configuration")
                            if label in {"mail_config", "mail_update", "mail_test_missing_credential"}:
                                from elephant.mail.models import MailAccountSpec

                                require(len(accounts) == 1 and accounts[0]["account_id"] == "weekly-demo",
                                        "mail configuration did not persist the selected account exactly once")
                                account = accounts[0]
                                connection = MailAccountSpec.model_validate_json(account["spec_json"])
                                require(account["owner_id"] == core.settings.owner.id
                                        and account["principal"] == source.principal.key
                                        and account["conversation"] == source.conversation.key
                                        and account["workspace_id"] == "personal",
                                        "mail configuration escaped the authenticated user scope")
                                require(connection.provider == "icloud" and connection.address == "weekly-demo@icloud.com"
                                        and connection.imap_host == "imap.mail.me.com" and connection.smtp_host == "smtp.mail.me.com"
                                        and connection.smtp_port == 587 and connection.smtp_security == "starttls",
                                        "mail configuration lost the selected provider defaults")
                                require(not account["credential_ref"] and not account["tested_revision"],
                                        "mail configuration invented credentials or a successful connection test")
                                if label == "mail_config":
                                    require(account["id"] not in prior_accounts and "/api/v1/mail" in final_text
                                            and re.search(r"密码|授权码|凭据", final_text),
                                            "mail configuration reply omitted its protected credential setup path")
                                else:
                                    require(connection.username == "weekly-reader" and account["revision"] >= 2,
                                            "mail update did not persist the selected login name")
                                if label == "mail_test_missing_credential":
                                    require(any(operation in {"accounts.list", "accounts.get", "accounts.test"}
                                                for operation in case["mail_operations"])
                                            and re.search(r"凭据|授权码|密码", final_text),
                                            "mail test did not verify and explain missing credentials")
                                case["mail_account"] = {"account_id": account["account_id"], "ready": False,
                                                        "spec": connection.model_dump(mode="json")}
                            if label == "mail_missing_account":
                                require({row["id"] for row in accounts} == prior_accounts,
                                        "mail clarification invented a new account")
                                require(re.search(r"邮箱|账号|标识", final_text)
                                        and re.search(r"[？?]|提供|告诉|哪个|给我|还差|缺少|缺.*账号|账号.*未知|尚未.*指定", final_text),
                                        "mail request did not ask for its missing account")
                case["passed"] = True
                return case
        raise AssertionError("dialogue case timed out")

    async def mail_execution(self, app, label, subject, goal, output, body, tokens):
        core = app.state.core
        with core.db.engine.connect() as conn:
            account = conn.execute(select(mail_accounts).where(mail_accounts.c.account_id == "weekly-demo")).mappings().one()
        core.mail.accounts.transport = self.mailbox
        await prepare_synthetic_mail_account(app, core.mail.accounts.view(account))
        with core.db.engine.connect() as conn:
            previous = {row.id for row in conn.execute(select(schedules.c.id))}
        await self.case(app, "rule_" + label,
            f"为 weekly-demo 邮箱 INBOX 创建并启用收信规则，主题恰好为“{subject}”时，{goal}"
            f"输出路径为 {output}，完成后在当前 QQ 回报处理结果与文件路径。"
            f"仅授权 workspace.file.*，资源 workspace:* 和 file:{output}。不发送邮件，不授权其他业务操作。")
        with core.db.engine.connect() as conn:
            rules = [dict(row) for row in conn.execute(select(schedules)).mappings() if row["id"] not in previous]
        require(len(rules) == 1 and rules[0]["enabled"], "natural-language request did not create one enabled mail rule")
        rule = rules[0]
        spec = json.loads(rule["spec_json"])
        require(len(spec["requested_outputs"]) == 1 and Path(spec["requested_outputs"][0]).name == Path(output).name,
                "enabled mail rule lost its requested output")
        output = spec["requested_outputs"][0]
        case = {"label": "mail_" + label + "_execution", "started": time.perf_counter(), "received": [],
                "claims": [], "mail_operations": [], "workspace_operations": [], "runner_failed": False,
                "synthetic_mailbox": True, "rule_spec": spec, "passed": False}
        self.current = case
        self.report["cases"].append(case)
        uid = len(self.mailbox.messages_received) + 1
        self.mailbox.messages_received.append({"uid": uid, "sender_address": "office@example.org",
            "to": ["weekly-demo@icloud.com"], "cc": [], "reply_to": [], "subject": subject, "body": body,
            "body_complete": True, "message_id": f"<live-mail-{uid}@example.org>", "references": [], "attachments": []})
        await asyncio.gather(asyncio.to_thread(core.mail.triggers.once), asyncio.to_thread(core.mail.triggers.once))
        deadline = time.monotonic() + CASE_TIMEOUT
        while time.monotonic() < deadline:
            await asyncio.sleep(0.1)
            require(not app.state.core_task.done() and not case["runner_failed"], "mail execution runner stopped")
            with core.db.engine.connect() as conn:
                receipts = conn.execute(select(mail_rule_receipts).where(
                    mail_rule_receipts.c.schedule_id == rule["id"])).mappings().all()
                require(len(receipts) == 1, "repeated mailbox scans lost or duplicated the task receipt")
                root = conn.execute(select(tasks).where(tasks.c.id == receipts[0]["task_id"])).mappings().one()
                deliveries = conn.execute(select(outbox).where(outbox.c.task_id == root["id"])).mappings().all()
            case["root_state"] = root["state"]
            require(root["state"] not in {"FAILED", "PARTIAL", "BLOCKED", "CANCELLED", "EXPIRED"},
                    "mail task did not complete successfully")
            finals = [json.loads(row["envelope_json"]) for row in deliveries if row["state"] == "SENT"
                      and json.loads(row["envelope_json"]).get("purpose") == "result"]
            if root["state"] != "SUCCEEDED" or not finals:
                continue
            result = json.loads(root["result_json"])
            artifact = next((item for item in result["artifacts"] if item["relative_path"] == output), None)
            require(artifact is not None, "verified mail result has no requested artifact")
            content = (core.tasks.archive_root / artifact["id"]).read_bytes()
            require(hashlib.sha256(content).hexdigest() == artifact["sha256"], "mail artifact archive digest changed")
            text, reply = content.decode(), "\n".join(item["text"] for item in finals)
            require(all(token in text and token in reply for token in tokens),
                    "mail artifact or final QQ result omitted the triggering message facts")
            require("input" in case["mail_operations"] and "workspace.write" in case["workspace_operations"]
                    and any(claim["role"] == "worker" for claim in case["claims"])
                    and any(claim["role"] == "leader" and claim["purpose"] == "finalize" for claim in case["claims"]),
                    "mail result skipped real Worker input/file tools or Leader verification")
            await asyncio.to_thread(core.mail.triggers.once)
            with core.db.engine.connect() as conn:
                require(len(conn.execute(select(mail_rule_receipts).where(
                    mail_rule_receipts.c.schedule_id == rule["id"])).all()) == 1, "finished mail task was resubmitted")
            case.update(passed=True, duration_seconds=self.elapsed(), root_state=root["state"],
                artifact=artifact, artifact_text=text, replies=[{"purpose": "result", "text": reply}],
                first_reply_seconds=case["received"][0]["seconds"])
            return
        raise AssertionError("mail execution timed out")

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
            gateway.registry.entries = {name: entry for name, entry in gateway.registry.entries.items()
                                        if name.startswith("mail.") or name in {"workspace.read", "workspace.write"}} | self.registry().entries
            for name in ("workspace.read", "workspace.write"):
                capability, execute, classifier = gateway.registry.entries[name]
                async def observed(arguments, context, name=name, execute=execute):
                    result = await execute(arguments, context)
                    self.current.setdefault("workspace_operations", []).append(name)
                    return result
                gateway.registry.entries[name] = (capability, observed, classifier)
            gateway.transport = LiveModelTransport(self.report, gateway=gateway,
                                                   transport=self.transport.transport)

        async def start(runner):
            return await self.seed_history(runner, original_start)

        async def invoke(client, operation, body):
            if operation == "mail" and self.current:
                self.current["mail_operations"].append((body.payload or {}).get("operation"))
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
                patch.object(RuntimeClient, "invoke", invoke), patch.dict(os.environ, {
                    "ELEPHANT_ADMIN_TOKEN": "synthetic-mail-admin",
                    "ELEPHANT_CREDENTIAL_KEY": base64.b64encode(os.urandom(32)).decode()}):
            async with httpx.AsyncClient(transport=httpx.MockTransport(self.receiver)) as receiver:
                app = create_app(settings, official_client=receiver)
                async with app.router.lifespan_context(app):
                    for label, text in cases or [("hello", "Hello～"), ("available", "在吗？"),
                                                ("weather", "查看一下今天嘉定的天气"),
                                                ("issues_legacy", "查看一下 yourtjhub 现在有多少 Issue"),
                                                ("mail_config", "帮我添加并启用 iCloud 邮箱配置，标识 weekly-demo，邮箱地址 weekly-demo@icloud.com，使用服务商默认服务器。先保存普通参数，登录测试等我交凭据后再做。请告诉我受保护的应用专用密码提交入口。"),
                                                ("mail_update", "把 weekly-demo 的登录名改成 weekly-reader，其他设置保留。"),
                                                ("mail_test_missing_credential", "检查一下 weekly-demo 现在能否测试连接。我还没有通过受保护入口提交应用专用密码，请按实际配置状态告诉我结果。"),
                                                ("mail_missing_account", "帮我给另一个还没告诉你账号标识的邮箱建规则，收到主题含周报的邮件就总结成三条；这只邮箱和 weekly-demo 无关。"),
                                                ("mail_draft", "邮箱标识用 weekly-demo，文件夹 INBOX。收到发件域名等于 example.org、主题包含周报、正文不包含旧稿的邮件时，总结为三条，输出文件 summary.md。请保存为未启用的草稿，不扫描邮箱，也不发送邮件。"),
                                                ("webpage", "帮我写一个网页，里面用 svg 描绘一个秦始皇骑北极熊的动画")]:
                        await self.case(app, label, text)
                        if label == "mail_draft" and cases is None:
                            await self.mail_execution(app, "summary", "摘要测试", "把邮件中的项目代号、负责人和验收状态整理为三条摘要，在完成后的 QQ 回报中也展示这三条摘要。",
                                "outputs/summary-live.md", "项目代号 ALPHA-LIVE。项目负责人 林悦。阶段状态 已验收。",
                                ("ALPHA-LIVE", "林悦", "验收"))
                            await self.mail_execution(app, "ticket", "工单测试", "提取正文的工单编号，文件只写编号一行，QQ 回报也保留编号。",
                                "outputs/ticket-live.txt", "请处理工单 TICKET-07123，问题为打印机无法连接。", ("TICKET-07123",))
                    sessions = {claim["session_id"] for case in self.report["cases"] for claim in case["claims"]
                                if claim["role"] == "leader"}
                    require(self.seeded and len(sessions) == 1, "cases did not reuse the seeded native Session")
                    require(self.report["requests"], "no real model requests were observed")
                    expected = {"capability_search", "capability_invoke", "content_read"}
                    for request in self.report["requests"]:
                        schemas = request["communication_schemas"]
                        require(set(schemas) == expected
                                and all(required == (request["role"] == "leader") for required in schemas.values()),
                                "communication schemas missing or lost required choice")


async def main():
    report = {"model": MODEL, "endpoint": ENDPOINT, "thinking": "disabled via reasoning_effort=none at test transport",
              "synthetic_business": True, "synthetic_mailbox": True,
              "legacy_paired_turns": 6, "cases": [], "requests": [], "passed": False}
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

    from elephant.adapters.pi.sessions import load_entries, unfinished_tools

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
