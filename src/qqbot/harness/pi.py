"""The sole pi RPC adapter. A quiescent run need not stop its resident process."""

import asyncio
import json
import os
import re
import signal
from pathlib import Path
from uuid import uuid4

from qqbot.domain.tasks import HarnessCapabilities, TaskResult, validate_user_facing_text

MAX_EVENT = 32 * 1024 * 1024
MAX_DIAGNOSTICS = 20 * 1024 * 1024


def compaction_settings(model_window, limits, output_tokens):
    reserve = max((model_window + 9) // 10, output_tokens + 1024)
    soft = limits.get("soft_compaction_input_tokens", model_window - reserve)
    recent = limits.get("keep_recent_tokens", min(model_window // 5, soft // 2))
    return {"enabled": True, "reserveTokens": model_window - soft, "keepRecentTokens": recent}


class PublicAnswerEvents:
    """Project pi text blocks only, never thinking/tool arguments or raw RPC records.

    A bounded tail is held back while generating to avoid exposing partial secret
    tokens. The host still authorizes every delivery; these events are not grants.
    `public_answer_final` occurs at agent_settled, not at a tool-call turn boundary.
    """

    def __init__(self, redact=lambda value: value, max_chars=10000):
        self.redact, self.max_chars = redact, max_chars
        self.message_id = None
        self.text = self.emitted = ""
        self.failed = False
        self.last_complete = None

    def accept(self, event):
        kind = event.get("type")
        message = event.get("message") or {}
        if kind == "message_start" and message.get("role") == "assistant":
            self.message_id = str(uuid4())
            self.text = self.emitted = ""
            self.failed = False
            self.last_complete = None
            return [{"type": "public_answer_start", "message_id": self.message_id}]
        if kind == "message_update" and message.get("role") == "assistant" and self.message_id:
            change = event.get("assistantMessageEvent") or {}
            if change.get("type") != "text_delta" or not isinstance(change.get("delta"), str):
                return []
            if self.failed:
                return []
            self.text += change["delta"]
            return self._emit(False)
        if kind == "message_end" and message.get("role") == "assistant":
            if self.message_id is None:
                self.message_id = str(uuid4())
            parts = message.get("content") or []
            self.text = "".join(part.get("text", "") for part in parts
                                if isinstance(part, dict) and part.get("type") == "text"
                                and isinstance(part.get("text"), str))
            self.failed = self.failed or bool(message.get("errorMessage")) or message.get("stopReason") in {
                "error", "aborted"}
            emitted = self._emit(True)
            self.last_complete = None if self.failed else (self.message_id, self.redact(self.text))
            return emitted
        if kind == "agent_settled" and self.last_complete:
            message_id, text = self.last_complete
            self.last_complete = None
            return [{"type": "public_answer_final", "message_id": message_id, "text": text}]
        return []

    def _emit(self, final):
        try:
            validate_user_facing_text(self.text)
            if len(self.text) > self.max_chars:
                raise ValueError("answer exceeds public streaming limit")
            safe = self.redact(self.text)
            # Retain enough of the newest text to catch split credentials; callers
            # can additionally disable streaming for tasks with sensitive output.
            visible = safe if final else safe[:max(0, len(safe) - 256)]
            if not visible.startswith(self.emitted):
                raise ValueError("public text prefix changed")
        except ValueError:
            self.failed = True
            visible = self.emitted
        if self.failed:
            return [{"type": "public_answer_end", "message_id": self.message_id,
                     "text": self.emitted, "error": True}] if final else []
        if not final and len(visible) - len(self.emitted) < 128:
            return []
        self.emitted = visible
        return [{"type": "public_answer_end" if final else "public_answer_delta",
                 "message_id": self.message_id, "text": visible, "error": False}]


class PiRunnerAdapter:
    capabilities = HarnessCapabilities(append_input=True, interactive_questions=True)

    def __init__(
        self,
        executable,
        workspace,
        *,
        provider="openai",
        model="gpt-4o-mini",
        api_key=None,
        timeout=1800,
        session_id=None,
        gateway=None,
        gateway_url=None,
    ):
        self.executable = str(Path(executable).resolve(strict=True))
        self.workspace = Path(workspace).resolve()
        self.provider, self.model, self.api_key, self.timeout = provider, model, api_key, timeout
        self.gateway, self.gateway_url = gateway, gateway_url
        self.quiescent_verified = False
        self.run_timeout = None
        self.busy = False
        self.input_lock = asyncio.Lock()
        self.accepting_input = False
        self.steered_inputs = set()
        self.actual_session_id = None
        self.event_sink = None
        self.public_event_sink = None
        self.public_answers = PublicAnswerEvents(self.safe_diagnostic)
        self.process = None
        self.queue = asyncio.Queue(maxsize=8)
        self.responses = {}
        self.readers = []
        self.diagnostics_bytes = 0
        self.stderr_tail = bytearray()
        self.error = None
        self.stopped_verified = False
        self.session_path = self.workspace / "sessions" / f"{session_id or uuid4()}.jsonl"

    def argv(self):
        argv = [
            self.executable,
            "--mode",
            "rpc",
            "--provider",
            self.provider,
            "--model",
            self.model,
            "--session",
            str(self.session_path),
            "--no-tools",
            "--no-extensions",
            "--no-mcp",
            "--no-skills",
            "--no-prompt-templates",
            "--no-context-files",
            "--no-themes",
            "--no-approve",
            "--offline",
        ]
        if self.gateway:
            argv.remove("--no-tools")
            argv += ["--no-builtin-tools", "--extension", str(Path(__file__).with_name("pi_bridge.ts"))]
        return argv

    async def start(self):
        self.workspace.mkdir(parents=True, exist_ok=True, mode=0o700)
        profile = self.workspace / "profile"
        profile.mkdir(mode=0o700, exist_ok=True)
        self.session_path.parent.mkdir(mode=0o700, exist_ok=True)
        env = {
            "PATH": "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin",
            "HOME": str(profile),
            "PI_CODING_AGENT_DIR": str(profile),
            "PI_OFFLINE": "1",
            "PI_TELEMETRY": "0",
            "LANG": "C.UTF-8",
        }
        limits = self.gateway.claim.get("context", {}).get("limits", {}) if self.gateway else {}
        compaction = compaction_settings(self.gateway.model_window, limits,
            max(self.gateway.provider.output_tokens, self.gateway.provider.compaction_output)) if self.gateway else {
                "enabled": True, "reserveTokens": 32768 - 6000, "keepRecentTokens": 2000}
        (profile / "settings.json").write_text(json.dumps({
            "cacheWarming": "off", "defaultThinkingLevel": "off",
            "compaction": compaction,
        }))
        if self.gateway:
            env["QQBOT_GATEWAY_URL"] = self.gateway_url
            env["QQBOT_PROCESS_TOKEN"] = self.gateway.process_token
            # Bootstrap credentials are deliberately unusable for model/tool requests.
            (profile / "models.json").write_text(json.dumps({"providers": {"qqbot": {
                "baseUrl": self.gateway_url + "/v1", "api": "openai-completions", "apiKey": "binding-required",
                "models": [{"id": self.model, "name": self.model, "reasoning": False, "input": ["text"],
                    "contextWindow": self.gateway.model_window, "maxTokens": max(self.gateway.provider.output_tokens, self.gateway.provider.compaction_output),
                    "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0}}]}}}))
        if self.api_key:
            if self.provider != "openai":
                raise ValueError("only explicit openai credentials are supported")
            env["OPENAI_API_KEY"] = self.api_key
        self.process = await asyncio.create_subprocess_exec(
            *self.argv(),
            cwd=self.workspace,
            env=env,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
            limit=MAX_EVENT + 1,
        )
        self.readers = [asyncio.create_task(self._stdout()), asyncio.create_task(self._stderr())]

    async def command(self, kind, *, wait=False, **values):
        if not self.process or self.process.returncode is not None:
            raise RuntimeError("runner stopped")
        command_id = str(uuid4())
        future = asyncio.get_running_loop().create_future() if wait else None
        if future:
            self.responses[command_id] = future
        self.process.stdin.write(
            json.dumps({"id": command_id, "type": kind, **values}, ensure_ascii=False).encode() + b"\n"
        )
        await self.process.stdin.drain()
        if future:
            try:
                response = await asyncio.wait_for(future, 10)
                if not response.get("success"):
                    raise RuntimeError(str(response.get("error", "RPC rejected command")))
                return response.get("data", {})
            finally:
                self.responses.pop(command_id, None)
        return command_id

    async def _stdout(self):
        try:
            while line := await self.process.stdout.readline():
                if len(line) > MAX_EVENT or not line.endswith(b"\n"):
                    raise ValueError("oversized or truncated RPC event")
                record = json.loads(line)
                if not isinstance(record, dict):
                    raise TypeError("RPC event must be an object")
                if self.event_sink:
                    self.event_sink(record)
                if self.public_event_sink:
                    for public_event in self.public_answers.accept(record):
                        self.public_event_sink(public_event)
                pending = self.responses.get(record.get("id")) if record.get("type") == "response" else None
                if pending is not None and not pending.done():
                    pending.set_result(record)
                else:
                    await self.queue.put(record)
        except (ValueError, TypeError, UnicodeError, RuntimeError) as error:
            await self.queue.put({"type": "host_error", "error": str(error)})
        finally:
            for pending in self.responses.values():
                if not pending.done():
                    pending.set_exception(RuntimeError("RPC closed before command acknowledgement"))
            # Shutdown cancels the reader after its queue consumer has already stopped.
            if not asyncio.current_task().cancelling():
                await self.queue.put({"type": "host_eof"})

    async def _stderr(self):
        while chunk := await self.process.stderr.read(65536):
            self.diagnostics_bytes += len(chunk)
            self.stderr_tail = (self.stderr_tail + chunk)[-8192:]
            if self.diagnostics_bytes > MAX_DIAGNOSTICS:
                self.error = "diagnostics limit exceeded"
                await self.queue.put({"type": "host_error", "error": self.error})
                return

    def safe_diagnostic(self, text):
        secrets = [self.api_key]
        if self.gateway:
            secrets += [self.gateway.model_token, self.gateway.tool_token, self.gateway.key, self.gateway.process_token]
        for secret in filter(None, secrets):
            text = text.replace(secret, "[REDACTED]")
            for length in range(min(len(secret), len(text)), 7, -1):
                if text.startswith(secret[-length:]):
                    text = "[REDACTED]" + text[length:]
                    break
                if text.endswith(secret[:length]):
                    text = text[:-length] + "[REDACTED]"
                    break
        return re.sub(r"(?i)(bearer\s+)[A-Za-z0-9._~+/-]+", r"\1[REDACTED]", text)

    def failure_summary(self, error):
        detail = self.safe_diagnostic(f"{type(error).__name__}: {error}")[:1000]
        tail = self.safe_diagnostic(self.stderr_tail.decode("utf-8", errors="replace"))[-2000:]
        return "Runner failed. " + detail + ("\nRunner stderr\n" + tail if tail else "")

    async def run(self, goal, *, keep_alive=False):
        if self.busy:
            raise RuntimeError("session already has an active run")
        self.busy, self.quiescent_verified = True, False
        self.accepting_input = True
        self.steered_inputs = set()
        self.public_answers = PublicAnswerEvents(self.safe_diagnostic)
        text, error, settled = "", None, False
        try:
            if not self.process:
                await self.start()
            message = json.dumps({"binding_id": self.gateway.binding}) if self.gateway else goal
            prompt_id = await self.command("prompt", message=message)
            async with asyncio.timeout(self.timeout) as deadline:
                self.run_timeout = deadline
                while True:
                    event = await self.queue.get()
                    kind = event.get("type")
                    if kind == "response" and event.get("id") == prompt_id:
                        if not event.get("success"):
                            raise RuntimeError(str(event.get("error", "prompt rejected")))
                        if event.get("data", {}).get("disposition") == "handled":
                            raise RuntimeError("prompt handled without an agent run")
                    elif kind == "message_end":
                        message = event.get("message", {})
                        if message.get("role") == "assistant":
                            error = message.get("errorMessage")
                            if message.get("stopReason") in {"error", "aborted"}:
                                error = error or message["stopReason"]
                            text = "".join(
                                part.get("text", "")
                                for part in message.get("content", [])
                                if part.get("type") == "text"
                            )
                    elif kind == "agent_settled":
                        async with self.input_lock:
                            self.accepting_input = False
                        if error or (not text.strip() and not (self.gateway and self.gateway.waiting)):
                            raise RuntimeError(error or "no final assistant output")
                        validate_user_facing_text(text)
                        if self.gateway:
                            state = await self.rpc("get_state")
                            if state.get("isStreaming") or state.get("isCompacting"):
                                raise RuntimeError("pi settled with pending work")
                            if state.get("pendingMessageCount"):
                                await self.rpc("clear_queue")
                            if self.steered_inputs:
                                history = await self.rpc("get_messages")
                                user_text = json.dumps([m for m in history.get("messages", []) if m.get("role") == "user"], ensure_ascii=False)
                                for input_id in self.steered_inputs:
                                    if "用户补充[" + input_id + "]" in user_text:
                                        await self.gateway.call("input-ack", {"input_id": input_id})
                            self.actual_session_id = state.get("sessionId")
                            actual = Path(state["sessionFile"])
                            if actual not in {self.session_path, Path("/workspace/sessions") / self.session_path.name}:
                                raise RuntimeError("pi session path changed unexpectedly")
                            self.actual_session_file = str(actual)
                            if not await self.gateway.drain():
                                raise RuntimeError("run I/O has not drained")
                            if self.gateway.claim.get("session_id") and not self.gateway.session_receipt:
                                raise RuntimeError("pi settled without a durable session receipt")
                            self.last_receipt = self.gateway.session_receipt
                        self.quiescent_verified, settled = True, True
                        outcome = (self.gateway.outcome or {}) if self.gateway else {}
                        status = (self.gateway.claim.get("notification_status") if self.gateway else None) or outcome.get("status", "completed")
                        if self.gateway and self.gateway.waiting:
                            status = self.gateway.wait_kind or "waiting_input"
                        elif self.gateway and any(value in {"unknown", "partial", "dispatched", "prepared"} for value in self.gateway.effects.values()):
                            status = "partial" if status == "completed" else status
                        return TaskResult(status=status, summary=text[:10000] or "Waiting checkpoint persisted.",
                            observation_refs=tuple(outcome.get("observation_refs", self.gateway.observations if self.gateway else ())),
                            effect_refs=tuple(outcome.get("effect_refs", self.gateway.effects if self.gateway else ())),
                            limitations=tuple(outcome.get("unresolved_items", ())))
                    elif kind in {"host_error", "host_eof"}:
                        raise RuntimeError(event.get("error", "RPC closed before agent_settled"))
        finally:
            self.run_timeout = None
            self.accepting_input = False
            self.busy = False
            if not keep_alive or not settled:
                await self.stop()

    async def rpc(self, kind, **values):
        if kind == "compact" and self.busy:
            raise RuntimeError("manual compaction requires an idle session")
        command_id = await self.command(kind, **values)
        async with asyncio.timeout(10):
            while True:
                event = await self.queue.get()
                if event.get("type") in {"host_error", "host_eof"}:
                    raise RuntimeError(event.get("error", "RPC closed"))
                if event.get("id") == command_id:
                    if not event.get("success"):
                        raise RuntimeError(str(event.get("error", "RPC command rejected")))
                    return event.get("data", {})

    async def append_input(self, input_id, text, *, host_notice=False):
        async with self.input_lock:
            if not self.busy or not self.accepting_input or not self.gateway or self.gateway.waiting:
                raise RuntimeError("run is not accepting steering input")
            if input_id in self.steered_inputs:
                return
            result = await self.command("steer", wait=True, message=json.dumps({"binding_id": self.gateway.binding,
                               "input_id": input_id, "text": text, "host_notice": host_notice}, ensure_ascii=False))
            if not host_notice:
                self.steered_inputs.add(input_id)
            return result

    async def stop(self):
        drained = await self.gateway.stop_requests() if self.gateway else True
        if self.gateway:
            self.gateway.revoke()
            await self.gateway.processes.close_run(self.gateway.context)
        process = self.process
        if not process:
            self.stopped_verified = drained
            return drained
        if process.returncode is None:
            for command in ("clear_queue", "abort"):
                try:
                    await asyncio.wait_for(self.command(command), 1)
                except (RuntimeError, BrokenPipeError, ConnectionResetError, TimeoutError):
                    pass
            try:
                await asyncio.wait_for(process.wait(), 1)
            except TimeoutError:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    await asyncio.wait_for(process.wait(), 2)
                except TimeoutError:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    await process.wait()
        for reader in self.readers:
            reader.cancel()
        await asyncio.gather(*self.readers, return_exceptions=True)
        try:
            os.killpg(process.pid, signal.SIGKILL)
            await asyncio.sleep(0.05)
            os.killpg(process.pid, 0)
            self.stopped_verified = False
        except ProcessLookupError:
            self.stopped_verified = drained and process.returncode is not None
        return self.stopped_verified


# Import compatibility; there is only one RPC implementation.
PiHarness = PiRunnerAdapter
