"""One process per run, no ambient credentials, extensions or executable tools."""

import asyncio
import json
import os
import re
import signal
from pathlib import Path
from uuid import uuid4

from qqbot.domain.tasks import HarnessCapabilities, TaskResult

MAX_EVENT = 2 * 1024 * 1024
MAX_DIAGNOSTICS = 20 * 1024 * 1024


class PiHarness:
    capabilities = HarnessCapabilities()

    def __init__(
        self,
        executable,
        workspace,
        *,
        provider="openai",
        model="gpt-4o-mini",
        api_key=None,
        timeout=1800,
        research=None,
        research_url=None,
        session_id=None,
    ):
        self.executable = str(Path(executable).resolve(strict=True))
        self.workspace = Path(workspace).resolve()
        self.provider, self.model, self.api_key, self.timeout = provider, model, api_key, timeout
        self.research, self.research_url = research, research_url
        self.event_sink = None
        self.process = None
        self.queue = asyncio.Queue(maxsize=8)
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
        if self.research:
            argv.remove("--no-tools")
            argv += [
                "--no-builtin-tools",
                "--tools",
                "research_search,research_fetch,research_write",
                "--extension",
                str(Path(__file__).with_name("research_extension.ts")),
            ]
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
        if self.research:
            env["QQBOT_RESEARCH_URL"] = self.research_url
            env["QQBOT_TOOL_TOKEN"] = self.research.tool_token
            model_config = {
                "providers": {
                    "qqbot": {
                        "baseUrl": self.research_url + "/v1",
                        "api": "openai-completions",
                        "apiKey": self.research.model_token,
                        "models": [
                            {
                                "id": self.model,
                                "name": self.model,
                                "reasoning": False,
                                "input": ["text"],
                                "contextWindow": 32768,
                                "maxTokens": 4096,
                                "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0},
                            }
                        ],
                    }
                }
            }
            (profile / "models.json").write_text(json.dumps(model_config))
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

    async def command(self, kind, **values):
        if not self.process or self.process.returncode is not None:
            raise RuntimeError("runner stopped")
        command_id = str(uuid4())
        self.process.stdin.write(
            json.dumps({"id": command_id, "type": kind, **values}, ensure_ascii=False).encode() + b"\n"
        )
        await self.process.stdin.drain()
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
                await self.queue.put(record)
        except (ValueError, TypeError, UnicodeError, RuntimeError) as error:
            await self.queue.put({"type": "host_error", "error": str(error)})
        finally:
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
        if self.research:
            secrets += [self.research.model_token, self.research.tool_token, self.research.upstream_key]
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

    async def run(self, goal):
        await self.start()
        text = ""
        error = None
        try:
            prompt_id = await self.command("prompt", message=goal)
            async with asyncio.timeout(self.timeout):
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
                        if error or not text.strip():
                            raise RuntimeError(error or "no final assistant output")
                        return TaskResult(
                            status="completed",
                            summary=text[:10000],
                            limitations=("Research quality requires human review.",)
                            if self.research
                            else (
                                "No research tools enabled. Source retrieval and report quality have not been verified.",
                            ),
                        )
                    elif kind in {"host_error", "host_eof"}:
                        raise RuntimeError(event.get("error", "RPC closed before agent_settled"))
        finally:
            await self.stop()

    async def stop(self):
        if self.research:
            self.research.revoke()
        process = self.process
        if not process:
            self.stopped_verified = True
            return True
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
            self.stopped_verified = process.returncode is not None
        return self.stopped_verified
