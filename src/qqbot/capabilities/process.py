"""Run-owned Linux namespace processes. No task-lease handoff is implied."""
import asyncio
import shutil
import sys
from pathlib import Path
from uuid import uuid4

from .subprocesses import start_process, stop_process
from .types import Action, Capability


class ProcessService:
    def __init__(self, *, executable="bwrap", max_processes=4, max_bytes=1048576):
        self.executable, self.max_processes, self.max_bytes = executable, max_processes, max_bytes
        self.processes = {}
        self.lock = asyncio.Lock()

    def _owned(self, arguments, context):
        record = self.processes[arguments["process_ref"]]
        if record["owner"] != (context.task_id, context.run_id, context.fence):
            raise PermissionError("process belongs to another run")
        return record

    async def start(self, arguments, context):
        if sys.platform != "linux" or not shutil.which(self.executable):
            raise RuntimeError("isolated process execution requires Linux bubblewrap")
        argv = arguments["argv"]
        if not isinstance(argv, list) or not argv or any(not isinstance(a, str) or "\x00" in a for a in argv):
            raise ValueError("argv must be a nonempty string array")
        timeout = arguments.get("timeout_seconds", 300)
        stdin = arguments.get("stdin", "")
        if not isinstance(timeout, (int, float)) or not 1 <= timeout <= 1800:
            raise ValueError("process timeout_seconds must be 1..1800")
        if not isinstance(stdin, str) or len(stdin.encode()) > 1048576:
            raise ValueError("process stdin must be a bounded string")
        workspace = Path(context.workspace).resolve(strict=True)
        if workspace.is_symlink() or not workspace.is_dir():
            raise ValueError("workspace must be a directory")
        command = [self.executable, "--unshare-all", "--die-with-parent", "--new-session",
                   "--clearenv", "--setenv", "PATH", "/usr/local/bin:/usr/bin:/bin",
                   "--setenv", "HOME", "/tmp", "--setenv", "LANG", "C.UTF-8"]
        for directory in ("/usr", "/bin", "/lib", "/lib64"):
            if Path(directory).exists():
                command.extend(["--ro-bind", directory, directory])
        command.extend(["--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp",
                        "--bind", str(workspace), "/workspace", "--chdir", "/workspace", "--", *argv])
        async with self.lock:
            if sum(not record["cleanup_done"] for record in self.processes.values()) >= self.max_processes:
                raise ValueError("process capacity exhausted")
            process = await start_process(*command, env={}, start_new_session=True,
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
            reference = "process:" + str(uuid4())
            record = {"owner": (context.task_id, context.run_id, context.fence), "process": process,
                      "output": bytearray(), "total_bytes": 0, "expired": False, "cleanup_done": False}
            self.processes[reference] = record
        async def supervise():
            async def drain():
                while chunk := await process.stdout.read(65536):
                    record["total_bytes"] += len(chunk)
                    record["output"].extend(chunk[:max(0, self.max_bytes - len(record["output"]))])
                await process.wait()
            async def feed():
                try:
                    process.stdin.write(stdin.encode())
                    await process.stdin.drain()
                except (BrokenPipeError, ConnectionResetError):
                    pass
                finally:
                    process.stdin.close()
            try:
                await asyncio.wait_for(asyncio.gather(feed(), drain()), timeout)
            except TimeoutError:
                record["expired"] = True
            finally:
                await self._cleanup(record)
        record["supervisor"] = asyncio.create_task(supervise())
        return {"process_ref": reference, "pid": process.pid, "running": True}

    async def _cleanup(self, record):
        try:
            await stop_process(record["process"])
        except asyncio.CancelledError:
            record["cleanup_done"] = True
            raise
        record["cleanup_done"] = True

    async def _stop(self, record):
        supervisor = record.get("supervisor")
        if supervisor and not supervisor.done():
            supervisor.cancel()
        try:
            if supervisor:
                results = await asyncio.gather(supervisor, return_exceptions=True)
                if isinstance(results[0], BaseException) and not isinstance(results[0], asyncio.CancelledError):
                    raise results[0]
        finally:
            if not record["cleanup_done"]:
                await self._cleanup(record)

    async def read(self, arguments, context):
        record = self._owned(arguments, context)
        offset, limit = arguments.get("offset", 0), arguments.get("limit", 8192)
        if type(offset) is not int or type(limit) is not int or offset < 0 or not 1 <= limit <= 65536:
            raise ValueError("invalid output page")
        output = bytes(record["output"])
        return {"process_ref": arguments["process_ref"], "text": output[offset:offset + limit].decode(errors="replace"),
                "next_offset": min(len(output), offset + limit), "exit_code": record["process"].returncode,
                "truncated": record["total_bytes"] > len(output), "expired": record["expired"]}

    async def wait(self, arguments, context):
        record = self._owned(arguments, context)
        seconds = arguments.get("wait_seconds", 1)
        if not isinstance(seconds, (int, float)) or not 0 <= seconds <= 30:
            raise ValueError("wait_seconds must be 0..30")
        try:
            await asyncio.wait_for(asyncio.shield(record["supervisor"]), seconds)
        except TimeoutError:
            pass
        except asyncio.CancelledError:
            await self._stop(record)
            raise
        return await self.read(arguments, context)

    async def terminate(self, arguments, context):
        record = self._owned(arguments, context)
        await self._stop(record)
        return await self.read(arguments, context)

    def active_run(self, context):
        return any(record["owner"] == (context.task_id, context.run_id, context.fence)
                   and not record["cleanup_done"] for record in self.processes.values())

    async def close_run(self, context):
        records = [record for record in self.processes.values()
                   if record["owner"] == (context.task_id, context.run_id, context.fence)]
        for record in records:
            await self._stop(record)
        self.processes = {ref: record for ref, record in self.processes.items() if record not in records}

    def register(self, registry):
        for name in ("start", "read", "wait", "terminate"):
            effect = "write" if name in {"start", "terminate"} else "read"
            properties = {"argv": {"type": "array", "items": {"type": "string"}, "minItems": 1},
                          "stdin": {"type": "string"}, "timeout_seconds": {"type": "integer", "minimum": 1,
                          "maximum": 1800}} if name == "start" else {
                          "process_ref": {"type": "string"}, "offset": {"type": "integer", "minimum": 0},
                          "limit": {"type": "integer", "minimum": 1, "maximum": 65536},
                          "wait_seconds": {"type": "number", "minimum": 0, "maximum": 30}}
            registry.register(Capability("process." + name, "Isolated run-owned workspace process " + name,
                {"type": "object", "properties": properties, "required": ["argv" if name == "start" else "process_ref"],
                 "additionalProperties": False}, timeout_seconds=35), getattr(self, name),
                lambda a, c, n=name, e=effect: Action("workspace.process." + n, ("workspace:" + c.workspace,), e))
