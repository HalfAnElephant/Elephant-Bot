"""Await launch and teardown even when a tool request is cancelled."""
import asyncio
import os
import signal
import sys
from pathlib import Path
from uuid import uuid4

HOST_LAUNCH = (
    "import os,sys; "
    "fd=os.open(sys.argv[1],os.O_WRONLY|os.O_NOFOLLOW); "
    "os.write(fd,str(os.getpid()).encode()); os.close(fd); "
    "os.execvpe(sys.argv[2],sys.argv[2:],os.environ)"
)


def create_host_cgroup():
    if sys.platform != "linux":
        raise RuntimeError("host process execution requires Linux with delegated cgroup v2")
    root = Path("/sys/fs/cgroup")
    if not (root / "cgroup.controllers").is_file():
        raise RuntimeError("host process execution requires cgroup v2")
    path = next((line.split(":", 2)[2] for line in Path("/proc/self/cgroup").read_text().splitlines()
                 if line.startswith("0::")), "")
    if not path.startswith("/") or ".." in Path(path).parts:
        raise RuntimeError("host process cgroup is unavailable")
    group = root / path.lstrip("/") / ("elephant-process-" + uuid4().hex)
    try:
        group.mkdir(mode=0o700)
        if not (group / "cgroup.kill").exists():
            group.rmdir()
            raise RuntimeError("host process execution requires cgroup.kill support")
    except OSError as error:
        raise RuntimeError("host process execution requires systemd Delegate=yes") from error
    return group


async def stop_host_cgroup(group):
    if not group.exists():
        return
    (group / "cgroup.kill").write_text("1")
    async with asyncio.timeout(10):
        while dict(line.split() for line in (group / "cgroup.events").read_text().splitlines()).get("populated") != "0":
            await asyncio.sleep(0.02)
    for directory, _, _ in os.walk(group, topdown=False):
        Path(directory).rmdir()


async def stop_process(process, *, cgroup=None):
    async def cleanup():
        if process is not None:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        if cgroup is not None:
            await stop_host_cgroup(cgroup)
        if process is not None:
            if process.stdin:
                process.stdin.close()
            async with asyncio.timeout(10 if cgroup is not None else None):
                await process.communicate()
                await process.wait()
    task = asyncio.create_task(cleanup())
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
    task.result()
    if cancelled:
        raise asyncio.CancelledError


async def start_process(*argv, cgroup=None, **options):
    launch = asyncio.create_task(asyncio.create_subprocess_exec(*argv, **options))
    try:
        return await asyncio.shield(launch)
    except asyncio.CancelledError:
        while not launch.done():
            try:
                await asyncio.shield(launch)
            except asyncio.CancelledError:
                pass
        if not launch.cancelled() and launch.exception() is None:
            await stop_process(launch.result(), cgroup=cgroup)
        raise
