"""Await launch and teardown even when a tool request is cancelled."""
import asyncio
import os
import signal


async def stop_process(process):
    async def cleanup():
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        if process.stdin:
            process.stdin.close()
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


async def start_process(*argv, **options):
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
            await stop_process(launch.result())
        raise
