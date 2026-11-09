"""Local control-plane load only: synthetic QQ events, no provider or QQ network calls."""
import argparse
import asyncio
import json
import math
import os
import platform
import resource
import tempfile
import time
from pathlib import Path

from sqlalchemy import func, select

from qqbot.app import create_app
from qqbot.clock import Clock
from qqbot.config import AdapterConfig, Settings
from qqbot.domain import AccessContext
from qqbot.storage.schema import inbox


def p95(values):
    return sorted(values)[max(0, math.ceil(len(values)*.95)-1)] if values else None


async def benchmark(args):
    os.environ["BENCH_ONEBOT_TOKEN"] = "offline-fixture"
    clock = Clock()
    with tempfile.TemporaryDirectory(prefix="elephant-load-") as directory:
        config = AdapterConfig(instance_id="load", kind="onebot", bot_id="100", secret_env="BENCH_ONEBOT_TOKEN",
            allowed_groups=tuple(str(i) for i in range(20)))
        app = create_app(Settings(database=str(Path(directory)/"load.db"), adapters=(config,)), clock=clock, run_background=False)
        core = app.state.core
        adapter = core.adapters["load"]
        await adapter.start()
        loop = asyncio.create_task(core.run())
        ingress, task_latency, query_latency, rss = [], [], [], []
        started = time.monotonic()
        total = int(args.seconds*args.rate)
        for i in range(total):
            deadline = started+i/args.rate
            await asyncio.sleep(max(0, deadline-time.monotonic()))
            event = adapter.normalize({"self_id":100,"post_type":"message","message_type":"group", "user_id":300+i%4,
                "group_id":i%20,"message_id":i+1,"time":int(clock.now().timestamp()),
                "message":[{"type":"text","data":{"text":"普通讨论：方案细节"}}]})
            before = time.perf_counter()
            accepted, _ = core.inbox.accept(event)
            if i%10 == 0:
                core.inbox.accept(event)
            ingress.append((time.perf_counter()-before)*1000)
            if i%int(args.rate) == 0:
                access = AccessContext(principal=event.principal, conversation=event.conversation, role="admin")
                before = time.perf_counter()
                task = core.tasks.accept(accepted, access, "Offline control-plane admission benchmark")
                task_latency.append((time.perf_counter()-before)*1000)
                before = time.perf_counter()
                core.tasks.get(task["public_id"], access)
                query_latency.append((time.perf_counter()-before)*1000)
                core.tasks.cancel(task["public_id"], access)
            if i%int(args.rate*30) == 0:
                rss.append({"elapsed_s":round(time.monotonic()-started,2),"maxrss":resource.getrusage(resource.RUSAGE_SELF).ru_maxrss})
        drain_start = time.monotonic()
        while core.inbox.pending() or core.active:
            if time.monotonic()-drain_start > 30:
                break
            await asyncio.sleep(.1)
        core.stopping = True
        loop.cancel()
        await asyncio.gather(loop, return_exceptions=True)
        with core.db.engine.connect() as connection:
            counts = dict(connection.execute(select(inbox.c.state, func.count()).group_by(inbox.c.state)).all())
        report = {"scope":"synthetic platform, real local SQLite/Core/TaskService; no model/QQ/harness load",
            "machine":platform.platform(),"python":platform.python_version(),"events":total,"conversations":20,
            "rate_per_second":args.rate,"scheduled_duration_seconds":args.seconds,
            "elapsed_seconds":round(time.monotonic()-started,2),"ingress_p95_ms":p95(ingress),
            "task_accept_p95_ms":p95(task_latency),"task_query_p95_ms":p95(query_latency),
            "task_samples":len(task_latency),"inbox_states":counts,"rss_samples":rss,
            "window_count":len(core.windows),"max_window_messages":max((len(w.events) for w in core.windows.values()),default=0)}
        report["passed"] = (counts.get("DONE",0)==total and sum(counts.values())==total
            and report["ingress_p95_ms"]<150 and report["task_accept_p95_ms"]<300 and report["task_query_p95_ms"]<300)
        Path(args.output).write_text(json.dumps(report,ensure_ascii=False,indent=2))
        print(json.dumps(report,ensure_ascii=False))
        await core.stop()
        await adapter.stop()
        core.db.close()
        if not report["passed"]:
            raise SystemExit(1)


if __name__ == "__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--seconds",type=int,default=600)
    parser.add_argument("--rate",type=int,default=50)
    parser.add_argument("--output",default="docs/control-plane-benchmark.json")
    asyncio.run(benchmark(parser.parse_args()))
