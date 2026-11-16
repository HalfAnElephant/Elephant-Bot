"""Offline control-plane benchmark using QQ Official event fixtures and real SQLite."""
import argparse
import asyncio
import json
import math
import platform
import random
import resource
import sqlite3
import subprocess
import tempfile
import time
from pathlib import Path

import httpx
from sqlalchemy import func, select

from qqbot.app import create_app
from qqbot.clock import Clock
from qqbot.config import AdapterConfig, Settings
from qqbot.storage.schema import inbox
from qqbot.storage.task_schema import tasks

CONVERSATIONS = 20
DUPLICATE_PERIOD = 10
MAX_EVENTS = 1_000_000


def positive_int(raw):
    value = int(raw)
    if value < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return value


def positive_float(raw):
    value = float(raw)
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError("must be finite and positive")
    return value


def percentiles(values):
    ordered = sorted(values)

    def percentile(ratio):
        return round(ordered[math.ceil(len(ordered) * ratio) - 1], 4) if ordered else None

    return {"count": len(ordered), "p50": percentile(.50), "p95": percentile(.95), "p99": percentile(.99)}


def revision():
    def git(*args):
        return subprocess.run(["git", *args], cwd=Path(__file__).resolve().parents[1],
                              capture_output=True, text=True, check=True, timeout=2).stdout.strip()

    try:
        return git("rev-parse", "HEAD"), bool(git("status", "--porcelain"))
    except (OSError, subprocess.SubprocessError):
        return None, None


def official_fixture(index, sender, timestamp):
    """Public QQ Official GROUP_AT_MESSAGE_CREATE format, without callback transport."""
    return {"op": 0, "id": f"offline-event-{index}", "t": "GROUP_AT_MESSAGE_CREATE", "d": {
        "id": f"offline-message-{index}", "group_openid": f"offline-group-{index % CONVERSATIONS}",
        "author": {"member_openid": f"offline-user-{sender}"}, "content": "合成离线控制面事件",
        "timestamp": timestamp.isoformat(),
    }}


async def benchmark(args):
    total = args.seconds * args.rate
    rng = random.Random(args.seed)
    clock = Clock()
    latency = {phase: [] for phase in ("normalization", "ingress", "duplicate", "task_accept",
        "task_query", "task_cancel", "inbox_tick", "ingress_lag")}
    counters = {"accepted": 0, "duplicates_attempted": 0, "duplicates_rejected": 0, "tasks_admitted": 0,
                    "tasks_queried": 0, "tasks_cancelled": 0, "network_attempts": 0}
    rss = []

    def offline(_request):
        counters["network_attempts"] += 1
        raise RuntimeError("network access is not allowed in this benchmark")

    def measure(phase, fn, *values):
        before = time.perf_counter()
        result = fn(*values)
        latency[phase].append((time.perf_counter() - before) * 1000)
        return result

    transport = httpx.MockTransport(offline)
    with tempfile.TemporaryDirectory(prefix="elephant-load-") as directory:
        async with (httpx.AsyncClient(transport=transport, trust_env=False) as qq_client,
                    httpx.AsyncClient(transport=transport, trust_env=False) as model_client):
            config = AdapterConfig(instance_id="offline-load", kind="official", transport="webhook",
                bot_id="offline-bot", app_id="offline-app", secret_env="ELEPHANT_BENCH_UNUSED_SECRET",
                allowed_groups=tuple(f"offline-group-{i}" for i in range(CONVERSATIONS)))
            settings = Settings(database=str(Path(directory) / "load.db"), adapters=(config,),
                interactive_execution_enabled=False)
            app = create_app(settings, clock=clock, run_background=False,
                official_client=qq_client, model_client=model_client)
            core = app.state.core
            adapter = core.adapters[config.instance_id]
            started = time.monotonic()

            async def consume():
                while not core.stopping:
                    before = time.perf_counter()
                    await core.tick(dispatch=False)
                    latency["inbox_tick"].append((time.perf_counter() - before) * 1000)
                    await asyncio.sleep(.05)

            # Never start Scheduler, Worker, adapter listener, or QQ delivery.
            consumer = asyncio.create_task(consume())
            try:
                for index in range(total):
                    deadline = started + index / args.rate
                    await asyncio.sleep(max(0, deadline - time.monotonic()))
                    if consumer.done():
                        consumer.result()
                    latency["ingress_lag"].append(max(0, time.monotonic() - deadline) * 1000)
                    payload = official_fixture(index, rng.randrange(4), clock.now())
                    event = measure("normalization", adapter.normalize, payload)
                    accepted, fresh = measure("ingress", core.inbox.accept, event)
                    counters["accepted"] += int(fresh)
                    if index % DUPLICATE_PERIOD == 0:
                        counters["duplicates_attempted"] += 1
                        _, duplicate_fresh = measure("duplicate", core.inbox.accept, event)
                        counters["duplicates_rejected"] += int(not duplicate_fresh)
                    if index % args.rate == 0:
                        access = core.access(event)
                        task = measure("task_accept", core.tasks.accept, accepted, access,
                            "Offline control-plane task admission")
                        counters["tasks_admitted"] += 1
                        result = measure("task_query", core.tasks.get, task["public_id"], access)
                        counters["tasks_queried"] += int(result["id"] == task["id"])
                        cancelled = measure("task_cancel", core.tasks.cancel, task["public_id"], access)
                        counters["tasks_cancelled"] += int(cancelled["state"] == "CANCELLED")
                    if index % (args.rate * 30) == 0:
                        rss.append({"elapsed_s": round(time.monotonic() - started, 4),
                            "maxrss": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss})
                drain_start = time.monotonic()
                while True:
                    if consumer.done():
                        consumer.result()
                    if not core.inbox.pending(limit=1):
                        timed_out = False
                        break
                    left = args.drain_timeout - (time.monotonic() - drain_start)
                    if left <= 0:
                        timed_out = True
                        break
                    await asyncio.sleep(min(.05, left))
                elapsed = time.monotonic() - started
                drain_elapsed = time.monotonic() - drain_start
                with core.db.engine.connect() as connection:
                    states = dict(connection.execute(select(inbox.c.state, func.count()).group_by(inbox.c.state)).all())
                    task_states = dict(connection.execute(
                        select(tasks.c.state, func.count()).group_by(tasks.c.state)).all())
            finally:
                consumer.cancel()
                await asyncio.gather(consumer, return_exceptions=True)
                try:
                    await core.stop()
                finally:
                    core.db.close()
    rss.append({"elapsed_s": round(elapsed, 4),
        "maxrss": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss})
    timing = {phase: percentiles(samples) for phase, samples in latency.items()}
    git_commit, git_dirty = revision()
    expected_duplicates = (total + DUPLICATE_PERIOD - 1) // DUPLICATE_PERIOD
    completed = states == {"DONE": total} and not timed_out
    result = {
        "report_version": 2, "scope": "QQ Official synthetic control plane, real local SQLite/Core/TaskService; no model/QQ/Pi",
        "git_commit": git_commit, "git_dirty": git_dirty, "seed": args.seed,
        "machine": platform.platform(), "python": platform.python_version(), "sqlite": sqlite3.sqlite_version,
        "events": total, "conversations": min(CONVERSATIONS, total), "rate_per_second": args.rate,
        "scheduled_duration_seconds": args.seconds, "elapsed_seconds": round(elapsed, 4),
        "inbox_states": states, "task_states": task_states, "counters": counters, "latency_ms": timing,
        "ingress_p95_ms": timing["ingress"]["p95"], "task_accept_p95_ms": timing["task_accept"]["p95"],
        "task_query_p95_ms": timing["task_query"]["p95"], "task_samples": counters["tasks_admitted"],
        "rss_samples": rss, "rss_unit": "bytes" if platform.system() == "Darwin" else "KiB",
        "drain": {"completed": completed, "timed_out": timed_out, "elapsed_seconds": round(drain_elapsed, 4),
            "timeout_seconds": args.drain_timeout},
    }
    result["passed"] = (completed and counters["accepted"] == total and counters["network_attempts"] == 0
        and counters["duplicates_attempted"] == counters["duplicates_rejected"] == expected_duplicates
        and counters["tasks_admitted"] == counters["tasks_queried"] == counters["tasks_cancelled"] == args.seconds
        and task_states == {"CANCELLED": args.seconds})
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=positive_int, default=600)
    parser.add_argument("--rate", type=positive_int, default=50)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--drain-timeout", type=positive_float, default=30)
    parser.add_argument("--output", type=Path, help="write to a new JSON file (exclusive); stdout is always emitted")
    args = parser.parse_args(argv)
    if args.seconds * args.rate > MAX_EVENTS:
        parser.error(f"seconds × rate must not exceed {MAX_EVENTS} events")
    if args.output and (args.output.exists() or args.output.is_symlink()):
        parser.error("output already exists; historical results are never overwritten")
    if args.output and not args.output.parent.is_dir():
        parser.error("output parent directory does not exist")
    report = asyncio.run(benchmark(args))
    serialized = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        try:
            with args.output.open("x", encoding="utf-8") as fp:
                fp.write(serialized + "\n")
        except OSError as error:
            parser.error(f"cannot write output: {error}")
    print(serialized)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
