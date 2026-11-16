"""Controlled mixed-load baseline through Core, Scheduler, real Pi, tools and Outbox."""
import argparse
import asyncio
import hashlib
import json
import os
import platform
import resource
import sqlite3
import subprocess
import tempfile
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx
from benchmark_control_plane import percentiles, process_tree_rss, revision, sample_runtime, sqlite_timings
from sqlalchemy import func, select

from elephant.adapters.pi.sessions import export_session
from elephant.app import create_app
from elephant.config import (
    AdapterConfig,
    ModelConfig,
    OrchestrationConfig,
    OwnerConfig,
    QQOwnerBinding,
    Settings,
)
from elephant.harness.pi import PiHarness
from elephant.runtime.scheduler import Scheduler
from elephant.storage.schema import outbox
from elephant.storage.task_schema import model_requests, observations, runs, tasks

ROOT = Path(__file__).resolve().parents[1]
ANSWER = "合成基准任务已完成。"


def completion(message):
    calls = message.get("tool_calls")
    if calls:
        message = {**message, "tool_calls": [{"index": i, **call} for i, call in enumerate(calls)]}
    frame = {"id": "synthetic", "object": "chat.completion.chunk", "model": "offline", "created": 1,
             "choices": [{"index": 0, "delta": message, "finish_reason": None}]}
    end = {**frame, "choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls" if calls else "stop"}],
           "usage": {"prompt_tokens": 100, "completion_tokens": 10}}
    return [f"data: {json.dumps(item)}\n\n".encode() for item in (frame, end)] + [b"data: [DONE]\n\n"]


class DelayedCompletion(httpx.AsyncByteStream):
    def __init__(self, message, first_delay, tail_delay):
        self.frames = completion(message)
        self.first_delay, self.tail_delay = first_delay, tail_delay

    async def __aiter__(self):
        await asyncio.sleep(self.first_delay)
        yield self.frames[0]
        await asyncio.sleep(self.tail_delay)
        for frame in self.frames[1:]:
            yield frame


async def mixed_load(directory, background):
    latency, rss, foreground = {}, [], []
    first_text, acquired_at, claims = {}, {}, []
    deliveries, model_calls, tool_reads = [], {}, []
    os.environ["ELEPHANT_BENCH_QQ_SECRET"] = "synthetic-qq-secret"
    config = AdapterConfig(instance_id="offline", kind="official", app_id="offline-app", bot_id="offline-bot",
        secret_env="ELEPHANT_BENCH_QQ_SECRET", allowed_groups=("foreground",), markdown_enabled=False)

    async def qq(request):
        if request.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "synthetic-token", "expires_in": 7200})
        if not request.url.path.endswith("/groups/foreground/messages"):
            raise AssertionError("unexpected synthetic QQ endpoint")
        assert json.loads(request.content)["content"] == ANSWER, "foreground answer differs from the fixture"
        before = time.perf_counter()
        await asyncio.sleep(.03)
        latency.setdefault("qq_transport", []).append((time.perf_counter() - before) * 1000)
        deliveries.append(time.perf_counter())
        return httpx.Response(200, json={"id": "synthetic-delivery-" + str(len(deliveries))})

    settings = Settings(database=str(directory / "core.db"), tasks_enabled=True, adapters=(config,),
        owner=OwnerConfig(qq_bindings=(QQOwnerBinding(app_id=config.app_id,
            user_openid="synthetic-owner", verified_at=datetime.now(UTC).isoformat()),)),
        model=ModelConfig(model="offline", endpoint="https://model.invalid/v1/chat/completions",
                          api_key_env="ELEPHANT_BENCH_UNUSED_SECRET"),
        orchestration=OrchestrationConfig(pi_executable=str(ROOT / "node_modules/.bin/pi"),
                                          leader_inbox_coalesce_seconds=0, model_request_limit=4))
    settings = settings.model_copy(update={"execution": settings.execution.model_copy(update={
        "notify_before_tools": False, "warm_idle_max": 16})})
    async with httpx.AsyncClient(transport=httpx.MockTransport(qq)) as qq_client:
        core = create_app(settings, run_background=False, official_client=qq_client).state.core
        scheduler = Scheduler(core, core.leader)
        core.scheduler = scheduler
        factory = scheduler.pool.factory
        slot = scheduler.capacity.slot

        @asynccontextmanager
        async def measured_slot(background_request):
            before = time.perf_counter()
            async with slot(background_request):
                phase = "model_slot_wait_background" if background_request else "model_slot_wait_foreground"
                latency.setdefault(phase, []).append((time.perf_counter() - before) * 1000)
                yield

        scheduler.capacity.slot = measured_slot

        def instrumented_factory(claim, process_id, session_id):
            runner = factory(claim, process_id, session_id)

            async def model(request):
                if str(request.url) != settings.model.endpoint:
                    raise AssertionError("unexpected synthetic model endpoint")
                key = runner.claim["run_id"]
                model_calls[key] = model_calls.get(key, 0) + 1
                worker = runner.claim.get("role", "worker") == "worker"
                if worker and model_calls[key] == 1:
                    message = {"role": "assistant", "content": None, "tool_calls": [{
                        "id": "read-fixture", "type": "function", "function": {"name": "capability_invoke",
                            "arguments": json.dumps({"name": "workspace.read", "arguments": {"path": "fixture.txt"}})}}]}
                else:
                    message = {"role": "assistant", "content": ANSWER}
                return httpx.Response(200, headers={"content-type": "text/event-stream"},
                    stream=DelayedCompletion(message, .15 if worker else .02, .03))

            runner.gateway.transport = httpx.MockTransport(model)
            start = runner.start

            async def ready():
                before = time.perf_counter()
                await start()
                await runner.command("get_state", wait=True)
                latency.setdefault("pi_cold_ready", []).append((time.perf_counter() - before) * 1000)

            runner.start = ready
            capability, read, classify = runner.gateway.registry.entries["workspace.read"]

            async def measured_read(arguments, context):
                before = time.perf_counter()
                result = await read(arguments, context)
                assert result["text"] == "synthetic fixture\n"
                tool_reads.append(context.run_id)
                latency.setdefault("workspace_read", []).append((time.perf_counter() - before) * 1000)
                return result

            runner.gateway.registry.entries["workspace.read"] = (capability, measured_read, classify)
            return runner

        scheduler.pool.factory = instrumented_factory
        acquire = scheduler.pool.acquire

        async def measured_acquire(claim):
            before = time.perf_counter()
            resident = {entry["runner"] for entry in scheduler.pool.entries.values()}
            runner = await acquire(claim)
            acquired_at[claim["run_id"]] = time.perf_counter()
            claims.append({"run_id": claim["run_id"], "task_id": claim["task_id"],
                           "role": claim.get("role", "worker"), "purpose": claim.get("purpose", "work"),
                           "warm": runner in resident})
            phase = "pi_warm_acquire" if runner in resident else "pi_cold_acquire"
            latency.setdefault(phase, []).append((time.perf_counter() - before) * 1000)
            (runner.gateway.workspace / "fixture.txt").write_text("synthetic fixture\n")
            return runner

        scheduler.pool.acquire = measured_acquire
        event_sink = scheduler.worker.event_sink

        def measured_events(identity):
            save = event_sink(identity)

            def accept(event):
                change = event.get("assistantMessageEvent") or {}
                if change.get("type") == "text_delta" and change.get("delta"):
                    first_text.setdefault(identity["run_id"], time.perf_counter())
                save(event)

            return accept

        scheduler.worker.event_sink = measured_events
        sampler = asyncio.create_task(sample_runtime(latency, rss))
        started, cpu_before = time.perf_counter(), time.process_time()
        children_before = resource.getrusage(resource.RUSAGE_CHILDREN)

        async def step_until(predicate):
            async with asyncio.timeout(120):
                while not predicate():
                    await core.tick(dispatch=False)
                    await scheduler.step()
                    await core.deliver()
                    if sampler.done():
                        sampler.result()
                    await asyncio.sleep(.02)

        try:
            with sqlite_timings(core.db, latency):
                background_ids = []
                for index in range(background):
                    receipt = core.enqueue_admin_task(f"Read fixture.txt for synthetic task {index}.",
                        idempotency_key=f"background-{index}")
                    background_ids.append(receipt["task_id"])
                adapter = core.adapters["offline"]
                for turn in range(3):
                    payload = {"op": 0, "id": f"event-{turn}", "t": "GROUP_AT_MESSAGE_CREATE", "d": {
                        "id": f"message-{turn}", "group_openid": "foreground",
                        "author": {"member_openid": "synthetic-owner"}, "content": "Say hello without tools.",
                        "timestamp": core.clock.now().isoformat()}}
                    before = time.perf_counter()
                    event = adapter.normalize(payload)
                    _, fresh = core.inbox.accept(event)
                    assert fresh
                    expected = len(deliveries) + 1
                    await step_until(lambda count=expected: len(deliveries) >= count)
                    with core.db.engine.connect() as conn:
                        active_workers = conn.scalar(select(func.count()).select_from(runs).where(
                            runs.c.role == "worker", runs.c.slot_reserved == 1))
                    foreground.append({"turn": turn, "elapsed_ms": (deliveries[-1] - before) * 1000,
                                       "active_background_runs_at_delivery": active_workers})

                def drained():
                    with core.db.engine.connect() as conn:
                        states = conn.execute(select(tasks.c.state).where(tasks.c.id.in_(background_ids))).scalars().all()
                    return all(state == "SUCCEEDED" for state in states) and not scheduler.active

                await step_until(drained)
            with core.db.engine.connect() as conn:
                task_states = dict(conn.execute(select(tasks.c.state, func.count()).group_by(tasks.c.state)).all())
                delivery_states = dict(conn.execute(select(outbox.c.state, func.count()).group_by(outbox.c.state)).all())
                usage = [json.loads(raw) for raw in conn.execute(select(model_requests.c.usage_json).where(
                    model_requests.c.kind != "tool", model_requests.c.state == "COMPLETED")).scalars()]
                observation_count = conn.scalar(select(func.count()).select_from(observations))
                queue = conn.execute(select(tasks.c.created_at, runs.c.started_at).join(
                    runs, runs.c.task_id == tasks.c.id).where(runs.c.role == "worker")).all()
            sessions = []
            for path in sorted(directory.rglob("*.jsonl")):
                sessions.append({"bytes": path.stat().st_size, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
            latency["foreground_roundtrip"] = [item["elapsed_ms"] for item in foreground]
            latency["pi_first_text_after_acquire"] = [(when - acquired_at[key]) * 1000
                for key, when in first_text.items()]
            latency["provider_first_token"] = [item["ttft_ms"] for item in usage if item.get("ttft_ms") is not None]
            latency["provider_complete"] = [item["elapsed_ms"] for item in usage]
            latency["worker_queue"] = [(datetime.fromisoformat(started) - datetime.fromisoformat(created)).total_seconds()
                * 1000 for created, started in queue]
            assert rss, "process-tree sampling did not produce evidence"
            result = {"background_tasks": background, "foreground": foreground,
                "latency_ms": {name: percentiles(values) for name, values in latency.items()},
                "process_tree_peak_rss_kib": max(rss),
                "workload_elapsed_seconds": time.perf_counter() - started,
                "task_states": task_states, "delivery_states": delivery_states,
                "tool_reads": len(tool_reads), "observations": observation_count, "model_calls": sum(model_calls.values()),
                "warm_hits": sum(item["warm"] for item in claims), "acquisitions": len(claims),
                "claims": claims, "sessions": sessions,
                "estimated_input_tokens": sum(item.get("input_tokens_estimated", 0) for item in usage),
                "passed": len(deliveries) == 3 and len(tool_reads) == observation_count == background
                    and (task_states == {"SUCCEEDED": 2 * background} if background else not task_states)
                    and delivery_states == {"SENT": 3}}
        finally:
            sampler.cancel()
            await asyncio.gather(sampler, return_exceptions=True)
            await core.stop()
            core.db.close()
        children_after = resource.getrusage(resource.RUSAGE_CHILDREN)
        result["cpu_self_seconds"] = time.process_time() - cpu_before
        result["cpu_children_seconds"] = (children_after.ru_utime + children_after.ru_stime
            - children_before.ru_utime - children_before.ru_stime)
        result["elapsed_seconds"] = time.perf_counter() - started
        return result


async def history_export(directory, messages):
    workspace = directory / f"history-{messages}"
    session_id = str(uuid4())
    path = workspace / "sessions" / (session_id + ".jsonl")
    path.parent.mkdir(parents=True)
    before = time.perf_counter()
    with path.open("x") as fp:
        fp.write(json.dumps({"type": "session", "version": 3, "id": session_id,
            "timestamp": datetime.now(UTC).isoformat(), "cwd": str(workspace)}) + "\n")
        parent = None
        for index in range(messages):
            entry = {"type": "message", "id": str(index), "parentId": parent,
                     "timestamp": datetime.now(UTC).isoformat(),
                     "message": {"role": "user", "content": "synthetic history " * 64,
                                 "timestamp": 1}}
            fp.write(json.dumps(entry) + "\n")
            parent = str(index)
    creation_ms = (time.perf_counter() - before) * 1000
    samples = []
    for _ in range(3):
        before = time.perf_counter()
        page = export_session(path, session_id="synthetic-history", after=messages - 9, limit=10)
        samples.append((time.perf_counter() - before) * 1000)
        assert len(page["events"]) == 10 and page["next_cursor"] == messages + 1 and not page["has_more"]
    ready, resident_rss = [], []
    for _ in range(3):
        runner = PiHarness(ROOT / "node_modules/.bin/pi", workspace, session_id=session_id)
        try:
            before = time.perf_counter()
            await runner.start()
            state = await runner.command("get_state", wait=True)
            ready.append((time.perf_counter() - before) * 1000)
            assert state["messageCount"] == messages and Path(state["sessionFile"]).resolve() == path.resolve()
            resident_rss.append(await asyncio.to_thread(process_tree_rss))
        finally:
            assert await runner.stop(), "Pi restart requires a verified stop"
    return {"messages_scanned_per_export": messages, "file_bytes": path.stat().st_size,
            "returned_events": 10, "creation_ms": creation_ms, "export_ms": percentiles(samples),
            "pi_cold_ready_ms": percentiles(ready), "process_tree_rss_kib": resident_rss}


async def benchmark(repeats):
    commit, dirty = revision()
    with tempfile.TemporaryDirectory(prefix="elephant-runtime-") as folder:
        directory = Path(folder)
        mixed = []
        for background in (0, 1, 10):
            for repeat in range(repeats):
                run = directory / f"mixed-{background}-{repeat}"
                run.mkdir()
                mixed.append({"repeat": repeat, **await mixed_load(run, background)})
        history = [await history_export(directory, count) for count in (100, 10000)]
    return {"report_version": 1, "git_commit": commit, "git_dirty": dirty,
        "recorded_at": datetime.now(UTC).isoformat(), "platform": platform.platform(),
        "python": platform.python_version(), "sqlite": sqlite3.sqlite_version, "cpu_count": os.cpu_count(),
        "scope": "real Core/Scheduler/Pi/tools/Outbox with synthetic provider and QQ transport",
        "fixture": {"provider_first_token_ms": {"leader": 20, "worker": 150},
                    "provider_tail_ms": 30, "qq_ack_ms": 30, "model_request_limit": 4,
                    "warm_idle_max": 16, "warm_ttl_seconds": 300, "foreground_turns": 3,
                    "prewarm_turns": 0, "rss_sampling_seconds": .5},
        "mixed": mixed, "history": history, "passed": all(run["passed"] for run in mixed)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, choices=range(1, 6), default=3)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.output.exists() or args.output.is_symlink() or not args.output.parent.is_dir():
        parser.error("output must be a new file in an existing directory")
    report = asyncio.run(benchmark(args.repeats))
    report["node"] = subprocess.check_output(["node", "--version"], text=True, timeout=10).strip()
    report["pi"] = subprocess.check_output([str(ROOT / "node_modules/.bin/pi"), "--version"], text=True, timeout=10).strip()
    with args.output.open("x", encoding="utf-8") as fp:
        json.dump(report, fp, ensure_ascii=False, indent=2)
        fp.write("\n")
    print(json.dumps({"passed": report["passed"], "mixed_runs": len(report["mixed"]),
                      "history_sizes": [item["messages_scanned_per_export"] for item in report["history"]]}))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
