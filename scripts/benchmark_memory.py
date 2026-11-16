"""Repeatable OpenViking adapter timings with synthetic HTTP extraction and indexing."""
import argparse
import asyncio
import json
import os
import platform
import sqlite3
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import httpx
from benchmark_control_plane import percentiles, positive_float, positive_int, revision

from elephant.adapters.openviking import OpenVikingMemory
from elephant.adapters.openviking.service import MEMORY_TYPES
from elephant.cli import migrate
from elephant.clock import Clock
from elephant.contracts.memory import MemoryBinding, MemoryScope, SourceBatch, SourceEvent
from elephant.memory.openviking_config import OpenVikingConfig
from elephant.storage.db import Database

REPETITIONS = 3
KEY_ENV = "ELEPHANT_BENCH_MEMORY_KEY"
QUERY = "synthetic-memory-fact"
CONTENT = QUERY + " is recorded from the original benchmark user source."


class SyntheticViking:
    def __init__(self, delay_ms):
        self.delay = delay_ms / 1000
        self.sessions, self.tasks, self.files = {}, {}, {}
        self.indexed = False
        self.latency, self.trace = {}, []
        self.uri = "viking://user/benchmark/memories/entities/fact.md"

    async def __call__(self, request):
        start = time.perf_counter()
        await asyncio.sleep(self.delay)
        path, method = request.url.path, request.method
        body = json.loads(request.content) if request.content else None
        assert request.headers["X-API-Key"] == "synthetic-memory-key"
        assert request.headers["X-OpenViking-User"] == "benchmark"
        if path == "/health":
            phase = "health"
            result = {"status": "ok", "version": "v0.5.4.1", "auth_mode": "api_key", "role": "user",
                      "account_id": "benchmark", "user_id": "benchmark"}
        elif path == "/api/v1/sessions" and method == "POST":
            phase = "session_create"
            assert body["memory_policy"]["peer"]["enabled"] is False
            assert body["memory_policy"]["working_memory"]["enabled"] is False
            self.sessions[body["session_id"]] = []
            result = {"session_id": body["session_id"]}
        elif path.endswith("/messages/batch") and method == "POST":
            phase = "source_append"
            self.sessions[path.split("/")[-3]].extend(body["messages"])
            result = {"added": len(body["messages"])}
        elif path.endswith("/commit") and method == "POST":
            phase = "extraction_submit"
            sid = path.split("/")[-2]
            source, = self.sessions[sid]
            assert source["content"] == CONTENT
            archive = "viking://user/benchmark/sessions/" + sid + "/history/archive_001"
            self.files[self.uri] = source["content"]
            diff_uri = archive + "/memory_diff.json"
            self.files[diff_uri] = json.dumps({"archive_uri": archive, "operations": {
                "adds": [{"uri": self.uri, "memory_type": "entities", "after": source["content"]}],
                "updates": [], "deletes": []}, "skipped_operations": []})
            task_id = "extract-" + sid
            self.tasks[task_id] = {"task_id": task_id, "task_type": "session_commit", "resource_id": sid,
                "status": "completed", "result": {"archive_uri": archive, "memory_diff_uri": diff_uri,
                    "effective_memory_types": list(MEMORY_TYPES), "session_skills_extracted": 0}}
            result = {"status": "accepted", "task_id": task_id, "archive_uri": archive}
        elif path == "/api/v1/content/reindex" and method == "POST":
            phase = "index_submit"
            assert body["uri"] == self.uri and body["mode"] == "vectors_only" and body["wait"] is False
            self.tasks["index-fact"] = {"task_id": "index-fact", "resource_id": self.uri,
                "status": "completed", "result": {"status": "completed", "failed_records": 0}}
            result = {"status": "accepted", "task_id": "index-fact"}
        elif path.startswith("/api/v1/tasks/") and method == "GET":
            task_id = path.rsplit("/", 1)[-1]
            phase = "index_poll" if task_id == "index-fact" else "extraction_poll"
            if task_id == "index-fact":
                self.indexed = True
            result = self.tasks[task_id]
        elif path == "/api/v1/content/read" and method == "GET":
            uri = request.url.params["uri"]
            phase = "diff_read" if uri.endswith("/memory_diff.json") else "document_read"
            result = self.files[uri]
        elif path == "/api/v1/search/find" and method == "POST":
            phase = "search"
            assert body["target_uri"] == "viking://user/benchmark/memories/"
            result = {"memories": [{"uri": self.uri, "score": 1.0}]
                if self.indexed and body["query"] in self.files.get(self.uri, "") else []}
        else:
            raise AssertionError("unexpected synthetic OpenViking endpoint")
        elapsed = (time.perf_counter() - start) * 1000
        self.latency.setdefault(phase, []).append(elapsed)
        self.trace.append({"phase": phase, "method": method, "elapsed_ms": round(elapsed, 4)})
        return httpx.Response(200, json=result if path == "/health" else {"status": "ok", "result": result})


async def run_once(directory, delay_ms, samples):
    latency, failures, remote = {}, [], SyntheticViking(delay_ms)
    scope = MemoryScope(owner_id="benchmark")
    binding = MemoryBinding(owner_id=scope.owner_id, account_id="benchmark", user_id="benchmark", api_key_env=KEY_ENV)
    config = OpenVikingConfig(endpoint="http://127.0.0.1:1933", bindings=(binding,))
    database = directory / "core.db"
    migrate(str(database))
    db, clock = Database(str(database)), Clock()
    active_phase = "initialization"

    async def timed(phase, call):
        nonlocal active_phase
        active_phase = phase
        start = time.perf_counter()
        try:
            return await call
        finally:
            latency.setdefault(phase, []).append((time.perf_counter() - start) * 1000)

    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(remote), trust_env=False) as client:
            service = OpenVikingMemory(db, clock, config, client=client)
            before = await service.search(scope, QUERY)
            assert before.status == "ready" and not before.hits
            source = SourceEvent(id="benchmark-original", seq=1, role="user", content=CONTENT)
            batch = SourceBatch(scope=scope, session_id="benchmark-source", from_seq=0, to_seq=1, events=(source,))
            sync_start = time.perf_counter()
            receipt = await timed("sync_accept", service.sync(batch))
            assert receipt.accepted and receipt.state == "ACCEPTED" and not receipt.extracted and not receipt.indexed
            receipt = await timed("sync_extract", service.poll(receipt.operation_id))
            assert receipt.extracted and not receipt.indexed
            before_index = await service.search(scope, QUERY)
            assert before_index.status == "ready" and not before_index.hits
            active_phase = "sync_index_ready"
            index_start = time.perf_counter()
            for _ in range(4):
                receipt = await service.poll(receipt.operation_id)
                if receipt.indexed:
                    break
            latency["sync_index_ready"] = [(time.perf_counter() - index_start) * 1000]
            assert receipt.state == "INDEXED" and receipt.indexed_seq == receipt.extracted_seq == receipt.exported_seq == 1
            latency["sync_to_indexed"] = [(time.perf_counter() - sync_start) * 1000]
            status = await service.status(scope)
            assert status["pending"] == 0 and status["state"] == "READY"
            for _ in range(samples):
                start = time.perf_counter()
                found = await timed("recall_search", service.search(scope, QUERY))
                hit, = found.hits
                assert found.status == "ready" and hit.uri == remote.uri
                document = await timed("recall_read", service.read(scope, hit.uri))
                assert document.content == CONTENT and document.source_event_ids == (source.id,)
                latency.setdefault("recall_total", []).append((time.perf_counter() - start) * 1000)
            replay = await service.sync(batch)
            assert replay.operation_id == receipt.operation_id and replay.indexed
            assert sum(item["phase"] == "source_append" for item in remote.trace) == 1
            assert sum(item["phase"] == "extraction_submit" for item in remote.trace) == 1
            assert not (await service.search(scope, "synthetic-absent-fact")).hits
            await service.close()
    except Exception as error:  # noqa: BLE001 -- preserve failed repetitions in the JSON report.
        failures.append({"phase": active_phase, "error_type": type(error).__name__})
    finally:
        db.close()
    return {"latency_samples_ms": latency, "http_samples_ms": remote.latency,
            "http_trace": remote.trace, "failures": failures}


async def benchmark(args):
    results = []
    with patch.dict(os.environ, {KEY_ENV: "synthetic-memory-key"}), tempfile.TemporaryDirectory(prefix="elephant-memory-") as name:
        directory = Path(name)
        for index in range(REPETITIONS):
            run = directory / str(index)
            run.mkdir()
            results.append(await run_once(run, args.delay_ms, args.samples))
    aggregate, http = {}, {}
    for result in results:
        for source, target in ((result["latency_samples_ms"], aggregate), (result["http_samples_ms"], http)):
            for phase, values in source.items():
                target.setdefault(phase, []).extend(values)
    commit, dirty = revision()
    failures = sum(len(result["failures"]) for result in results)
    return {"report_version": 1, "scope": "real OpenVikingMemory/SQLite with synthetic HTTP extraction, index and keyword search",
        "recorded_at": datetime.now(UTC).isoformat(), "git_commit": commit, "git_dirty": dirty,
        "environment": {"machine": platform.platform(), "python": platform.python_version(),
            "sqlite": sqlite3.sqlite_version, "httpx": httpx.__version__, "cpu_count": os.cpu_count()},
        "conditions": {"repetitions": REPETITIONS, "recall_samples_per_repetition": args.samples,
            "fixed_http_delay_ms": args.delay_ms, "concurrency": 1, "fresh_database_per_repetition": True,
            "external_network_calls": 0, "model_calls": 0, "warmup_samples": 0,
            "visibility_probes_per_repetition": 3, "sync_to_indexed_includes_preindex_visibility_probe": True,
            "search": "query matched against indexed synthetic documents derived from appended original source",
            "limits": "controlled protocol timings only; synthetic extraction, indexing and keyword search"},
        "latency_ms": {phase: percentiles(values) for phase, values in aggregate.items()},
        "http_latency_ms": {phase: percentiles(values) for phase, values in http.items()},
        "failure_count": failures, "passed": failures == 0,
        "runs": [{"repetition": index + 1,
            "latency_ms": {phase: percentiles(values) for phase, values in result["latency_samples_ms"].items()},
            "http_requests": result["http_trace"], "failures": result["failures"]}
            for index, result in enumerate(results)]}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=positive_int, default=10)
    parser.add_argument("--delay-ms", type=positive_float, default=5)
    parser.add_argument("--output", type=Path, help="write a new JSON report exclusively")
    args = parser.parse_args(argv)
    if args.output and (args.output.exists() or args.output.is_symlink()):
        parser.error("output already exists")
    if args.output and not args.output.parent.is_dir():
        parser.error("output parent directory does not exist")
    report = asyncio.run(benchmark(args))
    serialized = json.dumps(report, indent=2)
    if args.output:
        with args.output.open("x", encoding="utf-8") as output:
            output.write(serialized + "\n")
    print(serialized)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
