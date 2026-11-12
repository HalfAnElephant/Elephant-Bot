"""Behavior checks use the locked real pi with a deterministic local model transport."""
import json
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import select

from qqbot.domain.execution import ExecutionPlan, TaskGoal
from qqbot.domain.messages import (
    AccessContext,
    ConversationRef,
    InboundEvent,
    MediaSegment,
    PrincipalRef,
    Surface,
)
from qqbot.harness.pool import SessionPool
from qqbot.jobs.api import internal_router
from qqbot.jobs.service import TaskService
from qqbot.jobs.worker import GeneralRun, Spool, Worker
from qqbot.outbox.service import Outbox
from qqbot.providers import ModelCapacity
from qqbot.storage.schema import outbox
from qqbot.storage.task_schema import inputs, notifications, processes, runs, tasks, waits


@pytest.fixture
def execution(db, clock):
    service = TaskService(db, clock, Outbox(db, clock))
    conversation = ConversationRef(deployment_id="d", adapter_instance_id="a", bot_identity_id="b",
        surface=Surface.GROUP, remote_conversation_id="g")
    principal = PrincipalRef(adapter_instance_id="a", identity_namespace="qq", remote_user_id="u")
    event = InboundEvent(event_id="initial", event_type="message.created", conversation=conversation,
        principal=principal, platform_message_id="m", occurred_at=clock.now(), received_at=clock.now(), trace_id="t")
    return service, event, AccessContext(principal=principal, conversation=conversation)


def test_no_file_result_detach_and_protocol(execution, clock):
    service, event, access = execution
    task = service.accept(event, access, "hello")
    assert service.claim("old", protocol_version=1) is None
    clock.advance(6)
    service.detach_due()
    with service.db.engine.connect() as c:
        assert c.execute(select(runs)).first() is None
        assert c.execute(select(tasks.c.delivery)).scalar_one() == "DETACHED"
    claim = service.claim("new")
    service.submit(claim["run_id"], claim["fencing_token"], "new", {"status": "completed", "summary": "Hello"})
    assert service.get(task["public_id"], access)["state"] == "SUCCEEDED"
    with service.db.engine.connect() as c:
        messages = c.execute(select(outbox.c.envelope_json)).scalars().all()
        assert len(messages) == 1 and json.loads(messages[0])["text"] == "Hello"


def test_requested_output_still_required(execution):
    service, event, access = execution
    service.accept(event, access, TaskGoal(text="make a file", requested_outputs=("outputs/result.txt",)))
    claim = service.claim("w")
    with pytest.raises(ValueError, match="requested outputs"):
        service.submit(claim["run_id"], 1, "w", {"status": "completed", "summary": "done"})


def test_question_fast_reply_is_consumed_after_quiescence_once(execution):
    service, event, access = execution
    task = service.accept(event, access, "need input")
    claim = service.claim("w")
    identity = (claim["run_id"], 1, "w")
    answer = event.model_copy(update={"event_id": "answer"})
    service.ask(*identity, {"question_id": "q", "text": "Which repository?", "checkpoint": "No changes made"})
    first = service.add_input(task["public_id"], answer, access, "owner/repo", "q")
    assert service.add_input(task["public_id"], answer, access, "owner/repo", "q") == first
    assert service.get(task["public_id"], access)["state"] == "RUNNING"
    assert service.take_inputs(*identity) == []
    result = {"status": "waiting_input", "summary": "Question queued"}
    service.submit(*identity, result)
    assert service.get(task["public_id"], access)["state"] == "QUEUED"
    continuation = service.claim("w")
    assert continuation["task_id"] == task["id"] and continuation["run_id"] != claim["run_id"]
    assert continuation["continuation"]["inputs"] == [{"id": first["input_id"], "text": "owner/repo"}]
    assert service.claim("w") is None
    with service.db.engine.connect() as c:
        assert c.execute(select(waits.c.state)).scalar_one() == "CONSUMED"
        assert c.execute(select(inputs.c.state)).scalar_one() == "READY"
    service.session_receipt(continuation["run_id"], continuation["fencing_token"], "w", {
        "session_id": continuation["session_id"], "run_id": continuation["run_id"],
        "session_revision": "native-revision", "kind": "input_consumed", "input_ids": [first["input_id"]]})
    with service.db.engine.connect() as c:
        assert c.execute(select(inputs.c.state)).scalar_one() == "CONSUMED"
        assert c.execute(select(notifications.c.purpose)).scalars().all() == ["question"]


@pytest.mark.parametrize("question_reply", [False, True])
def test_image_only_task_input_is_persisted_and_delivered_once(execution, question_reply):
    from qqbot.ingress.service import Inbox

    service, event, access = execution
    task = service.accept(event, access, "inspect image", context={"source_manifest": []})
    claim = service.claim("w")
    identity = (claim["run_id"], 1, "w")
    if question_reply:
        service.ask(*identity, {"question_id": "q", "text": "Send the image", "checkpoint": "Need image"})
    answer = event.model_copy(update={"event_id": "image-answer", "platform_message_id": "image-message",
        "segments": (MediaSegment(type="image", attachment_ref="unresolved:fixture",
                                  image_url="https://gchat.qpic.cn/image"),)})
    stored, _ = Inbox(service.db).accept(answer)
    first = service.add_input(task["public_id"], stored, access, "", "q" if question_reply else None)
    assert service.add_input(task["public_id"], stored, access, "", "q" if question_reply else None) == first
    expected_image = {"source": stored.event_id, "message_id": stored.platform_message_id,
                      "url": "https://gchat.qpic.cn/image"}
    with service.db.engine.connect() as c:
        context = json.loads(c.execute(select(tasks.c.context_json)).scalar_one())
    assert context["images"] == [expected_image]
    assert len(context["source_manifest"]) == 1
    if question_reply:
        assert service.take_inputs(*identity) == []
        service.submit(*identity, {"status": "waiting_input", "summary": "Question queued"})
        continuation = service.claim("w")
        assert continuation["context"]["images"] == [expected_image]
        assert continuation["continuation"]["inputs"] == [{"id": first["input_id"], "text": "[图片]"}]
    else:
        assert service.take_inputs(*identity) == [{"id": first["input_id"], "text": "[图片]",
                                                  "images": [expected_image]}]


def test_task_input_images_use_persisted_source_and_text_only_shape(execution):
    from qqbot.ingress.service import Inbox

    service, event, access = execution
    task = service.accept(event, access, "inspect image", context={"source_manifest": []})
    claim = service.claim("w")
    answer = event.model_copy(update={"event_id": "answer"})
    Inbox(service.db).accept(answer)
    forged = answer.model_copy(update={"segments": (MediaSegment(type="image", attachment_ref="unresolved:fake",
                                                               image_url="https://example.com/forged"),)})
    accepted = service.add_input(task["public_id"], forged, access, "text reply")
    assert service.take_inputs(claim["run_id"], 1, "w") == [{"id": accepted["input_id"], "text": "text reply"}]
    with service.db.engine.connect() as c:
        assert "images" not in json.loads(c.execute(select(tasks.c.context_json)).scalar_one())
    with pytest.raises(ValueError, match="conflicting input replay"):
        service.add_input(task["public_id"], forged, access, "different text")


def sse(message, usage=None):
    calls = message.get("tool_calls")
    if calls:
        message = {**message, "tool_calls": [{"index": n, **call} for n, call in enumerate(calls)]}
    event = {"id": "fake", "object": "chat.completion.chunk", "model": "offline", "created": 1,
             "choices": [{"index": 0, "delta": message, "finish_reason": None}]}
    end = {**event, "choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls" if calls else "stop"}],
           "usage": usage or {"prompt_tokens": 300, "completion_tokens": 10}}
    return "data: " + json.dumps(event) + "\n\ndata: " + json.dumps(end) + "\n\ndata: [DONE]\n\n"


@pytest.mark.asyncio
async def test_locked_pi_separate_tasks_keep_isolated_sessions(execution, tmp_path):
    service, event, access = execution
    pi = Path("node_modules/.bin/pi").resolve()
    if not pi.exists():
        pytest.skip("locked pi package is not installed")
    requests = []
    def upstream(request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, headers={"content-type": "text/event-stream"},
            text=sse({"role": "assistant", "content": "Hello from pi"}))
    app = FastAPI()
    app.include_router(internal_router(service, "t" * 32))
    args = SimpleNamespace(workspace=str(tmp_path / "workers"), archive=str(tmp_path / "artifacts"),
        pi=str(pi), model="offline", model_endpoint="https://model.invalid/v1/chat/completions", worker_id="w")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://core",
        headers={"Authorization": "Bearer " + "t" * 32}) as client:
        def factory(claim, process_id, session_id):
            runner = GeneralRun(args, claim, client, process_id, session_id, ModelCapacity())
            runner.gateway.transport = httpx.MockTransport(upstream)
            return runner
        pool = SessionPool(factory)
        worker = Worker(client, "w", Spool(tmp_path / "spool"), None, pool=pool)
        try:
            first = service.accept(event, access, "hello", context={"key": "same-topic", "seed": "bounded background"})
            assert await worker.once()
            assert service.get(first["public_id"], access)["state"] == "SUCCEEDED"
            entry = next(iter(pool.entries.values()))
            pid = entry["runner"].process.pid
            old_token = entry["runner"].gateway.model_token
            assert entry["state"] == "IDLE"
            second = service.accept(event.model_copy(update={"event_id": "second"}), access, "again",
                context={"key": "same-topic", "seed": "bounded background"})
            assert await worker.once()
            assert service.get(second["public_id"], access)["state"] == "SUCCEEDED"
            assert entry["runner"].process.pid == pid
            second_entry = next(item for item in pool.entries.values() if item is not entry)
            assert second_entry["runner"].gateway.model_token != old_token
            assert second_entry["runner"].process.pid != pid
            assert len(requests) == 2
            assert "个人助手大象" in requests[0]["messages"][0]["content"]
            assert "个人助手大象" in requests[1]["messages"][0]["content"]
            assert all("bounded background" not in str(message) for message in requests[1]["messages"])
            with service.db.engine.connect() as c:
                rows = c.execute(select(runs)).mappings().all()
                assert len(rows) == 2 and all(r["quiescent_at"] and r["runner_stopped_at"] is None for r in rows)
                assert c.execute(select(processes.c.state)).scalars().all() == ["IDLE", "IDLE"]
        finally:
            await pool.close()

@pytest.mark.asyncio
async def test_twelve_second_tool_detaches_same_run_and_short_chat_starts(execution, tmp_path, clock):
    import asyncio
    import time

    from qqbot.capabilities.types import Action, Capability
    service, event, access = execution
    pi = Path("node_modules/.bin/pi").resolve()
    if not pi.exists():
        pytest.skip("locked pi package is not installed")
    invocations = []
    started = asyncio.Event()
    async def slow(arguments, context):
        invocations.append((context.task_id, context.run_id))
        started.set()
        await asyncio.sleep(12)
        return {"observed": "slow tool finished once"}
    def upstream(request):
        body = json.loads(request.content)
        current = str(body["messages"])
        if "short unrelated" in current or any(m.get("role") == "tool" for m in body["messages"]):
            message = {"role": "assistant", "content": "Observed result"}
        else:
            message = {"role": "assistant", "content": None, "tool_calls": [{"id": "slow-call", "type": "function",
                "function": {"name": "capability_invoke", "arguments": json.dumps({"name": "test.slow", "arguments": {}})}}]}
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, text=sse(message))
    app = FastAPI()
    app.include_router(internal_router(service, "t" * 32))
    args = SimpleNamespace(workspace=str(tmp_path / "workers"), archive=str(tmp_path / "artifacts"),
        pi=str(pi), model="offline", model_endpoint="https://model.invalid/v1/chat/completions", worker_id="w")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://core",
        headers={"Authorization": "Bearer " + "t" * 32}) as client:
        def factory(claim, process_id, session_id):
            runner = GeneralRun(args, claim, client, process_id, session_id, ModelCapacity())
            runner.gateway.transport = httpx.MockTransport(upstream)
            runner.gateway.registry.register(Capability("test.slow", "Delayed observation", timeout_seconds=15),
                slow, lambda a, c: Action("test.slow.read", ("test:slow",)))
            return runner
        pool = SessionPool(factory)
        worker = Worker(client, "w", Spool(tmp_path / "spool"), None, pool=pool)
        first = service.accept(event, access, "slow request", context={"key": "slow"})
        long_run = asyncio.create_task(worker.once())
        try:
            await asyncio.wait_for(started.wait(), 10)
            original = next(iter(pool.entries.values()))["runner"]
            identity = (original.claim["run_id"], original.process.pid, original.session_id)
            await asyncio.sleep(5.1)
            clock.advance(6)
            service.detach_due()
            with service.db.engine.connect() as c:
                assert c.execute(select(tasks.c.delivery).where(tasks.c.id == first["id"])).scalar_one() == "DETACHED"
            second = service.accept(event.model_copy(update={"event_id": "short"}), access,
                "short unrelated chat", context={"key": "short"})
            before = time.monotonic()
            assert await asyncio.wait_for(worker.once(), 4)
            assert time.monotonic() - before < 4 and not long_run.done()
            assert service.get(second["public_id"], access)["state"] == "SUCCEEDED"
            assert await asyncio.wait_for(long_run, 10)
            assert service.get(first["public_id"], access)["state"] == "SUCCEEDED"
            assert invocations == [(first["id"], identity[0])]
            assert (original.claim["run_id"], original.process.pid, original.session_id) == identity
        finally:
            long_run.cancel()
            await asyncio.gather(long_run, return_exceptions=True)
            await pool.close()


@pytest.mark.asyncio
async def test_external_wait_has_no_model_poll_and_wakes_once(execution):
    from qqbot.capabilities.gateway import CapabilityRegistry
    from qqbot.capabilities.types import Action, Capability
    from qqbot.jobs.waiters import ExternalWaiters
    service, event, access = execution
    task = service.accept(event, access, "wait for CI")
    claim = service.claim("w")
    service.ask(claim["run_id"], 1, "w", {"question_id": "ci", "text": "CI is running", "checkpoint": "Rerun accepted",
        "kind": "external", "condition": {"capability": "ci.read", "arguments": {}, "path": "status", "equals": "completed"}})
    service.submit(claim["run_id"], 1, "w", {"status": "waiting_external", "summary": "Waiting"})
    calls = []
    async def read(arguments, context):
        calls.append(context.task_id)
        return {"status": "completed", "conclusion": "success"}
    registry = CapabilityRegistry()
    registry.register(Capability("ci.read", "fixture CI"), read, lambda a,c: Action("github.run.read", ("repo:github.com/o/r",)))
    async def call(operation, payload):
        return {"waiters": service.external_waiters, "wait-authorize": lambda: service.authorize_waiter(payload),
            "wait-observed": lambda: service.observe_waiter(payload)}[operation]()
    observer = ExternalWaiters(call, registry)
    await observer.once()
    await observer.once()
    assert calls == [task["id"]]
    continuation = service.claim("w")
    assert continuation["task_id"] == task["id"]
    assert "success" in continuation["continuation"]["inputs"][0]["text"]


def test_question_timeout_starts_after_delivery(execution, clock):
    from qqbot.domain.messages import SendReceipt
    service, event, access = execution
    task = service.accept(event, access, "ask")
    claim = service.claim("w")
    service.ask(claim["run_id"], 1, "w", {"question_id": "q", "text": "Which one?", "checkpoint": "Need a choice"})
    service.submit(claim["run_id"], 1, "w", {"status": "waiting_input", "summary": "waiting"})
    clock.advance(3601)
    service.recover()
    assert service.get(task["public_id"], access)["state"] == "WAITING_INPUT"
    with service.db.engine.connect() as c:
        delivery = c.execute(select(outbox.c.id)).scalar_one()
        assert c.execute(select(waits.c.expires_at)).scalar_one() is None
    service.outbox._receipt(delivery, SendReceipt(state="SENT", platform_message_id="question-platform"))
    with service.db.engine.connect() as c:
        assert c.execute(select(waits.c.delivered_at)).scalar_one()


def test_workspace_claim_exclusion_and_pending_input_continuation(execution, clock):
    service, event, access = execution
    plan = ExecutionPlan(workspace_ref="repo")
    first = service.accept(event, access, "work", plan=plan)
    clock.advance(1)
    second = service.accept(event.model_copy(update={"event_id": "second"}), access, "work again", plan=plan)
    claim = service.claim("w")
    assert claim["task_id"] == first["id"]
    assert service.claim("other") is None
    extra = event.model_copy(update={"event_id": "late"})
    service.add_input(first["public_id"], extra, access, "also check tests")
    service.submit(claim["run_id"], 1, "w", {"status": "completed", "summary": "first pass"})
    assert service.get(first["public_id"], access)["state"] == "QUEUED"
    next_claim = service.claim("w")
    assert next_claim["task_id"] in {first["id"], second["id"]}
    with service.db.engine.connect() as c:
        assert c.execute(select(inputs.c.state)).scalar_one() in {"READY", "CONSUMED"}
        assert c.execute(select(notifications)).first() is None


async def test_reconciled_write_is_unknown_and_retry_uses_original_ledger(execution, clock):
    from qqbot.storage.action_schema import operations
    service, event, access = execution
    task = service.accept(event, access, "write")
    claim = service.claim("w")
    prepare = {"operation": "prepare", "operation_id": "op", "arguments_hash": "same",
        "action": {"name": "github.issue.write", "resources": ["repo:github.com/o/r"], "effect": "write"},
        "arguments": {"argv": ["issue", "create"]}}
    await service.ledger(claim["run_id"], 1, "w", prepare)
    await service.ledger(claim["run_id"], 1, "w", {**prepare, "operation_id": "prepared", "arguments_hash": "other"})
    await service.ledger(claim["run_id"], 1, "w", {"operation": "transition", "operation_id": "op",
        "expected": "PREPARED", "state": "DISPATCHED", "result": {}})
    clock.advance(31)
    service.recover()
    service.stop_verifier = lambda *_: True
    service.reconcile(task["public_id"], access, claim["run_id"])
    with service.db.engine.connect() as c:
        assert dict(c.execute(select(operations.c.id, operations.c.state)).all()) == {"op": "UNKNOWN", "prepared": "FAILED"}
    service.retry(task["public_id"], event.model_copy(update={"event_id": "retry"}), access)
    next_claim = service.claim("next")
    duplicate = await service.ledger(next_claim["run_id"], 1, "next", {**prepare, "operation_id": "other"})
    assert duplicate["operation_id"] == "op" and duplicate["state"] == "UNKNOWN"


async def test_malformed_external_observation_does_not_kill_other_waiters():
    from qqbot.capabilities.gateway import CapabilityRegistry
    from qqbot.capabilities.types import Action, Capability
    from qqbot.jobs.waiters import ExternalWaiters
    registry, observed = CapabilityRegistry(), []
    registry.register(Capability("ci", "CI"), lambda a, c: {"jobs": []},
        lambda a, c: Action("github.run.read", ("repo:github.com/o/r",)))
    async def call(operation, payload):
        if operation == "waiters":
            return [{"id": name, "task_id": name, "revision": 1, "principal": {},
                "condition": {"capability": "ci", "arguments": {}, "path": path, "equals": []}}
                for name, path in [("bad", "jobs.0.status"), ("good", "jobs")]]
        if operation == "wait-observed":
            observed.append(payload["wait_id"])
        return {"allowed": True}
    await ExternalWaiters(call, registry).once()
    assert observed == ["good"]


@pytest.mark.parametrize("injection", ["active", "settled"])
async def test_locked_pi_late_input_survives_settled_boundary(execution, tmp_path, injection):
    import asyncio
    service, event, access = execution
    pi = Path("node_modules/.bin/pi").resolve()
    if not pi.exists():
        pytest.skip("locked pi package is not installed")
    requests, checked, release = [], asyncio.Event(), asyncio.Event()
    async def upstream(request):
        requests.append(json.loads(request.content))
        if injection == "active" and len(requests) == 1:
            checked.set()
            await release.wait()
        return httpx.Response(200, headers={"content-type": "text/event-stream"},
            text=sse({"role": "assistant", "content": "Checked"}))
    app = FastAPI()
    app.include_router(internal_router(service, "t" * 32))
    args = SimpleNamespace(workspace=str(tmp_path / "workers"), archive=str(tmp_path / "artifacts"),
        pi=str(pi), model="offline", model_endpoint="https://model.invalid/v1/chat/completions", worker_id="w")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://core",
        headers={"Authorization": "Bearer " + "t" * 32}) as client:
        def factory(claim, process_id, session_id):
            runner = GeneralRun(args, claim, client, process_id, session_id, ModelCapacity())
            runner.gateway.transport = httpx.MockTransport(upstream)
            original = runner.rpc
            async def gated_rpc(kind, **kwargs):
                value = await original(kind, **kwargs)
                if injection == "settled" and kind == "get_state" and not checked.is_set():
                    checked.set()
                    await release.wait()
                return value
            runner.rpc = gated_rpc
            return runner
        pool = SessionPool(factory)
        worker = Worker(client, "w", Spool(tmp_path / "spool"), None, pool=pool)
        task = service.accept(event, access, "check", context={"key": "topic"})
        running = asyncio.create_task(worker.once())
        try:
            await asyncio.wait_for(checked.wait(), 10)
            runner = next(iter(pool.entries.values()))["runner"]
            extra = service.add_input(task["public_id"], event.model_copy(update={"event_id": "late"}), access, "and check the second item")
            if injection == "settled":
                with pytest.raises(RuntimeError, match="not accepting"):
                    await runner.append_input(extra["input_id"], "and check the second item")
            else:
                await runner.append_input(extra["input_id"], "and check the second item")
            release.set()
            assert await running
            if injection == "settled":
                assert service.get(task["public_id"], access)["state"] == "QUEUED"
                assert await worker.once()
            assert service.get(task["public_id"], access)["state"] == "SUCCEEDED"
            assert len(requests) == 2
            assert "and check the second item" in json.dumps(requests[1], ensure_ascii=False)
            with service.db.engine.connect() as c:
                assert c.execute(select(inputs.c.state)).scalar_one() == "CONSUMED"
                identity = json.loads(c.execute(select(processes.c.identity_json)).scalar_one())
                assert identity["actual_session_id"] and identity["actual_session_file"]
        finally:
            release.set()
            if not running.done():
                running.cancel()
                await asyncio.gather(running, return_exceptions=True)
            await pool.close()


def test_root_pi_marker_does_not_prove_host_tools_stopped(execution, clock):
    service, event, access = execution
    process_id, session_id = str(uuid4()), str(uuid4())
    task = service.accept(event, access, "write")
    claim = service.claim("w")
    service.bind_process(claim["run_id"], 1, "w", {"process_id": process_id, "session_id": session_id,
        "epoch": 1, "generation": 1, "scope_key": "topic", "session_file": "session.jsonl", "identity": {}})
    clock.advance(31)
    service.recover()
    service.stop_verifier = lambda *_: True
    with pytest.raises(ValueError, match="host tools"):
        service.reconcile(task["public_id"], access, claim["run_id"])
    with service.db.engine.connect() as c:
        assert c.execute(select(runs.c.slot_reserved)).scalar_one() == 1
    service.process_stopped("w", {"process_id": process_id, "epoch": 1, "stopped": True})
    assert service.reconcile(task["public_id"], access, claim["run_id"])["state"] == "FAILED"


def test_final_supersedes_only_normal_progress(execution):
    service, event, access = execution
    service.accept(event, access, "work")
    claim = service.claim("w")
    for identity, importance in [("normal", "normal"), ("receipt", "important")]:
        service.notify(claim["run_id"], 1, "w", {"notification_id": identity, "text": identity, "importance": importance})
    service.submit(claim["run_id"], 1, "w", {"status": "completed", "summary": "done"})
    with service.db.engine.connect() as c:
        states = dict(c.execute(select(outbox.c.origin_id, outbox.c.state)).all())
    assert states["normal"] == "SUPERSEDED"
    assert states["receipt"] == "PENDING"
    assert states["result:" + claim["run_id"]] == "PENDING"


async def test_pool_host_limits_disable_reuse_and_failed_warm_binding_starts_cold():
    created = []
    class Runner:
        quiescent_verified = True
        stopped_verified = False
        fail_binding = False
        async def bind(self, claim):
            if self.fail_binding:
                raise RuntimeError("stale warm binding")
        async def stop(self):
            self.stopped_verified = True
            return True
    def factory(*args):
        runner = Runner()
        created.append(runner)
        return runner
    pool = SessionPool(factory)
    claim = {"task_id": "task", "context": {"key": "topic", "pool": {"max_resident": 3, "max_idle": 1, "ttl": 123}}}
    first = await pool.acquire(claim)
    await pool.release(first, True)
    first.fail_binding = True
    second = await pool.acquire(claim)
    assert second is not first and first.stopped_verified
    assert pool.max_resident == 3 and pool.ttl == 123
    await pool.release(second, True)
    cold = {**claim, "context": {"key": "topic", "pool": {"max_resident": 3, "max_idle": 0, "ttl": 123}}}
    third = await pool.acquire(cold)
    assert third is not second and second.stopped_verified
    await pool.release(third, True)
    assert third.stopped_verified and not pool.entries


async def test_worker_exports_requested_binary_without_text_write_tool(execution, tmp_path):
    from qqbot.domain.tasks import TaskResult
    service, event, access = execution
    workspace = tmp_path / "work"
    (workspace / "outputs").mkdir(parents=True)
    document = b"%PDF-1.7\nfixture\x00\xff\n%%EOF"
    (workspace / "outputs" / "result.pdf").write_bytes(document)
    service.archive_root.mkdir()
    app = FastAPI()
    app.include_router(internal_router(service, "t" * 32))
    class Harness:
        stopped_verified = True
        gateway = SimpleNamespace(workspace=workspace, artifacts=[], evidence=[])
        archive = service.archive_root
        async def run(self, goal):
            return TaskResult(status="completed", summary="文件已生成。")
        async def stop(self):
            return True
    task = service.accept(event, access, TaskGoal(text="make PDF", requested_outputs=("outputs/result.pdf",)))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://core",
        headers={"Authorization": "Bearer " + "t" * 32}) as client:
        worker = Worker(client, "w", Spool(tmp_path / "spool"), lambda claim: Harness())
        assert await worker.once()
    result = json.loads(service.get(task["public_id"], access)["result_json"])
    artifact = result["artifacts"][0]
    assert artifact["mime_type"] == "application/pdf"
    assert (service.archive_root / artifact["id"]).read_bytes() == document


def test_continuation_keeps_task_budget_and_completion_reserve(execution):
    from qqbot.domain.execution import RunBudget
    service, event, access = execution
    task = service.accept(event, access, "wait", plan=ExecutionPlan(budget=RunBudget(model_calls=2)))
    claim = service.claim("w")
    service.reserve_budget(claim["run_id"], 1, "w", {"request_id": "first", "kind": "business"})
    service.ask(claim["run_id"], 1, "w", {"question_id": "q", "text": "Which target?", "checkpoint": "No writes"})
    service.add_input(task["public_id"], event.model_copy(update={"event_id": "reply"}), access, "the original", "q")
    service.submit(claim["run_id"], 1, "w", {"status": "waiting_input", "summary": "Waiting"})
    continuation = service.claim("w")
    with pytest.raises(ValueError, match="budget exhausted"):
        service.reserve_budget(continuation["run_id"], 2, "w", {"request_id": "second", "kind": "business"})
    service.reserve_budget(continuation["run_id"], 2, "w", {"request_id": "close", "kind": "completion"})
    with pytest.raises(ValueError, match="budget exhausted"):
        service.reserve_budget(continuation["run_id"], 2, "w", {"request_id": "extra", "kind": "completion"})
    with service.db.engine.connect() as c:
        assert json.loads(c.execute(select(tasks.c.budget_json)).scalar_one())["model_calls"] == 2


def test_unknown_question_delivery_never_starts_user_timeout(execution, clock):
    from qqbot.domain.messages import SendReceipt
    service, event, access = execution
    task = service.accept(event, access, "question")
    claim = service.claim("w")
    service.ask(claim["run_id"], 1, "w", {"question_id": "q", "text": "Which target?", "checkpoint": "No writes"})
    service.submit(claim["run_id"], 1, "w", {"status": "waiting_input", "summary": "Waiting"})
    with service.db.engine.connect() as c:
        delivery_id = c.execute(select(outbox.c.id)).scalar_one()
    service.outbox._receipt(delivery_id, SendReceipt(state="UNKNOWN", error_code="connection_lost"))
    clock.advance(1900)
    service.recover()
    assert service.get(task["public_id"], access)["state"] == "WAITING_INPUT"
    with service.db.engine.connect() as c:
        assert c.execute(select(waits.c.expires_at)).scalar_one() is None
    clock.advance(6000)
    service.recover()
    result = service.get(task["public_id"], access)
    assert result["state"] == "BLOCKED" and result["stop_reason"] == "delivery_timeout"
