"""OpenViking owns facts; the host persists sources, receipts and deletion fences."""
import asyncio
import json

import httpx
import pytest
from pydantic import ValidationError
from sqlalchemy import select

from qqbot.adapters.openviking import OpenVikingMemory
from qqbot.adapters.openviking.service import MEMORY_TYPES, digest
from qqbot.adapters.openviking.transport import MemoryBackendError
from qqbot.contracts.memory import (
    MemoryBinding,
    MemoryCorrection,
    MemoryDeletion,
    MemoryScope,
    SourceBatch,
    SourceEvent,
)
from qqbot.memory.openviking_config import OpenVikingConfig
from qqbot.storage.memory_sync_schema import (
    memory_documents,
    memory_operations,
)


class Viking:
    """Pinned HTTP schemas, including native non-idempotent append and async commit."""
    def __init__(self):
        self.calls, self.sessions, self.tasks, self.files, self.diffs = [], {}, {}, {}, {}
        self.offline = False
        self.lost_append = self.lost_commit = False
        self.drop_append = False
        self.role = "user"
        self.index_failed = False
        self.extra_hits = []

    async def __call__(self, request):
        path, method = request.url.path, request.method
        body = json.loads(request.content) if request.content else None
        self.calls.append((method, path, body))
        if self.offline:
            raise httpx.ConnectError("unavailable")
        user = request.headers["X-OpenViking-User"]
        account = request.headers["X-OpenViking-Account"]
        root = f"viking://user/{user}"
        result = {}
        if path == "/health":
            return httpx.Response(200, json={"status": "ok", "healthy": True, "auth_mode": "api_key",
                "version": "v0.5.4.1", "role": self.role, "account_id": account, "user_id": user})
        if path == "/api/v1/sessions" and method == "POST":
            sid = body["session_id"]
            self.sessions.setdefault(sid, {"messages": [], "config": body, "user": user})
            result = {"session_id": sid}
        elif path.endswith("/messages/batch"):
            sid = path.split("/")[-3]
            if not self.drop_append:
                self.sessions[sid]["messages"].extend({
                    "role": message["role"], "turn_id": message["turn_id"],
                    "source_message_ids": message["source_message_ids"],
                    "parts": [{"type": "text", "text": message["content"]}],
                } for message in body["messages"])
            if self.lost_append or self.drop_append:
                self.lost_append = False
                raise httpx.ReadTimeout("append receipt lost")
            result = {"message_count": len(self.sessions[sid]["messages"]), "added": len(body["messages"])}
        elif path.endswith("/commit"):
            sid = path.split("/")[-2]
            task_id = "commit-" + sid
            archive = root + "/sessions/" + sid + "/history/archive_001"
            self.tasks[task_id] = {"task_id": task_id, "task_type": "session_commit",
                "resource_id": sid, "status": "running", "result": None}
            self.sessions[sid]["archive"] = archive
            if self.lost_commit:
                self.lost_commit = False
                raise httpx.ReadTimeout("commit receipt lost")
            result = {"status": "accepted", "task_id": task_id, "archive_uri": archive, "archived": True}
        elif path == "/api/v1/tasks":
            result = [task for task in self.tasks.values() if all(
                task.get(name) == request.url.params[name]
                for name in ("resource_id", "task_type", "status") if name in request.url.params)]
        elif path.startswith("/api/v1/tasks/"):
            result = self.tasks[path.rsplit("/", 1)[-1]]
        elif path == "/api/v1/content/read":
            uri = request.url.params["uri"]
            if uri in self.files:
                result = self.files[uri]
                if request.url.params.get("raw") != "true":
                    result = result.split("\n\n<!-- MEMORY_FIELDS", 1)[0]
            elif uri in self.diffs:
                result = json.dumps(self.diffs[uri])
            elif uri.endswith("/messages.jsonl"):
                sid = uri.rsplit("/", 2)[-2]
                if sid not in self.sessions:
                    return httpx.Response(404, json={"status": "error"})
                result = "\n".join(json.dumps(message) for message in self.sessions[sid]["messages"])
            else:
                return httpx.Response(404, json={"status": "error"})
        elif path == "/api/v1/content/reindex":
            completed = {"status": "completed", "failed_records": 1 if self.index_failed else 0}
            if body["wait"]:
                result = completed
            else:
                task_id = "index-" + digest(body["uri"])[:16]
                self.tasks[task_id] = {"task_id": task_id, "task_type": "reindex",
                    "resource_id": body["uri"], "status": "completed", "result": completed}
                result = {"status": "accepted", "task_id": task_id}
        elif path == "/api/v1/content/batch-write":
            assert body["root_uri"].startswith(root + "/memories/")
            assert body["processing_mode"] == "vectors_only"
            for operation in body["operations"]:
                assert operation["uri"].rsplit("/", 1)[0] == body["root_uri"]
                if digest(self.files[operation["uri"]]) != operation["expected_sha256"]:
                    return httpx.Response(409, json={"status": "error"})
                self.files[operation["uri"]] = operation["content"]
            result = {"semantic_status": "completed"}
        elif path == "/api/v1/search/find":
            result = {"memories": [*self.extra_hits, *({"uri": uri, "score": 0.8, "abstract": "untrusted"}
                for uri in self.files if uri.startswith(body["target_uri"]))]}
        elif path == "/api/v1/fs" and method == "DELETE":
            self.files.pop(request.url.params["uri"], None)
        elif path.startswith("/api/v1/sessions/") and method == "DELETE":
            self.sessions.pop(path.rsplit("/", 1)[-1], None)
        else:
            raise AssertionError((method, path, body))
        return httpx.Response(200, json={"status": "ok", "result": result})

    def complete(self, *, sid=None, content="用户偏好用中文回复", relative="preferences/language.md", updates=False):
        sid = sid or next(reversed(self.sessions))
        session = self.sessions[sid]
        uri = f"viking://user/{session['user']}/memories/{relative}"
        self.files[uri] = content + '\n\n<!-- MEMORY_FIELDS\n{"version":1}\n-->'
        diff_uri = session["archive"] + "/memory_diff.json"
        self.diffs[diff_uri] = {"archive_uri": session["archive"], "operations": {
            "updates" if updates else "adds": [{"uri": uri, "memory_type": relative.split("/")[0], "after": content}],
            "deletes": []}, "skipped_operations": []}
        self.tasks["commit-" + sid].update(status="completed", result={"archive_uri": session["archive"],
            "memory_diff_uri": diff_uri, "effective_memory_types": list(MEMORY_TYPES), "session_skills_extracted": 0})
        return uri


@pytest.fixture
async def memory(db, clock, monkeypatch):
    monkeypatch.setenv("TEST_OV_USER_KEY", "fixture-user-key")
    scope = MemoryScope(owner_id="owner")
    binding = MemoryBinding(owner_id="owner", account_id="elephant", user_id="owner_private", api_key_env="TEST_OV_USER_KEY")
    config = OpenVikingConfig(endpoint="http://127.0.0.1:1933", bindings=(binding,))
    remote = Viking()
    client = httpx.AsyncClient(transport=httpx.MockTransport(remote))
    accepted = {}

    def loader(request_scope, session, start, end, branch, projection):
        for batch in accepted.values():
            if (batch.scope, batch.session_id, batch.branch, batch.projection_version) == (request_scope, session, branch, projection):
                events = tuple(event for event in batch.events if start < event.seq <= end)
                return batch.model_copy(update={"from_seq": start, "to_seq": end, "events": events})
        raise LookupError("original source missing")

    service = OpenVikingMemory(db, clock, config, source_loader=loader, client=client)
    yield service, remote, scope, accepted
    await service.close()
    await client.aclose()


def batch(scope, *, session="leader-a", start=0, event_id="source-1", content="以后请用中文回复", **event_options):
    return SourceBatch(scope=scope, session_id=session, from_seq=start, to_seq=start + 1,
        events=(SourceEvent(id=event_id, seq=start + 1, role="user", content=content, **event_options),))


async def indexed(service, receipt):
    for _ in range(8):
        receipt = await service.poll(receipt.operation_id)
        if receipt.indexed:
            return receipt
    raise AssertionError(receipt)


async def seeded(memory):
    service, remote, scope, originals = memory
    original = batch(scope)
    originals[original.session_id] = original
    receipt = await service.sync(original)
    uri = remote.complete()
    return original, await indexed(service, receipt), uri


async def test_real_contract_separates_export_accept_extract_and_index(memory):
    service, remote, scope, originals = memory
    source = batch(scope)
    originals[source.session_id] = source
    receipt = await service.sync(source)
    assert receipt.accepted and not receipt.extracted and not receipt.indexed
    assert (receipt.exported_seq, receipt.extracted_seq, receipt.indexed_seq) == (1, 0, 0)
    config = next(iter(remote.sessions.values()))["config"]
    assert config["auto_commit_policy"] is None
    assert config["memory_policy"]["memory_types"] == list(MEMORY_TYPES)
    assert config["memory_policy"]["working_memory"] == {"enabled": False}
    uri = remote.complete()
    extracted = await service.poll(receipt.operation_id)
    assert extracted.extracted and not extracted.indexed and extracted.extracted_seq == 1
    done = await indexed(service, extracted)
    assert done.indexed_seq == 1
    recalled = await service.search(scope, "用户的语言偏好")
    assert [hit.uri for hit in recalled.hits] == [uri]
    document = await service.read(scope, uri)
    assert document.content == "用户偏好用中文回复" and document.source_event_ids == ("source-1",)
    assert document.trust == "retrieved_data" and "MEMORY_FIELDS" not in document.content
    # Persistence stores immutable source references/hashes, never a writable copy of fact text.
    with service.db.engine.connect() as conn:
        assert "以后请用中文回复" not in str(conn.execute(select(memory_operations)).all())
        assert "用户偏好用中文回复" not in str(conn.execute(select(memory_documents)).all())
    assert (await service.sync(source)).operation_id == receipt.operation_id
    assert sum(path.endswith("/messages/batch") for _, path, _ in remote.calls) == 1


async def test_three_relays_preserve_origin_and_do_not_reextract(memory):
    service, remote, scope, originals = memory
    await seeded(memory)
    for number in range(3):
        relay = batch(scope, session=f"relay-{number}", event_id=f"relay-{number}", kind="relay",
                      source_event_ids=("source-1",), content="对同一个成果的汇总转述")
        originals[relay.session_id] = relay
        result = await service.sync(relay)
        assert result.state == "SKIPPED" and result.extracted
    assert sum(path.endswith("/commit") for _, path, _ in remote.calls) == 1


async def test_process_restart_uses_durable_source_refs_and_does_not_repeat_worker(memory, clock):
    service, remote, scope, originals = memory
    source = batch(scope)
    originals[source.session_id] = source
    receipt = await service.sync(source)
    uri = remote.complete()
    replacement = OpenVikingMemory(service.db, clock, service.config, source_loader=service.source_loader, client=service.http.client)
    done = await indexed(replacement, receipt)
    assert done.extracted and (await replacement.read(scope, uri)).source_event_ids == ("source-1",)
    assert sum(path.endswith("/commit") for _, path, _ in remote.calls) == 1


@pytest.mark.parametrize("kind", ["summary", "tool_output", "skill_review", "relay"])
async def test_non_original_projections_are_not_long_term_facts(memory, kind):
    service, remote, scope, _ = memory
    receipt = await service.sync(batch(scope, kind=kind))
    assert receipt.state == "SKIPPED" and not remote.calls


async def test_secret_source_is_filtered_without_a_local_fact_fallback(memory):
    service, remote, scope, _ = memory
    receipt = await service.sync(batch(scope, content="password=never-store-this"))
    assert receipt.state == "SKIPPED" and not remote.calls


async def test_offline_backend_leaves_visible_pending_refs_and_recovers_from_harness(memory):
    service, remote, scope, originals = memory
    source = batch(scope)
    originals[source.session_id] = source
    remote.offline = True
    receipt = await service.sync(source)
    assert receipt.state == "QUEUED" and receipt.error_type == "ConnectError" and not receipt.extracted
    assert (await service.search(scope, "语言偏好")).status == "degraded"
    assert (await service.status(scope))["pending"] == 1
    with service.db.engine.connect() as conn:
        assert "以后请用中文回复" not in str(conn.execute(select(memory_operations)).all())
    remote.offline = False
    accepted = await service.poll(receipt.operation_id)
    assert accepted.state == "ACCEPTED"
    remote.complete()
    assert (await indexed(service, accepted)).indexed


@pytest.mark.parametrize("lost", ["append", "commit"])
async def test_lost_receipt_reconciles_same_remote_operation_without_repetition(memory, lost):
    service, remote, scope, originals = memory
    source = batch(scope)
    originals[source.session_id] = source
    setattr(remote, "lost_" + lost, True)
    receipt = await service.sync(source)
    assert receipt.state == ("APPENDING" if lost == "append" else "COMMITTING")
    receipt = await service.poll(receipt.operation_id)
    assert receipt.state == "ACCEPTED"
    remote.complete()
    assert (await indexed(service, receipt)).indexed
    assert sum(path.endswith("/messages/batch") for _, path, _ in remote.calls) == 1
    assert sum(path.endswith("/commit") for _, path, _ in remote.calls) == 1


async def test_unknown_append_is_not_repeated_or_claimed_as_saved(memory):
    service, remote, scope, originals = memory
    source = batch(scope)
    originals[source.session_id] = source
    remote.drop_append = True
    receipt = await service.sync(source)
    remote.drop_append = False
    for _ in range(3):
        receipt = await service.poll(receipt.operation_id)
    assert receipt.state == "APPENDING" and not receipt.extracted and receipt.exported_seq == 0
    assert sum(path.endswith("/messages/batch") for _, path, _ in remote.calls) == 1
    assert not any(path.endswith("/commit") for _, path, _ in remote.calls)


async def test_correction_revokes_old_source_and_waits_for_confirmed_remote_index(memory):
    service, remote, scope, originals = memory
    original, _, uri = await seeded(memory)
    correction = batch(scope, session="correction", event_id="owner-correction", content="用户现在偏好英文回复")
    originals[correction.session_id] = correction
    request = MemoryCorrection(scope=scope, uri=uri, session_id=correction.session_id,
        source=correction.events[0], idempotency_key="correct-language")
    calls_before = len(remote.calls)
    result = await service.correct(request)
    assert not result.completed and result.state == "QUEUED" and result.memory_epoch == 1
    assert len(remote.calls) == calls_before
    with pytest.raises(MemoryBackendError, match="blocked"):
        await service.read(scope, uri)
    result = await service.poll(result.operation_id)
    assert result.completed and result.memory_epoch == 1
    assert (await service.read(scope, uri)).content == "用户现在偏好英文回复"
    assert (await service.read(scope, uri)).source_event_ids == ("owner-correction",)
    replay = await service.correct(request)
    assert replay.operation_id == result.operation_id and replay.memory_epoch == 1
    assert (await service.sync(original)).state == "SKIPPED"
    assert sum(path.endswith("/commit") for _, path, _ in remote.calls) == 1
    events = service.changes_since(scope)
    assert any(event["kind"] == "barrier" and event["refs"]["clean_session_required"] for event in events)
    assert any(event["kind"] == "corrected" for event in events)


async def test_forget_removes_derived_memory_and_input_archive_then_fences_replay(memory):
    service, remote, scope, _ = memory
    original, _, uri = await seeded(memory)
    calls_before = len(remote.calls)
    result = await service.forget(MemoryDeletion(scope=scope, uris=(uri,), idempotency_key="forget-language"))
    assert not result.completed and result.state == "QUEUED"
    assert len(remote.calls) == calls_before
    result = await service.poll(result.operation_id)
    assert result.completed and "source-1" in result.affected_source_ids
    assert uri not in remote.files and not remote.sessions
    assert not (await service.search(scope, "语言偏好")).hits
    assert (await service.sync(original)).state == "SKIPPED"
    remote.files[uri] = "old content restored by an external index rebuild"
    remote.extra_hits = [{"uri": uri, "score": 1.0}]
    assert not (await service.search(scope, "语言偏好")).hits
    with pytest.raises(PermissionError):
        await service.read(scope, uri)


async def test_forget_during_extraction_immediately_blocks_recall_and_discovers_late_derivations(memory):
    service, remote, scope, originals = memory
    source = batch(scope)
    originals[source.session_id] = source
    sync = await service.sync(source)
    deletion = await service.revoke_source(scope, "source-1", idempotency_key="forget-inflight")
    assert not deletion.completed and deletion.state == "QUEUED"
    deletion = await service.poll(deletion.operation_id)
    assert not deletion.completed and deletion.state == "WAITING_EXTRACTION"
    assert (await service.search(scope, "语言偏好")).status == "blocked"
    uri = remote.complete()
    done = await service.poll(deletion.operation_id)
    assert done.completed and uri not in remote.files
    assert (await service.poll(sync.operation_id)).state == "SUPERSEDED"


@pytest.mark.parametrize("operation", ["correct", "forget"])
async def test_mutation_registration_returns_durable_receipt_while_owner_driver_is_locked(memory, operation):
    service, remote, scope, originals = memory
    _, _, uri = await seeded(memory)
    if operation == "correct":
        correction = batch(scope, session="locked-correction", event_id="locked-owner-correction",
            content="用户现在偏好英文回复")
        originals[correction.session_id] = correction
        request = MemoryCorrection(scope=scope, uri=uri, session_id=correction.session_id,
            source=correction.events[0], idempotency_key="locked-correction")
    else:
        request = MemoryDeletion(scope=scope, uris=(uri,), idempotency_key="locked-forget")
    calls_before = len(remote.calls)
    async with service.locks[scope.owner_id]:
        pending = asyncio.create_task(getattr(service, operation)(request))
        await asyncio.sleep(0)
        try:
            assert pending.done(), "mutation registration must finish before the next owner-driver await"
            receipt = pending.result()
        finally:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
    row = service._row(receipt.operation_id)
    assert receipt.state == row["state"] == "QUEUED" and not receipt.completed
    assert row["payload_hash"] == digest(request.model_dump(mode="json"))
    assert receipt.memory_epoch == 1 and uri in receipt.changed_uris
    assert len(remote.calls) == calls_before and uri in remote.files
    assert (await service.status(scope))["state"] == "MUTATING"


@pytest.mark.parametrize("driver", ["poll", "reconcile"])
async def test_registered_correction_recovers_original_source_after_restart_and_driver_retry(memory, driver):
    service, remote, scope, originals = memory
    _, _, uri = await seeded(memory)
    correction = batch(scope, session="restart-correction", event_id="restart-owner-correction",
        content="用户现在偏好英文回复")
    originals[correction.session_id] = correction
    receipt = await service.correct(MemoryCorrection(scope=scope, uri=uri, session_id=correction.session_id,
        source=correction.events[0], idempotency_key="restart-correction"))
    loader = service.source_loader
    service.source_loader = None
    pending = await service.poll(receipt.operation_id)
    assert not pending.completed and pending.error_type == "MemoryBackendError"
    loaded = []
    def restored_loader(*arguments):
        loaded.append(arguments)
        return loader(*arguments)
    replacement = OpenVikingMemory(service.db, service.clock, service.config,
        source_loader=restored_loader, client=service.http.client)
    try:
        if driver == "poll":
            assert (await replacement.poll(receipt.operation_id)).completed
        else:
            await replacement.reconcile()
        assert replacement._row(receipt.operation_id)["state"] == "APPLIED"
        assert loaded == [(scope, correction.session_id, 0, 1, correction.branch, correction.projection_version)]
        document = await replacement.read(scope, uri)
        assert document.content == "用户现在偏好英文回复"
        assert document.source_event_ids == ("restart-owner-correction",)
        assert (await replacement.status(scope))["state"] == "READY"
        assert sum(path.endswith("/commit") for _, path, _ in remote.calls) == 1
    finally:
        await replacement.close()


async def test_group_and_project_namespaces_cannot_reuse_private_user_identity(memory):
    service, remote, scope, _ = memory
    await seeded(memory)
    group = scope.model_copy(update={"audience": "group"})
    calls = len(remote.calls)
    assert (await service.search(group, "owner secret")).status == "disabled"
    assert len(remote.calls) == calls
    project = scope.model_copy(update={"workspace_id": "project-a", "audience": "project"})
    original_resolver = service.binding_resolver
    service.binding_resolver = lambda request_scope: original_resolver(scope).model_copy(
        update={"workspace_id": request_scope.workspace_id})
    with pytest.raises(PermissionError, match="share"):
        await service.search(project, "owner secret")
    with pytest.raises(PermissionError):
        await service.read(scope, "viking://user/other/memories/profile.md")


@pytest.mark.parametrize("role", ["root", "admin"])
async def test_ordinary_data_requests_reject_root_or_admin_credentials(memory, role):
    service, remote, scope, originals = memory
    source = batch(scope)
    originals[source.session_id] = source
    remote.role = role
    receipt = await service.sync(source)
    assert receipt.state == "QUEUED" and receipt.error_type == "MemoryBackendError"
    assert all(path == "/health" for _, path, _ in remote.calls)


async def test_unregistered_or_foreign_remote_hits_never_leak_into_recall(memory):
    service, remote, scope, _ = memory
    _, _, uri = await seeded(memory)
    remote.extra_hits = [{"uri": "viking://user/other/memories/profile.md", "score": 2.0},
                         {"uri": uri.rsplit("/", 1)[0] + "/unregistered.md", "score": 1.0},
                         {"uri": "viking://user/owner_private/memories/skills/evil.md", "score": 3.0}]
    assert [item.uri for item in (await service.search(scope, "语言偏好")).hits] == [uri]


async def test_index_errors_are_not_reported_as_indexed_success(memory):
    service, remote, scope, originals = memory
    source = batch(scope)
    originals[source.session_id] = source
    receipt = await service.sync(source)
    remote.complete()
    remote.index_failed = True
    receipt = await service.poll(receipt.operation_id)
    receipt = await service.poll(receipt.operation_id)
    receipt = await service.poll(receipt.operation_id)
    assert receipt.extracted and not receipt.indexed and receipt.indexed_seq == 0
    assert not (await service.search(scope, "语言偏好")).hits


@pytest.mark.parametrize("endpoint", ["http://example.com", "https://u:p@example.com", "https://example.com/path", "https://example.com?secret=x"])
def test_insecure_endpoints_are_rejected(endpoint):
    with pytest.raises(ValidationError):
        OpenVikingConfig(endpoint=endpoint)


async def test_same_source_revision_cannot_change_silently(memory):
    service, _, scope, originals = memory
    source = batch(scope)
    originals[source.session_id] = source
    await service.sync(source)
    with pytest.raises(ValueError, match="conflicting replay"):
        await service.sync(source.model_copy(update={"events": (source.events[0].model_copy(update={"content": "different"}),)}))


async def test_restore_reconciles_read_only_and_preserves_deletion_fences(memory):
    service, remote, scope, _ = memory
    original, _, uri = await seeded(memory)
    deletion = await service.forget(MemoryDeletion(scope=scope, uris=(uri,), idempotency_key="restore-forget"))
    assert (await service.poll(deletion.operation_id)).completed
    manifest = service.export_barriers(scope)
    service.restore_barriers(scope, manifest)
    assert (await service.search(scope, "语言")).status == "blocked"
    with pytest.raises(MemoryBackendError, match="reconciliation"):
        await service.sync(original)
    with pytest.raises(MemoryBackendError, match="reconciliation"):
        await service.forget(MemoryDeletion(scope=scope, uris=(uri,), idempotency_key="blocked-write"))
    before = len(remote.calls)
    await service.reconcile()
    assert len(remote.calls) == before
    assert (await service.reconcile_restore(scope))["state"] == "READY"
    assert all(method == "GET" for method, _, _ in remote.calls[before:])
    assert (await service.sync(original)).state == "SKIPPED"
    remote.files[uri] = "stale backup content"
    assert not (await service.search(scope, "语言")).hits
    with pytest.raises(PermissionError):
        await service.read(scope, uri)


async def test_restore_stays_blocked_for_active_unknown_or_resurrected_remote_state(memory):
    service, remote, scope, _ = memory
    _, _, uri = await seeded(memory)
    deletion = await service.forget(MemoryDeletion(scope=scope, uris=(uri,), idempotency_key="restore-forget"))
    assert (await service.poll(deletion.operation_id)).completed
    service.restore_barriers(scope, service.export_barriers(scope))
    task = next(iter(remote.tasks.values()))
    task["status"] = "running"
    with pytest.raises(MemoryBackendError, match="quiescence"):
        await service.reconcile_restore(scope)
    assert (await service.status(scope))["state"] == "RESTORE_REQUIRED"
    task["status"] = "completed"
    remote.files[uri] = "stale backup content"
    with pytest.raises(MemoryBackendError, match="disagrees"):
        await service.reconcile_restore(scope)
    assert (await service.status(scope))["state"] == "RESTORE_REQUIRED"
    remote.files.pop(uri)
    remote.offline = True
    with pytest.raises(httpx.ConnectError):
        await service.reconcile_restore(scope)
    assert (await service.status(scope))["state"] == "RESTORE_REQUIRED"


@pytest.mark.parametrize("has_receipt", [False, True])
async def test_restore_observes_existing_extraction_without_replaying_it(memory, has_receipt):
    service, remote, scope, originals = memory
    source = batch(scope)
    originals[source.session_id] = source
    receipt = await service.sync(source)
    uri = remote.complete()
    service.restore_barriers(scope, service.export_barriers(scope))
    if not has_receipt:
        remote.tasks.clear()
    before = len(remote.calls)
    if has_receipt:
        assert (await service.reconcile_restore(scope))["state"] == "READY"
        assert (await service.poll(receipt.operation_id)).state == "SUPERSEDED"
        assert (await service.read(scope, uri)).source_event_ids == ("source-1",)
    else:
        with pytest.raises(MemoryBackendError, match="terminal receipt"):
            await service.reconcile_restore(scope)
        assert (await service.status(scope))["state"] == "RESTORE_REQUIRED"
    assert all(method == "GET" for method, _, _ in remote.calls[before:])


async def test_multi_source_export_extracts_independently_and_recovers_each_slice(memory):
    service, remote, scope, originals = memory
    source = SourceBatch(scope=scope, session_id="two-originals", from_seq=0, to_seq=2,
        events=(SourceEvent(id="source-a", seq=1, role="user", content="项目A的值为一"),
                SourceEvent(id="source-b", seq=2, role="user", content="项目B的值为二")))
    originals[source.session_id] = source
    receipt = await service.sync(source)
    first_sid = next(iter(remote.sessions))
    assert len(remote.sessions[first_sid]["messages"]) == 1
    first_uri = remote.complete(sid=first_sid, relative="entities/a.md", content="项目A的值为一")
    replacement = OpenVikingMemory(service.db, service.clock, service.config,
        source_loader=service.source_loader, client=service.http.client)
    for _ in range(20):
        receipt = await replacement.poll(receipt.operation_id)
        for sid, session in list(remote.sessions.items()):
            if remote.tasks["commit-" + sid]["status"] == "running":
                assert len(session["messages"]) == 1
                assert session["messages"][0]["source_message_ids"] == ["source-b"]
                remote.complete(sid=sid, relative="entities/b.md", content="项目B的值为二")
        if receipt.indexed:
            break
    assert receipt.indexed and receipt.indexed_seq == 2 and len(receipt.changed_uris) == 2
    second_uri = "viking://user/owner_private/memories/entities/b.md"
    assert (await replacement.read(scope, first_uri)).source_event_ids == ("source-a",)
    assert (await replacement.read(scope, second_uri)).source_event_ids == ("source-b",)
    forgotten = await replacement.forget(MemoryDeletion(scope=scope, uris=(first_uri,), idempotency_key="a-only"))
    forgotten = await replacement.poll(forgotten.operation_id)
    assert forgotten.completed and forgotten.changed_uris == (first_uri,)
    assert (await replacement.read(scope, second_uri)).content == "项目B的值为二"


@pytest.mark.parametrize("mutation", ["correct", "forget"])
async def test_uri_mutation_preserves_sibling_with_legacy_batch_union_and_freezes_replay(memory, mutation):
    from qqbot.storage.memory_sync_schema import memory_sources

    service, remote, scope, originals = memory
    source = batch(scope, source_event_ids=("legacy-a", "legacy-b"))
    originals[source.session_id] = source
    pending = await service.sync(source)
    first_uri = remote.complete(relative="entities/a.md", content="项目A的旧值")
    sid = next(iter(remote.sessions))
    second_uri = "viking://user/owner_private/memories/entities/b.md"
    remote.files[second_uri] = "项目B保持有效"
    remote.diffs[remote.sessions[sid]["archive"] + "/memory_diff.json"]["operations"]["adds"].append({
        "uri": second_uri, "memory_type": "entities", "after": "项目B保持有效"})
    await indexed(service, pending)
    if mutation == "correct":
        correction = batch(scope, session="new-owner-statement", event_id="new-a", content="项目A的新值")
        originals[correction.session_id] = correction
        request = MemoryCorrection(scope=scope, uri=first_uri, session_id=correction.session_id,
            source=correction.events[0], idempotency_key="legacy-a-correct")
    else:
        request = MemoryDeletion(scope=scope, uris=(first_uri,), idempotency_key="legacy-a-forget")
    result = await getattr(service, mutation)(request)
    assert result.changed_uris == (first_uri,)
    assert result.affected_source_ids == ("legacy-a", "legacy-b")
    result = await service.poll(result.operation_id)
    assert result.completed and result.changed_uris == (first_uri,)
    assert result.affected_source_ids == ("legacy-a", "legacy-b")
    changes = [event for event in service.changes_since(scope) if event["operation_id"] == result.operation_id]
    assert {event["kind"] for event in changes} == {"barrier", "corrected" if mutation == "correct" else "forgotten"}
    assert all(event["refs"]["source_event_ids"] == ["legacy-a", "legacy-b"] for event in changes)
    assert (await service.read(scope, second_uri)).content == "项目B保持有效"
    assert (await service.read(scope, second_uri)).source_event_ids == ("legacy-a", "legacy-b")
    if mutation == "correct":
        assert (await service.read(scope, first_uri)).source_event_ids == ("new-a",)
    else:
        assert first_uri not in remote.files
    with service.db.engine.connect() as conn:
        assert set(conn.execute(select(memory_sources.c.state).where(
            memory_sources.c.source_id.in_(["legacy-a", "legacy-b"]))).scalars()) == {"FROZEN"}
    manifest = service.export_barriers(scope)
    assert len(manifest["frozen_sources"]) == 2
    replacement = OpenVikingMemory(service.db, service.clock, service.config,
        source_loader=service.source_loader, client=service.http.client)
    replacement.restore_barriers(scope, manifest)
    await replacement.reconcile_restore(scope)
    assert (await replacement.read(scope, second_uri)).content == "项目B保持有效"
    assert (await replacement.sync(source)).state == "SKIPPED"
    assert sum(path.endswith("/commit") for _, path, _ in remote.calls) == 1


async def test_later_forget_never_rebuilds_a_previously_frozen_shared_source(memory):
    from qqbot.storage.memory_sync_schema import memory_sources

    service, remote, scope, originals = memory
    original = batch(scope, event_id="shared-source", content="项目A旧值与项目B旧值")
    originals[original.session_id] = original
    pending = await service.sync(original)
    first_uri = remote.complete(relative="entities/a.md", content="项目A旧值")
    sid = next(iter(remote.sessions))
    second_uri = "viking://user/owner_private/memories/entities/b.md"
    remote.files[second_uri] = "项目B旧值"
    remote.diffs[remote.sessions[sid]["archive"] + "/memory_diff.json"]["operations"]["adds"].append({
        "uri": second_uri, "memory_type": "entities", "after": "项目B旧值"})
    await indexed(service, pending)
    forgotten_a = await service.forget(MemoryDeletion(scope=scope, uris=(first_uri,), idempotency_key="first-forget-a"))
    assert (await service.poll(forgotten_a.operation_id)).completed
    assert (await service.read(scope, second_uri)).content == "项目B旧值"
    update_b = batch(scope, session="new-source-b", event_id="source-b-update", content="项目B新值")
    originals[update_b.session_id] = update_b
    pending_b = await service.sync(update_b)
    remote.complete(relative="entities/b.md", content="项目B新值", updates=True)
    await indexed(service, pending_b)
    forgotten_b = await service.revoke_source(scope, "source-b-update", idempotency_key="second-forget-b")
    assert (await service.poll(forgotten_b.operation_id)).completed
    assert first_uri not in remote.files and second_uri not in remote.files
    with service.db.engine.connect() as conn:
        states = dict(conn.execute(select(memory_sources.c.source_id, memory_sources.c.state)).all())
    assert states == {"shared-source": "FROZEN", "source-b-update": "REVOKED"}
    completed = next(change for change in service.changes_since(scope)
        if change["operation_id"] == forgotten_b.operation_id and change["kind"] == "forgotten")
    assert completed["refs"]["rebuild_source_ids"] == []
    manifest = service.export_barriers(scope)
    assert [source["source_id"] for source in manifest["frozen_sources"]] == ["shared-source"]
    replacement = OpenVikingMemory(service.db, service.clock, service.config,
        source_loader=service.source_loader, client=service.http.client)
    replacement.restore_barriers(scope, manifest)
    await replacement.reconcile_restore(scope)
    calls_before_replay = list(remote.calls)
    assert (await replacement.sync(original)).state == "SKIPPED"
    assert (await replacement.sync(update_b)).state == "SKIPPED"
    assert remote.calls == calls_before_replay and not remote.files
