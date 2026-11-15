"""Incremental raw-source synchronization, one serial mutation queue per owner.

Remote append/commit uncertainty is reconciled, never hidden by a second fact store.
"""
import asyncio
import hashlib
import inspect
import json
import math
import re
from collections import defaultdict
from contextlib import nullcontext
from urllib.parse import quote

import httpx
from sqlalchemy import insert, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from qqbot.contracts.memory import (
    MemoryBinding,
    MemoryCorrection,
    MemoryDeletion,
    MemoryDocument,
    MemoryHit,
    MemoryScope,
    MemorySearchResult,
    MutationReceipt,
    SourceBatch,
    SyncReceipt,
)
from qqbot.domain import stable_id
from qqbot.storage.memory_sync_schema import (
    memory_barriers as barriers,
)
from qqbot.storage.memory_sync_schema import (
    memory_changes as changes,
)
from qqbot.storage.memory_sync_schema import (
    memory_derivations as derivations,
)
from qqbot.storage.memory_sync_schema import (
    memory_documents as documents,
)
from qqbot.storage.memory_sync_schema import (
    memory_operations as operations,
)
from qqbot.storage.memory_sync_schema import (
    memory_owners as owners,
)
from qqbot.storage.memory_sync_schema import (
    memory_sources as sources,
)
from qqbot.storage.memory_sync_schema import (
    memory_streams as streams,
)

from .transport import MemoryBackendError, OpenVikingHTTP, RemoteMissing

TERMINAL = {"EXTRACTED", "INDEXED", "SKIPPED", "APPLIED", "SUPERSEDED", "FAILED"}
REMOTE_TERMINAL = {"completed", "failed", "cancelled"}
BACKEND_ERRORS = (httpx.HTTPError, MemoryBackendError, TimeoutError, ValueError, TypeError, KeyError)
MEMORY_TYPES = ("profile", "preferences", "entities", "events")
_SECRET = re.compile(r"(?i)(sk-[a-z0-9_-]{12,}|-----BEGIN .*PRIVATE KEY|(?:api[_-]?key|password|密码)\s*[:=]\s*\S+)")


def digest(value):
    value = value if isinstance(value, str) else json.dumps(value, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(value.encode()).hexdigest()


def affected_sources(refs):
    return tuple(sorted(set(refs.get("source_ids", ())) | set(refs.get("frozen_source_ids", ()))))


def put(conn, table, identifier, values):
    statement = sqlite_insert(table).values(id=identifier, **values)
    conn.execute(statement.on_conflict_do_update(index_elements=[table.c.id], set_=values))


class OpenVikingMemory:
    def __init__(self, db, clock, config, binding_resolver=None, *, source_loader=None, client=None):
        self.db, self.clock, self.config = db, clock, config
        self.binding_resolver = binding_resolver or self._configured_binding
        self.source_loader = source_loader
        self.http = OpenVikingHTTP(config, client=client)
        self.locks = defaultdict(asyncio.Lock)

    def _configured_binding(self, scope):
        return next((binding for binding in self.config.bindings
                     if (binding.owner_id, binding.workspace_id) == (scope.owner_id, scope.workspace_id)), None)

    def enabled(self, scope):
        return bool(self.config.endpoint and (
            self.config.group_enabled if scope.audience == "group" else self.config.private_enabled))

    def _binding(self, scope):
        binding = self.binding_resolver(scope)
        if not isinstance(binding, MemoryBinding) or (binding.owner_id, binding.workspace_id) != (
                scope.owner_id, scope.workspace_id):
            raise PermissionError("memory scope has no current host-authorized identity binding")
        if scope.audience == "group" and scope.workspace_id == "personal":
            raise PermissionError("group input cannot use the owner's personal memory scope")
        return binding

    def _now(self):
        return self.clock.now().isoformat()

    def _owner(self, scope, *, conn=None):
        binding = self._binding(scope)
        identity = stable_id(binding.account_id, binding.user_id)
        binding_hash = digest([self.config.endpoint, self.config.namespace, self.config.expected_version,
                               binding.owner_id, binding.workspace_id, binding.account_id, binding.user_id])
        with (nullcontext(conn) if conn is not None else self.db.transaction()) as connection:
            row = connection.execute(select(owners).where(owners.c.id == scope.key)).mappings().first()
            if row:
                if row["binding_hash"] != binding_hash:
                    raise PermissionError("memory binding changed; explicit receipt and barrier migration required")
                return dict(row)
            duplicate = connection.execute(select(owners.c.id).where(owners.c.remote_identity == identity)).first()
            if duplicate:
                raise PermissionError("private/project memory scopes cannot share a remote user namespace")
            values = {"scope_json": scope.model_dump_json(), "binding_json": binding.model_dump_json(),
                          "binding_hash": binding_hash, "remote_identity": identity, "epoch": 0,
                          "state": "READY", "last_error": None, "updated_at": self._now()}
            connection.execute(insert(owners).values(id=scope.key, **values))
            return {"id": scope.key, **values}

    async def _request(self, scope, method, path, **kwargs):
        if not self.enabled(scope):
            raise PermissionError("memory scope disabled")
        self._owner(scope)  # Current host identity is checked again at each remote boundary.
        return await self.http.request(self._binding(scope), method, path, **kwargs)

    def _row(self, operation_id):
        with self.db.engine.connect() as conn:
            row = conn.execute(select(operations).where(operations.c.id == operation_id)).mappings().first()
            if not row:
                raise LookupError("memory operation unavailable")
            return dict(row)

    def _update(self, operation_id, **values):
        with self.db.transaction() as conn:
            conn.execute(update(operations).where(operations.c.id == operation_id).values(
                **values, updated_at=self._now()))

    def _failure(self, scope, operation_id, error):
        with self.db.transaction() as conn:
            conn.execute(update(operations).where(operations.c.id == operation_id).values(
                error_type=type(error).__name__, updated_at=self._now()))
            conn.execute(update(owners).where(owners.c.id == scope.key).values(
                last_error=type(error).__name__, updated_at=self._now()))

    def _stream_id(self, batch, epoch):
        return stable_id(batch.scope.key, batch.session_id, batch.branch, batch.projection_version, str(epoch))

    @staticmethod
    def _source_ref(event):
        return {"id": event.id, "seq": event.seq, "revision": event.revision, "role": event.role,
                "kind": event.kind, "origins": list(event.origins), "content_hash": digest(event.content),
                "evidence_refs": list(event.evidence_refs)}

    def _source_allowed(self, conn, scope, event):
        if (event.kind in {"relay", "summary", "tool_output", "skill_review"} or event.role == "tool"
                or not event.content.strip() or _SECRET.search(event.content)
                or event.kind == "verified_fact" and not event.evidence_refs):
            return False
        if conn.execute(select(barriers.c.id).where(
                barriers.c.scope_id == scope.key, barriers.c.source_id.in_(event.origins))).first():
            return False
        if conn.execute(select(sources.c.id).where(sources.c.scope_id == scope.key,
                sources.c.source_id.in_(event.origins), sources.c.state.in_({"FROZEN", "REVOKED"}))).first():
            return False
        previous = conn.execute(select(sources).where(sources.c.scope_id == scope.key,
            sources.c.source_id.in_(event.origins), sources.c.revision == event.revision)).mappings().all()
        if any(row["event_id"] == event.id and row["content_hash"] != digest(event.content) for row in previous):
            raise ValueError("an original source revision changed without a new revision")
        return not set(event.origins).issubset({row["source_id"] for row in previous
                                              if row["state"] in {"QUEUED", "EXTRACTED"}})

    async def sync(self, batch: SourceBatch):
        if not self.enabled(batch.scope):
            return SyncReceipt(operation_id="", state="DISABLED", accepted=False)
        owner = self._owner(batch.scope)
        if owner["state"] == "RESTORE_REQUIRED":
            raise MemoryBackendError("restored memory requires remote journal reconciliation")
        if sum(len(event.content.encode()) for event in batch.events) > 512 * 1024:
            raise ValueError("memory export exceeds 512 KiB; export a smaller original-source range")
        with self.db.transaction() as conn:
            operation_id = self._sync_id(batch, owner["epoch"])
            old = conn.execute(select(operations).where(operations.c.id == operation_id)).mappings().first()
            if old:
                self._register_sync(conn, batch, owner)
            else:
                eligible = [event for event in batch.events if self._source_allowed(conn, batch.scope, event)]
                dependencies = None
                if len(eligible) > 1:
                    # OpenViking's diff has no per-document source IDs, so isolate extraction inputs.
                    dependencies, start = [], batch.from_seq
                    for index, event in enumerate(eligible):
                        end = eligible[index + 1].seq - 1 if index + 1 < len(eligible) else batch.to_seq
                        part = batch.model_copy(update={"from_seq": start, "to_seq": end,
                            "events": tuple(item for item in batch.events if start < item.seq <= end)})
                        dependencies.append(self._register_sync(conn, part, owner))
                        start = end
                self._register_sync(conn, batch, owner, dependencies=dependencies)
        async with self.locks[batch.scope.owner_id]:
            await self._drive(batch.scope, batch=batch)
        return self._sync_receipt(self._row(operation_id))

    def _sync_id(self, batch, epoch):
        return stable_id("memory-sync", batch.scope.key, batch.session_id, str(batch.from_seq),
            str(batch.to_seq), batch.branch, batch.projection_version, batch.policy_revision, str(epoch))

    def _register_sync(self, conn, batch, owner, *, dependencies=None):
        operation_id = self._sync_id(batch, owner["epoch"])
        all_refs = [self._source_ref(event) for event in batch.events]
        payload_hash = digest(all_refs)
        old = conn.execute(select(operations).where(operations.c.id == operation_id)).mappings().first()
        if old:
            if old["payload_hash"] != payload_hash:
                raise ValueError("conflicting replay of a memory source batch")
        else:
            pending = conn.execute(select(operations.c.id).where(operations.c.scope_id == batch.scope.key,
                    operations.c.state.not_in(TERMINAL))).fetchmany(128)
            if len(pending) >= 128:
                raise ValueError("memory synchronization queue is full")
            stream_id = self._stream_id(batch, owner["epoch"])
            stream = conn.execute(select(streams).where(streams.c.id == stream_id)).mappings().first()
            if not stream:
                conn.execute(insert(streams).values(id=stream_id, scope_id=batch.scope.key,
                    session_id=batch.session_id, branch=batch.branch, projection_version=batch.projection_version,
                    epoch=owner["epoch"], exported_seq=0, extracted_seq=0, indexed_seq=0))
            refs = {"session_id": batch.session_id, "from_seq": batch.from_seq, "to_seq": batch.to_seq,
                    "branch": batch.branch, "projection_version": batch.projection_version,
                    "policy_revision": batch.policy_revision, "events": all_refs, "eligible_ids": [],
                    "dependency_operation_ids": list(dependencies or ())}
            for event in batch.events:
                source_dependencies = conn.execute(select(sources.c.operation_id).where(
                    sources.c.scope_id == batch.scope.key, sources.c.source_id.in_(event.origins),
                    sources.c.revision == event.revision, sources.c.state == "QUEUED")).scalars().all()
                refs["dependency_operation_ids"].extend(source_dependencies)
                if dependencies is None and self._source_allowed(conn, batch.scope, event):
                    refs["eligible_ids"].append(event.id)
                    for source_id in event.origins:
                        put(conn, sources, stable_id(batch.scope.key, source_id, event.revision), {
                            "scope_id": batch.scope.key, "source_id": source_id, "revision": event.revision,
                            "event_id": event.id, "operation_id": operation_id,
                            "content_hash": digest(event.content), "state": "QUEUED"})
            conn.execute(insert(operations).values(id=operation_id, scope_id=batch.scope.key,
                stream_id=stream_id, kind="SYNC", epoch=owner["epoch"], state="QUEUED",
                refs_json=json.dumps(refs), payload_hash=payload_hash,
                remote_session_id="elephant-" + operation_id[:48], result_json="{}",
                created_at=self._now(), updated_at=self._now()))
        return operation_id

    def _sync_receipt(self, row):
        with self.db.engine.connect() as conn:
            owner = conn.execute(select(owners).where(owners.c.id == row["scope_id"])).mappings().one()
            stream = conn.execute(select(streams).where(streams.c.id == row["stream_id"])).mappings().one()
        result = json.loads(row["result_json"])
        return SyncReceipt(operation_id=row["id"], state=row["state"], accepted=True,
            indexed=row["state"] in {"INDEXED", "SKIPPED"},
            extracted=row["state"] in {"EXTRACTED", "INDEXED", "SKIPPED"},
            exported_seq=stream["exported_seq"], indexed_seq=stream["indexed_seq"],
            extracted_seq=stream["extracted_seq"], memory_epoch=owner["epoch"],
            remote_task_id=row["remote_task_id"], changed_uris=tuple(result.get("uris", ())),
            error_type=row["error_type"])

    async def _load_batch(self, scope, refs, batch):
        if batch is not None and batch.session_id == refs["session_id"] and (
                batch.from_seq <= refs["from_seq"] < refs["to_seq"] <= batch.to_seq):
            batch = batch.model_copy(update={"from_seq": refs["from_seq"], "to_seq": refs["to_seq"],
                "events": tuple(event for event in batch.events if refs["from_seq"] < event.seq <= refs["to_seq"])})
        if batch is None or (batch.session_id, batch.from_seq, batch.to_seq) != (
                refs["session_id"], refs["from_seq"], refs["to_seq"]):
            if not self.source_loader:
                raise MemoryBackendError("original Harness source export is required to resume this batch")
            batch = self.source_loader(scope, refs["session_id"], refs["from_seq"], refs["to_seq"],
                                       refs["branch"], refs["projection_version"])
            if inspect.isawaitable(batch):
                batch = await batch
        if not isinstance(batch, SourceBatch) or batch.scope != scope:
            raise PermissionError("Harness source export scope mismatch")
        if [self._source_ref(event) for event in batch.events] != refs["events"]:
            raise ValueError("Harness source export no longer matches the accepted batch")
        return batch

    async def _drive(self, scope, *, batch=None, correction=None):
        if self._owner(scope)["state"] == "RESTORE_REQUIRED":
            return
        with self.db.engine.connect() as conn:
            rows = [dict(row) for row in conn.execute(select(operations).where(
                operations.c.scope_id == scope.key, operations.c.state.not_in(TERMINAL - {"EXTRACTED"}))
                .order_by(operations.c.seq)).mappings()]
        # A deletion barrier immediately outranks older exports and new factual writes.
        mutations = [row for row in rows if row["kind"] != "SYNC"]
        if mutations:
            row = mutations[0]
            try:
                async with asyncio.timeout(self.config.operation_budget_seconds):
                    await self._apply_mutation(scope, row, correction=correction)
            except BACKEND_ERRORS as error:
                self._failure(scope, row["id"], error)
            return
        if not rows:
            return
        row = rows[0]
        try:
            async with asyncio.timeout(self.config.operation_budget_seconds):
                await self._advance_sync(scope, row, batch)
        except BACKEND_ERRORS as error:
            self._failure(scope, row["id"], error)

    async def _advance_sync(self, scope, row, batch=None):
        refs = json.loads(row["refs_json"])
        if row["epoch"] != self._owner(scope)["epoch"]:
            await self._settle_obsolete(scope, row)
            return
        if row["state"] in {"QUEUED", "WAITING_SOURCE"} and not refs["eligible_ids"]:
            with self.db.engine.connect() as conn:
                dependency_states = conn.execute(select(operations.c.state).where(
                    operations.c.id.in_(refs.get("dependency_operation_ids", ())))).scalars().all()
            if any(state in {"FAILED", "SUPERSEDED"} for state in dependency_states):
                self._update(row["id"], state="FAILED", error_type="SourceDependencyFailed")
                return
            if any(state not in {"INDEXED", "SKIPPED"} for state in dependency_states):
                self._update(row["id"], state="WAITING_SOURCE")
                return
            with self.db.engine.connect() as conn:
                results = conn.execute(select(operations.c.result_json).where(
                    operations.c.id.in_(refs.get("dependency_operation_ids", ())))).scalars().all()
            uris = sorted({uri for result in results for uri in json.loads(result).get("uris", ())})
            self._update(row["id"], state="SKIPPED", result_json=json.dumps({"uris": uris}), error_type=None)
            self._watermarks(row)
            return
        sid = quote(row["remote_session_id"], safe="")
        session_uri = f"viking://user/{self._binding(scope).user_id}/sessions/{row['remote_session_id']}"
        if row["state"] == "QUEUED":
            batch = await self._load_batch(scope, refs, batch)
            # Creating a deterministic empty session is idempotent; append is not.
            await self._request(scope, "POST", "/api/v1/sessions", json={
                "session_id": row["remote_session_id"], "auto_commit_policy": None,
                "memory_policy": {"self": {"enabled": True}, "peer": {"enabled": False},
                                  "working_memory": {"enabled": False}, "memory_types": list(MEMORY_TYPES)},
            })
            messages = [{"role": event.role, "content": event.content, "turn_id": event.id,
                         "source_message_ids": list(event.origins),
                         "message_kind": "user_query" if event.role == "user" else "assistant_step",
                         **({"created_at": event.created_at} if event.created_at else {})}
                        for event in batch.events if event.id in refs["eligible_ids"]]
            self._update(row["id"], state="APPENDING", error_type=None)
            await self._request(scope, "POST", f"/api/v1/sessions/{sid}/messages/batch", json={"messages": messages})
            self._update(row["id"], state="EXPORTED", error_type=None)
            self._watermarks(row)
            row = self._row(row["id"])
        elif row["state"] == "APPENDING":
            raw = await self._request(scope, "GET", "/api/v1/content/read", params={
                "uri": session_uri + "/messages.jsonl", "raw": "true"})
            persisted = [json.loads(line) for line in raw.splitlines() if line.strip()]
            expected = [event for event in refs["events"] if event["id"] in refs["eligible_ids"]]
            if not self._messages_match(persisted, expected):
                raise MemoryBackendError("unconfirmed append; original request is not automatically repeated")
            self._update(row["id"], state="EXPORTED", error_type=None)
            self._watermarks(row)
            row = self._row(row["id"])
        if row["state"] == "EXPORTED":
            self._update(row["id"], state="COMMITTING")
            result = await self._request(scope, "POST", f"/api/v1/sessions/{sid}/commit", json={
                "enable_working_memory": False, "keep_recent_count": 0})
            if not isinstance(result, dict) or result.get("status") != "accepted" or not result.get("task_id"):
                raise MemoryBackendError("commit did not confirm an extraction task")
            archive_uri = result.get("archive_uri")
            if archive_uri != session_uri + "/history/archive_001":
                raise MemoryBackendError("unexpected extraction archive identity")
            self._update(row["id"], state="ACCEPTED", remote_task_id=result["task_id"],
                         archive_uri=archive_uri, error_type=None)
            return
        if row["state"] == "COMMITTING":
            tasks = await self._request(scope, "GET", "/api/v1/tasks", params={
                "task_type": "session_commit", "resource_id": row["remote_session_id"], "limit": 200})
            candidates = [task for task in tasks if task.get("resource_id") == row["remote_session_id"]
                          and task.get("task_type") == "session_commit"]
            if len(candidates) != 1:
                raise MemoryBackendError("unconfirmed commit; owner writes await task reconciliation")
            self._update(row["id"], state="ACCEPTED", remote_task_id=candidates[0]["task_id"],
                         archive_uri=session_uri + "/history/archive_001", error_type=None)
            row = self._row(row["id"])
        if row["state"] == "ACCEPTED":
            await self._poll_extraction(scope, row)
        elif row["state"] == "EXTRACTED":
            await self._index(scope, row)

    @staticmethod
    def _messages_match(messages, expected):
        if len(messages) != len(expected):
            return False
        for message, event in zip(messages, expected, strict=True):
            parts = message.get("parts", [])
            text = "".join(part.get("text", "") for part in parts if part.get("type") == "text")
            if (message.get("turn_id") != event["id"] or message.get("role") != event["role"]
                    or tuple(message.get("source_message_ids") or ()) != tuple(event["origins"])
                    or digest(text) != event["content_hash"]):
                return False
        return True

    def _watermarks(self, row):
        with self.db.transaction() as conn:
            current = conn.execute(select(streams).where(streams.c.id == row["stream_id"])).mappings().one()
            rows = conn.execute(select(operations).where(operations.c.stream_id == row["stream_id"])) .mappings().all()
            for name, eligible in {
                "exported_seq": {"EXPORTED", "COMMITTING", "ACCEPTED", "EXTRACTED", "INDEXED", "SKIPPED"},
                "extracted_seq": {"EXTRACTED", "INDEXED", "SKIPPED"},
                "indexed_seq": {"INDEXED", "SKIPPED"},
            }.items():
                cursor = current[name]
                while True:
                    next_cursor = max([cursor, *(json.loads(item["refs_json"])["to_seq"] for item in rows
                        if item["state"] in eligible and json.loads(item["refs_json"])["from_seq"] <= cursor)])
                    if next_cursor == cursor:
                        break
                    cursor = next_cursor
                conn.execute(update(streams).where(streams.c.id == row["stream_id"]).values(**{name: cursor}))

    def _valid_uri(self, scope, uri, *, archive=False):
        binding = self._binding(scope)
        if not isinstance(uri, str) or "%" in uri or "\\" in uri or "?" in uri or "#" in uri:
            raise PermissionError("invalid scoped memory URI")
        if any(part in {".", "..", ""} for part in uri.removeprefix("viking://").split("/")):
            raise PermissionError("memory URI traversal is prohibited")
        root = f"viking://user/{binding.user_id}/sessions/" if archive else binding.root_uri + "/"
        if not uri.startswith(root):
            raise PermissionError("memory URI is outside the host-bound user workspace")
        if not archive:
            relative = uri[len(root):]
            if not (relative == "profile.md" or any(relative.startswith(kind + "/") for kind in MEMORY_TYPES[1:])):
                raise PermissionError("only factual memory types are visible to the agent")
        return uri

    async def _poll_extraction(self, scope, row):
        task = await self._request(scope, "GET", "/api/v1/tasks/" + quote(row["remote_task_id"], safe=""))
        if task.get("resource_id") != row["remote_session_id"] or task.get("task_type") != "session_commit":
            raise MemoryBackendError("extraction task identity mismatch")
        state = task.get("status")
        if state not in REMOTE_TERMINAL:
            return
        if state != "completed":
            self._update(row["id"], state="FAILED", error_type="ExtractionFailed")
            return
        result = task.get("result") or {}
        if (result.get("archive_uri") != row["archive_uri"] or result.get("user_config_error")
                or set(result.get("effective_memory_types", ())) - set(MEMORY_TYPES)
                or result.get("session_skills_extracted", 0)):
            raise MemoryBackendError("extraction policy or archive contract mismatch")
        diff_uri = row["archive_uri"] + "/memory_diff.json"
        if result.get("memory_diff_uri") != diff_uri:
            raise MemoryBackendError("completed task has no confirmed memory change receipt")
        raw = await self._request(scope, "GET", "/api/v1/content/read", params={"uri": diff_uri, "raw": "true"})
        diff = json.loads(raw)
        if diff.get("archive_uri") != row["archive_uri"] or not isinstance(diff.get("operations"), dict):
            raise MemoryBackendError("invalid memory change receipt")
        refs = json.loads(row["refs_json"])
        source_ids = {source_id for event in refs["events"] if event["id"] in refs["eligible_ids"]
                      for source_id in event["origins"]}
        changed = []
        for kind in ("adds", "updates", "deletes"):
            for item in diff["operations"].get(kind, []):
                uri = self._valid_uri(scope, item["uri"])
                if item.get("memory_type") not in MEMORY_TYPES:
                    raise MemoryBackendError("extraction wrote a disabled memory type")
                content_hash = ""
                if kind != "deletes":
                    visible = await self._request(scope, "GET", "/api/v1/content/read", params={"uri": uri})
                    if not isinstance(visible, str) or visible.strip() != item.get("after", "").strip():
                        raise MemoryBackendError("live memory no longer matches its extraction receipt")
                    content_hash = digest(visible.strip())
                changed.append((kind, uri, content_hash))
        # Persist only URI/hash/source relationships, never before/after fact text.
        with self.db.transaction() as conn:
            for kind, uri, content_hash in changed:
                put(conn, documents, stable_id(scope.key, uri), {"scope_id": scope.key, "uri": uri,
                    "content_hash": content_hash, "epoch": row["epoch"], "state": "DELETED" if kind == "deletes" else "ACTIVE",
                    "index_state": "PENDING", "index_task_id": None, "updated_at": self._now()})
                if kind != "deletes":
                    conn.execute(update(barriers).where(barriers.c.scope_id == scope.key,
                        barriers.c.uri == uri, barriers.c.epoch <= row["epoch"]).values(allowed_hash=content_hash))
                for source_id in source_ids:
                    identifier = stable_id(scope.key, uri, source_id, row["id"])
                    conn.execute(sqlite_insert(derivations).values(id=identifier, scope_id=scope.key,
                        uri=uri, source_id=source_id, operation_id=row["id"], active=1).on_conflict_do_nothing())
            conn.execute(update(sources).where(sources.c.operation_id == row["id"],
                sources.c.state.not_in({"FROZEN", "REVOKED"})).values(state="EXTRACTED"))
            conn.execute(update(operations).where(operations.c.id == row["id"]).values(
                state="EXTRACTED", result_json=json.dumps({"uris": list(dict.fromkeys(uri for _, uri, _ in changed)),
                    "diff_hash": digest(raw), "skipped_count": len(diff.get("skipped_operations", ())) }),
                error_type=None, updated_at=self._now()))
            self._change(conn, scope, row["id"], row["epoch"], "extracted", {
                "uris": list(dict.fromkeys(uri for _, uri, _ in changed)), "source_event_ids": sorted(source_ids)})
        self._watermarks(row)

    async def _index(self, scope, row):
        uris = json.loads(row["result_json"]).get("uris", [])
        for uri in uris:
            with self.db.engine.connect() as conn:
                doc = dict(conn.execute(select(documents).where(documents.c.id == stable_id(scope.key, uri))).mappings().one())
            if doc["state"] == "DELETED" or doc["index_state"] == "INDEXED":
                continue
            if doc["index_task_id"]:
                task = await self._request(scope, "GET", "/api/v1/tasks/" + quote(doc["index_task_id"], safe=""))
                if task.get("resource_id") != uri:
                    raise MemoryBackendError("index task identity mismatch")
                if task.get("status") not in REMOTE_TERMINAL:
                    return
                if (task.get("status") != "completed" or (task.get("result") or {}).get("status") != "completed"
                        or (task.get("result") or {}).get("failed_records", 0)):
                    raise MemoryBackendError("memory indexing failed")
                with self.db.transaction() as conn:
                    conn.execute(update(documents).where(documents.c.id == doc["id"]).values(index_state="INDEXED"))
            else:
                result = await self._request(scope, "POST", "/api/v1/content/reindex", json={
                    "uri": uri, "mode": "vectors_only", "wait": False, "recursive": False})
                if result.get("status") != "accepted" or not result.get("task_id"):
                    raise MemoryBackendError("indexing did not accept a tracked task")
                with self.db.transaction() as conn:
                    conn.execute(update(documents).where(documents.c.id == doc["id"]).values(index_task_id=result["task_id"]))
                return
        self._update(row["id"], state="INDEXED", error_type=None)
        self._watermarks(row)

    def _change(self, conn, scope, operation_id, epoch, kind, refs):
        conn.execute(sqlite_insert(changes).values(id=stable_id(operation_id, kind), scope_id=scope.key,
            operation_id=operation_id, epoch=epoch, kind=kind, refs_json=json.dumps(refs),
            created_at=self._now()).on_conflict_do_nothing())

    def _register_mutation(self, scope, kind, key, refs, payload_hash):
        owner = self._owner(scope)
        if owner["state"] == "RESTORE_REQUIRED":
            raise MemoryBackendError("restored memory requires remote journal reconciliation")
        operation_id = stable_id("memory-mutation", scope.key, key)
        with self.db.transaction() as conn:
            old = conn.execute(select(operations).where(operations.c.id == operation_id)).mappings().first()
            if old:
                if old["kind"] != kind or old["payload_hash"] != payload_hash:
                    raise ValueError("conflicting replay of a memory mutation")
                return operation_id
            # Revoke original evidence, not just one vector hit or display summary.
            source_ids = set(refs.get("source_ids", ()))
            uris = set(refs.get("uris", ()))
            uri_sources = set(conn.execute(select(derivations.c.source_id).where(
                derivations.c.scope_id == scope.key, derivations.c.uri.in_(uris),
                derivations.c.active == 1)).scalars())
            shared_sources = set(conn.execute(select(derivations.c.source_id).where(
                derivations.c.scope_id == scope.key, derivations.c.source_id.in_(uri_sources),
                derivations.c.uri.not_in(uris), derivations.c.active == 1)).scalars())
            source_ids.update(uri_sources - shared_sources)
            frozen_sources = shared_sources - source_ids
            if frozen_sources:
                # Legacy batch attribution can be ambiguous. Preserve sibling facts and stop source replay.
                conn.execute(update(sources).where(sources.c.scope_id == scope.key,
                    sources.c.source_id.in_(frozen_sources)).values(state="FROZEN"))
            if source_ids:
                uris.update(conn.execute(select(derivations.c.uri).where(
                    derivations.c.scope_id == scope.key, derivations.c.source_id.in_(source_ids),
                    derivations.c.active == 1)).scalars())
            if kind == "CORRECT" and (source_ids | frozen_sources).intersection(refs["correction"]["origins"]):
                raise ValueError("an owner correction needs a new original source identity")
            refs = {**refs, "explicit_source_ids": refs.get("source_ids", []),
                    "requested_uris": refs.get("uris", []), "source_ids": sorted(source_ids), "uris": sorted(uris),
                    "frozen_source_ids": sorted(frozen_sources)}
            epoch = owner["epoch"] + 1
            conn.execute(update(owners).where(owners.c.id == scope.key).values(
                epoch=epoch, state="MUTATING", last_error=None, updated_at=self._now()))
            for source_id in source_ids:
                put(conn, barriers, stable_id(operation_id, "source", source_id), {
                    "scope_id": scope.key, "epoch": epoch, "operation_id": operation_id,
                    "source_id": source_id, "uri": None, "allowed_hash": None, "created_at": self._now()})
            for uri in uris:
                put(conn, barriers, stable_id(operation_id, "uri", uri), {
                    "scope_id": scope.key, "epoch": epoch, "operation_id": operation_id,
                    "source_id": None, "uri": uri, "allowed_hash": None, "created_at": self._now()})
            conn.execute(insert(operations).values(id=operation_id, scope_id=scope.key, kind=kind,
                epoch=epoch, state="QUEUED", refs_json=json.dumps(refs), payload_hash=payload_hash,
                result_json="{}", created_at=self._now(), updated_at=self._now()))
            self._change(conn, scope, operation_id, epoch, "barrier", refs={
                "uris": sorted(uris), "source_event_ids": list(affected_sources(refs)),
                "clean_session_required": True})
        return operation_id

    async def correct(self, request: MemoryCorrection):
        self._valid_uri(request.scope, request.uri)
        if (request.source.role != "user" or request.source.kind != "original"
                or not request.source.content.strip() or _SECRET.search(request.source.content)):
            raise ValueError("memory correction requires an original, non-secret owner statement")
        with self.db.engine.connect() as conn:
            if not conn.execute(select(documents.c.id).where(documents.c.scope_id == request.scope.key,
                    documents.c.uri == request.uri)).first():
                raise LookupError("correction target must be a known scoped memory")
        refs = {"uris": [request.uri], "source_ids": [], "correct_uri": request.uri,
                "session_id": request.session_id, "branch": request.branch,
                "projection_version": request.projection_version,
                "correction": self._source_ref(request.source)}
        operation_id = self._register_mutation(request.scope, "CORRECT", request.idempotency_key,
                                                refs, digest(request.model_dump(mode="json")))
        return self._mutation_receipt(self._row(operation_id))

    async def forget(self, request: MemoryDeletion):
        for uri in request.uris:
            self._valid_uri(request.scope, uri)
        refs = {"uris": list(request.uris), "source_ids": list(request.source_event_ids),
                "delete_archives": request.delete_archives}
        operation_id = self._register_mutation(request.scope, "FORGET", request.idempotency_key,
                                                refs, digest(request.model_dump(mode="json")))
        return self._mutation_receipt(self._row(operation_id))

    async def revoke_source(self, scope, source_id, *, idempotency_key):
        return await self.forget(MemoryDeletion(scope=scope, source_event_ids=(source_id,),
                                               idempotency_key=idempotency_key))

    def _mutation_receipt(self, row):
        refs = json.loads(row["refs_json"])
        return MutationReceipt(operation_id=row["id"], state=row["state"],
            completed=row["state"] == "APPLIED", memory_epoch=row["epoch"],
            changed_uris=tuple(refs.get("uris", ())), affected_source_ids=affected_sources(refs),
            error_type=row["error_type"])

    async def _settle_obsolete(self, scope, row):
        if row["state"] in {"QUEUED", "WAITING_SOURCE", "SKIPPED", "INDEXED", "EXTRACTED"}:
            self._update(row["id"], state="SUPERSEDED", error_type=None)
            return True
        if row["state"] == "APPENDING":
            # An unknown, non-idempotent append must be observed before its archive is removed.
            raw = await self._request(scope, "GET", "/api/v1/content/read", params={
                "uri": f"viking://user/{self._binding(scope).user_id}/sessions/{row['remote_session_id']}/messages.jsonl",
                "raw": "true"})
            refs = json.loads(row["refs_json"])
            expected = [item for item in refs["events"] if item["id"] in refs["eligible_ids"]]
            if not self._messages_match([json.loads(line) for line in raw.splitlines() if line.strip()], expected):
                raise MemoryBackendError("revocation awaits the unknown append receipt")
            self._update(row["id"], state="EXPORTED")
            row = self._row(row["id"])
        if row["state"] == "EXPORTED":
            # Automatic commit is disabled; no extraction could have been dispatched here.
            await self._delete_remote_session(scope, row["remote_session_id"])
            self._update(row["id"], state="SUPERSEDED", error_type=None)
            return True
        if row["state"] == "COMMITTING":
            tasks = await self._request(scope, "GET", "/api/v1/tasks", params={
                "task_type": "session_commit", "resource_id": row["remote_session_id"], "limit": 200})
            matching = [task for task in tasks if task.get("resource_id") == row["remote_session_id"]
                        and task.get("task_type") == "session_commit"]
            if len(matching) != 1:
                raise MemoryBackendError("revocation awaits the unknown extraction receipt")
            self._update(row["id"], state="ACCEPTED", remote_task_id=matching[0]["task_id"],
                archive_uri=f"viking://user/{self._binding(scope).user_id}/sessions/{row['remote_session_id']}/history/archive_001")
            row = self._row(row["id"])
        if row["state"] == "ACCEPTED":
            # Let the existing extraction settle and record its complete diff; cancelling
            # midway could leave applied facts with no attributable change receipt.
            await self._poll_extraction(scope, row)
            row = self._row(row["id"])
            if row["state"] == "EXTRACTED":
                self._update(row["id"], state="SUPERSEDED", error_type=None)
                return True
            if row["state"] == "FAILED":
                raise MemoryBackendError("failed extraction needs partial-write reconciliation before mutation")
            return False
        if row["state"] == "FAILED":
            raise MemoryBackendError("failed extraction needs partial-write reconciliation before mutation")
        return row["state"] == "SUPERSEDED"

    async def _delete_remote_session(self, scope, session_id):
        try:
            await self._request(scope, "DELETE", "/api/v1/sessions/" + quote(session_id, safe=""))
        except RemoteMissing:
            pass

    async def _correction_event(self, scope, refs, correction):
        expected = refs["correction"]
        if correction is not None and correction.source.id == expected["id"]:
            event = correction.source
        else:
            if not self.source_loader:
                raise MemoryBackendError("original owner correction export required for recovery")
            batch = self.source_loader(scope, refs["session_id"], expected["seq"] - 1, expected["seq"],
                                       refs["branch"], refs["projection_version"])
            if inspect.isawaitable(batch):
                batch = await batch
            if not isinstance(batch, SourceBatch) or batch.scope != scope:
                raise PermissionError("correction source export scope mismatch")
            event = next((event for event in batch.events if event.id == expected["id"]), None)
        if event is None or self._source_ref(event) != expected:
            raise ValueError("original correction no longer matches its accepted revision")
        return event

    async def _apply_mutation(self, scope, row, correction=None):
        with self.db.engine.connect() as conn:
            old = [dict(item) for item in conn.execute(select(operations).where(
                operations.c.scope_id == scope.key, operations.c.kind == "SYNC", operations.c.epoch < row["epoch"],
                operations.c.state.not_in({"SUPERSEDED", "SKIPPED", "INDEXED", "EXTRACTED"}))) .mappings()]
        for previous in old:
            if not await self._settle_obsolete(scope, previous):
                self._update(row["id"], state="WAITING_EXTRACTION")
                return
        refs = json.loads(row["refs_json"])
        # A batch may have produced new derived URIs after the barrier was registered.
        with self.db.transaction() as conn:
            if "requested_uris" in refs:
                explicit = set(refs["explicit_source_ids"])
                candidates = set(conn.execute(select(derivations.c.source_id).where(
                    derivations.c.scope_id == scope.key, derivations.c.uri.in_(refs["requested_uris"]),
                    derivations.c.active == 1)).scalars())
                shared = set(conn.execute(select(derivations.c.source_id).where(
                    derivations.c.scope_id == scope.key, derivations.c.source_id.in_(candidates),
                    derivations.c.uri.not_in(refs["requested_uris"]), derivations.c.active == 1)).scalars()) - explicit
                refs["source_ids"] = sorted(explicit | (candidates - shared))
                refs["frozen_source_ids"] = sorted(shared | set(refs.get("frozen_source_ids", ())))
                conn.execute(barriers.delete().where(barriers.c.operation_id == row["id"],
                    barriers.c.source_id.in_(shared)))
                if shared:
                    conn.execute(update(sources).where(sources.c.scope_id == scope.key,
                        sources.c.source_id.in_(shared)).values(state="FROZEN"))
            uris = set(refs["uris"])
            if refs["source_ids"]:
                uris.update(conn.execute(select(derivations.c.uri).where(
                    derivations.c.scope_id == scope.key, derivations.c.source_id.in_(refs["source_ids"]),
                    derivations.c.active == 1)).scalars())
            for source_id in refs["source_ids"]:
                put(conn, barriers, stable_id(row["id"], "source", source_id), {
                    "scope_id": scope.key, "epoch": row["epoch"], "operation_id": row["id"],
                    "source_id": source_id, "uri": None, "allowed_hash": None, "created_at": self._now()})
            for uri in uris:
                put(conn, barriers, stable_id(row["id"], "uri", uri), {
                    "scope_id": scope.key, "epoch": row["epoch"], "operation_id": row["id"],
                    "source_id": None, "uri": uri, "allowed_hash": None, "created_at": self._now()})
            refs["uris"] = sorted(uris)
            conn.execute(update(operations).where(operations.c.id == row["id"]).values(
                refs_json=json.dumps(refs), state="APPLYING", updated_at=self._now()))
        correct_uri = refs.get("correct_uri")
        event = await self._correction_event(scope, refs, correction) if correct_uri else None
        for uri in refs["uris"]:
            self._valid_uri(scope, uri)
            if uri == correct_uri:
                await self._replace(scope, uri, event.content)
            else:
                try:
                    await self._request(scope, "DELETE", "/api/v1/fs", params={
                        "uri": uri, "recursive": "false", "wait": "true"})
                except RemoteMissing:
                    pass
                try:
                    await self._request(scope, "GET", "/api/v1/content/read", params={"uri": uri, "raw": "true"})
                except RemoteMissing:
                    pass
                else:
                    raise MemoryBackendError("memory removal has not been confirmed")
        # Remove extraction inputs as well as facts; deleting a session alone never suffices.
        with self.db.engine.connect() as conn:
            archived = conn.execute(select(operations.c.remote_session_id).where(
                operations.c.scope_id == scope.key, operations.c.remote_session_id.is_not(None),
                operations.c.id.in_(select(sources.c.operation_id).where(
                    sources.c.scope_id == scope.key,
                    sources.c.source_id.in_([*refs["source_ids"], *refs.get("frozen_source_ids", ())]))))) .scalars().all()
        for session_id in dict.fromkeys(archived):
            await self._delete_remote_session(scope, session_id)
        with self.db.transaction() as conn:
            protected_sources = select(sources.c.source_id).where(sources.c.scope_id == scope.key,
                sources.c.state.in_({"FROZEN", "REVOKED"}))
            rebuild_candidates = conn.execute(select(derivations.c.source_id).where(
                derivations.c.scope_id == scope.key, derivations.c.uri.in_(refs["uris"]),
                derivations.c.active == 1, derivations.c.source_id.not_in(protected_sources))).scalars()
            unaffected = set(rebuild_candidates) - set(refs["source_ids"]) - set(refs.get("frozen_source_ids", ()))
            if unaffected:
                conn.execute(update(sources).where(sources.c.scope_id == scope.key,
                    sources.c.source_id.in_(unaffected), sources.c.state.not_in({"FROZEN", "REVOKED"})).values(state="REBUILD"))
            if refs["source_ids"]:
                conn.execute(update(sources).where(sources.c.scope_id == scope.key,
                    sources.c.source_id.in_(refs["source_ids"])).values(state="REVOKED"))
            conn.execute(update(derivations).where(derivations.c.scope_id == scope.key,
                derivations.c.uri.in_(refs["uris"])).values(active=0))
            for uri in refs["uris"]:
                content_hash = digest(event.content.strip()) if uri == correct_uri else ""
                put(conn, documents, stable_id(scope.key, uri), {"scope_id": scope.key, "uri": uri,
                    "content_hash": content_hash, "epoch": row["epoch"],
                    "state": "ACTIVE" if uri == correct_uri else "DELETED",
                    "index_state": "INDEXED" if uri == correct_uri else "DELETED",
                    "index_task_id": None, "updated_at": self._now()})
                if uri == correct_uri:
                    conn.execute(update(barriers).where(barriers.c.scope_id == scope.key,
                        barriers.c.uri == uri).values(allowed_hash=content_hash))
            if event:
                for source_id in event.origins:
                    put(conn, sources, stable_id(scope.key, source_id, event.revision), {
                        "scope_id": scope.key, "source_id": source_id, "revision": event.revision, "event_id": event.id,
                        "operation_id": row["id"], "content_hash": digest(event.content), "state": "EXTRACTED"})
                    put(conn, derivations, stable_id(scope.key, correct_uri, source_id, row["id"]), {
                        "scope_id": scope.key, "uri": correct_uri, "source_id": source_id, "operation_id": row["id"], "active": 1})
            conn.execute(update(operations).where(operations.c.id == row["id"]).values(
                state="APPLIED", error_type=None, updated_at=self._now()))
            other = conn.execute(select(operations.c.id).where(operations.c.scope_id == scope.key,
                operations.c.kind != "SYNC", operations.c.id != row["id"],
                operations.c.state != "APPLIED")).first()
            conn.execute(update(owners).where(owners.c.id == scope.key).values(
                state="MUTATING" if other else "READY", last_error=None, updated_at=self._now()))
            self._change(conn, scope, row["id"], row["epoch"], "corrected" if event else "forgotten", {
                "uris": refs["uris"], "source_event_ids": list(affected_sources(refs)),
                "replacement_source_ids": list(event.origins) if event else [],
                "rebuild_source_ids": sorted(unaffected), "delete_archives": refs.get("delete_archives", False)})

    async def _replace(self, scope, uri, content):
        wanted_hash = digest(content.strip())
        current_visible = await self._request(scope, "GET", "/api/v1/content/read", params={"uri": uri})
        current = await self._request(scope, "GET", "/api/v1/content/read", params={"uri": uri, "raw": "true"})
        if digest(current_visible.strip()) != wanted_hash:
            result = await self._request(scope, "POST", "/api/v1/content/batch-write", json={
                "root_uri": uri.rsplit("/", 1)[0], "wait": True,
                "processing_mode": "vectors_only",
                "operations": [{"uri": uri, "content": content, "mode": "replace",
                                "expected_sha256": digest(current)}]})
            if result.get("conflicts") or result.get("semantic_status") in {"failed", "partial"}:
                raise MemoryBackendError("memory correction or indexing failed")
        observed = await self._request(scope, "GET", "/api/v1/content/read", params={"uri": uri})
        if digest(observed.strip()) != wanted_hash:
            raise MemoryBackendError("memory correction has no matching remote content receipt")
        # A previously timed-out write may have persisted text but not its indexing receipt.
        result = await self._request(scope, "POST", "/api/v1/content/reindex", json={
            "uri": uri, "mode": "vectors_only", "wait": True, "recursive": False})
        if result.get("status") != "completed" or result.get("failed_records", 0):
            raise MemoryBackendError("memory correction indexing is not complete")

    async def poll(self, operation_id):
        row = self._row(operation_id)
        with self.db.engine.connect() as conn:
            scope = MemoryScope.model_validate_json(conn.execute(select(owners.c.scope_json)
                .where(owners.c.id == row["scope_id"])).scalar_one())
        self._owner(scope)
        async with self.locks[scope.owner_id]:
            await self._drive(scope)
        row = self._row(operation_id)
        return self._sync_receipt(row) if row["kind"] == "SYNC" else self._mutation_receipt(row)

    def _visible(self, conn, scope, uri):
        row = conn.execute(select(documents).where(documents.c.scope_id == scope.key,
            documents.c.uri == uri, documents.c.state == "ACTIVE")).mappings().first()
        if not row:
            return None
        original_ids = tuple(dict.fromkeys(conn.execute(select(derivations.c.source_id).where(
            derivations.c.scope_id == scope.key, derivations.c.uri == uri, derivations.c.active == 1)).scalars()))
        if not original_ids or conn.execute(select(barriers.c.id).where(
                barriers.c.scope_id == scope.key, barriers.c.source_id.in_(original_ids))).first():
            return None
        uri_barrier = conn.execute(select(barriers).where(barriers.c.scope_id == scope.key,
            barriers.c.uri == uri).order_by(barriers.c.epoch.desc()).limit(1)).mappings().first()
        if uri_barrier and row["content_hash"] != uri_barrier["allowed_hash"]:
            return None
        return {**row, "source_event_ids": original_ids}

    async def read(self, scope, uri):
        self._valid_uri(scope, uri)
        owner = self._owner(scope)
        if owner["state"] != "READY":
            raise MemoryBackendError("memory recall is blocked while a mutation is unresolved")
        with self.db.engine.connect() as conn:
            row = self._visible(conn, scope, uri)
        if not row:
            raise PermissionError("memory document is not currently visible in this scope")
        content = await self._request(scope, "GET", "/api/v1/content/read", params={"uri": uri})
        if not isinstance(content, str) or digest(content.strip()) != row["content_hash"]:
            raise MemoryBackendError("remote memory revision changed without an accepted change receipt")
        if len(content) > self.config.recall_max_chars or _SECRET.search(content):
            raise MemoryBackendError("memory document exceeds the safe recall budget")
        # Revalidate after network I/O: forget can install a barrier while read is awaiting HTTP.
        latest = self._owner(scope)
        with self.db.engine.connect() as conn:
            current = self._visible(conn, scope, uri)
        if latest["epoch"] != owner["epoch"] or latest["state"] != "READY" or not current or current["content_hash"] != row["content_hash"]:
            raise PermissionError("memory visibility changed during retrieval")
        return MemoryDocument(uri=uri, content=content, source_event_ids=row["source_event_ids"],
            revision=row["content_hash"], updated_at=row["updated_at"], memory_epoch=owner["epoch"])

    async def search(self, scope, query, *, limit=8):
        if not self.enabled(scope):
            return MemorySearchResult(status="disabled")
        if (not isinstance(query, str) or not query.strip() or len(query) > self.config.max_query_chars
                or _SECRET.search(query)):
            raise ValueError("memory search requires a bounded, nonempty query")
        if type(limit) is not int or not 1 <= limit <= 16:
            raise ValueError("memory search limit must be 1..16")
        owner = self._owner(scope)
        if owner["state"] != "READY":
            return MemorySearchResult(status="blocked", memory_epoch=owner["epoch"])
        try:
            result = await self._request(scope, "POST", "/api/v1/search/find", json={
                "query": query, "target_uri": self._binding(scope).root_uri + "/",
                "limit": limit * 2, "context_type": "memory", "include_provenance": True})
            hits, seen = [], set()
            for item in result.get("memories", []):
                uri = item.get("uri")
                try:
                    self._valid_uri(scope, uri)
                except PermissionError:
                    continue
                if uri in seen:
                    continue
                with self.db.engine.connect() as conn:
                    row = self._visible(conn, scope, uri)
                if not row or row["index_state"] != "INDEXED":
                    continue
                # Search yields references, not unvalidated remote abstracts or instructions.
                score = item.get("score", 0)
                score = float(score) if isinstance(score, (int, float)) and math.isfinite(score) else 0.0
                hits.append(MemoryHit(uri=uri, score=score, source_event_ids=row["source_event_ids"],
                    revision=row["content_hash"], updated_at=row["updated_at"]))
                seen.add(uri)
                if len(hits) >= limit:
                    break
            latest = self._owner(scope)
            if latest["epoch"] != owner["epoch"] or latest["state"] != "READY":
                return MemorySearchResult(status="blocked", memory_epoch=latest["epoch"])
            return MemorySearchResult(status="ready", hits=tuple(hits), memory_epoch=owner["epoch"])
        except BACKEND_ERRORS as error:
            return MemorySearchResult(status="degraded", memory_epoch=owner["epoch"], error_type=type(error).__name__)

    async def status(self, scope):
        if not self.enabled(scope):
            return {"state": "disabled", "pending": 0, "memory_epoch": 0, "streams": []}
        owner = self._owner(scope)
        with self.db.engine.connect() as conn:
            pending = conn.execute(select(operations.c.id).where(operations.c.scope_id == scope.key,
                operations.c.state.not_in(TERMINAL - {"EXTRACTED"}))).scalars().all()
            watermarks = [dict(row) for row in conn.execute(select(streams).where(
                streams.c.scope_id == scope.key, streams.c.epoch == owner["epoch"])).mappings()]
        return {"state": owner["state"], "memory_epoch": owner["epoch"], "pending": len(pending),
                "pending_operation_ids": pending, "last_error": owner["last_error"], "streams": watermarks,
                "backend": "openviking", "local_fact_store": False}

    async def reconcile(self):
        with self.db.engine.connect() as conn:
            scopes = [MemoryScope.model_validate_json(raw) for raw in conn.execute(select(owners.c.scope_json)).scalars()]
        for scope in scopes:
            try:
                async with self.locks[scope.owner_id]:
                    await self._drive(scope)
            except (PermissionError, *BACKEND_ERRORS):
                continue  # One revoked/offline owner cannot starve other owners' synchronization.

    async def reconcile_restore(self, scope):
        """Confirm remote quiescence and restored fences using read-only backend calls."""
        async with self.locks[scope.owner_id]:
            owner = self._owner(scope)
            if owner["state"] != "RESTORE_REQUIRED":
                return await self.status(scope)
            await self.http.verify(self._binding(scope))
            # Query each active state separately so a truncated history cannot hide active work.
            for state in ("pending", "running", "cancelling"):
                active = await self._request(scope, "GET", "/api/v1/tasks", params={
                    "status": state, "include_internal": "true", "limit": 1})
                if not isinstance(active, list) or active:
                    raise MemoryBackendError("restored memory awaits remote task quiescence")
            with self.db.engine.connect() as conn:
                pending = [dict(row) for row in conn.execute(select(operations).where(
                    operations.c.scope_id == scope.key,
                    operations.c.state.not_in(TERMINAL - {"FAILED", "EXTRACTED"}))).mappings()]
            for row in pending:
                if row["kind"] != "SYNC" or row["state"] == "FAILED":
                    raise MemoryBackendError("restored mutation or partial failure needs journal repair")
                if row["state"] in {"COMMITTING", "ACCEPTED"}:
                    tasks = await self._request(scope, "GET", "/api/v1/tasks", params={
                        "task_type": "session_commit", "resource_id": row["remote_session_id"], "limit": 200})
                    if (not isinstance(tasks, list) or len(tasks) != 1
                            or tasks[0].get("resource_id") != row["remote_session_id"]
                            or tasks[0].get("task_type") != "session_commit"
                            or tasks[0].get("status") != "completed"):
                        raise MemoryBackendError("restored extraction has no confirmed terminal receipt")
                    self._update(row["id"], state="ACCEPTED", remote_task_id=tasks[0]["task_id"],
                        archive_uri=f"viking://user/{self._binding(scope).user_id}/sessions/{row['remote_session_id']}/history/archive_001")
                    await self._poll_extraction(scope, self._row(row["id"]))
                    if self._row(row["id"])["state"] != "EXTRACTED":
                        raise MemoryBackendError("restored extraction has no complete change receipt")
            with self.db.engine.connect() as conn:
                fences = [dict(row) for row in conn.execute(select(barriers).where(
                    barriers.c.scope_id == scope.key, barriers.c.uri.is_not(None))
                    .order_by(barriers.c.epoch.desc())).mappings()]
            checked = set()
            for fence in fences:
                uri = fence["uri"]
                if uri in checked:
                    continue
                checked.add(uri)
                try:
                    content = await self._request(scope, "GET", "/api/v1/content/read", params={"uri": uri})
                except RemoteMissing:
                    if fence["allowed_hash"] is not None:
                        raise MemoryBackendError("restored correction is missing remotely") from None
                else:
                    if fence["allowed_hash"] is None or digest(content.strip()) != fence["allowed_hash"]:
                        raise MemoryBackendError("remote memory disagrees with restored deletion journal")
            with self.db.transaction() as conn:
                if pending:
                    conn.execute(update(operations).where(operations.c.id.in_([row["id"] for row in pending]))
                        .values(state="SUPERSEDED", error_type=None, updated_at=self._now()))
                conn.execute(update(owners).where(owners.c.id == scope.key, owners.c.epoch == owner["epoch"],
                    owners.c.state == "RESTORE_REQUIRED").values(state="READY", last_error=None, updated_at=self._now()))
            return await self.status(scope)

    def changes_since(self, scope, after=0):
        self._owner(scope)
        with self.db.engine.connect() as conn:
            return [{**dict(row), "refs": json.loads(row["refs_json"])} for row in conn.execute(
                select(changes).where(changes.c.scope_id == scope.key, changes.c.seq > after)
                .order_by(changes.c.seq).limit(128)).mappings()]

    def export_barriers(self, scope):
        owner = self._owner(scope)
        with self.db.engine.connect() as conn:
            return {"scope": scope.model_dump(), "memory_epoch": owner["epoch"],
                    "barriers": [dict(row) for row in conn.execute(select(barriers).where(
                        barriers.c.scope_id == scope.key)).mappings()],
                    "frozen_sources": [dict(row) for row in conn.execute(select(sources).where(
                        sources.c.scope_id == scope.key, sources.c.state == "FROZEN")).mappings()]}

    def restore_barriers(self, scope, manifest):
        """Apply the newest deletion journal before opening recall on an old backup."""
        owner = self._owner(scope)
        if manifest.get("scope") != scope.model_dump() or type(manifest.get("memory_epoch")) is not int:
            raise ValueError("deletion journal scope or epoch mismatch")
        if manifest["memory_epoch"] < owner["epoch"]:
            raise ValueError("deletion journal is older than the restored database")
        with self.db.transaction() as conn:
            for item in manifest.get("frozen_sources", []):
                if (set(item) != set(sources.c.keys()) or item.get("scope_id") != scope.key
                        or item.get("state") != "FROZEN"):
                    raise ValueError("invalid frozen source receipt")
                put(conn, sources, item["id"], {key: value for key, value in item.items() if key != "id"})
            for item in manifest.get("barriers", []):
                if item.get("scope_id") != scope.key or item.get("epoch", -1) > manifest["memory_epoch"]:
                    raise ValueError("invalid deletion journal entry")
                if item.get("uri"):
                    self._valid_uri(scope, item["uri"])
                item["id"]
                conn.execute(sqlite_insert(barriers).values(**item).on_conflict_do_nothing(index_elements=[barriers.c.id]))
                self._change(conn, scope, item["operation_id"], item["epoch"], "restored_barrier", {
                    "uris": [item["uri"]] if item.get("uri") else [],
                    "source_event_ids": [item["source_id"]] if item.get("source_id") else [],
                    "clean_session_required": True})
            conn.execute(update(owners).where(owners.c.id == scope.key).values(
                epoch=manifest["memory_epoch"], state="RESTORE_REQUIRED", updated_at=self._now()))
        # A caller must explicitly reconcile the remote mutation journal; old pending writes stay fenced.

    async def close(self):
        await self.http.close()
