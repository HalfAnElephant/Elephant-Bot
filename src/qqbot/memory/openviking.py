"""Bounded OpenViking snapshots, with SQLite remaining the source of authority.

Only already-authorized history and active records are exported. Search returns IDs,
never trusted remote prose. Each snapshot replaces its predecessor; no background
memory extraction is enabled, so a deletion can remove all exported descendants.
"""
import asyncio
import hashlib
import json
import os
from datetime import timedelta

import httpx
from sqlalchemy import delete, insert, select, update

from qqbot.domain import AccessContext
from qqbot.memory import _safe
from qqbot.storage.openviking_schema import openviking_scopes


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


class OpenVikingMemory:
    def __init__(self, db, clock, settings, social, permissions, validate, *, client=None):
        self.db, self.clock, self.settings = db, clock, settings
        self.social, self.permissions, self.validate = social, permissions, validate
        self.client = client or httpx.AsyncClient(follow_redirects=False, trust_env=False)
        self._owns_client = client is None
        self.status = "not_configured"
        self.lock = asyncio.Lock()
        self._cleanup_cursor = ""

    @property
    def config(self):
        return self.settings.memory.openviking

    def enabled(self, access):
        if not self.config.endpoint:
            return False
        if access.conversation.surface.value == "private":
            return self.config.private_enabled
        return access.conversation.surface.value == "group" and self.config.group_enabled

    def authorized(self, access):
        resource = "conversation:" + access.conversation.key
        return all(self.permissions.check(access, action, resource).allowed for action in (
            "conversation.receive", "history.read", "memory.read", "memory.external.write",
            "memory.external.read",
        ))

    def binding(self):
        # Binding omits secrets, but changes on endpoint/deployment/namespace changes.
        return digest([self.config.endpoint, self.settings.deployment_id, self.config.namespace])

    def scope_id(self, access):
        return digest([self.binding(), access.conversation.key, access.principal.key])

    def prefix(self, scope):
        deployment = digest(self.settings.deployment_id)[:16]
        return f"viking://resources/{self.config.namespace}-{deployment}/{scope}"

    async def request(self, method, path, *, access=None, context=None, **kwargs):
        if access is not None and (not self.enabled(access) or not self.authorized(access)
                                  or context.get("permission_version") != self.permissions.policy_version
                                  or not self.validate(access, context)):
            raise PermissionError("OpenViking scope no longer authorized")
        if access is not None and "exported_ids" in context:
            available = {r.memory_id for r in self.social.accessible_records(access)}
            if not set(context["exported_ids"]).issubset(available):
                raise PermissionError("OpenViking exported memory no longer authorized")
        headers = {}
        key = os.environ.get(self.config.api_key_env)
        if key:
            headers["X-API-Key"] = key
        async with self.client.stream(
            method, self.config.endpoint.rstrip("/") + path, headers=headers,
            timeout=self.config.timeout_seconds, follow_redirects=False, **kwargs,
        ) as response:
            if response.status_code == 404 and method == "DELETE":
                return {}
            if response.status_code == 409 and path == "/api/v1/fs/mkdir":
                return {}
            response.raise_for_status()
            content = bytearray()
            async for chunk in response.aiter_bytes():
                if len(content) + len(chunk) > 512 * 1024:
                    raise ValueError("OpenViking response too large")
                content.extend(chunk)
        body = json.loads(content)
        if not isinstance(body, dict) or body.get("status") != "ok":
            raise ValueError("OpenViking response unsuccessful")
        result = body.get("result", {})
        if not isinstance(result, dict):
            raise TypeError("invalid OpenViking result")
        return result

    def _saved(self, scope):
        with self.db.engine.connect() as conn:
            return conn.execute(select(openviking_scopes).where(
                openviking_scopes.c.id == scope)).mappings().first()

    async def clear(self, row):
        # Delete only our exact known snapshot. Keep ledger on uncertain deletion.
        if row["binding"] != self.binding():
            return False
        await self.request("DELETE", "/api/v1/sessions/" + row["generation"])
        await self.request("DELETE", "/api/v1/fs", params={
            "uri": self.prefix(row["id"]), "recursive": "true",
        })
        with self.db.transaction() as conn:
            conn.execute(delete(openviking_scopes).where(openviking_scopes.c.id == row["id"]))
        return True

    async def prepare(self, access, turn, context):
        cache_key = (access.conversation.key, access.principal.key, turn.text)
        self.social.external_rankings.pop(cache_key, None)
        if not self.enabled(access) or not self.authorized(access):
            self.status = "disabled" if self.config.endpoint else "not_configured"
            return
        try:
            async with asyncio.timeout(self.config.operation_budget_seconds):
                async with self.lock:
                    await self._prepare(access, turn, context, cache_key)
        except (TimeoutError, httpx.HTTPError, ValueError, TypeError, KeyError, PermissionError):
            # Chat and authoritative local memory remain usable on backend failure.
            self.status = "unavailable_local_fallback"

    async def _prepare(self, access, turn, context, cache_key):
        if not self.validate(access, context):
            return
        query = turn.text[:self.config.max_query_chars]
        _safe(query)
        records = sorted(self.social.accessible_records(access), key=lambda r: r.memory_id)
        records = records[:self.config.max_records]
        for record in records:
            _safe(record.text + record.value_json)
        history = context.get("seed", {}).get("history", [])[-16:]
        # Selected history already contains the current turn when available. Preserve
        # coalesced fragments once using the turn's stable source set.
        source_set = set(turn.source_event_ids)
        messages = [{"role": item["role"], "content": item["text"][:4000]} for item in history
                    if item["role"] in {"user", "assistant"} and item["source"] not in source_set]
        messages.append({"role": "user", "content": query})
        for message in messages:
            _safe(message["content"])
        scope = self.scope_id(access)
        version, policy = self.social.version(access), self.permissions.policy_version
        generation = "elephant-" + digest([
            scope, version, policy, messages,
            [(r.memory_id, r.text, r.source_event_ids) for r in records],
        ])
        saved = self._saved(scope)
        if saved and (saved["generation"] != generation or saved["state"] != "READY"):
            await self.clear(saved)
            saved = None
        root = self.prefix(scope)
        uri_ids = {f"{root}/{digest(r.memory_id)}.md": r.memory_id for r in records}
        guard = {"access": access, "context": {
            **context, "exported_ids": [r.memory_id for r in records],
        }}
        if not saved:
            validation = {key: value for key, value in context.items() if key in {
                "scope", "source_manifest", "config_versions", "permission_version", "context_version",
                "persona_version", "policy_version", "invited", "control_response",
            }}
            validation["exported_ids"] = [r.memory_id for r in records]
            validation["external_permission_version"] = policy
            validation["turn"] = {"source_revision": context["turn"]["source_revision"]}
            # Store before network, so crash/timeout can be reconciled by deleting the
            # deterministic snapshot before retrying; never blindly append messages.
            with self.db.transaction() as conn:
                conn.execute(insert(openviking_scopes).values(
                    id=scope, binding=self.binding(), generation=generation, state="PENDING",
                    access_json=access.model_dump_json(), validation_json=json.dumps(validation),
                ))
            parent = root.rsplit("/", 1)[0]
            await self.request("POST", "/api/v1/fs/mkdir", json={"uri": parent}, **guard)
            await self.request("POST", "/api/v1/fs/mkdir", json={"uri": root}, **guard)
            if records:
                await self.request("POST", "/api/v1/content/batch-write", json={
                    "root_uri": root, "wait": False,
                    "operations": [{"uri": f"{root}/{digest(r.memory_id)}.md", "content": r.text,
                                    "mode": "upsert"} for r in records],
                }, **guard)
            await self.request("POST", "/api/v1/sessions", json={
                "session_id": generation,
                "memory_policy": {"self": {"enabled": False}, "peer": {"enabled": False},
                                  "working_memory": {"enabled": False}},
                "auto_commit_policy": None,
            }, **guard)
            await self.request("POST", f"/api/v1/sessions/{generation}/messages/batch",
                               json={"messages": messages}, **guard)
            with self.db.transaction() as conn:
                conn.execute(update(openviking_scopes).where(openviking_scopes.c.id == scope)
                             .values(state="READY"))
        result = await self.request("POST", "/api/v1/search/find", json={
            "query": query, "target_uri": root + "/", "limit": 8,
        }, **guard)
        ranked = []
        for item in result.get("resources", [])[:64]:
            if isinstance(item, dict) and item.get("uri") in uri_ids:
                memory_id = uri_ids[item["uri"]]
                if memory_id not in ranked:
                    ranked.append(memory_id)
        # The cache contains identifiers only. SocialService rechecks status, source
        # authorization, expiration and tombstones before returning any local text.
        if not self.validate(access, context) or not self.authorized(access):
            return
        self.social.external_rankings[cache_key] = {
            "ids": ranked, "version": version, "policy": policy,
            "expires_at": self.clock.now() + timedelta(seconds=30),
        }
        while len(self.social.external_rankings) > 128:
            self.social.external_rankings.pop(next(iter(self.social.external_rankings)))
        self.status = "ready"

    async def reconcile(self):
        try:
            async with asyncio.timeout(self.config.operation_budget_seconds):
                await self._reconcile()
        except (TimeoutError, httpx.HTTPError, ValueError, TypeError, KeyError, PermissionError):
            self.status = "cleanup_pending"

    async def _reconcile(self):
        """Delete revoked/forgotten snapshots, without re-exporting or regranting access."""
        if not self.config.endpoint:
            with self.db.engine.connect() as conn:
                if conn.execute(select(openviking_scopes.c.id).limit(1)).first():
                    self.status = "endpoint_removed_cleanup_required"
            return
        async with self.lock:
            with self.db.engine.connect() as conn:
                rows = conn.execute(select(openviking_scopes).where(
                    openviking_scopes.c.id > self._cleanup_cursor
                ).order_by(openviking_scopes.c.id).limit(128)).mappings().all()
            for row in rows:
                self._cleanup_cursor = row["id"]
                access = AccessContext.model_validate_json(row["access_json"])
                validation = json.loads(row["validation_json"])
                valid = (self.enabled(access) and self.authorized(access)
                         and validation.get("external_permission_version") == self.permissions.policy_version
                         and self.validate(access, validation))
                if valid:
                    available = {r.memory_id for r in self.social.accessible_records(access)}
                    valid = set(validation.get("exported_ids", ())).issubset(available)
                if not valid or row["state"] == "PENDING":
                    try:
                        async with asyncio.timeout(self.config.operation_budget_seconds):
                            if not await self.clear(row):
                                self.status = "old_endpoint_cleanup_required"
                    except (TimeoutError, httpx.HTTPError, ValueError, TypeError):
                        self.status = "cleanup_pending"

            if len(rows) < 128:
                self._cleanup_cursor = ""

    async def close(self):
        if self._owns_client:
            await self.client.aclose()
