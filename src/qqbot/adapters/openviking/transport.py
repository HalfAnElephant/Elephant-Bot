"""Bounded HTTP for the pinned OpenViking v0.5.4.1 session/content API."""
import hashlib
import json
import os
import time

import httpx


class MemoryBackendError(RuntimeError):
    pass


class RemoteMissing(MemoryBackendError):
    pass


class OpenVikingHTTP:
    def __init__(self, config, *, client=None):
        self.config = config
        self.client = client or httpx.AsyncClient(follow_redirects=False, trust_env=False)
        self.owns_client = client is None
        self.identities = {}

    def headers(self, binding):
        key = os.environ.get(binding.api_key_env, "")
        if not key:
            raise MemoryBackendError("OpenViking USER credential unavailable")
        return {"X-API-Key": key, "X-OpenViking-Account": binding.account_id,
                "X-OpenViking-User": binding.user_id}

    async def _request(self, binding, method, path, **kwargs):
        if not self.config.endpoint:
            raise MemoryBackendError("OpenViking endpoint unavailable")
        async with self.client.stream(
            method, self.config.endpoint.rstrip("/") + path, headers=self.headers(binding),
            timeout=self.config.timeout_seconds, follow_redirects=False, **kwargs,
        ) as response:
            if response.status_code == 404:
                raise RemoteMissing("OpenViking object not found")
            response.raise_for_status()
            content = bytearray()
            async for chunk in response.aiter_bytes():
                content.extend(chunk)
                if len(content) > 4 * 1024 * 1024:
                    raise MemoryBackendError("OpenViking response exceeds bound")
        data = json.loads(content)
        if not isinstance(data, dict) or data.get("status") != "ok":
            raise MemoryBackendError("OpenViking returned an unsuccessful response")
        return data

    async def verify(self, binding):
        credential_hash = hashlib.sha256(self.headers(binding)["X-API-Key"].encode()).hexdigest()
        key = (binding.account_id, binding.user_id, credential_hash)
        if self.identities.get(key, 0) > time.monotonic():
            return
        health = await self._request(binding, "GET", "/health")
        if (health.get("version") != self.config.expected_version or health.get("auth_mode") != "api_key"
                or health.get("role") != "user" or health.get("account_id") != binding.account_id
                or health.get("user_id") != binding.user_id):
            raise MemoryBackendError("OpenViking version or USER identity contract mismatch")
        self.identities[key] = time.monotonic() + 30

    async def request(self, binding, method, path, **kwargs):
        await self.verify(binding)
        return (await self._request(binding, method, path, **kwargs)).get("result")

    async def close(self):
        if self.owns_client:
            await self.client.aclose()
