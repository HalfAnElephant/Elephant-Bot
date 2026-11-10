"""Meter Core model stages without retaining prompts, credentials or responses."""

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from uuid import uuid4

import httpx
from sqlalchemy import insert

from qqbot.providers import normalize_usage
from qqbot.storage.schema import audit_events

logger = logging.getLogger(__name__)
model_scope = ContextVar('model_scope', default=None)


@contextmanager
def call_scope(**identity):
    token = model_scope.set(identity)
    try:
        yield
    finally:
        model_scope.reset(token)


class MeteredResponse:
    def __init__(self, response):
        self.response = response
        self.content = bytearray()
        self.complete = False

    def __getattr__(self, name):
        return getattr(self.response, name)

    async def aiter_bytes(self):
        async for chunk in self.response.aiter_bytes():
            if len(self.content) <= 1024 * 1024:
                self.content.extend(chunk)
            yield chunk
        self.complete = True

    async def aread(self):
        return b''.join([chunk async for chunk in self.aiter_bytes()])

    def json(self):
        if not self.complete or len(self.content) > 1024 * 1024:
            raise ValueError('usage response incomplete or too large')
        return json.loads(self.content)


class MeteredClient:
    def __init__(self, client, db, clock, phase='core', *, slots=None):
        self.client, self.db, self.clock, self.phase = client, db, clock, phase
        self.slots = slots or asyncio.Semaphore(1)

    def for_phase(self, phase):
        return MeteredClient(self.client, self.db, self.clock, phase, slots=self.slots)

    def record(self, started, kwargs, response, error, queue_ms):
        raw = None
        if response is not None and response.is_stream_consumed:
            try:
                raw = response.json().get('usage')
            except (ValueError, TypeError, AttributeError, httpx.ResponseNotRead):
                pass
        detail = {**(model_scope.get() or {}), 'phase': self.phase,
                  'model': kwargs.get('json', {}).get('model'),
                  'elapsed_ms': round((time.monotonic() - started) * 1000, 2),
                  'queue_ms': queue_ms,
                  'status': response.status_code if response is not None else None,
                  'error_type': type(error).__name__ if error else None,
                  'usage': normalize_usage(raw)}
        # Raw vendor usage may contain arbitrary fields. Keep only normalized counters.
        detail['usage'].pop('raw', None)
        try:
            with self.db.transaction() as c:
                c.execute(insert(audit_events).values(
                    id=str(uuid4()), kind='model.call', target=self.phase, actor='core',
                    created_at=self.clock.now().isoformat(), detail=json.dumps(detail)))
        except Exception:
            logger.exception('Core model metering failed')

    async def post(self, *args, **kwargs):
        started, response, error = time.monotonic(), None, None
        queue_ms = None
        try:
            async with self.slots:
                queue_ms = round((time.monotonic() - started) * 1000, 2)
                response = await self.client.post(*args, **kwargs)
            return response
        except BaseException as exc:
            error = exc
            raise
        finally:
            self.record(started, kwargs, response, error, queue_ms)

    @asynccontextmanager
    async def stream(self, *args, **kwargs):
        started, response, error = time.monotonic(), None, None
        queue_ms = None
        try:
            async with self.slots:
                queue_ms = round((time.monotonic() - started) * 1000, 2)
                async with self.client.stream(*args, **kwargs) as upstream:
                    response = MeteredResponse(upstream)
                    yield response
        except BaseException as exc:
            error = exc
            raise
        finally:
            self.record(started, kwargs, response, error, queue_ms)

    async def aclose(self):
        await self.client.aclose()

