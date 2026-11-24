"""Host wording never accepts new factual claims from a model."""

import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest
from sqlalchemy import select

from elephant.providers import ModelCapacity
from elephant.runtime.host_expression import render_expression
from elephant.storage.task_schema import model_requests

EVENT = SimpleNamespace(conversation=SimpleNamespace(key="conversation"))


def host_core(db, clock, client, *, allowed=True):
    return SimpleNamespace(db=db, clock=clock, model_client=client,
        scheduler=SimpleNamespace(capacity=ModelCapacity(total=2)), access=lambda _event: None,
        permissions=SimpleNamespace(check=lambda *_args: SimpleNamespace(allowed=allowed)),
        settings=SimpleNamespace(model=SimpleNamespace(
            endpoint="https://example.test/chat/completions", model="offline",
            api_key_env="ELEPHANT_MODEL_KEY")))


@pytest.mark.parametrize(("reply", "expected"), [
    ('{"choices":[1,0]}', "任务仍在停止中。\n已有一项核验记录。"),
    ('{"choices":[2,0]}', "正在停止。"),
    ('{"choices":[0,0],"extra":"已完成"}', "正在停止。"),
    ("not json", "正在停止。"),
])
async def test_host_expression_only_selects_approved_sentences(db, clock, monkeypatch, reply, expected):
    monkeypatch.setenv("ELEPHANT_MODEL_KEY", "test-key")
    calls = []

    def response(request):
        calls.append(json.loads(request.content))
        return httpx.Response(200, json={"choices": [{"message": {"content": reply}}],
            "usage": {"prompt_tokens": 3, "completion_tokens": 2}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
        core = host_core(db, clock, client)
        facts = [["正在停止。", "任务仍在停止中。"], ["已有一项核验记录。"]]
        body = await render_expression(core, event=EVENT, request_id="control-1", task_id="task-1",
            facts=facts, fallback="正在停止。")
        assert body == expected
        assert await render_expression(core, event=EVENT, request_id="control-1", task_id="task-1",
            facts=facts, fallback="正在停止。") == "正在停止。"
    assert len(calls) == 1 and "tools" not in calls[0]
    with db.engine.connect() as conn:
        row = conn.execute(select(model_requests)).mappings().one()
    assert row["state"] == ("COMPLETED" if reply.startswith('{"choices":[1,0]}') else "FAILED")


async def test_host_expression_timeout_uses_fallback(db, clock, monkeypatch):
    monkeypatch.setenv("ELEPHANT_MODEL_KEY", "test-key")

    async def slow(_request):
        await asyncio.sleep(3)
        return httpx.Response(200, json={"choices": [{"message": {"content": '{"choices":[0]}'}}]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(slow)) as client:
        core = host_core(db, clock, client)
        assert await render_expression(core, event=EVENT, request_id="slow", task_id="task-1",
            facts=[["已取消。"]], fallback="已取消。") == "已取消。"
    with db.engine.connect() as conn:
        assert conn.scalar(select(model_requests.c.state)) == "FAILED"


async def test_host_expression_without_model_keeps_fallback(db, clock):
    core = SimpleNamespace(db=db, clock=clock, settings=SimpleNamespace(model=SimpleNamespace(
        endpoint=None, model=None, api_key_env="ELEPHANT_MODEL_KEY")))
    assert await render_expression(core, event=EVENT, request_id="offline", task_id="task-1",
        facts=[["已取消。", "任务已取消。"]], fallback="已取消。") == "已取消。"
    with db.engine.connect() as conn:
        assert not conn.execute(select(model_requests)).first()


async def test_host_expression_denied_model_permission_never_posts(db, clock, monkeypatch):
    monkeypatch.setenv("ELEPHANT_MODEL_KEY", "test-key")

    def forbidden(_request):
        raise AssertionError("denied request reached model provider")

    async with httpx.AsyncClient(transport=httpx.MockTransport(forbidden)) as client:
        core = host_core(db, clock, client, allowed=False)
        assert await render_expression(core, event=EVENT, request_id="denied", task_id="task-1",
            facts=[["已取消。", "任务已取消。"]], fallback="已取消。") == "已取消。"
    with db.engine.connect() as conn:
        assert not conn.execute(select(model_requests)).first()


async def test_host_expression_respects_shared_model_capacity(db, clock, monkeypatch):
    monkeypatch.setenv("ELEPHANT_MODEL_KEY", "test-key")

    def forbidden(_request):
        raise AssertionError("request bypassed shared model capacity")

    async with httpx.AsyncClient(transport=httpx.MockTransport(forbidden)) as client:
        core = host_core(db, clock, client)
        core.scheduler.capacity = ModelCapacity(total=1)
        async with core.scheduler.capacity.slot(background=True):
            assert await render_expression(core, event=EVENT, request_id="busy", task_id="task-1",
                facts=[["已取消。", "任务已取消。"]], fallback="已取消。") == "已取消。"
    with db.engine.connect() as conn:
        assert conn.scalar(select(model_requests.c.state)) == "FAILED"


async def test_host_expression_rechecks_permission_after_waiting_for_capacity(db, clock, monkeypatch):
    monkeypatch.setenv("ELEPHANT_MODEL_KEY", "test-key")

    def forbidden(_request):
        raise AssertionError("revoked facts reached model provider")

    async with httpx.AsyncClient(transport=httpx.MockTransport(forbidden)) as client:
        core = host_core(db, clock, client)
        core.scheduler.capacity = ModelCapacity(total=1)
        async with core.scheduler.capacity.slot(background=True):
            pending = asyncio.create_task(render_expression(core, event=EVENT,
                request_id="revoked-while-waiting", task_id="task-1",
                facts=[["已取消。", "任务已取消。"]], fallback="已取消。"))
            await asyncio.sleep(0)
            core.permissions.check = lambda *_args: SimpleNamespace(allowed=False)
        assert await pending == "已取消。"
    with db.engine.connect() as conn:
        assert conn.scalar(select(model_requests.c.state)) == "FAILED"
