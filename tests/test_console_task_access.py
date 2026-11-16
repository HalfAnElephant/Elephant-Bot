"""Synthetic console contracts: authorization is checked at read time."""
import asyncio
import json

import pytest
from fastapi.middleware.asyncexitstack import AsyncExitStackMiddleware
from fastapi.testclient import TestClient
from sqlalchemy import insert, select, update

from elephant.app import create_app
from elephant.config import Settings
from elephant.permissions import PermissionPolicy, PermissionRule, SubjectSelector
from elephant.storage.task_schema import events, runs, tasks

AUTH = {"Authorization": "Bearer synthetic-console", "Idempotency-Key": "console-access-fixture"}


def revoke_read(core, task_id, *, subject=None):
    rule = PermissionRule(id="fixture-deny", actions=("task.read",), resources=("task:" + task_id,), effect="deny",
                          subject=subject or SubjectSelector())
    core.permissions.set_policy(PermissionPolicy(preset="custom", rules=(rule,)),
                                expected_version=core.permissions.policy_version, actor="synthetic-fixture")


@pytest.mark.parametrize("user_id", ["owner", "personal-owner"])
def test_parent_projection_does_not_include_unreadable_children(tmp_path, clock, monkeypatch, user_id):
    monkeypatch.setenv("ELEPHANT_ADMIN_TOKEN", "synthetic-console")
    app = create_app(Settings(database=str(tmp_path / "core.db"), tasks_enabled=True),
                     clock=clock, run_background=False)
    body = {"goal": "Synthetic fixture"}
    with TestClient(app) as client:
        created = client.post("/api/v1/tasks", headers=AUTH, json=body)
        assert created.status_code == 200, created.text
        parent = created.json()
        child, = parent["children"]
        url = "/api/v1/tasks/" + parent["public_id"]
        assert client.get(url, headers=AUTH).json()["children"][0]["public_id"] == child["public_id"]
        revoke_read(app.state.core, child["id"], subject=SubjectSelector(
            kind="person", adapter_instance_id="admin_api", identity_namespace="admin_api", user_id=user_id))
        for response in (client.get(url, headers=AUTH),
                         client.post("/api/v1/tasks", headers=AUTH, json=body),
                         client.get("/api/v1/tasks", headers=AUTH)):
            assert response.status_code == 200, response.text
            view = response.json()
            if isinstance(view, list):
                view, = view
            assert view["children"] == []
            assert child["public_id"] not in response.text and child["id"] not in response.text

        revoke_read(app.state.core, parent["task_id"])
        assert client.get("/api/v1/tasks", headers=AUTH).json() == []
        assert client.get(url, headers=AUTH).status_code == 403
        repeated = client.post("/api/v1/tasks", headers=AUTH, json=body)
        assert repeated.status_code == 422 and parent["public_id"] not in repeated.text


@pytest.mark.parametrize("revocation", ["task_read", "source", "token", "revision"])
async def test_open_console_event_stream_rechecks_authority_between_frames(tmp_path, clock, monkeypatch, revocation):
    monkeypatch.setenv("ELEPHANT_ADMIN_TOKEN", "synthetic-console")
    app = create_app(Settings(database=str(tmp_path / "core.db"), tasks_enabled=True),
                     clock=clock, run_background=False)
    async with app.router.lifespan_context(app):
        core = app.state.core
        parent = core.enqueue_admin_task("Synthetic fixture", idempotency_key="console-stream")
        child, = parent["children"]
        worker = core.tasks.claim("synthetic-worker")
        assert worker["task_id"] == child["id"]
        with core.db.transaction() as conn:
            conn.execute(insert(events), [
                {"event_id": "before-check", "run_id": worker["run_id"], "seq": 1, "payload": "FIRST_FIXTURE"},
                {"event_id": "after-check", "run_id": worker["run_id"], "seq": 2, "payload": "SECOND_FIXTURE"},
            ])
        path = "/api/v1/tasks/" + child["public_id"] + "/events"
        scope = {"type": "http", "asgi": {"version": "3.0", "spec_version": "2.4"}, "http_version": "1.1",
                 "method": "GET", "path": path, "root_path": "", "query_string": b"", "scheme": "http",
                 "server": ("test", 80), "client": ("test", 1),
                 "headers": [(b"authorization", b"Bearer synthetic-console")]}
        sent = []

        async def receive():
            await asyncio.Event().wait()

        async def send(message):
            sent.append(message)
            if b"FIRST_FIXTURE" not in message.get("body", b""):
                return
            if revocation == "task_read":
                revoke_read(core, worker["task_id"])
            elif revocation == "source":
                core.tasks.context_validator = lambda *_: False
            elif revocation == "token":
                monkeypatch.setenv("ELEPHANT_ADMIN_TOKEN", "rotated-synthetic-token")
            else:
                with core.db.transaction() as conn:
                    conn.execute(update(tasks).where(tasks.c.id == parent["task_id"]).values(goal_revision=1))

        # Drive the router directly so response middleware cannot queue the next frame
        # before the test simulates a concurrent permission change.
        route_app = AsyncExitStackMiddleware(app.router)
        await asyncio.wait_for(route_app(scope, receive, send), timeout=5)
        assert sent[0]["status"] == 200
        assert b"FIRST_FIXTURE" in sent[1]["body"]
        assert sent[2]["body"] == b"event: access-revoked\ndata: {}\n\n"
        assert sent[3]["more_body"] is False
        assert not any(b"SECOND_FIXTURE" in message.get("body", b"") for message in sent)


async def test_reopened_console_stream_only_reads_current_goal_revision(tmp_path, clock, monkeypatch):
    monkeypatch.setenv("ELEPHANT_ADMIN_TOKEN", "synthetic-console")
    app = create_app(Settings(database=str(tmp_path / "core.db"), tasks_enabled=True),
                     clock=clock, run_background=False)
    async with app.router.lifespan_context(app):
        core = app.state.core
        parent = core.enqueue_admin_task("Synthetic fixture", idempotency_key="console-review-reopen")
        worker = core.tasks.claim("synthetic-worker")
        with core.db.transaction() as conn:
            root = conn.execute(select(tasks).where(tasks.c.id == parent["task_id"])).mappings().one()
            context = json.loads(root["context_json"]) | {"memory_replan": {"from_revision": 0}}
            conn.execute(update(tasks).where(tasks.c.id == root["id"]).values(
                goal_revision=1, context_json=json.dumps(context)))
            old_run = conn.execute(select(runs).where(runs.c.id == worker["run_id"])).mappings().one()
            conn.execute(update(runs).where(runs.c.id == old_run["id"]).values(task_id=root["id"]))
            conn.execute(insert(runs).values(**dict(old_run) | {
                "id": "current-fixture-run", "task_id": root["id"], "basis_goal_revision": 1,
            }))
            conn.execute(insert(events), [
                {"event_id": "prior-event", "run_id": old_run["id"], "seq": 1, "payload": "OLD_FIXTURE"},
                {"event_id": "fresh-event", "run_id": "current-fixture-run", "seq": 1, "payload": "NEW_FIXTURE"},
            ])
        path = "/api/v1/tasks/" + parent["public_id"] + "/events"
        scope = {"type": "http", "asgi": {"version": "3.0", "spec_version": "2.4"}, "http_version": "1.1",
                 "method": "GET", "path": path, "root_path": "", "query_string": b"", "scheme": "http",
                 "server": ("test", 80), "client": ("test", 1),
                 "headers": [(b"authorization", b"Bearer synthetic-console")]}
        disconnected = asyncio.Event()
        sent = []

        async def receive():
            if not disconnected.is_set():
                await disconnected.wait()
            return {"type": "http.disconnect"}

        async def send(message):
            sent.append(message)
            if b"NEW_FIXTURE" in message.get("body", b""):
                disconnected.set()

        route_app = AsyncExitStackMiddleware(app.router)
        await asyncio.wait_for(route_app(scope, receive, send), timeout=5)
        assert sent[0]["status"] == 200
        assert any(b"NEW_FIXTURE" in message.get("body", b"") for message in sent)
        assert not any(b"OLD_FIXTURE" in message.get("body", b"") for message in sent)
