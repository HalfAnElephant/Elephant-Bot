"""Important progress keeps its authorized route without reviving ordinary chat."""
import json
from datetime import timedelta

import httpx
import pytest
from sqlalchemy import insert, select, update
from test_agent_runtime import leader_complete, root_goal
from test_agent_runtime import runtime as runtime  # noqa: PLC0414
from test_leader_core import make_core
from test_leader_decisions import failed_attention
from test_outbox_results import result

from elephant.adapters.qq_official.adapter import OfficialAdapter
from elephant.domain import OutboundEnvelope, stable_id
from elephant.outbox.service import Outbox
from elephant.storage.schema import outbox, reply_counters
from elephant.storage.task_schema import notifications, tasks


@pytest.fixture
async def progress_http():
    calls = []

    def transport(request):
        if request.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "fixture", "expires_in": 7200})
        calls.append(json.loads(request.content))
        return httpx.Response(200, json={"id": "progress-" + str(len(calls))})

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        yield client, calls


@pytest.fixture
def progress_adapter(incoming, official_config, clock, progress_http):
    client, calls = progress_http
    adapter = OfficialAdapter(official_config.model_copy(update={"proactive_verified": True}),
        "d", incoming, clock, client)
    return adapter, calls


def install_task_event(runtime, adapter, official_event):
    event = adapter.normalize(official_event)
    runtime.event = event
    runtime.access = runtime.access.model_copy(update={"principal": event.principal, "conversation": event.conversation})
    return event


@pytest.mark.parametrize("exhausted", [False, True])
async def test_attention_progress_posts_once_without_a_passive_reply(runtime, progress_adapter, official_event, exhausted):
    adapter, calls = progress_adapter
    # Task and adapter must share the same clock, including passive expiry.
    adapter.clock = runtime.clock
    event = install_task_event(runtime, adapter, official_event)
    if exhausted:
        with runtime.db.transaction() as conn:
            conn.execute(insert(reply_counters).values(
                key=stable_id(event.conversation.key, event.reply_context.platform_message_id),
                seq=event.reply_context.max_replies - 1))
    else:
        runtime.clock.advance(301)
    root, _, claim = failed_attention(runtime, children=2)
    assert claim["purpose"] == "attention"
    leader_complete(runtime, claim, "连接仍有问题，需要继续跟进。")
    with runtime.db.engine.connect() as conn:
        notice = conn.execute(select(notifications)).mappings().one()
        assert notice["purpose"] == "progress" and notice["importance"] == "important"
        row = conn.execute(select(outbox)).mappings().one()
        assert OutboundEnvelope.model_validate_json(row["envelope_json"]).allow_proactive
        assert conn.execute(select(tasks.c.state).where(tasks.c.id == root["task_id"])).scalar_one() != "SUCCEEDED"
    await runtime.service.outbox.dispatch_once({"qq": adapter})
    await runtime.service.outbox.dispatch_once({"qq": adapter})
    assert len(calls) == 1 and "msg_id" not in calls[0] and "msg_seq" not in calls[0]
    with runtime.db.engine.connect() as conn:
        assert conn.execute(select(outbox.c.state)).scalar_one() == "SENT"


@pytest.mark.parametrize("important", [False, True])
async def test_chat_task_notify_respects_explicit_importance(tmp_path, official_config, official_event, clock, important, progress_http):
    from conftest import official_payload

    core = make_core(tmp_path, official_config.model_copy(update={"proactive_verified": True}), clock)
    client, calls = progress_http
    adapter = core.adapters["qq"]
    await adapter.client.aclose()
    adapter.client = client
    adapter._owns_client = False
    try:
        event, _ = core.inbox.accept(adapter.normalize(official_payload(official_event, private=True, text="请检查项目目前的情况")))
        await core.process(event)
        clock.advance(301)
        claim = core.sessions.claim("leader")
        await core.leader.invoke("notify", (claim["run_id"], claim["fencing_token"], "leader"), {
            "text": "这项检查还需要一些时间。", "notification_id": "explicit-progress",
            "importance": "important" if important else "normal"})
        with core.db.engine.connect() as conn:
            envelope = OutboundEnvelope.model_validate_json(conn.execute(select(outbox.c.envelope_json)).scalar_one())
            assert envelope.allow_proactive is important
        await core.outbox.dispatch_once({"qq": adapter}, pre_send=core.pre_send)
        await core.outbox.dispatch_once({"qq": adapter}, pre_send=core.pre_send)
        assert len(calls) == (1 if important else 0)
        if important:
            assert "msg_id" not in calls[0] and "msg_seq" not in calls[0]
            with core.db.engine.connect() as conn:
                assert conn.execute(select(outbox.c.state)).scalar_one() == "SENT"
    finally:
        await core.stop()
        core.db.close()


async def test_recovery_upgrades_only_live_important_task_progress(runtime, progress_adapter, official_event):
    adapter, calls = progress_adapter
    adapter.clock = runtime.clock
    install_task_event(runtime, adapter, official_event)
    root = root_goal(runtime)
    with runtime.db.transaction() as conn:
        task = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
        for index, importance in enumerate(["important", "important", "important", "normal"]):
            runtime.service._notification(conn, task, None, "进度 " + str(index), "progress", importance, "notice-" + str(index))
        rows = conn.execute(select(outbox)).mappings().all()
        for row in rows:
            envelope = OutboundEnvelope.model_validate_json(row["envelope_json"])
            state = {"notice-0": "WAITING_FOR_TRIGGER", "notice-1": "UNKNOWN", "notice-2": "SENT", "notice-3": "WAITING_FOR_TRIGGER"}[row["origin_id"]]
            conn.execute(update(outbox).where(outbox.c.id == row["id"]).values(state=state,
                envelope_json=envelope.model_copy(update={"allow_proactive": False}).model_dump_json()))
    runtime.clock.advance(301)
    runtime.service.outbox.recover()
    runtime.service.outbox.recover()
    with runtime.db.engine.connect() as conn:
        rows = {row.origin_id: row for row in conn.execute(select(outbox))}
        assert rows["notice-0"].state == "PENDING"
        assert OutboundEnvelope.model_validate_json(rows["notice-0"].envelope_json).allow_proactive
        assert rows["notice-1"].state == "UNKNOWN"
        assert rows["notice-2"].state == "SENT"
        assert not OutboundEnvelope.model_validate_json(rows["notice-3"].envelope_json).allow_proactive
    await runtime.service.outbox.dispatch_once({"qq": adapter})
    await runtime.service.outbox.dispatch_once({"qq": adapter})
    assert len(calls) == 1 and "msg_id" not in calls[0]


@pytest.mark.parametrize("restart", [False, True])
async def test_expired_chat_progress_waiters_finish_and_fresh_passive_still_sends(
    db, clock, progress_adapter, official_event, restart
):
    adapter, calls = progress_adapter
    event = adapter.normalize(official_event)
    outgoing = Outbox(db, clock)
    old = [result(outgoing, db, event, clock, "history-" + str(index), purpose="progress",
        expires_at=clock.now() + timedelta(seconds=300)) for index in range(24)]
    with db.transaction() as conn:
        conn.execute(update(outbox).where(outbox.c.id.in_(old)).values(state="WAITING_FOR_TRIGGER"))
    clock.advance(301)
    if restart:
        outgoing.recover()
    for _ in range(1 if restart else len(old)):
        await outgoing.dispatch_once({"qq": adapter})
    with db.engine.connect() as conn:
        rows = conn.execute(select(outbox).where(outbox.c.id.in_(old))).mappings().all()
        assert all(row["state"] == "FAILED" and json.loads(row["receipt_json"])["error_code"] == "EXPIRED" for row in rows)
        assert all(not json.loads(row["envelope_json"])["allow_proactive"] for row in rows)
    assert calls == []
    from conftest import official_payload
    fresh_event = adapter.normalize(official_payload(official_event, message_id="fresh-passive",
        timestamp=clock.now().isoformat()))
    fresh = result(outgoing, db, fresh_event, clock, "fresh", purpose="progress", expires_at=clock.now() + timedelta(seconds=300))
    await outgoing.dispatch_once({"qq": adapter})
    assert len(calls) == 1 and calls[0]["msg_id"] == fresh_event.platform_message_id
    with db.engine.connect() as conn:
        assert conn.execute(select(outbox.c.state).where(outbox.c.id == fresh)).scalar_one() == "SENT"


def test_normal_background_progress_does_not_gain_proactive_intent(runtime, progress_adapter, official_event):
    adapter, _ = progress_adapter
    install_task_event(runtime, adapter, official_event)
    root = root_goal(runtime)
    with runtime.db.transaction() as conn:
        task = conn.execute(select(tasks).where(tasks.c.id == root["task_id"])).mappings().one()
        assert "schedule_fire_id" not in json.loads(task["context_json"])
        runtime.service._notification(conn, task, None, "正在继续检查。", "progress", "normal", "normal-progress")
        envelope = OutboundEnvelope.model_validate_json(conn.execute(select(outbox.c.envelope_json)).scalar_one())
        assert not envelope.allow_proactive
        assert envelope.reply_context.msg_seq is not None


async def test_uncertain_multipart_progress_does_not_block_next_notification(db, clock, progress_adapter, official_event):
    adapter, calls = progress_adapter
    event = adapter.normalize(official_event)
    outgoing = Outbox(db, clock)
    old = OutboundEnvelope(delivery_id="old-0", origin_kind="task", origin_id="old", target=event.conversation,
        parent_task_id="task", notification_sequence=1, text="Earlier progress", purpose="progress",
        expires_at=clock.now() + timedelta(hours=1))
    with db.transaction() as conn:
        outgoing.enqueue(conn, old)
        outgoing.enqueue(conn, old.model_copy(update={"delivery_id": "old-1", "part_no": 1}))
        conn.execute(update(outbox).where(outbox.c.id == "old-0").values(state="UNKNOWN", attempts=1))
        outgoing.enqueue(conn, old.model_copy(update={"delivery_id": "new", "origin_id": "independent",
            "notification_sequence": 2, "text": "Important update", "allow_proactive": True}))
    await outgoing.dispatch_once({"qq": adapter})
    assert len(calls) == 1 and calls[0]["content"] == "Important update"
    with db.engine.connect() as conn:
        assert dict(conn.execute(select(outbox.c.id, outbox.c.state)).all()) == {
            "old-0": "UNKNOWN", "old-1": "PENDING", "new": "SENT"}


async def test_important_chat_waits_for_its_requester_without_extending_ttl(db, clock, progress_adapter, official_event):
    from conftest import official_payload

    outgoing = Outbox(db, clock)
    adapter, calls = progress_adapter
    adapter.config = adapter.config.model_copy(update={"proactive_verified": False})
    event = adapter.normalize(official_event)
    expires = clock.now() + timedelta(minutes=2)
    delivery = result(outgoing, db, event, clock, "notice", origin_id="progress:notice:turn:session:input",
        purpose="progress", allow_proactive=True, reply_context=None, expires_at=expires)
    await outgoing.dispatch_once({"qq": adapter})
    assert calls == []
    clock.advance(30)
    fresh = official_payload(official_event, message_id="new", timestamp=clock.now().isoformat())
    with db.transaction() as conn:
        assert outgoing.retrigger(conn, adapter.normalize(official_payload(fresh, user_id="other"))) == []
        assert outgoing.retrigger(conn, adapter.normalize(fresh)) == [delivery]
        row = conn.execute(select(outbox.c.envelope_json)).scalar_one()
        assert OutboundEnvelope.model_validate_json(row).expires_at == expires
    await outgoing.dispatch_once({"qq": adapter})
    assert len(calls) == 1 and calls[0]["msg_id"] == "new"


def test_trigger_cannot_revive_expired_held_worker_progress(db, clock, progress_adapter, official_event):
    from conftest import official_payload

    outgoing = Outbox(db, clock)
    adapter, _ = progress_adapter
    event = adapter.normalize(official_event)
    delivery = result(outgoing, db, event, clock, "held", origin_kind="task",
        purpose="progress", expires_at=clock.now() + timedelta(seconds=10))
    with db.transaction() as conn:
        conn.execute(update(outbox).where(outbox.c.id == delivery).values(state="LEADER_PENDING"))
    clock.advance(11)
    with db.transaction() as conn:
        fresh = official_payload(official_event, message_id="new", timestamp=clock.now().isoformat())
        assert outgoing.retrigger(conn, adapter.normalize(fresh)) == []
        row = conn.execute(select(outbox.c.state, outbox.c.receipt_json)).one()
        assert row.state == "FAILED" and json.loads(row.receipt_json)["error_code"] == "EXPIRED"
