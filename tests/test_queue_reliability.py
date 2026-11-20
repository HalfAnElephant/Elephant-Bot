"""Concurrent durable result replay and multipart outbox candidate fairness."""
import asyncio
import errno
import json
from collections import Counter
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
from sqlalchemy import select, update
from test_worker_failures import environment as worker_environment

from elephant.adapters.qq_official.adapter import OfficialAdapter
from elephant.domain import AccessContext, ConversationRef, InboundEvent, OutboundEnvelope, Surface
from elephant.ingress.service import Inbox
from elephant.jobs.worker import Spool, Worker
from elephant.outbox.service import Outbox
from elephant.storage.db import Database
from elephant.storage.schema import outbox
from elephant.storage.task_schema import runs

environment = worker_environment


def pending_results(environment, tmp_path, count):
    service, task, client = environment
    source = InboundEvent.model_validate_json(task["event_json"])
    for i in range(1, count):
        event = source.model_copy(update={"event_id": f"source-{i}", "platform_message_id": f"message-{i}"})
        service.accept(event, AccessContext(principal=event.principal, conversation=event.conversation), "Synthetic goal")
    worker = Worker(client, "worker", Spool(tmp_path / "spool"), None)
    payloads = {}
    for _ in range(count):
        claim = service.claim("worker")
        assert claim
        payload = {"run_id": claim["run_id"], "fencing_token": claim["fencing_token"],
            "result": {"status": "failed", "summary": "Synthetic stopped result", "agent_authored": False},
            "stopped": True, "quiescent": False}
        payloads[worker.spool.save(payload)] = payload
    return worker, payloads


async def test_replay_snapshot_survives_active_delivery_removing_next_record(environment, tmp_path):
    worker, payloads = pending_results(environment, tmp_path, 2)
    paths = sorted(payloads)
    first = payloads[paths[0]]["run_id"]
    entered, release = asyncio.Event(), asyncio.Event()
    client = worker.client
    calls = Counter()

    class Gate:
        async def post(self, path, **kwargs):
            if path.endswith("/result"):
                run = kwargs["json"]["run_id"]
                calls[run] += 1
                if run == first:
                    entered.set()
                    await release.wait()
            return await client.post(path, **kwargs)

    worker.client = Gate()
    replay = asyncio.create_task(worker.replay())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        assert await worker.deliver_result(payloads[paths[1]], paths[1])
    finally:
        release.set()
        await replay
    assert list(worker.spool.directory.glob("*.json")) == []
    assert list(calls.values()) == [1, 1]
    with environment[0].db.engine.connect() as conn:
        committed = conn.execute(select(runs.c.slot_reserved, runs.c.result_json)).all()
    assert len(committed) == 2 and all(not slot and result for slot, result in committed)


async def test_same_spooled_result_is_delivered_once_during_replay(environment, tmp_path):
    worker, payloads = pending_results(environment, tmp_path, 1)
    path, payload = next(iter(payloads.items()))
    entered, release = asyncio.Event(), asyncio.Event()
    client = worker.client
    calls = 0

    class Gate:
        async def post(self, endpoint, **kwargs):
            nonlocal calls
            if endpoint.endswith("/result"):
                calls += 1
                entered.set()
                await release.wait()
            return await client.post(endpoint, **kwargs)

    worker.client = Gate()
    replay = asyncio.create_task(worker.replay())
    await asyncio.wait_for(entered.wait(), 2)
    delivery = asyncio.create_task(worker.deliver_result(payload, path))
    await asyncio.sleep(0)
    release.set()
    await asyncio.gather(replay, delivery)
    assert calls == 1
    with environment[0].db.engine.connect() as conn:
        committed = conn.execute(select(runs.c.result_json)).scalar_one()
    assert json.loads(committed)["summary"] == "Synthetic stopped result"
    assert not path.exists()


@pytest.mark.parametrize("problem", ["permission", "io", "json"])
async def test_replay_does_not_hide_real_read_errors(environment, tmp_path, monkeypatch, problem):
    worker, payloads = pending_results(environment, tmp_path, 1)
    path = next(iter(payloads))
    read = Path.read_text
    if problem == "json":
        path.write_text("{invalid")
        expected = json.JSONDecodeError
    else:
        error = PermissionError(errno.EACCES, "synthetic permission denied") if problem == "permission" else OSError(errno.EIO, "synthetic I/O error")
        expected = type(error)

        def broken(candidate, *args, **kwargs):
            if candidate == path:
                raise error
            return read(candidate, *args, **kwargs)
        monkeypatch.setattr(Path, "read_text", broken)
    with pytest.raises(expected):
        await worker.replay()
    assert path.exists()
    with environment[0].db.engine.connect() as conn:
        assert conn.execute(select(runs.c.slot_reserved)).scalar_one() == 1


@pytest.mark.parametrize("restart", [False, True])
@pytest.mark.parametrize("origin_kind", ["chat", "task"])
async def test_unknown_predecessors_do_not_fill_outbox_candidate_batch(db, clock, official_config, restart, origin_kind):
    service = Outbox(db, clock)
    hot = ConversationRef(deployment_id="d", adapter_instance_id="qq", bot_identity_id="bot",
        surface=Surface.PRIVATE, remote_conversation_id="300")
    for i in range(50):
        with db.transaction() as conn:
            for part in (0, 1):
                service.enqueue(conn, OutboundEnvelope(delivery_id=f"blocked-{i}-{part}", target=hot,
                    text=f"Synthetic part {part}", origin_kind=origin_kind, origin_id=f"origin-{i}", part_no=part,
                    allow_proactive=True, expires_at=clock.now() + timedelta(hours=1)))
            conn.execute(update(outbox).where(outbox.c.id == f"blocked-{i}-0").values(
                state="UNKNOWN", attempts=1, attempted_at=clock.now().isoformat()))
        clock.advance(.001)
    # Old attempts must not trip the separate, unchanged bot-wide minute quota.
    clock.advance(75)
    with db.transaction() as conn:
        service.enqueue(conn, OutboundEnvelope(delivery_id="healthy", target=hot.model_copy(update={
            "remote_conversation_id": "301"}), text="Independent healthy result", origin_kind=origin_kind,
            origin_id="independent", allow_proactive=True, expires_at=clock.now() + timedelta(hours=1)))
    restarted = Database(db.path) if restart else db
    service = Outbox(restarted, clock)
    if restart:
        service.recover()
    sent = []

    def transport(request):
        if request.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "synthetic", "expires_in": 7200})
        sent.append((request.url.path, json.loads(request.content)))
        return httpx.Response(200, json={"id": "healthy-ack"})

    config = official_config.model_copy(update={"allowed_users": ("300", "301"), "proactive_verified": True})
    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        adapter = OfficialAdapter(config, "d", Inbox(restarted), clock, client)
        await service.dispatch_once({"qq": adapter})
        clock.advance(3)
        await service.dispatch_once({"qq": adapter})
    with restarted.engine.connect() as conn:
        records = list(conn.execute(select(outbox)).mappings())
    assert len(sent) == 1 and sent[0][0] == "/v2/users/301/messages"
    assert next(row for row in records if row["id"] == "healthy")["state"] == "SENT"
    assert all(row["state"] == "UNKNOWN" and row["attempts"] == 1 for row in records if row["part_no"] == 0 and row["id"] != "healthy")
    assert all(row["state"] == "PENDING" and row["attempts"] == 0 for row in records if row["part_no"] == 1)
    if restart:
        restarted.close()


async def test_lost_result_ack_replays_original_durable_receipt(environment, tmp_path):
    worker, payloads = pending_results(environment, tmp_path, 1)
    path = next(iter(payloads))
    client = worker.client
    calls = 0

    class LostAck:
        async def post(self, endpoint, **kwargs):
            nonlocal calls
            response = await client.post(endpoint, **kwargs)
            if endpoint.endswith("/result"):
                calls += 1
                if calls == 1:
                    raise httpx.ReadTimeout("Synthetic acknowledgment lost after Core commit")
            return response

    worker.client = LostAck()
    with pytest.raises(httpx.ReadTimeout):
        await worker.replay()
    assert path.exists()
    with environment[0].db.engine.connect() as conn:
        committed = dict(conn.execute(select(runs)).mappings().one())
    assert committed["result_json"] and committed["slot_reserved"] == 0
    await worker.replay()
    assert calls == 2 and not path.exists()
    with environment[0].db.engine.connect() as conn:
        assert dict(conn.execute(select(runs)).mappings().one()) == committed


def test_spool_quota_handles_real_concurrent_file_removal(tmp_path, monkeypatch):
    from threading import Thread

    spool = Spool(tmp_path / "spool")
    orphan = spool.save({"old": True})
    stat = Path.stat

    def removed(path, *args, **kwargs):
        if path == orphan:
            thread = Thread(target=orphan.unlink)
            thread.start()
            thread.join(timeout=2)
            assert not thread.is_alive()
        return stat(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", removed)
    result = spool.save({"new": True})
    assert json.loads(result.read_text()) == {"new": True}


@pytest.mark.parametrize("code", [errno.EACCES, errno.EIO])
def test_spool_quota_preserves_real_stat_errors(tmp_path, monkeypatch, code):
    spool = Spool(tmp_path / "spool")
    old = spool.save({"old": True})
    stat = Path.stat

    def broken(path, *args, **kwargs):
        if path == old:
            raise OSError(code, "Synthetic filesystem failure")
        return stat(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", broken)
    with pytest.raises(OSError) as error:
        spool.save({"new": True})
    assert error.value.errno == code
