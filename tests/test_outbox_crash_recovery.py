"""A real process exit after synthetic QQ acceptance cannot justify another send."""
import asyncio
import json
import multiprocessing
import os
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
from sqlalchemy import select

from elephant.adapters.qq_official.adapter import OfficialAdapter
from elephant.clock import FakeClock
from elephant.config import AdapterConfig
from elephant.domain import ConversationRef, OutboundEnvelope, Surface
from elephant.ingress.service import Inbox
from elephant.outbox.service import Outbox
from elephant.storage.db import Database
from elephant.storage.operations import backup, export_deletions, restore
from elephant.storage.schema import outbox

CRASH_EXIT = 73


def crash_after_platform_acceptance(database, evidence, config):
    os.environ[config["secret_env"]] = "synthetic-crash-test-secret"
    db, clock = Database(database), FakeClock()

    def accept(request):
        if request.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "synthetic-token", "expires_in": 7200})
        assert request.method == "POST" and request.url.path == "/v2/users/300/messages"
        with Path(evidence).open("x") as output:
            json.dump({"path": request.url.path, "body": json.loads(request.content),
                       "accepted_id": "synthetic-platform-accepted"}, output)
            output.flush()
            os.fsync(output.fileno())
        os._exit(CRASH_EXIT)

    async def dispatch():
        async with httpx.AsyncClient(transport=httpx.MockTransport(accept)) as client:
            adapter = OfficialAdapter(AdapterConfig.model_validate(config), "d", Inbox(db), clock, client)
            await Outbox(db, clock).dispatch_once({"qq": adapter})

    asyncio.run(dispatch())


def enqueue(service, db, clock, origin, text):
    message = OutboundEnvelope(delivery_id=origin, origin_kind="task", origin_id=origin,
        target=ConversationRef(deployment_id="d", adapter_instance_id="qq", bot_identity_id="bot",
            surface=Surface.PRIVATE, remote_conversation_id="300"),
        text=text, allow_proactive=True, purpose="result", expires_at=clock.now() + timedelta(hours=1))
    with db.transaction() as conn:
        return service.enqueue(conn, message)


@pytest.mark.parametrize("isolated_restore", [False, True], ids=["restart", "snapshot-restore"])
async def test_platform_acceptance_survives_process_exit_without_resend(
    db, clock, tmp_path, official_config, isolated_restore
):
    config = official_config.model_copy(update={"proactive_verified": True})
    service = Outbox(db, clock)
    first = enqueue(service, db, clock, "crashed-delivery", "synthetic first result")
    database = db.path
    db.close()
    evidence = tmp_path / "platform-acceptance.json"
    process = multiprocessing.get_context("spawn").Process(
        target=crash_after_platform_acceptance,
        args=(database, str(evidence), config.model_dump()))
    process.start()
    try:
        process.join(timeout=15)
        assert not process.is_alive(), "Synthetic platform acceptance was never reached"
        assert process.exitcode == CRASH_EXIT
    finally:
        if process.is_alive():
            process.kill()
            process.join(timeout=5)
        process.close()
    accepted = evidence.read_bytes()
    assert json.loads(accepted)["body"]["content"] == "synthetic first result"
    restarted = Database(database)
    try:
        with restarted.engine.connect() as conn:
            attempted = dict(conn.execute(select(outbox)).mappings().one())
        assert attempted["state"] == "SENDING" and attempted["attempts"] == 1
        assert attempted["receipt_json"] is None and attempted["platform_message_id"] is None
        if isolated_restore:
            snapshot, ledger = tmp_path / "snapshot.db", tmp_path / "deletions.json"
            backup(database, snapshot)
            export_deletions(database, ledger)
            isolated = tmp_path / "isolated.db"
            restore(snapshot, isolated, ledger)
            restarted.close()
            restarted = Database(str(isolated))
        service = Outbox(restarted, clock)
        service.recover()
        sent = []

        def accept_new(request):
            if request.url.path.endswith("getAppAccessToken"):
                return httpx.Response(200, json={"access_token": "synthetic-token", "expires_in": 7200})
            assert request.method == "POST" and request.url.path == "/v2/users/300/messages"
            sent.append(json.loads(request.content))
            return httpx.Response(200, json={"id": "synthetic-independent-result"})

        async with httpx.AsyncClient(transport=httpx.MockTransport(accept_new)) as client:
            adapter = OfficialAdapter(config, "d", Inbox(restarted), clock, client)
            await service.dispatch_once({"qq": adapter})
            assert sent == []
            second = enqueue(service, restarted, clock, "independent-delivery", "synthetic independent result")
            clock.advance(3)
            await service.dispatch_once({"qq": adapter})
            await service.dispatch_once({"qq": adapter})
        assert [body["content"] for body in sent] == ["synthetic independent result"]
        with restarted.engine.connect() as conn:
            rows = {row["id"]: dict(row) for row in conn.execute(select(outbox)).mappings()}
        first_row = rows[first]
        assert first_row["state"] == "UNKNOWN" and first_row["attempts"] == 1
        assert first_row["sent_at"] is None and first_row["platform_message_id"] is None
        if isolated_restore:
            assert first_row["receipt_json"] is None
        else:
            assert json.loads(first_row["receipt_json"]) == {
                "state": "UNKNOWN", "platform_message_id": None, "error_code": "INTERRUPTED_SEND",
            }
        for key in ("dedupe_key", "envelope_json", "attempted_at", "origin_id"):
            assert first_row[key] == attempted[key]
        assert rows[second]["state"] == "SENT" and rows[second]["attempts"] == 1
        assert evidence.read_bytes() == accepted
    finally:
        restarted.close()
