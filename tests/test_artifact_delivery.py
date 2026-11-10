"""Archived files use the durable delivery contract independently of text."""
import base64
import json
from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select

from qqbot.adapters.onebot_v11.adapter import OneBotAdapter
from qqbot.adapters.qq_official.adapter import OfficialAdapter
from qqbot.domain import AccessContext, Artifact, OutboundEnvelope
from qqbot.harness.artifacts import export_artifacts, read_artifact
from qqbot.jobs.service import TaskService
from qqbot.outbox.service import Outbox
from qqbot.storage.schema import outbox
from qqbot.storage.task_schema import notifications


class Socket:
    def __init__(self, adapter, data):
        self.adapter, self.data, self.calls = adapter, data, []

    async def send_json(self, payload):
        self.calls.append(payload)
        future = self.adapter.pending[(self.adapter.generation, payload["echo"])]
        future.set_result({"status": "ok", "retcode": 0, "data": self.data})


def archive(tmp_path, root, name="report.txt", *, content=b"content"):
    workspace = tmp_path / "workspace"
    workspace.mkdir(exist_ok=True)
    (workspace / name).write_bytes(content)
    return Artifact(**export_artifacts(workspace, root, [name])[0])


def outgoing(event, clock, artifact):
    return OutboundEnvelope(
        delivery_id=str(uuid4()), target=event.conversation, text="attachment", artifact=artifact,
        origin_kind="task", origin_id="result", requester=event.principal,
        expires_at=clock.now() + timedelta(days=1),
    )


@pytest.mark.parametrize("private", [False, True])
@pytest.mark.parametrize("image", [False, True])
async def test_native_delivery_survives_restart(db, clock, incoming, nap_config, nap_event, tmp_path, private, image):
    adapter = OneBotAdapter(nap_config, "d", incoming, clock)
    payload = {**nap_event}
    if private:
        payload.pop("group_id")
        payload["message_type"] = "private"
    event = adapter.normalize(payload)
    adapter.websocket = Socket(adapter, {"file_id": "file-1", "message_id": "message-1"})
    adapter.account_online = True
    service = Outbox(db, clock)
    artifact = archive(tmp_path, service.archive_root, "picture.png" if image else "report.txt")
    with db.transaction() as c:
        service.enqueue(c, outgoing(event, clock, artifact))
    # A new instance reads only the persisted reference and verifies the archive.
    service = Outbox(db, clock)
    await service.dispatch_once({"nap": adapter})
    await service.dispatch_once({"nap": adapter})
    assert len(adapter.websocket.calls) == 1
    call = adapter.websocket.calls[0]
    if image:
        assert call["action"] == ("send_private_msg" if private else "send_group_msg")
        encoded = call["params"]["message"][0]["data"]["file"]
    else:
        assert call["action"] == ("upload_private_file" if private else "upload_group_file")
        assert call["params"]["name"] == "report.txt"
        encoded = call["params"]["file"]
    assert base64.b64decode(encoded.removeprefix("base64://")) == b"content"
    with db.engine.connect() as c:
        row = c.execute(select(outbox)).mappings().one()
    assert row["state"] == "SENT"
    assert "artifact_data" not in json.loads(row["envelope_json"])


async def test_missing_receipt_is_unknown_and_not_retried(db, clock, incoming, nap_config, nap_event, tmp_path):
    adapter = OneBotAdapter(nap_config, "d", incoming, clock)
    adapter.websocket, adapter.account_online = Socket(adapter, {}), True
    event = adapter.normalize(nap_event)
    service = Outbox(db, clock)
    with db.transaction() as c:
        service.enqueue(c, outgoing(event, clock, archive(tmp_path, service.archive_root)))
    await service.dispatch_once({"nap": adapter})
    service.recover()
    clock.advance(20)
    await service.dispatch_once({"nap": adapter})
    assert len(adapter.websocket.calls) == 1
    with db.engine.connect() as c:
        assert c.execute(select(outbox.c.state)).scalar_one() == "UNKNOWN"


@pytest.mark.parametrize("failure", ["changed", "missing", "revoked", "expired"])
async def test_artifact_trust_and_permission_fail_closed(db, clock, incoming, nap_config, nap_event, tmp_path, failure):
    adapter = OneBotAdapter(nap_config, "d", incoming, clock)
    adapter.websocket, adapter.account_online = Socket(adapter, {"file_id": "file"}), True
    event = adapter.normalize(nap_event)
    service = Outbox(db, clock)
    artifact = archive(tmp_path, service.archive_root)
    with db.transaction() as c:
        service.enqueue(c, outgoing(event, clock, artifact))
    path = service.archive_root / artifact.id
    if failure == "changed":
        path.write_bytes(b"changed")
    elif failure == "missing":
        path.unlink()
    elif failure == "expired":
        clock.advance(86401)
    await service.dispatch_once({"nap": adapter}, pre_send=lambda envelope: failure != "revoked")
    assert adapter.websocket.calls == []
    with db.engine.connect() as c:
        assert c.execute(select(outbox.c.state)).scalar_one() == "FAILED"


def test_artifact_symlink_is_rejected(tmp_path):
    root = tmp_path / "archive"
    artifact = archive(tmp_path, root)
    path = root / artifact.id
    path.unlink()
    path.symlink_to(tmp_path / "workspace" / "report.txt")
    with pytest.raises(OSError):
        read_artifact(root, artifact)


async def test_task_result_text_does_not_mark_file_delivered(db, clock, incoming, nap_config, nap_event, tmp_path):
    adapter = OneBotAdapter(nap_config, "d", incoming, clock)
    adapter.websocket, adapter.account_online = Socket(adapter, {}), True
    event = adapter.normalize(nap_event)
    service = TaskService(db, clock, Outbox(db, clock), archive_root=tmp_path / "custom-archive")
    access = AccessContext(principal=event.principal, conversation=event.conversation)
    service.accept(event, access, "create report")
    claim = service.claim("worker")
    artifact = archive(tmp_path, service.archive_root)
    result = {"status": "completed", "summary": "报告已生成", "artifacts": [artifact.model_dump()]}
    service.submit(claim["run_id"], claim["fencing_token"], "worker", result)
    service.submit(claim["run_id"], claim["fencing_token"], "worker", result)
    adapter.websocket.data = {"message_id": "text"}
    await service.outbox.dispatch_once({"nap": adapter})
    with db.engine.connect() as c:
        assert c.execute(select(outbox.c.state).order_by(outbox.c.part_no)).scalars().all() == ["SENT", "PENDING"]
        assert c.execute(select(notifications.c.state)).scalar_one() == "ACCEPTED"
    clock.advance(3)
    adapter.websocket.data = {"file_id": "file"}
    await service.outbox.dispatch_once({"nap": adapter})
    with db.engine.connect() as c:
        assert c.execute(select(notifications.c.state)).scalar_one() == "SENT"
    assert len(adapter.websocket.calls) == 2


async def test_official_artifact_requires_verified_content(official_config, incoming, clock, official_event, tmp_path):
    adapter = OfficialAdapter(official_config, "d", incoming, clock)
    event = adapter.normalize(official_event)
    envelope = outgoing(event, clock, archive(tmp_path, tmp_path / "archive"))
    receipt = await adapter.send(envelope)
    assert receipt.state == "FAILED" and receipt.error_code == "ARTIFACT_UNAVAILABLE"
    await adapter.stop()


async def test_offline_file_resumes_without_losing_reference(db, clock, incoming, nap_config, nap_event, tmp_path):
    adapter = OneBotAdapter(nap_config, "d", incoming, clock)
    adapter.websocket = Socket(adapter, {"file_id": "file"})
    event = adapter.normalize(nap_event)
    service = Outbox(db, clock)
    with db.transaction() as c:
        service.enqueue(c, outgoing(event, clock, archive(tmp_path, service.archive_root)))
    await service.dispatch_once({"nap": adapter})
    assert adapter.websocket.calls == []
    adapter.account_online = True
    await service.dispatch_once({"nap": adapter})
    assert len(adapter.websocket.calls) == 1
    with db.engine.connect() as c:
        assert c.execute(select(outbox.c.state)).scalar_one() == "SENT"


async def test_file_send_exception_is_unknown(db, clock, incoming, nap_config, nap_event, tmp_path):
    adapter = OneBotAdapter(nap_config, "d", incoming, clock)
    adapter.account_online = True
    calls = []

    class LostSocket:
        async def send_json(self, payload):
            calls.append(payload)
            raise OSError("connection lost after transmission")

    adapter.websocket = LostSocket()
    event = adapter.normalize(nap_event)
    service = Outbox(db, clock)
    with db.transaction() as c:
        service.enqueue(c, outgoing(event, clock, archive(tmp_path, service.archive_root)))
    await service.dispatch_once({"nap": adapter})
    await service.dispatch_once({"nap": adapter})
    assert len(calls) == 1
    with db.engine.connect() as c:
        assert c.execute(select(outbox.c.state)).scalar_one() == "UNKNOWN"


async def test_official_result_contains_attachment_manifest(db, clock, incoming, official_config, official_event, tmp_path):
    adapter = OfficialAdapter(official_config, "d", incoming, clock)
    event = adapter.normalize(official_event)
    service = TaskService(db, clock, Outbox(db, clock))
    access = AccessContext(principal=event.principal, conversation=event.conversation)
    service.accept(event, access, "create report")
    claim = service.claim("worker")
    artifact = archive(tmp_path, service.archive_root)
    service.submit(claim["run_id"], claim["fencing_token"], "worker", {
        "status": "completed", "summary": "报告已生成", "artifacts": [artifact.model_dump()],
    })
    with db.engine.connect() as c:
        envelopes = [json.loads(raw) for raw in c.execute(select(outbox.c.envelope_json)).scalars()]
    assert any("待发送文件保留24小时" in envelope["text"] for envelope in envelopes)
    assert not any("文件尚未发送" in envelope["text"] for envelope in envelopes)
    assert any(artifact.sha256 in envelope["text"] for envelope in envelopes)
    assert sum(envelope["artifact"] is not None for envelope in envelopes) == 1
    await adapter.stop()


async def test_deleted_source_blocks_pending_file_via_core(tmp_path, nap_config, nap_event, clock):
    from sqlalchemy import insert

    from qqbot.app import create_app
    from qqbot.config import Settings
    from qqbot.storage.social_schema import social_tombstones

    app = create_app(Settings(database=str(tmp_path / "core.db"), adapters=(nap_config,)),
                     clock=clock, run_background=False)
    core = app.state.core
    adapter = core.adapters["nap"]
    adapter.websocket, adapter.account_online = Socket(adapter, {"file_id": "file"}), True
    event = core.inbox.accept(adapter.normalize(nap_event))[0]
    core.submit_turn(event, text="create report", invited=True)
    claim = core.tasks.claim("worker")
    artifact = archive(tmp_path, core.tasks.archive_root)
    core.tasks.submit(claim["run_id"], claim["fencing_token"], "worker", {
        "status": "completed", "summary": "报告已生成", "artifacts": [artifact.model_dump()],
    })
    with core.db.engine.connect() as c:
        envelopes = [OutboundEnvelope.model_validate_json(raw)
                     for raw in c.execute(select(outbox.c.envelope_json)).scalars()]
    attachment = next(envelope for envelope in envelopes if envelope.artifact)
    assert core.pre_send(attachment)
    with core.db.transaction() as c:
        c.execute(insert(social_tombstones).values(source_id=event.event_id, conversation_id=event.conversation.key))
    assert not core.pre_send(attachment)
    await core.outbox.dispatch_once(core.adapters, pre_send=core.pre_send)
    assert adapter.websocket.calls == []
    adapter.websocket = None
    await core.stop()
    core.db.close()
