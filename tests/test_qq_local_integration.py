import base64
import json

import httpx
import pytest
from sqlalchemy import select, update
from test_qq_agent_rich import rich_setup as _rich_setup
from test_qq_rich_messages import envelope, target

from qqbot.adapters.qq_official.adapter import OfficialAdapter
from qqbot.adapters.qq_official.local_media import LocalMediaStore
from qqbot.domain import PrincipalRef, Surface
from qqbot.domain.official import OfficialContent, OfficialMedia
from qqbot.harness.gateway import RunGateway
from qqbot.jobs.rich_delivery import local_file_upload, rich_status
from qqbot.storage.schema import outbox
from qqbot.storage.task_schema import tasks

rich_setup = _rich_setup

PNG = b"\x89PNG\r\n\x1a\n" + b"safe-test-image"


def payload(**overrides):
    return {"request_id": "file-one", "filename": "image.png", "media_type": "image",
            "mime_type": "image/png", "content_base64": base64.b64encode(PNG).decode(), **overrides}


async def local_file_send(service, args, body):
    body = dict(body)
    content = base64.b64decode(body.pop("content_base64"))
    body["size"] = len(content)
    async def chunks():
        yield content
    return await local_file_upload(service, args, body, chunks())


async def test_file_core_scoped_idempotent_and_mime_rejection(rich_setup):
    service, args, task, _ = rich_setup
    result = await local_file_send(service, args, payload())
    assert not result["delivered"] and result["state"] == "PENDING"
    assert await local_file_send(service, args, payload()) == result
    with pytest.raises(ValueError, match="different file"):
        await local_file_send(service, args, payload(content_base64=base64.b64encode(PNG + b"changed").decode()))
    with pytest.raises(ValueError, match="MIME"):
        await local_file_send(service, args, payload(request_id="wrong-mime", mime_type="application/pdf"))
    with service.db.engine.connect() as c:
        row = c.execute(select(outbox)).mappings().one()
        assert "content_base64" not in row["envelope_json"]
        assert "local_ref" in row["envelope_json"]
    with service.db.transaction() as c:
        c.execute(update(tasks).where(tasks.c.id == task["id"]).values(state="CANCEL_REQUESTED"))
    with pytest.raises(PermissionError):
        await local_file_send(service, args, payload(request_id="cancelled"))


async def test_run_gateway_local_output_to_core_durable_enqueue(rich_setup, tmp_path):
    service, args, task, _ = rich_setup
    (tmp_path / "outputs").mkdir()
    (tmp_path / "outputs" / "answer.png").write_bytes(PNG)
    async def call(op, data):
        if op == "authorize": return service.authorize(*args, data)
        if op == "ledger": return await service.ledger(*args, data)
        if op == "qq-file-send": return await local_file_send(service, args, data)
        if op == "qq-status": return rich_status(service, args, data)
        raise AssertionError(op)
    async def upload(metadata, content):
        return await local_file_upload(service, args, metadata, content)
    gateway = RunGateway(tmp_path, "offline", "fixture", "https://model.invalid/v1/chat/completions", call=call, upload=upload)
    gateway.bind({"task_id": task["id"], "run_id": args[0], "fencing_token": args[1],
        "permission_snapshot": {"principal": json.loads(task["permission_json"])["principal"]}})
    result = await gateway.tools.invoke("qq.file.send", {"request_id": "gateway-file", "path": "outputs/answer.png",
        "media_type": "image"}, gateway.context)
    assert result.status == "verified", result
    assert result.data["verified_effect"] == "durable_enqueue"
    assert result.data["delivered"] is False


@pytest.mark.parametrize("surface", [Surface.PRIVATE, Surface.GROUP, Surface.GUILD_CHANNEL, Surface.GUILD_PRIVATE])
async def test_local_binary_actual_adapter_transport(surface, official_config, incoming, clock, db, monkeypatch):
    from qqbot.adapters.qq_official import local_upload
    store = LocalMediaStore(db, clock)
    principal = PrincipalRef(adapter_instance_id="qq", identity_namespace="app", remote_user_id="u")
    ref = store.put(PNG, task_id="task", conversation_id=target(surface).key, principal_key=principal.key,
                    request_id="request", filename="answer.png", media_type="image")
    calls = []
    def handle(request):
        calls.append(request)
        path = request.url.path
        if path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "secret-token", "expires_in": 7200})
        if path.endswith("upload_prepare"):
            assert "file_data" not in json.loads(request.content)
            return httpx.Response(200, json={"upload_id": "u", "block_size": len(PNG),
                "parts": [{"index": 1, "presigned_url": "https://bucket-123.cos.ap-shanghai.myqcloud.com/file?signed=yes"}]})
        if request.method == "PUT":
            assert request.content == PNG and "authorization" not in request.headers
            return httpx.Response(200)
        if path.endswith("upload_part_finish"): return httpx.Response(200, json={})
        if path.endswith("files"): return httpx.Response(200, json={"file_info": "scoped-file"})
        if surface in {Surface.GUILD_CHANNEL, Surface.GUILD_PRIVATE}:
            assert "multipart/form-data" in request.headers["content-type"]
            assert b'name="file_image"' in request.content and PNG in request.content
            assert b"local_ref" not in request.content
        else:
            assert json.loads(request.content)["media"] == {"file_info": "scoped-file"}
        return httpx.Response(200, json={"id": "sent"})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        original = local_upload.upload_local
        async def upload(*args, **kwargs):
            return await original(*args, **kwargs, cos_client=client)
        monkeypatch.setattr(local_upload, "upload_local", upload)
        qq = OfficialAdapter(official_config, "d", incoming, clock, client)
        message = envelope(clock, target=target(surface), parent_task_id="task", requester=principal,
            official_content=OfficialContent(media=OfficialMedia(type="image", local_ref=ref["id"], filename="answer.png")))
        result = await qq.send(message)
        if surface in {Surface.GUILD_CHANNEL, Surface.GUILD_PRIVATE}:
            assert result.error_code == "LOCAL_MEDIA_LIMIT_UNVERIFIED"
            assert calls == []
            return
        assert result.state == "SENT"
        assert len([r for r in calls if r.url.path.endswith("messages")]) == 1
        denied = await qq.send(message.model_copy(update={"parent_task_id": "foreign-task"}))
        assert denied.error_code == "LOCAL_MEDIA_UNAVAILABLE"
        assert len([r for r in calls if r.url.path.endswith("messages")]) == 1


async def test_adapter_cancel_closes_late_opened_snapshot(official_config, incoming, clock, monkeypatch):
    import asyncio
    import tempfile
    import threading
    from uuid import uuid4

    entered, release = threading.Event(), threading.Event()
    snapshot = tempfile.TemporaryFile()  # noqa: SIM115 - transfer ownership to async completion callback
    def slow_open(*args, **kwargs):
        entered.set()
        release.wait(2)
        return {"media_type": "image", "filename": "image.png"}, snapshot
    monkeypatch.setattr(LocalMediaStore, "open", slow_open)
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200,
            json={"access_token": "token", "expires_in": 7200}))) as client:
        qq = OfficialAdapter(official_config, "d", incoming, clock, client)
        principal = PrincipalRef(adapter_instance_id="qq", identity_namespace="app", remote_user_id="u")
        message = envelope(clock, parent_task_id="task", requester=principal,
            official_content=OfficialContent(media=OfficialMedia(type="image", local_ref=str(uuid4()))))
        pending = asyncio.create_task(qq.send(message))
        assert await asyncio.to_thread(entered.wait, 1)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        release.set()
        for _ in range(100):
            if snapshot.closed:
                break
            await asyncio.sleep(.01)
        assert snapshot.closed
