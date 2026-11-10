"""Official QQ upload receipts and delivered-message receipts are separate contracts."""
import base64
import hashlib
import json
import logging
from datetime import timedelta
from types import SimpleNamespace

import httpx
import pytest
from sqlalchemy import select
from test_artifact_delivery import archive, outgoing

from qqbot.adapters.qq_official.adapter import OfficialAdapter
from qqbot.domain import AdapterCapabilities, DeliveryCheck, SendReceipt, Surface
from qqbot.outbox.service import Outbox
from qqbot.storage.schema import outbox


def event_for(adapter, packet, surface):
    packet = {**packet, "d": {**packet["d"]}}
    if surface == Surface.PRIVATE:
        packet["t"] = "C2C_MESSAGE_CREATE"
        packet["d"].update(author={"user_openid": "300"})
    elif surface == Surface.GUILD_CHANNEL:
        packet["t"] = "AT_MESSAGE_CREATE"
        packet["d"].update(channel_id="200", author={"id": "300"})
    return adapter.normalize(packet)


def queue(service, event, clock, artifact):
    envelope = outgoing(event, clock, artifact).model_copy(update={"reply_context": event.reply_context})
    with service.db.transaction() as c:
        return service.enqueue(c, envelope)


def states(db):
    with db.engine.connect() as c:
        return c.execute(select(outbox.c.state).order_by(outbox.c.part_no)).scalars().all()


@pytest.mark.parametrize("surface", [Surface.GROUP, Surface.PRIVATE])
@pytest.mark.parametrize("name", ["report.pdf", "picture.png"])
async def test_official_upload_and_send_are_separate_durable_stages(
    db, clock, incoming, official_config, official_event, tmp_path, surface, name,
):
    calls = []

    def transport(request):
        calls.append(request)
        if request.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "fixture", "expires_in": 7200})
        if request.url.path.endswith("/files"):
            return httpx.Response(200, json={"file_uuid": "uploaded-only", "file_info": "media", "ttl": 300})
        assert request.url.path.endswith("/messages")
        return httpx.Response(200, json={"id": "delivered"})

    adapter = OfficialAdapter(official_config, "d", incoming, clock,
                              httpx.AsyncClient(transport=httpx.MockTransport(transport)))
    event = event_for(adapter, official_event, surface)
    service = Outbox(db, clock)
    artifact = archive(tmp_path, service.archive_root, name)
    queue(service, event, clock, artifact)
    await service.dispatch_once({"qq": adapter})
    clock.advance(20)
    await service.dispatch_once({"qq": adapter})
    assert states(db) == ["SENT"]
    assert len(calls) == 3
    path = "/v2/groups/200" if surface == Surface.GROUP else "/v2/users/300"
    assert calls[1].url.path == path + "/files"
    assert json.loads(calls[1].content) == {
        "file_type": 1 if name.endswith(".png") else 4, "file_name": name,
        "srv_send_msg": False, "file_data": base64.b64encode(b"content").decode(),
    }
    assert calls[2].url.path == path + "/messages"
    assert json.loads(calls[2].content) == {
        "content": "attachment", "msg_type": 7, "media": {"file_info": "media"},
        "msg_id": "400", "msg_seq": 1,
    }
    assert event.reply_context.max_replies == (4 if surface == Surface.PRIVATE else 5)
    await adapter.stop()


@pytest.mark.parametrize("surface", [Surface.GROUP, Surface.PRIVATE])
async def test_larger_local_files_use_authenticated_prepare_and_credential_free_cos_put(
    db, clock, incoming, official_config, official_event, tmp_path, surface, caplog,
):
    caplog.set_level(logging.INFO, logger="httpx")
    content = b"x" * 5_000_001
    chunks, confirms, calls = [], [], []
    block_size = 3_000_000
    path = "/v2/groups/200" if surface == Surface.GROUP else "/v2/users/300"

    def transport(request):
        calls.append(request)
        if request.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "fixture", "expires_in": 7200})
        if request.method == "PUT":
            assert request.url.host == "uploads.cos.ap-guangzhou.myqcloud.com"
            assert "authorization" not in request.headers
            assert "x-union-appid" not in request.headers
            assert "cookie" not in request.headers
            chunks.append(request.content)
            return httpx.Response(200)
        body = json.loads(request.content)
        assert request.headers["authorization"] == "QQBot fixture"
        if request.url.path.endswith("/upload_prepare"):
            assert request.url.path == path + "/upload_prepare"
            assert body == {
                "file_type": 4, "file_name": "report.pdf", "file_size": str(len(content)),
                "md5": hashlib.md5(content).hexdigest(), "sha1": hashlib.sha1(content).hexdigest(),
                "md5_10m": hashlib.md5(content[:10002432]).hexdigest(),
            }
            return httpx.Response(200, json={
                "upload_id": "upload", "block_size": str(block_size),
                "parts": [{"index": index, "block_size": str(min(block_size, len(content) - index * block_size)),
                           "presigned_url": f"https://uploads.cos.ap-guangzhou.myqcloud.com/file?part={index}&sign=secret"}
                          for index in range(2)],
            })
        if request.url.path.endswith("/upload_part_finish"):
            confirms.append(body)
            return httpx.Response(200, json={})
        if request.url.path.endswith("/files"):
            assert body == {"file_type": 4, "file_name": "report.pdf", "srv_send_msg": False, "upload_id": "upload"}
            return httpx.Response(200, json={"file_info": "media", "ttl": 300})
        return httpx.Response(200, json={"id": "delivered"})

    adapter = OfficialAdapter(official_config, "d", incoming, clock,
                              httpx.AsyncClient(transport=httpx.MockTransport(transport),
                                                headers={"Cookie": "private-cookie"}))
    event = event_for(adapter, official_event, surface)
    service = Outbox(db, clock)
    queue(service, event, clock, archive(tmp_path, service.archive_root, "report.pdf", content=content))
    await service.dispatch_once({"qq": adapter})
    assert states(db) == ["SENT"]
    assert b"".join(chunks) == content
    assert confirms == [{"upload_id": "upload", "part_index": index, "block_size": str(len(chunk)),
                         "md5": hashlib.md5(chunk).hexdigest()} for index, chunk in enumerate(chunks)]
    assert sum(request.url.path.endswith("/messages") for request in calls) == 1
    assert [request.url.query for request in calls if request.method == "PUT"] == [
        b"part=0&sign=secret", b"part=1&sign=secret",
    ]
    assert "uploads.cos.ap-guangzhou.myqcloud.com/file" in caplog.text
    assert "api.bot.qq.com" in caplog.text
    for secret in ("sign=secret", "private-cookie", "QQBot fixture", "file_info"):
        assert secret not in caplog.text
    with db.engine.connect() as c:
        assert "sign=secret" not in c.execute(select(outbox.c.receipt_json)).scalar_one()
    await adapter.stop()


@pytest.mark.parametrize("stage", ["upload", "message"])
@pytest.mark.parametrize("failure", ["timeout", "server_error"])
async def test_upload_uncertainty_is_retryable_but_message_uncertainty_is_not(
    db, clock, incoming, official_config, official_event, tmp_path, stage, failure,
):
    uploads, messages = [], []
    failed = False

    def transport(request):
        nonlocal failed
        if request.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "fixture", "expires_in": 7200})
        upload = request.url.path.endswith("/files")
        (uploads if upload else messages).append(json.loads(request.content))
        if not failed and upload == (stage == "upload"):
            failed = True
            if failure == "timeout":
                raise httpx.ReadTimeout("receipt lost")
            return httpx.Response(503)
        return httpx.Response(200, json={"file_info": "media", "ttl": 300} if upload else {"id": "delivered"})

    adapter = OfficialAdapter(official_config, "d", incoming, clock,
                              httpx.AsyncClient(transport=httpx.MockTransport(transport)))
    event = event_for(adapter, official_event, Surface.GROUP)
    service = Outbox(db, clock)
    queue(service, event, clock, archive(tmp_path, service.archive_root))
    await service.dispatch_once({"qq": adapter})
    assert states(db) == ["RETRYABLE" if stage == "upload" else "UNKNOWN"]
    clock.advance(11)
    await service.dispatch_once({"qq": adapter})
    assert states(db) == ["SENT" if stage == "upload" else "UNKNOWN"]
    assert len(uploads) == (2 if stage == "upload" else 1)
    assert len(messages) == 1
    assert messages[0]["msg_seq"] == 1
    await adapter.stop()


@pytest.mark.parametrize("changed", ["source", "reply_window", "media_ttl"])
async def test_official_rechecks_authority_and_reply_window_after_upload(
    db, clock, incoming, official_config, official_event, tmp_path, changed,
):
    uploads, messages = [], []
    allowed = True

    def transport(request):
        nonlocal allowed
        if request.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "fixture", "expires_in": 7200})
        if request.url.path.endswith("/files"):
            uploads.append(request)
            if changed == "source":
                allowed = False
            elif changed == "reply_window":
                clock.advance(301)
            return httpx.Response(200, json={"file_info": "media", "ttl": 1})
        messages.append(request)
        return httpx.Response(200, json={"id": "delivered"})

    checks = 0

    def authorize(envelope):
        nonlocal checks
        checks += 1
        if changed == "media_ttl" and checks == 3:
            clock.advance(2)
        return allowed

    adapter = OfficialAdapter(official_config, "d", incoming, clock,
                              httpx.AsyncClient(transport=httpx.MockTransport(transport)))
    event = event_for(adapter, official_event, Surface.GROUP)
    service = Outbox(db, clock)
    queue(service, event, clock, archive(tmp_path, service.archive_root))
    await service.dispatch_once({"qq": adapter}, pre_send=authorize)
    expected = {"source": "FAILED", "reply_window": "WAITING_FOR_TRIGGER", "media_ttl": "RETRYABLE"}
    assert states(db) == [expected[changed]]
    assert len(uploads) == 1 and messages == []
    if changed == "reply_window":
        renewed = event.model_copy(update={"reply_context": event.reply_context.model_copy(update={
            "platform_message_id": "401", "expires_at": clock.now() + timedelta(seconds=300),
        })})
        with db.transaction() as c:
            service.retrigger(c, renewed)
        # This retry will use the renewed reply credential, retaining the original delivery ID.
        changed = "renewed"
        await service.dispatch_once({"qq": adapter}, pre_send=authorize)
        assert states(db) == ["SENT"]
        assert json.loads(messages[0].content)["msg_id"] == "401"
        assert json.loads(messages[0].content)["msg_seq"] == 1
    await adapter.stop()


@pytest.mark.parametrize("bad_response", [{}, {"id": "upload-not-message"}, {"file_info": "media", "ttl": -1}])
async def test_upload_success_without_usable_file_info_never_marks_delivery_sent(
    db, clock, incoming, official_config, official_event, tmp_path, bad_response,
):
    paths = []

    def transport(request):
        paths.append(request.url.path)
        if request.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "fixture", "expires_in": 7200})
        return httpx.Response(200, json=bad_response)

    adapter = OfficialAdapter(official_config, "d", incoming, clock,
                              httpx.AsyncClient(transport=httpx.MockTransport(transport)))
    service = Outbox(db, clock)
    event = event_for(adapter, official_event, Surface.GROUP)
    queue(service, event, clock, archive(tmp_path, service.archive_root))
    await service.dispatch_once({"qq": adapter})
    assert states(db) == ["FAILED"]
    assert not any(path.endswith("/messages") for path in paths)
    await adapter.stop()


@pytest.mark.parametrize("surface", [Surface.GROUP, Surface.PRIVATE])
async def test_media_missing_actual_message_id_stays_unknown(
    db, clock, incoming, official_config, official_event, tmp_path, surface,
):
    paths = []

    def transport(request):
        paths.append(request.url.path)
        if request.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "fixture", "expires_in": 7200})
        return httpx.Response(200, json={"file_info": "media", "ttl": 300})

    adapter = OfficialAdapter(official_config, "d", incoming, clock,
                              httpx.AsyncClient(transport=httpx.MockTransport(transport)))
    service = Outbox(db, clock)
    event = event_for(adapter, official_event, surface)
    queue(service, event, clock, archive(tmp_path, service.archive_root))
    await service.dispatch_once({"qq": adapter})
    clock.advance(20)
    await service.dispatch_once({"qq": adapter})
    assert states(db) == ["UNKNOWN"]
    assert sum(path.endswith("/messages") for path in paths) == 1
    await adapter.stop()


@pytest.mark.parametrize("name", ["picture.png", "report.txt"])
async def test_guild_supports_multipart_images_and_explicitly_rejects_regular_files(
    db, clock, incoming, official_config, official_event, tmp_path, name,
):
    calls = []

    def transport(request):
        calls.append(request)
        if request.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "fixture", "expires_in": 7200})
        assert request.url.path == "/channels/200/messages"
        assert request.headers["content-type"].startswith("multipart/form-data;")
        assert b'name="file_image"; filename="picture.png"' in request.content
        assert b"content" in request.content and b"400" in request.content
        assert b"msg_seq" not in request.content
        return httpx.Response(200, json={"id": "image-delivered"})

    adapter = OfficialAdapter(official_config, "d", incoming, clock,
                              httpx.AsyncClient(transport=httpx.MockTransport(transport)))
    service = Outbox(db, clock)
    event = event_for(adapter, official_event, Surface.GUILD_CHANNEL)
    queue(service, event, clock, archive(tmp_path, service.archive_root, name))
    await service.dispatch_once({"qq": adapter})
    assert states(db) == ["SENT" if name.endswith("png") else "FAILED"]
    assert len(calls) == (2 if name.endswith("png") else 0)
    await adapter.stop()


async def test_simple_artifact_adapter_send_signature_remains_compatible(
    db, clock, incoming, official_config, official_event, tmp_path,
):
    adapter = OfficialAdapter(official_config, "d", incoming, clock)
    event = event_for(adapter, official_event, Surface.GROUP)
    await adapter.stop()
    service = Outbox(db, clock)
    queue(service, event, clock, archive(tmp_path, service.archive_root))
    calls = []

    async def capabilities(target):
        return AdapterCapabilities(supports_files=True)

    async def validate(envelope):
        return DeliveryCheck(allowed=True, reason="OK")

    async def send(envelope):
        calls.append(envelope)
        return SendReceipt(state="SENT", platform_message_id="file")

    fake = SimpleNamespace(capabilities=capabilities, validate_delivery=validate, send=send)
    await service.dispatch_once({"qq": fake}, pre_send=lambda envelope: True)
    assert states(db) == ["SENT"] and len(calls) == 1


@pytest.mark.parametrize("url", [
    "http://uploads.cos.ap-guangzhou.myqcloud.com/file?sign=secret",
    "https://127.0.0.1/file?sign=secret",
    "https://untrusted.invalid/file?sign=secret",
    "https://user:password@uploads.cos.ap-guangzhou.myqcloud.com/file?sign=secret",
    "https://uploads.cos.ap-guangzhou.myqcloud.com:8443/file?sign=secret",
])
async def test_chunk_upload_rejects_untrusted_presigned_destinations(
    db, clock, incoming, official_config, official_event, tmp_path, url,
):
    calls = []
    content = b"x" * 5_000_001

    def transport(request):
        calls.append(request)
        if request.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "fixture", "expires_in": 7200})
        assert request.url.path.endswith("/upload_prepare")
        return httpx.Response(200, json={
            "upload_id": "upload", "block_size": str(len(content)),
            "parts": [{"index": 0, "block_size": str(len(content)), "presigned_url": url}],
        })

    adapter = OfficialAdapter(official_config, "d", incoming, clock,
                              httpx.AsyncClient(transport=httpx.MockTransport(transport)))
    service = Outbox(db, clock)
    event = event_for(adapter, official_event, Surface.GROUP)
    queue(service, event, clock, archive(tmp_path, service.archive_root, content=content))
    await service.dispatch_once({"qq": adapter})
    assert states(db) == ["FAILED"]
    assert len(calls) == 2
    with db.engine.connect() as c:
        receipt = c.execute(select(outbox.c.receipt_json)).scalar_one()
    assert "secret" not in receipt and "presigned" not in receipt
    await adapter.stop()


@pytest.mark.parametrize("problem", ["digest", "permission", "replies_exhausted"])
async def test_official_media_preflight_does_not_upload_disallowed_content(
    clock, incoming, official_config, official_event, tmp_path, problem,
):
    calls = []

    def transport(request):
        calls.append(request)
        raise AssertionError("preflight should reject before any external request")

    adapter = OfficialAdapter(official_config, "d", incoming, clock,
                              httpx.AsyncClient(transport=httpx.MockTransport(transport)))
    event = event_for(adapter, official_event, Surface.GROUP)
    artifact = archive(tmp_path, tmp_path / "artifacts")
    envelope = outgoing(event, clock, artifact).model_copy(update={
        "artifact_data": b"changed" if problem == "digest" else b"content",
        "reply_context": event.reply_context.model_copy(update={"msg_seq": 6 if problem == "replies_exhausted" else 1}),
    })
    receipt = await adapter.send_artifact(envelope, pre_send=lambda envelope: problem != "permission")
    assert receipt.state == ("WAITING_FOR_TRIGGER" if problem == "replies_exhausted" else "FAILED")
    assert calls == []
    await adapter.stop()


async def test_core_source_revoked_during_official_upload_blocks_actual_delivery(
    tmp_path, official_config, official_event, clock,
):
    from sqlalchemy import insert

    from qqbot.app import create_app
    from qqbot.config import Settings
    from qqbot.storage.social_schema import social_tombstones
    from qqbot.storage.task_schema import notifications

    core = create_app(Settings(database=str(tmp_path / "core.db"), adapters=(official_config,)),
                      clock=clock, run_background=False).state.core
    adapter = core.adapters["qq"]
    event = core.inbox.accept(adapter.normalize(official_event))[0]
    core.submit_turn(event, text="create report", invited=True)
    claim = core.tasks.claim("worker")
    artifact = archive(tmp_path, core.tasks.archive_root)
    core.tasks.submit(claim["run_id"], claim["fencing_token"], "worker", {
        "status": "completed", "summary": "报告已生成", "artifacts": [artifact.model_dump()],
    })
    uploads, messages = [], []

    def transport(request):
        if request.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "fixture", "expires_in": 7200})
        if request.url.path.endswith("/files"):
            uploads.append(request)
            with core.db.transaction() as c:
                c.execute(insert(social_tombstones).values(
                    source_id=event.event_id, conversation_id=event.conversation.key,
                ))
            return httpx.Response(200, json={"file_info": "media", "ttl": 300})
        messages.append(json.loads(request.content))
        return httpx.Response(200, json={"id": "text-delivered"})

    await adapter.client.aclose()
    adapter.client = httpx.AsyncClient(transport=httpx.MockTransport(transport))
    await core.outbox.dispatch_once(core.adapters, pre_send=core.pre_send)
    assert states(core.db) == ["SENT", "PENDING"]
    clock.advance(3)
    await core.outbox.dispatch_once(core.adapters, pre_send=core.pre_send)
    assert states(core.db) == ["SENT", "FAILED"]
    assert len(uploads) == 1
    assert len(messages) == 1 and messages[0]["msg_type"] == 0
    with core.db.engine.connect() as c:
        assert c.execute(select(notifications.c.state)).scalar_one() == "ACCEPTED"
    await core.stop()
    core.db.close()


def test_cos_log_filter_only_redacts_typed_url_arguments():
    from qqbot.adapters.qq_official.adapter import _CosUploadLogFilter

    logger = logging.getLogger("httpx")
    assert sum(isinstance(item, _CosUploadLogFilter) for item in logger.filters) == 1
    original = httpx.URL("https://uploads.cos.ap-guangzhou.myqcloud.com/file?sign=secret&file_info=secret")
    public = httpx.URL("https://example.com/search?q=keep-this")
    record = logging.LogRecord("httpx", logging.INFO, __file__, 1, "%s %s %s", (
        "body text ?query=keep-this", original, public,
    ), None)
    assert logger.filter(record)
    assert record.getMessage() == (
        "body text ?query=keep-this https://uploads.cos.ap-guangzhou.myqcloud.com/file "
        "https://example.com/search?q=keep-this"
    )
    assert original.query == b"sign=secret&file_info=secret"


async def test_guild_mixed_result_delivers_images_before_unsupported_files(
    db, clock, incoming, official_config, official_event, tmp_path,
):
    from qqbot.domain import AccessContext
    from qqbot.jobs.service import TaskService
    from qqbot.storage.task_schema import notifications

    calls = []

    def transport(request):
        calls.append(request)
        if request.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "fixture", "expires_in": 7200})
        assert request.url.path == "/channels/200/messages"
        return httpx.Response(200, json={"id": "delivered"})

    adapter = OfficialAdapter(official_config, "d", incoming, clock,
                              httpx.AsyncClient(transport=httpx.MockTransport(transport)))
    event = event_for(adapter, official_event, Surface.GUILD_CHANNEL)
    service = TaskService(db, clock, Outbox(db, clock))
    service.accept(event, AccessContext(principal=event.principal, conversation=event.conversation), "make files")
    claim = service.claim("worker")
    file = archive(tmp_path, service.archive_root)
    image = archive(tmp_path, service.archive_root, "picture.png")
    service.submit(claim["run_id"], claim["fencing_token"], "worker", {
        "status": "completed", "summary": "文件已生成", "artifacts": [file.model_dump(), image.model_dump()],
    })
    await service.outbox.dispatch_once({"qq": adapter})
    clock.advance(3)
    await service.outbox.dispatch_once({"qq": adapter})
    assert states(db) == ["SENT", "SENT", "FAILED"]
    assert len(calls) == 3
    assert calls[-1].headers["content-type"].startswith("multipart/form-data;")
    with db.engine.connect() as c:
        assert c.execute(select(notifications.c.state)).scalar_one() == "ACCEPTED"
    await adapter.stop()
