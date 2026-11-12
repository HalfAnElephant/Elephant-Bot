import json
from datetime import timedelta
from uuid import uuid4

import httpx
import pytest
from pydantic import ValidationError
from sqlalchemy import select

from qqbot.adapters.qq_official.adapter import OfficialAdapter
from qqbot.domain import ConversationRef, OutboundEnvelope, ReplyContext, Surface
from qqbot.domain.official import OfficialContent, OfficialMedia, TuwenCard
from qqbot.outbox.service import Outbox
from qqbot.storage.schema import outbox


def target(surface=Surface.PRIVATE):
    return ConversationRef(deployment_id="d", adapter_instance_id="qq", bot_identity_id="bot",
                           surface=surface, remote_conversation_id="300")


def envelope(clock, **kwargs):
    values = dict(delivery_id=str(uuid4()), target=target(), text="hello", origin_id="msg-1",  # noqa: C408
        expires_at=clock.now()+timedelta(minutes=5), reply_context=ReplyContext(platform_message_id="in-1",
        expires_at=clock.now()+timedelta(minutes=5), msg_seq=1))
    return OutboundEnvelope(**(values | kwargs))


def adapter(config, incoming, clock, handler):
    return OfficialAdapter(config, "d", incoming, clock,
        httpx.AsyncClient(transport=httpx.MockTransport(handler)))


async def test_markdown_preserved_and_encoded(official_config, incoming, clock, db):
    calls=[]
    def handle(request):
        body=json.loads(request.content); calls.append((request.url.path,body))
        return httpx.Response(200,json={"access_token":"tok","expires_in":7200} if "Token" in request.url.path
                              else {"id":"out-1"})
    qq=adapter(official_config,incoming,clock,handle)
    service=Outbox(db,clock,markdown_adapters={"qq"})
    text="# Heading\n\n**bold**\n```python\nprint(1)\n```"
    with db.transaction() as c:
        service.enqueue_text(c,target=target(),text=text,origin_kind="chat",origin_id="md",
            reply_context=envelope(clock).reply_context)
    await service.dispatch_once({"qq":qq})
    assert calls[-1][1]["markdown"] == {"content":text}
    assert calls[-1][1]["msg_type"] == 2
    with db.engine.connect() as c:
        row=c.execute(select(outbox)).mappings().one()
    assert row["state"] == "SENT"
    assert OutboundEnvelope.model_validate_json(row["envelope_json"]).text == text


@pytest.mark.parametrize(("kind","file_type"),[("image",1),("audio",3),("video",2),("file",4)])
async def test_media_upload_no_auto_send_and_destination_bound(kind,file_type,official_config,incoming,clock):
    calls=[]
    def handle(request):
        body=json.loads(request.content);calls.append((request.url.path,body))
        if "Token" in request.url.path:return httpx.Response(200,json={"access_token":"tok","expires_in":7200})
        return httpx.Response(200,json={"file_info":"scoped-info"} if request.url.path.endswith("files") else {"id":"sent"})
    qq=adapter(official_config,incoming,clock,handle)
    message=envelope(clock,text="",official_content=OfficialContent(media=OfficialMedia(type=kind,url="https://example.com/a")))
    result=await qq.send(message)
    assert result.state == "SENT"
    assert calls[-2][0] == "/v2/users/300/files"
    assert calls[-2][1]["srv_send_msg"] is False
    assert calls[-2][1]["file_type"] == file_type
    assert calls[-1][1]["media"] == {"file_info":"scoped-info"}
    assert calls[-1][1]["msg_seq"] == 1


async def test_upload_failure_cannot_send(official_config,incoming,clock):
    calls=[]
    def handle(request):
        calls.append(request.url.path)
        return httpx.Response(200,json={"access_token":"tok","expires_in":7200}) if "Token" in request.url.path else httpx.Response(400)
    qq=adapter(official_config,incoming,clock,handle)
    result=await qq.send(envelope(clock,official_content=OfficialContent(media=OfficialMedia(type="image",url="https://example.com/a"))))
    assert result.state == "FAILED"
    assert not any(path.endswith("messages") for path in calls)


async def test_card_and_surface_rejection(official_config,incoming,clock):
    calls=[]
    def handle(request):
        calls.append(json.loads(request.content))
        return httpx.Response(200,json={"access_token":"tok","expires_in":7200} if "Token" in request.url.path else {"id":"ok"})
    qq=adapter(official_config,incoming,clock,handle)
    rich=OfficialContent(card=TuwenCard(title="Title",description="Detail",url="https://example.com",pic_url="https://example.com/a.png"))
    assert (await qq.send(envelope(clock,text="",official_content=rich))).state == "SENT"
    assert calls[-1]["msg_type"] == 8
    assert calls[-1]["card"]["type"] == "tuwen"
    count=len(calls)
    result=await qq.send(envelope(clock,target=target(Surface.GUILD_CHANNEL),official_content=rich))
    assert result.error_code == "UNSUPPORTED_SURFACE" and len(calls)==count


@pytest.mark.parametrize("url",["file:///tmp/key","http://example.com/a","https://127.0.0.1/a",
    "https://[::1]/a","https://localhost/a","https://a.internal/a","https://user:pw@example.com/a"])
def test_unsafe_media_urls_rejected(url):
    with pytest.raises(ValidationError):OfficialMedia(type="image",url=url)


def test_official_attachments_and_structures_preserved(official_config,incoming,clock,official_event):
    qq=adapter(official_config,incoming,clock,lambda r:httpx.Response(500))
    event=official_event|{"d":official_event["d"]|{"attachments":[
        {"url":"//example.com/a.png","content_type":"image/png","size":3},
        {"url":"https://example.com/a.mp4","content_type":"video/mp4"}],
        "msg_elements":[{"type":"forward","children":[{"text":"inner"}]}],
        "message_reference":{"message_id":"REFIDX_1"}}}
    normalized=qq.normalize(event)
    image,video=normalized.segments[1:3]
    assert image.image_url == "https://example.com/a.png" and image.size == 3
    assert video.type == "video" and video.source_url.endswith("mp4")
    assert normalized.reply_to == "REFIDX_1"
    assert normalized.segments[-1].payload["value"][0]["children"][0]["text"] == "inner"


async def test_capabilities_are_surface_specific(official_config,incoming,clock):
    qq=adapter(official_config.model_copy(update={"streaming_enabled":True,"typing_enabled":True}),incoming,clock,lambda r:httpx.Response(500))
    private=await qq.capabilities(target())
    group=await qq.capabilities(target(Surface.GROUP))
    channel=await qq.capabilities(target(Surface.GUILD_CHANNEL))
    assert private.supports_streaming and private.supports_typing and private.supports_markdown
    assert not group.supports_streaming and group.supports_markdown
    assert channel.supports_markdown and channel.supports_images
    assert not channel.supports_audio and not channel.supports_files


def test_rich_payload_recovery_is_not_plain_text(db,clock):
    service=Outbox(db,clock)
    message=envelope(clock,content_format="markdown",text="**keep**")
    with db.transaction() as c:
        ident=service.enqueue(c,message)
        row=c.execute(select(outbox).where(outbox.c.id==ident)).mappings().one()
        restored=service._prepare_pending(c,row)
    assert restored.text == "**keep**" and restored.content_format == "markdown"


def test_keyboard_json_restart_roundtrip(db,clock):
    from qqbot.adapters.qq_official.rendering import message_body
    from qqbot.domain.interactions import (
        QQButton,
        QQButtonAction,
        QQButtonRender,
        QQButtonRow,
        QQKeyboard,
        QQKeyboardContent,
    )
    keyboard=QQKeyboard(content=QQKeyboardContent(rows=(QQButtonRow(buttons=(
        QQButton(id="one",render_data=QQButtonRender(label="One"),action=QQButtonAction(type=1,data="token")),)),)))
    item=envelope(clock,content_format="markdown",official_content=OfficialContent(keyboard=keyboard.to_payload()))
    service=Outbox(db,clock)
    with db.transaction() as c:
        delivery=service.enqueue(c,item)
        row=c.execute(select(outbox).where(outbox.c.id==delivery)).mappings().one()
        restored=service._prepare_pending(c,row)
    assert message_body(restored)["keyboard"] == keyboard.to_payload()


async def test_template_disabled_for_official_adapter(official_config,incoming,clock):
    from qqbot.domain.official import TemplateMarkdown
    qq=adapter(official_config.model_copy(update={"markdown_enabled":False}),incoming,clock,lambda r:httpx.Response(500))
    item=envelope(clock,official_content=OfficialContent(template_markdown=TemplateMarkdown(template_id="a")))
    assert (await qq.validate_delivery(item)).reason == "MARKDOWN_DISABLED"


async def test_offline_outbox_remains_pending_until_reconnect(db,clock):
    from qqbot.domain import DeliveryCheck, SendReceipt
    class OfflineAdapter:
        online=False
        async def validate_delivery(self,item):
            return DeliveryCheck(allowed=self.online,reason="OK" if self.online else "OFFLINE")
        async def send(self,item):return SendReceipt(state="SENT",platform_message_id="ok")
    service=Outbox(db,clock);fake=OfflineAdapter()
    with db.transaction() as c:service.enqueue(c,envelope(clock))
    await service.dispatch_once({"qq":fake})
    with db.engine.connect() as c:assert c.execute(select(outbox.c.state)).scalar_one() == "PENDING"
    fake.online=True
    await service.dispatch_once({"qq":fake})
    with db.engine.connect() as c:assert c.execute(select(outbox.c.state)).scalar_one() == "SENT"


async def test_guild_direct_messages_have_separate_text_route(official_config,incoming,clock):
    sent=[]
    def handle(request):
        sent.append((request.url.path,json.loads(request.content)))
        return httpx.Response(200,json={"access_token":"tok","expires_in":7200} if "Token" in request.url.path else {"id":"sent"})
    qq=adapter(official_config,incoming,clock,handle)
    event=qq.normalize({"op":0,"id":"evt","t":"DIRECT_MESSAGE_CREATE","d":{
        "id":"msg","guild_id":"dm-guild","channel_id":"ch","author":{"id":"u"},
        "content":"hello","timestamp":clock.now().isoformat()}})
    assert event.conversation.surface == Surface.GUILD_PRIVATE
    item=envelope(clock,target=event.conversation)
    assert (await qq.send(item)).state == "SENT"
    assert sent[-1][0] == "/dms/dm-guild/messages"
    assert "msg_type" not in sent[-1][1]


@pytest.mark.parametrize("limit", ["official_messages_per_day", "official_bot_messages_per_minute",
                                    "official_messages_per_minute"])
async def test_official_local_quota_defers_second_delivery(limit, official_config, incoming, clock, db):
    calls = []
    def handle(request):
        if "Token" in request.url.path:
            return httpx.Response(200, json={"access_token": "tok", "expires_in": 7200})
        calls.append(request)
        return httpx.Response(200, json={"id": "out"})
    qq = adapter(official_config.model_copy(update={limit: 1}), incoming, clock, handle)
    service = Outbox(db, clock)
    with db.transaction() as c:
        service.enqueue(c, envelope(clock, origin_id="first"))
    await service.dispatch_once({"qq": qq})
    clock.advance(seconds=5)
    with db.transaction() as c:
        second = service.enqueue(c, envelope(clock, origin_id="second"))
    await service.dispatch_once({"qq": qq})
    assert len(calls) == 1
    with db.engine.connect() as c:
        assert c.execute(select(outbox.c.state).where(outbox.c.id == second)).scalar_one() == "PENDING"


@pytest.mark.parametrize("surface,path", [(Surface.GUILD_CHANNEL, "/channels/300/messages"),
                                          (Surface.GUILD_PRIVATE, "/dms/300/messages")])
@pytest.mark.parametrize("variant", ["image", "markdown", "template"])
async def test_guild_verified_rich_contract(surface, path, variant, official_config, incoming, clock):
    calls = []
    def handle(request):
        if "Token" in request.url.path:
            return httpx.Response(200, json={"access_token": "tok", "expires_in": 7200})
        calls.append((request.url.path, json.loads(request.content)))
        return httpx.Response(200, json={"id": "sent"})
    qq = adapter(official_config, incoming, clock, handle)
    kwargs = {"target": target(surface)}
    if variant == "image":
        kwargs["official_content"] = OfficialContent(media=OfficialMedia(type="image", url="https://example.com/image"))
    else:
        kwargs["official_content"] = OfficialContent.model_validate_json(json.dumps({
            "keyboard": {"id": "keyboard-template"}, "reference": {"message_id": "quoted"},
            **({"template_markdown": {"template_id": "template", "params": [{"key": "key", "values": ["value"]}]}}
               if variant == "template" else {})}))
        if variant == "markdown":
            kwargs["content_format"] = "markdown"
    assert (await qq.send(envelope(clock, **kwargs))).state == "SENT"
    assert len(calls) == 1 and calls[0][0] == path
    body = calls[0][1]
    assert "msg_type" not in body and "msg_seq" not in body and body["msg_id"] == "in-1"
    if variant == "image":
        assert body["image"] == "https://example.com/image" and "media" not in body
    else:
        assert body["keyboard"] == {"id": "keyboard-template"}
        assert body["message_reference"]["message_id"] == "quoted"
        assert body["markdown"] == ({"content": "hello"} if variant == "markdown" else
                                    {"custom_template_id": "template", "params": [{"key": "key", "values": ["value"]}]})


@pytest.mark.parametrize("kind", ["audio", "video", "file"])
async def test_guild_unsupported_media_never_sends(kind, official_config, incoming, clock):
    qq = adapter(official_config, incoming, clock, lambda request: pytest.fail("must not send"))
    result = await qq.send(envelope(clock, target=target(Surface.GUILD_CHANNEL),
        official_content=OfficialContent(media=OfficialMedia(type=kind, url="https://example.com/a"))))
    assert result.state == "FAILED" and result.error_code == "UNSUPPORTED_SURFACE"
