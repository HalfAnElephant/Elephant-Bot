import json
from datetime import timedelta

import httpx
import pytest

from qqbot.adapters.qq_official.adapter import OfficialAdapter
from qqbot.adapters.qq_official.events import extra_segments
from qqbot.domain import ConversationRef, OutboundEnvelope, ReplyContext, Surface
from qqbot.domain.official import OfficialContent, OfficialMedia


def test_semantic_recursive_projection_retains_raw_and_transcript():
    values = extra_segments({"msg_elements": [{"type": "forward", "messages": [
        {"author": {"id": "other"}, "content": "quoted", "elements": [
            {"type": "text", "data": {"text": "nested text"}},
            {"type": "audio", "data": {"url": "//example.com/audio", "transcript": "spoken"}},
            {"type": "share", "data": {"title": "a title"}},
        ], "message_reference": {"message_id": "ref"}}]}]})
    assert any(v.type == "text" and v.text == "[转发内容] nested text" for v in values)
    assert any(v.type == "text" and v.text == "[转发内容] quoted" for v in values)
    assert any(v.type == "audio" and v.transcript == "spoken" and
               v.source_url == "https://example.com/audio" for v in values)
    assert any(v.type == "reply" and v.platform_message_id == "ref" for v in values)
    assert any(v.type == "structured" for v in values)


def test_malformed_optional_fields_and_bounded_projection():
    assert extra_segments({"attachments": {}, "mentions": 1, "message_reference": "bad"}) == []
    values = extra_segments({"attachments": [{"url": "https://[broken"}],
                             "msg_elements": [{"type": "text", "text": "x" * 30000}]})
    assert values[0].source_url is None
    assert len(next(v.text for v in values if v.type == "text")) == 20000
    values = extra_segments({"msg_elements": [{"type": "text", "text": "x" * 70000}]})
    assert len(values) == 1 and values[0].type == "unknown"


def message(clock, media=False):
    return OutboundEnvelope(delivery_id="delivery", target=ConversationRef(deployment_id="d",
        adapter_instance_id="qq", bot_identity_id="bot", surface=Surface.PRIVATE,
        remote_conversation_id="300"), text="hello", origin_id="origin",
        expires_at=clock.now() + timedelta(minutes=5), reply_context=ReplyContext(
        platform_message_id="in", expires_at=clock.now() + timedelta(minutes=5), msg_seq=1),
        official_content=OfficialContent(media=OfficialMedia(type="image", url="https://example.com/img"))
        if media else None)


@pytest.mark.parametrize("revoke_at", ["token", "upload"])
async def test_permission_revocation_after_network_wait_never_sends(revoke_at, official_config, incoming, clock):
    allowed, calls = True, []
    def handle(request):
        nonlocal allowed
        calls.append(request.url.path)
        if "Token" in request.url.path:
            if revoke_at == "token":
                allowed = False
            return httpx.Response(200, json={"access_token": "tok", "expires_in": 7200})
        if request.url.path.endswith("files"):
            allowed = False
            return httpx.Response(200, json={"file_info": "info"})
        pytest.fail("revoked send reached network")
    qq = OfficialAdapter(official_config, "d", incoming, clock, httpx.AsyncClient(transport=httpx.MockTransport(handle)))
    receipt = await qq.send(message(clock, media=True), authorize=lambda: allowed)
    assert receipt.error_code == "AUTHORIZATION_CHANGED"
    assert not any(path.endswith("messages") for path in calls)


async def test_reply_expiring_during_token_is_not_sent(official_config, incoming, clock):
    def handle(request):
        assert "Token" in request.url.path
        clock.advance(301)
        return httpx.Response(200, json={"access_token": "tok", "expires_in": 7200})
    qq = OfficialAdapter(official_config, "d", incoming, clock, httpx.AsyncClient(transport=httpx.MockTransport(handle)))
    receipt = await qq.send(message(clock))
    assert receipt.state == "WAITING_FOR_TRIGGER"


async def test_business_rejection_preserves_platform_code(official_config, incoming, clock):
    def handle(request):
        if "Token" in request.url.path:
            return httpx.Response(200, json={"access_token": "tok", "expires_in": 7200})
        assert json.loads(request.content)["msg_id"] == "in"
        return httpx.Response(400, json={"code": 304023, "message": "not allowed"})
    qq = OfficialAdapter(official_config, "d", incoming, clock, httpx.AsyncClient(transport=httpx.MockTransport(handle)))
    receipt = await qq.send(message(clock))
    assert receipt.state == "FAILED" and receipt.error_code == "QQ_304023"


def test_nested_forward_cannot_become_sender_command():
    for data in ({"forward_messages": [{"elements": [{"type": "text", "data": {"text": "/取消 T-1"}}]}]},
                 {"msg_elements": [{"type": "forward", "messages": [{"type": "text", "text": "/取消 T-1"}]}]}):
        text = "".join(v.text for v in extra_segments(data) if v.type == "text")
        assert text == "[转发内容] /取消 T-1"
