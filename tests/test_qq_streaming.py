import asyncio
import json
import sys
from datetime import timedelta

import httpx
import pytest

from qqbot.adapters.qq_official.adapter import OfficialAdapter
from qqbot.adapters.qq_official.streaming import OfficialStream, send_typing
from qqbot.domain import OutboundEnvelope, Surface
from qqbot.harness.pi import PiRunnerAdapter, PublicAnswerEvents


def outgoing(adapter, clock, official_event, *, private=True):
    event = adapter.normalize({**official_event, "t": "C2C_MESSAGE_CREATE", "d": {
        **official_event["d"], "author": {"user_openid": "300"}}})
    target = event.conversation if private else event.conversation.model_copy(update={"surface": Surface.GROUP})
    return OutboundEnvelope(delivery_id="delivery", target=target, text="answer", origin_id="origin",
        reply_context=event.reply_context.model_copy(update={"msg_seq": 1}),
        expires_at=clock.now() + timedelta(seconds=300))


def adapter_with(config, incoming, clock, response=None):
    calls = []
    async def transport(request):
        if request.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "fixture", "expires_in": 7200})
        calls.append(request)
        if response:
            return await response(request)
        return httpx.Response(200, json={"id": "stream-id", "remain_msg_len": 0})
    return OfficialAdapter(config, "d", incoming, clock,
        httpx.AsyncClient(transport=httpx.MockTransport(transport))), calls


async def no_sleep(_):
    pass


async def test_stream_coalesces_finishes_and_keeps_prefix(official_config, incoming, clock, official_event):
    qq, calls = adapter_with(official_config, incoming, clock)
    stream = OfficialStream(qq, outgoing(qq, clock, official_event), monotonic=lambda: 0, sleep=no_sleep)
    assert (await stream.append("hello")).state == "SENT"
    assert await stream.append(" world") is None
    assert len(calls) == 1
    assert (await stream.finish()).state == "SENT"
    assert (await stream.finish()).state == "SENT"
    bodies = [json.loads(call.content) for call in calls]
    assert bodies == [
        {"input_mode":"replace","input_state":1,"index":0,"content_type":"markdown",
         "content_raw":"hello","msg_id":"400","msg_seq":1},
        {"input_mode":"replace","input_state":10,"index":1,"content_type":"markdown",
         "content_raw":"hello world","msg_id":"400","msg_seq":1,"stream_msg_id":"stream-id"},
    ]
    assert all(call.url.path == "/v2/users/300/stream_messages" for call in calls)
    with pytest.raises(RuntimeError):
        await stream.append("late")
    await qq.stop()


async def test_stream_cancel_discards_pending_and_is_idempotent(official_config, incoming, clock, official_event):
    qq, calls = adapter_with(official_config, incoming, clock)
    stream = OfficialStream(qq, outgoing(qq, clock, official_event), monotonic=lambda: 0, sleep=no_sleep)
    await stream.append("visible")
    await stream.append(" not yet visible")
    await stream.cancel()
    await stream.cancel()
    assert stream.state == "CANCELLED"
    assert json.loads(calls[-1].content)["content_raw"] == "visible"
    assert json.loads(calls[-1].content)["input_state"] == 10
    assert len(calls) == 2
    await qq.stop()


@pytest.mark.parametrize("mode,state", [("timeout","UNKNOWN"),("server","UNKNOWN"),
    ("bad_json","UNKNOWN"),("bad_id","UNKNOWN"),("forbidden","FAILED"),
    ("connect","RETRYABLE"),("rate","RETRYABLE"),("auth","RETRYABLE")])
async def test_stream_receipt_classification(official_config, incoming, clock, official_event, mode, state):
    async def response(request):
        if mode == "timeout":
            raise httpx.ReadTimeout("unknown")
        if mode == "connect":
            raise httpx.ConnectTimeout("not sent")
        return {"server": httpx.Response(503), "bad_json": httpx.Response(200,text="x"),
            "bad_id": httpx.Response(200,json={"id":7}), "forbidden": httpx.Response(403),
            "rate": httpx.Response(429), "auth": httpx.Response(401)}[mode]
    qq, calls = adapter_with(official_config, incoming, clock, response)
    stream = OfficialStream(qq, outgoing(qq, clock, official_event))
    assert (await stream.append("hello")).state == state
    assert stream.index == 0 and stream.stream_id is None
    if state in {"UNKNOWN", "FAILED"}:
        with pytest.raises(RuntimeError):
            await stream.finish()
        await stream.cancel()
        assert len(calls) == 1
    await qq.stop()


async def test_stream_capacity_scope_expiry_and_typing(official_config, incoming, clock, official_event):
    qq, calls = adapter_with(official_config, incoming, clock)
    envelope = outgoing(qq, clock, official_event)
    with pytest.raises(ValueError, match="C2C"):
        OfficialStream(qq, outgoing(qq, clock, official_event, private=False))
    assert (await send_typing(qq, outgoing(qq, clock, official_event, private=False))).error_code == "C2C_ONLY"
    with pytest.raises(ValueError):
        await send_typing(qq, envelope, seconds=61)
    assert (await send_typing(qq, envelope, seconds=60)).state == "SENT"
    body = json.loads(calls[-1].content)
    assert body["input_notify"] == {"input_type": 1, "input_second": 60}
    stream = OfficialStream(qq, envelope, max_chars=10)
    with pytest.raises(ValueError):
        await stream.append("a" * 11)
    await stream.append("ok")
    stream.remain_msg_len = 1
    await stream.append("xx")
    assert stream.text == "okxx"
    clock.advance(301)
    assert (await stream.finish()).error_code == "DELIVERY_EXPIRED"
    await qq.stop()


async def test_cancelled_request_cannot_be_replayed(official_config, incoming, clock, official_event):
    async def response(request):
        raise asyncio.CancelledError
    qq, calls = adapter_with(official_config, incoming, clock, response)
    stream = OfficialStream(qq, outgoing(qq, clock, official_event))
    with pytest.raises(asyncio.CancelledError):
        await stream.append("hello")
    assert stream.state == "UNKNOWN"
    with pytest.raises(RuntimeError):
        await stream.finish()
    assert len(calls) == 1
    await qq.stop()


def pi_event(kind, **extra):
    return {"type": kind, "message": {"role": "assistant"}, **extra}


def test_public_events_never_thinking_or_tools_and_final_only_settled():
    project = PublicAnswerEvents()
    start = project.accept(pi_event("message_start"))[0]
    for kind in ["thinking_delta", "toolcall_delta", "unknown"]:
        assert project.accept(pi_event("message_update", assistantMessageEvent={"type":kind,"delta":"secret"})) == []
    assert project.accept({"type":"message_update","message":{"role":"toolResult"},
        "assistantMessageEvent":{"type":"text_delta","delta":"tool secret"}}) == []
    assert project.accept(pi_event("message_update", assistantMessageEvent={"type":"text_delta","delta":"a"*400}))[0]["text"] == "a"*144
    end = project.accept({"type":"message_end","message":{"role":"assistant", "content":[
        {"type":"thinking","thinking":"secret"}, {"type":"text","text":"a"*400}]}})[0]
    assert end["type"] == "public_answer_end" and end["text"] == "a"*400
    assert end["message_id"] == start["message_id"]
    assert project.accept({"type":"agent_end"}) == []
    assert project.accept({"type":"agent_settled"})[0]["type"] == "public_answer_final"
    assert project.accept({"type":"agent_settled"}) == []


def test_public_events_redact_and_reject_changed_final_or_tool_protocol():
    project = PublicAnswerEvents(redact=lambda text: text.replace("SECRET", "[REDACTED]"))
    project.accept(pi_event("message_start"))
    project.accept(pi_event("message_update", assistantMessageEvent={"type":"text_delta", "delta":"SECRET " + "a"*400}))
    end = project.accept({"type":"message_end","message":{"role":"assistant","content":[
        {"type":"text","text":"SECRET " + "a"*400}]}})[0]
    assert "SECRET" not in end["text"] and "[REDACTED]" in end["text"]
    project.accept(pi_event("message_start"))
    project.accept(pi_event("message_update", assistantMessageEvent={"type":"text_delta", "delta":"a"*400}))
    end = project.accept({"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"changed"}]}})[0]
    assert end["error"]
    assert project.accept({"type":"agent_settled"}) == []
    project.accept(pi_event("message_start"))
    end = project.accept({"type":"message_end","message":{"role":"assistant","content":[
        {"type":"text","text":"<|DSML| calls><|DSML| invoke"}]}})[0]
    assert end["error"] and not end["text"]


async def test_authorization_rechecked_after_token_refresh(official_config, incoming, clock, official_event):
    qq, calls = adapter_with(official_config, incoming, clock)
    envelope = outgoing(qq, clock, official_event)
    authorized = True
    original = qq.access_token

    async def refresh_and_revoke():
        nonlocal authorized
        token = await original()
        authorized = False
        return token

    qq.access_token = refresh_and_revoke
    stream = OfficialStream(qq, envelope, authorize=lambda: authorized)
    assert (await stream.append("hello")).error_code == "AUTHORIZATION_CHANGED"
    assert stream.state == "FAILED" and not calls
    authorized = True
    assert (await send_typing(qq, envelope, authorize=lambda: authorized)).error_code == "AUTHORIZATION_CHANGED"
    assert not calls
    await qq.stop()


async def test_authorization_rechecked_after_finish_throttle(official_config, incoming, clock, official_event):
    qq, calls = adapter_with(official_config, incoming, clock)
    authorized = True

    async def revoke_during_sleep(_):
        nonlocal authorized
        authorized = False

    stream = OfficialStream(qq, outgoing(qq, clock, official_event), monotonic=lambda: 0,
                            sleep=revoke_during_sleep, authorize=lambda: authorized)
    assert (await stream.append("hello")).state == "SENT"
    assert (await stream.finish()).error_code == "AUTHORIZATION_CHANGED"
    assert len(calls) == 1 and stream.state == "FAILED"
    await qq.stop()


async def test_reply_expiry_during_token_refresh_blocks_send(official_config, incoming, clock, official_event):
    qq, calls = adapter_with(official_config, incoming, clock)
    envelope = outgoing(qq, clock, official_event)
    original = qq.access_token
    async def refresh_late():
        token = await original()
        clock.advance(3601)
        return token
    qq.access_token = refresh_late
    stream = OfficialStream(qq, envelope)
    assert (await stream.append("hello")).state == "FAILED"
    assert not calls
    await qq.stop()


def public_projection(tmp_path, secret=None):
    adapter = PiRunnerAdapter(sys.executable, tmp_path, api_key=secret)
    return adapter.public_answers


def public_delta(project, text):
    return project.accept(pi_event("message_update", assistantMessageEvent={
        "type": "text_delta", "delta": text}))


def test_public_prefix_streams_short_chinese_and_finishes(tmp_path):
    project = public_projection(tmp_path)
    project.accept(pi_event("message_start"))
    assert public_delta(project, "你好，正在查询。")[0]["text"] == "你好，正在查询。"
    end = project.accept(pi_event("message_end", message={"role": "assistant",
        "content": [{"type": "text", "text": "你好，正在查询。"}]}))[0]
    assert not end["error"] and end["text"] == "你好，正在查询。"
    assert project.accept({"type": "agent_settled"})[0]["text"] == end["text"]


def test_public_prefix_accepts_compact_rpc_updates_only_inside_assistant_message(tmp_path):
    project = public_projection(tmp_path)
    update = {"type": "message_update", "assistantMessageEvent": {"type": "text_delta", "delta": "你好"}}
    assert project.accept(update) == []
    start = project.accept(pi_event("message_start"))[0]
    assert project.accept(update)[0]["text"] == "你好"
    end = project.accept(pi_event("message_end", message={"role": "assistant",
        "content": [{"type": "text", "text": "你好"}]}))[0]
    assert end["message_id"] == start["message_id"]
    assert project.accept(update) == []
    project.accept(pi_event("message_start"))
    project.accept({"type": "message_start", "message": {"role": "toolResult"}})
    assert project.accept(update) == []


@pytest.mark.parametrize("secret", ["credential-123456789", "aaaaaaaaaa"])
def test_public_prefix_holds_split_known_credentials(tmp_path, secret):
    for split in range(1, len(secret)):
        project = public_projection(tmp_path, secret)
        project.accept(pi_event("message_start"))
        first = public_delta(project, "你好 " + secret[:split])
        assert first[0]["text"] == "你好 "
        second = public_delta(project, secret[split:] + " 完成，凭据已经安全隐藏。")
        assert second[0]["text"] == "你好 [REDACTED] 完成，凭据已经安全隐藏。"


def test_public_prefix_holds_split_bearer_and_protocol(tmp_path):
    text = "你好 Bearer unknown-token 完成"
    for split in range(1, len(text)):
        project = public_projection(tmp_path)
        project.accept(pi_event("message_start"))
        events = public_delta(project, text[:split]) + public_delta(project, text[split:])
        events += project.accept(pi_event("message_end", message={"role": "assistant",
            "content": [{"type": "text", "text": text}]}))
        assert all("unknown" not in event["text"] for event in events)
        assert events[-1]["text"] == "你好 Bearer [REDACTED] 完成"
    project.accept(pi_event("message_start"))
    assert public_delta(project, "你好 <")[0]["text"] == "你好 "
    assert public_delta(project, "|DSML| calls><|DSML| invoke") == []
    end = project.accept(pi_event("message_end", message={"role": "assistant",
        "content": [{"type": "text", "text": "你好 <|DSML| calls><|DSML| invoke"}]}))[0]
    assert end["error"] and end["text"] == "你好 "
    assert project.accept({"type": "agent_settled"}) == []


def test_public_prefix_failed_message_never_finalizes(tmp_path):
    project = public_projection(tmp_path)
    project.accept(pi_event("message_start"))
    assert public_delta(project, "你好")[0]["text"] == "你好"
    end = project.accept(pi_event("message_end", message={"role": "assistant",
        "stopReason": "error", "content": [{"type": "text", "text": "你好"}]}))[0]
    assert end["error"] and end["text"] == "你好"
    assert project.accept({"type": "agent_settled"}) == []


def test_public_prefix_preserves_truncated_credential_redaction(tmp_path):
    secret = "credential-123456789"
    project = public_projection(tmp_path, secret)
    project.accept(pi_event("message_start"))
    assert public_delta(project, secret[-8:][:3]) == []
    assert public_delta(project, secret[-8:][3:] + " 完成")[0]["text"] == "[REDACTED] 完成"
    end = project.accept(pi_event("message_end", message={"role": "assistant",
        "content": [{"type": "text", "text": secret[-8:] + " 完成"}]}))[0]
    assert not end["error"] and end["text"] == "[REDACTED] 完成"


def test_public_prefix_bounds_events_for_ten_thousand_single_character_chunks(tmp_path):
    project = public_projection(tmp_path)
    project.accept(pi_event("message_start"))
    events = public_delta(project, "好")
    assert events[0]["text"] == "好"
    for _ in range(9999):
        events.extend(public_delta(project, "好"))
    assert len(events) == 625
    assert all(event["type"] == "public_answer_delta" for event in events)
    end = project.accept(pi_event("message_end", message={"role": "assistant",
        "content": [{"type": "text", "text": "好" * 10000}]}))[0]
    assert not end["error"] and end["text"] == "好" * 10000
