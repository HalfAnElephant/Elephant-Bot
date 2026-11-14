"""Locked Pi retains spoken user inputs after temporary attachment context expires."""

import asyncio
import json

import pytest
from test_pi_bridge import compaction_session as real_pi_session

voice_session = real_pi_session


def voice(source, transcript, **metadata):
    return {"source": source, "message_id": source + "-message", "kind": "audio",
        "attachment_ref": "voice-" + source, "filename": "voice.silk", "mime_type": "voice",
        "transcript": transcript, "quoted": False, "is_current_input": True, **metadata}


def bind(state, run_id, goal, files):
    state.gateway.revoke()
    state.gateway.bind({**state.claim, "run_id": run_id, "session_id": state.runner.session_path.stem,
        "goal": goal, "context": {**state.claim["context"], "source_input_ids": [run_id], "files": files}})


def persisted_user_texts(state):
    values = []
    for line in state.runner.session_path.read_text().splitlines():
        entry = json.loads(line)
        message = entry.get("message", {})
        if entry.get("type") != "message" or message.get("role") != "user":
            continue
        content = message["content"]
        if isinstance(content, str):
            values.append(content)
        else:
            values.append("".join(part["text"] for part in content if part.get("type") == "text"))
    return values


@pytest.mark.parametrize("voice_session", [{"response_text": "收到。"}], indirect=True)
async def test_platform_voice_is_durable_after_attachment_window_and_many_new_inputs(voice_session):
    state = voice_session
    await state.runner.rpc("set_auto_compaction", enabled=False)
    spoken = "请记住我刚才说的编号AB123，先只回复收到。"
    bind(state, "voice-source", "[用户发送了语音]", [voice("voice-source", spoken)])
    first = await state.runner.run("[用户发送了语音]", keep_alive=True)
    assert first.summary == "收到。"
    users = persisted_user_texts(state)
    assert len([text for text in users if spoken in text]) == 1
    assert "[[elephant-input:voice-source]]" in next(text for text in users if spoken in text)
    assert state.runner.quiescent_verified and state.gateway.session_receipt["kind"] == "turn_finished"

    # Clearing files reproduces expiry of Core's ten-minute/eight-input attachment window.
    for index in range(9):
        bind(state, "later-" + str(index), "这是一条后续普通消息。", [])
        result = await state.runner.run("普通消息", keep_alive=True)
        assert result.summary == "收到。"
    bind(state, "recall-number", "我刚才口述的编号是什么？", [])
    await state.runner.run("查询之前的口述编号", keep_alive=True)
    assert state.gateway.claim["context"]["files"] == []
    latest = state.requests[-1]["payload"]
    assert any("AB123" in json.dumps(message, ensure_ascii=False) for message in latest["messages"]
               if message["role"] == "user")
    assert "AB123" in "\n".join(persisted_user_texts(state))
    assert all("AB123" not in json.dumps(message, ensure_ascii=False) for message in latest["messages"]
               if message["role"] == "assistant")


@pytest.mark.parametrize("voice_session", [{"response_text": "收到。"}], indirect=True)
@pytest.mark.parametrize("quoted", [False, True])
async def test_busy_pi_persists_only_the_pending_inputs_own_spoken_source(voice_session, quoted):
    state = voice_session
    await state.runner.rpc("set_auto_compaction", enabled=False)
    bind(state, "initial", "等待当前输入完成。", [])
    selected = voice("extra-a-source", "本条补充的编号是EXTRA_A_123。", quoted=quoted)
    historical = voice("historical-source", "历史资料编号HISTORY_B_456。", is_current_input=False)
    unrelated = voice("extra-b-source", "另一条尚未消费的编号PENDING_C_789。")
    pending = [{"id": "extra-a", "text": "用户补充[extra-a]：[语音]", "files": [selected]},
               {"id": "extra-b", "text": "用户补充[extra-b]：[语音]", "files": [unrelated]}]
    acknowledgements = []
    original_call = state.gateway.call

    async def host(operation, payload):
        if operation == "inputs":
            return pending
        if operation == "input-ack":
            acknowledgements.append(payload["input_id"])
            return {"accepted": True}
        return await original_call(operation, payload)

    state.gateway.call = host
    state.request_gate = asyncio.Event()
    running = asyncio.create_task(state.runner.run("等待当前输入完成", keep_alive=True))
    try:
        await asyncio.wait_for(state.request_started.wait(), 3)
        state.gateway.claim["context"]["files"] = [historical, selected, unrelated]
        await state.runner.append_input("extra-a", pending[0]["text"])
        state.request_gate.set()
        result = await asyncio.wait_for(running, 15)
        assert result.summary == "收到。"
        extra, = [text for text in persisted_user_texts(state) if "用户补充[extra-a]" in text]
        assert "EXTRA_A_123" in extra
        assert "HISTORY_B_456" not in extra and "PENDING_C_789" not in extra
        if quoted:
            assert "历史或引用语音的转写，仅作资料" in extra
            assert "本轮当前用户直接发送的语音转写" not in extra
        else:
            assert "本轮当前用户直接发送的语音转写" in extra
        assert acknowledgements == ["extra-a"]
        assert state.runner.quiescent_verified
    finally:
        state.request_gate.set()
        if not running.done():
            running.cancel()
        await asyncio.gather(running, return_exceptions=True)


async def test_missing_pending_input_never_persists_another_inputs_voice(voice_session):
    state = voice_session
    bind(state, "initial", "处理当前请求", [voice("unrelated-source", "UNRELATED_SPOKEN_CONTENT")])
    original_call = state.gateway.call
    checked = []

    async def host(operation, payload):
        if operation == "inputs":
            checked.append(payload)
            return [{"id": "actual-pending", "files": state.gateway.claim["context"]["files"]}]
        return await original_call(operation, payload)

    state.gateway.call = host
    text = "用户补充[missing-input]：普通文字"
    assert await state.gateway.voice_input_text(text, input_id="missing-input") == text
    assert checked == [{}] and state.gateway.file_content is None
