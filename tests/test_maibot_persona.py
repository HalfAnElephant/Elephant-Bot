import json
from pathlib import Path

import httpx
import pytest
import yaml
from pydantic import ValidationError

from qqbot import dialogue
from qqbot.dialogue import PersonaSpec, PersonaStore, render_persona, select_temporary_reply_style
from qqbot.harness.gateway import GENERAL_PROMPT, RunGateway
from qqbot.providers import COMPLETION_INSTRUCTION


def default_persona():
    return PersonaSpec.model_validate(yaml.safe_load(Path("personas/elephant_default.yaml").read_text()))


def test_maibot_fields_preserve_legacy_config_and_personality_import():
    legacy = {"persona_id": "old", "version": 1, "name": "大象", "identity": "旧身份", "traits": ["好奇"],
              "style": {"tone": "旧风格"}, "boundaries": ["隐私边界"]}
    persona = PersonaSpec.model_validate(legacy)
    assert persona.multiple_probability == 0 and persona.reply_style == ""
    assert "旧风格" in render_persona({"core": persona.model_dump(), "group": {}})
    imported = {**legacy, "personality": legacy["identity"]}
    del imported["identity"]
    assert PersonaSpec.model_validate(imported) == persona
    assert "identity" in persona.model_dump() and "personality" not in persona.model_dump()
    for probability in (-0.1, 1.1, float("nan")):
        with pytest.raises(ValidationError):
            PersonaSpec.model_validate({**legacy, "multiple_probability": probability})


def test_default_persona_surfaces_and_conversation_overrides():
    persona = default_persona()
    assert persona.name == "大象" and "女大学生" in persona.identity
    assert "省略主语" in persona.reply_style and len(persona.multiple_reply_style) == 5
    assert persona.multiple_probability == 0
    snapshot = {"core": persona.model_dump(), "group": {"name": "小象", "reply_length": "简短"}}
    group = render_persona(snapshot, "group")
    private = render_persona(snapshot, "private")
    assert group.startswith("你是小象。") and private.startswith("你是小象。")
    assert persona.group_chat_prompt in group and persona.group_chat_prompt not in private
    assert persona.private_chat_prompts in private and persona.private_chat_prompts not in group
    assert "简短" in group and "不虚构事实" in private


def test_temporary_style_is_optional_and_does_not_replace_base_style(monkeypatch):
    persona = default_persona().model_dump()
    assert select_temporary_reply_style(persona) == ""
    persona.update(multiple_probability=1, multiple_reply_style=["", "  偶尔吐槽  "])
    monkeypatch.setattr(dialogue.random, "random", lambda: 0.5)
    temporary = select_temporary_reply_style(persona)
    assert temporary == "偶尔吐槽"
    prompt = render_persona(persona, "private", temporary_style=temporary)
    assert persona["reply_style"] in prompt and "偶尔吐槽" in prompt
    persona["multiple_probability"] = 0.1
    assert select_temporary_reply_style(persona) == ""


def test_new_persona_persists_and_invalid_reload_keeps_last_good(tmp_path):
    path = tmp_path / "persona.yaml"
    path.write_text(Path("personas/elephant_default.yaml").read_text())
    global_path = tmp_path / "persona-global.json"
    store = PersonaStore(path, global_path=global_path)
    updated = default_persona().model_copy(update={"reply_style": "自定义口语", "multiple_probability": 0.25})
    store.set_core(updated)
    snapshot = store.snapshot("private")
    assert PersonaStore(path, global_path=global_path).snapshot("private") == snapshot
    saved = json.loads(global_path.read_text())
    saved["persona"]["multiple_probability"] = 2
    global_path.write_text(json.dumps(saved))
    with pytest.raises(ValidationError):
        store.reload()
    assert store.snapshot("private") == snapshot


@pytest.mark.parametrize("notification", [False, True])
async def test_gateway_binds_persona_once_and_keeps_material_out_of_system(tmp_path, monkeypatch, notification):
    calls = []
    def choose(candidates):
        calls.append(candidates)
        return candidates[len(calls) % len(candidates)]
    monkeypatch.setattr(dialogue.random, "choice", choose)
    monkeypatch.setattr(dialogue.random, "random", lambda: 0.5)
    async def host(operation, payload):
        assert operation == "authorize"
        return {"allowed": True}
    gateway = RunGateway(tmp_path, "offline", "", "https://model.invalid", call=host)
    persona = default_persona().model_dump(mode="json")
    persona.update(multiple_probability=1, multiple_reply_style=["风格甲", "风格乙"])
    claim = {"task_id": "task", "run_id": "run", "fencing_token": 1, "goal": "聊天",
             "permission_snapshot": {"principal": {"user_id": "user"}},
             "notification_status": "failed" if notification else None,
             "context": {"expression_habits": "UNTRUSTED_EXPRESSION",
                         "seed": {"surface": "private", "persona": {"core": persona, "group": {}},
                                  "history": ["UNTRUSTED_HISTORY"]}}}
    gateway.bind(claim)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://test") as client:
        async def read():
            response = await client.post("/binding", json={"binding_id": gateway.binding},
                                         headers={"Authorization": "Bearer " + gateway.process_token})
            assert response.status_code == 200
            return response.json()
        first, second = await read(), await read()
        assert first["system_prompt"] == second["system_prompt"] and len(calls) == 1
        assert "你是大象" in first["system_prompt"]
        assert persona["private_chat_prompts"] in first["system_prompt"]
        assert "UNTRUSTED_EXPRESSION" not in first["system_prompt"]
        assert "UNTRUSTED_HISTORY" not in first["system_prompt"]
        assert first["expression_habits"] == "UNTRUSTED_EXPRESSION"
        gateway.revoke()
        gateway.bind({**claim, "run_id": "next"})
        third = await read()
        assert len(calls) == 2 and first["system_prompt"] != third["system_prompt"]
    assert "大象" not in GENERAL_PROMPT + COMPLETION_INSTRUCTION
