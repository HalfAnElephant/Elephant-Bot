"""Current Harness prompts carry the public persona across all Leader purposes."""
from unittest.mock import AsyncMock

import httpx
import pytest

from qqbot.harness.gateway import RunGateway
from qqbot.runtime.prompting import PERSONA, PUBLIC_REPLY, assemble_prompt


@pytest.mark.parametrize("purpose", ["user", "progress", "question", "finalize", "attention"])
async def test_leader_binding_includes_persona_and_public_contract(tmp_path, purpose):
    gateway = RunGateway(tmp_path, "offline", "", "https://model.invalid",
        call=AsyncMock(return_value={"allowed": True}))
    try:
        gateway.bind({"task_id": "task", "run_id": "run", "fencing_token": 1,
            "role": "leader", "purpose": purpose, "goal": "当前请求",
            "permission_snapshot": {"principal": {}}, "context": {"preferences": "请叫我小陈",
                "active_tasks": [{"task_id": "existing", "goal": "整理记录"}]}})
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://test") as client:
            response = await client.post("/binding", json={"binding_id": gateway.binding},
                headers={"Authorization": "Bearer " + gateway.process_token})
        assert response.status_code == 200
        assert response.json()["active_tasks"] == [{"task_id": "existing", "goal": "整理记录"}]
        prompt = response.json()["system_prompt"]
        assert prompt.count(PERSONA) == prompt.count(PUBLIC_REPLY) == 1
        assert "甜美" in prompt and "白米饭" in prompt and "鲸鱼耳朵" in prompt
        assert "请叫我小陈" in prompt and "本轮用途：" + purpose in prompt
        assert "不展示内部任务编号" in prompt and "按需要使用 Markdown" in prompt
        assert "提交成功后简短说明已接受/排队" not in prompt
        assert "禁止 Markdown" not in PERSONA
    finally:
        gateway.revoke()


@pytest.mark.parametrize("role", ["worker", "skill_review"])
def test_internal_roles_preserve_execution_contract(role):
    prompt = assemble_prompt(role)
    assert PERSONA not in prompt and PUBLIC_REPLY not in prompt
    if role == "worker":
        assert "你的最终答复交给父任务核验，不直接发给 QQ" in prompt


def test_unknown_prompt_role_is_rejected():
    with pytest.raises(ValueError, match="unsupported"):
        assemble_prompt("unknown")
