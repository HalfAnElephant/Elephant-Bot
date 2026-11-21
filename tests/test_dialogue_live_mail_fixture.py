"""Offline regressions for the opt-in evaluator’s mail cases."""
import base64
import importlib.util
import json
from pathlib import Path

import pytest
from fastapi import FastAPI, Header, HTTPException
from sqlalchemy import update
from test_agent_runtime import root_goal, worker_complete
from test_agent_runtime import runtime as agent_runtime
from test_host_controls import control_runtime as control_runtime  # noqa: PLC0414

from elephant.admin.mail import build_mail_router
from elephant.storage.task_schema import runs

runtime = agent_runtime


def evaluator():
    path = Path(__file__).resolve().parents[1] / "evals" / "dialogue_live.py"
    loader = importlib.util.spec_from_file_location("dialogue_live_for_test", path)
    module = importlib.util.module_from_spec(loader)
    loader.loader.exec_module(module)
    return module


@pytest.mark.parametrize("enabled", [False, True])
async def test_synthetic_mail_account_credentials_are_prepared_and_reused(
        control_runtime, monkeypatch, enabled):
    monkeypatch.setenv("ELEPHANT_CREDENTIAL_KEY", base64.b64encode(b"r" * 32).decode())
    core = control_runtime.core
    service = core.mail.accounts
    event = control_runtime.event("configure fake mailbox")
    account = service.invoke(event, "personal", "create", {
        "account_id": "weekly-demo",
        "spec": {"provider": "qq", "address": "fake@example.test"},
        "enabled": enabled,
    })
    assert account["enabled"] is enabled
    assert not account["ready"]
    assert account["credential_ref"] is None
    observed = []
    original_save = service.set_credential

    def record_save(identifier, secret, *, expected_revision):
        result = original_save(identifier, secret, expected_revision=expected_revision)
        observed.append((result["enabled"], result["ready"], result["revision"]))
        return result

    monkeypatch.setattr(service, "set_credential", record_save)

    def authenticate(authorization: str | None = Header(default=None)):
        if authorization != "Bearer synthetic-mail-admin":
            raise HTTPException(401)

    app = FastAPI()
    app.include_router(build_mail_router(core, authenticate))
    module = evaluator()
    prepared = await module.prepare_synthetic_mail_account(app, account)
    assert observed == [(enabled, enabled, account["revision"] + 1)]
    assert prepared["ready"] and prepared["enabled"] and prepared["credential_ref"]
    assert prepared["revision"] == account["revision"] + (1 if enabled else 2)
    assert service.view(service.admin_row(account["id"])) == prepared

    reused = await module.prepare_synthetic_mail_account(app, prepared)
    assert reused == prepared
    assert len(observed) == 1


def test_mail_timeout_snapshot_omits_result_and_receipt_contents(runtime):
    root = root_goal(runtime)
    worker_complete(runtime)
    with runtime.db.transaction() as conn:
        conn.execute(update(runs).values(result_json=json.dumps({"status": "completed", "summary": "private-body"}),
            receipt_json=json.dumps({"kind": "turn_finished", "result": {"summary": "private-receipt"}})))
    snapshot = evaluator().mail_task_snapshot(runtime.leader.core, root["task_id"])
    assert snapshot["attempts"][0]["result_status"] == "completed"
    assert snapshot["attempts"][0]["receipt_kind"] == "turn_finished"
    assert len(snapshot["tasks"]) == 2 and snapshot["inbox"]
    assert "private-body" not in json.dumps(snapshot) and "private-receipt" not in json.dumps(snapshot)


@pytest.mark.parametrize(("reply", "expected"), [
    (
        (
            "我查了，你名下现在只有 weekly-demo 这一个邮箱账号，没有别的可选的。\n\n"
            "所以得先知道那只有两个信息：账号标识，还有邮箱地址（用来配置）。"
            "有这两个我才能先把账号建好，再挂上规则。"
        ),
        True,
    ),
    ("请提供另一个邮箱的 **账号 ID**。", True),
    ("我需要先知道这个邮箱的账号标识，然后再建规则。", True),
    ("新邮箱的账号标识是什么？", True),
    ("你要用哪个邮箱账号来建这条规则？", True),
    ("你告诉我那只是哪个账号、地址是啥，我就去建。", True),
    ("请先确认这是哪一个邮箱账户。", True),
    ("请告诉我无需提供邮箱账号标识。", False),
    ("请确认不用给我邮箱账号标识。", False),
    ("请告诉我收件地址、账号标识不用给。", False),
    ("我先查一下当前有几个邮箱账号。", False),
    ("这个邮箱的账号标识还没指定。", False),
    ("请提供邮箱地址，账号标识我会自动生成。", False),
    ("你不需要先提供邮箱账号标识。", False),
    ("请告诉我，这个邮箱的账号标识。", True),
    ("请提供邮箱地址。账号标识还没指定，对吗？", False),
    ("目前只找到 weekly-demo。要我继续创建邮箱规则吗？", False),
    ("请告诉我周报要总结成几条？邮箱账号还不清楚。", False),
    ("目前没有该账号。你把那只邮箱的信息给我吧～\n\n- 邮箱地址\n- 服务商", True),
    ("请提供目标邮箱地址和邮箱服务商。", True),
    ("目前缺少邮箱地址和服务商。要我继续吗？", False),
    ("请确认无需提供邮箱地址和服务商。", False),
    ("我已按现有 weekly-demo 配好规则，账号标识是 weekly-demo。", False),
])
def test_mail_clarification_requires_details_to_identify_the_account(reply, expected):
    assert evaluator().asks_for_mail_account_clarification(reply) is expected
