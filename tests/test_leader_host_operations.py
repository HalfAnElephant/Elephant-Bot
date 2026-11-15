"""Owner Leader turns journal host effects without creating a Worker task."""
import pytest
from sqlalchemy import select
from test_agent_runtime import runtime as runtime  # noqa: PLC0414
from test_leader_dispatch_recovery import native_pending_submit

from qqbot.adapters.pi.sessions import unfinished_tools
from qqbot.storage.action_schema import operations
from qqbot.storage.task_schema import runs


def prepare(operation_id, digest="same-command"):
    return {"operation": "prepare", "operation_id": operation_id, "arguments_hash": digest,
        "arguments": {"argv": ["true"]}, "action": {"name": "process.start", "effect": "unknown",
            "resources": ["workspace:*"]}}


async def ledger(runtime, claim, payload):
    return await runtime.leader.invoke("ledger", (claim["run_id"], claim["fencing_token"], "leader"), payload)


async def test_owner_user_turn_can_authorize_and_journal_host_effects(runtime):
    r = runtime
    r.sessions.enqueue(r.event, r.access)
    claim = r.sessions.claim("leader")
    identity = claim["run_id"], claim["fencing_token"], "leader"
    assert (await r.leader.invoke("authorize", identity, prepare("command")["action"]))["allowed"]
    assert (await ledger(r, claim, prepare("command")))["state"] == "PREPARED"
    assert (await ledger(r, claim, {"operation": "transition", "operation_id": "command",
        "expected": "PREPARED", "state": "DISPATCHED", "result": {}}))["changed"]
    assert (await ledger(r, claim, {"operation": "transition", "operation_id": "command",
        "expected": "DISPATCHED", "state": "VERIFIED", "result": {"exit_code": 0}}))["changed"]
    assert [row["id"] for row in await ledger(r, claim, {"operation": "list"})] == ["command"]
    with r.db.engine.connect() as conn:
        assert conn.execute(select(runs.c.task_id).where(runs.c.id == claim["run_id"])).scalar_one() is None
        assert conn.execute(select(operations.c.task_id)).scalar_one() == claim["task_id"]
    r.sessions.owner = lambda _: None
    with pytest.raises(PermissionError, match="verified owner"):
        await r.leader.invoke("authorize", identity, prepare("other")["action"])
    with pytest.raises(PermissionError, match="verified owner"):
        await ledger(r, claim, prepare("other"))


async def test_stopped_direct_turn_preserves_unknown_command_and_scope_across_retry(runtime):
    r = runtime
    r.sessions.enqueue(r.event, r.access)
    claim = r.sessions.claim("leader")
    await ledger(r, claim, prepare("command"))
    await ledger(r, claim, {"operation": "transition", "operation_id": "command",
        "expected": "PREPARED", "state": "DISPATCHED", "result": {}})
    await ledger(r, claim, prepare("undispatched", "other-command"))
    path = native_pending_submit(r, claim, {"argv": ["true"]}, tool_name="capability_invoke")
    r.clock.advance(31)
    assert r.leader.recover_attempt(claim["run_id"])["state"] == "RECOVERED"
    assert unfinished_tools(path) == ()
    retry = r.sessions.claim("leader")
    assert retry["task_id"] == claim["task_id"]
    reused = await ledger(r, retry, prepare("would-repeat"))
    assert reused["operation_id"] == "command" and reused["state"] == "UNKNOWN"
    with r.db.engine.connect() as conn:
        assert dict(conn.execute(select(operations.c.id, operations.c.state)).all()) == {
            "command": "UNKNOWN", "undispatched": "FAILED"}
    assert (await ledger(r, retry, {"operation": "transition", "operation_id": "command",
        "expected": "UNKNOWN", "state": "VERIFIED", "result": {"verified": True}}))["changed"]
    with pytest.raises(PermissionError, match="binding"):
        await ledger(r, retry, {"operation": "transition", "operation_id": "undispatched",
            "expected": "PREPARED", "state": "DISPATCHED", "result": {}})


async def test_direct_reconcile_uses_original_host_workspace_and_current_authorization(runtime):
    from qqbot.runtime.recovery import RuntimeRecovery

    r = runtime
    r.sessions.enqueue(r.event, r.access)
    claim = r.sessions.claim("leader")
    await ledger(r, claim, prepare("command"))
    await ledger(r, claim, {"operation": "transition", "operation_id": "command",
        "expected": "PREPARED", "state": "DISPATCHED", "result": {}})
    await ledger(r, claim, {"operation": "transition", "operation_id": "command",
        "expected": "DISPATCHED", "state": "UNKNOWN", "result": {}})
    contexts = []
    async def verify(context, action, arguments, result):
        contexts.append(context)
        return "VERIFIED", {"receipt": "confirmed"}
    r.leader.core.recovery = RuntimeRecovery(r.leader.core, verifier=verify)
    result = await ledger(r, claim, {"operation": "reconcile", "operation_id": "command"})
    assert result == {"operation_id": "command", "state": "VERIFIED",
        "result": {"receipt": "confirmed"}, "changed": True}
    assert contexts[0].task_id == claim["task_id"]
    assert contexts[0].run_id == claim["run_id"]
    assert contexts[0].environment == "host"
    with pytest.raises(PermissionError, match="another Leader turn"):
        await ledger(r, claim, {"operation": "reconcile", "operation_id": "foreign"})
