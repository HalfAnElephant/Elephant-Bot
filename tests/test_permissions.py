from datetime import timedelta

import pytest
from pydantic import ValidationError
from sqlalchemy import select

from qqbot.permissions import (
    PermissionPolicy,
    PermissionRule,
    PermissionService,
    PermissionSubject,
    PolicyVersionConflict,
    SubjectSelector,
)
from qqbot.storage.permission_schema import permission_audit

SUBJECT = PermissionSubject(adapter_instance_id="qq", identity_namespace="qq_member", bot_identity_id="bot",
                            group_id="group", user_id="user")


def rule(id, effect="deny", **kwargs):
    return PermissionRule(id=id, effect=effect, **kwargs)


def test_default_allows_write_and_management():
    service = PermissionService()
    for action in ("github.pr.review.approve", "memory.manage", "permission.upsert", "task.cancel"):
        decision = service.check(SUBJECT, action, "repo:github.com/a/b")
        assert decision.allowed
        assert decision.policy_version == 0
        assert decision.matched_rule is None


def test_subject_specificity_priority_and_namespace():
    group = SubjectSelector(kind="group", adapter_instance_id="qq", group_id="group")
    person = SubjectSelector(kind="person", adapter_instance_id="qq", identity_namespace="qq_member", user_id="user")
    member = SubjectSelector(kind="group_member", adapter_instance_id="qq", identity_namespace="qq_member",
                             group_id="group", user_id="user")
    rules = (rule("group", subject=group), rule("person", "allow", subject=person), rule("member", subject=member))
    service = PermissionService(policy=PermissionPolicy(rules=rules))
    assert service.explain(SUBJECT, "task.create", "task:1").matched_rule.id == "member"
    service = PermissionService(policy=PermissionPolicy(rules=rules[:2]))
    assert service.check(SUBJECT, "task.create", "task:1").allowed
    higher = rule("group-high", subject=group, priority=1)
    service = PermissionService(policy=PermissionPolicy(rules=rules + (higher,)))
    assert service.check(SUBJECT, "task.create", "task:1").matched_rule.id == "group-high"
    outsider = SUBJECT.model_copy(update={"adapter_instance_id": "other"})
    assert service.check(outsider, "task.create", "task:1").matched_rule is None


def test_resource_action_specificity_and_deny_tie():
    rules = (rule("all"), rule("prefix", "allow", resources=("repo:github.com/org/*",)),
             rule("exact", resources=("repo:github.com/org/repo",), actions=("github.pr.*",)),
             rule("action", "allow", resources=("repo:github.com/org/repo",), actions=("github.pr.approve",)))
    service = PermissionService(policy=PermissionPolicy(rules=rules))
    assert service.check(SUBJECT, "github.pr.approve", "repo:github.com/org/repo").matched_rule.id == "action"
    assert service.check(SUBJECT, "github.pr.list", "repo:github.com/org/repo").matched_rule.id == "exact"
    assert service.check(SUBJECT, "github.issue.list", "repo:github.com/org/repo").matched_rule.id == "prefix"
    tied = rules + (rule("deny-action", resources=("repo:github.com/org/repo",), actions=("github.pr.approve",)),)
    for ordering in (tied, tuple(reversed(tied))):
        decision = PermissionService(policy=PermissionPolicy(rules=ordering)).check(
            SUBJECT, "github.pr.approve", "repo:github.com/org/repo")
        assert decision.matched_rule.id == "deny-action"
        assert decision.matching_rule_ids[0] == "deny-action"


def test_disabled_rules_and_default_deny():
    service = PermissionService(policy=PermissionPolicy(default_effect="deny", rules=(rule("off", "allow", enabled=False),)))
    assert not service.check(SUBJECT, "task.create", "task:1").allowed
    with pytest.raises(PermissionError):
        service.require(SUBJECT, "task.create", "task:1")


def test_persist_cas_audit_and_immediate_revocation(db, clock):
    service = PermissionService(db, clock)
    other = PermissionService(db, clock)
    assert service.list()["rules"] == []
    service.upsert(rule("deny"), expected_version=0, actor="test")
    assert not other.check(SUBJECT, "github.pr.approve", "repo:a/b").allowed
    with pytest.raises(PolicyVersionConflict):
        other.delete("deny", expected_version=0, actor="test")
    assert service.policy_version == 1
    service.upsert(rule("deny", enabled=False), expected_version=1, actor="test")
    assert other.check(SUBJECT, "github.pr.approve", "repo:a/b").allowed
    service.delete("deny", expected_version=2, actor="test")
    with pytest.raises(LookupError):
        service.delete("missing", expected_version=3, actor="test")
    assert service.policy_version == 3
    with db.engine.connect() as conn:
        audits = conn.execute(select(permission_audit).order_by(permission_audit.c.version)).mappings().all()
    assert [item["operation"] for item in audits] == ["upsert", "upsert", "delete"]
    assert all(item["actor"] == "test" for item in audits)
    assert PermissionPolicy.model_validate_json(audits[0]["before_json"]).rules == ()
    assert PermissionService(db, clock).get("deny") is None


@pytest.mark.parametrize("kwargs", [
    {"actions": ("github*p",)}, {"resources": ("repo:*/*",)},
    {"subject": {"kind": "person", "user_id": "123"}},
])
def test_invalid_policy_boundary(kwargs):
    with pytest.raises(ValidationError):
        rule("invalid", **kwargs)


def test_http_permission_crud_keeps_authentication(tmp_path, official_config, clock, monkeypatch):
    from fastapi.testclient import TestClient

    from qqbot.app import create_app
    from qqbot.config import Settings

    monkeypatch.setenv("QQBOT_ADMIN_TOKEN", "admin-test-token")
    app = create_app(Settings(database=str(tmp_path / "core.db"), adapters=(official_config,)),
                     clock=clock, run_background=False)
    auth = {"Authorization": "Bearer admin-test-token"}
    with TestClient(app) as client:
        assert client.get("/api/v1/permissions").status_code == 401
        assert client.get("/api/v1/permissions", headers=auth).json()["policy_version"] == 0
        body = {"expected_version": 0, "rule": rule("blocked", actions=("task.cancel",)).model_dump(mode="json")}
        assert client.put("/api/v1/permissions/blocked", headers=auth, json=body).status_code == 200
        assert client.put("/api/v1/permissions/blocked", headers=auth, json=body).status_code == 409
        assert client.get("/api/v1/permissions/blocked", headers=auth).json()["rule"]["id"] == "blocked"
        check = {"subject": SUBJECT.model_dump(), "action": "task.cancel", "resource": "task:1"}
        for endpoint in ("check", "explain"):
            result = client.post("/api/v1/permissions/" + endpoint, headers=auth, json=check).json()
            assert result["effect"] == "deny" and result["policy_version"] == 1
        assert client.delete("/api/v1/permissions/blocked?expected_version=1", headers=auth).status_code == 200


async def test_official_adapter_ignores_legacy_allowlists_but_keeps_platform_checks(
    official_config, incoming, clock, official_event
):
    from qqbot.adapters.qq_official.adapter import OfficialAdapter
    from qqbot.domain import OutboundEnvelope

    adapter = OfficialAdapter(official_config.model_copy(update={"allowed_groups": ()}), "d", incoming, clock)
    event = adapter.normalize(official_event)
    outgoing = OutboundEnvelope(
        delivery_id="permission-test", target=event.conversation, text="hello",
        origin_kind="chat", origin_id=event.event_id, expires_at=clock.now() + timedelta(seconds=300),
        reply_context=event.reply_context.model_copy(update={"msg_seq": 1}),
    )
    assert (await adapter.validate_delivery(outgoing)).allowed
    adapter.permissions = PermissionService(policy=PermissionPolicy(rules=(
        rule("notification-deny", actions=("notification.send",)),
    )))
    assert (await adapter.validate_delivery(outgoing)).reason == "TARGET_DENIED"
    wrong = outgoing.model_copy(update={"target": outgoing.target.model_copy(update={"bot_identity_id": "other"})})
    assert (await adapter.validate_delivery(wrong)).reason == "IDENTITY_MISMATCH"
    await adapter.stop()


@pytest.mark.parametrize("private", [True, False])
async def test_scanned_owner_reply_keeps_identity_through_durable_delivery(
    private, official_config, incoming, clock, official_event, db,
):
    from types import SimpleNamespace

    import httpx
    from conftest import official_payload

    from qqbot.adapters.qq_official.adapter import OfficialAdapter
    from qqbot.admin.qq_provisioning import _owner_policy
    from qqbot.domain import OutboundEnvelope
    from qqbot.outbox.service import Outbox
    from qqbot.storage.schema import outbox

    policy = _owner_policy(PermissionPolicy(), SimpleNamespace(
        instance_id=official_config.instance_id, app_id=official_config.app_id, scanner_openid="300",
    ), official_config.bot_id)
    permissions = PermissionService(db, clock)
    permissions.set_policy(policy, expected_version=0, actor="test")
    calls = []

    def handle(request):
        calls.append(request)
        if request.url.path.endswith("getAppAccessToken"):
            return httpx.Response(200, json={"access_token": "test-token", "expires_in": 7200})
        return httpx.Response(200, json={"id": "reply-sent"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        adapter = OfficialAdapter(official_config, "d", incoming, clock, client, permissions=permissions)
        event = adapter.normalize(official_payload(official_event, private=private, user_id="300"))
        outgoing = OutboundEnvelope(
            delivery_id="owner-reply", target=event.conversation, requester=event.principal, text="hello",
            origin_kind="chat", origin_id=event.event_id, expires_at=clock.now() + timedelta(seconds=300),
            reply_context=event.reply_context,
        )
        queue = Outbox(db, clock)
        with db.transaction() as conn:
            queue.enqueue(conn, outgoing)
        await queue.dispatch_once({official_config.instance_id: adapter})
        with db.engine.connect() as conn:
            row = conn.execute(select(outbox)).mappings().one()
        assert row["state"] == "SENT"
        assert row["platform_message_id"] == "reply-sent"
        assert OutboundEnvelope.model_validate_json(row["envelope_json"]).requester == event.principal
        assert len([call for call in calls if call.url.path.endswith("/messages")]) == 1

        outgoing = outgoing.model_copy(update={"reply_context": event.reply_context.model_copy(update={"msg_seq": 1})})
        for requester in (
            None,
            event.principal.model_copy(update={"remote_user_id": "outsider"}),
            event.principal.model_copy(update={"identity_namespace": "qq_app:other"}),
        ):
            assert (await adapter.validate_delivery(outgoing.model_copy(update={"requester": requester}))).reason == "TARGET_DENIED"
        permissions.delete("qq-owner:" + official_config.instance_id + ":allow",
            expected_version=permissions.policy_version, actor="test")
        assert (await adapter.validate_delivery(outgoing)).reason == "TARGET_DENIED"


def test_unknown_action_restrictions_are_subject_scoped():
    selector = SubjectSelector(kind="group", adapter_instance_id="qq", group_id="group")
    service = PermissionService(policy=PermissionPolicy(rules=(
        rule("restricted", subject=selector, actions=("github.pr.approve",)),
    )))
    assert service.has_restrictions(SUBJECT)
    assert service.has_restrictions(SUBJECT, "github")
    assert not service.has_restrictions(SUBJECT, "weather")
    assert not service.has_restrictions(SUBJECT.model_copy(update={"group_id": "other"}))
