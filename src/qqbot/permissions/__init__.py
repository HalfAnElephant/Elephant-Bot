"""Deterministic application authorization with persisted, versioned policies."""

from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import insert, select, update

from qqbot.domain import AccessContext, ConversationRef, PrincipalRef
from qqbot.storage.permission_schema import permission_audit, permission_policy


class PermissionModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class PermissionSubject(PermissionModel):
    adapter_instance_id: str | None = None
    bot_identity_id: str | None = None
    identity_namespace: str | None = None
    group_id: str | None = None
    user_id: str | None = None

    @classmethod
    def from_access(cls, access: AccessContext):
        return cls.from_refs(access.conversation, access.principal)

    @classmethod
    def from_refs(cls, conversation: ConversationRef, principal: PrincipalRef | None = None):
        return cls(
            adapter_instance_id=conversation.adapter_instance_id,
            bot_identity_id=conversation.bot_identity_id,
            group_id=conversation.remote_conversation_id if conversation.surface != "private" else None,
            user_id=principal.remote_user_id if principal else None,
            identity_namespace=principal.identity_namespace if principal else None,
        )


class SubjectSelector(PermissionSubject):
    kind: Literal["global", "adapter", "bot", "group", "person", "group_member"] = "global"

    @model_validator(mode="after")
    def valid_scope(self):
        required = {
            "global": (),
            "adapter": ("adapter_instance_id",),
            "bot": ("adapter_instance_id", "bot_identity_id"),
            "group": ("adapter_instance_id", "group_id"),
            "person": ("adapter_instance_id", "identity_namespace", "user_id"),
            "group_member": ("adapter_instance_id", "identity_namespace", "group_id", "user_id"),
        }
        if any(not getattr(self, field) for field in required[self.kind]):
            raise ValueError("subject selector requires platform identity namespace and IDs")
        return self


class PermissionRule(PermissionModel):
    id: str = Field(min_length=1, max_length=128)
    enabled: bool = True
    priority: int = 0
    subject: SubjectSelector = Field(default_factory=SubjectSelector)
    actions: tuple[str, ...] = Field(default=("*",), min_length=1)
    resources: tuple[str, ...] = Field(default=("*",), min_length=1)
    effect: Literal["allow", "deny"]

    @model_validator(mode="after")
    def valid_patterns(self):
        for action in self.actions:
            if not action or (
                "*" in action and action != "*" and not (action.endswith(".*") and action.count("*") == 1)
            ):
                raise ValueError("actions require an exact name, namespace.* or *")
        for resource in self.resources:
            if not resource or (
                "*" in resource
                and resource != "*"
                and not (resource.endswith("*") and resource.count("*") == 1 and len(resource) > 2)
            ):
                raise ValueError("resources require an exact ID, bounded prefix* or *")
        return self


class PermissionPolicy(PermissionModel):
    preset: Literal["allow_all", "custom"] = "allow_all"
    default_effect: Literal["allow", "deny"] = "allow"
    rules: tuple[PermissionRule, ...] = ()

    @model_validator(mode="after")
    def unique_rules(self):
        if len({rule.id for rule in self.rules}) != len(self.rules):
            raise ValueError("duplicate permission rule IDs")
        return self


class PermissionDecision(PermissionModel):
    effect: Literal["allow", "deny"]
    matched_rule: PermissionRule | None = None
    reason: str
    policy_version: int
    matching_rule_ids: tuple[str, ...] = ()

    @property
    def allowed(self):
        return self.effect == "allow"


class PolicyVersionConflict(ValueError):
    pass


class PermissionService:
    def __init__(self, db=None, clock=None, policy: PermissionPolicy | None = None):
        self.db, self.clock = db, clock
        self.initial_policy = policy or PermissionPolicy()

    def _snapshot(self, conn=None):
        if self.db is None:
            return 0, self.initial_policy
        if conn is None:
            with self.db.engine.connect() as connection:
                return self._snapshot(connection)
        row = conn.execute(select(permission_policy).where(permission_policy.c.id == 1)).mappings().first()
        return (
            (row["version"], PermissionPolicy.model_validate_json(row["policy_json"]))
            if row
            else (0, self.initial_policy)
        )

    @property
    def policy_version(self):
        return self._snapshot()[0]

    def list(self):
        version, policy = self._snapshot()
        return {"policy_version": version, **policy.model_dump(mode="json")}

    def get(self, rule_id):
        return next((rule for rule in self._snapshot()[1].rules if rule.id == rule_id), None)

    @staticmethod
    def _pattern_specificity(patterns, value, *, action=False):
        scores = []
        for pattern in patterns:
            if pattern == value:
                scores.append(2)
            elif pattern == "*":
                scores.append(0)
            elif pattern.endswith("*") and value.startswith(pattern[:-1]):
                scores.append(1)
        score = max(scores, default=-1)
        return min(score, 1) if not action and len(patterns) > 1 else score

    @staticmethod
    def _subject_specificity(selector, subject):
        for field in PermissionSubject.model_fields:
            expected = getattr(selector, field)
            if expected is not None and expected != getattr(subject, field):
                return -1
        return {"global": 0, "adapter": 1, "bot": 1, "group": 2, "person": 3, "group_member": 4}[
            selector.kind
        ]

    def check(self, subject, action: str, resource: str, context=None):
        if isinstance(subject, AccessContext):
            subject = PermissionSubject.from_access(subject)
        if not isinstance(subject, PermissionSubject):
            subject = PermissionSubject.model_validate(subject)
        version, policy = self._snapshot()
        matched = []
        for rule in policy.rules:
            if not rule.enabled:
                continue
            scope = self._subject_specificity(rule.subject, subject)
            resource_score = self._pattern_specificity(rule.resources, resource)
            action_score = self._pattern_specificity(rule.actions, action, action=True)
            if min(scope, resource_score, action_score) >= 0:
                matched.append(
                    ((rule.priority, scope, resource_score, action_score, rule.effect == "deny"), rule)
                )
        matched.sort(key=lambda item: (item[0], item[1].id), reverse=True)
        winner = matched[0][1] if matched else None
        return PermissionDecision(
            effect=winner.effect if winner else policy.default_effect,
            matched_rule=winner,
            reason="highest matching priority and specificity" if winner else "policy default effect",
            policy_version=version,
            matching_rule_ids=tuple(rule.id for _, rule in matched),
        )

    def has_restrictions(self, subject, namespace=None):
        if isinstance(subject, AccessContext):
            subject = PermissionSubject.from_access(subject)
        if not isinstance(subject, PermissionSubject):
            subject = PermissionSubject.model_validate(subject)
        policy = self._snapshot()[1]
        if policy.default_effect == "deny":
            return True
        for rule in policy.rules:
            if (
                not rule.enabled
                or rule.effect != "deny"
                or self._subject_specificity(rule.subject, subject) < 0
            ):
                continue
            if namespace is None or any(
                action == "*"
                or action.startswith(namespace + ".")
                or namespace.startswith(action.rstrip("*"))
                for action in rule.actions
            ):
                return True
        return False

    def explain(self, subject, action, resource, context=None):
        return self.check(subject, action, resource, context)

    def require(self, subject, action, resource, context=None):
        decision = self.check(subject, action, resource, context)
        if not decision.allowed:
            raise PermissionError(f"{action} denied by policy version {decision.policy_version}")
        return decision

    def _mutate(self, expected_version, actor, operation, rule_id, transform):
        if self.db is None:
            raise RuntimeError("permission mutations require persistent storage")
        with self.db.transaction() as conn:
            version, before = self._snapshot(conn)
            if version != expected_version:
                raise PolicyVersionConflict("permission policy version changed")
            after = transform(before)
            values = {"version": version + 1, "policy_json": after.model_dump_json()}
            if version:
                conn.execute(update(permission_policy).where(permission_policy.c.id == 1).values(**values))
            else:
                conn.execute(insert(permission_policy).values(id=1, **values))
            now = self.clock.now() if self.clock else datetime.now(UTC)
            conn.execute(
                insert(permission_audit).values(
                    version=version + 1,
                    actor=actor,
                    operation=operation,
                    rule_id=rule_id,
                    before_json=before.model_dump_json(),
                    after_json=after.model_dump_json(),
                    created_at=now.isoformat(),
                )
            )
        return {"policy_version": version + 1, **after.model_dump(mode="json")}

    def upsert(self, rule: PermissionRule, *, expected_version: int, actor: str):
        rule = PermissionRule.model_validate(rule)
        return self._mutate(
            expected_version,
            actor,
            "upsert",
            rule.id,
            lambda policy: policy.model_copy(
                update={"rules": tuple(item for item in policy.rules if item.id != rule.id) + (rule,)}
            ),
        )

    def delete(self, rule_id: str, *, expected_version: int, actor: str):
        def remove(policy):
            if not any(rule.id == rule_id for rule in policy.rules):
                raise LookupError("permission rule unavailable")
            return policy.model_copy(
                update={"rules": tuple(rule for rule in policy.rules if rule.id != rule_id)}
            )

        return self._mutate(expected_version, actor, "delete", rule_id, remove)

    def set_policy(self, policy: PermissionPolicy, *, expected_version: int, actor: str):
        policy = PermissionPolicy.model_validate(policy)
        return self._mutate(expected_version, actor, "set_policy", None, lambda _: policy)
