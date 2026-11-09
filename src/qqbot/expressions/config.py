"""MaiBot's default fast expression mode, scoped by full platform conversation identities."""

from typing import Literal

from pydantic import Field, model_validator

from qqbot.domain import DTO


class LearningRule(DTO):
    conversation_id: str = ""
    adapter_instance_id: str = ""
    type: Literal["group", "private"] = "group"
    use: bool = True
    learn: bool = True

    @model_validator(mode="after")
    def exact_scope(self):
        if "*" in self.conversation_id or "*" in self.adapter_instance_id:
            raise ValueError("expression rules require exact identities; leave defaults empty")
        return self


class BlacklistedUser(DTO):
    adapter_instance_id: str = Field(min_length=1)
    identity_namespace: str = Field(min_length=1)
    user_id: str = Field(min_length=1)


class ExpressionConfig(DTO):
    enabled: bool = True
    expression_checked_only: bool = False
    expression_self_reflect: bool = True
    learning_list: tuple[LearningRule, ...] = (LearningRule(),)
    expression_groups: tuple[tuple[str, ...], ...] = ()
    learning_blacklist: tuple[BlacklistedUser, ...] = ()
    learner_endpoint: str | None = None
    learner_model: str | None = None
    learner_key_env: str = "QQBOT_EXPRESSION_KEY"
    timeout_seconds: int = Field(default=300, ge=1, le=1800)
    min_messages: int = Field(default=10, ge=1, le=100)
    max_messages: int = Field(default=40, ge=10, le=100)
    learn_interval_seconds: int = Field(default=300, ge=30, le=86400)
    max_expression_learner: int = Field(default=3, ge=1, le=8)

    @model_validator(mode="after")
    def valid(self):
        if self.min_messages > self.max_messages:
            raise ValueError("expression min_messages exceeds max_messages")
        for group in self.expression_groups:
            if len(group) < 2 or len(group) != len(set(group)) or any(not key or "*" in key for key in group):
                raise ValueError("expression sharing requires at least two distinct exact conversation keys")
        return self

    def rule(self, conversation):
        kind = "private" if conversation.surface == "private" else "group"
        matches = [
            (int(bool(rule.conversation_id)) * 2 + int(bool(rule.adapter_instance_id)), index, rule)
            for index, rule in enumerate(self.learning_list)
            if rule.type == kind
            and (not rule.conversation_id or rule.conversation_id == conversation.key)
            and (not rule.adapter_instance_id or rule.adapter_instance_id == conversation.adapter_instance_id)
        ]
        return max(matches, key=lambda item: item[:2])[2] if matches else LearningRule(type=kind, use=False, learn=False)

    def blacklisted(self, principal):
        return any(
            user.adapter_instance_id == principal.adapter_instance_id
            and user.identity_namespace == principal.identity_namespace
            and user.user_id == principal.remote_user_id
            for user in self.learning_blacklist
        )
