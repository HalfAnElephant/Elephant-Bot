"""Validated, deterministic filters for an owner's incoming-mail schedule."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class MailCondition(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)

    field: Literal['sender_address', 'sender_domain', 'subject', 'body'] = Field(
        description="sender_address is the full address; sender_domain is only the part after @, without @.")
    op: Literal['equals', 'contains'] = Field(
        description="Use equals for an exact match and contains for a substring. Put negated conditions in none.")
    value: str = Field(min_length=1, max_length=4096,
        description="Only the literal text to compare, without adding quotation marks or operator words.")

    @field_validator('value')
    @classmethod
    def reject_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError('mail condition cannot be blank')
        return value


class MailTrigger(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)

    account_id: str = Field(min_length=1, max_length=256)
    folder: str = Field(default='INBOX', min_length=1, max_length=256)
    all: tuple[MailCondition, ...] = Field(default=(), max_length=32, description="Every condition must match (AND).")
    any: tuple[MailCondition, ...] = Field(default=(), max_length=32, description="At least one condition must match (OR); omit when there is no OR group.")
    none: tuple[MailCondition, ...] = Field(default=(), max_length=32, description="No condition may match. For body does not contain X, use field=body, op=contains, value=X here.")

    @field_validator('account_id', 'folder')
    @classmethod
    def reject_invalid_source(cls, value: str) -> str:
        if not value.strip() or any(ord(char) < 32 or ord(char) == 127 for char in value):
            raise ValueError('invalid mail source')
        return value

    def matches(self, *, sender_address: str, subject: str, body: str, body_complete: bool = True) -> bool:
        """Match already-normalized sender and decoded text, never interpreting it as instructions.

        An incomplete body is unknown, including for exclusion filters. A rule
        fires only if all groups can be conclusively satisfied.
        """
        normalized_sender = sender_address.strip()
        local, separator, domain = normalized_sender.partition('@')
        # The protocol boundary must provide one addr-spec, not a display-name
        # header or a list. Missing/malformed sender data is unknown, not a
        # confirmed non-match (especially for a `none` sender condition).
        sender_known = bool(local and separator and domain) and not any(
            char.isspace() or ord(char) < 32 or ord(char) == 127 or char in '<>,;'
            for char in normalized_sender
        ) and '@' not in domain
        fields = {
            'sender_address': normalized_sender.casefold() if sender_known else None,
            'sender_domain': domain.casefold() if sender_known else None,
            'subject': subject.casefold(),
            'body': body.casefold(),
        }

        def match(condition: MailCondition) -> bool | None:
            if condition.field == 'body' and not body_complete:
                return None
            text = fields[condition.field]
            if text is None:
                return None
            value = condition.value.casefold()
            return text == value if condition.op == 'equals' else value in text

        return (
            all(match(condition) is True for condition in self.all)
            and (not self.any or any(match(condition) is True for condition in self.any))
            and all(match(condition) is False for condition in self.none)
        )
