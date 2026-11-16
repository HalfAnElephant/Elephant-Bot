"""Typed QQ keyboard content. Display permissions never replace server authorization."""
from typing import Literal
from urllib.parse import urlsplit

from pydantic import Field, model_validator

from .messages import DTO


class QQButtonPermission(DTO):
    type: Literal[0, 1, 2] = 2
    specify_user_ids: tuple[str, ...] = ()
    specify_role_ids: tuple[str, ...] = ()

    @model_validator(mode="after")
    def validate_targets(self):
        if self.type == 0 and not (self.specify_user_ids or self.specify_role_ids):
            raise ValueError("specified-user permission requires targets")
        if self.type != 0 and (self.specify_user_ids or self.specify_role_ids):
            raise ValueError("targets require specified-user permission")
        return self


class QQButtonModal(DTO):
    content: str = Field(min_length=1, max_length=40)
    confirm_text: str = Field(default="确认", max_length=4)
    cancel_text: str = Field(default="取消", max_length=4)

    @model_validator(mode="after")
    def no_links(self):
        if "://" in self.content or "www." in self.content.lower():
            raise ValueError("confirmation text must not contain URLs")
        return self


class QQButtonAction(DTO):
    type: Literal[0, 1, 2]
    data: str = Field(min_length=1, max_length=2048)
    permission: QQButtonPermission = Field(default_factory=QQButtonPermission)
    unsupport_tips: str | None = Field(default=None, max_length=100)
    enter: bool = False
    reply: bool = False
    anchor: Literal[1] | None = None
    modal: QQButtonModal | None = None

    @model_validator(mode="after")
    def valid_action(self):
        if self.type == 0:
            url = urlsplit(self.data)
            if url.scheme not in {"http", "https"} or not url.hostname or url.username or url.password:
                raise ValueError("jump buttons require an HTTP(S) URL")
        if self.type != 2 and (self.enter or self.reply or self.anchor is not None):
            raise ValueError("enter/reply/anchor require a command button")
        return self


class QQButtonRender(DTO):
    label: str = Field(min_length=1, max_length=10)
    visited_label: str | None = Field(default=None, max_length=10)
    style: Literal[0, 1, 3, 4] = 0


class QQButton(DTO):
    id: str = Field(min_length=1, max_length=128)
    render_data: QQButtonRender
    action: QQButtonAction
    group_id: str | None = Field(default=None, min_length=1, max_length=128)

    @model_validator(mode="after")
    def callback_groups_only(self):
        if self.group_id is not None and self.action.type != 1:
            raise ValueError("mutually exclusive groups require callback buttons")
        return self


class QQButtonRow(DTO):
    # Conservative engineering bounds; actual client/app limits may be lower.
    buttons: tuple[QQButton, ...] = Field(min_length=1, max_length=5)


class QQKeyboardContent(DTO):
    rows: tuple[QQButtonRow, ...] = Field(min_length=1, max_length=5)

    @model_validator(mode="after")
    def unique_ids(self):
        ids = [button.id for row in self.rows for button in row.buttons]
        if len(set(ids)) != len(ids):
            raise ValueError("button ids must be unique within a keyboard")
        return self


class QQKeyboard(DTO):
    id: str | None = Field(default=None, min_length=1, max_length=128)
    content: QQKeyboardContent | None = None

    @model_validator(mode="after")
    def exactly_one(self):
        if (self.id is None) == (self.content is None):
            raise ValueError("exactly one of keyboard id or content is required")
        return self

    def to_payload(self) -> dict:
        return self.model_dump(mode="json", exclude_none=True)


class WorkflowPayload(DTO):
    value: str | None = Field(default=None, min_length=1, max_length=2000)
    task_id: str | None = Field(default=None, min_length=1, max_length=128)
    question_id: str | None = Field(default=None, min_length=1, max_length=128)
    pages: tuple[str, ...] = Field(default=(), max_length=20)
    page: int = Field(default=0, ge=0, le=19)

    @model_validator(mode="after")
    def bounded_pages(self):
        if any(not page or len(page) > 4000 for page in self.pages):
            raise ValueError("pages require 1..4000 characters")
        if self.pages and self.page >= len(self.pages):
            raise ValueError("page outside document")
        return self


class WorkflowButton(DTO):
    label: str = Field(min_length=1, max_length=10)
    action: Literal["choice", "page", "cancel_prompt", "cancel_task", "dismiss"]
    payload: WorkflowPayload = Field(default_factory=WorkflowPayload)

    @model_validator(mode="after")
    def action_fields(self):
        p = self.payload
        if self.action == "choice":
            if not p.value or p.pages or (p.question_id and not p.task_id):
                raise ValueError("choice requires value and optional scoped task/question")
        elif self.action == "page":
            if not p.pages or p.task_id or p.value or p.question_id:
                raise ValueError("page requires only pages and index")
        elif self.action in {"cancel_prompt", "cancel_task"}:
            if not p.task_id or p.value or p.pages or p.question_id:
                raise ValueError("cancel requires only task id")
        elif p != WorkflowPayload():
            raise ValueError("dismiss takes no payload")
        return self
