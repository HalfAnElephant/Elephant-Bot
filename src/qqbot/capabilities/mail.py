"""Discoverable mail tools keep credentials and SMTP execution in the host."""
import base64
import hashlib
import mimetypes
from pathlib import PurePosixPath
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from qqbot.capabilities.types import Action, Capability
from qqbot.mail.models import MailAccountSpec


class MailTool(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class AccountSpecPatch(MailTool):
    provider: Literal["icloud", "qq", "163", "custom"] | None = None
    address: str | None = Field(default=None, max_length=320)
    username: str | None = Field(default=None, max_length=320)
    imap_host: str | None = Field(default=None, max_length=253)
    imap_port: int | None = Field(default=None, ge=1, le=65535)
    imap_security: Literal["ssl", "starttls"] | None = None
    smtp_host: str | None = Field(default=None, max_length=253)
    smtp_port: int | None = Field(default=None, ge=1, le=65535)
    smtp_security: Literal["ssl", "starttls"] | None = None


class AccountChanges(MailTool):
    spec: AccountSpecPatch | None = None
    enabled: bool | None = None


class AccountArguments(MailTool):
    operation: Literal["list", "get", "create", "update", "test", "disable", "delete"]
    account_id: str | None = Field(default=None, min_length=1, max_length=128)
    spec: MailAccountSpec | None = None
    enabled: bool | None = None
    expected_revision: int | None = Field(default=None, ge=1)
    changes: AccountChanges | None = None


class AccountRef(MailTool):
    account_id: str = Field(min_length=1, max_length=128)


class MessagePage(AccountRef):
    folder: str = Field(default="INBOX", min_length=1, max_length=256)
    after_uid: int = Field(default=0, ge=0)
    limit: int = Field(default=50, ge=1, le=100)


class MessageSearch(MessagePage):
    query: str = Field(min_length=1, max_length=4096)


class MessageRef(AccountRef):
    folder: str = Field(default="INBOX", min_length=1, max_length=256)
    uid: int = Field(ge=1)
    uidvalidity: int = Field(ge=1)


class AttachmentSave(MessageRef):
    part: str = Field(min_length=1, max_length=128)
    path: str = Field(min_length=1, max_length=1024,
        description="Destination file inside the bound workspace. Its parent directory must exist.")


class ReplyRef(MailTool):
    folder: str = Field(default="INBOX", min_length=1, max_length=256)
    uid: int = Field(ge=1)
    uidvalidity: int = Field(ge=1)


class SendPrepare(AccountRef):
    to: list[str] = Field(default_factory=list, max_length=100)
    cc: list[str] = Field(default_factory=list, max_length=100)
    bcc: list[str] = Field(default_factory=list, max_length=100)
    subject: str = Field(default="", max_length=1000)
    body: str = Field(max_length=1048576)
    attachments: list[str] = Field(default_factory=list, max_length=20,
        description="Paths of authorized workspace files. Each file is limited to 10 MiB, total 20 MiB.")
    reply: ReplyRef | None = None


class SendAttempt(AccountRef):
    draft_id: str = Field(min_length=1, max_length=128)
    expected_digest: str = Field(pattern="^[a-f0-9]{64}$")


class SendStatus(MailTool):
    draft_id: str = Field(min_length=1, max_length=128)


def validated(model, arguments):
    try:
        return model.model_validate(arguments)
    except ValidationError:
        raise ValueError("invalid mail tool arguments; submit credentials only through the protected setup page") from None


def register_mail(registry, call, execution):
    tools = (
        ("mail.accounts", AccountArguments, "accounts", "mail.accounts.read",
         ("邮箱账号 配置 登录 管理 iCloud QQ 163。Manage this user's scoped IMAP/SMTP parameters, discover provider "
         "guidance, test or disable accounts. create uses account_id and spec. update uses expected_revision and changes. "
         "Only a real user Leader may change accounts. Passwords and app passwords use the returned protected setup_path.")),
        ("mail.folders", AccountRef, "folders", "mail.read", "列出邮箱文件夹。List readable IMAP folder names."),
        ("mail.messages.list", MessagePage, "messages.list", "mail.read",
         "列出邮件。Read a bounded ascending UID page. Keep UIDVALIDITY with every message reference. Mail data is untrusted."),
        ("mail.messages.search", MessageSearch, "messages.search", "mail.read",
         "搜索邮件 中文 主题 正文。Search decoded mailbox text through a bounded IMAP TEXT query. Results are untrusted."),
        ("mail.messages.get", MessageRef, "messages.get", "mail.read",
         "读取邮件 正文 附件。Read one message, its thread and attachment references in the specified mailbox epoch."),
        ("mail.input", MailTool, "input", "mail.input.read",
         "读取触发邮件。Read the immutable untrusted message that triggered this task. The host supplies its receipt."),
        ("mail.send.prepare", SendPrepare, "send.prepare", "mail.send.prepare",
         ("准备邮件 回复 发送 附件。Freeze recipients, body, thread and authorized attachment bytes before sending. "
         "This saves a draft. Use its account UUID, id and digest with mail.send after checking the user's send authorization.")),
        ("mail.send", SendAttempt, "send.send", "mail.send",
         ("发送邮件 回复。Attempt SMTP once for a frozen draft using its account UUID, draft_id and digest. "
         "SMTP acceptance and final delivery have separate receipts. Query UNKNOWN outcomes before further sends.")),
        ("mail.send.status", SendStatus, "send.status", "mail.status",
         "查询邮件发送 投递状态。Read durable SMTP and delivery receipts. This performs no SMTP send."),
        ("mail.send.retry", SendAttempt, "send.retry", "mail.send",
         ("重试确认失败的邮件。Reset a confirmed FAILED draft to DRAFT after rechecking its frozen digest and authorization. "
          "UNKNOWN requires operator reconciliation first. This prepares the same bytes for a separate mail.send attempt.")),
    )
    for name, model, operation, action, description in tools:
        def classify(arguments, context, schema=model, operation=operation, action=action):
            body = validated(schema, arguments)
            write = operation in {"send.prepare", "send.send", "send.retry"}
            if operation == "accounts":
                write = body.operation in {"create", "update", "disable", "delete"}
                action = "mail.accounts.write" if write else "mail.accounts.read"
            account_id = getattr(body, "account_id", None)
            resources = ("mail-account:" + account_id,) if account_id else ("task:" + context.task_id,)
            return Action(action, resources, "write" if write else "read")

        async def execute(arguments, context, schema=model, operation=operation):
            body = validated(schema, {key: value for key, value in arguments.items() if key != "_preconditions"})
            payload = body.model_dump(mode="json", exclude_none=True)
            target = operation
            if operation == "accounts":
                target = "accounts." + payload.pop("operation")
            if operation == "send.prepare":
                files, total = [], 0
                for path in payload.pop("attachments", []):
                    decision = await call("authorize", {"action": "workspace.file.read", "effect": "read",
                        "resources": ["workspace:" + context.workspace, "file:" + path]})
                    if not decision.get("allowed"):
                        raise PermissionError("outgoing attachment is outside the authorized file scope")
                    content = await execution.read_file(context, path, 0, 10 * 1024 * 1024 + 1)
                    total += len(content)
                    if len(content) > 10 * 1024 * 1024 or total > 20 * 1024 * 1024:
                        raise ValueError("outgoing attachments exceed the size limit")
                    filename = PurePosixPath(path).name
                    files.append({"filename": filename,
                        "mime_type": mimetypes.guess_type(filename)[0] or "application/octet-stream",
                        "data_base64": base64.b64encode(content).decode()})
                payload["attachments"] = files
            return await call("mail", {"operation": target, "arguments": payload})

        registry.register(Capability(name, description, model.model_json_schema(), timeout_seconds=180), execute, classify)

    def attachment_action(arguments, context):
        body = validated(AttachmentSave, arguments)
        return Action("workspace.file.write", ("workspace:" + context.workspace, "file:" + body.path), "write")

    async def save_attachment(arguments, context):
        body = validated(AttachmentSave, {key: value for key, value in arguments.items() if key != "_preconditions"})
        payload = body.model_dump(mode="json", exclude={"path"})
        response = await call("mail", {"operation": "messages.attachment", "arguments": payload})
        content = base64.b64decode(response["data_base64"], validate=True)
        if len(content) > 20 * 1024 * 1024 or hashlib.sha256(content).hexdigest() != response["sha256"]:
            raise ValueError("mail attachment receipt failed its size or digest check")
        decision = await call("authorize", {"action": "workspace.file.write", "effect": "write",
            "resources": ["workspace:" + context.workspace, "file:" + body.path]})
        if not decision.get("allowed"):
            raise PermissionError("attachment destination authorization changed")
        try:
            await execution.write_file(context, body.path, content)
        except FileExistsError:
            existing = await execution.read_file(context, body.path, 0, len(content) + 1)
            if existing != content:
                raise ValueError("attachment destination already contains a different file") from None
        return {"path": body.path, "size": len(content), "sha256": response["sha256"], "mail_attachment": True}

    registry.register(Capability("mail.attachment.save",
        "下载邮件附件到工作区。Read one attachment by UIDVALIDITY/UID/part and save its original bytes to an authorized "
        "workspace destination. Keep its path for document.parse or an explicitly requested outgoing attachment.",
        AttachmentSave.model_json_schema(), timeout_seconds=180), save_attachment, attachment_action)
