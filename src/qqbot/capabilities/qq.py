"""Worker-side discovery only. QQ identity and credentials stay in Core."""
import asyncio
from pathlib import Path, PurePosixPath
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from qqbot.capabilities.types import Action, Capability


class LocalFileSend(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    request_id: str = Field(min_length=1, max_length=128)
    path: str = Field(min_length=1, max_length=1024)
    media_type: Literal["image", "audio", "video", "file"]
    text: str = Field(default="", max_length=1800)



def register_qq(registry, call, *, workspace=None, upload=None, snapshot_file=None):
    from qqbot.jobs.rich_delivery import RichButtons, RichMessage, RichStatus
    for name, model, operation, description, effect in (
        ("qq.message.send", RichMessage, "qq-send",
         ("发送 QQ 图片 文件 卡片 Markdown 键盘。Queue a QQ rich reply in this task's conversation only: Markdown, HTTPS media, cards and keyboards. "
         "Requires user intent to send. QUEUED is not delivered; query status. For generated local files use qq.file.send."), "write"),
        ("qq.message.status", RichStatus, "qq-status",
         "查询 QQ 消息送达状态。Read delivery status for a QQ rich message from this task. Never resend UNKNOWN automatically.", "read"),
        ("qq.buttons.create", RichButtons, "qq-buttons",
         ("创建 QQ 选择 分页 确认 取消 按钮。Create scoped expiring QQ buttons (choice/page/cancel_prompt/dismiss). Not sent until qq.message.send. "
         "Choice feeds current task; task_id must match current task. No arbitrary commands."), "write"),
    ):
        async def execute(arguments, context, op=operation, schema=model):
            # Strip only host-generated preconditions; all model fields are validated strictly.
            body = schema.model_validate({k: v for k, v in arguments.items() if k != "_preconditions"})
            return await call(op, body.model_dump(mode="json"))
        registry.register(Capability(name, description, model.model_json_schema()), execute,
            lambda a, c, e=effect: Action("notification.send", ("task:" + c.task_id,), e))

    if workspace is not None:
        def current_root():
            return Path(workspace() if callable(workspace) else workspace)

        async def send_file(arguments, context):
            from qqbot.adapters.qq_official.local_media import snapshot_workspace_file
            root = current_root()
            body = LocalFileSend.model_validate({k: v for k, v in arguments.items() if k != "_preconditions"})
            if context is None or Path(context.workspace) != root:
                raise PermissionError("file workspace does not match current run")
            path = PurePosixPath(body.path)
            if not path.parts or path.parts[0] != "outputs" or len(path.parts) < 2:
                raise ValueError("local QQ sends require a generated file under outputs/")
            decision = await call("authorize", {"action": "notification.send"})
            if not isinstance(decision, dict) or decision.get("allowed") is not True:
                raise PermissionError("file send is not authorized")
            if upload is None:
                raise ValueError("streaming file transport unavailable")
            preparing = asyncio.create_task(asyncio.to_thread(snapshot_file or snapshot_workspace_file,
                                                             root, body.path, body.media_type))
            try:
                snapshot, size, mime_type = await asyncio.shield(preparing)
            except asyncio.CancelledError:
                def close_late(task):
                    if not task.cancelled() and task.exception() is None:
                        task.result()[0].close()
                preparing.add_done_callback(close_late)
                raise
            try:
                async def chunks():
                    while chunk := await asyncio.to_thread(snapshot.read, 65536):
                        yield chunk
                metadata = {"request_id": body.request_id, "filename": path.name, "media_type": body.media_type,
                            "mime_type": mime_type, "size": size, "text": body.text}
                return await upload(metadata, chunks())
            finally:
                snapshot.close()

        registry.register(Capability("qq.file.send",
            "QQ 官方机器人发送本地生成的图片、音视频、文件。Send a generated file under this task's outputs/ only. "
            "Requires user intent to send. Official QQ limits: image 30 MiB, audio 20 MiB, video/file 100 MiB; no symlinks, secret paths or arbitrary local files. "
            "Core queues delivery; PENDING/UNKNOWN is not delivered. Query qq.message.status, do not blindly retry.",
            LocalFileSend.model_json_schema(), timeout_seconds=600), send_file,
            lambda a, c: Action("notification.send", ("task:" + c.task_id,), "write"))
