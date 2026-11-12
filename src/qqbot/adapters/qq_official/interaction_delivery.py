"""Signed, scoped application interactions; never arbitrary instructions or approvals."""
import json
from datetime import timedelta
from uuid import uuid4

from qqbot.domain import InboundEvent, OutboundEnvelope, ReplyContext, TextSegment
from qqbot.domain.official import OfficialContent
from qqbot.permissions import PermissionSubject

from .interactions import acknowledge_interaction, parse_interaction, workflow_keyboard


async def handle_callback(adapter, core, store, body, headers):
    payload = json.loads(body)
    if not isinstance(payload, dict):
        raise ValueError("invalid payload")  # noqa: TRY004 - callback validation boundary
    if payload.get("t") != "INTERACTION_CREATE" or payload.get("op") != 0:
        return adapter.callback(body, headers)
    adapter.verify(body, headers.get("x-signature-timestamp", ""), headers.get("x-signature-ed25519", ""))
    event = parse_interaction(payload, deployment_id=adapter.deployment_id,
        instance_id=adapter.config.instance_id, app_id=adapter.config.app_id, bot_id=adapter.config.bot_id)
    event_id = payload.get("id")
    if not isinstance(event_id, str) or not event_id or len(event_id) > 256:
        raise ValueError("missing outer event id")
    def authorized():
        return core.permissions.check(PermissionSubject.from_refs(event.conversation, event.principal),
            "notification.send", "conversation:" + event.conversation.key).allowed
    if not authorized():
        await acknowledge_interaction(adapter, event, 4)
        return {"op": 12}
    decision = store.consume(event)
    ack = await acknowledge_interaction(adapter, event, decision.code)
    if ack != "ACKED" or not decision.action or not store.claim(event.key):
        return {"op": 12}
    try:
        if not authorized():
            store.finish(event.key, failed=True)
            return {"op": 12}
        keyboard = None
        if decision.action == "help":
            text = "可使用 /帮助、/状态 查看功能；发送消息开始对话。群聊不支持原生流式。"
        elif decision.action == "status":
            cap = await adapter.capabilities(event.conversation)
            text = ("服务运行中。当前会话：" + event.conversation.surface.value
                    + "；Markdown：" + ("已启用" if cap.supports_markdown else "未启用")
                    + "；流式：" + ("已启用" if cap.supports_streaming else "未启用")
                    + "。权限和客户端显示仍以实际 QQ 返回为准。")
        else:
            text, keyboard = run_workflow(core, store, event, decision)
        # Capabilities or workflow resolution can await/change policy. Outbox also
        # retains requester for its independent send-time authorization.
        if not authorized():
            store.finish(event.key, failed=True)
            return {"op": 12}
        envelope = OutboundEnvelope(delivery_id=str(uuid4()), target=event.conversation,
            text=text, plain_text=True,
            official_content=OfficialContent(keyboard=keyboard.to_payload()) if keyboard else None, origin_kind="command", origin_id="interaction:" + event.key,
            requester=event.principal, expires_at=core.clock.now() + timedelta(minutes=5),
            reply_context=ReplyContext(platform_message_id="interaction:" + event_id,
                event_id=event_id, source="interaction", expires_at=core.clock.now() + timedelta(minutes=5),
                max_replies=1))
        with core.db.transaction() as c:
            core.outbox.enqueue(c, envelope)
        store.finish(event.key)
    except (PermissionError, LookupError, ValueError):
        store.finish(event.key, failed=True)
        return {"op": 12}
    except Exception:
        store.finish(event.key, failed=True)
        raise
    return {"op": 12}


def run_workflow(core, store, event, decision):
    """Resolve bounded workflow data. TaskService enforces ownership and policy."""
    from qqbot.domain.interactions import WorkflowButton
    button = WorkflowButton.model_validate_json(json.dumps({"label": "action", "action": decision.action,
                                                            "payload": decision.payload or {}}))
    payload = button.payload
    access = core.access(event)
    if payload.task_id:
        # Group task visibility is broader than ownership. A button capability
        # must not let an issuer redirect another member's active task.
        from sqlalchemy import select

        from qqbot.storage.task_schema import tasks
        with core.db.engine.connect() as c:
            owner = c.execute(select(tasks.c.requester).where(tasks.c.public_id == payload.task_id,
                tasks.c.conversation == event.conversation.key)).scalar_one_or_none()
        if owner != event.principal.key:
            raise PermissionError("workflow task is not owned by this principal")
    if decision.action == "dismiss":
        return "已取消本次操作。", None
    if decision.action == "choice":
        if payload.task_id:
            inbound = InboundEvent(event_id="interaction:" + event.key, event_type="message.created",
                conversation=event.conversation, principal=event.principal, platform_message_id=event.id,
                occurred_at=event.occurred_at, received_at=core.clock.now(),
                segments=(TextSegment(text=payload.value),), mentions_bot=True, trace_id=event.key)
            core.tasks.add_input(payload.task_id, inbound, access, payload.value, question_id=payload.question_id)
            return "已提交选择：" + payload.value, None
        return "已选择：" + payload.value, None
    if decision.action == "page":
        pages, page = payload.pages, payload.page
        buttons = [{"label": label, "action": "page", "payload": {"pages": list(pages), "page": index}}
                   for label, index in (("上一页", page - 1), ("下一页", page + 1)) if 0 <= index < len(pages)]
        keyboard = workflow_keyboard(store, event.conversation, event.principal, buttons) if buttons else None
        return f"{page + 1}/{len(pages)}\n" + pages[page], keyboard
    if decision.action == "cancel_prompt":
        task = core.tasks.get(payload.task_id, access)
        core.permissions.require(access, "task.cancel", "task:" + task["id"])
        keyboard = workflow_keyboard(store, event.conversation, event.principal, [
            {"label": "确认取消", "action": "cancel_task", "payload": {"task_id": payload.task_id}},
            {"label": "保留任务", "action": "dismiss"},
        ], group_key="confirm:" + event.key, internal=True)
        return "确认取消任务 " + payload.task_id + "？", keyboard
    if decision.action == "cancel_task":
        result = core.tasks.cancel(payload.task_id, access)
        return "任务 " + payload.task_id + "：" + result["state"], None
    raise ValueError("unsupported application action")
