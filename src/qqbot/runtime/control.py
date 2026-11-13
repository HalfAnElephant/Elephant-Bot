"""Deterministic task controls and authenticated notification-reply binding."""
import hashlib
import json
import re
from dataclasses import dataclass

from sqlalchemy import select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from qqbot.domain import stable_id
from qqbot.storage.agent_schema import agent_events, focus, leader_inbox
from qqbot.storage.schema import audit_events, outbox
from qqbot.storage.task_schema import inputs, notifications, runs, tasks, waits

from .task_graph import TERMINAL

_PUBLIC_ID = r"T-[A-Za-z0-9][A-Za-z0-9-]{0,63}"
_CLOSED = TERMINAL | {"CANCEL_REQUESTED"}
_CANCEL = {"取消", "取消任务", "取消刚才那个", "取消刚才那个任务", "取消刚才的任务", "取消上个任务", "取消这个任务",
           "停止刚才那个", "停止刚才那个任务", "停止上个任务", "停止这个任务"}
_STATUS = {"任务状态", "任务进度", "进度怎么样", "任务怎么样了", "刚才那个怎么样", "刚才那个怎么样了",
           "刚才那个任务怎么样", "刚才那个任务怎么样了", "刚才那个完成了吗", "上个任务怎么样了", "现在怎么样了"}


@dataclass(frozen=True)
class TaskIntent:
    action: str
    reference: str | None = None
    text: str = ""


def task_intent(text):
    """Match complete bounded commands, leaving negation, quotations and prose to Leader."""
    original = text.strip()
    text = original.rstrip("。.!！?？").strip()
    if text in _CANCEL:
        return TaskIntent("cancel")
    if text in _STATUS:
        return TaskIntent("status")
    match = re.fullmatch(rf"(?:请|帮我)?(?:取消|停止)(?:任务)?\s*({_PUBLIC_ID})(?:这个任务|任务)?", text)
    if match:
        return TaskIntent("cancel", match[1])
    match = re.fullmatch(rf"(?:请|帮我)?把\s*({_PUBLIC_ID})\s*(?:取消|停止)", text)
    if match:
        return TaskIntent("cancel", match[1])
    match = re.fullmatch(rf"(?:请|帮我)?(?:查看|查询|看看|看下)(?:任务)?\s*({_PUBLIC_ID})\s*(?:的)?\s*(?:状态|进度)?", text)
    if match:
        return TaskIntent("status", match[1])
    match = re.fullmatch(rf"(?:任务)?\s*({_PUBLIC_ID})\s*(?:的|现在)?\s*(?:状态|进度|怎么样|怎么样了|完成了吗)?", text)
    if match:
        return TaskIntent("status", match[1])
    match = re.fullmatch(rf"(?:请|帮我)?(?:补充|追加|回答)(?:任务)?\s*({_PUBLIC_ID})(?:\s+|[：:])(.+)", original, re.DOTALL)
    if match:
        return TaskIntent("input", match[1], match[2])
    match = re.fullmatch(r"(?:给|向)?(?:刚才那个(?:任务)?|上个任务|这个任务)(?:补充|追加)(?:\s+|[：:])(.+)", original, re.DOTALL)
    if match:
        return TaskIntent("input", text=match[1])
    return None


class HostTaskControls:
    def __init__(self, core):
        self.core, self.db = core, core.db

    def command(self, event, command, argument):
        if command == "/任务" and not argument.strip():
            return self._route(event, TaskIntent("list"))
        if command == "/补充":
            reference, _, text = argument.strip().partition(" ")
            if not reference or not text.strip():
                return "请引用需要补充的进度消息，再发送补充内容。"
            return self._route(event, TaskIntent("input", reference, text))
        return self._route(event, TaskIntent("cancel" if command == "/取消" else "status", argument.strip() or None))

    def route(self, event):
        return self._route(event, task_intent(event.text))

    def _visible(self, conn, reference, event):
        row = self.core.tasks._visible(conn, reference, self.core.access(event))
        if row["requester"] != event.principal.key:
            raise PermissionError("task belongs to another subject")
        return row

    def _roots(self, conn, event, *, current_only=True):
        query = select(tasks).where(tasks.c.conversation == event.conversation.key,
            tasks.c.requester == event.principal.key, tasks.c.kind == "goal")
        if current_only:
            dialogue_id = conn.execute(select(focus.c.dialogue_id).where(
                focus.c.key == stable_id(event.conversation.key, event.principal.key))).scalar_one_or_none()
            query = query.where(tasks.c.origin_dialogue_id == dialogue_id, tasks.c.state.not_in(_CLOSED))
        access = self.core.access(event)
        return [row for row in conn.execute(query.order_by(tasks.c.created_at.desc(), tasks.c.id).limit(8)).mappings()
                if self.core.permissions.check(access, "task.read", "task:" + row["id"]).allowed]

    def _referenced(self, conn, event):
        if not event.reply_to:
            return []
        notices = conn.execute(select(notifications).join(outbox, outbox.c.origin_id == notifications.c.id).where(
            outbox.c.conversation_id == event.conversation.key, outbox.c.platform_message_id == event.reply_to,
            outbox.c.origin_kind == "task", outbox.c.state == "SENT")).mappings().all()
        referenced = {}
        for notice in notices:
            row = conn.execute(select(tasks).where(tasks.c.id == notice["task_id"])).mappings().one()
            if row["requester"] != event.principal.key or row["conversation"] != event.conversation.key:
                raise PermissionError("task notification belongs to another subject")
            self._visible(conn, row["public_id"], event)
            root = self.core.tasks.graph.root(conn, row)
            referenced[root["id"]] = (root, notice)
        return list(referenced.values())

    def _selection(self, conn, candidates, action):
        if not candidates:
            return "当前对话没有可操作的事项。可以直接告诉我需要做什么。"
        lines = []
        for task in candidates:
            view = self.core.tasks.graph.status(conn, task)
            title = ((view.get("goal") or "此前的请求").splitlines() or [""])[0][:60]
            lines.append(title + " · " + self._progress(task["state"]))
        prompt = "目前这些事情的进度如下。" if action == "list" else "请说明你指的是哪个目标，或引用对应的进度消息。"
        return prompt + "\n" + "\n".join(lines)

    @staticmethod
    def _progress(state):
        return {
            "QUEUED": "已收到，尚未开始",
            "RUNNING": "正在处理",
            "WAITING_CHILDREN": "正在处理",
            "WAITING_INPUT": "等待你补充信息",
            "WAITING_EXTERNAL": "等待外部结果",
            "CANCEL_REQUESTED": "正在停止",
            "SUCCEEDED": "已完成",
            "PARTIAL": "已完成一部分",
            "BLOCKED": "暂时无法继续",
            "FAILED": "这次未能完成",
            "CANCELLED": "已取消",
            "EXPIRED": "已超时停止",
            "NEEDS_RECONCILIATION": "正在核实处理结果",
        }.get(state, "正在核实进度")

    def _question_target(self, conn, target, notice, event):
        if notice and notice["purpose"] != "question":
            return target, None, None
        root = self.core.tasks.graph.root(conn, target)
        query = select(waits.c.id, waits.c.task_id).join(tasks, tasks.c.id == waits.c.task_id).where(
            waits.c.kind == "input", waits.c.state.in_(["INTENT", "WAITING"]),
            tasks.c.state.in_(["RUNNING", "WAITING_INPUT"]), tasks.c.deadline > self.core.clock.now().isoformat(),
            (waits.c.expires_at.is_(None)) | (waits.c.expires_at > self.core.clock.now().isoformat()))
        if target["kind"] == "goal":
            query = query.where((tasks.c.root_task_id == root["id"]) | (tasks.c.id == root["id"]))
        else:
            query = query.where(tasks.c.id == target["id"])
        question_ids = set()
        if notice and notice["purpose"] == "question":
            question_ids.update(conn.execute(select(waits.c.id).where(waits.c.notification_id == notice["id"])).scalars())
            raw = conn.execute(select(runs.c.request_json).where(runs.c.id == notice["run_id"])).scalar_one_or_none()
            claim = json.loads(raw) if raw else {}
            if claim.get("purpose") == "question" and claim.get("target_root_task_id") == root["id"]:
                question_ids.update(json.loads(payload).get("question_id") for payload in conn.execute(
                    select(agent_events.c.payload_json).join(leader_inbox, leader_inbox.c.event_id == agent_events.c.id)
                    .where(leader_inbox.c.id.in_(claim.get("source_ids", [])), agent_events.c.kind == "needs_input")).scalars())
            # A reply to an old resolved question cannot answer a newer unrelated question.
            query = query.where(waits.c.id.in_(question_ids))
        questions = conn.execute(query).mappings().all()
        if len(questions) > 1:
            candidates = [conn.execute(select(tasks).where(tasks.c.id == item["task_id"])).mappings().one() for item in questions]
            for candidate in candidates:
                self._visible(conn, candidate["public_id"], event)
            return None, None, {"response": self._selection(conn, candidates, "input"),
                                "references": [row["public_id"] for row in candidates]}
        if questions:
            row = conn.execute(select(tasks).where(tasks.c.id == questions[0]["task_id"])).mappings().one()
            return row, questions[0]["id"], None
        return target, None, None

    def _bind(self, conn, event, intent):
        notice = None
        if intent and intent.action == "list":
            candidates = self._roots(conn, event, current_only=False)
            return {"action": "list", "response": self._selection(conn, candidates, "list"),
                    "references": [row["public_id"] for row in candidates]}
        if intent and intent.reference:
            target = self._visible(conn, intent.reference, event)
        else:
            referenced = self._referenced(conn, event)
            if not intent:
                if not referenced or all(row[0]["state"] in _CLOSED for row in referenced):
                    return None
                intent = TaskIntent("input", text=event.text)
            candidates = [row[0] for row in referenced] if referenced else self._roots(conn, event)
            if len(candidates) != 1:
                return {"action": intent.action, "response": self._selection(conn, candidates, intent.action),
                        "references": [row["public_id"] for row in candidates]}
            target = candidates[0]
            notice = referenced[0][1] if referenced else None
        question_id = None
        if intent.action == "input":
            target, question_id, selection = self._question_target(conn, target, notice, event)
            if selection:
                return {"action": "input", **selection}
        return {"action": intent.action, "reference": target["public_id"], "task_id": target["id"],
                "text": intent.text, "question_id": question_id}

    def _authorize(self, conn, event, receipt):
        for reference in receipt.get("references", []):
            self._visible(conn, reference, event)
        if not receipt.get("reference"):
            return None
        row = self._visible(conn, receipt["reference"], event)
        action = {"status": "task.read", "cancel": "task.cancel", "input": "task.input"}[receipt["action"]]
        self.core.permissions.require(self.core.access(event), action, "task:" + row["id"])
        return row

    def _route(self, event, intent):
        identifier = stable_id("host-task-control", event.event_id)
        identity = {"actor": event.principal.key, "conversation": event.conversation.key,
                    "event_sha256": hashlib.sha256(event.model_dump_json().encode()).hexdigest()}
        with self.db.transaction() as conn:
            old = conn.execute(select(audit_events.c.detail).where(audit_events.c.id == identifier)).scalar_one_or_none()
            receipt = json.loads(old) if old else self._bind(conn, event, intent)
            if receipt is None:
                return None
            if old and any(receipt.get(key) != value for key, value in identity.items()):
                raise ValueError("conflicting task-control event replay")
            receipt.update(identity)
            if not old:
                conn.execute(sqlite_insert(audit_events).values(id=identifier, kind="host.task.control",
                    target=receipt.get("task_id", event.conversation.key), actor=event.principal.key,
                    created_at=self.core.clock.now().isoformat(), detail=json.dumps(receipt, ensure_ascii=False))
                    .on_conflict_do_nothing(index_elements=[audit_events.c.id]))
                receipt = json.loads(conn.execute(select(audit_events.c.detail).where(audit_events.c.id == identifier)).scalar_one())
                if any(receipt.get(key) != value for key, value in identity.items()):
                    raise ValueError("conflicting task-control event replay")
            target = self._authorize(conn, event, receipt)
        if receipt.get("response") is not None:
            return receipt["response"]
        access = self.core.access(event)
        if receipt["action"] == "cancel":
            result = self.core.tasks.cancel(receipt["reference"], access)
            response = self._progress(result["state"]) + "。"
        elif receipt["action"] == "input":
            with self.db.engine.connect() as conn:
                old_input = conn.execute(select(inputs).where(inputs.c.source_id == event.event_id)).mappings().first()
            if old_input:
                if (old_input["task_id"] != receipt["task_id"] or old_input["text"] != receipt["text"]
                        or old_input["question_id"] != receipt["question_id"]
                        or old_input["actor"] not in {None, event.principal.key}):
                    raise ValueError("conflicting task input replay")
            else:
                self.core.tasks.add_input(receipt["reference"], event, access, receipt["text"], question_id=receipt["question_id"])
            response = "已收到补充内容。"
        else:
            with self.db.engine.connect() as conn:
                target = self._visible(conn, receipt["reference"], event)
                view = self.core.tasks.graph.status(conn, target)
            response = self._progress(view["state"]) + "。"
            if view.get("goal"):
                response += "\n" + view["goal"][:300]
            if (view.get("result") or {}).get("summary"):
                response += "\n" + view["result"]["summary"][:600]
        receipt["response"] = response
        with self.db.transaction() as conn:
            conn.execute(update(audit_events).where(audit_events.c.id == identifier).values(detail=json.dumps(receipt, ensure_ascii=False)))
        return response
