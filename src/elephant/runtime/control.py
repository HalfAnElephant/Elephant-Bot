"""Deterministic task controls and authenticated notification-reply binding."""
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from elephant.domain import InboundEvent, stable_id
from elephant.outbox.presentation import chat_bubbles, plain_text, split_text
from elephant.storage.agent_schema import focus
from elephant.storage.schema import audit_events, inbox, outbox
from elephant.storage.task_schema import notification_question_ids, notifications, tasks, waits

from .task_graph import TERMINAL

_PUBLIC_ID = r"T-[A-Za-z0-9][A-Za-z0-9-]{0,63}"
_CLOSED = TERMINAL | {"CANCEL_REQUESTED", "NEEDS_RECONCILIATION"}
_CANCEL = {"取消", "取消任务", "取消刚才那个", "取消刚才那个任务", "取消刚才的任务", "取消上个任务", "取消这个任务",
           "停止刚才那个", "停止刚才那个任务", "停止上个任务", "停止这个任务"}
_STATUS = {"任务状态", "任务进度", "进度怎么样", "任务怎么样了", "刚才那个怎么样", "刚才那个怎么样了",
           "刚才那个任务怎么样", "刚才那个任务怎么样了", "刚才那个完成了吗", "上个任务怎么样了", "现在怎么样了"}


@dataclass(frozen=True)
class TaskIntent:
    action: str
    reference: str | None = None
    text: str = ""


def task_references(text):
    return set(re.findall(rf"(?<![A-Za-z0-9_-])({_PUBLIC_ID})(?![A-Za-z0-9_-])", text))


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
    match = re.fullmatch(rf"(?:请|帮我)?(补充|追加|回答)(?:任务)?\s*({_PUBLIC_ID})(?:\s+|[：:])(.+)", original, re.DOTALL)
    if match:
        return TaskIntent("answer" if match[1] == "回答" else "input", match[2], match[3])
    match = re.fullmatch(r"(?:给|向)?(?:刚才那个(?:任务)?|上个任务|这个任务)(?:补充|追加)(?:\s+|[：:])(.+)", original, re.DOTALL)
    if match:
        return TaskIntent("input", text=match[1])
    return None


class TurnRouter:
    def __init__(self, core):
        self.core, self.db = core, core.db

    def command(self, event, command, argument):
        if command == "/tasks" and not argument.strip():
            return self._route(event, TaskIntent("list"))
        if command == "/followup":
            reference, _, text = argument.strip().partition(" ")
            if not reference or not text.strip():
                return "请引用需要补充的进度消息，再发送补充内容。"
            return self._route(event, TaskIntent("input", reference, text))
        return self._route(event, TaskIntent("cancel" if command == "/cancel" else "status", argument.strip() or None))

    def route(self, event):
        return self._route(event, task_intent(event.text))

    def replayable(self, event):
        with self.db.engine.connect() as conn:
            return conn.execute(select(audit_events.c.id).where(
                audit_events.c.id == stable_id("host-task-control", event.event_id))).scalar_one_or_none() is not None

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
        notices = conn.execute(select(notifications).distinct().join(outbox, outbox.c.origin_id == notifications.c.id).where(
            outbox.c.conversation_id == event.conversation.key, outbox.c.platform_message_id == event.reply_to,
            outbox.c.origin_kind == "task", outbox.c.state == "SENT")).mappings().all()
        referenced = []
        for notice in notices:
            row = conn.execute(select(tasks).where(tasks.c.id == notice["task_id"])).mappings().one()
            self._visible(conn, row["public_id"], event)
            referenced.append((row, notice))
        return referenced

    def _quoted_control(self, conn, event):
        if not event.reply_to:
            return None
        sources = conn.execute(select(outbox.c.origin_id).where(outbox.c.conversation_id == event.conversation.key,
            outbox.c.platform_message_id == event.reply_to, outbox.c.state == "SENT", outbox.c.origin_kind == "chat")).scalars()
        rows = conn.execute(select(audit_events.c.id, audit_events.c.detail).where(
            audit_events.c.kind == "host.task.control", audit_events.c.id.in_(
                [stable_id("host-task-control", source) for source in sources]))).all()
        if len(rows) > 1:
            raise LookupError("reply matches several control receipts")
        if not rows:
            return None
        identifier, raw = rows[0]
        receipt = json.loads(raw)
        if receipt["actor"] != event.principal.key or receipt["conversation"] != event.conversation.key:
            raise PermissionError("task control belongs to another subject")
        return identifier, receipt

    def _selection(self, conn, candidates, action, event, source_tasks):
        if not candidates:
            return "当前对话没有可操作的事项。可以直接告诉我需要做什么。"
        lines = []
        for index, task in enumerate(candidates, 1):
            view = self.core.tasks.graph.status(conn, task, access=self.core.access(event))
            if view.get("goal") is not None:
                self._status_sources(conn, task, source_tasks)
            title = ((view.get("goal") or "此前的请求").splitlines() or [""])[0][:60]
            lines.append(f"{index}. {title} · " + self._progress(task["state"]))
        prompt = "目前这些事情的进度如下。" if action == "list" else "请说明你指的是哪个目标，或引用对应的进度消息。"
        return prompt + "\n" + "\n".join(lines) + "\n回复选择序号即可，有效期为五分钟。"

    def _selection_receipt(self, conn, event, candidates, action, *, text="", questions=None):
        source_tasks = {}
        receipt = {"action": action, "response": self._selection(conn, candidates, action, event, source_tasks),
                   "references": [row["public_id"] for row in candidates]}
        if not candidates:
            return receipt
        receipt["delivery_basis"] = {"source_tasks": source_tasks}
        dialogue_id = conn.execute(select(focus.c.dialogue_id).where(
            focus.c.key == stable_id(event.conversation.key, event.principal.key))).scalar_one_or_none()
        receipt["selection"] = {"actor": event.principal.key, "conversation": event.conversation.key,
            "dialogue_id": dialogue_id, "source_seq": event.ingest_seq, "event_id": event.event_id,
            "expires_at": (self.core.clock.now() + timedelta(minutes=5)).isoformat(),
            "action": "status" if action == "list" else action, "text": text,
            "candidates": [{"task_id": row["id"], "reference": row["public_id"],
                "question_id": questions[index] if questions else None}
                for index, row in enumerate(candidates)], "consumed_by": None}
        return receipt

    def _choose(self, conn, event, quoted):
        match = re.fullmatch(r"(?:选择?|第)?\s*([1-9][0-9]?)\s*(?:个|项)?[。.!！]?", event.text.strip())
        if not match:
            return None
        if event.reply_to:
            if not quoted or not quoted[1].get("selection"):
                return None
            identifier, detail = quoted
        else:
            query = select(audit_events).where(audit_events.c.kind == "host.task.control",
                audit_events.c.actor == event.principal.key, audit_events.c.target == event.conversation.key)
            pending = []
            for row in conn.execute(query.order_by(audit_events.c.created_at.desc(), audit_events.c.id.desc()).limit(32)).mappings():
                detail = json.loads(row["detail"])
                selection = detail.get("selection")
                if selection and selection["actor"] == event.principal.key and selection["conversation"] == event.conversation.key:
                    pending.append((selection["source_seq"], row["created_at"], row["id"], detail))
            if not pending:
                return None
            _, _, identifier, detail = max(pending, key=lambda item: item[:3])
        selection = detail["selection"]
        if selection["consumed_by"]:
            return {"action": "status", "response": "此前的选择已处理，请重新查询需要处理的事项。"}
        if datetime.fromisoformat(selection["expires_at"]) <= self.core.clock.now():
            return {"action": "status", "response": "此前的选择已过期，请重新查询需要处理的事项。"}
        dialogue_id = conn.execute(select(focus.c.dialogue_id).where(
            focus.c.key == stable_id(event.conversation.key, event.principal.key))).scalar_one_or_none()
        if dialogue_id != selection["dialogue_id"]:
            return {"action": "status", "response": "当前对话已切换，请重新查询需要处理的事项。"}
        index = int(match[1]) - 1
        if index >= len(selection["candidates"]):
            return {"action": "status", "response": "请选择此前列表中的有效序号。"}
        candidate = selection["candidates"][index]
        target = self._visible(conn, candidate["reference"], event)
        if target["id"] != candidate["task_id"]:
            raise PermissionError("selection target identity changed")
        question_id = candidate["question_id"]
        if question_id and not self.core.tasks._open_input_wait(conn, target["id"], question_id):
            return {"action": "input", "response": "此前的问题已结束，请重新查询需要处理的事项。"}
        source = {}
        if selection["action"] in {"cancel", "input"}:
            if not selection.get("event_id"):
                return {"action": selection["action"], "response": "请重新发送原来的任务指令。"}
            source = {"source_event_id": selection["event_id"], "source_event_sha256": detail["event_sha256"]}
        selection["consumed_by"] = event.event_id
        conn.execute(update(audit_events).where(audit_events.c.id == identifier).values(detail=json.dumps(detail, ensure_ascii=False)))
        return {"action": selection["action"], "reference": target["public_id"], "task_id": target["id"],
                "text": selection["text"], "question_id": question_id, **source}

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

    def _question_target(self, conn, target, notice, event, *, text):
        root = self.core.tasks.graph.root(conn, target)
        query = self.core.tasks.open_input_waits()
        if target["kind"] == "goal":
            query = query.where((tasks.c.root_task_id == root["id"]) | (tasks.c.id == root["id"]))
        else:
            query = query.where(tasks.c.id == target["id"])
        question_ids = ()
        if notice and notice["purpose"] == "question":
            question_ids = notification_question_ids(conn, notice["id"])
            # A reply to an old resolved question cannot answer a newer unrelated question.
            query = query.where(waits.c.id.in_(question_ids))
        questions = conn.execute(query.order_by(waits.c.created_at, waits.c.id)).mappings().all()
        if questions and (len(questions) > 1 or (target["kind"] == "goal" and len(question_ids) > 1)):
            candidates = [conn.execute(select(tasks).where(tasks.c.id == item["task_id"])).mappings().one() for item in questions]
            for candidate in candidates:
                self._visible(conn, candidate["public_id"], event)
            return None, None, self._selection_receipt(conn, event, candidates, "input",
                text=text, questions=[row["id"] for row in questions])
        if questions:
            row = conn.execute(select(tasks).where(tasks.c.id == questions[0]["task_id"])).mappings().one()
            self._visible(conn, row["public_id"], event)
            return row, questions[0]["id"], None
        return None, None, {"action": "input", "response": "这个问题已结束或无法确认，请引用当前需要回答的问题。"}

    def _bind(self, conn, event, intent):
        if event.reply_to and conn.execute(select(inbox.c.id).where(
                inbox.c.conversation_id == event.conversation.key, inbox.c.event_type == "message.recalled",
                inbox.c.message_id == event.reply_to)).first():
            raise LookupError("referenced message was recalled")
        referenced = self._referenced(conn, event)
        quoted_control = self._quoted_control(conn, event)
        quoted = quoted_control[1] if quoted_control else None
        if quoted and quoted.get("reference"):
            task = self._visible(conn, quoted["reference"], event)
            if task["id"] != quoted["task_id"]:
                raise PermissionError("task control target identity changed")
            referenced.append((task, {"purpose": "progress"}))
        if intent and intent.action == "list":
            candidates = self._roots(conn, event, current_only=False)
            return self._selection_receipt(conn, event, candidates, "list")
        if len(referenced) > 1:
            return {"action": "status", "response": "无法确认引用的是哪项任务，请单独发送明确的任务操作。"}
        target, notice = referenced[0] if referenced else (None, None)
        if intent and intent.reference:
            explicit = self._visible(conn, intent.reference, event)
            relayed_answer = (notice and notice["purpose"] == "question" and intent.action == "answer"
                              and target["kind"] == "goal" and target["id"] == explicit["root_task_id"])
            if event.reply_to and not relayed_answer and (target is None or target["id"] != explicit["id"]):
                return {"action": "status", "response": "任务编号与引用消息无法对应，请单独发送明确的任务操作。"}
            target = explicit
        elif target is None:
            if not intent:
                return self._choose(conn, event, quoted_control) or (quoted and {
                    "action": "status", "response": "请引用对应的任务进度消息，或单独说明要操作的任务。"})
            if event.reply_to:
                raise LookupError("reply does not identify a task")
            candidates = self._roots(conn, event)
            if len(candidates) != 1:
                return self._selection_receipt(conn, event, candidates, intent.action, text=intent.text)
            target = candidates[0]
        if not intent:
            if target["state"] in _CLOSED:
                return {"action": "status", "response": "这项任务已无法接收补充内容。需要继续时，请另发一条请求。"}
            intent = TaskIntent("answer" if notice["purpose"] == "question" else "input", text=event.text)
        action = intent.action
        question_id = None
        if action == "answer":
            target, question_id, selection = self._question_target(conn, target, notice, event, text=intent.text)
            if selection:
                return selection
            action = "input"
        return {"action": action, "reference": target["public_id"], "task_id": target["id"],
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

    def _status_response(self, view):
        response = self._progress(view["state"]) + "。"
        progress = view["progress"]
        if progress["current_action"] and progress["current_action"] != self._progress(view["state"]):
            response += "\n" + progress["current_action"] + "。"
        counts = progress.get("verified_counts", {})
        facts = []
        for kind, unit in (("task", "项执行工作"), ("observation", "份资料核对记录"),
                           ("operation", "条已核实操作回执"), ("artifact", "份已登记文件")):
            if counts.get(kind):
                facts.append(str(counts[kind]) + unit)
        if facts:
            response += "\n已有" + "、".join(facts) + "。"
        if progress["last_effective_progress_at"]:
            response += "\n最近有效进展时间 " + progress["last_effective_progress_at"]
        if view.get("goal"):
            response += "\n" + view["goal"][:300]
        if (view.get("result") or {}).get("summary"):
            response += "\n" + view["result"]["summary"][:600]
        return response

    def _status_sources(self, conn, task, source_tasks):
        root = self.core.tasks.graph.root(conn, task)
        for row in (task, root):
            source_tasks.setdefault(row["id"], {"context": json.loads(row["context_json"])})
        source_tasks[task["id"]]["goal"] = task["goal"]

    def _legacy_delivery_basis(self, conn, event, referenced):
        access = self.core.access(event)
        facts, contexts, references = [], {}, []
        for task in referenced:
            task = self._visible(conn, task["public_id"], event)
            root = self.core.tasks.graph.root(conn, task)
            self._visible(conn, root["public_id"], event)
            view = self.core.tasks.graph.status(conn, task, access=access)
            projection = {key: value for key, value in view["progress"].items() if key != "budget"}
            projection["delivery"] = {key: value for key, value in projection["delivery"].items() if key != "outbox_states"}
            facts.append({"view": {key: value for key, value in view.items() if key not in {"progress", "last_progress"}},
                          "progress": projection})
            for row in {task["id"]: task, root["id"]: root}.values():
                context = json.loads(row["context_json"])
                facts.append({key: row[key] for key in ("id", "owner_id", "conversation", "requester", "kind",
                    "root_task_id", "parent_task_id", "goal_revision", "goal", "goal_json", "result_json", "state")} | {
                    "context": context})
                if view.get("goal") is not None or view.get("result") is not None or projection["verified_completed"]:
                    contexts[row["id"]] = context
            references.append(task["public_id"])
        encoded = json.dumps(facts, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return {"references": references, "fingerprint": hashlib.sha256(encoded.encode()).hexdigest(),
                "contexts": list(contexts.values())}

    def delivery_valid(self, envelope):
        if envelope.origin_kind != "chat":
            return None
        identifier = stable_id("host-task-control", envelope.origin_id)
        with self.db.engine.connect() as conn:
            raw = conn.execute(select(audit_events.c.detail).where(audit_events.c.id == identifier,
                audit_events.c.kind == "host.task.control")).scalar_one_or_none()
            if raw is None:
                return None
            receipt = json.loads(raw)
            source = conn.execute(select(inbox.c.event_json).where(inbox.c.id == envelope.origin_id)).scalar_one_or_none()
            if not source or hashlib.sha256(source.encode()).hexdigest() != receipt.get("event_sha256"):
                return False
            event = InboundEvent.model_validate_json(source)
            if (event.conversation != envelope.target or event.principal != envelope.requester
                    or receipt.get("actor") != event.principal.key or receipt.get("conversation") != event.conversation.key
                    or not self.core.allowed(event) or envelope.official_content is not None):
                return False
            response = receipt.get("response")
            if not isinstance(response, str):
                return False
            if envelope.content_format == "markdown":
                if envelope.part_no != 0 or envelope.text != response:
                    return False
            else:
                parts = [part for bubble in chat_bubbles(response) for part in split_text(plain_text(bubble))]
                if envelope.part_no < 0 or envelope.part_no >= len(parts) or envelope.text != parts[envelope.part_no]:
                    return False
            try:
                self._authorize(conn, event, receipt)
                access = self.core.access(event)
                source_context = {"scope": event.conversation.key, "principal": event.principal.key,
                    "workspace_id": receipt.get("source_workspace_id", "personal"),
                    "source_manifest": [{"kind": "inbox", "id": event.event_id, "digest": receipt["event_sha256"]}]}
                if not self.core.validate_context(access, source_context):
                    return False
                basis = receipt.get("delivery_basis")
                if basis:
                    if "source_tasks" in basis:
                        graph = self.core.tasks.graph
                        for task_id, snapshot in basis["source_tasks"].items():
                            task = conn.execute(select(tasks).where(tasks.c.id == task_id)).mappings().one_or_none()
                            if (task is None or not graph.may_read(task_id, access)
                                    or "goal" in snapshot and task["goal"] != snapshot["goal"]
                                    or not self.core.validate_context(access, snapshot["context"])
                                    or graph.event_scope_revision(conn, task, access) is None):
                                return False
                        return True
                    referenced = [self._visible(conn, reference, event) for reference in basis["references"]]
                    current = self._legacy_delivery_basis(conn, event, referenced)
                    if current["fingerprint"] != basis["fingerprint"]:
                        return False
                    return all(self.core.validate_context(access, context) for context in basis["contexts"])
                return not receipt.get("references") and not (
                    receipt["action"] in {"status", "list"} and receipt.get("reference"))
            except (PermissionError, LookupError, ValueError):
                return False

    def _effect_source(self, conn, event, receipt):
        source = event
        if receipt.get("source_event_id"):
            raw = conn.scalar(select(inbox.c.event_json).where(inbox.c.id == receipt["source_event_id"],
                inbox.c.conversation_id == event.conversation.key, inbox.c.principal_id == event.principal.key))
            if not raw or hashlib.sha256(raw.encode()).hexdigest() != receipt["source_event_sha256"]:
                raise PermissionError("selection source changed")
            source = InboundEvent.model_validate_json(raw)
        messages = {event.platform_message_id, event.reply_to, source.platform_message_id, source.reply_to} - {None}
        if conn.execute(select(inbox.c.id).where(inbox.c.conversation_id == event.conversation.key,
                inbox.c.event_type == "message.recalled", inbox.c.message_id.in_(messages)).limit(1)).first():
            raise PermissionError("task control source was recalled")
        return source

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
            receipt.setdefault("source_workspace_id", self.core.sessions.workspace_for(event, conn))
            if not old:
                conn.execute(sqlite_insert(audit_events).values(id=identifier, kind="host.task.control",
                    target=receipt.get("task_id", event.conversation.key), actor=event.principal.key,
                    created_at=self.core.clock.now().isoformat(), detail=json.dumps(receipt, ensure_ascii=False))
                    .on_conflict_do_nothing(index_elements=[audit_events.c.id]))
                receipt = json.loads(conn.execute(select(audit_events.c.detail).where(audit_events.c.id == identifier)).scalar_one())
                if any(receipt.get(key) != value for key, value in identity.items()):
                    raise ValueError("conflicting task-control event replay")
            target = self._authorize(conn, event, receipt)
            if receipt.get("response") is None and receipt["action"] == "status":
                source_tasks = {}
                view = self.core.tasks.graph.status(conn, target, access=self.core.access(event), source_tasks=source_tasks)
                receipt["response"] = self._status_response(view)
                if view["goal"] is not None or view["result"] is not None:
                    self._status_sources(conn, target, source_tasks)
                receipt["delivery_basis"] = {"source_tasks": source_tasks}
                conn.execute(update(audit_events).where(audit_events.c.id == identifier).values(
                    detail=json.dumps(receipt, ensure_ascii=False)))
        if receipt.get("response") is not None:
            return receipt["response"]
        with self.db.transaction() as conn:
            receipt = json.loads(conn.scalar(select(audit_events.c.detail).where(audit_events.c.id == identifier)))
            if receipt.get("response") is not None:
                return receipt["response"]
            self._authorize(conn, event, receipt)
            access = self.core.access(event)
            action = receipt["action"]
            try:
                with conn.begin_nested():
                    source = self._effect_source(conn, event, receipt)
                    if action == "cancel":
                        result = self.core.tasks.cancel(receipt["reference"], access, conn=conn)
                        response = self._progress(result["state"]) + "。"
                    else:
                        self.core.tasks.add_input(receipt["reference"], source, access, receipt["text"],
                            question_id=receipt["question_id"], selected_by=event if source is not event else None,
                            conn=conn)
                        response = "已收到补充内容。"
            except (PermissionError, LookupError, ValueError):
                response = ("未执行这次取消。请重新发送明确的取消指令。" if action == "cancel"
                            else "未接收这次补充。请重新发送内容，或引用当前需要回答的问题。")
            receipt["response"] = response
            conn.execute(update(audit_events).where(audit_events.c.id == identifier).values(detail=json.dumps(receipt, ensure_ascii=False)))
        return response
