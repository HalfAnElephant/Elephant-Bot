"""Deterministic task controls and authenticated notification-reply binding."""
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from qqbot.domain import InboundEvent, stable_id
from qqbot.outbox.presentation import chat_bubbles, plain_text, split_text
from qqbot.storage.agent_schema import agent_events, focus, leader_inbox
from qqbot.storage.schema import audit_events, inbox, outbox
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

    def _selection(self, conn, candidates, action, event):
        if not candidates:
            return "当前对话没有可操作的事项。可以直接告诉我需要做什么。"
        lines = []
        for index, task in enumerate(candidates, 1):
            view = self.core.tasks.graph.status(conn, task, access=self.core.access(event))
            title = ((view.get("goal") or "此前的请求").splitlines() or [""])[0][:60]
            lines.append(f"{index}. {title} · " + self._progress(task["state"]))
        prompt = "目前这些事情的进度如下。" if action == "list" else "请说明你指的是哪个目标，或引用对应的进度消息。"
        return prompt + "\n" + "\n".join(lines) + "\n回复选择序号即可，有效期为五分钟。"

    def _selection_receipt(self, conn, event, candidates, action, *, text="", questions=None):
        receipt = {"action": action, "response": self._selection(conn, candidates, action, event),
                   "references": [row["public_id"] for row in candidates]}
        if not candidates:
            return receipt
        receipt["delivery_basis"] = self._freeze_delivery(conn, event, candidates)
        dialogue_id = conn.execute(select(focus.c.dialogue_id).where(
            focus.c.key == stable_id(event.conversation.key, event.principal.key))).scalar_one_or_none()
        receipt["selection"] = {"actor": event.principal.key, "conversation": event.conversation.key,
            "dialogue_id": dialogue_id, "source_seq": event.ingest_seq,
            "expires_at": (self.core.clock.now() + timedelta(minutes=5)).isoformat(),
            "action": "status" if action == "list" else action, "text": text,
            "candidates": [{"task_id": row["id"], "reference": row["public_id"],
                "question_id": questions[index] if questions else None}
                for index, row in enumerate(candidates)], "consumed_by": None}
        return receipt

    def _choose(self, conn, event):
        match = re.fullmatch(r"(?:选择?|第)?\s*([1-9][0-9]?)\s*(?:个|项)?[。.!！]?", event.text.strip())
        if not match:
            return None
        query = select(audit_events).where(audit_events.c.kind == "host.task.control",
            audit_events.c.actor == event.principal.key, audit_events.c.target == event.conversation.key)
        if event.reply_to:
            sources = conn.execute(select(outbox.c.origin_id).where(outbox.c.conversation_id == event.conversation.key,
                outbox.c.platform_message_id == event.reply_to, outbox.c.state == "SENT", outbox.c.origin_kind == "chat")).scalars().all()
            query = query.where(audit_events.c.id.in_([stable_id("host-task-control", source) for source in sources]))
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
        if question_id:
            question = conn.execute(select(waits).where(waits.c.id == question_id,
                waits.c.task_id == target["id"], waits.c.state.in_({"INTENT", "WAITING"}))).mappings().first()
            if not question or question["expires_at"] and question["expires_at"] <= self.core.clock.now().isoformat():
                return {"action": "input", "response": "此前的问题已结束，请重新查询需要处理的事项。"}
        elif selection["action"] == "input":
            target, question_id, next_selection = self._question_target(conn, target, None, event,
                                                                        text=selection["text"])
            if next_selection:
                return next_selection
        selection["consumed_by"] = event.event_id
        conn.execute(update(audit_events).where(audit_events.c.id == identifier).values(detail=json.dumps(detail, ensure_ascii=False)))
        return {"action": selection["action"], "reference": target["public_id"], "task_id": target["id"],
                "text": selection["text"], "question_id": question_id}

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

    def _question_target(self, conn, target, notice, event, *, text=None):
        if notice and notice["purpose"] != "question":
            return target, None, None
        root = self.core.tasks.graph.root(conn, target)
        query = select(waits.c.id, waits.c.task_id).join(tasks, tasks.c.id == waits.c.task_id).where(
            waits.c.kind == "input", waits.c.state.in_(["INTENT", "WAITING"]),
            tasks.c.state.in_(["RUNNING", "WAITING_INPUT"]),
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
            return None, None, self._selection_receipt(conn, event, candidates, "input",
                text=event.text if text is None else text,
                questions=[row["id"] for row in questions])
        if questions:
            row = conn.execute(select(tasks).where(tasks.c.id == questions[0]["task_id"])).mappings().one()
            return row, questions[0]["id"], None
        return target, None, None

    def _bind(self, conn, event, intent):
        notice = None
        chosen = self._choose(conn, event)
        if chosen:
            return chosen
        if intent and intent.action == "list":
            candidates = self._roots(conn, event, current_only=False)
            return self._selection_receipt(conn, event, candidates, "list")
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
                return self._selection_receipt(conn, event, candidates, intent.action, text=intent.text)
            target = candidates[0]
            notice = referenced[0][1] if referenced else None
        question_id = None
        if intent.action == "input":
            target, question_id, selection = self._question_target(conn, target, notice, event, text=intent.text)
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

    def _freeze_delivery(self, conn, event, referenced, *, views=None):
        access = self.core.access(event)
        facts, contexts, references = [], {}, []
        for task in referenced:
            task = self._visible(conn, task["public_id"], event)
            root = self.core.tasks.graph.root(conn, task)
            self._visible(conn, root["public_id"], event)
            view = (views or {}).get(task["id"]) or self.core.tasks.graph.status(conn, task, access=access)
            projection = {key: value for key, value in view["progress"].items() if key != "budget"}
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
                    referenced = [self._visible(conn, reference, event) for reference in basis["references"]]
                    current = self._freeze_delivery(conn, event, referenced)
                    if current["fingerprint"] != basis["fingerprint"]:
                        return False
                    return all(self.core.validate_context(access, context) for context in basis["contexts"])
                return not receipt.get("references") and not (
                    receipt["action"] in {"status", "list"} and receipt.get("reference"))
            except (PermissionError, LookupError, ValueError):
                return False

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
                view = self.core.tasks.graph.status(conn, target, access=self.core.access(event))
                receipt["response"] = self._status_response(view)
                receipt["delivery_basis"] = self._freeze_delivery(conn, event, [target], views={target["id"]: view})
                conn.execute(update(audit_events).where(audit_events.c.id == identifier).values(
                    detail=json.dumps(receipt, ensure_ascii=False)))
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
            raise ValueError("unsupported host task control action")
        receipt["response"] = response
        with self.db.transaction() as conn:
            conn.execute(update(audit_events).where(audit_events.c.id == identifier).values(detail=json.dumps(receipt, ensure_ascii=False)))
        return response
