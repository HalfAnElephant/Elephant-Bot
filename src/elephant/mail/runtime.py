"""Host-bound mailbox operations share the Agent's current lease and authority."""
import asyncio
import base64
import hashlib
import json

from sqlalchemy import select

from elephant.domain import InboundEvent
from elephant.mail.accounts import MailAccounts
from elephant.mail.sending import MailSender, address
from elephant.mail.triggers import MailTriggers
from elephant.permissions import PermissionPolicy, PermissionRule, PermissionService
from elephant.storage.task_schema import runs, tasks


class MailRuntime:
    def __init__(self, core, *, accounts=None, sender=None):
        self.core, self.db = core, core.db
        self.accounts = accounts or MailAccounts(core)
        self.sender = sender or MailSender(core, self.accounts)
        self.triggers = MailTriggers(core, self.accounts)

    def receipts(self, conn, event, context, *, access=None, policy_snapshot=None):
        if not context.get("mail_receipt_id") and not context.get("task_sources"):
            return []
        access = access or self.core.access(event)
        pending, visited, receipts = [context], set(), {}
        while pending:
            current = pending.pop()
            if current.get("mail_receipt_id"):
                if current.get("workspace_id", "personal") != context.get("workspace_id", "personal"):
                    raise PermissionError("mail task sources cannot cross projects")
                receipt = self.triggers.receipt(conn, event, current)
                receipts[receipt["id"]] = receipt
            for task_id in current.get("task_sources", ()):
                if task_id in visited:
                    continue
                visited.add(task_id)
                source = conn.execute(select(tasks.c.requester, tasks.c.conversation, tasks.c.context_json).where(
                    tasks.c.id == task_id)).mappings().first()
                if (source is None or source["requester"] != event.principal.key
                        or source["conversation"] != event.conversation.key):
                    raise PermissionError("task source is unavailable in this mail scope")
                self.core.permissions.require(access, "task.read", "task:" + task_id, snapshot=policy_snapshot)
                pending.append(json.loads(source["context_json"]))
        return list(receipts.values())

    @staticmethod
    def _granted(receipt, access, action, resources):
        from elephant.runtime.schedules import ScheduleSpec

        try:
            sender = address(json.loads(receipt["message_json"])["sender_address"])
        except ValueError:
            sender = None
        snapshot = [PermissionRule.model_validate(rule) for rule in json.loads(receipt["permissions_json"])]
        current = ScheduleSpec.model_validate_json(receipt["spec_json"]).mail_permissions
        for rules in (snapshot, current):
            expanded = tuple(rule.model_copy(update={"resources": tuple(
                "mail-recipient:" + sender if resource == "mail-recipient:$sender" and sender else resource
                for resource in rule.resources)}) for rule in rules)
            policy = PermissionService(policy=PermissionPolicy(preset="custom", default_effect="deny", rules=expanded))
            if not all(policy.check(access, action, resource).allowed for resource in resources):
                return False
        return True

    def authorize_context(self, access, context, action, resources):
        event = type("Identity", (), {"principal": access.principal, "conversation": access.conversation})()
        with self.db.engine.connect() as conn:
            receipts = self.receipts(conn, event, context, access=access)
        if not receipts or context.get("role") == "leader" and context.get("purpose") == "user":
            return
        if action == "model.request" or action.startswith("task.") or action in {
                "notification.send", "mail.input.read", "mail.status", "mail.send.prepare"}:
            return
        for receipt in receipts:
            if action == "mail.read" and all(resource == "mail-account:" + receipt["account_id"] for resource in resources):
                continue
            if not self._granted(receipt, access, action, resources):
                raise PermissionError("mail rule did not authorize this action and resource")

    def _claim(self, identity, action, *, effect="read", resources=()):
        if self.core.stopping:
            raise PermissionError("mail runtime is stopping")
        payload = {"action": action, "effect": effect, "resources": list(resources)}
        with self.db.engine.connect() as conn:
            role = conn.execute(select(runs.c.role).where(runs.c.id == identity[0])).scalar_one_or_none()
            if role == "leader":
                run, claim = self.core.leader.lease(conn, *identity)
                self.core.leader.authorize(conn, run, claim, payload)
                event = InboundEvent.model_validate_json(claim["event_json"])
                context = claim["context"]
                root_id = claim.get("target_root_task_id")
                root = conn.execute(select(tasks).where(tasks.c.id == root_id)).mappings().first() if root_id else None
                scope_id = root_id or "user:" + claim["source_ids"][0]
                user_turn = claim["purpose"] == "user" and root is None
            elif role == "worker":
                run, task = self.core.tasks._lease(conn, *identity)
                self.core.tasks.authorize(*identity, payload)
                event = InboundEvent.model_validate_json(task["event_json"])
                context = json.loads(task["context_json"])
                root = self.core.tasks.graph.root(conn, task)
                scope_id, user_turn = root["id"], False
            else:
                raise PermissionError("mail requires an active user Leader or Worker")
        return event, context, root, scope_id, user_turn

    def _message_scope(self, event, context, account, folder, uid=None, uidvalidity=None):
        with self.db.engine.connect() as conn:
            receipts = self.receipts(conn, event, context)
        if context.get("role") == "leader" and context.get("purpose") == "user":
            return
        for receipt in receipts:
            original = (account["id"] == receipt["account_id"] and folder == receipt["folder"]
                and uid == receipt["uid"] and uidvalidity == receipt["uidvalidity"])
            if not original and not self._granted(receipt, self.core.access(event), "mail.read",
                    ("mail-account:" + account["id"],)):
                raise PermissionError("mail rule only authorized its triggering message")

    def _send_authority(self, identity, draft):
        content = draft["content"]
        recipients = [*content["to"], *content.get("cc", ()), *content.get("bcc", ())]
        resources = ("mail-account:" + draft["account_id"], *("mail-recipient:" + value for value in recipients))
        event, context, root, scope_id, user_turn = self._claim(identity, "mail.send", effect="write", resources=resources)
        if not user_turn and draft["scope_id"] != scope_id:
            raise PermissionError("outgoing mail belongs to another user request or task")
        self.authorize_context(self.core.access(event), context, "mail.send", resources)
        if not user_turn and not context.get("mail_receipt_id"):
            intent = json.loads(root["context_json"]).get("dispatch_spec", {}) if root else {}
            if "mail.send" not in intent.get("requested_actions", ()):
                raise PermissionError("this task has no recorded user authorization to send mail")
        return event, context

    @staticmethod
    def _arguments(payload):
        if not isinstance(payload, dict) or set(payload) - {"operation", "arguments"}:
            raise ValueError("invalid mail operation")
        operation, arguments = payload.get("operation"), payload.get("arguments", {})
        if not isinstance(operation, str) or not isinstance(arguments, dict):
            raise TypeError("mail operation and argument object are required")
        return operation, arguments

    def _invoke(self, identity, payload):
        operation, arguments = self._arguments(payload)
        account_operation = operation.startswith("accounts.")
        write = operation in {"accounts.create", "accounts.update", "accounts.disable", "accounts.delete", "send.prepare", "send.send", "send.retry"}
        action = ("mail.accounts.write" if write else "mail.accounts.read") if account_operation else {
            "input": "mail.input.read", "send.prepare": "mail.send.prepare", "send.send": "mail.send",
            "send.retry": "mail.send",
            "send.status": "mail.status"}.get(operation, "mail.read")
        resources = ("mail-account:" + str(arguments.get("account_id", "scope")),)
        event, context, _root, scope_id, user_turn = self._claim(identity, action,
            effect="write" if write else "read", resources=resources)
        workspace_id = context.get("workspace_id", "personal")
        if account_operation:
            if write and not user_turn:
                raise PermissionError("mail account changes require this real user's Leader turn")
            return self.accounts.invoke(event, workspace_id, operation, arguments)
        if operation == "input":
            if arguments:
                raise ValueError("mail.input takes its receipt from the host-bound task")
            return self.triggers.input(event, context)
        if operation == "send.prepare":
            supplied = dict(arguments)
            account_id = supplied.pop("account_id")
            account = self.accounts.resolve(event, workspace_id, account_id, require_ready=True)
            if supplied.get("reply"):
                reply = supplied["reply"]
                self._message_scope(event, context, account, reply.get("folder", "INBOX"),
                    reply.get("uid"), reply.get("uidvalidity"))
            canonical = json.dumps({"account_id": account["id"], "account_revision": account["revision"],
                "arguments": supplied}, sort_keys=True, ensure_ascii=False)
            key = hashlib.sha256(canonical.encode()).hexdigest()
            result = self.sender.prepare(event, workspace_id, account_id, supplied,
                idempotency_key=key, scope_id=scope_id)
            event, context, _root, _scope_id, _user_turn = self._claim(identity, action,
                effect="write", resources=("mail-account:" + account["id"],))
            current = self.accounts.resolve(event, workspace_id, account["id"], require_ready=True)
            if current["revision"] != account["revision"]:
                raise PermissionError("mail account changed while the outgoing draft was prepared")
            if supplied.get("reply"):
                self._message_scope(event, context, current, reply.get("folder", "INBOX"),
                    reply.get("uid"), reply.get("uidvalidity"))
            return result
        if operation in {"send.send", "send.status", "send.retry"}:
            if set(arguments) - {"draft_id", "expected_digest", "account_id"}:
                raise ValueError("unexpected outgoing mail arguments")
            draft = self.sender.get(event, workspace_id, arguments["draft_id"])
            if operation == "send.status":
                if not user_turn and draft["scope_id"] != scope_id:
                    raise PermissionError("outgoing mail belongs to another task")
                return draft
            if arguments.get("account_id") != draft["account_id"]:
                raise PermissionError("outgoing account does not match the frozen draft")
            self._send_authority(identity, draft)
            if operation == "send.retry":
                result = self.sender.retry(event, workspace_id, draft["id"], expected_digest=arguments["expected_digest"],
                    authorize=lambda: self._send_authority(identity, draft))
                return {**result, "retry_prepared": True}
            return self.sender.send(event, workspace_id, draft["id"], expected_digest=arguments["expected_digest"],
                authorize=lambda: self._send_authority(identity, draft))
        fields = {"messages.list": {"account_id", "folder", "after_uid", "limit"},
            "messages.search": {"account_id", "folder", "after_uid", "limit", "query"},
            "messages.get": {"account_id", "folder", "uid", "uidvalidity"},
            "messages.attachment": {"account_id", "folder", "uid", "uidvalidity", "part"},
            "folders": {"account_id"}}
        if operation not in fields or set(arguments) - fields[operation]:
            raise ValueError("unexpected mail operation or arguments")
        account = self.accounts.resolve(event, workspace_id, arguments["account_id"], require_ready=True)
        self._claim(identity, action, resources=("mail-account:" + account["id"],))
        folder = arguments.get("folder", "INBOX")
        uid, epoch = arguments.get("uid"), arguments.get("uidvalidity")
        self._message_scope(event, context, account, folder, uid, epoch)
        if operation == "folders":
            result = self.accounts.folders(account)
            self._read_authority(identity, account, folder, uid, epoch, action)
            return result
        if operation == "messages.attachment":
            result = self.accounts.attachment(account, folder, uid, arguments["part"], epoch)
            data = result.pop("data")
            self._read_authority(identity, account, folder, uid, epoch, action)
            return {**result, "data_base64": base64.b64encode(data).decode(),
                "size": len(data), "sha256": hashlib.sha256(data).hexdigest()}
        if operation == "messages.get" and (type(uid) is not int or uid < 1 or type(epoch) is not int or epoch < 1):
            raise ValueError("a message UID and UIDVALIDITY are required")
        page = self.accounts.messages(account, folder, **{key: arguments[key]
            for key in ("after_uid", "limit", "query", "uid") if key in arguments})
        if epoch is not None and page["uidvalidity"] != epoch:
            raise ValueError("mailbox UIDVALIDITY changed; list messages again")
        self._read_authority(identity, account, folder, uid, epoch, action)
        return {"source": "untrusted_email", "account_id": account["id"], "folder": folder, **page}

    def _read_authority(self, identity, account, folder, uid, epoch, action):
        event, context, _root, _scope_id, _user_turn = self._claim(identity, action,
            resources=("mail-account:" + account["id"],))
        current = self.accounts.resolve(event, context.get("workspace_id", "personal"), account["id"], require_ready=True)
        if current["revision"] != account["revision"]:
            raise PermissionError("mail account changed during this read")
        self._message_scope(event, context, current, folder, uid, epoch)

    async def invoke(self, identity, payload):
        return await asyncio.to_thread(self._invoke, identity, payload)
