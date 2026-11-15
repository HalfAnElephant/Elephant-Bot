"""Incremental IMAP polling joins the existing durable schedule/task transaction."""
import asyncio
import hashlib
import json
from datetime import UTC

from sqlalchemy import insert, select, update

from qqbot.domain import InboundEvent, stable_id
from qqbot.domain.messages import TextSegment
from qqbot.storage.agent_schema import dialogues
from qqbot.storage.mail_trigger_schema import mail_received, mail_rule_receipts, mail_rule_state
from qqbot.storage.schedule_schema import schedules


class MailTriggerChanged(PermissionError):
    pass


class MailTriggers:
    def __init__(self, core, accounts):
        self.core, self.accounts, self.db = core, accounts, core.db

    def _account(self, event, workspace_id, account_id, *, conn=None):
        account = self.accounts.resolve(event, workspace_id, account_id, require_ready=True, conn=conn)
        self.core.permissions.require(self.core.access(event), "mail.read", "mail-account:" + account["id"])
        return account

    def prepare_activation(self, event, workspace_id, spec, *, replay_from_uid=None):
        account = self._account(event, workspace_id, spec.mail.account_id)
        snapshot = self.accounts.mailbox(account, spec.mail.folder)
        if (type(snapshot.get("uidvalidity")) is not int or snapshot["uidvalidity"] < 1
                or type(snapshot.get("highest_uid")) is not int or snapshot["highest_uid"] < 0):
            raise ValueError("mailbox did not provide a valid UID epoch and watermark")
        return {"account_id": account["id"], "account_revision": account["revision"],
            "mailbox_generation": account.get("mailbox_generation", 1), "workspace_id": workspace_id,
            "replay_from_uid": replay_from_uid, **snapshot}

    def activate(self, conn, schedule_id, event, spec, activation):
        account = self._account(event, spec.workspace_id, spec.mail.account_id, conn=conn)
        if (activation is None or account["id"] != activation["account_id"]
                or account["revision"] != activation["account_revision"]
                or spec.workspace_id != activation["workspace_id"]):
            raise ValueError("mail account changed during activation; retry with its current configuration")
        previous = conn.execute(select(mail_rule_state).where(
            mail_rule_state.c.schedule_id == schedule_id)).mappings().first()
        replay = activation["replay_from_uid"]
        baseline = replay - 1 if replay is not None else activation["highest_uid"]
        if (previous and previous["uidvalidity"] == activation["uidvalidity"]
                and previous["mailbox_generation"] == activation["mailbox_generation"]
                and replay is None):
            baseline = max(baseline, previous["last_uid"])
        values = {"account_id": account["id"], "account_revision": account["revision"],
            "mailbox_generation": activation["mailbox_generation"],
            "uidvalidity": activation["uidvalidity"], "last_uid": baseline, "activation_uid": baseline,
            "generation": previous["generation"] + 1 if previous else 1}
        if previous:
            conn.execute(update(mail_rule_state).where(mail_rule_state.c.schedule_id == schedule_id).values(**values))
        else:
            conn.execute(insert(mail_rule_state).values(schedule_id=schedule_id, **values))

    def receipt(self, conn, event, context):
        from qqbot.runtime.schedules import ScheduleSpec

        row = conn.execute(select(mail_rule_receipts,
            schedules.c.owner_id, schedules.c.principal, schedules.c.conversation, schedules.c.spec_json,
            schedules.c.enabled, schedules.c.deleted_at,
            mail_rule_state.c.account_id, mail_rule_state.c.account_revision,
            mail_rule_state.c.generation.label("active_generation"),
            mail_received.c.folder, mail_received.c.uidvalidity, mail_received.c.uid,
            mail_received.c.message_json, mail_received.c.digest).join(schedules,
            schedules.c.id == mail_rule_receipts.c.schedule_id).join(mail_rule_state,
            mail_rule_state.c.schedule_id == schedules.c.id).join(mail_received,
            mail_received.c.id == mail_rule_receipts.c.message_id).where(
            mail_rule_receipts.c.id == context.get("mail_receipt_id"))).mappings().first()
        if (row is None or not row["enabled"] or row["deleted_at"]
                or row["generation"] != row["active_generation"]
                or row["owner_id"] != self.core.sessions.owner(event)
                or row["principal"] != event.principal.key or row["conversation"] != event.conversation.key):
            raise PermissionError("mail trigger authority has been revoked")
        spec = ScheduleSpec.model_validate_json(row["spec_json"])
        if spec.mail is None or spec.workspace_id != context.get("workspace_id", "personal"):
            raise PermissionError("mail trigger project changed")
        account = self._account(event, spec.workspace_id, row["account_id"], conn=conn)
        if account["revision"] != row["account_revision"]:
            raise PermissionError("mail account changed after this trigger was activated")
        if hashlib.sha256(row["message_json"].encode()).hexdigest() != row["digest"]:
            raise PermissionError("mail input receipt changed")
        return row

    def authorized(self, conn, event, context):
        try:
            self.receipt(conn, event, context)
            return True
        except (PermissionError, LookupError, ValueError):
            return False

    def input(self, event, context):
        with self.db.engine.connect() as conn:
            row = self.receipt(conn, event, context)
            return {"source": "untrusted_email", "receipt_id": row["id"],
                "account_id": row["account_id"], "folder": row["folder"],
                "uidvalidity": row["uidvalidity"], "uid": row["uid"],
                "message": json.loads(row["message_json"])}

    def _error(self, schedule_id, revision, code, *, pause=False):
        values = {"last_error": code, "updated_at": self.core.clock.now().isoformat()}
        if pause:
            values["enabled"] = 0
        with self.db.transaction() as conn:
            conn.execute(update(schedules).where(schedules.c.id == schedule_id,
                schedules.c.revision == revision, schedules.c.deleted_at.is_(None)).values(**values))

    def _current(self, conn, row, event, spec):
        if self.core.stopping:
            return None
        current = conn.execute(select(schedules).where(schedules.c.id == row["id"])).mappings().one()
        if (current["revision"] != row["revision"] or not current["enabled"] or current["deleted_at"]):
            return None
        if self.core.schedules._identity(event, write=True) != row["owner_id"]:
            raise MailTriggerChanged("mail_owner_changed")
        state = conn.execute(select(mail_rule_state).where(
            mail_rule_state.c.schedule_id == row["id"])).mappings().first()
        if state is None:
            raise MailTriggerChanged("mail_activation_required")
        account = self._account(event, spec.workspace_id, state["account_id"], conn=conn)
        if account["revision"] != state["account_revision"]:
            raise MailTriggerChanged("mail_account_changed")
        self.core.schedules._validate_sources(conn, event, json.loads(row["source_context_json"]))
        return state

    def _scan(self, row):
        from qqbot.runtime.schedules import ScheduleSpec

        spec = ScheduleSpec.model_validate_json(row["spec_json"])
        if spec.mail is None:
            return
        event = InboundEvent.model_validate_json(row["event_json"])
        with self.db.engine.connect() as conn:
            state = self._current(conn, row, event, spec)
            if state is None:
                return
            account = self._account(event, spec.workspace_id, state["account_id"], conn=conn)
        page = self.accounts.messages(account, spec.mail.folder, after_uid=state["last_uid"], limit=50)
        if page["uidvalidity"] != state["uidvalidity"]:
            raise MailTriggerChanged("mail_uidvalidity_changed")
        messages = sorted(page["messages"], key=lambda message: message["uid"])
        scanned = page["scanned_through"]
        if type(scanned) is not int or not state["last_uid"] <= scanned <= max(state["last_uid"], page["highest_uid"]):
            raise ValueError("mailbox returned an invalid scan cursor")
        for message in messages:
            uid = message["uid"]
            if type(uid) is not int or uid <= state["last_uid"]:
                continue
            if uid > scanned:
                raise ValueError("mailbox message exceeds its scan cursor")
            encoded = json.dumps(message, ensure_ascii=False, sort_keys=True)
            digest = hashlib.sha256(encoded.encode()).hexdigest()
            message_id = stable_id("mail-input", state["account_id"], str(state["mailbox_generation"]),
                spec.mail.folder, str(page["uidvalidity"]), str(uid))
            fire_id = stable_id("mail-fire", row["id"], str(state["mailbox_generation"]), str(page["uidvalidity"]), str(uid))
            now = self.core.clock.now().astimezone(UTC)
            with self.db.transaction() as conn:
                current = self._current(conn, row, event, spec)
                if current is None or current["generation"] != state["generation"]:
                    return
                if uid <= current["last_uid"]:
                    continue
                old_message = conn.execute(select(mail_received.c.digest).where(
                    mail_received.c.id == message_id)).scalar_one_or_none()
                if old_message is not None and old_message != digest:
                    raise MailTriggerChanged("mail_input_changed")
                if old_message is None:
                    conn.execute(insert(mail_received).values(id=message_id, account_id=state["account_id"],
                        mailbox_generation=state["mailbox_generation"],
                        folder=spec.mail.folder, uidvalidity=page["uidvalidity"], uid=uid,
                        message_json=encoded, digest=digest, created_at=now.isoformat()))
                conn.execute(update(mail_rule_state).where(mail_rule_state.c.schedule_id == row["id"]).values(last_uid=uid))
                if not spec.mail.matches(sender_address=message["sender_address"], subject=message["subject"],
                        body=message["body"], body_complete=message.get("body_complete", True)):
                    if not message.get("body_complete", True):
                        conn.execute(update(schedules).where(schedules.c.id == row["id"]).values(
                            last_error="mail_body_incomplete", updated_at=now.isoformat()))
                    continue
                if conn.execute(select(mail_rule_receipts.c.id).where(mail_rule_receipts.c.id == fire_id)).first():
                    continue
                dialogue = conn.execute(select(dialogues).where(dialogues.c.id == row["dialogue_id"],
                    dialogues.c.owner_id == row["owner_id"])).mappings().one()
                trigger = event.model_copy(update={"event_id": fire_id, "platform_message_id": fire_id,
                    "occurred_at": now, "received_at": now, "segments": (TextSegment(text=spec.goal),),
                    "reply_to": None, "reply_context": None})
                task_id = self.core.schedules._submit_fire(conn, dialogue, event, trigger, spec, row,
                    spec.workspace_id, fire_id, mail_receipt_id=fire_id)
                conn.execute(insert(mail_rule_receipts).values(id=fire_id, schedule_id=row["id"],
                    message_id=message_id, schedule_revision=row["revision"], generation=current["generation"],
                    permissions_json=json.dumps([rule.model_dump(mode="json") for rule in spec.mail_permissions]),
                    task_id=task_id, created_at=now.isoformat()))
                conn.execute(update(schedules).where(schedules.c.id == row["id"]).values(
                    last_fire_at=now.isoformat(), last_task_id=task_id, last_error=None, updated_at=now.isoformat()))
        with self.db.transaction() as conn:
            current = self._current(conn, row, event, spec)
            if current and current["generation"] == state["generation"] and current["last_uid"] < scanned:
                conn.execute(update(mail_rule_state).where(mail_rule_state.c.schedule_id == row["id"]).values(last_uid=scanned))

    def once(self):
        if self.core.stopping:
            return True
        with self.db.engine.connect() as conn:
            rows = conn.execute(select(schedules).where(schedules.c.enabled == 1,
                schedules.c.deleted_at.is_(None), schedules.c.next_fire_at.is_(None))).mappings().all()
        for row in rows:
            try:
                self._scan(row)
            except MailTriggerChanged as error:
                self._error(row["id"], row["revision"], str(error), pause=True)
            except (PermissionError, LookupError):
                self._error(row["id"], row["revision"], "mail_authorization_revoked", pause=True)
            except (OSError, ValueError, TimeoutError):
                self._error(row["id"], row["revision"], "mail_poll_failed")
        return True

    async def step(self):
        return await asyncio.to_thread(self.once)
