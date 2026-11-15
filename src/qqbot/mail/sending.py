"""Freeze outgoing messages before SMTP and retain ambiguous sends for reconciliation."""
import base64
import hashlib
import json
import re
import smtplib
import ssl
from email.errors import HeaderParseError
from email.headerregistry import Address
from email.message import EmailMessage
from email.policy import SMTP
from email.utils import format_datetime
from uuid import uuid4

from sqlalchemy import func, insert, or_, select, update

from qqbot.storage.mail_send_schema import mail_outgoing

MAX_ATTACHMENT_BYTES = 10 * 1024 * 1024
MAX_ATTACHMENTS_BYTES = 20 * 1024 * 1024
MAX_BODY_BYTES = 1024 * 1024
MAX_WIRE_BYTES = 30 * 1024 * 1024
MESSAGE_ID = re.compile(r"<[^\s<>]{1,250}>")


def address(value):
    if (not isinstance(value, str) or not 1 <= len(value) <= 320
            or any(ord(char) < 32 or ord(char) == 127 for char in value)):
        raise ValueError("mail address must be a single bounded addr-spec")
    try:
        parsed = Address(addr_spec=value)
        if not parsed.username or not parsed.domain or not parsed.username.isascii():
            raise ValueError
        domain = parsed.domain.encode("idna").decode().lower()
        if any(char in domain for char in "<>[]:/\\"):
            raise ValueError
        return str(Address(username=parsed.username, domain=domain))
    except (ValueError, IndexError, UnicodeError, HeaderParseError):
        raise ValueError("mail address must be a valid ASCII-local addr-spec") from None


def addresses(value):
    if not isinstance(value, (list, tuple)) or len(value) > 100:
        raise ValueError("mail recipients must be a list of at most 100 addresses")
    return list(dict.fromkeys(address(item) for item in value))


class SMTPTransport:
    """One TLS SMTP transaction, with a final authority check immediately before DATA."""

    def send(self, spec, secret, wire, sender, recipients, authorize):
        client = None
        data_started = False
        accepted, rejected = [], {}
        try:
            authorize()
            context = ssl.create_default_context()
            if spec.smtp_security == "ssl":
                client = smtplib.SMTP_SSL(spec.smtp_host, spec.smtp_port, timeout=30, context=context)
            elif spec.smtp_security == "starttls":
                client = smtplib.SMTP(spec.smtp_host, spec.smtp_port, timeout=30)
                code, _ = client.ehlo()
                if code != 250:
                    raise smtplib.SMTPHeloError(code, b"EHLO rejected")
                client.starttls(context=context)
            else:
                raise ValueError("SMTP requires SSL or STARTTLS")
            code, _ = client.ehlo()
            if code != 250:
                raise smtplib.SMTPHeloError(code, b"EHLO rejected")
            authorize()
            client.login(spec.username, secret)
            authorize()
            code, _ = client.mail(sender)
            if code != 250:
                return {"state": "FAILED", "error": "smtp_sender_rejected", "smtp_code": code}
            for recipient in recipients:
                code, _ = client.rcpt(recipient)
                if code in {250, 251}:
                    accepted.append(recipient)
                else:
                    rejected[recipient] = code
            if not accepted:
                return {"state": "FAILED", "error": "smtp_recipients_rejected",
                        "rejected_recipients": rejected}
            authorize()
            data_started = True
            code, _ = client.data(wire)
            if code != 250:
                return {"state": "FAILED", "error": "smtp_data_rejected", "smtp_code": code,
                        "rejected_recipients": rejected}
            return {"state": "PARTIAL_ACCEPTED" if rejected else "ACCEPTED", "smtp_code": code,
                    "accepted_recipients": accepted, "rejected_recipients": rejected}
        except PermissionError:
            raise
        except smtplib.SMTPDataError as error:
            return {"state": "FAILED", "error": "smtp_data_rejected", "smtp_code": error.smtp_code,
                    "rejected_recipients": rejected}
        except (OSError, smtplib.SMTPException, UnicodeError):
            return {"state": "UNKNOWN" if data_started else "FAILED",
                    "error": "smtp_outcome_unknown" if data_started else "smtp_connection_or_authentication_failed",
                    "rejected_recipients": rejected}
        finally:
            if client is not None:
                try:
                    client.close()
                except OSError:
                    pass


class MailSender:
    def __init__(self, core, accounts, transport=None):
        self.core, self.db, self.clock = core, core.db, core.clock
        self.accounts = accounts
        self.transport = transport or SMTPTransport()

    def _identity(self, event, workspace_id, action, account_id):
        owner = self.core.sessions.owner(event)
        if owner is None or not self.core.allowed(event):
            raise PermissionError("mail requires an authenticated active owner")
        self.core.sessions.validate_workspace(event, workspace_id)
        self.core.permissions.require(self.core.access(event), action, "mail-account:" + account_id)
        return owner

    def _owned(self, conn, event, workspace_id, draft_id, *, action="mail.status", administrative=False):
        row = conn.execute(select(mail_outgoing).where(mail_outgoing.c.id == draft_id)).mappings().first()
        if row is None:
            raise LookupError("outgoing mail unavailable")
        admin = administrative and event.principal.identity_namespace == "admin_api"
        owner = self._identity(event, "personal" if admin else workspace_id, action, row["account_id"])
        if owner != row["owner_id"]:
            raise PermissionError("outgoing mail belongs to another owner")
        if not admin and (row["principal"] != event.principal.key
                or row["conversation"] != event.conversation.key or row["workspace_id"] != workspace_id):
            raise PermissionError("outgoing mail belongs to another authenticated scope")
        return row

    @staticmethod
    def _view(row):
        content = json.loads(row["content_json"])
        return {"id": row["id"], "account_id": row["account_id"], "scope_id": row["scope_id"],
                "account_revision": row["account_revision"],
                "transport_snapshot": json.loads(row["transport_snapshot"]),
                "state": row["state"], "digest": row["content_hash"], "message_id": row["message_id"],
                "content": content, "result": json.loads(row["result_json"]),
                "delivery_status": json.loads(row["result_json"]).get("delivery_status", "unconfirmed"),
                "created_at": row["created_at"], "updated_at": row["updated_at"]}

    def get(self, event, workspace_id, draft_id, *, administrative=False):
        with self.db.engine.connect() as conn:
            return self._view(self._owned(conn, event, workspace_id, draft_id, administrative=administrative))

    @classmethod
    def _retry(cls, previous, account, request_hash):
        if previous["account_id"] != account["id"] or previous["request_hash"] != request_hash:
            raise ValueError("mail creation key reused with different content or account")
        if previous["state"] == "DRAFT" and (previous["account_revision"] != account["revision"]
                or previous["credential_ref"] != account["credential_ref"]):
            raise ValueError("mail account changed; prepare a new draft with a new creation key")
        return cls._view(previous)

    def prepare(self, event, workspace_id, account_id, arguments, *, idempotency_key, scope_id):
        if (not isinstance(arguments, dict)
                or set(arguments) - {"to", "cc", "bcc", "subject", "body", "attachments", "reply"}):
            raise ValueError("unexpected outgoing mail arguments")
        for value in (idempotency_key, scope_id):
            if not isinstance(value, str) or not 1 <= len(value) <= 256:
                raise ValueError("outgoing mail requires bounded idempotency and source scope keys")
        account = self.accounts.resolve(event, workspace_id, account_id, require_ready=True)
        owner = self._identity(event, workspace_id, "mail.send", account["id"])
        request_hash = hashlib.sha256(json.dumps(arguments, sort_keys=True,
            separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
        scope = (mail_outgoing.c.owner_id == owner, mail_outgoing.c.principal == event.principal.key,
                 mail_outgoing.c.conversation == event.conversation.key,
                 mail_outgoing.c.workspace_id == workspace_id, mail_outgoing.c.scope_id == scope_id)
        with self.db.engine.connect() as conn:
            previous = conn.execute(select(mail_outgoing).where(*scope,
                mail_outgoing.c.idempotency_key == idempotency_key)).mappings().first()
            if previous:
                if arguments.get("reply") is not None:
                    self._identity(event, workspace_id, "mail.read", account["id"])
                return self._retry(previous, account, request_hash)
        spec = self.accounts.spec(account)
        to, cc, bcc = (addresses(arguments.get(name, [])) for name in ("to", "cc", "bcc"))
        subject, body = arguments.get("subject", ""), arguments.get("body", "")
        if (not isinstance(subject, str) or len(subject) > 1000
                or any(ord(char) < 32 or ord(char) == 127 for char in subject)):
            raise ValueError("mail subject must be bounded text without control characters")
        if not isinstance(body, str) or len(body.encode()) > MAX_BODY_BYTES or "\x00" in body:
            raise ValueError("mail body must be bounded UTF-8 text without NUL")
        reply = arguments.get("reply")
        thread = {}
        if reply is not None:
            if (not isinstance(reply, dict) or set(reply) != {"folder", "uid", "uidvalidity"}
                    or type(reply["uid"]) is not int or reply["uid"] < 1
                    or type(reply["uidvalidity"]) is not int or reply["uidvalidity"] < 1
                    or not isinstance(reply["folder"], str) or not 1 <= len(reply["folder"]) <= 256
                    or any(ord(char) < 32 for char in reply["folder"])):
                raise ValueError("reply requires a valid folder, UID and UIDVALIDITY")
            self._identity(event, workspace_id, "mail.read", account["id"])
            result = self.accounts.messages(account, reply["folder"], uid=reply["uid"])
            self._identity(event, workspace_id, "mail.read", account["id"])
            originals = [item for item in result["messages"] if item["uid"] == reply["uid"]]
            if result["uidvalidity"] != reply["uidvalidity"] or len(originals) != 1:
                raise ValueError("reply source is no longer available in this mailbox epoch")
            original = originals[0]
            message_id = original.get("message_id", "")
            if not isinstance(message_id, str) or not MESSAGE_ID.fullmatch(message_id):
                raise ValueError("reply source has no valid Message-ID")
            if not to:
                reply_to = original.get("reply_to") or [original["sender_address"]]
                to = addresses([reply_to] if isinstance(reply_to, str) else reply_to)
            if not subject:
                original_subject = " ".join(str(original.get("subject", "")).splitlines())[:990]
                subject = original_subject if original_subject.lower().startswith("re:") else "Re: " + original_subject
            references = original.get("references", [])
            if isinstance(references, str):
                references = references.split()
            if (not isinstance(references, (list, tuple)) or len(references) > 100
                    or any(not isinstance(item, str) or not MESSAGE_ID.fullmatch(item) for item in references)):
                raise ValueError("reply source has invalid References")
            thread = {"in_reply_to": message_id, "references": list(dict.fromkeys([*references, message_id]))}
        recipients = list(dict.fromkeys([*to, *cc, *bcc]))
        if not recipients or len(recipients) > 100:
            raise ValueError("mail requires between 1 and 100 unique recipients")
        attachments = arguments.get("attachments", [])
        if not isinstance(attachments, (list, tuple)) or len(attachments) > 20:
            raise ValueError("mail accepts at most 20 attachments")
        files, file_data, total = [], [], 0
        for attachment in attachments:
            if (not isinstance(attachment, dict)
                    or set(attachment) != {"filename", "mime_type", "data_base64"}):
                raise ValueError("mail attachment requires filename, MIME type and base64 data")
            filename, mime_type, encoded = (attachment[name] for name in ("filename", "mime_type", "data_base64"))
            if (not isinstance(filename, str) or not 1 <= len(filename) <= 255 or filename in {".", ".."}
                    or any(ord(char) < 32 or ord(char) == 127 or char in "/\\" for char in filename)):
                raise ValueError("mail attachment filename must be a bounded basename")
            if not isinstance(mime_type, str) or not re.fullmatch(r"[A-Za-z0-9!#$&^_.+-]+/[A-Za-z0-9!#$&^_.+-]+", mime_type):
                raise ValueError("invalid attachment MIME type")
            if not isinstance(encoded, str) or len(encoded) > (MAX_ATTACHMENT_BYTES + 2) // 3 * 4:
                raise ValueError("mail attachment exceeds 10 MiB")
            try:
                data = base64.b64decode(encoded, validate=True)
            except ValueError:
                raise ValueError("mail attachment is not valid base64") from None
            total += len(data)
            if len(data) > MAX_ATTACHMENT_BYTES or total > MAX_ATTACHMENTS_BYTES:
                raise ValueError("mail attachments exceed the size limit")
            files.append({"filename": filename, "mime_type": mime_type.lower(), "size": len(data),
                          "sha256": hashlib.sha256(data).hexdigest()})
            file_data.append(data)
        content = {"from": address(spec.address), "to": to, "cc": cc, "bcc": bcc, "subject": subject,
                   "body": body, "attachments": files, "reply": reply, **thread}
        encoded = json.dumps(content, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        now = self.clock.now()
        with self.db.transaction() as conn:
            current = self.accounts.resolve(event, workspace_id, account["id"], require_ready=True, conn=conn)
            if current["revision"] != account["revision"] or current["credential_ref"] != account["credential_ref"]:
                raise PermissionError("mail account changed while preparing the draft")
            previous = conn.execute(select(mail_outgoing).where(*scope,
                mail_outgoing.c.idempotency_key == idempotency_key)).mappings().first()
            if previous:
                return self._retry(previous, account, request_hash)
            duplicate = conn.execute(select(mail_outgoing).where(*scope, mail_outgoing.c.account_id == account["id"],
                mail_outgoing.c.content_hash == digest, mail_outgoing.c.state != "FAILED",
                or_(mail_outgoing.c.state != "DRAFT", (mail_outgoing.c.account_revision == account["revision"])
                    & (mail_outgoing.c.credential_ref == account["credential_ref"])))
                .order_by(mail_outgoing.c.created_at).limit(1)).mappings().first()
            if duplicate:
                return self._view(duplicate)
            draft_id = uuid4().hex
            message_id = "<" + draft_id + "@" + address(spec.address).rsplit("@", 1)[1] + ">"
            message = EmailMessage(policy=SMTP)
            message["From"], message["Subject"] = content["from"], subject
            if to:
                message["To"] = ", ".join(to)
            if cc:
                message["Cc"] = ", ".join(cc)
            message["Date"], message["Message-ID"] = format_datetime(now), message_id
            if thread:
                message["In-Reply-To"] = thread["in_reply_to"]
                message["References"] = " ".join(thread["references"])
            message.set_content(body, cte="quoted-printable")
            for metadata, data in zip(files, file_data, strict=True):
                maintype, subtype = metadata["mime_type"].split("/", 1)
                message.add_attachment(data, maintype=maintype, subtype=subtype, filename=metadata["filename"])
            wire = message.as_bytes()
            if len(wire) > MAX_WIRE_BYTES:
                raise ValueError("encoded outgoing mail exceeds 30 MiB")
            conn.execute(insert(mail_outgoing).values(id=draft_id, owner_id=owner, principal=event.principal.key,
                conversation=event.conversation.key, workspace_id=workspace_id, account_id=account["id"],
                account_revision=account["revision"], transport_snapshot=json.dumps({
                    "smtp_host": spec.smtp_host, "smtp_port": spec.smtp_port, "smtp_security": spec.smtp_security,
                    "username": spec.username, "sender_address": content["from"]}, ensure_ascii=False),
                credential_ref=account["credential_ref"], scope_id=scope_id,
                idempotency_key=idempotency_key, request_hash=request_hash,
                content_hash=digest, content_json=encoded, wire=wire,
                message_id=message_id, state="DRAFT", result_json="{}",
                created_at=now.isoformat(), updated_at=now.isoformat()))
            return self._view(conn.execute(select(mail_outgoing).where(mail_outgoing.c.id == draft_id)).mappings().one())

    def _current_account(self, event, workspace_id, row, *, conn=None):
        account = self.accounts.resolve(event, workspace_id, row["account_id"], require_ready=True, conn=conn)
        if account["revision"] != row["account_revision"] or account["credential_ref"] != row["credential_ref"]:
            raise PermissionError("mail account changed after this draft was frozen")
        return account

    def retry(self, event, workspace_id, draft_id, *, expected_digest, authorize):
        with self.db.transaction() as conn:
            row = self._owned(conn, event, workspace_id, draft_id, action="mail.send")
            if expected_digest != row["content_hash"]:
                raise ValueError("outgoing mail digest differs from the frozen draft")
            if row["state"] != "FAILED":
                raise ValueError("only a confirmed FAILED outgoing mail can be retried")
            self._current_account(event, workspace_id, row, conn=conn)
            authorize()
            previous = json.loads(row["result_json"])
            previous_result = {key: value for key, value in previous.items()
                if key not in {"retry_count", "previous_result"}}
            previous_result["state"] = row["state"]
            result = {"retry_count": previous.get("retry_count", 0) + 1,
                "previous_result": previous_result, "delivery_status": "not_sent"}
            changed = conn.execute(update(mail_outgoing).where(mail_outgoing.c.id == draft_id,
                mail_outgoing.c.state == "FAILED").values(state="DRAFT", result_json=json.dumps(result),
                    updated_at=self.clock.now().isoformat())).rowcount
            if not changed:
                raise ValueError("outgoing mail state changed before retry preparation")
            return self._view(conn.execute(select(mail_outgoing).where(mail_outgoing.c.id == draft_id)).mappings().one())

    def send(self, event, workspace_id, draft_id, *, expected_digest, authorize):
        with self.db.transaction() as conn:
            row = self._owned(conn, event, workspace_id, draft_id, action="mail.send")
            if expected_digest != row["content_hash"]:
                raise ValueError("outgoing mail digest differs from the frozen draft")
            if row["state"] != "DRAFT":
                return self._view(row)
            pending = conn.execute(select(mail_outgoing.c.id).where(
                mail_outgoing.c.owner_id == row["owner_id"], mail_outgoing.c.principal == row["principal"],
                mail_outgoing.c.conversation == row["conversation"], mail_outgoing.c.workspace_id == workspace_id,
                mail_outgoing.c.account_id == row["account_id"], mail_outgoing.c.scope_id == row["scope_id"],
                mail_outgoing.c.state.in_({"UNKNOWN", "DISPATCHING"})).limit(1)).scalar_one_or_none()
            if pending:
                raise ValueError("this task has an unresolved SMTP outcome; inspect outgoing draft " + pending)
            account = self._current_account(event, workspace_id, row, conn=conn)
            spec, secret = self.accounts.spec(account), self.accounts.credential(account)
            authorize()
            conn.execute(update(mail_outgoing).where(mail_outgoing.c.id == draft_id,
                mail_outgoing.c.state == "DRAFT").values(state="DISPATCHING", updated_at=self.clock.now().isoformat()))
            row = dict(row)

        def check():
            try:
                self._identity(event, workspace_id, "mail.send", row["account_id"])
                self._current_account(event, workspace_id, row)
            except (LookupError, ValueError):
                raise PermissionError("mail account authorization is no longer available") from None
            authorize()

        try:
            check()
            content = json.loads(row["content_json"])
            recipients = list(dict.fromkeys([*content["to"], *content["cc"], *content["bcc"]]))
            result = self.transport.send(spec, secret, row["wire"], content["from"], recipients, check)
            state = result.get("state")
            if state not in {"ACCEPTED", "PARTIAL_ACCEPTED", "FAILED", "UNKNOWN"}:
                raise ValueError("invalid SMTP receipt state")
        except PermissionError:
            self._finish(draft_id, "FAILED", {"error": "authorization_revoked_before_data"})
            raise
        except BaseException as error:
            self._finish(draft_id, "UNKNOWN", {"error": "smtp_outcome_unknown"})
            if not isinstance(error, Exception):
                raise
        else:
            self._finish(draft_id, state, result)
        return self.get(event, workspace_id, draft_id)

    def _finish(self, draft_id, state, result):
        result = {**result, "delivery_status": "unconfirmed"}
        with self.db.transaction() as conn:
            previous = json.loads(conn.execute(select(mail_outgoing.c.result_json).where(
                mail_outgoing.c.id == draft_id)).scalar_one())
            result = {**{key: previous[key] for key in ("retry_count", "previous_result") if key in previous}, **result}
            conn.execute(update(mail_outgoing).where(mail_outgoing.c.id == draft_id,
                mail_outgoing.c.state.in_({"DISPATCHING", "UNKNOWN"})).values(state=state,
                result_json=json.dumps(result, ensure_ascii=False), updated_at=self.clock.now().isoformat()))

    def recover(self):
        with self.db.transaction() as conn:
            return conn.execute(update(mail_outgoing).where(mail_outgoing.c.state == "DISPATCHING").values(
                state="UNKNOWN", result_json=func.json_set(mail_outgoing.c.result_json,
                    '$.error', 'process_stopped_during_smtp', '$.delivery_status', 'unconfirmed'),
                updated_at=self.clock.now().isoformat())).rowcount

    def resolve_delivery(self, event, workspace_id, draft_id, *, state, evidence):
        if event.principal.identity_namespace != "admin_api":
            raise PermissionError("mail delivery reconciliation requires administrator evidence")
        if state not in {"accepted", "delivered", "bounced", "not_sent", "failed"}:
            raise ValueError("mail reconciliation requires accepted, delivered, bounced or not_sent")
        if not isinstance(evidence, str) or not 1 <= len(evidence.strip()) <= 4000:
            raise ValueError("mail reconciliation requires bounded operator evidence")
        with self.db.transaction() as conn:
            row = self._owned(conn, event, workspace_id, draft_id, action="mail.resolve", administrative=True)
            if row["state"] not in {"UNKNOWN", "ACCEPTED", "PARTIAL_ACCEPTED"}:
                raise ValueError("only uncertain or SMTP-accepted mail can be reconciled")
            if state in {"failed", "not_sent"} and row["state"] != "UNKNOWN":
                raise ValueError("recorded SMTP acceptance requires a delivery outcome, such as delivered or bounced")
            result = {**json.loads(row["result_json"]), "delivery_status": state,
                      "reconciliation_evidence": evidence, "reconciled_at": self.clock.now().isoformat()}
            smtp_state = "FAILED" if state in {"failed", "not_sent"} else row["state"]
            if smtp_state == "UNKNOWN":
                smtp_state = "ACCEPTED"
            conn.execute(update(mail_outgoing).where(mail_outgoing.c.id == draft_id).values(
                state=smtp_state, result_json=json.dumps(result, ensure_ascii=False),
                updated_at=self.clock.now().isoformat()))
            return self._view(conn.execute(select(mail_outgoing).where(mail_outgoing.c.id == draft_id)).mappings().one())
