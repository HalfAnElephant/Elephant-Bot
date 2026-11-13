"""Persist a scanned owner's official QQ connection before activating its receiver."""
import asyncio
import hashlib
import hmac
import os
import re
import time
from datetime import UTC
from pathlib import Path

import httpx

from qqbot.adapters.qq_official.credential_store import (
    load_credential,
    master_key,
    remove_credential,
    save_credential,
)
from qqbot.config import AdapterConfig, QQOwnerBinding, Settings
from qqbot.configuration import version
from qqbot.permissions import (
    PermissionPolicy,
    PermissionRule,
    PermissionSubject,
    PolicyVersionConflict,
    SubjectSelector,
)

TOKEN_URL = "https://bots.qq.com/app/getAppAccessToken"
_ADMIN = PermissionSubject(adapter_instance_id="admin_api", identity_namespace="admin_api", user_id="owner")


class ProvisioningError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _pending(session):
    if session.automatic is not True:
        raise ProvisioningError("automatic_session_required")
    if session.state != "validating" or session.provisioned:
        raise ProvisioningError("session_inactive")
    if time.monotonic() >= session.expires:
        raise ProvisioningError("session_expired")
    if (not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", session.instance_id)
            or not isinstance(session.app_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", session.app_id)
            or not isinstance(session.secret, str) or not 1 <= len(session.secret) <= 4096
            or not isinstance(session.scanner_openid, str) or not 1 <= len(session.scanner_openid) <= 256
            or any(ord(char) < 32 for char in session.secret + session.scanner_openid)):
        raise ProvisioningError("scanned_identity_invalid")


def _authorize(core, session):
    token = os.environ.get(core.settings.admin_token_env, "")
    expected = hashlib.sha256(("Bearer " + token).encode()).hexdigest()
    if not token or not hmac.compare_digest(session.owner, expected):
        raise ProvisioningError("administrator_changed")
    try:
        core.permissions.require(_ADMIN, "admin.post", "http:/api/v1/config/save")
        core.permissions.require(_ADMIN, "permission.set_policy", "permission:policy")
    except PermissionError:
        raise ProvisioningError("permission_denied") from None


def _existing(settings, session):
    existing = next((item for item in settings.adapters if item.instance_id == session.instance_id), None)
    if session.replace_app_id is not None and session.replace_app_id != session.app_id:
        raise ProvisioningError("adapter_identity_conflict")
    if existing and (existing.kind != "official" or existing.app_id != session.app_id):
        raise ProvisioningError("adapter_identity_conflict")
    if not existing and session.replace_app_id is not None:
        raise ProvisioningError("rotation_target_missing")
    if any(item.app_id == session.app_id and item.instance_id != session.instance_id for item in settings.adapters):
        raise ProvisioningError("app_already_configured")
    if any(item.app_id == session.app_id and item.user_openid != session.scanner_openid
           for item in settings.owner.qq_bindings):
        raise ProvisioningError("owner_binding_conflict")
    return existing


def _owner_policy(previous, session):
    prefix = "qq-owner:" + session.instance_id + ":"
    rule_ids = {prefix + "deny", prefix + "allow"}
    existing = {rule.id: rule for rule in previous.rules if rule.id in rule_ids}
    deny_subject = SubjectSelector(kind="adapter", adapter_instance_id=session.instance_id)
    allow_subject = SubjectSelector(kind="person", adapter_instance_id=session.instance_id,
        identity_namespace="qq_app:" + session.app_id, user_id=session.scanner_openid)
    if existing:
        if (set(existing) != rule_ids
                or existing[prefix + "deny"].subject != deny_subject
                or existing[prefix + "allow"].subject != allow_subject
                or existing[prefix + "deny"].effect != "deny" or existing[prefix + "allow"].effect != "allow"
                or any(rule.actions != ("*",) or rule.resources != ("*",) for rule in existing.values())):
            raise ProvisioningError("permission_rule_conflict")
        return previous
    retained = tuple(rule for rule in previous.rules if rule.id not in rule_ids)
    priority = max((rule.priority for rule in retained), default=0) + 1
    rules = (
        PermissionRule(id=prefix + "deny", priority=priority, effect="deny",
            subject=deny_subject),
        PermissionRule(id=prefix + "allow", priority=priority + 1, effect="allow",
            subject=allow_subject),
    )
    return previous.model_copy(update={"preset": "custom", "rules": retained + rules})


def _committed(session, candidate):
    session.provisioned, session.state = True, "connecting"
    session.result = {"saved": True, "saved_version": version(candidate), "activated": False,
        "owner_bound": True, "connection": {"connected": False, "state": "CONNECTING"}}
    session.clear()


async def provision_owner(core, session, *, client, activate) -> dict:
    _pending(session)
    with core.config_store.lock:
        _authorize(core, session)
        _existing(core.config_store.read(), session)
    try:
        master_key()
    except ValueError:
        raise ProvisioningError("credential_key_unavailable") from None
    identity = (session.instance_id, session.app_id, session.secret, session.scanner_openid,
                session.replace_app_id, session.owner)
    try:
        response = await client.post(TOKEN_URL, json={"appId": session.app_id, "clientSecret": session.secret},
                                     timeout=10, follow_redirects=False)
        if response.status_code != 200 or len(response.content) > 65536:
            raise ValueError
        token = response.json().get("access_token")
        if not isinstance(token, str) or not token or len(token) > 65536:
            raise ValueError
    except (httpx.HTTPError, ValueError, TypeError, KeyError, AttributeError):
        raise ProvisioningError("credential_validation_failed") from None
    with core.config_store.lock:
        _pending(session)
        if identity != (session.instance_id, session.app_id, session.secret, session.scanner_openid,
                        session.replace_app_id, session.owner):
            raise ProvisioningError("session_changed")
        _authorize(core, session)
        previous = core.config_store.read()
        existing = _existing(previous, session)
        snapshot = core.permissions.list()
        previous_policy = PermissionPolicy.model_validate({key: value for key, value in snapshot.items()
                                                          if key != "policy_version"})
        policy = _owner_policy(previous_policy, session)
        directory = Path(existing.credential_store_dir) if existing and existing.credential_ref else core.config_store.path.parent / "qq-credentials"
        reference = candidate = committed_policy_version = None
        stage = "credential_save_failed"
        try:
            reference = save_credential(directory, session.app_id, session.secret)
            if not hmac.compare_digest(load_credential(directory, reference, session.app_id).encode(), session.secret.encode()):
                raise ValueError("credential readback mismatch")
            updates = {"secret_env": "", "credential_ref": reference,
                "credential_store_dir": str(directory.resolve()), "enabled": True, "transport": "websocket"}
            adapter = existing.model_copy(update={**updates,
                "admin_users": tuple(dict.fromkeys((*existing.admin_users, session.scanner_openid)))}) if existing else AdapterConfig(
                    instance_id=session.instance_id, kind="official", bot_id=session.app_id, app_id=session.app_id,
                    admin_users=(session.scanner_openid,), token_url=TOKEN_URL, **updates)
            bindings = previous.owner.qq_bindings
            if not any(item.app_id == session.app_id for item in bindings):
                bindings += (QQOwnerBinding(app_id=session.app_id, user_openid=session.scanner_openid,
                    verified_at=core.clock.now().astimezone(UTC).isoformat()),)
            candidate = previous.model_copy(update={"adapters": tuple(
                adapter if item.instance_id == session.instance_id else item for item in previous.adapters)
                if existing else (*previous.adapters, adapter),
                "owner": previous.owner.model_copy(update={"qq_bindings": bindings}), "permissions": policy})
            candidate = Settings.model_validate_json(candidate.model_dump_json(), strict=False)
            adapter = next(item for item in candidate.adapters if item.instance_id == session.instance_id)
            stage = "permission_save_failed"
            committed = core.permissions.set_policy(policy, expected_version=snapshot["policy_version"], actor="admin_api:owner")
            committed_policy_version = committed["policy_version"]
            stage = "configuration_save_failed"
            if version(core.config_store.read()) != version(previous):
                raise ProvisioningError("configuration_changed")
            core.config_store.save(previous, candidate)
        except BaseException as error:
            try:
                saved = candidate is not None and core.config_store.read() == candidate
            except (OSError, ValueError):
                raise ProvisioningError("configuration_state_unknown") from None
            if saved:
                _committed(session, candidate)
                if not isinstance(error, Exception):
                    raise
            else:
                if committed_policy_version is not None:
                    try:
                        core.permissions.set_policy(previous_policy, expected_version=committed_policy_version,
                                                    actor="admin_api:owner")
                    except PolicyVersionConflict:
                        stage = "permission_rollback_conflict"
                    except Exception:  # noqa: BLE001 -- Preserve concurrent policy updates when compensation fails.
                        stage = "permission_rollback_failed"
                # A saved config-version snapshot must retain its encrypted credential for rollback.
                if reference and (candidate is None or not (core.config_store.directory / (version(candidate) + ".json")).exists()):
                    try:
                        remove_credential(directory, reference)
                    except OSError:
                        stage = "credential_cleanup_failed"
                if not isinstance(error, Exception):
                    raise
                if isinstance(error, PolicyVersionConflict):
                    raise ProvisioningError("permission_changed") from None
                if isinstance(error, ProvisioningError) and stage == "configuration_save_failed":
                    raise
                raise ProvisioningError(stage) from None
        _committed(session, candidate)
    try:
        connection = await activate(candidate, adapter)
    except asyncio.CancelledError:
        raise
    except Exception as error:  # noqa: BLE001 -- Keep committed settings recoverable and expose fixed diagnostics.
        session.result.update(activation_error=type(error).__name__, connection={"connected": False,
            "state": "FAILED", "error": "ACTIVATION_FAILED", "retry": True})
        return session.result
    safe = {key: connection[key] for key in ("connected", "state", "error", "retry") if key in connection}
    safe["connected"] = connection.get("connected") is True
    safe.setdefault("state", "CONNECTED" if safe["connected"] else "CONNECTING")
    for key in ("state", "error"):
        value = safe.get(key)
        if value is not None and (not isinstance(value, str) or not re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", value)
                or any(private in value for private in (identity[2], identity[3], token))):
            safe[key] = "ACTIVATION_STATE_INVALID"
    session.result.update(activated=safe["connected"], connection=safe)
    session.state = "connected" if safe["connected"] else "connecting"
    return session.result
