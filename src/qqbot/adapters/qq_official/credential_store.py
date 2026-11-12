"""Encrypted, app-bound local credentials. The master key is never stored here."""
import base64
import json
import os
import re
from pathlib import Path
from uuid import uuid4

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

KEY_ENV = "QQBOT_CREDENTIAL_KEY"


def master_key() -> bytes:
    try:
        key = base64.b64decode(os.environ.get(KEY_ENV, ""), validate=True)
        if len(key) != 32:
            raise ValueError
        return key
    except (ValueError, TypeError):
        raise ValueError("credential encryption key is unavailable") from None


def _path(directory, reference):
    if not re.fullmatch(r"[a-f0-9]{32}", reference):
        raise ValueError("invalid credential reference")
    return Path(directory) / (reference + ".json")


def save_credential(directory, app_id: str, secret: str) -> str:
    key = master_key()
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    if directory.is_symlink() or directory.stat().st_mode & 0o077:
        raise ValueError("credential directory must be private")
    reference = uuid4().hex
    nonce = os.urandom(12)
    encrypted = AESGCM(key).encrypt(nonce, secret.encode(), app_id.encode())
    data = json.dumps({"version": 1, "app_id": app_id,
                       "sealed": base64.b64encode(nonce + encrypted).decode()}).encode()
    path = _path(directory, reference)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise
    return reference


def load_credential(directory, reference: str, app_id: str) -> str:
    try:
        path = _path(directory, reference)
        if path.parent.is_symlink() or path.parent.stat().st_mode & 0o077:
            raise ValueError
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, "rb") as handle:
            if os.fstat(handle.fileno()).st_mode & 0o077:
                raise ValueError
            data = json.loads(handle.read(16385))
        if data.get("version") != 1 or data.get("app_id") != app_id:
            raise ValueError
        raw = base64.b64decode(data["sealed"], validate=True)
        return AESGCM(master_key()).decrypt(raw[:12], raw[12:], app_id.encode()).decode()
    except (OSError, ValueError, KeyError, TypeError, InvalidTag):
        raise ValueError("credential is unavailable or invalid") from None


def remove_credential(directory, reference):
    _path(directory, reference).unlink(missing_ok=True)
