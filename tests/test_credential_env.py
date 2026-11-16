import base64
import json

import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from elephant.adapters.qq_official.credential_store import load_credential
from elephant.credentials_env import credential_env


def test_pre_rename_sealed_credentials_read_with_legacy_key(tmp_path, monkeypatch):
    key = b"k" * 32
    nonce = b"n" * 12
    monkeypatch.delenv("ELEPHANT_CREDENTIAL_KEY", raising=False)
    monkeypatch.setenv("QQBOT_CREDENTIAL_KEY", base64.b64encode(key).decode())
    reference = "1" * 32
    for identity, secret, directory in (
        ("qq-fixture", "qq-secret", "qq-credentials"),
        ("mail:mail-fixture", "mail-secret", "mail-credentials"),
    ):
        root = tmp_path / directory
        root.mkdir(mode=0o700)
        credential = root / (reference + ".json")
        payload = nonce + AESGCM(key).encrypt(nonce, secret.encode(), identity.encode())
        credential.write_text(json.dumps({"version": 1, "app_id": identity, "sealed": base64.b64encode(payload).decode()}))
        credential.chmod(0o600)
        assert load_credential(root, reference, identity) == secret
        with pytest.raises(ValueError, match="unavailable or invalid"):
            load_credential(root, reference, "other-identity")


def test_explicit_credential_name_wins_without_generic_prefix_aliases(monkeypatch):
    monkeypatch.setenv("QQBOT_MODEL_KEY", "legacy")
    monkeypatch.delenv("ELEPHANT_MODEL_KEY", raising=False)
    assert credential_env("ELEPHANT_MODEL_KEY") == "legacy"
    monkeypatch.setenv("ELEPHANT_MODEL_KEY", "")
    assert credential_env("ELEPHANT_MODEL_KEY") == ""
    monkeypatch.setenv("ELEPHANT_MODEL_KEY", "current")
    assert credential_env("QQBOT_MODEL_KEY") == "legacy"
    monkeypatch.delenv("QQBOT_MODEL_KEY")
    assert credential_env("QQBOT_MODEL_KEY") == "current"
    monkeypatch.setenv("QQBOT_UNRECOGNIZED", "must-not-resolve")
    assert credential_env("ELEPHANT_UNRECOGNIZED", "fallback") == "fallback"
