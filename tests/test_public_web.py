import json
import socket

import pytest

from qqbot.harness.public_web import fetch_public, public_addresses


def test_ssrf_blocked_and_redirect_schemes(monkeypatch):
    for address in (
        "127.0.0.1",
        "10.0.0.1",
        "169.254.169.254",
        "::1",
        "fc00::1",
        "224.0.0.1",
        "ff02::1",
        "240.0.0.1",
    ):
        monkeypatch.setattr(
            socket,
            "getaddrinfo",
            lambda *args, address=address, **kwargs: [
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", (address, 443))
            ],
        )
        with pytest.raises(ValueError):
            public_addresses("test.example", 443)
    for url in (
        "file:///etc/passwd",
        "http://example.com",
        "https://user:pass@example.com",
        "https://example.com:444",
    ):
        with pytest.raises(ValueError):
            fetch_public(url)
def test_embedded_proxy_preserves_supervisor_signal_handlers():
    import signal

    import uvicorn

    from qqbot.jobs.worker import CapabilityServer

    before = signal.getsignal(signal.SIGTERM)
    server = CapabilityServer(uvicorn.Config("unused"))
    with server.capture_signals():
        assert signal.getsignal(signal.SIGTERM) is before
    assert signal.getsignal(signal.SIGTERM) is before


def test_root_manifest_mode_survives_supervisor_umask(tmp_path, monkeypatch):
    import os
    import stat
    from pathlib import Path
    from types import SimpleNamespace

    from qqbot.harness.launcher import atomic_root_json

    original_stat = Path.stat

    def root_directory_stat(path, *args, **kwargs):
        info = original_stat(path, *args, **kwargs)
        if path == tmp_path:
            return SimpleNamespace(st_mode=info.st_mode, st_uid=0)
        return info

    monkeypatch.setattr(Path, "stat", root_directory_stat)
    previous = os.umask(0o077)
    try:
        destination = tmp_path / "manifest.json"
        atomic_root_json(destination, {"test": "value"}, mode=0o444)
    finally:
        os.umask(previous)
    assert stat.S_IMODE(destination.stat().st_mode) == 0o444
    assert json.loads(destination.read_text()) == {"test": "value"}


def test_workspace_mode_survives_supervisor_umask(tmp_path, monkeypatch):
    import os
    import stat
    from types import SimpleNamespace

    from qqbot.harness import launcher

    monkeypatch.setattr(launcher, "WORKSPACES", tmp_path)
    monkeypatch.setattr(
        launcher.pwd, "getpwnam", lambda name: SimpleNamespace(pw_uid=os.getuid(), pw_gid=os.getgid())
    )
    previous = os.umask(0o077)
    try:
        directory = launcher.prepare_workspace("11111111-1111-4111-8111-111111111111")
    finally:
        os.umask(previous)
    assert stat.S_IMODE(directory.stat().st_mode) == 0o770
