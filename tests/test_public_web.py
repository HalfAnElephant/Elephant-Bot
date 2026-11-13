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


@pytest.mark.parametrize("allow_redirect", [False, True])
async def test_web_redirect_rechecks_domain_policy_before_dns(monkeypatch, tmp_path, allow_redirect):
    from types import SimpleNamespace

    from qqbot.capabilities import ToolContext, ToolGateway, standard_registry
    from qqbot.harness import public_web

    looked_up, requested, checked = [], [], []

    def addresses(host, port):
        looked_up.append(host)
        return []

    def request(connection, method, path, headers):
        requested.append(connection.host)
        connection.sock = SimpleNamespace(settimeout=lambda timeout: None)

    def response(connection):
        data = bytearray(b"page")
        def read1(size):
            chunk = bytes(data[:size])
            del data[:size]
            return chunk
        return SimpleNamespace(status=302 if connection.host == "allowed.example" else 200,
            getheader=lambda name, default=None: {
                "Location": "https://other.example/page", "Content-Type": "text/plain",
            }.get(name, default), read1=read1, close=lambda: None)

    async def permission(context, action):
        checked.append(action.resources[0])
        return allow_redirect or action.resources == ("domain:allowed.example",)

    monkeypatch.setattr(public_web, "public_addresses", addresses)
    monkeypatch.setattr(public_web.PinnedHTTPS, "request", request)
    monkeypatch.setattr(public_web.PinnedHTTPS, "getresponse", response)
    monkeypatch.setattr(public_web.PinnedHTTPS, "close", lambda connection: None)
    gateway = ToolGateway(standard_registry(), authority=lambda context: True, permission=permission, ledger=None)
    context = ToolContext("task", "run", 1, {}, str(tmp_path), None)
    if allow_redirect:
        result = await gateway.invoke("web.fetch", {"url": "https://allowed.example"}, context)
        assert result.data["text"] == "page"
        assert requested == looked_up == ["allowed.example", "other.example"]
    else:
        with pytest.raises(PermissionError, match="denied"):
            await gateway.invoke("web.fetch", {"url": "https://allowed.example"}, context)
        assert requested == looked_up == ["allowed.example"]
    assert checked[-1] == "domain:other.example"


def test_connect_attempts_are_bounded_and_share_deadline(monkeypatch):
    from qqbot.harness import public_web

    now, attempts, timeouts = [0.0], [], []
    class Socket:
        def settimeout(self, timeout):
            timeouts.append(timeout)
        def connect(self, address):
            attempts.append(address)
            now[0] += 6
            raise OSError("unreachable")
        def close(self):
            pass

    monkeypatch.setattr(public_web.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(public_web.socket, "socket", lambda *args: Socket())
    addresses = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 443))] * 16
    connection = public_web.PinnedHTTPS("public.example", 443, addresses, deadline=15)
    with pytest.raises(TimeoutError, match="deadline"):
        connection.connect()
    assert len(attempts) == 3 and timeouts == [10, 9, 3]
    now[0] = 0
    connection.deadline = 100
    with pytest.raises(OSError, match="unreachable"):
        connection.connect()
    assert len(attempts) == 7


def test_all_dns_answers_are_validated_before_candidate_limit(monkeypatch):
    answers = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 443))] * 15
    answers.append((socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443)))
    monkeypatch.setattr(socket, "getaddrinfo", lambda *args, **kwargs: answers)
    with pytest.raises(ValueError, match="public unicast"):
        public_addresses("public.example", 443)
