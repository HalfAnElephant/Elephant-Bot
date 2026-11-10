import json
import socket

import httpx
import pytest

from qqbot.harness.research import ResearchCapabilities, fetch_public, public_addresses


@pytest.mark.parametrize("count", [0, 16, 25])
def test_public_dns_bounds_connection_candidates(monkeypatch, count):
    addresses = [
        (socket.AF_INET, socket.SOCK_STREAM, 6, "", (f"1.1.1.{index + 1}", 443))
        for index in range(count)
    ]
    monkeypatch.setattr(socket, "getaddrinfo", lambda *args, **kwargs: addresses)
    if not addresses:
        with pytest.raises(ValueError, match="empty"):
            public_addresses("cdn.example", 443)
    else:
        assert public_addresses("cdn.example", 443) == addresses[:16]


def test_public_dns_checks_addresses_beyond_connection_limit(monkeypatch):
    addresses = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("1.1.1.1", 443))] * 16
    addresses.append((socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443)))
    monkeypatch.setattr(socket, "getaddrinfo", lambda *args, **kwargs: addresses)
    with pytest.raises(ValueError, match="public unicast"):
        public_addresses("cdn.example", 443)


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


@pytest.mark.asyncio
async def test_tool_grants_budget_and_artifact(tmp_path, monkeypatch):
    capabilities = ResearchCapabilities(tmp_path, "allowed", "master-key")
    monkeypatch.setattr(
        "qqbot.harness.research.fetch_public",
        lambda url: {
            "url": url,
            "retrieved_at": "2026-11-08T00:00:00Z",
            "sha256": "a" * 64,
            "text": "retrieved text",
        },
    )
    transport = httpx.ASGITransport(app=capabilities.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://proxy") as client:
        auth = {"Authorization": "Bearer " + capabilities.tool_token}
        assert (await client.post("/tools/search", json={"query": "test"})).status_code == 403
        response = await client.post("/tools/search", headers=auth, json={"query": "test"})
        assert response.status_code == 200
        assert capabilities.evidence
        response = await client.post(
            "/tools/write", headers=auth, json={"path": "outputs/report.md", "text": "report"}
        )
        assert response.status_code == 200
        assert (tmp_path / "outputs/report.md").read_text() == "report"
        response = await client.post("/tools/write", headers=auth, json={"path": "../secret", "text": "bad"})
        assert response.status_code == 422
        capabilities.tool_calls = 60
        assert (await client.post("/tools/search", headers=auth, json={"query": "x"})).status_code == 429
        capabilities.revoke()
        assert (await client.post("/tools/search", headers=auth, json={"query": "x"})).status_code == 403


@pytest.mark.asyncio
async def test_model_proxy_credentials_and_revocation(tmp_path):
    requests = []

    def upstream(request):
        requests.append(request)
        assert request.headers["authorization"] == "Bearer master-key"
        body = json.loads(request.content)
        assert body["max_tokens"] == 4096 and body["stream"] is False
        return httpx.Response(
            200,
            json={
                "id": "r",
                "model": "allowed",
                "choices": [{"message": {"role": "assistant", "content": "done"}, "finish_reason": "stop"}],
            },
        )

    capabilities = ResearchCapabilities(
        tmp_path, "allowed", "master-key", transport=httpx.MockTransport(upstream)
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=capabilities.app), base_url="http://proxy"
    ) as client:
        auth = {"Authorization": "Bearer " + capabilities.model_token}
        body = {"model": "allowed", "messages": [{"role": "user", "content": "hello"}], "stream": True}
        response = await client.post("/v1/chat/completions", headers=auth, json=body)
        assert response.status_code == 200 and "[DONE]" in response.text
        assert "master-key" not in response.text
        body["model"] = "unapproved"
        assert (await client.post("/v1/chat/completions", headers=auth, json=body)).status_code == 422
        capabilities.revoke()
        body["model"] = "allowed"
        assert (await client.post("/v1/chat/completions", headers=auth, json=body)).status_code == 403
    assert len(requests) == 1


@pytest.mark.asyncio
async def test_real_pi_research_with_offline_model(tmp_path, monkeypatch):
    import asyncio
    from pathlib import Path
    from types import SimpleNamespace

    from qqbot.jobs.worker import ResearchRun

    executable = Path(__file__).resolve().parents[1] / "node_modules/.bin/pi"
    if not executable.exists():
        pytest.skip("locked pi package not installed")
    args = SimpleNamespace(
        workspace=str(tmp_path / "runs"),
        archive=str(tmp_path / "archive"),
        pi=str(executable),
        model="offline-model",
        worker_id="w",
    )
    claim = {
        "run_id": "11111111-1111-4111-8111-111111111111",
        "session_id": "11111111-1111-4111-8111-111111111111",
        "fencing_token": 1,
    }
    calls = []

    def model(request):
        body = json.loads(request.content)
        calls.append(body)
        if len(calls) == 1:
            message = {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "tool1",
                        "type": "function",
                        "function": {"name": "research_search", "arguments": json.dumps({"query": "test"})},
                    }
                ],
            }
        elif len(calls) == 2:
            message = {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "tool2",
                        "type": "function",
                        "function": {
                            "name": "research_write",
                            "arguments": json.dumps(
                                {"path": "outputs/report.md", "text": "Offline contract report"}
                            ),
                        },
                    }
                ],
            }
        else:
            message = {"role": "assistant", "content": "Report created from retrieved evidence."}
        return httpx.Response(
            200,
            json={
                "id": "r" + str(len(calls)),
                "model": "offline-model",
                "choices": [
                    {
                        "index": 0,
                        "message": message,
                        "finish_reason": "tool_calls" if message.get("tool_calls") else "stop",
                    }
                ],
                "usage": {"prompt_tokens": 10, "completion_tokens": 10, "total_tokens": 20},
            },
        )

    monkeypatch.setattr(
        "qqbot.harness.research.fetch_public",
        lambda url: {
            "url": url,
            "retrieved_at": "2026-11-08T00:00:00Z",
            "sha256": "a" * 64,
            "text": "retrieved test evidence",
        },
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={"state": "RUNNING"})),
        base_url="http://core",
    ) as core:
        harness = ResearchRun(args, claim, core)
        harness.research.transport = httpx.MockTransport(model)
        async with asyncio.timeout(20):
            result = await harness.run("Research test and create outputs/report.md")
        assert result.status == "completed"
        assert harness.research.evidence
        assert harness.research.artifacts == ["outputs/report.md"]
        assert (harness.workspace / "outputs/report.md").read_text() == "Offline contract report"
        assert len(calls) == 3
        assert all(
            "bash" not in [tool["function"]["name"] for tool in body.get("tools", [])] for body in calls
        )


def test_production_manifest_is_fixed_and_linux_gated():
    import sys
    from datetime import UTC, datetime, timedelta

    from qqbot.harness.launcher import production_policy, systemd_argv, validate_manifest

    task_id = "11111111-1111-4111-8111-111111111111"
    run_id = "22222222-2222-4222-8222-222222222222"
    policy = {
        "isolation_verified": True,
        "approved_models": ["approved"],
        "proxy_host": "10.203.0.1",
        "proxy_port_min": 30000,
        "proxy_port_max": 30999,
    }
    manifest = {
        "schema_version": 1,
        "task_id": task_id,
        "run_id": run_id,
        "session_id": run_id,
        "profile": "pi_research",
        "model": "approved",
        "proxy_url": "http://10.203.0.1:30000",
        "model_token": "a" * 43,
        "tool_token": "b" * 43,
        "expires_at": (datetime.now(UTC) + timedelta(seconds=60)).isoformat(),
    }
    assert validate_manifest(manifest, task_id, run_id, policy) == manifest
    general = {key: value for key, value in manifest.items() if key not in {"model_token", "tool_token"}}
    general.update(schema_version=2, profile="pi", process_id=run_id, process_epoch=1,
        session_id="33333333-3333-4333-8333-333333333333", process_token="c" * 43,
        model_context_window=32768, output_tokens=1000, soft_input_tokens=6000, keep_recent_tokens=2000,
        skill_version="a" * 64)
    assert validate_manifest(general, task_id, run_id, policy) == general
    for changes in ({"soft_input_tokens": 40000}, {"keep_recent_tokens": 6000}, {"process_epoch": 0},
                    {"process_id": task_id}, {"model_context_window": True}, {"output_tokens": 0}):
        with pytest.raises(ValueError):
            validate_manifest({**general, **changes}, task_id, run_id, policy)
    for key, value in [
        ("run_id", "../escape"),
        ("proxy_url", "http://127.0.0.1:30000"),
        ("profile", "pi_code"),
        ("model", "unapproved"),
    ]:
        with pytest.raises(ValueError):
            validate_manifest({**manifest, key: value}, task_id, run_id, policy)
    argv = systemd_argv(task_id, run_id, policy)
    assert "--property=RootDirectory=/var/lib/qqbot-runner-root" in argv
    assert "--property=KillMode=control-group" in argv
    assert "--property=NetworkNamespacePath=/run/netns/qqbot-runner" in argv
    assert not any("/etc/qqbot" in arg for arg in argv)
    if sys.platform != "linux":
        with pytest.raises(RuntimeError, match="requires Linux"):
            production_policy()


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
