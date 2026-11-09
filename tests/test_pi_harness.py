import asyncio
import json
import os
from pathlib import Path

import pytest

from qqbot.harness.artifacts import export_artifacts
from qqbot.harness.pi import PiHarness
from qqbot.jobs.worker import Spool


def test_artifact_boundaries(tmp_path):
    workspace = tmp_path / "workspace"
    archive = tmp_path / "archive"
    workspace.mkdir()
    (workspace / "ok.md").write_text("report")
    result = export_artifacts(workspace, archive, ["ok.md"])
    assert (archive / result[0]["id"]).read_text() == "report"
    for path in ["../secret", "/etc/passwd", "bad.exe"]:
        with pytest.raises(ValueError):
            export_artifacts(workspace, archive, [path])
    (workspace / "link.md").symlink_to(workspace / "ok.md")
    with pytest.raises(OSError):
        export_artifacts(workspace, archive, ["link.md"])
    os.link(workspace / "ok.md", workspace / "hard.md")
    with pytest.raises(ValueError):
        export_artifacts(workspace, archive, ["hard.md"])


@pytest.mark.parametrize(("suffix", "mime", "content"), [
    (".zip", "application/zip", b"PK\x03\x04\xff\x00"),
    (".pdf", "application/pdf", b"%PDF-1.7\n\xff\x00"),
    (".png", "image/png", b"\x89PNG\r\n\x1a\n\xff\x00"),
    (".jpg", "image/jpeg", b"\xff\xd8\xff\x00"),
    (".webp", "image/webp", b"RIFF\xff\x00WEBP"),
    (".csv", "text/csv", "name,value\n中文,1\n".encode()),
    (".html", "text/html", "<!doctype html><p>中文</p>".encode()),
])
def test_artifact_binary_and_text_mime_map(tmp_path, suffix, mime, content):
    import hashlib
    workspace, archive = tmp_path / "work", tmp_path / "archive"
    workspace.mkdir()
    (workspace / ("deliverable" + suffix)).write_bytes(content)
    exported = export_artifacts(workspace, archive, ["deliverable" + suffix])
    assert exported[0]["mime_type"] == mime
    assert exported[0]["size"] == len(content)
    assert exported[0]["sha256"] == hashlib.sha256(content).hexdigest()
    assert (archive / exported[0]["id"]).read_bytes() == content


def test_artifact_custom_mime_map_and_invalid_text(tmp_path):
    workspace, archive = tmp_path / "work", tmp_path / "archive"
    workspace.mkdir()
    (workspace / "custom.bin").write_bytes(b"\xff\x00")
    result = export_artifacts(workspace, archive, ["custom.bin"], mime_types={".bin": "application/octet-stream"})
    assert result[0]["mime_type"] == "application/octet-stream"
    (workspace / "invalid.csv").write_bytes(b"\xff")
    with pytest.raises(UnicodeDecodeError):
        export_artifacts(workspace, archive, ["invalid.csv"])
    with pytest.raises(ValueError):
        export_artifacts(workspace, archive, ["custom.bin"], mime_types={".txt": "text/plain"})


def test_artifact_size_and_failed_batch_cleanup(tmp_path, monkeypatch):
    workspace, archive = tmp_path / "work", tmp_path / "archive"
    workspace.mkdir()
    (workspace / "first.pdf").write_bytes(b"pdf")
    (workspace / "second.zip").write_bytes(b"zip")
    with pytest.raises(ValueError, match="budget"):
        export_artifacts(workspace, archive, ["first.pdf", "second.zip"], max_bytes=5)
    assert list(archive.iterdir()) == []
    def fail_sync(fd):
        raise OSError("fixture disk error")
    monkeypatch.setattr(os, "fsync", fail_sync)
    with pytest.raises(OSError, match="disk error"):
        export_artifacts(workspace, archive, ["first.pdf"])
    assert list(archive.iterdir()) == []


def test_artifact_rejects_symlink_directory_and_changed_binary(tmp_path, monkeypatch):
    workspace, archive = tmp_path / "work", tmp_path / "archive"
    workspace.mkdir()
    private = tmp_path / "private"
    private.mkdir()
    (private / "secret.pdf").write_bytes(b"private")
    (workspace / "linked").symlink_to(private, target_is_directory=True)
    with pytest.raises(OSError):
        export_artifacts(workspace, archive, ["linked/secret.pdf"])
    binary = workspace / "mutable.pdf"
    binary.write_bytes(b"before")
    original = os.read
    changed = False
    def mutate(fd, count):
        nonlocal changed
        content = original(fd, count)
        if not changed:
            changed = True
            binary.write_bytes(b"after that grew")
        return content
    monkeypatch.setattr(os, "read", mutate)
    with pytest.raises(ValueError, match="changed"):
        export_artifacts(workspace, archive, ["mutable.pdf"])
    assert list(archive.iterdir()) == []


def test_spool_bound(tmp_path):
    spool = Spool(tmp_path, max_bytes=30)
    path = spool.save({"value": "ok"})
    assert json.loads(path.read_text()) == {"value": "ok"}
    with pytest.raises(RuntimeError):
        spool.save({"value": "x" * 40})


@pytest.mark.asyncio
async def test_ack_agent_end_do_not_complete(tmp_path):
    fake = tmp_path / "fake-pi"
    fake.write_text("""#!/usr/bin/env python3
import sys,json,time
for line in sys.stdin:
 c=json.loads(line)
 if c['type']=='prompt':
  print(json.dumps({'type':'response','id':c['id'],'success':True,'data':{'disposition':'started'}}),flush=True)
  print(json.dumps({'type':'agent_end'}),flush=True)
  time.sleep(.15)
  print(json.dumps({'type':'message_end','message':{'role':'assistant','content':[{'type':'text','text':'done next'}]}}),flush=True)
  print(json.dumps({'type':'agent_settled'}),flush=True)
 else: break
""")
    fake.chmod(0o700)
    harness = PiHarness(fake, tmp_path / "work", timeout=8)
    result = await harness.run("hello")
    assert result.summary == "done next"
    assert harness.process.returncode is not None


@pytest.mark.asyncio
async def test_real_pi_control_without_credentials(tmp_path):
    executable = Path(__file__).resolve().parents[1] / "node_modules/.bin/pi"
    if not executable.exists():
        pytest.skip("locked pi package not installed")
    harness = PiHarness(executable, tmp_path / "real", timeout=8)
    await harness.start()
    try:
        command_id = await harness.command("get_state")
        async with asyncio.timeout(8):
            while True:
                event = await harness.queue.get()
                if event.get("id") == command_id:
                    assert event["success"]
                    break
    finally:
        assert await harness.stop()
    assert harness.session_path.is_absolute()


def test_stderr_diagnostics_are_bounded_and_redacted(tmp_path):
    harness = PiHarness(__file__, tmp_path / "work", api_key="secret-upstream-key-123456")
    harness.stderr_tail = bytearray(
        b"PermissionError reading manifest\nAuthorization: Bearer secret-upstream-key-123456"
    )
    summary = harness.failure_summary(RuntimeError("RPC closed"))
    assert "PermissionError" in summary and "RPC closed" in summary
    assert "secret-upstream-key-123456" not in summary
    assert "[REDACTED]" in summary
