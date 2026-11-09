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
    for path in ["../secret", "/etc/passwd", "bad.zip"]:
        with pytest.raises(ValueError):
            export_artifacts(workspace, archive, [path])
    (workspace / "link.md").symlink_to(workspace / "ok.md")
    with pytest.raises(OSError):
        export_artifacts(workspace, archive, ["link.md"])
    os.link(workspace / "ok.md", workspace / "hard.md")
    with pytest.raises(ValueError):
        export_artifacts(workspace, archive, ["hard.md"])


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
