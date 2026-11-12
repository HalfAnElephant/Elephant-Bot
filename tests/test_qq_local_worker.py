import asyncio
import io
import threading
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from qqbot.capabilities.gateway import CapabilityRegistry
from qqbot.capabilities.qq import register_qq


def setup(tmp_path, call, upload=None):
    registry = CapabilityRegistry()
    register_qq(registry, call, workspace=tmp_path, upload=upload or (lambda *_: pytest.fail("must not upload")))
    return registry.entries["qq.file.send"][1], SimpleNamespace(workspace=str(tmp_path))


async def test_worker_local_file_send_reads_after_authorization_off_loop(tmp_path, monkeypatch):
    from qqbot.adapters.qq_official import local_media
    calls, threads = [], []
    def read(root, path, kind):
        assert calls == ["authorize"]
        assert root == tmp_path and path == "outputs/result.png"
        threads.append(threading.get_ident())
        return io.BytesIO(b"safe-content"), 12, "image/png"
    monkeypatch.setattr(local_media, "snapshot_workspace_file", read)
    async def call(op, payload):
        calls.append(op)
        if op == "authorize":
            return {"allowed": True}
        pytest.fail("unexpected operation")
    async def upload(payload, chunks):
        assert payload == {"request_id": "one", "filename": "result.png", "media_type": "image",
            "mime_type": "image/png", "size": 12, "text": "hello"}
        assert b"".join([chunk async for chunk in chunks]) == b"safe-content"
        return {"delivery_id": "delivery", "delivered": False, "state": "PENDING"}
    execute, context = setup(tmp_path, call, upload)
    result = await execute({"request_id": "one", "path": "outputs/result.png", "media_type": "image", "text": "hello"}, context)
    assert result["delivered"] is False
    assert threads[0] != threading.get_ident()


@pytest.mark.parametrize("path", ["/etc/passwd", "config.yaml", "../outputs/a.png", "outputs"])
async def test_worker_rejects_nonoutput_paths_before_authority_or_read(tmp_path, path):
    async def call(*args):
        pytest.fail("invalid source must not reach Core")
    execute, context = setup(tmp_path, call)
    with pytest.raises(ValueError, match="outputs"):
        await execute({"request_id": "one", "path": path, "media_type": "image"}, context)


async def test_worker_denial_does_not_read_or_upload(tmp_path, monkeypatch):
    from qqbot.adapters.qq_official import local_media
    monkeypatch.setattr(local_media, "snapshot_workspace_file", lambda *a: pytest.fail("must not read"))
    calls = []
    async def call(op, payload):
        calls.append(op)
        return {"allowed": False}
    execute, context = setup(tmp_path, call)
    with pytest.raises(PermissionError):
        await execute({"request_id": "one", "path": "outputs/a.png", "media_type": "image"}, context)
    assert calls == ["authorize"]


async def test_worker_cannot_override_workspace_or_destination(tmp_path):
    async def call(*args):
        pytest.fail("invalid identity")
    execute, context = setup(tmp_path, call)
    with pytest.raises(PermissionError):
        await execute({"request_id": "one", "path": "outputs/a.png", "media_type": "image"},
                      SimpleNamespace(workspace="/elsewhere"))
    with pytest.raises(ValidationError):
        await execute({"request_id": "one", "path": "outputs/a.png", "media_type": "image", "target": "other"}, context)


async def test_cancelled_read_never_uploads_when_background_read_finishes(tmp_path, monkeypatch):
    from qqbot.adapters.qq_official import local_media
    started, release = threading.Event(), threading.Event()
    calls = []
    def read(*args):
        started.set()
        release.wait(timeout=2)
        return io.BytesIO(b"safe"), 4, "image/png"
    monkeypatch.setattr(local_media, "snapshot_workspace_file", read)
    async def call(op, payload):
        calls.append(op)
        return {"allowed": True}
    execute, context = setup(tmp_path, call)
    task = asyncio.create_task(execute({"request_id": "one", "path": "outputs/a.png", "media_type": "image"}, context))
    try:
        assert await asyncio.to_thread(started.wait, 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        release.set()
    await asyncio.sleep(.01)
    assert calls == ["authorize"]


async def test_real_workspace_bytes_are_forwarded_without_absolute_path(tmp_path):
    directory = tmp_path / "outputs"
    directory.mkdir()
    (directory / "report.txt").write_text("用户要求的报告", encoding="utf-8")
    calls = []
    async def call(op, payload):
        calls.append((op, payload))
        return {"allowed": True} if op == "authorize" else {"delivery_id": "one", "state": "PENDING"}
    async def upload(payload, chunks):
        calls.append(("upload", payload))
        assert b"".join([part async for part in chunks]).decode() == "用户要求的报告"
        return {"delivery_id": "one", "state": "PENDING"}
    execute, context = setup(tmp_path, call, upload)
    await execute({"request_id": "one", "path": "outputs/report.txt", "media_type": "file"}, context)
    payload = calls[-1][1]
    assert payload["mime_type"] == "text/plain" and payload["filename"] == "report.txt"
    assert "path" not in payload and "workspace" not in payload and "content_base64" not in payload



@pytest.mark.parametrize("mode", ["symlink", "hardlink", "hidden", "traversal"])
async def test_real_worker_forbidden_files_never_upload(tmp_path, mode):
    directory = tmp_path / "outputs"
    directory.mkdir()
    original = tmp_path / "secret.txt"
    original.write_text("private")
    name = "report.txt"
    if mode == "symlink":
        (directory / name).symlink_to(original)
    elif mode == "hardlink":
        (directory / name).hardlink_to(original)
    elif mode == "hidden":
        name = ".env"
        (directory / name).write_text("SECRET=value")
    else:
        name = "../secret.txt"
    calls = []
    async def call(op, payload):
        calls.append(op)
        return {"allowed": True}
    execute, context = setup(tmp_path, call)
    with pytest.raises((ValueError, OSError)):
        await execute({"request_id": "one", "path": "outputs/" + name, "media_type": "file"}, context)
    assert calls == ["authorize"]
