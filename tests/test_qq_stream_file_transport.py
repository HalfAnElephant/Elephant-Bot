import asyncio
import base64
import json
import shutil
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import select
from test_qq_agent_rich import rich_setup as _rich_setup

from qqbot.jobs.api import internal_router
from qqbot.jobs.worker import GeneralRun
from qqbot.providers import ModelCapacity
from qqbot.storage.local_media_schema import qq_local_media
from qqbot.storage.schema import outbox

rich_setup = _rich_setup
TOKEN = "s" * 32
DATA = b"%PDF-1.7\nreport"


def metadata(args, **changes):
    body = {"request_id": "stream", "filename": "report.pdf", "media_type": "file",
            "mime_type": "application/pdf", "size": len(DATA), "text": "report"} | changes
    value = {"worker_id": args[2], "run_id": args[0], "fencing_token": args[1], "payload": body}
    return {"X-QQ-Upload": base64.urlsafe_b64encode(json.dumps(value).encode()).decode(),
            "Content-Length": str(body["size"]), "Content-Type": "application/octet-stream"}


def app_for(service):
    app = FastAPI()
    app.include_router(internal_router(service, TOKEN))
    return app


async def test_authenticated_stream_endpoint_no_base64_body_and_idempotency(rich_setup):
    service, args, _task, _calls = rich_setup
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app_for(service)), base_url="http://core") as client:
        async def untouched():
            pytest.fail("unauthorized request must not read body")
            yield b""
        assert (await client.post("/internal/tasks/qq-file-upload", headers=metadata(args), content=untouched())).status_code == 401
        client.headers["Authorization"] = "Bearer " + TOKEN
        async def chunks():
            for part in (DATA[:4], DATA[4:]):
                yield part
        first = await client.post("/internal/tasks/qq-file-upload", headers=metadata(args), content=chunks())
        assert first.status_code == 200, first.text
        assert first.json()["state"] == "PENDING"
        again = await client.post("/internal/tasks/qq-file-upload", headers=metadata(args), content=chunks())
        assert again.json() == first.json()
        old = await client.post("/internal/tasks/qq-file-send", json={"worker_id": args[2], "run_id": args[0],
            "fencing_token": args[1], "payload": {"content_base64": "YWJj"}})
        assert old.status_code == 422 and "removed" in old.text


@pytest.mark.parametrize("change", [{"size": len(DATA) + 1}, {"mime_type": "text/plain"}])
async def test_invalid_length_or_mime_does_not_consume_storage(rich_setup, change):
    service, args, _task, _calls = rich_setup
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app_for(service)), base_url="http://core",
                                  headers={"Authorization": "Bearer " + TOKEN}) as client:
        response = await client.post("/internal/tasks/qq-file-upload", headers=metadata(args, **change), content=DATA)
        assert response.status_code == 422
    with service.db.engine.connect() as c:
        assert not c.execute(select(qq_local_media)).first()
        assert not c.execute(select(outbox)).first()


async def test_official_type_limit_and_busy_admission_precede_body_read(rich_setup):
    service, args, _task, _calls = rich_setup
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app_for(service)), base_url="http://core",
                                  headers={"Authorization": "Bearer " + TOKEN}) as client:
        async def untouched():
            pytest.fail("rejected upload must not consume body")
            yield b""
        response = await client.post("/internal/tasks/qq-file-upload", headers=metadata(args,
            filename="image.png", media_type="image", mime_type="image/png", size=30 * 1024 * 1024 + 1), content=untouched())
        assert response.status_code == 422
        service.qq_upload_slots = asyncio.Semaphore(0)
        service.qq_upload_runs = set()
        response = await client.post("/internal/tasks/qq-file-upload", headers=metadata(args), content=untouched())
        assert response.status_code == 429


async def test_actual_general_run_bind_and_warm_workspace_change_send_stream(rich_setup, tmp_path):
    service, ids, task, _calls = rich_setup
    app = FastAPI()
    @app.post("/internal/tasks/bind-process")
    async def bind_process():
        return {}  # This test exercises workspace binding and the real file HTTP route, not process accounting.
    app.include_router(internal_router(service, TOKEN))
    args = SimpleNamespace(workspace=str(tmp_path / "processes"), archive=str(tmp_path / "archive"),
        pi=shutil.which("false"), model="offline", model_endpoint="https://model.invalid/v1/chat/completions", worker_id=ids[2],
        spool=str(tmp_path / "spool"))
    claim = {"task_id": task["id"], "run_id": ids[0], "fencing_token": ids[1],
             "permission_snapshot": {"principal": json.loads(task["permission_json"])["principal"]}}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://core",
                                  headers={"Authorization": "Bearer " + TOKEN}) as client:
        runner = GeneralRun(args, claim, client, "process", "session", ModelCapacity())
        initial = runner.gateway.workspace
        await runner.bind(claim)
        assert runner.gateway.workspace != initial
        for iteration in range(2):
            if iteration:
                custom = tmp_path / "second-workspace"
                custom.mkdir()
                args.workspace_binding = ["chosen=" + str(custom)]
                runner.gateway.revoke()
                await runner.bind({**claim, "plan": {"workspace_ref": "chosen"}})
            (runner.gateway.workspace / "outputs" / "report.pdf").write_bytes(DATA)
            result = await runner.gateway.tools.invoke("qq.file.send", {"request_id": str(iteration),
                "path": "outputs/report.pdf", "media_type": "file"}, runner.gateway.context)
            assert result.status == "verified", result
            assert result.data["verified_effect"] == "durable_enqueue"


async def test_official_100_mib_file_stream_is_accepted_without_large_json(rich_setup):
    service, args, _task, _calls = rich_setup
    size = 100 * 1024 * 1024
    async def chunks():
        yield b"%PDF-1.7" + b"x" * (65536 - 8)
        for _ in range(size // 65536 - 1):
            yield b"x" * 65536
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app_for(service)), base_url="http://core",
                                  headers={"Authorization": "Bearer " + TOKEN}) as client:
        response = await client.post("/internal/tasks/qq-file-upload", headers=metadata(args, size=size), content=chunks())
        assert response.status_code == 200, response.text
        assert response.json()["state"] == "PENDING"
    with service.db.engine.connect() as c:
        assert c.execute(select(qq_local_media.c.size)).scalar_one() == size


async def test_cancel_waits_for_disk_owner_before_closing_staging(rich_setup, monkeypatch):
    import threading

    from qqbot.adapters.qq_official.local_media import LocalMediaStore
    from qqbot.jobs.rich_delivery import local_file_upload
    service, args, _task, _calls = rich_setup
    entered, release = threading.Event(), threading.Event()
    original = LocalMediaStore.put_file
    def slow_put(self, source, **kwargs):
        entered.set()
        release.wait(5)
        assert not source.closed
        return original(self, source, **kwargs)
    monkeypatch.setattr(LocalMediaStore, "put_file", slow_put)
    async def chunks():
        yield DATA
    payload = json.loads(base64.urlsafe_b64decode(metadata(args)["X-QQ-Upload"]))["payload"]
    pending = asyncio.create_task(local_file_upload(service, args, payload, chunks()))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        pending.cancel()
        await asyncio.sleep(.01)
        pending.cancel()
        await asyncio.sleep(.01)
        assert not pending.done()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await pending
    with service.db.engine.connect() as c:
        assert not c.execute(select(outbox)).first()
