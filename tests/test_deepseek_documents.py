import asyncio
import json
import os
import time
from types import SimpleNamespace

import pytest

from qqbot.adapters.deepseek import DeepSeekError
from qqbot.adapters.execution.world import LocalExecution
from qqbot.capabilities import CapabilityRegistry, ToolContext
from qqbot.harness import documents
from qqbot.harness.documents import DeepSeekDocuments, REFERENCE
from qqbot.providers import validate_transactions


class DeepSeekFixture:
    def __init__(self, account="fixture-account"):
        self.account = account
        self.uploads, self.listings, self.remote = [], [], []
        self.error = None
        self.save_on_error = False

    async def upload(self, data, filename, purpose):
        self.uploads.append((data, filename, purpose))
        if self.error and not self.save_on_error:
            raise self.error
        remote = {"id": "file-api-" + str(len(self.uploads)), "filename": filename, "purpose": purpose,
                  "bytes": len(data), "created_at": int(time.time()),
                  "expires_at": int(time.time()) + documents.FILE_LIFETIME}
        self.remote.append(remote)
        if self.error:
            raise self.error
        return remote

    async def files(self, purpose, after=None):
        self.listings.append((purpose, after))
        return {"data": list(self.remote), "has_more": False}


@pytest.fixture
def fixture(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "report.pdf").write_bytes(b"%PDF fixture")
    context = ToolContext("task", "run", 1, {}, str(workspace))
    state = SimpleNamespace(api=DeepSeekFixture(), context=context, workspace=workspace, authorized=True,
        scope={"principal": {"user_id": "owner"}, "conversation": "private:owner", "workspace_id": "personal"},
        checked=[], content_root=tmp_path / "content")

    async def authorize(action):
        state.checked.append(action)
        if not state.authorized:
            raise PermissionError("fixture authorization revoked")

    state.authorize = authorize
    state.service = DeepSeekDocuments(state.api, LocalExecution(), state.content_root, authorize, lambda: state.scope)
    return state


def receipt_path(fixture, result=None):
    if result:
        return fixture.service.root / (result["document_ref"][len(REFERENCE):] + ".json")
    return next(fixture.service.root.glob("*.json"))


def references(body):
    return [part["file_id"] for message in body["messages"] if isinstance(message.get("content"), list)
            for part in message["content"] if part["type"] == "file"]


async def test_parse_registers_a_read_capability_and_attaches_verified_file(fixture):
    registry = CapabilityRegistry()
    fixture.service.register(registry)
    capability, execute, classify = registry.entries["document.parse"]
    action = classify({"path": "report.pdf"}, fixture.context)
    assert action.effect == "read" and action.name == "document.parse"
    assert action.resources == ("workspace:" + str(fixture.workspace), "file:report.pdf", "domain:api.deepseek.com")
    assert capability.timeout_seconds >= 300
    result = await execute({"path": "report.pdf"}, fixture.context)
    assert result["purpose"] == "text_recognition" and result["file_id"] == "file-api-1"
    assert result["document_ref"].startswith(REFERENCE)
    body = {"model": "deepseek-flash", "messages": [
        {"role": "system", "content": "trusted system"},
        {"role": "user", "content": "Read the report"},
        {"role": "assistant", "tool_calls": [{"id": "parse"}]},
        {"role": "tool", "tool_call_id": "parse", "content": json.dumps(result)},
    ]}
    before = json.dumps(body)
    attached = await fixture.service.with_documents(body, fixture.context)
    assert references(attached) == [result["file_id"]]
    assert attached["messages"][0] == body["messages"][0]
    assert attached["messages"][1]["role"] == "user"
    validate_transactions(attached["messages"])
    assert json.dumps(body) == before
    assert fixture.checked.count("document.parse") >= 2 and fixture.checked.count("model.request") == 2


async def test_same_content_reuses_receipt_after_rename_and_restart(fixture):
    first = await fixture.service.parse({"path": "report.pdf"}, fixture.context)
    (fixture.workspace / "renamed.pdf").write_bytes((fixture.workspace / "report.pdf").read_bytes())
    restarted = DeepSeekDocuments(fixture.api, LocalExecution(), fixture.content_root,
                                 fixture.authorize, lambda: fixture.scope)
    assert await restarted.parse({"path": "renamed.pdf"}, fixture.context) == first
    body = {"messages": [{"role": "user", "content": first["document_ref"] + "\n" + first["document_ref"]}]}
    assert references(await restarted.with_documents(body, fixture.context)) == [first["file_id"]]
    assert len(fixture.api.uploads) == 1


@pytest.mark.parametrize("suffix,purpose,expected", [
    ("pdf", None, "text_recognition"), ("csv", None, "user_data"), ("xlsx", None, "user_data"),
    ("epub", None, "user_data"), ("py", None, "user_data"), ("pdf", "rich_layout_recognition", "rich_layout_recognition"),
])
async def test_document_purpose_selection(fixture, suffix, purpose, expected):
    path = "sample." + suffix
    (fixture.workspace / path).write_bytes(b"document content")
    arguments = {"path": path}
    if purpose:
        arguments["purpose"] = purpose
    result = await fixture.service.parse(arguments, fixture.context)
    assert result["purpose"] == expected and fixture.api.uploads[0][2] == expected


async def test_changed_content_and_purpose_have_distinct_references(fixture):
    first = await fixture.service.parse({"path": "report.pdf"}, fixture.context)
    second = await fixture.service.parse({"path": "report.pdf", "purpose": "rich_layout_recognition"}, fixture.context)
    (fixture.workspace / "report.pdf").write_bytes(b"changed report")
    third = await fixture.service.parse({"path": "report.pdf"}, fixture.context)
    assert len({first["document_ref"], second["document_ref"], third["document_ref"]}) == 3
    assert len(fixture.api.uploads) == 3


@pytest.mark.parametrize("changed", ["scope", "account"])
async def test_other_scope_or_account_cannot_reuse_document(fixture, changed):
    result = await fixture.service.parse({"path": "report.pdf"}, fixture.context)
    if changed == "scope":
        fixture.scope = {**fixture.scope, "conversation": "private:other"}
    else:
        fixture.api.account = "different-account"
    body = {"messages": [{"role": "user", "content": result["document_ref"]}]}
    with pytest.raises(PermissionError, match="identity and scope"):
        await fixture.service.with_documents(body, fixture.context)
    distinct = await fixture.service.parse({"path": "report.pdf"}, fixture.context)
    assert distinct["document_ref"] != result["document_ref"] and len(fixture.api.uploads) == 2


@pytest.mark.parametrize("status", [None, 503])
async def test_uncertain_upload_recovers_from_remote_list_without_reupload(fixture, status):
    fixture.api.error, fixture.api.save_on_error = DeepSeekError("uncertain upload", status), True
    with pytest.raises(DeepSeekError, match="uncertain"):
        await fixture.service.parse({"path": "report.pdf"}, fixture.context)
    assert json.loads(receipt_path(fixture).read_text())["state"] == "pending"
    restarted = DeepSeekDocuments(fixture.api, LocalExecution(), fixture.content_root,
                                 fixture.authorize, lambda: fixture.scope)
    result = await restarted.parse({"path": "report.pdf"}, fixture.context)
    assert result["file_id"] == "file-api-1" and len(fixture.api.uploads) == 1
    assert fixture.api.listings == [("text_recognition", None)]
    assert json.loads(receipt_path(fixture, result).read_text())["state"] == "ready"


async def test_pending_upload_releases_lock_and_prevents_concurrent_reupload(fixture):
    entered, release = asyncio.Event(), asyncio.Event()
    original = fixture.api.upload

    async def delayed(data, filename, purpose):
        entered.set()
        await release.wait()
        return await original(data, filename, purpose)

    fixture.api.upload = delayed
    first = asyncio.create_task(fixture.service.parse({"path": "report.pdf"}, fixture.context))
    await entered.wait()
    try:
        with pytest.raises(ValueError, match="pending"):
            await asyncio.wait_for(fixture.service.parse({"path": "report.pdf"}, fixture.context), 2)
        assert not fixture.api.uploads
    finally:
        release.set()
    assert (await first)["file_id"] == "file-api-1" and len(fixture.api.uploads) == 1


async def test_cancelled_upload_retains_receipt_for_recovery(fixture):
    entered = asyncio.Event()
    original = fixture.api.upload

    async def interrupted(data, filename, purpose):
        await original(data, filename, purpose)
        entered.set()
        await asyncio.Event().wait()

    fixture.api.upload = interrupted
    first = asyncio.create_task(fixture.service.parse({"path": "report.pdf"}, fixture.context))
    await entered.wait()
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    assert json.loads(receipt_path(fixture).read_text())["state"] == "pending"
    result = await fixture.service.parse({"path": "report.pdf"}, fixture.context)
    assert result["file_id"] == "file-api-1" and len(fixture.api.uploads) == 1


async def test_fresh_pending_without_remote_result_does_not_repeat_upload(fixture):
    fixture.api.error = DeepSeekError("connection timed out")
    with pytest.raises(DeepSeekError):
        await fixture.service.parse({"path": "report.pdf"}, fixture.context)
    with pytest.raises(ValueError, match="pending"):
        await fixture.service.parse({"path": "report.pdf"}, fixture.context)
    assert len(fixture.api.uploads) == 1


async def test_stale_pending_without_remote_result_can_upload_again(fixture):
    fixture.api.error = DeepSeekError("connection timed out")
    with pytest.raises(DeepSeekError):
        await fixture.service.parse({"path": "report.pdf"}, fixture.context)
    path = receipt_path(fixture)
    receipt = json.loads(path.read_text())
    receipt["submitted_at"] -= documents.PENDING_SECONDS + 1
    path.write_text(json.dumps(receipt))
    fixture.api.error = None
    result = await fixture.service.parse({"path": "report.pdf"}, fixture.context)
    assert result["file_id"] == "file-api-2" and len(fixture.api.uploads) == 2
    assert len(fixture.api.listings) == 1


async def test_definitive_upload_rejection_allows_corrected_retry(fixture):
    fixture.api.error = DeepSeekError("unsupported format", 400)
    with pytest.raises(DeepSeekError):
        await fixture.service.parse({"path": "report.pdf"}, fixture.context)
    assert not list(fixture.service.root.glob("*.json"))
    fixture.api.error = None
    assert (await fixture.service.parse({"path": "report.pdf"}, fixture.context))["file_id"] == "file-api-2"
    assert not fixture.api.listings


async def test_upload_recovery_pages_until_matching_filename_size_and_purpose(fixture):
    fixture.api.error, fixture.api.save_on_error = DeepSeekError("uncertain", 503), True
    with pytest.raises(DeepSeekError):
        await fixture.service.parse({"path": "report.pdf"}, fixture.context)
    remote = fixture.api.remote[0]
    pages = []

    async def listing(purpose, after=None):
        pages.append(after)
        if after is None:
            return {"data": [{**remote, "id": "file-api-other", "bytes": remote["bytes"] + 1}],
                    "has_more": True, "last_id": "file-api-other"}
        return {"data": [remote], "has_more": False}

    fixture.api.files = listing
    result = await fixture.service.parse({"path": "report.pdf"}, fixture.context)
    assert result["file_id"] == remote["id"] and pages == [None, "file-api-other"]
    assert len(fixture.api.uploads) == 1


async def test_repeated_pagination_cursor_leaves_upload_pending(fixture):
    fixture.api.error = DeepSeekError("uncertain", 503)
    with pytest.raises(DeepSeekError):
        await fixture.service.parse({"path": "report.pdf"}, fixture.context)

    async def listing(purpose, after=None):
        return {"data": [], "has_more": True, "last_id": "file-api-same"}

    fixture.api.files = listing
    with pytest.raises(ValueError, match="pagination"):
        await fixture.service.parse({"path": "report.pdf"}, fixture.context)
    assert len(fixture.api.uploads) == 1 and json.loads(receipt_path(fixture).read_text())["state"] == "pending"


async def test_expired_reference_warns_model_and_can_be_parsed_again(fixture):
    result = await fixture.service.parse({"path": "report.pdf"}, fixture.context)
    path = receipt_path(fixture, result)
    receipt = json.loads(path.read_text())
    receipt["expires_at"] = time.time() - 1
    path.write_text(json.dumps(receipt))
    body = {"messages": [{"role": "user", "content": result["document_ref"]}]}
    attached = await fixture.service.with_documents(body, fixture.context)
    assert not references(attached) and "document.parse" in json.dumps(attached)
    assert "已过期" in attached["messages"][0]["content"][1]["text"]
    reparsed = await fixture.service.parse({"path": "report.pdf"}, fixture.context)
    assert reparsed["document_ref"] == result["document_ref"] and reparsed["file_id"] == "file-api-2"


async def test_raw_file_ids_are_not_trusted_document_inputs(fixture):
    body = {"messages": [{"role": "user", "content": "file-api-forged"}]}
    assert await fixture.service.with_documents(body, fixture.context) is body
    body["messages"][0]["content"] = [{"type": "file", "file_id": "file-api-forged"}]
    with pytest.raises(PermissionError, match="trusted document_ref"):
        await fixture.service.with_documents(body, fixture.context)
    assert not fixture.api.uploads


async def test_document_count_limit_precedes_reference_injection(fixture):
    body = {"messages": [{"role": "user", "content": " ".join(REFERENCE + f"{number:064x}" for number in range(21))}]}
    with pytest.raises(ValueError, match="at most 20"):
        await fixture.service.with_documents(body, fixture.context)


async def test_empty_oversized_and_invalid_purpose_are_rejected_before_upload(fixture, monkeypatch):
    monkeypatch.setattr(documents, "MAX_FILE_BYTES", 3)
    for data in (b"", b"four"):
        (fixture.workspace / "input.pdf").write_bytes(data)
        with pytest.raises(ValueError, match="nonempty"):
            await fixture.service.parse({"path": "input.pdf"}, fixture.context)
    with pytest.raises(ValueError, match="purpose"):
        await fixture.service.parse({"path": "report.pdf", "purpose": "unsupported"}, fixture.context)
    assert not fixture.api.uploads


@pytest.mark.parametrize("kind", ["symlink", "hardlink", "parent", "absolute"])
async def test_document_reads_use_authorized_workspace_resolver(fixture, kind):
    original = fixture.workspace / "report.pdf"
    if kind == "symlink":
        (fixture.workspace / "link.pdf").symlink_to(original)
        path = "link.pdf"
    elif kind == "hardlink":
        os.link(original, fixture.workspace / "link.pdf")
        path = "link.pdf"
    elif kind == "parent":
        path = "../report.pdf"
    else:
        path = str(original)
    with pytest.raises((ValueError, OSError)):
        await fixture.service.parse({"path": path}, fixture.context)
    assert not fixture.api.uploads


async def test_authorization_revocation_blocks_cached_reference_injection(fixture):
    result = await fixture.service.parse({"path": "report.pdf"}, fixture.context)
    fixture.authorized = False
    body = {"messages": [{"role": "user", "content": result["document_ref"]}]}
    with pytest.raises(PermissionError, match="revoked"):
        await fixture.service.with_documents(body, fixture.context)
    with pytest.raises(PermissionError, match="revoked"):
        await fixture.service.parse({"path": "report.pdf"}, fixture.context)
    assert len(fixture.api.uploads) == 1


async def test_cache_receipt_budget_preserves_active_documents(fixture, monkeypatch):
    monkeypatch.setattr(documents, "MAX_RECEIPTS", 1)
    result = await fixture.service.parse({"path": "report.pdf"}, fixture.context)
    (fixture.workspace / "other.pdf").write_bytes(b"other content")
    with pytest.raises(ValueError, match="receipt budget"):
        await fixture.service.parse({"path": "other.pdf"}, fixture.context)
    path = receipt_path(fixture, result)
    receipt = json.loads(path.read_text())
    receipt["expires_at"] = time.time() - 1
    path.write_text(json.dumps(receipt))
    await fixture.service.parse({"path": "other.pdf"}, fixture.context)
    assert not path.exists() and len(fixture.api.uploads) == 2


async def test_cache_receipt_cannot_be_replaced_by_a_symlink(fixture):
    result = await fixture.service.parse({"path": "report.pdf"}, fixture.context)
    path = receipt_path(fixture, result)
    original = path.read_text()
    path.unlink()
    (fixture.workspace / "forged.json").write_text(original)
    path.symlink_to(fixture.workspace / "forged.json")
    body = {"messages": [{"role": "user", "content": result["document_ref"]}]}
    with pytest.raises(OSError):
        await fixture.service.with_documents(body, fixture.context)
