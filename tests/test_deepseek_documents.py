import asyncio
import json
import os
import time
from types import SimpleNamespace

import httpx
import pytest
from test_provider_gateway import FakeCore

from qqbot.adapters.deepseek import DeepSeekError
from qqbot.adapters.execution.world import LocalExecution
from qqbot.capabilities import CapabilityRegistry, ToolContext
from qqbot.harness import documents
from qqbot.harness.documents import REFERENCE, DeepSeekDocuments
from qqbot.harness.gateway import RunGateway
from qqbot.permissions import PermissionPolicy, PermissionRule, PermissionService
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
        checked=[], resource_checks=[], content_root=tmp_path / "content")

    async def authorize(action, *, resources=None):
        state.checked.append(action)
        state.resource_checks.append((action, resources))
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
    assert all(resources == action.resources for checked, resources in fixture.resource_checks if checked == "document.parse")
    assert all(resources is None for checked, resources in fixture.resource_checks if checked == "model.request")


async def test_ocr_reads_original_without_changing_durable_document_receipts(fixture):
    parsed = await fixture.service.parse({"path": "report.pdf"}, fixture.context)
    before = receipt_path(fixture, parsed).read_bytes()
    calls = []
    expected = {"purpose": "text_recognition", "page_count": 1,
        "pages": [{"index": 0, "text": "Original document text", "finish_reason": "length"}]}

    async def recognize(data, filename):
        calls.append((data, filename))
        return expected

    fixture.api.ocr = recognize
    registry = CapabilityRegistry()
    fixture.service.register(registry)
    capability, execute, classify = registry.entries["document.ocr"]
    action = classify({"path": "report.pdf"}, fixture.context)
    assert action.name == "document.ocr" and action.effect == "read"
    assert action.resources == fixture.service.classify({"path": "report.pdf"}, fixture.context).resources
    assert capability.timeout_seconds > 220
    assert await execute({"path": "report.pdf"}, fixture.context) == expected
    assert calls == [(b"%PDF fixture", "document.pdf")]
    assert receipt_path(fixture, parsed).read_bytes() == before
    assert all(resources == action.resources for checked, resources in fixture.resource_checks if checked == "document.ocr")


@pytest.mark.parametrize("path", ["../outside.pdf", "/etc/passwd", "linked.pdf", "empty.pdf", "large.pdf"])
async def test_ocr_rejects_unreadable_and_oversized_sources_before_network(fixture, monkeypatch, path):
    monkeypatch.setattr(documents, "MAX_FILE_BYTES", 3)
    (fixture.workspace.parent / "outside.pdf").write_bytes(b"abc")
    (fixture.workspace / "linked.pdf").symlink_to(fixture.workspace.parent / "outside.pdf")
    (fixture.workspace / "empty.pdf").write_bytes(b"")
    (fixture.workspace / "large.pdf").write_bytes(b"four")
    with pytest.raises((ValueError, OSError)):
        await fixture.service.ocr({"path": path}, fixture.context)
    assert not list(fixture.service.root.glob("*.json"))


@pytest.mark.parametrize("phase", ["before_read", "after_read", "after_ocr"])
async def test_ocr_rechecks_authorization_and_scope(fixture, monkeypatch, phase):
    calls = []
    original = fixture.service.execution.read_file

    async def read(*args):
        data = await original(*args)
        if phase == "after_read":
            fixture.scope = {**fixture.scope, "conversation": "changed-scope"}
        return data

    async def recognize(data, filename):
        calls.append((data, filename))
        fixture.authorized = False
        return {"page_count": 1, "pages": []}

    monkeypatch.setattr(fixture.service.execution, "read_file", read)
    fixture.api.ocr = recognize
    fixture.authorized = phase != "before_read"
    with pytest.raises(PermissionError):
        await fixture.service.ocr({"path": "report.pdf"}, fixture.context)
    assert bool(calls) == (phase == "after_ocr")


async def test_gateway_ocr_archives_large_text_with_scoped_resource_grants(fixture):
    resources = ("workspace:" + str(fixture.workspace), "file:report.pdf", "domain:api.deepseek.com")
    permissions = PermissionService(policy=PermissionPolicy(preset="custom", default_effect="deny", rules=(
        PermissionRule(id="ocr", effect="allow", actions=("document.ocr",), resources=resources),
    )))
    core = FakeCore()
    expected = {"purpose": "text_recognition", "page_count": 1,
        "pages": [{"index": 0, "text": "Recognized original text " * 2000, "finish_reason": "stop"}]}

    async def call(operation, payload):
        if operation == "authorize":
            for resource in payload.get("resources") or ():
                permissions.require({}, payload["action"], resource)
        return await core(operation, payload)

    async def recognize(data, filename):
        assert data == b"%PDF fixture" and filename == "document.pdf"
        return expected

    fixture.api.ocr = recognize
    gateway = RunGateway(fixture.workspace, "deepseek-flash", "fixture-key", "https://api.deepseek.com/chat/completions",
        call=call, content_root=fixture.content_root)
    gateway.bind({"task_id": "document-task", "run_id": "document-run", "fencing_token": 1, "goal": "read document",
        "permission_snapshot": {"principal": fixture.scope["principal"]},
        "context": {"scope": fixture.scope["conversation"], "workspace_id": fixture.scope["workspace_id"]}})
    gateway.documents.api = fixture.api
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://gateway") as client:
        response = await client.post("/tools/capability_invoke", headers={"Authorization": "Bearer " + gateway.tool_token},
            json={"call_id": "ocr", "arguments": {"name": "document.ocr", "arguments": {"path": "report.pdf"}}})
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["truncated"] and result["content_ref"]
    assert json.loads(await gateway.read_content(gateway.context, result["content_ref"])) == expected
    checked = [payload for operation, payload in core.calls
        if operation == "authorize" and payload["action"] == "document.ocr"]
    assert len(checked) >= 3 and all(tuple(payload["resources"]) == resources for payload in checked)
    assert not list(gateway.documents.root.glob("*.json"))


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
    attached = await fixture.service.with_documents(body, fixture.context)
    assert not references(attached)
    assert "已过期或不可用" in attached["messages"][0]["content"][1]["text"]
    assert result["file_id"] not in json.dumps(attached)
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
    resources = ("workspace:" + str(fixture.workspace), "file:report.pdf", "domain:api.deepseek.com")
    assert all(actual == resources for action, actual in fixture.resource_checks if action == "document.parse")


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


async def test_missing_historical_references_do_not_block_the_model(fixture):
    body = {"messages": [{"role": "user", "content": " ".join(REFERENCE + f"{number:064x}" for number in range(21))}]}
    attached = await fixture.service.with_documents(body, fixture.context)
    assert not references(attached)
    assert json.dumps(attached, ensure_ascii=False).count("已过期或不可用") == 21


@pytest.mark.parametrize("user_input", ["继续普通聊天", "压缩并总结上面的对话"])
async def test_recent_twenty_valid_documents_survive_unavailable_history(fixture, user_input):
    valid = []
    for number in range(21):
        (fixture.workspace / "report.pdf").write_bytes(("document " + str(number)).encode())
        valid.append(await fixture.service.parse({"path": "report.pdf"}, fixture.context))
    (fixture.workspace / "report.pdf").write_bytes(b"expired document")
    expired = await fixture.service.parse({"path": "report.pdf"}, fixture.context)
    path = receipt_path(fixture, expired)
    receipt = json.loads(path.read_text())
    receipt["expires_at"] = time.time() - 1
    path.write_text(json.dumps(receipt))
    original_scope = fixture.scope
    fixture.scope = {**original_scope, "conversation": "foreign-private"}
    (fixture.workspace / "report.pdf").write_bytes(b"private foreign document")
    foreign = await fixture.service.parse({"path": "report.pdf"}, fixture.context)
    fixture.scope = original_scope
    missing = REFERENCE + "0" * 64
    body = {"messages": [
        {"role": "system", "content": "trusted system"},
        {"role": "user", "content": "\n".join([result["document_ref"] for result in valid] +
            [expired["document_ref"], foreign["document_ref"], missing])},
        {"role": "assistant", "content": "这些文档已记录。"},
        {"role": "user", "content": user_input},
    ]}
    attached = await fixture.service.with_documents(body, fixture.context)
    assert references(attached) == [result["file_id"] for result in reversed(valid[1:])]
    injected = attached["messages"][1]["content"]
    assert any(valid[0]["document_ref"] in part.get("text", "") and "本次省略" in part["text"] for part in injected)
    assert sum("已过期或不可用" in part.get("text", "") for part in injected) == 3
    assert foreign["file_id"] not in json.dumps(attached)
    validate_transactions(attached["messages"])
    followup = {"messages": [*body["messages"], {"role": "user", "content": "重新使用 " + valid[0]["document_ref"]}]}
    selected = references(await fixture.service.with_documents(followup, fixture.context))
    assert selected == [valid[0]["file_id"], *[result["file_id"] for result in reversed(valid[2:])]]


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


@pytest.mark.parametrize("request_kind", ["business", "compaction"])
async def test_gateway_document_resource_grants_and_long_history_allow_model_requests(fixture, request_kind):
    resources = ("workspace:" + str(fixture.workspace), "file:report.pdf", "domain:api.deepseek.com")
    conversation = "conversation:" + fixture.scope["conversation"]
    permissions = PermissionService(policy=PermissionPolicy(preset="custom", default_effect="deny", rules=(
        PermissionRule(id="runtime", effect="allow", actions=("task.execute", "model.request"), resources=(conversation,)),
        PermissionRule(id="document", effect="allow", actions=("document.parse",), resources=resources),
    )))
    core, model_requests = FakeCore(), []

    async def call(operation, payload):
        if operation == "authorize":
            for resource in payload.get("resources") or (conversation,):
                permissions.require({}, payload["action"], resource)
        return await core(operation, payload)

    def upstream(request):
        assert request.url.path == "/chat/completions"
        model_requests.append(json.loads(request.content))
        return httpx.Response(200, json={"choices": [{"message": {"role": "assistant", "content": "done"}}]})

    gateway = RunGateway(fixture.workspace, "deepseek-flash", "fixture-key", "https://api.deepseek.com/chat/completions",
        call=call, content_root=fixture.content_root, transport=httpx.MockTransport(upstream))
    gateway.bind({"task_id": "document-task", "run_id": "document-run", "fencing_token": 1, "goal": "read documents",
        "role": "leader", "permission_snapshot": {"principal": fixture.scope["principal"]},
        "context": {"scope": fixture.scope["conversation"], "workspace_id": fixture.scope["workspace_id"]}})
    gateway.documents.api = fixture.api
    assert not permissions.check({}, "document.parse", conversation).allowed
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://gateway") as client:
        result = await client.post("/tools/capability_invoke", headers={"Authorization": "Bearer " + gateway.tool_token},
            json={"call_id": "parse", "arguments": {"name": "document.parse", "arguments": {"path": "report.pdf"}}})
        assert result.status_code == 200, result.text
        parsed = [result.json()["data"]]
        for number in range(20):
            (fixture.workspace / "report.pdf").write_bytes(("next document " + str(number)).encode())
            parsed.append(await gateway.documents.parse({"path": "report.pdf"}, gateway.context))
        gateway.provider.request_kind = request_kind
        response = await client.post("/v1/chat/completions", headers={"Authorization": "Bearer " + gateway.model_token},
            json={"model": gateway.model, "messages": [
                {"role": "user", "content": "\n".join(result["document_ref"] for result in parsed)},
                {"role": "user", "content": "summarize" if request_kind == "compaction" else "continue chatting"},
            ]})
        assert response.status_code == 200, response.text
    assert references(model_requests[0]) == [result["file_id"] for result in reversed(parsed[1:])]
    document_checks = [payload for operation, payload in core.calls
                       if operation == "authorize" and payload["action"] == "document.parse"]
    assert len(document_checks) >= 4 and all(tuple(payload["resources"]) == resources for payload in document_checks)
    model_checks = [payload for operation, payload in core.calls
                    if operation == "authorize" and payload["action"] == "model.request"]
    assert model_checks and all(payload.get("resources") is None for payload in model_checks)
