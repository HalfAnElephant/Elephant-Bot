"""Replay discovery and content-reference failures with local tools and the locked real pi."""
import asyncio
import hashlib
import json
import socket
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import pytest
import uvicorn

from qqbot.capabilities import Action, Capability
from qqbot.harness.gateway import MAX_TOOL_RESULT_BYTES, RunGateway
from qqbot.harness.pi import PiRunnerAdapter
from qqbot.jobs.worker import CapabilityServer


class CoreFixture:
    def __init__(self):
        self.calls, self.operations = [], {}
        self.allowed = True

    async def __call__(self, operation, payload):
        self.calls.append((operation, payload))
        if operation == "authorize":
            if not self.allowed:
                raise PermissionError("current policy denied this action")
            return {"allowed": True}
        if operation == "reserve-budget":
            return {"accepted": True, "remaining": 30}
        if operation in {"observation", "usage"}:
            return {"recorded": True}
        if operation == "ledger":
            identity = payload["operation_id"]
            if payload["operation"] == "prepare":
                return self.operations.setdefault(identity, {"state": "PREPARED", "result": {}}).copy()
            entry = self.operations[identity]
            if entry["state"] != payload["expected"]:
                return {"changed": False}
            entry.update(state=payload["state"], result=payload["result"])
            return {"changed": True}
        raise AssertionError(operation)


@pytest.fixture
def discovery(tmp_path):
    core, calls = CoreFixture(), []
    async def github(arguments, context):
        calls.append((arguments, context))
        return {"exit_code": 0, "stdout": json.dumps([
            {"number": 7, "title": "Verified fixture PR", "url": "https://github.com/owner/repo/pull/7"}]),
            "stderr": "", "truncated": False}
    gateway = RunGateway(tmp_path, "offline", "provider-fixture-secret", "https://model.invalid/v1/chat/completions",
                         call=core, github=github, model_window=1000000, content_root=tmp_path / "content")
    gateway.bind({"task_id": "discovery-task", "run_id": "discovery-run", "fencing_token": 3,
        "goal": "看下 yourtj hub 有哪些 PR", "permission_snapshot": {"principal": {"user_id": "real-user"}},
        "repository": "owner/repo", "context": {"seed": "", "limits": {
            "soft_compaction_input_tokens": 150000,
            "keep_recent_tokens": 2000}}})
    return gateway, core, calls


async def invoke(client, gateway, name, arguments, identity="fixture"):
    return await client.post("/tools/" + name, headers={"Authorization": "Bearer " + gateway.tool_token},
                             json={"call_id": identity, "arguments": arguments})


@pytest.mark.parametrize("query", [
    "github pull request list", "github repository search", "repo", "PR", "看下你的仓库有哪些 PR",
    "列出拉取请求", "批准这个合并请求", "GitHub issue comment", "重新运行 CI",
])
def test_natural_keywords_rank_github_without_exact_cli_names(discovery, query):
    gateway, _core, _calls = discovery
    result = gateway.registry.search(query)
    assert result[0]["name"] == "gh.exec"
    assert result[0]["parameters"]["required"] == ["argv"]
    assert all(not entry["name"].startswith("process.") for entry in gateway.registry.search("PR"))


@pytest.mark.parametrize(("query", "expected"), [("天气预报", "web.search"), ("运行 pytest 测试", "process.start"),
                                                ("git push", "git.transport")])
def test_other_domains_remain_discoverable(discovery, query, expected):
    gateway, _core, _calls = discovery
    assert gateway.registry.search(query)[0]["name"] == expected


def test_new_capability_uses_its_own_description_without_alias_registration(discovery):
    gateway, _core, _calls = discovery
    gateway.registry.register(Capability("fixture.invoice", "发票查询 invoice lookup"), lambda a, c: {},
                               lambda a, c: Action("fixture.read", ()))
    assert gateway.registry.search("帮我查询发票")[0]["name"] == "fixture.invoice"
    assert gateway.registry.search("new business invoice lookup")[0]["name"] == "fixture.invoice"


async def test_empty_search_returns_complete_schemas_in_one_response(discovery):
    gateway, core, calls = discovery
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://gateway") as client:
        response = await invoke(client, gateway, "capability_search", {"query": ""})
    assert response.status_code == 200
    result = response.json()
    assert result["status"] == "ok" and result["truncated"] is False
    assert {entry["name"] for entry in result["data"]} == set(gateway.registry.entries)
    assert len(result["data"]) > 10
    gh = next(entry for entry in result["data"] if entry["name"] == "gh.exec")
    assert gh["parameters"]["properties"]["argv"]["items"] == {"type": "string"}
    assert "Full output is available through content_read" not in response.text
    assert not calls and not any(operation == "ledger" for operation, _ in core.calls)


async def test_binding_exposes_host_repository_and_keeps_catalog_on_demand(discovery):
    gateway, _core, _calls = discovery
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://gateway") as client:
        response = await client.post("/binding", json={"binding_id": gateway.binding},
                                     headers={"Authorization": "Bearer " + gateway.process_token})
    result = response.json()
    assert "capability_catalog" not in result
    assert result["repository"] == "owner/repo" and result["github_host"] == "github.com"


async def test_large_discovery_has_valid_reference_and_complete_pageable_result(discovery):
    gateway, _core, _calls = discovery
    for index in range(20):
        gateway.registry.register(Capability(f"fixture.large{index}", "large fixture " + "中" * 2000,
            {"type": "object", "properties": {"literal": {"type": "string"}}}), lambda a, c: {},
            lambda a, c: Action("fixture.read", ()))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://gateway") as client:
        result = (await invoke(client, gateway, "capability_search", {"query": ""})).json()
        assert result["truncated"] is True
        reference = result["content_ref"]
        assert reference.startswith("content:") and len(reference) == 72
        assert len(json.dumps(result, ensure_ascii=False).encode()) < MAX_TOOL_RESULT_BYTES
        text, offset = "", 0
        for page in range(20):
            response = await invoke(client, gateway, "content_read",
                {"reference": reference, "offset": offset, "limit": 65536}, identity="page" + str(page))
            assert response.status_code == 200
            data = response.json()
            text += data["text"]
            if data["next_offset"] is None:
                break
            offset = data["next_offset"]
        complete = json.loads(text)
        assert {entry["name"] for entry in complete["data"]} == set(gateway.registry.entries)
        assert hashlib.sha256(text.encode()).hexdigest() == reference[8:]


async def test_content_reference_larger_than_previous_read_limit_is_readable(discovery):
    gateway, _core, _calls = discovery
    content = json.dumps({"value": "x" * (2 * 1024 * 1024 + 10)}).encode()
    reference = await gateway.store_content(gateway.context, content)
    assert (await gateway.read_content(gateway.context, reference)).encode() == content


async def test_invalid_and_unknown_references_return_actionable_errors(discovery):
    gateway, _core, _calls = discovery
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://gateway") as client:
        invalid = await invoke(client, gateway, "content_read", {"reference": "content_ref:0", "limit": 8000})
        assert invalid.status_code == 422
        assert "use a returned content_ref" in invalid.json()["detail"]
        missing = await invoke(client, gateway, "content_read", {"reference": "content:" + "0" * 64}, identity="missing")
        assert missing.status_code == 422 and "unavailable for this task" in missing.json()["detail"]


async def test_error_details_are_bounded_and_known_secrets_redacted(discovery):
    gateway, _core, _calls = discovery
    def fail(arguments, context):
        raise ValueError("fix parameters " + gateway.key + " " + gateway.tool_token + " Bearer fixture-token " + "x" * 5000)
    gateway.registry.register(Capability("fixture.fail", "fail"), fail, lambda a, c: Action("fixture.read", ()))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://gateway") as client:
        response = await invoke(client, gateway, "capability_invoke", {"name": "fixture.fail", "arguments": {}})
    assert response.status_code == 422
    detail = response.json()["detail"]
    assert len(detail) <= 1000 and "fix parameters" in detail
    assert gateway.key not in detail and gateway.tool_token not in detail and "fixture-token" not in detail


async def test_current_permissions_and_unsafe_write_ledger_still_apply(discovery):
    gateway, core, _calls = discovery
    effects = []
    gateway.registry.register(Capability("fixture.write", "write"), lambda a, c: effects.append(1) or {"accepted": True},
                               lambda a, c: Action("fixture.write", ("task:" + c.task_id,), "unknown"))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://gateway") as client:
        arguments = {"name": "fixture.write", "arguments": {}}
        first = await invoke(client, gateway, "capability_invoke", arguments)
        replay = await invoke(client, gateway, "capability_invoke", arguments)
        assert first.json()["status"] == replay.json()["status"] == "unknown" and effects == [1]
        core.allowed = False
        denied = await invoke(client, gateway, "capability_invoke", arguments, identity="denied")
        assert denied.status_code == 403 and effects == [1]


@asynccontextmanager
async def real_pi(gateway, tmp_path):
    executable = Path(__file__).resolve().parents[1] / "node_modules/.bin/pi"
    if not executable.exists():
        pytest.skip("locked pi 1.5.0 package is not installed")
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(8)
    server = CapabilityServer(uvicorn.Config(gateway.app, log_level="warning", access_log=False))
    server_task = asyncio.create_task(server.serve(sockets=[listener]))
    runner = PiRunnerAdapter(executable, tmp_path, provider="qqbot", model="offline", timeout=25,
        gateway=gateway, gateway_url="http://127.0.0.1:" + str(listener.getsockname()[1]))
    try:
        async with asyncio.timeout(3):
            while not server.started:
                if server_task.done():
                    await server_task
                await asyncio.sleep(0.01)
        yield runner
    finally:
        await runner.stop()
        server.should_exit = True
        await asyncio.wait_for(server_task, 3)
        listener.close()


def sse(message):
    calls = message.get("tool_calls")
    if calls:
        message = {**message, "tool_calls": [{"index": index, **call} for index, call in enumerate(calls)]}
    event = {"id": "fixture", "object": "chat.completion.chunk", "model": "offline", "created": 1,
             "choices": [{"index": 0, "delta": message, "finish_reason": None}]}
    finish = {**event, "choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls" if calls else "stop"}],
              "usage": {"prompt_tokens": 1000, "completion_tokens": 40}}
    return "data: " + json.dumps(event) + "\n\ndata: " + json.dumps(finish) + "\n\ndata: [DONE]\n\n"


async def test_real_pi_discovers_github_then_invokes_it(discovery, tmp_path):
    gateway, _core, calls = discovery
    requests = []
    def upstream(request):
        payload = json.loads(request.content)
        requests.append(payload)
        if len(requests) == 1:
            system = payload["messages"][0]["content"]
            assert "capability_search" in system
            assert "owner/repo" in str(payload["messages"])
            message = {"role": "assistant", "tool_calls": [{"id": "fixture_search", "type": "function", "function": {
                "name": "capability_search", "arguments": json.dumps({"query": "github PR"})}}]}
        elif len(requests) == 2:
            assert "gh.exec" in str(payload["messages"])
            message = {"role": "assistant", "tool_calls": [{"id": "fixture_gh", "type": "function", "function": {
                "name": "capability_invoke", "arguments": json.dumps({"name": "gh.exec", "arguments": {
                    "argv": ["pr", "list", "--json", "number,title,url"]}})}}]}
        else:
            assert "Verified fixture PR" in str(payload["messages"])
            message = {"role": "assistant", "content": "There is one fixture PR, #7."}
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, text=sse(message))
    gateway.transport = httpx.MockTransport(upstream)
    gateway.provider.transport = gateway.transport
    async with real_pi(gateway, tmp_path) as runner:
        result = await runner.run("trusted goal")
    assert result.summary == "There is one fixture PR, #7."
    assert len(calls) == 1 and len(requests) == 3
    assert calls[0][1].repository == "owner/repo" and calls[0][1].principal == {"user_id": "real-user"}


async def test_real_pi_receives_bounded_actionable_gateway_error(discovery, tmp_path):
    gateway, _core, calls = discovery
    requests = []
    def upstream(request):
        payload = json.loads(request.content)
        requests.append(payload)
        if len(requests) == 1:
            message = {"role": "assistant", "tool_calls": [{"id": "fixture_bad_ref", "type": "function", "function": {
                "name": "content_read", "arguments": json.dumps({"reference": "content_ref:0", "limit": 8000})}}]}
        else:
            results = [entry for entry in payload["messages"] if entry.get("role") == "tool"]
            assert "invalid content reference" in str(results)
            assert "use a returned content_ref" in str(results)
            assert gateway.tool_token not in str(results) and gateway.key not in str(results)
            message = {"role": "assistant", "content": "The reference was invalid and can be corrected."}
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, text=sse(message))
    gateway.transport = httpx.MockTransport(upstream)
    gateway.provider.transport = gateway.transport
    async with real_pi(gateway, tmp_path) as runner:
        result = await runner.run("trusted goal")
    assert result.summary == "The reference was invalid and can be corrected."
    assert len(requests) == 2 and not calls


@pytest.mark.parametrize("kind", ["host", "missing_file"])
async def test_native_host_and_workspace_errors_are_bounded_actionable_results(discovery, kind):
    gateway, _core, _calls = discovery
    if kind == "host":
        async def reject(operation, payload):
            if operation == "skills":
                response = httpx.Response(422, json={"detail": "Invalid skill call identity. " + gateway.key},
                    request=httpx.Request("POST", "http://runtime.local/skills"))
                response.raise_for_status()
            return {"accepted": True, "allowed": True}
        gateway.call = reject
        name, arguments = "skills_list", {}
    else:
        gateway.notify_before_tools = False
        name, arguments = "capability_invoke", {"name": "workspace.read", "arguments": {"path": "missing.txt"}}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://gateway") as client:
        response = await invoke(client, gateway, name, arguments)
    assert response.status_code == 422
    assert gateway.key not in response.text
    assert ("Invalid skill call identity" if kind == "host" else "missing.txt") in response.json()["detail"]


async def test_finalizer_cannot_export_a_child_path_as_its_own_file(discovery):
    gateway, _core, _calls = discovery
    claim = gateway.claim
    gateway.revoke()
    gateway.bind({**claim, "role": "leader", "purpose": "finalize"})
    assert "答复前看到 WAITING_CHILDREN 不是结算失败" in gateway.system_prompt
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://gateway") as client:
        response = await invoke(client, gateway, "task_outcome", {"status": "completed", "attachments": ["outputs/child.txt"]})
    assert response.status_code == 422 and "verified child artifacts" in response.json()["detail"]
    assert not gateway.artifacts
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://gateway") as client:
        response = await invoke(client, gateway, "task_outcome", {"status": "completed"})
    assert response.json()["accepted"] and response.json()["committed"] is False
    assert "end this turn" in response.json()["next_action"]


async def test_output_sidecar_stays_workspace_file_without_breaking_artifact_export(discovery):
    from qqbot.harness.artifacts import export_artifacts
    gateway, _core, _calls = discovery
    gateway.notify_before_tools = False
    (gateway.workspace / "outputs").mkdir()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://gateway") as client:
        for path, text in [("outputs/.keep", ""), ("outputs/result.txt", "ISOLATED_OK")]:
            response = await invoke(client, gateway, "capability_invoke",
                {"name": "workspace.write", "arguments": {"path": path, "text": text}}, identity=path)
            assert response.status_code == 200, response.text
        assert gateway.artifacts == ["outputs/result.txt"]
        rejected = await invoke(client, gateway, "task_outcome", {"status": "completed", "attachments": ["outputs/.keep"]}, identity="bad-artifact")
        assert rejected.status_code == 422 and "unsupported artifact type" in rejected.text
        assert gateway.outcome is None
        accepted = await invoke(client, gateway, "task_outcome", {"status": "completed", "attachments": ["outputs/result.txt"]}, identity="good-artifact")
        assert accepted.status_code == 200
    assert (gateway.workspace / "outputs/.keep").is_file()
    archive = gateway.workspace / "archive"
    artifacts = export_artifacts(gateway.workspace, archive, gateway.artifacts)
    assert len(artifacts) == 1 and artifacts[0]["relative_path"] == "outputs/result.txt"
    assert (archive / artifacts[0]["id"]).read_bytes() == b"ISOLATED_OK"
