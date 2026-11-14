"""Real Search, Fetch, document and spoken SILK acceptance with synthetic inputs."""
import asyncio
import json
import os
import tempfile
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import httpx

from qqbot.harness.gateway import RunGateway

MODEL = "deepseek-flash"
ENDPOINT = "https://api.deepseek.com/chat/completions"
REPORT = Path("test-results/deepseek-tools-live.json")


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def document_pdf(code):
    text = f"BT /F1 18 Tf 40 160 Td (Verification code {code}) Tj ET".encode()
    objects = [b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        (b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 600 240] "
         b"/Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>"),
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        b"<< /Length " + str(len(text)).encode() + b" >>\nstream\n" + text + b"\nendstream"]
    result, offsets = bytearray(b"%PDF-1.4\n"), [0]
    for index, value in enumerate(objects, 1):
        offsets.append(len(result))
        result.extend(str(index).encode() + b" 0 obj\n" + value + b"\nendobj\n")
    start = len(result)
    result.extend(f"xref\n0 {len(offsets)}\n0000000000 65535 f \n".encode())
    for offset in offsets[1:]:
        result.extend(f"{offset:010d} 00000 n \n".encode())
    result.extend(f"trailer\n<< /Size {len(offsets)} /Root 1 0 R >>\nstartxref\n{start}\n%%EOF\n".encode())
    return bytes(result)


async def tool_data(gateway, result):
    if result.truncated:
        require(bool(result.content_ref), "truncated tool result has no content reference")
        return json.loads(await gateway.read_content(gateway.context, result.content_ref))
    return result.data


async def exercise(report):
    key = os.environ.get("DEEPSEEK_API_KEY", "")
    require(bool(key), "DEEPSEEK_API_KEY is required")
    async def call(operation, payload):
        if operation == "authorize":
            return {"allowed": True}
        if operation == "reserve-budget":
            return {"remaining": 20}
        if operation == "usage":
            return {"recorded": True}
        raise AssertionError("unexpected fixture host operation")

    with tempfile.TemporaryDirectory(prefix="deepseek-tools-live-") as temporary:
        root = Path(temporary)
        gateway = RunGateway(root / "workspace", MODEL, key, ENDPOINT, call=call, content_root=root / "content")
        gateway.workspace.mkdir()
        gateway.bind({"task_id": "fixture", "run_id": "fixture", "fencing_token": 1, "goal": "acceptance",
            "permission_snapshot": {"principal": "synthetic-owner"},
            "context": {"scope": "synthetic-private", "workspace_id": "personal"}})
        uploaded = None
        try:
            search = await gateway.tools.invoke("web.search", {"query": "DeepSeek API documentation"}, gateway.context)
            search_data = await tool_data(gateway, search)
            report["search"] = {"results": len(search_data.get("results", [])), "truncated": search.truncated}
            require(bool(search_data.get("results")), "Search returned no structured results")
            report["search"]["passed"] = True
            page = await gateway.tools.invoke("web.fetch", {
                "url": "https://api-docs.deepseek.com/zh-cn/guides/asr/"}, gateway.context)
            page_data = await tool_data(gateway, page)
            require("语音识别" in page_data["text"], "Fetch did not extract the official ASR page")
            report["fetch"] = {"passed": True, "characters": len(page_data["text"]), "truncated": page.truncated}
            code = "ELEPHANT" + uuid4().hex[:12].upper()
            (gateway.workspace / "input.pdf").write_bytes(document_pdf(code))
            parsed = await gateway.tools.invoke("document.parse", {"path": "input.pdf"}, gateway.context)
            uploaded = parsed.data["file_id"]
            again = await gateway.tools.invoke("document.parse", {"path": "input.pdf"}, gateway.context)
            require(again.data["file_id"] == uploaded, "document upload was not reused")
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://fixture") as client:
                response = await client.post("/v1/chat/completions", headers={
                    "Authorization": "Bearer " + gateway.model_token}, json={"model": MODEL, "messages": [{
                    "role": "user", "content": parsed.data["document_ref"] + "\nReturn only the verification code in this document."}]})
                require(response.status_code == 200, "file model request failed")
                answer = response.json()["choices"][0]["message"]["content"]
                require(code.casefold() in answer.casefold(), "model did not read the uploaded PDF's verification code")
                report["document"] = {"passed": True, "purpose": parsed.data["purpose"], "reused_upload": True}
                speech = (Path(__file__).resolve().parents[1] / "tests/fixtures/voice-input.silk").read_bytes()
                gateway.claim["context"]["files"] = [{"source": "voice-fixture", "message_id": "voice",
                    "kind": "audio", "filename": "voice.silk", "url": "https://fixture.example/voice.silk",
                    "is_current_input": True, "quoted": False}]
                with patch("qqbot.harness.gateway.fetch_public_bytes", return_value=speech):
                    binding = await client.post("/binding", headers={
                        "Authorization": "Bearer " + gateway.process_token}, json={"binding_id": gateway.binding})
                    require(binding.status_code == 200, "voice input preparation failed")
                    response = await client.post("/v1/chat/completions", headers={
                        "Authorization": "Bearer " + gateway.model_token}, json={"model": MODEL,
                        "messages": [{"role": "user", "content": "Repeat the user's spoken sentence.\n" + binding.json()["audio_context"]}]})
                require(response.status_code == 200, "voice model request failed")
                transcript = "\n".join(gateway.file_content).casefold()
                require("voice" in transcript and "test" in transcript, "ASR did not recognize the spoken SILK fixture")
                report["voice"] = {"passed": True, "format": "Tencent SILK", "synthetic_spoken_fixture": True}
        finally:
            if uploaded:
                result = await gateway.deepseek._request("DELETE", "/files/" + uploaded)
                require(result.get("deleted") is True, "live fixture document cleanup failed")
                report["document_cleanup"] = True


async def main():
    report = {"passed": False}
    try:
        async with asyncio.timeout(600):
            await exercise(report)
        report["passed"] = True
    except Exception as error:
        report["error_type"] = type(error).__name__
        raise
    finally:
        REPORT.parent.mkdir(exist_ok=True)
        REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2))
        print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
