import importlib.util
import re
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI

from qqbot.capabilities.types import ToolResult

spec = importlib.util.spec_from_file_location(
    "deepseek_tools_live", Path(__file__).parents[1] / "evals/deepseek_tools_live.py"
)
live = importlib.util.module_from_spec(spec)
spec.loader.exec_module(live)


@pytest.mark.parametrize("files_read_passed,ocr_correct", [(True, True), (False, True), (False, False)])
async def test_document_acceptance_records_files_failure_and_checks_ocr(monkeypatch, files_read_passed, ocr_correct):
    instances = []

    class Gateway:
        def __init__(self, workspace, *args, **kwargs):
            instances.append(self)
            self.workspace = workspace
            self.context = None
            self.tools = SimpleNamespace(invoke=self.invoke)
            self.deepseek = SimpleNamespace(_request=self.cleanup)
            self.model_token = "fixture-model"
            self.process_token = "fixture-process"
            self.binding = "fixture-binding"
            self.file_content = ["voice test"]
            self.app = FastAPI()
            self.app.post("/v1/chat/completions")(self.complete)
            self.app.post("/binding")(self.prepare)
            self.model_messages = []

        def bind(self, claim):
            self.claim = claim

        async def invoke(self, name, arguments, context):
            if name == "web.search":
                data = {"results": ["fixture"]}
            elif name == "web.fetch":
                data = {"text": "语音识别"}
            elif name == "document.parse":
                self.code = re.search(rb"Verification code ([A-Z0-9]+)",
                    (self.workspace / "input.pdf").read_bytes())[1].decode()
                data = {"file_id": "file-api-fixture", "document_ref": "deepseek-document:fixture",
                    "purpose": "text_recognition"}
            else:
                assert name == "document.ocr" and arguments == {"path": "input.pdf"}
                data = {"purpose": "text_recognition", "page_count": 1,
                    "pages": [{"index": 0, "text": "Verification code " + (self.code if ocr_correct else "WRONG"),
                        "finish_reason": "stop"}]}
            return ToolResult("ok", data)

        async def complete(self, body: dict):
            text = body["messages"][0]["content"]
            self.model_messages.append(text)
            if len(self.model_messages) == 1:
                answer = self.code if files_read_passed else "The file content is empty."
            elif "document.ocr" in text:
                answer = re.search(r"Verification code ([A-Z0-9]+)", text)[1]
            else:
                answer = "voice test"
            return {"choices": [{"message": {"content": answer}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 50}}

        async def prepare(self, body: dict):
            return {"audio_context": "voice test"}

        async def cleanup(self, method, path):
            assert method == "DELETE" and path == "/files/file-api-fixture"
            return {"deleted": True}

    monkeypatch.setenv("DEEPSEEK_API_KEY", "fixture-key")
    monkeypatch.setattr(live, "RunGateway", Gateway)
    report = {}
    if ocr_correct:
        await live.exercise(report)
        assert report["document"]["passed"] and report["ocr"]["passed"]
        assert report["document"]["ocr_fallback_used"] == (not files_read_passed)
        assert len(instances[0].model_messages) == (2 if files_read_passed else 3)
        if not files_read_passed:
            assert report["document"]["answer"] == "The file content is empty."
            assert report["document"]["fallback_answer"] == report["document"]["expected_code"]
    else:
        with pytest.raises(AssertionError, match="OCR did not read"):
            await live.exercise(report)
        assert not report["document"]["passed"] and not report["ocr"]["passed"]
    assert report["document"]["files_read_passed"] is files_read_passed
    assert report["document_cleanup"]
