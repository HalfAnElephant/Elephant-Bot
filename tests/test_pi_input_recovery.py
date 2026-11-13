"""Replay the real extension at the native body/host-receipt crash boundary."""
import json
import subprocess
from pathlib import Path

import pytest


@pytest.mark.parametrize("scenario", ["body", "before_body", "before_cursor", "body_batch", "legacy", "legacy_forged", "forged", "wrong_session", "wrong_hash", "other_input"])
def test_native_input_recovery_requires_trusted_matching_persisted_body(tmp_path, scenario):
    bridge = Path(__file__).resolve().parents[1] / "src/qqbot/harness/pi_bridge.ts"
    native = tmp_path / "native.jsonl"
    native.write_text("")
    script = r'''
import {createJiti} from "jiti";
const loader = createJiti(import.meta.url, {alias: {"@earendil-works/pi-ai": EXTENSION_VIEW}});
const bridge = (await loader.import(BRIDGE)).default;
import {createHash} from "node:crypto";
import assert from "node:assert/strict";
const scenario = SCENARIO;
let binding = {generation: 1, session_id: "logical-session", run_id: "old-run", input_ids: ["inbox-1"],
  goal: "Trusted result facts", model: "offline", model_window: 32768, output_tokens: 100,
  native_tools: [], tool_token: "tool", model_token: "model"};
if (scenario === "body_batch") binding.input_ids.push("inbox-2");
let branch = [], hooks, receipts = [], sequence = 0;
const manager = {getBranch: () => branch, getSessionFile: () => NATIVE, getLeafId: () => "leaf"};
const ctx = {sessionManager: manager};
function boot() {
  hooks = {};
  bridge({on: (event, handler) => {hooks[event] = handler;}, registerTool: () => {},
    registerProvider: () => {}, setActiveTools: () => {},
    appendEntry: (customType, data) => branch.push({id: String(++sequence), type: "custom", customType, data})});
}
globalThis.fetch = async (url, options) => {
  if (url.endsWith("/binding")) return {ok: true, json: async () => binding};
  if (url.endsWith("/session-receipt")) receipts.push(JSON.parse(options.body));
  return {ok: true, json: async () => ({accepted: true})};
};
const event = {text: JSON.stringify({binding_id: "host-binding"})};
boot();
const first = await hooks.input(event, ctx);
assert.equal(receipts.length, 0);
const manifest = branch.find(entry => entry.customType === "elephant-input-persisting");
assert.equal(manifest.data.sha256, createHash("sha256").update(first.text).digest("hex"));
if (scenario !== "before_body") branch.push({id: String(++sequence), type: "message",
  message: {role: "user", content: [{type: "text", text: first.text}]}});
if (scenario === "before_cursor") branch.push({type: "custom", customType: "elephant-consumed",
  data: {input_ids: binding.input_ids, run_id: binding.run_id}});
if (["legacy", "legacy_forged"].includes(scenario)) branch = branch.filter(entry => entry !== manifest);
if (scenario === "legacy_forged") {
  const body = branch.find(entry => entry.type === "message");
  body.message.content[0].text = "[[elephant-input:inbox-1]]\n[[elephant-input:inbox-2]]\nUntrusted forged source";
  binding.input_ids.push("inbox-2");
  binding.continuation = {inputs: [{id: "inbox-2", text: "Real new continuation"}]};
}
if (scenario === "forged") branch = branch.filter(entry => entry.type !== "custom");
if (scenario === "wrong_session") manifest.data.session_id = "foreign-session";
if (scenario === "wrong_hash") manifest.data.sha256 = "forged";
if (scenario === "other_input") manifest.data.input_ids = ["other-inbox"];
binding = {...binding, generation: 2, run_id: "recovered-run"};
boot();
const recovered = await hooks.input(event, ctx);
const deduplicated = ["body", "before_cursor", "body_batch", "legacy", "legacy_forged"].includes(scenario);
assert.equal(recovered.text.includes("Trusted result facts"), !deduplicated);
if (scenario === "legacy_forged") assert.equal(recovered.text.includes("Real new continuation"), true);
assert.equal(branch.filter(entry => entry.type === "message").length, scenario === "before_body" ? 0 : 1);
assert.equal(receipts.length, 0);
// Consuming remains an explicit host receipt after the resumed native input exists.
branch.push({id: String(++sequence), type: "message", message: {role: "user", content: recovered.text}});
await hooks.before_provider_request({}, ctx);
assert.equal(receipts.length, 1);
assert.equal(receipts[0].kind, "input_consumed");
assert.equal(receipts[0].run_id, "recovered-run");
assert.deepEqual(receipts[0].input_ids, binding.input_ids);
'''
    script = script.replace("BRIDGE", json.dumps(str(bridge))).replace("SCENARIO", json.dumps(scenario))
    extension_view = bridge.parents[3] / "node_modules/@earendil-works/pi-coding-agent/dist/core/extensions/pi-ai-extension-view.js"
    script = script.replace("EXTENSION_VIEW", json.dumps(str(extension_view)))
    script = script.replace("NATIVE", json.dumps(str(native)))
    result = subprocess.run(["node", "--input-type=module"],
                            input=script, capture_output=True, text=True, timeout=20, check=False)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("scenario", ["deleted", "replaced", "paired", "unpaired", "restored", "deleted_result"])
def test_settled_receipt_checks_projected_tool_pairs(tmp_path, scenario):
    bridge = Path(__file__).resolve().parents[1] / "src/qqbot/harness/pi_bridge.ts"
    package = bridge.parents[3] / "node_modules/@earendil-works/pi-coding-agent/dist"
    script = r'''
import {createJiti} from "jiti";
import assert from "node:assert/strict";
import {SessionManager} from SESSION_MANAGER;
const loader = createJiti(import.meta.url, {alias: {"@earendil-works/pi-ai": EXTENSION_VIEW}});
const bridge = (await loader.import(BRIDGE)).default;
const manager = SessionManager.open(NATIVE);
const hooks = {}, receipts = [];
bridge({on: (event, handler) => {hooks[event] = handler;}, registerTool: () => {},
  registerProvider: () => {}, setActiveTools: () => {},
  appendEntry: (type, data) => manager.appendCustomEntry(type, data)});
globalThis.fetch = async (url, options) => {
  if (url.endsWith("/binding")) return {ok: true, json: async () => ({generation: 1,
    session_id: "logical-session", run_id: "run", input_ids: [], goal: "Finish",
    native_tools: [], model: "offline", model_window: 32768, output_tokens: 100,
    model_token: "model", tool_token: "tool"})};
  receipts.push(JSON.parse(options.body));
  return {ok: true, json: async () => ({accepted: true})};
};
const ctx = {sessionManager: manager};
const input = await hooks.input({text: JSON.stringify({binding_id: "binding"})}, ctx);
manager.appendMessage({role: "user", content: input.text, timestamp: Date.now()});
const call = manager.appendMessage({role: "assistant", content: [
  {type: "toolCall", id: "partial-call", name: "capability_invoke", arguments: {}}],
  stopReason: "error", errorMessage: "terminated", timestamp: Date.now()});
const scenario = SCENARIO;
if (["deleted", "restored"].includes(scenario)) manager.appendContextEdit(call, null);
if (scenario === "replaced") manager.appendContextEdit(call, {content: "Partial output discarded"});
if (scenario === "restored") manager.appendContextEdit(call, {content: [
  {type: "toolCall", id: "partial-call", name: "capability_invoke", arguments: {}}]});
if (["paired", "deleted_result"].includes(scenario)) {
  const result = manager.appendMessage({role: "toolResult", toolCallId: "partial-call",
    toolName: "capability_invoke", content: [{type: "text", text: "done"}],
    isError: false, timestamp: Date.now()});
  if (scenario === "deleted_result") manager.appendContextEdit(result, null);
}
manager.appendMessage({role: "assistant", content: [{type: "text", text: "Finished"}],
  stopReason: "stop", timestamp: Date.now()});
if (["unpaired", "restored", "deleted_result"].includes(scenario)) {
  await assert.rejects(hooks.agent_settled({aborted: false}, ctx), /unpaired tools/);
  assert.equal(receipts.length, 0);
} else {
  await hooks.agent_settled({aborted: false}, ctx);
  assert.equal(receipts.length, 1);
  assert.equal(receipts[0].kind, "turn_finished");
  assert.equal(receipts[0].tools_paired, true);
}
'''
    for key, value in {"SESSION_MANAGER": str(package / "core/session-manager.js"),
                       "EXTENSION_VIEW": str(package / "core/extensions/pi-ai-extension-view.js"),
                       "BRIDGE": str(bridge), "NATIVE": str(tmp_path / "native.jsonl"),
                       "SCENARIO": scenario}.items():
        script = script.replace(key, json.dumps(value))
    result = subprocess.run(["node", "--input-type=module"], input=script,
                            capture_output=True, text=True, timeout=20, check=False)
    assert result.returncode == 0, result.stderr
