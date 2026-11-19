"""Native Pi log export. Only this adapter parses the Harness's file format."""
import hashlib
import json
import shutil
import subprocess
from pathlib import Path

from elephant.contracts.memory import SourceEvent


def load_entries(path):
    path = Path(path)
    if not path.exists():
        return []
    if path.is_symlink() or path.stat().st_size > 256 * 1024 * 1024:
        raise ValueError("invalid or oversized Pi session")
    entries = []
    with path.open() as source:
        for line in source:
            if not line.endswith("\n") or len(line.encode()) > 16 * 1024 * 1024:
                raise ValueError("truncated or oversized Pi session entry")
            entry = json.loads(line)
            if not isinstance(entry, dict):
                raise ValueError("invalid Pi session entry")  # noqa: TRY004 — stable native-log validation contract
            entries.append(entry)
    return entries


def branch_entries(entries):
    indexed = {entry["id"]: entry for entry in entries if "id" in entry and entry.get("type") != "session"}
    leaf = next((entry for entry in reversed(entries) if entry.get("id") in indexed), None)
    branch, seen = [], set()
    while leaf:
        if leaf["id"] in seen:
            raise ValueError("cyclic Pi session branch")
        seen.add(leaf["id"])
        branch.append(leaf)
        parent = leaf.get("parentId")
        if parent and parent not in indexed:
            raise ValueError("Pi session branch has a missing ancestor")
        leaf = indexed.get(parent)
    return list(reversed(branch))


def _export_session_v1(path, *, session_id, after=0, limit=100, role="leader", purpose="user"):
    if not 1 <= limit <= 100 or after < 0:
        raise ValueError("invalid session export cursor")
    entries = load_entries(path)
    branch = branch_entries(entries)
    positions = {entry.get("id"): index + 1 for index, entry in enumerate(entries)}
    origins = {}
    for entry in branch:
        if entry.get("type") == "custom" and entry.get("customType") == "elephant-source":
            origins[entry["data"]["id"]] = entry["data"].get("kind", "relay")
    events, cursor, source_ids = [], after, ()
    branch_id = "main"
    for entry in branch:
        position = positions[entry["id"]]
        if entry.get("type") == "branch_summary":
            branch_id = entry["id"]
        if position <= after:
            continue
        if len(events) >= limit:
            break
        cursor = position
        if entry.get("type") != "message":
            continue
        message = entry.get("message", {})
        raw_role = message.get("role")
        if raw_role not in {"user", "assistant", "toolResult"}:
            continue
        parts = message.get("content", [])
        content = parts if isinstance(parts, str) else "\n".join(
            part.get("text", "") for part in parts if part.get("type") == "text")
        kind = "original"
        if raw_role == "user":
            lines = content.splitlines()
            ids = []
            kept = []
            for line in lines:
                if line.startswith("[[elephant-input:") and line.endswith("]]"):
                    ids.append(line[len("[[elephant-input:"):-2])
                else:
                    kept.append(line)
            source_ids = tuple(ids)
            content = "\n".join(kept)
            kind = "original" if ids and all(origins.get(source) == "user" for source in ids) else "relay"
        elif raw_role == "toolResult":
            kind = "tool_output"
        elif role != "leader" or purpose != "user":
            kind = "relay"
        if not content:
            continue
        # Byte limits protect transport; this is not a token estimate.
        if len(content) > 64000:
            kind = "relay"
            content = content[:64000]
        events.append(SourceEvent(id=session_id + ":" + entry["id"], seq=position,
            role="tool" if raw_role == "toolResult" else raw_role, content=content,
            revision=hashlib.sha256(json.dumps(entry, sort_keys=True).encode()).hexdigest(),
            source_event_ids=source_ids, kind="skill_review" if role == "skill_review" else kind,
            created_at=entry.get("timestamp")))
    return {"session_id": session_id, "events": tuple(events), "next_cursor": cursor,
            "branch": branch_id, "has_more": any(positions[e["id"]] > cursor for e in branch)}


def export_session(path, *, session_id, after=0, limit=100, role="leader", purpose="user",
                   turns=None, input_origins=None, projection_version="pi-original-v2"):
    if projection_version == "pi-original-v1":
        return _export_session_v1(path, session_id=session_id, after=after, limit=limit, role=role, purpose=purpose)
    if projection_version != "pi-original-v2" or not 1 <= limit <= 100 or after < 0:
        raise ValueError("invalid session export projection or cursor")
    entries = load_entries(path)
    branch = branch_entries(entries)
    positions = {entry.get("id"): index + 1 for index, entry in enumerate(entries)}
    turns, input_origins = turns or {}, input_origins or {}
    origins, entry_runs = {}, {}
    run_id = None
    for entry in reversed(branch):
        if entry.get("type") == "custom" and entry.get("customType") == "elephant-turn-boundary":
            run_id = entry.get("data", {}).get("run_id")
        if run_id:
            entry_runs[entry["id"]] = run_id
        if entry.get("type") == "custom" and entry.get("customType") == "elephant-run":
            run_id = None
    active_run, source_ids, source_kind = {}, (), "relay"
    events, cursor, branch_id = [], after, "main"
    for entry in branch:
        position = positions[entry["id"]]
        data = entry.get("data", {})
        if entry.get("type") == "custom":
            custom = entry.get("customType")
            if custom == "elephant-source":
                origins[data["id"]] = data.get("kind", "relay")
            elif custom == "elephant-run":
                active_run = {**data, **turns.get(data.get("run_id"), {})}
                source_ids = tuple(dict.fromkeys(origin for source in data.get("source_event_ids", ())
                    for origin in input_origins.get(source, [source])))
                source_kind = "relay"
        if entry.get("type") == "branch_summary":
            branch_id = entry["id"]
        if position > after and len(events) >= limit:
            break
        if position > after:
            cursor = position
        if entry.get("type") != "message":
            continue
        current = turns.get(entry_runs.get(entry["id"]), active_run)
        current_role, current_purpose = current.get("role", role), current.get("purpose", purpose)
        message = entry.get("message", {})
        raw_role = message.get("role")
        if raw_role not in {"user", "assistant", "toolResult"}:
            continue
        parts = message.get("content", [])
        separator = "" if raw_role == "assistant" else "\n"
        content = parts if isinstance(parts, str) else separator.join(
            part.get("text", "") for part in parts if part.get("type") == "text")
        evidence_refs, event_origins = (), source_ids
        kind = "relay"
        if raw_role == "user":
            ids, kept = [], []
            for line in content.splitlines():
                if line.startswith("[[elephant-input:") and line.endswith("]]"):
                    ids.append(line[len("[[elephant-input:"):-2])
                else:
                    kept.append(line)
            if ids:
                source_ids = tuple(dict.fromkeys(origin for source in ids
                    for origin in input_origins.get(source, [source])))
            source_kind = "original" if (current_role == "leader" and current_purpose == "user" and ids
                and all(origins.get(source) == "user" for source in ids)) else "relay"
            content, kind, event_origins = "\n".join(kept), source_kind, source_ids
        elif raw_role == "toolResult":
            kind = "tool_output"
        elif (content and current_role == "worker" and current.get("summary") == content[:10000]
                and current.get("evidence_refs")):
            content = current["summary"]
            kind, event_origins = "verified_fact", ("result:" + current["run_id"],)
            evidence_refs = tuple(current["evidence_refs"])
        if position <= after or not content:
            continue
        if len(content) > 64000:
            kind, content = "relay", content[:64000]
        events.append(SourceEvent(id=session_id + ":" + entry["id"], seq=position,
            role="tool" if raw_role == "toolResult" else raw_role, content=content,
            revision=hashlib.sha256(json.dumps(entry, sort_keys=True).encode()).hexdigest(),
            source_event_ids=event_origins, evidence_refs=evidence_refs,
            kind="skill_review" if current_role == "skill_review" else kind, created_at=entry.get("timestamp")))
    return {"session_id": session_id, "events": tuple(events), "next_cursor": cursor,
        "branch": branch_id, "has_more": any(positions[e["id"]] > cursor for e in branch),
        "projection_version": projection_version}


def boundary_receipts(path):
    return [entry["data"] for entry in branch_entries(load_entries(path))
            if entry.get("type") == "custom" and entry.get("customType") == "elephant-turn-boundary"]


def unfinished_tools(path, *, discarded=()):
    branch = branch_entries(load_entries(path))
    edits = {entry.get("targetId"): entry["replacement"] for entry in branch if entry.get("type") == "context_edit"
             and "replacement" in entry and entry.get("targetId") in discarded}
    hidden = {target for target, replacement in edits.items() if replacement is None}
    pending = {}
    for entry in branch:
        if entry.get("type") != "message" or entry["id"] in hidden:
            continue
        message = entry.get("message", {})
        if message.get("role") == "assistant":
            for part in message.get("content", []):
                if part.get("type") == "toolCall":
                    pending[part["id"]] = part
        elif message.get("role") == "toolResult":
            pending.pop(message.get("toolCallId"), None)
    return tuple(pending.values())


def aborted_submit(path, call, *, run_id, input_ids):
    """Recognize only the incomplete, non-executed call at this stopped turn's tail."""
    args = call.get("arguments")
    if call.get("name") != "tasks_submit" or not isinstance(args, dict) or {"goal", "children"} <= args.keys():
        return None
    entries = load_entries(path)
    branch = branch_entries(entries)
    if not branch:
        return None
    leaf = branch[-1]
    edited = (leaf.get("type") == "context_edit" and "replacement" in leaf and leaf["replacement"] is None)
    if edited and (len(branch) < 2 or leaf.get("targetId") != branch[-2]["id"]):
        return None
    candidate = branch[-2] if edited else leaf
    message = candidate.get("message", {})
    tools = [part for part in message.get("content", ()) if isinstance(part, dict) and part.get("type") == "toolCall"]
    if (candidate.get("type") != "message" or message.get("role") != "assistant"
            or message.get("stopReason") != "aborted" or tools != [call]):
        return None
    current = consumed = False
    for entry in branch[: -2 if edited else -1]:
        if entry.get("type") != "custom":
            continue
        kind, data = entry.get("customType"), entry.get("data", {})
        if kind == "elephant-run":
            current = (data.get("run_id"), data.get("role"), data.get("purpose"), data.get("source_event_ids")) == (
                run_id, "leader", "user", input_ids)
            consumed = False
        elif kind == "elephant-consumed":
            consumed = current and data.get("run_id") == run_id and data.get("input_ids") == input_ids
        elif kind == "elephant-turn-boundary":
            current = consumed = False
    if not consumed:
        return None
    # A duplicated call ID or a result, even off-branch, is not evidence of a partial call.
    occurrences = 0
    for entry in entries:
        msg = entry.get("message", {})
        if msg.get("role") == "toolResult" and msg.get("toolCallId") == call["id"]:
            return None
        if msg.get("role") == "assistant":
            occurrences += sum(1 for part in msg.get("content", ()) if isinstance(part, dict)
                               and part.get("type") == "toolCall" and part.get("id") == call["id"])
    if occurrences != 1:
        return None
    return {"entry": candidate, "revision": leaf["id"], "edited": edited}


def mutation_calls(path, *, run_id, input_ids):
    calls, active = [], False
    for entry in branch_entries(load_entries(path)):
        if entry.get("type") == "custom" and entry.get("customType") == "elephant-consumed":
            data = entry.get("data", {})
            active = data.get("run_id") == run_id and set(data.get("input_ids", ())) == set(input_ids)
        elif entry.get("type") == "custom" and entry.get("customType") == "elephant-turn-boundary":
            active = False
        elif active and entry.get("type") == "message":
            message = entry.get("message", {})
            if message.get("role") == "assistant":
                calls.extend(part for part in message.get("content", []) if part.get("type") == "toolCall"
                    and part.get("name") in {"memory_correct", "memory_forget"})
    return calls


def restore_tool_results(path, receipts, *, pi_executable):
    """Append genuine host receipts through Pi's native writer after verified stop."""
    _write_recovery(path, receipts, pi_executable=pi_executable)


def discard_aborted_submit(path, discard, *, pi_executable):
    _write_recovery(path, (), pi_executable=pi_executable, discard=discard)


def _write_recovery(path, receipts, *, pi_executable, discard=None):
    native = load_entries(path)
    if not native or native[0].get("type") != "session" or native[0].get("version") != 3:
        raise ValueError("native recovery requires a current Pi session header")
    entries = branch_entries(native)
    if not entries or (not receipts and not discard):
        raise ValueError("native recovery requires an existing session and evidence")
    if discard and entries[-1]["id"] != discard["revision"]:
        raise ValueError("native session revision changed")
    executable = Path(shutil.which(pi_executable) or pi_executable).resolve(strict=True)
    module = None
    for parent in executable.parents:
        manifest = parent / "package.json"
        if manifest.is_file():
            package = json.loads(manifest.read_text())
            if package.get("name") == "@earendil-works/pi-coding-agent" and package.get("version") == "1.5.0":
                module = parent / "dist" / "index.js"
                break
    if module is None or not module.is_file():
        raise ValueError("locked Pi public SessionManager API is unavailable")
    payload = json.dumps({"module": module.as_uri(), "path": str(Path(path).resolve()),
        "revision": entries[-1]["id"], "receipts": receipts, "discard": discard}, ensure_ascii=False)
    if len(payload.encode()) > 1024 * 1024:
        raise ValueError("native recovery receipts exceed the bounded transport")
    script = """
import {readFileSync, openSync, fsyncSync, closeSync} from 'node:fs';
import {isDeepStrictEqual} from 'node:util';
const body = JSON.parse(readFileSync(0, 'utf8'));
const {SessionManager} = await import(body.module);
const session = SessionManager.open(body.path);
if (session.getLeafId() !== body.revision) throw new Error('Native session revision changed');
const pending = new Map();
for (const entry of session.getBranch()) {
  if (entry.type !== 'message') continue;
  const message = entry.message;
  if (message.role === 'assistant') for (const part of message.content) {
    if (part.type === 'toolCall') pending.set(part.id, part);
  }
  if (message.role === 'toolResult') pending.delete(message.toolCallId);
}
for (const receipt of body.receipts) {
  if (!isDeepStrictEqual(pending.get(receipt.call.id), receipt.call)) throw new Error('Native call changed');
}
for (const receipt of body.receipts) {
  session.appendMessage({role: 'toolResult', toolCallId: receipt.call.id, toolName: receipt.call.name,
    content: [{type: 'text', text: JSON.stringify(receipt.result)}], details: {}, isError: receipt.is_error === true, timestamp: Date.now()});
}
if (body.discard) {
  const {entry, edited} = body.discard;
  const branch = session.getBranch();
  if (!isDeepStrictEqual(branch.at(edited ? -2 : -1), entry)) throw new Error('Native aborted entry changed');
  if (edited) {
    const leaf = branch.at(-1);
    if (leaf.type !== 'context_edit' || leaf.targetId !== entry.id || leaf.replacement !== null) {
      throw new Error('Native context edit changed');
    }
  } else {
    session.appendContextEdit(entry.id, null);
  }
}
const fd = openSync(body.path, 'r');
try { fsyncSync(fd); } finally { closeSync(fd); }
"""
    try:
        subprocess.run([shutil.which("node") or "node", "--input-type=module", "-e", script],
            input=payload, text=True, capture_output=True, timeout=15, check=True)
    except (subprocess.SubprocessError, OSError) as error:
        raise ValueError("native tool-result recovery failed") from error


def review_snapshot(path, *, run_id, max_chars=64000):
    """Freeze reference material at this run's native boundary, including tool calls."""
    entries = load_entries(path)
    boundary = next((index for index in range(len(entries) - 1, -1, -1)
        if entries[index].get("type") == "custom"
        and entries[index].get("customType") == "elephant-turn-boundary"
        and entries[index].get("data", {}).get("run_id") == run_id), None)
    if boundary is None:
        raise ValueError("the source turn has no durable native boundary")
    material, remaining = [], max_chars
    for entry in reversed(branch_entries(entries[:boundary + 1])):
        if entry.get("type") != "message":
            continue
        message = entry.get("message", {})
        if message.get("role") not in {"user", "assistant", "toolResult"}:
            continue
        rendered = json.dumps(message, ensure_ascii=False)
        if len(rendered) > remaining:
            if not material:
                raise ValueError("the source turn exceeds the review snapshot budget")
            break
        material.append(message)
        remaining -= len(rendered)
    if not material:
        raise ValueError("the source turn has no reviewable messages")
    return tuple(reversed(material))


def recovery_snapshot(path, *, run_id):
    """Expose native recovery facts without letting the runtime rewrite Pi history."""
    entries = load_entries(path)
    branch = branch_entries(entries)
    pending, consumed, boundaries, yielded = {}, set(), [], False
    for entry in branch:
        if entry.get("type") == "custom":
            data = entry.get("data", {})
            if not isinstance(data, dict) or data.get("run_id") != run_id:
                continue
            if entry.get("customType") == "elephant-consumed":
                consumed.update(data.get("input_ids", []))
            elif entry.get("customType") == "elephant-turn-boundary":
                boundaries.append(data)
            elif entry.get("customType") == "elephant-yield-boundary":
                yielded = True
        if entry.get("type") != "message":
            continue
        message = entry.get("message", {})
        if message.get("role") == "assistant":
            for part in message.get("content", []):
                if isinstance(part, dict) and part.get("type") == "toolCall":
                    pending[part["id"]] = part
        elif message.get("role") == "toolResult":
            pending.pop(message.get("toolCallId"), None)
    return {"exists": Path(path).exists(), "session_revision": branch[-1]["id"] if branch else None,
        "pending_tools": tuple(pending.values()), "consumed_input_ids": sorted(consumed),
        "boundaries": boundaries, "yielded": yielded}
