"""Native Pi log export. Only this adapter parses the Harness's file format."""
import hashlib
import json
from pathlib import Path

from qqbot.contracts.memory import SourceEvent


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


def export_session(path, *, session_id, after=0, limit=100, role="leader", purpose="user"):
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


def boundary_receipts(path):
    return [entry["data"] for entry in branch_entries(load_entries(path))
            if entry.get("type") == "custom" and entry.get("customType") == "elephant-turn-boundary"]


def unfinished_tools(path):
    pending = {}
    for entry in branch_entries(load_entries(path)):
        if entry.get("type") != "message":
            continue
        message = entry.get("message", {})
        if message.get("role") == "assistant":
            for part in message.get("content", []):
                if part.get("type") == "toolCall":
                    pending[part["id"]] = part
        elif message.get("role") == "toolResult":
            pending.pop(message.get("toolCallId"), None)
    return tuple(pending.values())


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
