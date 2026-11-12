"""Loss-aware bounded official QQ normalization, without network access.

Projections are untrusted message content, not executable commands. Unknown
versions remain in a bounded raw segment alongside understood semantic fields.
"""
import json
from urllib.parse import urlsplit

from qqbot.domain import (
    MediaSegment,
    MentionSegment,
    ReplySegment,
    StructuredSegment,
    TextSegment,
    UnknownSegment,
    stable_id,
)


def attachment_url(value):
    if not isinstance(value, str) or len(value) > 8192:
        return None
    if value.startswith("//"):
        value = "https:" + value
    try:
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
            return None
        _ = parsed.port  # Validate malformed port syntax without fetching anything.
    except ValueError:
        return None
    return value


def _items(value):
    return value[:100] if isinstance(value, list) else []


def _media(data, kind=None):
    mime = str(data.get("content_type") or data.get("mime_type") or "")[:255]
    kind = kind or next((p for p in ("image", "audio", "video") if mime.startswith(p + "/")), "file")
    url = attachment_url(data.get("url") or data.get("file_url"))
    size = data.get("size")
    transcript = data.get("transcript") or data.get("text") if kind == "audio" else None
    return MediaSegment(
        type=kind, attachment_ref="unresolved:" + stable_id(json.dumps(data, sort_keys=True)),
        image_url=url if kind == "image" else None, source_url=url, mime_type=mime or None,
        filename=str(data.get("filename") or data.get("file_name"))[:255]
        if data.get("filename") or data.get("file_name") else None,
        transcript=transcript[:10000] if isinstance(transcript, str) else None,
        size=size if isinstance(size, int) and not isinstance(size, bool) and size >= 0 else None,
    )


def _project(value, *, quoted=False):
    """Recognize only explicit typed elements; never reinterpret arbitrary keys."""
    result, seen, stack, nodes, chars = [], set(), [(value, 0, quoted)], 0, 0
    def add(segment):
        nonlocal chars
        encoded = segment.model_dump_json()
        if encoded in seen or len(result) >= 100:
            return
        if isinstance(segment, TextSegment):
            remaining = 20000 - chars
            if remaining <= 0:
                return
            segment = TextSegment(text=segment.text[:remaining])
            chars += len(segment.text)
        seen.add(encoded)
        result.append(segment)

    while stack and nodes < 500 and len(result) < 100:
        node, depth, quoted = stack.pop()
        nodes += 1
        if depth > 6:
            continue
        if isinstance(node, list):
            stack.extend((item, depth + 1, quoted) for item in reversed(node[:100]))
            continue
        if not isinstance(node, dict):
            continue
        kind = node.get("type") or node.get("element_type")
        if isinstance(kind, str):
            kind = kind.lower()
        data = node.get("data") if isinstance(node.get("data"), dict) else node
        if kind in ("text", "markdown"):
            text = data.get("text") or data.get("content")
            if isinstance(text, str):
                add(TextSegment(text=("[转发内容] " if quoted else "") + text))
        if kind in ("image", "audio", "video", "file"):
            add(_media(data, kind))
        if kind in ("mention", "at"):
            user = data.get("user_openid") or data.get("id") or data.get("qq")
            if isinstance(user, (str, int)):
                add(MentionSegment(remote_user_id=str(user)[:256]))
        reference = node.get("message_reference")
        if isinstance(reference, dict) and isinstance(reference.get("message_id"), (str, int)):
            add(ReplySegment(platform_message_id=str(reference["message_id"])[:256]))
        # Forwarded message content is quoted, not promoted to sender authority.
        if "author" in node and isinstance(node.get("content"), str):
            add(TextSegment(text="[转发内容] " + node["content"]))
        for attachment in _items(node.get("attachments")):
            if isinstance(attachment, dict):
                add(_media(attachment))
        # Known share-card fields, including Ark key/value convention.
        if kind in ("ark", "card", "share"):
            for key in ("title", "description", "desc", "prompt"):
                if isinstance(data.get(key), str):
                    add(TextSegment(text="[分享卡片] " + data[key]))
        if node.get("key") in ("#TITLE#", "#DESC#", "#PROMPT#") and isinstance(node.get("value"), str):
            add(TextSegment(text="[分享卡片] " + node["value"]))
        for key in ("msg_elements", "elements", "messages", "nodes", "children", "content", "data", "kv", "obj"):
            child = node.get(key)
            if isinstance(child, (list, dict)) and child is not node:
                stack.append((child, depth + 1, quoted or kind in ("forward", "forward_message")
                              or key in ("messages", "nodes")))
    return result


def extra_segments(data):
    result = []
    for attachment in _items(data.get("attachments")):
        if isinstance(attachment, dict):
            result.append(_media(attachment))
    for mention in _items(data.get("mentions")):
        if isinstance(mention, dict) and (mention.get("id") or mention.get("user_openid")):
            result.append(MentionSegment(remote_user_id=str(mention.get("id") or mention["user_openid"])))
    ref = data.get("message_reference")
    if isinstance(ref, dict) and ref.get("message_id"):
        result.append(ReplySegment(platform_message_id=str(ref["message_id"])))
    for field in ("msg_elements", "ark_data", "forward_messages"):
        value = data.get(field)
        if value is not None:
            payload = {"value": value}
            encoded = json.dumps(payload, ensure_ascii=False)
            if len(encoded.encode()) <= 65536:
                result.extend(_project(value, quoted=field == "forward_messages"))
                result.append(StructuredSegment(original_type=field, payload=payload))
            else:
                result.append(UnknownSegment(original_type=field, digest=stable_id(encoded)))
    return result
