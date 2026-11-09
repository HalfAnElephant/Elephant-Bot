"""Deterministic topic navigation using host-observed references and short follow-ups."""

import re
from datetime import datetime
from urllib.parse import urlsplit

from qqbot.domain import stable_id
from qqbot.memory import keywords

RESOURCE_URL = re.compile(r"https?://[^\s<>\])，。]+/(?:pull|issues)/\d+", re.IGNORECASE)


def resource_reference(url):
    parsed = urlsplit(url.rstrip(".,;"))
    match = re.fullmatch(r"/([^/]+)/([^/]+)/(pull|issues)/(\d+)/?", parsed.path)
    if not match:
        return None
    owner, repo, kind, number = match.groups()
    repository = f"{parsed.netloc.lower()}/{owner}/{repo}"
    return {
        "resource_id": f"{'pr' if kind == 'pull' else 'issue'}:{repository}#{number}",
        "repository": "repo:" + repository,
        "url": url.rstrip(".,;"),
    }


def entities(text):
    resources = []
    for url in RESOURCE_URL.findall(text):
        reference = resource_reference(url)
        if reference:
            resources.extend((reference["resource_id"], reference["repository"]))
    repositories = re.findall(r"https?://([^\s/]+)/([^\s/]+)/([^\s/#?<>\])}，。]+)", text)
    resources.extend("repo:" + "/".join(parts) for parts in repositories)
    if re.search(r"天气|气温|温度|下雨|预报|\bweather\b", text, re.IGNORECASE):
        prefix = re.split(r"天气|气温|温度|预报|下雨", text, maxsplit=1)[0]
        prefix = re.sub(r"^(?:请|帮我|麻烦|查询|查一下|查查|看看|查|一下|了解|那么|那)+", "", prefix)
        place = re.sub(r"(?:今天|明天|后天|现在|这周|本周|未来|最近|的|怎么样|如何)", "", prefix).strip()
        place = re.sub(r"(?:会|要|是否)$", "", place)
        if place and re.fullmatch(r"[\u3400-\u9fffA-Za-z ·-]{1,24}", place):
            resources.append("location:" + place)
        return "weather", list(dict.fromkeys(resources))
    if resources or re.search(r"\b(?:PR|Issue|GitHub|CI)\b|仓库|合并请求", text, re.IGNORECASE):
        return "github", list(dict.fromkeys(resources))
    return "general", list(dict.fromkeys(resources))


def displayed_references(text, source_id, displayed_at, known_entities, *, ordinal_offset=0):
    references = []
    repositories = [item for item in known_entities if item.startswith("repo:")]
    for line in text.splitlines():
        numbered = re.match(r"\s*(?:(\d+)[.、)]|[-*])\s+(.+)", line)
        if not numbered:
            continue
        label = numbered.group(2)
        ordinal = int(numbered.group(1)) if numbered.group(1) else ordinal_offset + len(references) + 1
        targets = [resource_reference(url) for url in RESOURCE_URL.findall(label)]
        targets = [target for target in targets if target]
        if not targets and len(repositories) == 1:
            match = re.search(r"(?:\bPR\s*#?|#)(\d+)\b", label, re.IGNORECASE)
            if match:
                repository = repositories[0].removeprefix("repo:")
                targets = [
                    {
                        "resource_id": f"pr:{repository}#{match[1]}",
                        "repository": repositories[0],
                        "url": f"https://{repository}/pull/{match[1]}",
                    }
                ]
        if len(targets) == 1:
            references.append(
                {
                    **targets[0],
                    "ordinal": ordinal,
                    "label": label[:500],
                    "observed_at": displayed_at,
                    "source_id": source_id,
                    "source_kind": "delivered_reference",
                }
            )
    return references


def select_topic(index, turn, now, idle_seconds, *, reply_topic=None):
    items = index.setdefault("items", [])
    active = next((item for item in items if item["id"] == index.get("active")), None)
    kind, resources = entities(turn.text)
    tokens = keywords(turn.text)
    selected, reason = None, "new_topic"
    if reply_topic:
        selected = next((item for item in items if item["id"] == reply_topic), None)
        reason = "reply_reference"
    exact = {item for item in resources if item.startswith(("pr:", "issue:"))}
    if selected is None and exact:
        selected = next(
            (item for item in reversed(items) if exact.intersection(item.get("entities", ()))), None
        )
        reason = "resource_reference"
    returning = bool(re.search(r"回到|刚才|之前|前面", turn.text))
    ordinal_match = re.search(r"第([一二三四五六七八九十\d]+)(?:个|条|项)", turn.text)
    if selected is None and returning:
        candidates = [item for item in reversed(items) if kind == "general" or item["kind"] == kind]
        if ordinal_match:
            candidates = [item for item in candidates if item.get("displays")]
        selected = candidates[0] if candidates else None
        reason = "explicit_return"
    if selected is None and kind != "general":
        candidates = [item for item in reversed(items) if item["kind"] == kind]
        concrete = {item for item in resources if item.startswith(("location:", "repo:"))}
        selected = (
            next((item for item in candidates if concrete.intersection(item.get("entities", ()))), None)
            if concrete
            else next(iter(candidates), None)
        )
        reason = "matching_entity" if concrete else "matching_subject"
    if selected is None and active and not re.search(r"换个话题|另外说|说点别的", turn.text):
        elapsed = (now - datetime.fromisoformat(active["last_at"])).total_seconds()
        if kind == "general" and elapsed <= idle_seconds:
            score = len(tokens.intersection(active.get("tokens", ())))
            if len(turn.text.strip()) <= 20 or score >= 4:
                selected, reason = (
                    active,
                    "short_followup" if len(turn.text.strip()) <= 20 else "lexical_relevance",
                )
    if selected is None:
        selected = {
            "id": stable_id(turn.conversation.key, turn.anchor_event_id),
            "kind": kind,
            "entities": [],
            "tokens": [],
            "source_ids": [],
            "displays": [],
            "last_at": now.isoformat(),
        }
        items.append(selected)
    selected["entities"] = list(dict.fromkeys((*selected.get("entities", ()), *resources)))[:64]
    selected["tokens"] = sorted(
        (token for token in keywords(turn.text) if len(token) <= 64), key=lambda token: (-len(token), token)
    )[:128]
    selected["source_ids"] = list(dict.fromkeys((*selected.get("source_ids", ()), *turn.source_event_ids)))[
        -128:
    ]
    selected["last_at"] = now.isoformat()
    index["active"] = selected["id"]
    index["items"] = sorted(items, key=lambda item: item["last_at"])[-16:]
    reference = None
    if ordinal_match and selected.get("displays"):
        number = ordinal_match[1]
        ordinal = (
            int(number)
            if number.isdigit()
            else {
                "一": 1,
                "二": 2,
                "三": 3,
                "四": 4,
                "五": 5,
                "六": 6,
                "七": 7,
                "八": 8,
                "九": 9,
                "十": 10,
            }.get(number)
        )
        matches = [item for item in selected["displays"][-1]["references"] if item["ordinal"] == ordinal]
        if len({item["resource_id"] for item in matches}) == 1:
            reference = matches[0]
        elif matches:
            reason = "ambiguous_display_ordinal"
    return selected, reference, reason
