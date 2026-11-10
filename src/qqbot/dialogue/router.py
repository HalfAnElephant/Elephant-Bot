"""Resolve explicit conversational controls without selecting business capabilities."""

import re

from qqbot.domain.messages import Surface

TASK_ID = re.compile(r"\bT-[0-9a-f]{8}-[0-9a-f]{3}\b", re.IGNORECASE)
ACTIVE = {"QUEUED", "RUNNING", "WAITING_INPUT", "WAITING_EXTERNAL"}


def task_references(text):
    return {match.group().lower() for match in TASK_ID.finditer(text)}


def control_intent(text):
    text = TASK_ID.sub("", text).strip(" ，,。:：")
    if re.fullmatch(
        r"(?:取消|停止|终止)(?:这(?:个|项|次)?(?:任务|操作|查询)?|刚才的?(?:任务|操作|查询))?(?:吧|了)?"
        r"|(?:不用|别)(?:继续|查|做|跑)了|先停(?:一下|下)?", text,
    ):
        return "cancel"
    if re.fullmatch(r"暂停(?:这(?:个|项|次)?任务)?(?:一下|吧)?", text):
        return "clarify"
    request_prefix = r"(?:我(?:想|要|需要)|(?:请|麻烦)(?:你)?)"
    if re.match("^" + request_prefix + r"(?:用|使用|选|采用|改成|换成)", text):
        return None
    if re.match(
        r"^(?:换个话题|另外(?:一个)?问题|新(?:的)?(?:任务|问题)|另外帮我"
        r"|我(?:想|要|需要)|(?:请|麻烦)(?:你)?"
        r"|(?:你)?帮我|(?:你)?(?:能不能|能否|可不可以)(?:帮我|给我)?)", text,
    ):
        return "new_task"
    return None


def ambiguous_wait_answer(event):
    if event.reply_to or task_references(event.text):
        return False
    return bool(re.search(r"[?？]|(?:什么|怎么|为什么|哪[个里种]|谁|吗|么|呢)[。！!\s]*$", event.text))


def route_turn(event, candidates):
    references = task_references(event.text)
    explicit = [item for item in candidates if item["public_id"].lower() in references or item["reply_match"]]
    text = TASK_ID.sub("", event.text).strip(" ，,。:：")
    intent = control_intent(event.text)
    cancel = intent == "cancel"
    continuation = bool(re.match(
        r"^(?:补充|加(?:一条|个条件)|只(?:看|查|要|保留|考虑)|仅(?:看|查|要|保留)|限定"
        r"|(?:目标|仓库|分支|条件|筛选|时间范围)(?:改成|换成|是)|改成|换成|更正|纠正"
        r"|不是.{1,60}(?:而是|是)|继续(?:$|这个|那个|刚才|查|做|跑)|接着(?:$|查|做|跑))", text,
    ))
    if len(explicit) > 1 or len(references) > 1:
        return "clarify", explicit
    if references and not explicit:
        return "clarify", []
    independent = intent == "new_task" and not references
    continuation = continuation or bool(explicit and text and not independent)
    if not cancel and not continuation and intent != "clarify":
        return "new_task" if event.mentions_bot or event.conversation.surface == Surface.PRIVATE else "unrelated_chat", None
    possible = explicit or [item for item in candidates if item["state"] in ACTIVE]
    possible = possible or candidates
    if len(possible) > 1:
        return "clarify", possible
    if not possible or intent == "clarify":
        return "clarify", possible
    task = possible[0]
    if cancel:
        return "cancel", task
    if task["wait_id"]:
        return ("answer_wait", task) if references else ("unrelated_chat", None)
    return "continue_task", task
