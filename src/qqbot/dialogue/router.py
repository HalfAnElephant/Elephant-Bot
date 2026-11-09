import re
from typing import Literal

from qqbot.domain import DTO


class Route(DTO):
    action: Literal["ANSWER_NOW", "ENQUEUE_TASK", "NEED_INPUT"]
    reason: str


def route_request(text: str) -> Route:
    text = text.strip()
    research = bool(
        re.search(
            r"(?:研究|调研|对比|分析).*(?:仓库|项目|网页|报告|资料)|(?:生成|写).*(?:研究报告|对比报告)", text
        )
    )
    if not research:
        return Route(action="ANSWER_NOW", reason="SHORT_REQUEST")
    if re.search(r"这(?:几个|三个|些|两[个份])(?:仓库|项目|网页)", text) and not re.search(
        r"https?://", text
    ):
        return Route(action="NEED_INPUT", reason="MISSING_TARGETS")
    return Route(action="ENQUEUE_TASK", reason="EXPLICIT_RESEARCH_REQUEST")
