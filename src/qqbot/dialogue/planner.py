"""Deterministic initial delivery hints; pi plans the actual work."""

import hashlib
import re
from pathlib import PurePosixPath

from qqbot.context.topics import RESOURCE_URL, resource_reference
from qqbot.domain.execution import ExecutionPlan, TaskGoal


def wants_background(text):
    for clause in re.split(r"[，。！？,;；\n]|但是|不过|而是|但", text):
        if re.search(r"(?:不(?:要|用|必|想)?|别|无需).*(?:后台|慢慢做|等待|等到)", clause):
            continue
        if re.search(r"(?:放到|转到|放|转|交给)后台|慢慢做|等(?:待|到).*(?:CI|构建|测试)",
                     clause, re.IGNORECASE):
            return True
    return False


def plan_turn(turn, settings, *, background=False, permission_version=0, context=None):
    detached = background or wants_background(turn.text)
    checks, outputs = [], []
    explicit_root_outputs = set()
    github = bool(re.search(r"\b(?:PR|Issue|GitHub|CI)\b|仓库|github\.com", turn.text, re.IGNORECASE))
    weather = bool(re.search(r"天气|气温|温度|下雨|预报|\bweather\b", turn.text, re.IGNORECASE)) or (
        (context or {}).get("seed", {}).get("topic", {}).get("kind") == "weather"
    )
    artifact_request = bool(re.search(r"生成|导出|保存为|另存为|写一[份个张]|给我", turn.text))
    if artifact_request:
        filenames = re.finditer(
            r"(?<![\w/:])(?:[\w.-]+/)*[\w.-]+\.(?:md|pdf|txt|json|csv|html|zip|png|jpe?g|webp|docx|xlsx|pptx)\b",
            turn.text,
            re.IGNORECASE,
        )
        for match in filenames:
            filename = match.group()
            if not re.search(r"生成|导出|保存为|另存为|写一[份个张]|给我", turn.text[: match.start()]):
                filename = re.sub(
                    r"^(?:请|帮我|把|生成|导出|输出|保存为|另存为|到|文件名为|文件名是|名为|一个|一份)+",
                    "",
                    filename,
                )
            path = PurePosixPath(filename)
            if not path.is_absolute() and ".." not in path.parts:
                outputs.append(str(path))
                if filename.startswith("./"):
                    explicit_root_outputs.add(str(path))
        qualified_names = {PurePosixPath(path).name for path in outputs if "/" in path}
        outputs = [path for path in outputs if "/" in path or path not in qualified_names
                   or path in explicit_root_outputs]
        if outputs or re.search(r"文件|PDF|DOCX|XLSX|PPTX|电子表格|幻灯片", turn.text, re.IGNORECASE):
            checks.append("artifact")
    fresh_query = bool(re.search(r"查询|查一下|查查|帮我查|列出|现在|当前|今天|明天|有哪些", turn.text))
    if fresh_query and (github or weather):
        checks.append("observation")
    write_request = bool(
        re.search(
            r"创建.*Issue|提交.*PR|提\s*PR|创建.*PR|重跑.*CI|重新运行.*CI|合并.*PR|批准.*PR|approve|merge",
            turn.text,
            re.IGNORECASE,
        )
    )
    conditional = bool(re.search(r"如果|若|没问题|确认后|否则|可以的话", turn.text))
    if github and write_request and not conditional:
        checks.append("effect")
    references = tuple(
        resource_reference(url)["resource_id"]
        for url in RESOURCE_URL.findall(turn.text)
        if resource_reference(url) is not None
    )
    goal = TaskGoal(
        text=turn.text,
        target_refs=references,
        requested_outputs=tuple(dict.fromkeys(outputs)),
        success_checks=tuple(checks),
    )
    return goal, ExecutionPlan(
        initial_delivery="DETACHED" if detached else "ATTACHED",
        foreground_wait_ms=settings.execution.foreground_wait_ms,
        model_config_ref=hashlib.sha256(settings.model.model_dump_json().encode()).hexdigest(),
        context_config_ref=settings.context.version,
        budget=settings.execution.budget if detached else settings.execution.interactive_budget,
        permission_snapshot_ref=str(permission_version),
    )
