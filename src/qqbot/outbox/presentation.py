"""Deterministic QQ plain text and lossless, boundary-aware message splitting."""
import re
from uuid import uuid4

MESSAGE_LIMIT = 1800
FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")
REFERENCE = re.compile(r'^ {0,3}\[([^\]]+)\]:\s*(?:<([^>]+)>|(\S+))(?:\s+.*)?$')
INLINE_CODE = re.compile(r"(`+)(.*?)\1")
URL = re.compile(r"https?://[^\s<>]+")
PATH = re.compile(r"(?:[A-Za-z]:\\|~?/|\.\.?/)[^\s<>]+|(?<!\w)(?:[\w.-]+/)+[^\s<>]+")


def _inline(text, references):
    marker = "\ufff0" + uuid4().hex + ":"
    literals = []
    def protect(value):
        literals.append(value)
        return marker + str(len(literals) - 1) + "\ufff1"
    text = INLINE_CODE.sub(lambda match: protect(match[2]), text)
    link = re.compile(r"!?\[([^\]\n]*)\]\(")
    start, pieces = 0, []
    for match in link.finditer(text):
        if match.start() < start:
            continue
        depth, end = 1, match.end()
        while end < len(text) and depth:
            if text[end] == "(" and (end == 0 or text[end - 1] != "\\"):
                depth += 1
            elif text[end] == ")" and (end == 0 or text[end - 1] != "\\"):
                depth -= 1
            end += 1
        if depth:
            continue
        destination = text[match.end():end - 1].strip()
        if destination.startswith("<") and ">" in destination:
            destination = destination[1:destination.index(">")]
        else:
            destination = destination.split(' "', 1)[0].split(" '", 1)[0]
        destination = re.sub(r"\\([()])", r"\1", destination)
        label = _inline(match[1], references)
        pieces.extend((text[start:match.start()], protect(label + " (" + destination + ")" if label and label != destination else destination)))
        start = end
    pieces.append(text[start:])
    text = "".join(pieces)
    def reference(label, identity, original):
        destination = references.get((identity or label).casefold())
        return protect(_inline(label, references) + " (" + destination + ")") if destination else original
    text = re.sub(r"!?\[([^\]]+)\]\[([^\]]*)\]",
                  lambda match: reference(match[1], match[2], match[0]), text)
    text = re.sub(r"(?<![\w\\])\[([^\]\n]+)\](?![\[(])",
                  lambda match: reference(match[1], "", match[0]), text)
    text = re.sub(r"<((?:https?://|mailto:)[^<>\s]+)>", lambda match: protect(match[1]), text)
    text = URL.sub(lambda match: protect(match[0]), text)
    text = re.sub(r"(?<![A-Za-z0-9_/\\])(\*\*|__|~~)(?=\S)(.+?)(?<=\S)\1(?![A-Za-z0-9_/\\])", r"\2", text)
    text = re.sub(r"(?<![A-Za-z0-9_/\\])(\*|_)(?=\S)(.+?)(?<=\S)\1(?![A-Za-z0-9_/\\])", r"\2", text)
    text = PATH.sub(lambda match: protect(match[0]), text)
    text = re.sub(r"\\([\\`*{}\[\]()#+.!_|>~-])", r"\1", text)
    token = re.compile(re.escape(marker) + r"(\d+)\ufff1")
    def restore(match):
        return token.sub(restore, literals[int(match[1])])
    return token.sub(restore, text)


def _table_cells(line):
    cells, value, fence = [], [], ""
    index = 0
    while index < len(line):
        character = line[index]
        if character == "\\" and index + 1 < len(line) and line[index + 1] == "|":
            value.append("|")
            index += 2
            continue
        if character == "`":
            end = index + 1
            while end < len(line) and line[end] == "`":
                end += 1
            ticks = line[index:end]
            if not fence:
                fence = ticks
            elif fence == ticks:
                fence = ""
            value.append(ticks)
            index = end
            continue
        if character == "|" and not fence:
            cells.append("".join(value).strip())
            value = []
        else:
            value.append(character)
        index += 1
    cells.append("".join(value).strip())
    if line.lstrip().startswith("|"):
        cells = cells[1:]
    if line.rstrip().endswith("|"):
        cells = cells[:-1]
    return cells


def plain_text(text):
    lines = text.splitlines(keepends=True)
    references, used_references = {}, set()
    code = ""
    for raw in lines:
        match = FENCE.match(raw.rstrip("\r\n"))
        if match:
            if not code:
                code = match[1]
            elif match[1][0] == code[0] and len(match[1]) >= len(code) and not match[2].strip():
                code = ""
            continue
        reference = REFERENCE.match(raw.rstrip("\r\n")) if not code else None
        if reference:
            references[reference[1].casefold()] = reference[2] or reference[3]
        elif not code and not raw.startswith(("    ", "\t")):
            visible = INLINE_CODE.sub("", raw)
            for candidate in re.finditer(r"(?<![\w\\])!?\[([^\]\n]+)\](?:\[([^\]]*)\])?", visible):
                used_references.add((candidate[2] or candidate[1]).casefold())
    output, code, table = [], "", False
    for index, raw in enumerate(lines):
        line = raw.rstrip("\r\n")
        ending = raw[len(line):]
        match = FENCE.match(line)
        if match:
            if not code:
                code = match[1]
                continue
            if match[1][0] == code[0] and len(match[1]) >= len(code) and not match[2].strip():
                code = ""
                continue
        if code or line.startswith(("    ", "\t")):
            output.append(raw)
            continue
        reference = REFERENCE.match(line)
        if reference:
            if reference[1].casefold() not in used_references:
                output.append(reference[1] + " (" + (reference[2] or reference[3]) + ")" + ending)
            continue
        if index + 1 < len(lines) and "|" in line:
            cells = _table_cells(lines[index + 1].strip())
            if cells and all(re.fullmatch(r":?-{3,}:?", cell) for cell in cells):
                table = True
                output.append("　".join(_inline(cell, references) for cell in _table_cells(line)) + ending)
                continue
        if table:
            cells = _table_cells(line)
            if cells and all(re.fullmatch(r":?-{3,}:?", cell) for cell in cells):
                continue
            if "|" in line:
                output.append("　".join(_inline(cell, references) for cell in cells) + ending)
                continue
            table = False
        if index and lines[index - 1].strip() and re.fullmatch(r" {0,3}=+\s*", line):
            continue
        if re.fullmatch(r" {0,3}(?:\*\s*){3,}| {0,3}(?:-\s*){3,}| {0,3}(?:_\s*){3,}", line):
            continue
        heading = re.match(r"^ {0,3}#{1,6}\s+", line)
        if heading:
            line = re.sub(r"\s+#+\s*$", "", line[heading.end():])
        line = re.sub(r"^ {0,3}(?:>\s?)+", "", line)
        line = re.sub(r"^(\s{0,3})[-+*]\s+", r"\1• ", line)
        line = re.sub(r"^(\s{0,3})(\d{1,9})[.)]\s+", r"\1\2）", line)
        output.append(_inline(line, references) + ending)
    result = "".join(output)
    if not result.strip():
        raise ValueError("outbound text is empty after plain-text conversion")
    return result


def split_text(text, limit=MESSAGE_LIMIT):
    """Prefer complete paragraphs, lines and sentences, retaining every output character."""
    if type(limit) is not int or limit < 1:
        raise ValueError("message size limit must be a positive integer")
    parts = []
    while len(text) > limit:
        urls = [match.span() for match in URL.finditer(text)]
        def safe(position, spans=urls):
            return position > 0 and not any(start < position < end for start, end in spans)
        candidates = []
        for pattern in (r"\r?\n\s*\r?\n", r"\r?\n", r"[。！？!?；;]+[”’\"')）]*|\.(?=\s|$)", r"\s+"):
            candidates = [match.end() for match in re.finditer(pattern, text[:limit]) if safe(match.end())]
            if candidates:
                break
        boundary = candidates[-1] if candidates else limit
        for start, end in urls:
            if start < boundary < end and start:
                boundary = start
                break
        parts.append(text[:boundary])
        text = text[boundary:]
    if text:
        parts.append(text)
    return parts
