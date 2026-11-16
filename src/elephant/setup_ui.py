"""Terminal presentation for the setup wizard; no setup or credential logic."""

from __future__ import annotations

import getpass
import os
import re
import select
import sys
import unicodedata
import warnings
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import TextIO

try:
    import termios
    import tty
except ImportError:  # Non-POSIX terminals use numbered menus.
    termios = tty = None


_STEP = re.compile(r"^(\d+)/(\d+)\s+(.+)$")


def _columns(value: str) -> int:
    return sum(
        (2 if unicodedata.east_asian_width(char) in {"F", "W"} else 1)
        for char in value
        if not unicodedata.combining(char)
    )


class SetupUI:
    """Small Chinese-language controls that also work with redirected input."""

    def __init__(self, *, stdin: TextIO | None = None, stdout: TextIO | None = None):
        self.stdin = sys.stdin if stdin is None else stdin
        self.stdout = sys.stdout if stdout is None else stdout
        self.terminal = (
            self.stdin.isatty() and self.stdout.isatty() and os.environ.get("TERM", "") not in {"", "dumb"}
        )
        self.color = self.terminal and "NO_COLOR" not in os.environ

    def _style(self, text: str, codes: str) -> str:
        return f"\x1b[{codes}m{text}\x1b[0m" if self.color else text

    def _line(self, text: str = "") -> None:
        print(text, file=self.stdout, flush=True)

    def _read(self, prompt: str) -> str:
        print(prompt, end="", file=self.stdout, flush=True)
        try:
            result = self.stdin.readline()
        except EOFError as exc:
            raise KeyboardInterrupt from exc
        if not result:
            raise KeyboardInterrupt
        return result.rstrip("\r\n")

    def _width(self) -> int:
        try:
            return os.get_terminal_size(self.stdout.fileno()).columns
        except (OSError, ValueError):
            return 0

    def banner(self) -> None:
        self._line()
        if not self.terminal or self._width() < 56:
            self._line("  Elephant · 初始化向导")
            self._line("  配置模型，然后按需接入 QQ。")
            return

        # Keep styled text separate from display-width calculations; CJK text
        # takes two cells per character on standard UTF-8 terminals.
        width = 52

        def edge(text: str) -> str:
            return self._style(text, "36")

        self._line("  " + edge("╭" + "─" * width + "╮"))
        rows = (
            ("  .----.", "", ""),
            (" / o  o \\", "E L E P H A N T", "1;36"),
            (r"( \_  _/ )", "初始化向导", "1"),
            (r" \__||__/", "配置模型，然后按需接入 QQ。", ""),
            (r"    \_)", "", ""),
        )
        for mark, label, code in rows:
            raw = f"   {mark:<11}  {label}"
            padding = " " * (width - _columns(raw))
            text = f"   {edge(f'{mark:<11}')}  {self._style(label, code) if code else label}{padding}"
            self._line(f"  {edge('│')}{text}{edge('│')}")
        self._line("  " + edge("╰" + "─" * width + "╯"))

    def section(self, title: str, hint: str | None = None) -> None:
        self._line()
        match = _STEP.fullmatch(title)
        if match:
            step, total, name = match.groups()
            badge = self._style(f" {step.zfill(2)} / {total.zfill(2)} ", "1;30;46")
            rule = f"  {self._style('─' * 12, '36')}" if self.terminal and self._width() >= 56 else ""
            self._line(f"  {badge}  {self._style(name, '1')}{rule}")
        else:
            self._line(f"  {self._style('◆', '36')}  {self._style(title, '1')}")
        if hint:
            self._line(f"     {hint}")

    def status(self, kind: str, message: str) -> None:
        label, color = {
            "ok": ("完成", "1;32"),
            "warn": ("注意", "1;33"),
            "error": ("未完成", "1;31"),
            "info": ("提示", "1;36"),
        }[kind]
        self._line(f"     {self._style(f'[{label}]', color)} {message}")

    @contextmanager
    def progress(self, message: str) -> Iterator[None]:
        """Announce pending work; the caller reports its actual outcome."""
        self._line(f"     {self._style('[进行中]', '1;36')} {message}")
        yield

    def ask(self, question: str, *, default: str | None = None, secret: bool = False) -> str:
        if secret:
            if default is not None:
                raise ValueError("A secret prompt cannot expose a default")
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("error", getpass.GetPassWarning)
                    return getpass.getpass(f"     {question}（输入不可见）：", stream=self.stdout)
            except EOFError as exc:
                raise KeyboardInterrupt from exc
        suffix = f" [{default}]" if default is not None else ""
        answer = self._read(f"     {question}{suffix}：").strip()
        return answer or (default if default is not None else "")

    def confirm(self, question: str, *, default: bool = False) -> bool:
        return self.choose(question, ("是", "否"), default=0 if default else 1) == 0

    def choose(self, question: str, options: Sequence[str], *, default: int = 0) -> int:
        if not 0 <= default < len(options):
            raise ValueError("Choice requires options and a valid default index")
        self._line(f"     {self._style(question, '1')}")
        terminal_fds = self._terminal_fds(options)
        if terminal_fds is not None:
            return self._menu(options, default, *terminal_fds)
        for index, option in enumerate(options):
            suffix = "（默认）" if index == default else ""
            self._line(f"       {index + 1}. {option}{suffix}")
        while True:
            answer = self._read(f"     输入编号，或回车选择默认项 [{default + 1}]：").strip()
            if not answer:
                return default
            if answer in {str(i + 1) for i in range(len(options))}:
                return int(answer) - 1
            self.status("warn", f"请输入 1 至 {len(options)}。")

    def _terminal_fds(self, options: Sequence[str]) -> tuple[int, list] | None:
        if not self.terminal or termios is None or tty is None:
            return None
        try:
            fd = self.stdin.fileno()
            size = os.get_terminal_size(self.stdout.fileno())
            if (
                not os.isatty(fd)
                or size.lines <= len(options) + 2
                or size.columns < max([58, *(_columns(item) + 10 for item in options)])
            ):
                return None
            return fd, termios.tcgetattr(fd)
        except (OSError, ValueError, termios.error):
            return None

    @staticmethod
    def _key(fd: int) -> bytes:
        key = os.read(fd, 1)
        if key != b"\x1b":
            return key
        if not select.select([fd], [], [], 0.08)[0]:
            return key
        prefix = os.read(fd, 1)
        if prefix in {b"[", b"O"}:
            sequence = bytearray()
            for _ in range(12):
                if not select.select([fd], [], [], 0.08)[0]:
                    break
                code = os.read(fd, 1)
                if not code:
                    break
                sequence.extend(code)
                if b"@" <= code <= b"~":
                    break
            return key + prefix + sequence
        return key + prefix

    def _menu(self, options: Sequence[str], default: int, fd: int, old_mode: list) -> int:
        guide = "     ↑/↓ 选择   Enter 确认   Esc 取消"
        current = default
        drawn = False

        def render() -> None:
            nonlocal drawn
            if drawn:
                self.stdout.write(f"\x1b[{len(options) + 1}A\r")
            for index, option in enumerate(options):
                marker = "❯" if index == current else " "
                row = f"       {marker} {option}"
                styled_row = self._style(row, "1;36") if index == current else row
                self.stdout.write(f"\x1b[2K{styled_row}\n")
            self.stdout.write(f"\x1b[2K{self._style(guide, '2')}\n")
            self.stdout.flush()
            drawn = True

        def collapse(summary: str) -> None:
            # Clear only the active choice block, retaining all earlier setup
            # decisions and shell scrollback. Leave one readable result line.
            rows = len(options) + 1
            self.stdout.write(f"\x1b[{rows}A\r")
            self.stdout.write("\x1b[2K\n" * rows)
            self.stdout.write(f"\x1b[{rows}A\r")
            self._line(summary)

        try:
            tty.setcbreak(fd)
            render()
            while True:
                key = self._key(fd)
                if key in {b"", b"\x03", b"\x04", b"\x1b"}:
                    raise KeyboardInterrupt
                if key in {b"\r", b"\n"}:
                    collapse(f"       {self._style('✓', '1;32')} {options[current]}")
                    return current
                if key in {b"\x1b[A", b"\x1bOA"}:
                    current = (current - 1) % len(options)
                elif key in {b"\x1b[B", b"\x1bOB"}:
                    current = (current + 1) % len(options)
                elif len(key) == 1 and b"1" <= key <= b"9" and int(key) <= len(options):
                    current = int(key) - 1
                else:
                    continue
                render()
        except KeyboardInterrupt:
            if drawn:
                collapse(f"       {self._style('已取消', '33')}")
            raise
        finally:
            termios.tcsetattr(fd, termios.TCSANOW, old_mode)
