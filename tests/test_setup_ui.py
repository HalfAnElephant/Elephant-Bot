import getpass
import io
import warnings

import pytest

from elephant import setup_ui


def test_numbered_fallback_formats_plain_chinese_and_validates_choice():
    output = io.StringIO()
    ui = setup_ui.SetupUI(stdin=io.StringIO("0\n2\n\n"), stdout=output)
    ui.banner()
    ui.section("选择服务商", "可以稍后更改")
    with ui.progress("加载"):
        ui.status("ok", "已完成")
    assert ui.choose("使用哪一个？", ("OpenAI", "兼容服务")) == 1
    assert ui.confirm("启用 QQ？") is False
    rendered = output.getvalue()
    assert "\x1b" not in rendered
    assert "初始化向导" in rendered
    assert "1. OpenAI（默认）" in rendered
    assert "请输入 1 至 2" in rendered
    assert "[进行中] 加载" in rendered
    assert "[完成] 已完成" in rendered


def test_ask_default_and_closed_input():
    ui = setup_ui.SetupUI(stdin=io.StringIO("\n"), stdout=io.StringIO())
    assert ui.ask("模型名称", default="example") == "example"
    with pytest.raises(KeyboardInterrupt):
        ui.ask("下一项")


def test_hidden_prompt_cannot_fall_back_to_visible_input(monkeypatch):
    def unprotected(*args, **kwargs):
        warnings.warn("echo is on", getpass.GetPassWarning)
        pytest.fail("must stop before reading the secret")

    monkeypatch.setattr(getpass, "getpass", unprotected)
    ui = setup_ui.SetupUI(stdin=io.StringIO("fake-key"), stdout=io.StringIO())
    with pytest.raises(getpass.GetPassWarning):
        ui.ask("密钥", secret=True)
    assert "fake-key" not in ui.stdout.getvalue()


def test_no_color_on_a_terminal(monkeypatch):
    class Terminal(io.StringIO):
        def isatty(self):
            return True

    monkeypatch.setenv("TERM", "xterm")
    monkeypatch.setenv("NO_COLOR", "")
    ui = setup_ui.SetupUI(stdin=Terminal(), stdout=Terminal())
    ui.banner()
    assert ui.terminal
    assert "\x1b" not in ui.stdout.getvalue()


@pytest.mark.skipif(setup_ui.termios is None, reason="POSIX terminal only")
@pytest.mark.parametrize("keys,expected", [([b"\x1b[A", b"\r"], 1), ([b"\x1b"], None)])
def test_arrow_key_wrap_or_cancel_restores_terminal(monkeypatch, keys, expected):
    modes = []
    monkeypatch.setattr(setup_ui.tty, "setcbreak", lambda fd: modes.append(("cbreak", fd)))
    monkeypatch.setattr(setup_ui.termios, "tcsetattr", lambda fd, when, mode: modes.append((fd, mode)))
    keystrokes = iter(keys)
    ui = setup_ui.SetupUI(stdin=io.StringIO(), stdout=io.StringIO())
    monkeypatch.setattr(ui, "_terminal_fds", lambda options: (5, ["original"]))
    monkeypatch.setattr(ui, "_key", lambda fd: next(keystrokes))
    if expected is None:
        with pytest.raises(KeyboardInterrupt):
            ui.choose("选择", ("一", "二"))
    else:
        assert ui.choose("选择", ("一", "二")) == expected
        assert "✓ 二" in ui.stdout.getvalue()
    # The last redraw erases the two options and keyboard guide, then leaves
    # one status line; preceding wizard sections are outside the redraw block.
    last_redraw = ui.stdout.getvalue().rsplit("\x1b[3A\r", 1)[-1]
    assert "↑/↓" not in last_redraw
    assert ("已取消" if expected is None else "✓ 二") in last_redraw
    assert modes == [("cbreak", 5), (5, ["original"])]


def test_wide_banner_uses_padded_frame_and_numbered_sections(monkeypatch):
    ui = setup_ui.SetupUI(stdin=io.StringIO(), stdout=io.StringIO())
    ui.terminal = True
    monkeypatch.setattr(ui, "_width", lambda: 80)
    ui.banner()
    ui.section("1/4  模型服务", "选择常用模型服务商")
    lines = ui.stdout.getvalue().splitlines()
    frame = [line for line in lines if line.startswith(("  ╭", "  │", "  ╰"))]
    assert len(frame) == 7
    assert {setup_ui._columns(line) for line in frame} == {56}
    assert any("01 / 04" in line and "模型服务" in line for line in lines)
