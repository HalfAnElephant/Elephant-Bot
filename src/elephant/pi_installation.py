"""Resolve the Pi selected by the installer without altering saved settings."""

import os
import stat
from pathlib import Path

DEFAULT_PI = "node_modules/.bin/pi"
MARKER = ".elephant-pi"


def runtime_pi_path(configured: str, *, root: Path | None = None) -> str:
    """Only replace the bundle default; an explicit custom path always wins."""
    root = (Path.cwd() if root is None else root).absolute()
    bundled = root / DEFAULT_PI
    target = Path(configured)
    if (target if target.is_absolute() else root / target) != bundled:
        return configured
    try:
        fd = os.open(root / MARKER, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    except FileNotFoundError:
        return configured
    with os.fdopen(fd, encoding="utf-8") as file:
        info = os.fstat(file.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid not in {0, os.geteuid()} or info.st_mode & 0o022:
            raise ValueError("untrusted Pi installation selection")
        raw = file.read(4097)
    if len(raw) > 4096 or not raw.endswith("\n"):
        raise ValueError("invalid Pi installation selection")
    selected = raw.removesuffix("\n")
    if selected == "bundled":
        return str(bundled)
    if not Path(selected).is_absolute() or any(ord(ch) < 32 or ord(ch) == 127 for ch in selected):
        raise ValueError("invalid selected Pi executable")
    return selected
