import socket
import tempfile
from pathlib import Path

import pytest

from qqbot.cli import clean_stale_socket


@pytest.fixture
def short_path():
    with tempfile.TemporaryDirectory(dir="/tmp", prefix="eb-") as folder:
        yield Path(folder) / "core.sock"


def test_stale_socket_cleanup_preserves_live_and_unexpected_paths(short_path):
    path = short_path
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
        server.bind(str(path))
        server.listen(1)
        with pytest.raises(RuntimeError, match="still active"):
            clean_stale_socket(path)
        assert path.exists()
    clean_stale_socket(path)
    assert not path.exists()
    path.write_text("not a socket")
    with pytest.raises(RuntimeError, match="unexpected"):
        clean_stale_socket(path)
    assert path.read_text() == "not a socket"
