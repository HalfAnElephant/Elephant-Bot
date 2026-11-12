from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from qqbot.capabilities.process import ProcessService


@pytest.mark.parametrize('stdin', ['', 'input after exit'])
async def test_already_closed_child_stdin_does_not_fail_supervisor(monkeypatch, tmp_path, stdin):
    import qqbot.capabilities.process as module

    writer = Mock()
    writer.write.side_effect = RuntimeError('uvloop transport is closed')
    writer.is_closing.return_value = True
    child = SimpleNamespace(pid=123, returncode=0, stdin=writer,
        stdout=SimpleNamespace(read=AsyncMock(return_value=b'')), wait=AsyncMock(return_value=0))
    monkeypatch.setattr(module.sys, 'platform', 'linux')
    monkeypatch.setattr(module.shutil, 'which', lambda _: '/usr/bin/bwrap')
    monkeypatch.setattr(module, 'start_process', AsyncMock(return_value=child))
    monkeypatch.setattr(module, 'stop_process', AsyncMock())
    context = SimpleNamespace(workspace=str(tmp_path), task_id='task', run_id='run', fence=1)
    service = ProcessService()
    handle = await service.start({'argv': ['true'], 'stdin': stdin}, context)
    result = await service.wait({'process_ref': handle['process_ref'], 'wait_seconds': 1}, context)
    assert result['exit_code'] == 0 and not service.active_run(context)
    writer.close.assert_called_once()
    assert writer.write.call_count == bool(stdin)
