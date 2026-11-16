import base64
import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from elephant.adapters.execution.world import LocalExecution
from elephant.capabilities.gateway import CapabilityRegistry, ToolGateway
from elephant.capabilities.mail import register_mail
from elephant.capabilities.types import ToolContext


@pytest.fixture
def mail_tools(tmp_path):
    call = AsyncMock(return_value={'allowed': True, 'id': 'draft'})
    registry = CapabilityRegistry()
    register_mail(registry, call, LocalExecution())
    context = ToolContext(task_id='task', run_id='run', fence=1, principal={}, workspace=str(tmp_path))
    return SimpleNamespace(call=call, registry=registry, context=context, root=tmp_path)


async def test_account_tool_rejects_credential_fields_before_journaling(mail_tools):
    ledger = SimpleNamespace(prepare=AsyncMock(), transition=AsyncMock())
    gateway = ToolGateway(mail_tools.registry, authority=lambda _: True, permission=lambda *_: True, ledger=ledger)
    secret = 'synthetic-password-must-not-be-journaled'
    with pytest.raises(ValueError) as error:
        await gateway.invoke('mail.accounts', {'operation': 'update', 'account_id': 'demo', 'expected_revision': 1,
            'changes': {'spec': {'password': secret}}}, mail_tools.context)
    assert secret not in str(error.value)
    ledger.prepare.assert_not_awaited()
    mail_tools.call.assert_not_awaited()


async def test_send_preparation_snapshots_only_authorized_workspace_files(mail_tools):
    (mail_tools.root / 'report.txt').write_bytes(b'original bytes')
    _capability, execute, classify = mail_tools.registry.entries['mail.send.prepare']
    args = {'account_id': 'demo', 'to': ['reader@example.org'], 'body': 'report', 'attachments': ['report.txt']}
    assert classify(args, mail_tools.context).effect == 'write'
    await execute(args, mail_tools.context)
    call = next(item for item in mail_tools.call.await_args_list if item.args[0] == 'mail')
    attachment = call.args[1]['arguments']['attachments'][0]
    assert attachment == {'filename': 'report.txt', 'mime_type': 'text/plain',
        'data_base64': base64.b64encode(b'original bytes').decode()}
    assert any(item.args[0] == 'authorize' and item.args[1]['action'] == 'workspace.file.read'
        for item in mail_tools.call.await_args_list)


async def test_send_preparation_rejects_symlink_and_parent_escape(mail_tools):
    outside = mail_tools.root.parent / 'outside-mail-file'
    outside.write_bytes(b'outside')
    (mail_tools.root / 'link.txt').symlink_to(outside)
    execute = mail_tools.registry.entries['mail.send.prepare'][1]
    for path in ('link.txt', '../outside-mail-file'):
        with pytest.raises((OSError, ValueError)):
            await execute({'account_id': 'demo', 'to': ['reader@example.org'], 'body': 'report',
                'attachments': [path]}, mail_tools.context)
    assert all(item.args[0] != 'mail' for item in mail_tools.call.await_args_list)


async def test_attachment_download_preserves_bytes_and_existing_files(mail_tools):
    content = b'original encoded attachment bytes'

    async def call(operation, payload):
        if operation == 'authorize':
            return {'allowed': True}
        return {'data_base64': base64.b64encode(content).decode(),
            'sha256': hashlib.sha256(content).hexdigest(), 'size': len(content)}

    registry = CapabilityRegistry()
    register_mail(registry, call, LocalExecution())
    _capability, execute, classify = registry.entries['mail.attachment.save']
    arguments = {'account_id': 'demo', 'folder': 'INBOX', 'uid': 1, 'uidvalidity': 7,
        'part': '2', 'path': 'invoice.pdf'}
    assert classify(arguments, mail_tools.context).name == 'workspace.file.write'
    result = await execute(arguments, mail_tools.context)
    assert result['sha256'] == hashlib.sha256(content).hexdigest()
    assert (mail_tools.root / 'invoice.pdf').read_bytes() == content
    assert await execute(arguments, mail_tools.context) == result
    (mail_tools.root / 'invoice.pdf').write_bytes(b'keep existing file')
    with pytest.raises(ValueError, match='different file'):
        await execute(arguments, mail_tools.context)
    assert (mail_tools.root / 'invoice.pdf').read_bytes() == b'keep existing file'


async def test_attachment_download_rechecks_file_authority_before_writing(mail_tools):
    async def call(operation, payload):
        if operation == 'authorize':
            return {'allowed': False}
        return {'data_base64': base64.b64encode(b'file').decode(),
            'sha256': hashlib.sha256(b'file').hexdigest(), 'size': 4}

    registry = CapabilityRegistry()
    register_mail(registry, call, LocalExecution())
    with pytest.raises(PermissionError):
        await registry.entries['mail.attachment.save'][1]({'account_id': 'demo', 'folder': 'INBOX',
            'uid': 1, 'uidvalidity': 7, 'part': '2', 'path': 'file.txt'}, mail_tools.context)
    assert not (mail_tools.root / 'file.txt').exists()


@pytest.mark.parametrize('retry_prepared,expected', [(False, 'FAILED'), (True, 'VERIFIED')])
async def test_send_verification_distinguishes_undispatched_draft_and_prepared_retry(mail_tools, retry_prepared, expected):
    from elephant.capabilities.types import Action
    from elephant.harness.gateway import RunGateway

    call = AsyncMock(return_value={'id': 'draft', 'state': 'DRAFT'})
    gateway = RunGateway(mail_tools.root, 'model', '', 'https://model.invalid', call=call)
    try:
        state, _receipt = await gateway.verify(mail_tools.context, Action('mail.send', ()),
            {'draft_id': 'draft'}, {'retry_prepared': retry_prepared})
        assert state == expected
    finally:
        gateway.revoke()
