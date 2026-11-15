"""Original file staging is bounded, recoverable and isolated from executable content."""
import asyncio
import hashlib
import json
import os
import threading
from types import SimpleNamespace

import pytest
from sqlalchemy import insert, update

from qqbot.adapters.execution.world import LocalExecution
from qqbot.adapters.qq_official.events import extra_segments
from qqbot.harness import attachments
from qqbot.storage.agent_schema import dialogue_inputs


def source(filename='report.docx'):
    return {'source': 'event-1', 'message_id': 'm1', 'attachment_ref': 'file-1',
            'filename': filename, 'url': 'https://cdn.example/file'}


async def test_original_binary_is_saved_verbatim_with_stable_metadata(tmp_path):
    context = SimpleNamespace(workspace=str(tmp_path))
    original = b'PK\x03\x04opaque original document\x00\xff'
    item = source()
    identity = attachments.attachment_identity(item, {'workspace_id': 'alpha'})
    world = LocalExecution()
    first = await attachments.stage_attachment(world, context, item, identity, original)
    again = await attachments.stage_attachment(world, context, item, identity, original)
    assert first == again
    assert (tmp_path / first['path']).read_bytes() == original
    assert first['sha256'] == hashlib.sha256(original).hexdigest()
    assert first['filename'] == 'report.docx' and first['size'] == len(original)
    with pytest.raises(ValueError, match='refusing overwrite'):
        await attachments.stage_attachment(world, context, item, identity, b'different content')
    assert (tmp_path / first['path']).read_bytes() == original


async def test_filename_and_existing_link_cannot_escape_workspace(tmp_path):
    work = tmp_path / 'work'
    work.mkdir()
    outside = tmp_path / 'outside.txt'
    outside.write_bytes(b'preserved')
    context = SimpleNamespace(workspace=str(work))
    item = source('../../outside.txt')
    identity = attachments.attachment_identity(item, {'workspace_id': 'alpha'})
    path = attachments.attachment_path(item, identity)
    assert '/' not in path and '\\' not in path
    (work / path).symlink_to(outside)
    with pytest.raises((ValueError, OSError)):
        await attachments.stage_attachment(LocalExecution(), context, item, identity, b'changed')
    assert outside.read_bytes() == b'preserved'


def test_cache_recovery_is_scoped_and_checks_original_hash(tmp_path):
    item = source()
    alpha = attachments.attachment_identity(item, {'workspace_id': 'alpha'})
    beta = attachments.attachment_identity(item, {'workspace_id': 'beta'})
    attachments.cache_attachment(tmp_path, alpha, b'original', source_id=item['source'])
    assert attachments.cached_attachment(tmp_path, alpha) == b'original'
    assert attachments.cached_attachment(tmp_path, beta) is None
    receipt = json.loads((tmp_path / 'inbound' / alpha / 'receipt.json').read_text())
    (tmp_path / 'inbound' / alpha / receipt['sha256']).write_bytes(b'tampered')
    with pytest.raises(ValueError, match='integrity'):
        attachments.cached_attachment(tmp_path, alpha)


def test_cache_and_download_budgets_are_enforced(tmp_path, monkeypatch):
    identity = attachments.attachment_identity(source(), {})
    monkeypatch.setattr(attachments, 'MAX_FILE_BYTES', 3)
    with pytest.raises(ValueError, match='byte limit'):
        attachments.cache_attachment(tmp_path, identity, b'1234', source_id='event')
    monkeypatch.setattr(attachments, 'MAX_CACHE_BYTES', 4097)
    with pytest.raises(ValueError, match='budget exhausted'):
        attachments.cache_attachment(tmp_path, identity, b'12', source_id='event')


def test_cache_gc_preserves_active_sources(db, clock, tmp_path):
    identities = [attachments.attachment_identity(source(name), {}) for name in ('active.pdf', 'idle.pptx')]
    for identity, event in zip(identities, ('active', 'idle'), strict=True):
        attachments.cache_attachment(tmp_path, identity, b'original', source_id=event)
        old = clock.now().timestamp() - 3600
        os.utime(tmp_path / 'inbound' / identity, (old, old))
    with db.transaction() as conn:
        conn.execute(insert(dialogue_inputs).values(id='pending', dialogue_id='dialogue', owner_id='owner',
            event_json='{}', context_json=json.dumps({'files': [{'source': 'active'}]}), state='PENDING',
            created_at=clock.now().isoformat()))
    assert attachments.cleanup_attachment_cache(tmp_path, db, clock.now(), retention_seconds=60) == 1
    assert (tmp_path / 'inbound' / identities[0]).exists()
    with db.transaction() as conn:
        conn.execute(update(dialogue_inputs).where(dialogue_inputs.c.id == 'pending').values(handled_at=clock.now().isoformat()))
    assert attachments.cleanup_attachment_cache(tmp_path, db, clock.now(), retention_seconds=60) == 1


async def test_atomic_workspace_write_failure_leaves_no_partial_file(tmp_path, monkeypatch):
    def fail(_descriptor):
        raise OSError('fixture IO failure')
    monkeypatch.setattr(os, 'fsync', fail)
    with pytest.raises(OSError):
        await LocalExecution().write_file(SimpleNamespace(workspace=str(tmp_path)), 'original.pdf', b'whole file')
    assert list(tmp_path.iterdir()) == []


def test_cache_gc_keeps_originals_needed_by_future_schedules(db, clock, tmp_path):
    from qqbot.storage.schedule_schema import schedules
    item = source('annual-report.pptx')
    identity = attachments.attachment_identity(item, {})
    attachments.cache_attachment(tmp_path, identity, b'original', source_id=item['source'])
    old = clock.now().timestamp() - 40 * 86400
    os.utime(tmp_path / 'inbound' / identity, (old, old))
    with db.transaction() as conn:
        conn.execute(insert(schedules).values(id='future', owner_id='owner', principal='principal', conversation='private',
            creation_key='annual', creation_json='{}', event_json=json.dumps({'event_id': 'schedule-source'}),
            dialogue_id='dialogue', spec_json='{}', revision=1, enabled=1,
            source_context_json=json.dumps({'files': [item]}),
            created_at=clock.now().isoformat(), updated_at=clock.now().isoformat()))
    assert attachments.cleanup_attachment_cache(tmp_path, db, clock.now()) == 0
    assert (tmp_path / 'inbound' / identity).exists()
    with db.transaction() as conn:
        conn.execute(update(schedules).where(schedules.c.id == 'future').values(enabled=0))
    assert attachments.cleanup_attachment_cache(tmp_path, db, clock.now()) == 1


async def test_atomic_workspace_writers_preserve_exclusive_creation(tmp_path):
    world, context = LocalExecution(), SimpleNamespace(workspace=str(tmp_path))
    results = await asyncio.gather(world.write_file(context, 'same.bin', b'first'),
        world.write_file(context, 'same.bin', b'second'), return_exceptions=True)
    assert sum(isinstance(result, FileExistsError) for result in results) == 1
    assert (tmp_path / 'same.bin').read_bytes() in {b'first', b'second'}
    assert len(list(tmp_path.iterdir())) == 1


async def test_cancelled_atomic_write_drains_and_removes_temporary_file(tmp_path, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    original_fsync = os.fsync
    def pause(descriptor):
        entered.set()
        assert release.wait(3)
        original_fsync(descriptor)
    monkeypatch.setattr(os, 'fsync', pause)
    writing = asyncio.create_task(LocalExecution().write_file(SimpleNamespace(workspace=str(tmp_path)), 'whole.bin', b'whole'))
    assert await asyncio.to_thread(entered.wait, 2)
    writing.cancel()
    await asyncio.sleep(0)
    assert not writing.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await writing
    assert (tmp_path / 'whole.bin').read_bytes() == b'whole'
    assert len(list(tmp_path.iterdir())) == 1


def test_attachment_metadata_and_image_filename_fallback():
    segments = extra_segments({'attachments': [
        {'url': '//cdn.example/file', 'filename': 'data.csv', 'content_type': 'text/csv', 'file_size': 123},
        {'url': '//cdn.example/image', 'name': 'photo.PNG'},
    ]})
    assert segments[0].type == 'file' and segments[0].size == 123
    assert segments[0].filename == 'data.csv' and segments[0].source_url == 'https://cdn.example/file'
    assert segments[1].type == 'image' and segments[1].filename == 'photo.PNG'


async def test_process_exit_after_atomic_publish_leaves_readable_single_link_file(tmp_path):
    import subprocess
    import sys
    script = '''
import asyncio, os, sys
from types import SimpleNamespace
from qqbot.adapters.execution import world
publish = world._publish_exclusive
def exit_after_publish(*args):
    publish(*args)
    os._exit(73)
world._publish_exclusive = exit_after_publish
asyncio.run(world.LocalExecution().write_file(SimpleNamespace(workspace=sys.argv[1]), 'published.bin', b'complete'))
'''
    result = await asyncio.to_thread(subprocess.run, [sys.executable, '-c', script, str(tmp_path)],
        capture_output=True, timeout=5, check=False)
    assert result.returncode == 73, result.stderr
    assert (tmp_path / 'published.bin').stat().st_nlink == 1
    assert await LocalExecution().read_file(SimpleNamespace(workspace=str(tmp_path)), 'published.bin', 0, 100) == b'complete'
