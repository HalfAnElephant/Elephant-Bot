"""Preserve original inbound files and stage them through the selected execution world."""
import fcntl
import hashlib
import json
import os
import re
import shutil
import stat
import tempfile
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

MAX_FILES = 4
MAX_FILE_BYTES = 20 * 1024 * 1024
MAX_CACHE_BYTES = 256 * 1024 * 1024


def attachment_identity(attachment, scope):
    source = {key: attachment.get(key) for key in ('source', 'message_id', 'attachment_ref', 'filename')}
    return hashlib.sha256(json.dumps([scope, source], sort_keys=True).encode()).hexdigest()


def _cache_root(content_root):
    root = Path(content_root) / 'inbound'
    if root.is_symlink():
        raise ValueError('attachment cache cannot be a symlink')
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    return root



@contextmanager
def _cache_lock(root, *, exclusive=False):
    descriptor = os.open(root / '.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode) or os.fstat(descriptor).st_nlink != 1:
            raise ValueError('attachment cache lock must be a regular file')
        fcntl.flock(descriptor, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        yield
    finally:
        os.close(descriptor)

def _read_regular(path, limit):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as source:
        info = os.fstat(source.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > limit:
            raise ValueError('attachment cache requires a bounded regular file')
        data = source.read(limit + 1)
    if len(data) > limit:
        raise ValueError('attachment cache exceeds byte limit')
    return data


def _cached_attachment(root, identity):
    if not re.fullmatch(r"[0-9a-f]{64}", identity):
        raise ValueError("invalid attachment cache identity")
    directory = root / identity
    if directory.is_symlink():
        raise ValueError('attachment cache entry cannot be a symlink')
    try:
        receipt = json.loads(_read_regular(directory / 'receipt.json', 4096))
    except FileNotFoundError:
        return None
    if not re.fullmatch(r'[0-9a-f]{64}', receipt.get('sha256', '')):
        raise ValueError('invalid attachment cache receipt')
    data = _read_regular(directory / receipt['sha256'], MAX_FILE_BYTES)
    if len(data) != receipt['size'] or hashlib.sha256(data).hexdigest() != receipt['sha256']:
        raise ValueError('attachment cache integrity mismatch')
    os.utime(directory, None, follow_symlinks=False)
    return data


def cached_attachment(content_root, identity):
    root = _cache_root(content_root)
    with _cache_lock(root):
        return _cached_attachment(root, identity)


def cache_attachment(content_root, identity, data, *, source_id):
    if len(data) > MAX_FILE_BYTES:
        raise ValueError('attachment exceeds byte limit')
    root = _cache_root(content_root)
    with _cache_lock(root, exclusive=True):
        existing = _cached_attachment(root, identity)
        if existing is not None:
            if existing != data:
                raise ValueError('original attachment identity changed')
            return
        usage = sum(entry.stat(follow_symlinks=False).st_size for directory in root.iterdir()
            if directory.is_dir() and not directory.is_symlink() for entry in directory.iterdir()
            if entry.is_file() and not entry.is_symlink())
        if usage + len(data) + 4096 > MAX_CACHE_BYTES:
            raise ValueError('inbound attachment cache budget exhausted')
        digest = hashlib.sha256(data).hexdigest()
        temporary = Path(tempfile.mkdtemp(prefix='.incoming-', dir=root))
        try:
            receipt = json.dumps({'sha256': digest, 'size': len(data), 'source_id': source_id}).encode()
            for name, content in ((digest, data), ('receipt.json', receipt)):
                with (temporary / name).open('xb') as target:
                    target.write(content)
                    target.flush()
                    os.fsync(target.fileno())
            directory = os.open(temporary, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            os.rename(temporary, root / identity)
            directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            shutil.rmtree(temporary, ignore_errors=True)


def cleanup_attachment_cache(content_root, db, now, *, retention_seconds=30 * 86400):
    """Retain active input sources while expiring idle cached originals and crash debris."""
    from sqlalchemy import select

    from qqbot.storage.agent_schema import dialogue_inputs
    from qqbot.storage.schedule_schema import schedules
    from qqbot.storage.task_schema import runs, tasks

    root = _cache_root(content_root)
    removed = 0
    with _cache_lock(root, exclusive=True), db.transaction() as conn:
        active = set()
        def keep(context):
            active.update(item['id'] for item in context.get('source_manifest', ()) if item.get('kind') == 'inbox')
            active.update(item['source'] for item in context.get('files', ()) if item.get('source'))
        for raw in conn.execute(select(tasks.c.context_json).where(tasks.c.state.not_in(
                {'SUCCEEDED', 'PARTIAL', 'BLOCKED', 'FAILED', 'CANCELLED', 'EXPIRED'}))).scalars():
            keep(json.loads(raw))
        for raw in conn.execute(select(runs.c.request_json).where(runs.c.slot_reserved == 1)).scalars():
            keep(json.loads(raw).get('context', {}))
        for raw in conn.execute(select(dialogue_inputs.c.context_json).where(dialogue_inputs.c.handled_at.is_(None))).scalars():
            keep(json.loads(raw))
        for context_json, event_json in conn.execute(select(schedules.c.source_context_json, schedules.c.event_json).where(
                schedules.c.enabled == 1, schedules.c.deleted_at.is_(None))):
            keep(json.loads(context_json))
            source_id = json.loads(event_json).get('event_id')
            if source_id:
                active.add(source_id)
        for directory in root.iterdir():
            if not directory.is_dir() or directory.is_symlink():
                continue
            age = now.timestamp() - directory.stat().st_mtime
            if directory.name.startswith('.incoming-'):
                if age > 86400:
                    shutil.rmtree(directory)
                continue
            if age <= retention_seconds:
                continue
            receipt = json.loads(_read_regular(directory / 'receipt.json', 4096))
            if receipt.get('source_id') not in active:
                shutil.rmtree(directory)
                removed += 1
    return removed


def attachment_path(attachment, identity):
    original = attachment.get('filename') or PurePosixPath(urlsplit(attachment.get('url') or '').path).name or 'file'
    name = PurePosixPath(original.replace('\\', '/')).name
    name = re.sub(r'[^\w.\-]', '_', name).encode()[:100].decode(errors='ignore') or 'file'
    return 'attachment-' + identity + '-' + name


async def stage_attachment(execution, context, attachment, identity, data):
    path = attachment_path(attachment, identity)
    try:
        await execution.write_file(context, path, data)
    except FileExistsError:
        existing = await execution.read_file(context, path, 0, MAX_FILE_BYTES + 1)
        if existing != data:
            raise ValueError('existing workspace attachment differs; refusing overwrite') from None
    saved = await execution.read_file(context, path, 0, MAX_FILE_BYTES + 1)
    if saved != data:
        raise ValueError('saved attachment integrity mismatch')
    return {'source': attachment.get('source'), 'message_id': attachment.get('message_id'),
        'filename': attachment.get('filename'), 'mime_type': attachment.get('mime_type'),
        'path': path, 'sha256': hashlib.sha256(data).hexdigest(), 'size': len(data)}
