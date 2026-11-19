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

COPY_CHUNK = 1024 * 1024


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

def _open_regular(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    source = os.fdopen(fd, 'rb')
    info = os.fstat(fd)
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        source.close()
        raise ValueError('attachment cache requires a regular file without hardlinks')
    return source


def _read_regular(path, limit):
    with _open_regular(path) as source:
        if os.fstat(source.fileno()).st_size > limit:
            raise ValueError('attachment cache exceeds byte limit')
        data = source.read(limit + 1)
    if len(data) > limit:
        raise ValueError('attachment cache exceeds byte limit')
    return data


def _cached_receipt(root, identity):
    if not re.fullmatch(r"[0-9a-f]{64}", identity):
        raise ValueError("invalid attachment cache identity")
    directory = root / identity
    if directory.is_symlink():
        raise ValueError('attachment cache entry cannot be a symlink')
    try:
        receipt = json.loads(_read_regular(directory / 'receipt.json', 4096))
    except FileNotFoundError:
        return None
    if (not isinstance(receipt, dict) or not re.fullmatch(r'[0-9a-f]{64}', str(receipt.get('sha256')))
            or type(receipt.get('size')) is not int or receipt['size'] < 0):
        raise ValueError('invalid attachment cache receipt')
    return receipt


@contextmanager
def cached_attachment(content_root, identity):
    root = _cache_root(content_root)
    with _cache_lock(root):
        receipt = _cached_receipt(root, identity)
        source = None
        if receipt is not None:
            os.utime(root / identity, None, follow_symlinks=False)
            source = _open_regular(root / identity / receipt['sha256'])
    if source is None:
        yield None
    else:
        with source:
            yield source, receipt


def cached_transcript(content_root, identity, account):
    root = _cache_root(content_root)
    with _cache_lock(root):
        if _cached_receipt(root, identity) is None:
            return None
        if not re.fullmatch(r"[0-9a-f]{64}", account):
            raise ValueError("invalid transcription account")
        try:
            result = json.loads(_read_regular(root / identity / ("asr-" + account + ".json"), 65536))
        except FileNotFoundError:
            return None
        if not isinstance(result, dict) or not isinstance(result.get("text"), str) or len(result["text"]) > 10000:
            raise ValueError("invalid cached transcript")
        return result


def cache_transcript(content_root, identity, account, result):
    from elephant.configuration import atomic_write

    root = _cache_root(content_root)
    with _cache_lock(root, exclusive=True):
        if _cached_receipt(root, identity) is None or not re.fullmatch(r"[0-9a-f]{64}", account):
            raise ValueError("transcription requires a preserved original and account")
        encoded = json.dumps(result, ensure_ascii=False)
        if len(encoded.encode()) > 65536:
            raise ValueError("transcript exceeds cache limit")
        atomic_write(root / identity / ("asr-" + account + ".json"), encoded)


def cache_attachment(content_root, identity, download, *, source_id):
    root = _cache_root(content_root)
    with _cache_lock(root):
        if _cached_receipt(root, identity) is not None:
            return
        temporary = Path(tempfile.mkdtemp(prefix='.incoming-', dir=root))
    try:
        incoming = temporary / 'body'
        with incoming.open('x+b') as target:
            download(target)
            target.flush()
            os.fsync(target.fileno())
            size = target.tell()
            target.seek(0)
            digest = hashlib.file_digest(target, 'sha256').hexdigest()
        incoming.rename(temporary / digest)
        receipt = {'sha256': digest, 'size': size, 'source_id': source_id}
        with (temporary / 'receipt.json').open('x', encoding='utf-8') as target:
            json.dump(receipt, target)
            target.flush()
            os.fsync(target.fileno())
        with _cache_lock(root, exclusive=True):
            existing = _cached_receipt(root, identity)
            if existing is not None:
                if existing != receipt:
                    raise ValueError('original attachment identity changed')
                return
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

    from elephant.storage.agent_schema import dialogue_inputs
    from elephant.storage.schedule_schema import schedules
    from elephant.storage.task_schema import runs, tasks

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


async def stage_attachment(execution, context, attachment, identity, source, receipt):
    path = attachment_path(attachment, identity)
    existing = False
    digest = hashlib.sha256()
    size = 0

    def chunks():
        nonlocal size
        while chunk := source.read(COPY_CHUNK):
            size += len(chunk)
            digest.update(chunk)
            yield chunk
        if size != receipt['size'] or digest.hexdigest() != receipt['sha256']:
            raise ValueError('attachment cache integrity mismatch')

    try:
        await execution.write_file(context, path, chunks())
    except FileExistsError:
        existing = True
    digest = hashlib.sha256()
    size = 0
    while chunk := await execution.read_file(context, path, size, min(COPY_CHUNK, receipt['size'] - size + 1)):
        size += len(chunk)
        digest.update(chunk)
        if size > receipt['size']:
            break
    if size != receipt['size'] or digest.hexdigest() != receipt['sha256']:
        raise ValueError('existing workspace attachment differs; refusing overwrite' if existing
                         else 'saved attachment integrity mismatch')
    return {'source': attachment.get('source'), 'message_id': attachment.get('message_id'),
        'filename': attachment.get('filename'), 'mime_type': attachment.get('mime_type'),
        'path': path, 'sha256': receipt['sha256'], 'size': size}
