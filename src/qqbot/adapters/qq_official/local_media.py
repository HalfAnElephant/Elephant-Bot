"""Private immutable task-scoped media staging. Never expose arbitrary server paths."""
import codecs
import hashlib
import io
import json
import os
import stat
import struct
import tempfile
import zipfile
from datetime import timedelta
from pathlib import Path, PurePosixPath
from uuid import UUID, uuid4

from sqlalchemy import delete, func, insert, select

from qqbot.storage.local_media_schema import qq_local_media
from qqbot.storage.schema import outbox

from .local_limits import MAX_FILE_BYTES, limit_for

MAX_BYTES = MAX_FILE_BYTES


def _read_fd(fd, max_bytes):
    before = os.fstat(fd)
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or not 0 < before.st_size <= max_bytes:
        raise ValueError("file must be a bounded regular single-link file")
    chunks, size = [], 0
    while chunk := os.read(fd, min(65536, max_bytes - size + 1)):
        size += len(chunk)
        if size > max_bytes or size > before.st_size:
            raise ValueError("file changed or exceeded limit")
        chunks.append(chunk)
    after = os.fstat(fd)
    if (size != before.st_size or before.st_size != after.st_size or before.st_mtime_ns != after.st_mtime_ns
            or before.st_ctime_ns != after.st_ctime_ns or after.st_nlink != 1):
        raise ValueError("file changed during read")
    return b"".join(chunks)


def stage_workspace_file(root, relative_path, max_bytes=MAX_BYTES):
    """Use in to_thread; root must be the host-selected current task workspace."""
    if not isinstance(relative_path, str) or "\\" in relative_path or "\x00" in relative_path:
        raise ValueError("invalid relative file path")
    path = PurePosixPath(relative_path)
    if (path.is_absolute() or not path.parts or path.parts[0] != "outputs"
            or any(p.startswith(".") for p in path.parts)):
        raise ValueError("hidden or escaping paths are forbidden")
    if any(p.lower() in {"credentials", "credentials.json", "secrets", "secrets.json", "id_rsa", "id_ed25519"}
           or p.lower().endswith((".pem", ".key", ".p12", ".pfx")) for p in path.parts):
        raise ValueError("credential paths are forbidden")
    directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for component in path.parts[:-1]:
            nested = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
            os.close(directory)
            directory = nested
        fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        try:
            with os.fdopen(os.dup(fd), "rb") as source:
                return _snapshot(source, max_bytes)[0]
        finally:
            os.close(fd)
    finally:
        os.close(directory)



def read_workspace_file(root, relative_path, max_bytes=MAX_BYTES):
    """Compatibility bytes API; large-file callers should use stage_workspace_file."""
    with stage_workspace_file(root, relative_path, max_bytes) as staged:
        return staged.read()


def _snapshot(source, maximum):
    """Copy a seekable source to an owned temporary file with bounded buffers."""
    source.seek(0)
    try:
        before = os.fstat(source.fileno())
    except (AttributeError, io.UnsupportedOperation):
        before = None
    if before and (not stat.S_ISREG(before.st_mode) or before.st_nlink not in (0, 1)
                   or not 0 < before.st_size <= maximum):
        raise ValueError("file must be a bounded regular single-link file")
    output, digest, size = tempfile.TemporaryFile("w+b"), hashlib.sha256(), 0  # noqa: SIM115 - ownership returned
    try:
        while chunk := source.read(65536):
            size += len(chunk)
            if size > maximum or (before and size > before.st_size):
                raise ValueError("file changed or exceeded size limit")
            output.write(chunk)
            digest.update(chunk)
        if not size:
            raise ValueError("media exceeds size limit or is empty")
        if before:
            after = os.fstat(source.fileno())
            if (size != before.st_size or before.st_size != after.st_size
                    or before.st_mtime_ns != after.st_mtime_ns or before.st_ctime_ns != after.st_ctime_ns
                    or after.st_nlink != before.st_nlink):
                raise ValueError("file changed during read")
        output.flush()
        output.seek(0)
        return output, size, digest.hexdigest()
    except BaseException:
        output.close()
        raise



def snapshot_workspace_file(root, relative_path, media_type):
    """Return (owned seekable snapshot, size, MIME); caller must close snapshot."""
    source = stage_workspace_file(root, relative_path, max_bytes=limit_for(media_type))
    try:
        mime = sniff_media_file(source, PurePosixPath(relative_path).name, media_type)
        return source, os.fstat(source.fileno()).st_size, mime
    except BaseException:
        source.close()
        raise


def _office_zip_preflight(source):
    """Bound archive metadata before ZipFile allocates entry objects.

    This is an OOXML structure guard, not a whole-file size threshold. Large
    document bodies remain permitted under the official file-size limit.
    ZIP64 and split/self-extracting archives are outside this classifier.
    """
    source.seek(0, os.SEEK_END)
    size = source.tell()
    source.seek(0)
    if source.read(4) != b"PK\x03\x04" or size < 22:
        raise ValueError("unsupported Office ZIP structure")
    tail_size = min(size, 65535 + 22)
    source.seek(size - tail_size)
    tail = source.read(tail_size)
    position = tail.rfind(b"PK\x05\x06")
    if position < 0 or position + 22 > len(tail):
        raise ValueError("missing Office ZIP directory")
    _, disk, directory_disk, disk_entries, entries, directory_size, directory_offset, comment = struct.unpack_from(
        "<4s4H2LH", tail, position)
    end_offset = size - tail_size + position
    if (disk or directory_disk or disk_entries != entries or not 1 <= entries <= 4096
            or directory_size > 8 * 1024 * 1024 or directory_offset == 0xFFFFFFFF
            or position + 22 + comment != len(tail)
            or directory_offset + directory_size != end_offset):
        raise ValueError("unsupported or excessive Office ZIP directory")
    # Independently walk the actual directory: untrusted EOCD counts may lie.
    source.seek(directory_offset)
    seen, consumed = 0, 0
    while consumed < directory_size:
        header = source.read(46)
        if len(header) != 46 or header[:4] != b"PK\x01\x02":
            raise ValueError("invalid Office ZIP directory entry")
        compressed, expanded = struct.unpack_from("<LL", header, 20)
        name_size, extra_size, comment_size, entry_disk = struct.unpack_from("<4H", header, 28)
        local_offset = struct.unpack_from("<L", header, 42)[0]
        record_size = 46 + name_size + extra_size + comment_size
        seen += 1
        if (seen > 4096 or consumed + record_size > directory_size or entry_disk
                or 0xFFFFFFFF in (compressed, expanded, local_offset) or local_offset >= directory_offset):
            raise ValueError("unsupported Office ZIP entry bounds")
        source.seek(name_size, os.SEEK_CUR)
        extra = source.read(extra_size)
        at = 0
        while at < len(extra):
            if at + 4 > len(extra):
                raise ValueError("invalid Office ZIP extra field")
            kind, length = struct.unpack_from("<HH", extra, at)
            at += 4 + length
            if kind == 1 or at > len(extra):
                raise ValueError("unsupported Office ZIP64 metadata")
        source.seek(comment_size, os.SEEK_CUR)
        consumed += record_size
    if seen != entries:
        raise ValueError("inconsistent Office ZIP entry count")
    source.seek(0)

def sniff_media_file(source, filename, media_type):
    """Validate a seekable snapshot; binary formats never materialize the whole file."""
    source.seek(0)
    prefix = source.read(32)
    suffix = Path(filename).suffix.lower() if isinstance(filename, str) else ""
    if suffix in {".txt", ".md", ".csv", ".json"}:
        # MIME classification only: validate UTF-8 incrementally, not full document semantics.
        sniff_media(b"{}" if suffix == ".json" else b"text", filename, media_type)
        source.seek(0)
        decoder = codecs.getincrementaldecoder("utf-8")()
        first, last, scalar = "", "", ""
        try:
            while chunk := source.read(65536):
                text = decoder.decode(chunk)
                if "\x00" in text:
                    raise ValueError("invalid text media")
                stripped = text.strip()
                if stripped:
                    first = first or stripped[0]
                    last = stripped[-1]
                    if len(scalar) <= 32:
                        scalar += stripped[:33]
            decoder.decode(b"", final=True)
            if suffix == ".json":
                # Bounded lexical envelope, not a parser. Never load a 100 MiB
                # JSON array into millions of Python objects merely to detect MIME.
                if first in {"{", "[", '"'}:
                    if last != {"{": "}", "[": "]", '"': '"'}[first]:
                        raise ValueError("invalid JSON envelope")
                elif len(scalar) <= 32:
                    json.loads(scalar)
                else:
                    raise ValueError("invalid JSON envelope")
        except (ValueError, UnicodeError, RecursionError):
            raise ValueError("invalid text media") from None
        mime = {".txt": "text/plain", ".md": "text/markdown", ".csv": "text/csv",
                ".json": "application/json"}[suffix]
    elif suffix in {".docx", ".xlsx", ".pptx"}:
        # Filename validation is shared with the bytes API; inspect ZIP directory only.
        sniff_media(b"PK\x03\x04", "safe.zip", "file")
        if (not isinstance(filename, str) or not 1 <= len(filename) <= 128
                or len(filename.encode("utf-8")) > 255 or filename.startswith(".")
                or any(ord(c) < 32 or ord(c) == 127 or c in "/\\" for c in filename) or media_type != "file"):
            raise ValueError("filename must be a safe basename")
        member, mime = {".docx": ("word/document.xml", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
            ".xlsx": ("xl/workbook.xml", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
            ".pptx": ("ppt/presentation.xml", "application/vnd.openxmlformats-officedocument.presentationml.presentation")}[suffix]
        try:
            _office_zip_preflight(source)
            with zipfile.ZipFile(source) as archive:
                names = archive.namelist()
                valid = (len(names) <= 4096 and "[Content_Types].xml" in names and member in names
                         and not any("vbaproject" in name.lower() for name in names))
        except (zipfile.BadZipFile, ValueError):
            valid = False
        if not valid:
            raise ValueError("media type, filename and signature do not match supported formats")
    else:
        mime = sniff_media(prefix, filename, media_type)
    source.seek(0)
    return mime

def sniff_media(content, filename, media_type):
    """Conservative format classification, not an antivirus/content-safety scan."""
    if (not isinstance(filename, str) or not 1 <= len(filename) <= 128
            or len(filename.encode("utf-8")) > 255 or filename in {".", ".."}
            or filename.startswith(".") or any(ord(c) < 32 or ord(c) == 127 or c in "/\\" for c in filename)):
        raise ValueError("filename must be a safe basename")
    suffix = Path(filename).suffix.lower()
    if not isinstance(content, bytes) or not content:
        raise ValueError("empty or invalid media")
    rules = {
        ".png": ("image", "image/png", content.startswith(b"\x89PNG\r\n\x1a\n")),
        ".jpg": ("image", "image/jpeg", content.startswith(b"\xff\xd8\xff")),
        ".jpeg": ("image", "image/jpeg", content.startswith(b"\xff\xd8\xff")),
        ".gif": ("image", "image/gif", content.startswith((b"GIF87a", b"GIF89a"))),
        ".webp": ("image", "image/webp", content[:4] == b"RIFF" and content[8:12] == b"WEBP"),
        ".mp4": ("video", "video/mp4", len(content) >= 12 and content[4:8] == b"ftyp"),
        ".silk": ("audio", "audio/silk", content.startswith((b"#!SILK_V3", b"\x02#!SILK_V3"))),
        ".mp3": ("audio", "audio/mpeg", content.startswith(b"ID3") or
                  (len(content) > 2 and content[0] == 255 and content[1] & 224 == 224)),
        ".wav": ("audio", "audio/wav", content[:4] == b"RIFF" and content[8:12] == b"WAVE"),
        ".ogg": ("audio", "audio/ogg", content.startswith(b"OggS")),
        ".pdf": ("file", "application/pdf", content.startswith(b"%PDF-")),
        ".zip": ("file", "application/zip", content.startswith((b"PK\x03\x04", b"PK\x05\x06"))),
    }
    if suffix in {".txt", ".md", ".csv", ".json"}:
        try:
            text = content.decode("utf-8")
            if "\x00" in text:
                raise ValueError
            if suffix == ".json":
                json.loads(text)
        except (ValueError, UnicodeError, RecursionError):
            raise ValueError("invalid text media") from None
        rules[suffix] = ("file", {".txt": "text/plain", ".md": "text/markdown", ".csv": "text/csv",
                                 ".json": "application/json"}[suffix], True)
    office = {
        ".docx": ("word/document.xml", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
        ".xlsx": ("xl/workbook.xml", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
        ".pptx": ("ppt/presentation.xml", "application/vnd.openxmlformats-officedocument.presentationml.presentation"),
    }
    if suffix in office:
        member, mime = office[suffix]
        try:
            source = io.BytesIO(content)
            _office_zip_preflight(source)
            with zipfile.ZipFile(source) as archive:
                names = archive.namelist()
                valid = (len(names) <= 4096 and "[Content_Types].xml" in names and member in names
                         and not any("vbaproject" in name.lower() for name in names))
        except (zipfile.BadZipFile, ValueError):
            valid = False
        rules[suffix] = ("file", mime, valid)
    kind, mime, valid = rules.get(suffix, (None, None, False))
    if kind != media_type or not valid:
        raise ValueError("media type, filename and signature do not match supported formats")
    return mime


class LocalMediaStore:
    def __init__(self, db, clock, *, max_bytes=MAX_BYTES, ttl_seconds=86400,
                 task_budget=4 * MAX_FILE_BYTES, total_budget=16 * MAX_FILE_BYTES):
        self.db, self.clock = db, clock
        self.root = Path(db.path).parent / "qq-media"
        self.max_bytes, self.ttl_seconds = max_bytes, ttl_seconds
        self.task_budget, self.total_budget = task_budget, total_budget

    def _root_fd(self):
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        if os.fstat(fd).st_mode & 0o077:
            os.close(fd)
            raise ValueError("media directory must be private")
        return fd

    @staticmethod
    def _id(ref):
        if not isinstance(ref, str) or str(UUID(ref)) != ref:
            raise ValueError("invalid local media reference")
        return ref

    @staticmethod
    def _scope(task_id, conversation_id, principal_key):
        return ((qq_local_media.c.task_id == task_id) & (qq_local_media.c.conversation_id == conversation_id)
                & (qq_local_media.c.principal_key == principal_key))

    def put(self, content, **kwargs):
        """Compatibility bytes API. Core streaming ingress should call put_file."""
        if not isinstance(content, bytes):
            raise ValueError("media exceeds size limit or is empty")  # noqa: TRY004 - API boundary
        with io.BytesIO(content) as source:
            return self.put_file(source, **kwargs)

    def put_file(self, source, *, task_id, conversation_id, principal_key, request_id, filename, media_type):
        maximum = min(self.max_bytes, limit_for(media_type))
        staged, size, digest = _snapshot(source, maximum)
        try:
            mime = sniff_media_file(staged, filename, media_type)
            return self._persist(staged, size, digest, mime, task_id=task_id, conversation_id=conversation_id,
                principal_key=principal_key, request_id=request_id, filename=filename, media_type=media_type)
        finally:
            staged.close()

    def _persist(self, staged, size, digest, mime, *, task_id, conversation_id, principal_key, request_id, filename, media_type):
        if any(not isinstance(v, str) or not 1 <= len(v) <= 1024
               for v in (task_id, conversation_id, principal_key, request_id)):
            raise ValueError("invalid media scope")
        with self.db.transaction() as c:
            old = c.execute(select(qq_local_media).where(self._scope(task_id, conversation_id, principal_key),
                qq_local_media.c.request_id == request_id)).mappings().first()
            if old:
                if (old["sha256"], old["filename"], old["media_type"]) != (digest, filename, media_type):
                    raise ValueError("request_id already used with different file")
                if old["expires_at"] <= self.clock.now().isoformat():
                    raise ValueError("local media expired; use a new request_id")
                return dict(old)
            total = c.execute(select(func.coalesce(func.sum(qq_local_media.c.size), 0))).scalar_one()
            per_task = c.execute(select(func.coalesce(func.sum(qq_local_media.c.size), 0)).where(
                qq_local_media.c.task_id == task_id)).scalar_one()
            if total + size > self.total_budget or per_task + size > self.task_budget:
                raise ValueError("local media storage budget exceeded")
            ref = str(uuid4())
            record = dict(id=ref,  # noqa: C408
                task_id=task_id, conversation_id=conversation_id, principal_key=principal_key,
                request_id=request_id, sha256=digest, mime_type=mime, filename=filename, media_type=media_type,
                size=size, expires_at=(self.clock.now() + timedelta(seconds=self.ttl_seconds)).isoformat())
            root = self._root_fd()
            try:
                fd = os.open(ref, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=root)
                try:
                    with os.fdopen(fd, "wb") as output:
                        staged.seek(0)
                        while chunk := staged.read(65536):
                            output.write(chunk)
                        output.flush()
                        os.fsync(output.fileno())
                    c.execute(insert(qq_local_media).values(**record))
                except BaseException:
                    os.unlink(ref, dir_fd=root)
                    raise
            finally:
                os.close(root)
        return record

    def read(self, ref, **scope):
        metadata, source = self.open(ref, **scope)
        with source:
            return metadata, source.read()

    def open(self, ref, *, task_id, conversation_id, principal_key):
        ref = self._id(ref)
        with self.db.engine.connect() as c:
            row = c.execute(select(qq_local_media).where(qq_local_media.c.id == ref,
                self._scope(task_id, conversation_id, principal_key))).mappings().first()
        if not row or row["expires_at"] <= self.clock.now().isoformat():
            raise ValueError("local media unavailable or expired")
        root = self._root_fd()
        try:
            fd = os.open(ref, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=root)
            try:
                if os.fstat(fd).st_mode & 0o077:
                    raise ValueError("media file must be private")
                with os.fdopen(os.dup(fd), "rb") as source:
                    staged, size, digest = _snapshot(source, min(self.max_bytes, limit_for(row["media_type"])))
            finally:
                os.close(fd)
        finally:
            os.close(root)
        if size != row["size"] or digest != row["sha256"]:
            staged.close()
            raise ValueError("local media integrity mismatch")
        return dict(row), staged

    def cleanup(self):
        """Expire only unreferenced blobs; UNKNOWN retains bytes for reconciliation."""
        removed = 0
        with self.db.transaction() as c:
            protected = set()
            for raw in c.execute(select(outbox.c.envelope_json).where(outbox.c.state.in_(
                    ["PENDING", "RETRYABLE", "SENDING", "UNKNOWN", "WAITING_FOR_TRIGGER", "STREAM_PENDING"]))).scalars():
                try:
                    media = (json.loads(raw).get("official_content") or {}).get("media") or {}
                    if isinstance(media.get("local_ref"), str):
                        protected.add(media["local_ref"])
                except (ValueError, TypeError, AttributeError):
                    # Corrupt pending state is not evidence that a blob is safe to discard.
                    return 0
            rows = c.execute(select(qq_local_media).where(
                qq_local_media.c.expires_at <= self.clock.now().isoformat())).mappings().all()
            if not rows and not self.root.exists():
                return 0
            root = self._root_fd()
            try:
                for row in rows:
                    if row["id"] in protected:
                        continue
                    try:
                        os.unlink(self._id(row["id"]), dir_fd=root)
                    except FileNotFoundError:
                        pass
                    c.execute(delete(qq_local_media).where(qq_local_media.c.id == row["id"]))
                    removed += 1
                # A process death before metadata commit can leave an orphan. Only
                # delete old regular UUID entries in this private reserved directory.
                known = set(c.execute(select(qq_local_media.c.id)).scalars())
                cutoff = self.clock.now().timestamp() - self.ttl_seconds
                for name in os.listdir(root):
                    try:
                        self._id(name)
                    except ValueError:
                        continue
                    if name in known or name in protected:
                        continue
                    try:
                        info = os.stat(name, dir_fd=root, follow_symlinks=False)
                        if stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_mtime <= cutoff:
                            os.unlink(name, dir_fd=root)
                            removed += 1
                    except FileNotFoundError:
                        pass
            finally:
                os.close(root)
        return removed
