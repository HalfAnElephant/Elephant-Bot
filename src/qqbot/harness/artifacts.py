"""Export only regular, single-link files through descriptor-relative opens."""

import hashlib
import os
import stat
from pathlib import Path, PurePosixPath
from uuid import uuid4

DEFAULT_ARTIFACT_MIME_TYPES = {
    ".txt": "text/plain", ".md": "text/markdown", ".json": "application/json",
    ".zip": "application/zip", ".pdf": "application/pdf", ".png": "image/png",
    ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".webp": "image/webp",
    ".csv": "text/csv", ".html": "text/html",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
}


def export_artifacts(workspace, archive, paths, max_bytes=20 * 1024 * 1024, *, mime_types=None):
    if len(paths) > 20:
        raise ValueError("too many artifacts")
    mime_types = dict(DEFAULT_ARTIFACT_MIME_TYPES if mime_types is None else mime_types)
    archive = Path(archive)
    archive.mkdir(parents=True, exist_ok=True, mode=0o700)
    archive_fd = os.open(archive, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        root = os.open(workspace, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except BaseException:
        os.close(archive_fd)
        raise
    exported, created = [], []
    total = 0
    try:
        for relative in paths:
            path = PurePosixPath(relative)
            if path.is_absolute() or ".." in path.parts or not path.parts:
                raise ValueError("artifact path must stay in workspace")
            mime_type = mime_types.get(path.suffix.lower())
            if not isinstance(mime_type, str) or not mime_type:
                raise ValueError("unsupported artifact type")
            directory = os.dup(root)
            try:
                for component in path.parts[:-1]:
                    next_directory = os.open(
                        component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory
                    )
                    os.close(directory)
                    directory = next_directory
                fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
                try:
                    info = os.fstat(fd)
                    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                        raise ValueError("artifact must be a regular single-link file")
                    total += info.st_size
                    if total > max_bytes:
                        raise ValueError("artifact budget exceeded")
                    content = bytearray()
                    while chunk := os.read(fd, min(65536, max_bytes - len(content) + 1)):
                        content.extend(chunk)
                        if len(content) > info.st_size or len(content) > max_bytes:
                            raise ValueError("artifact changed or exceeded budget")
                    after = os.fstat(fd)
                    if (
                        after.st_size != info.st_size
                        or after.st_mtime_ns != info.st_mtime_ns
                        or len(content) != info.st_size
                    ):
                        raise ValueError("artifact changed during export")
                    if mime_type.startswith("text/") or mime_type == "application/json":
                        content.decode("utf-8")
                finally:
                    os.close(fd)
            finally:
                os.close(directory)
            artifact_id = str(uuid4())
            destination_fd = os.open(artifact_id, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                                     0o600, dir_fd=archive_fd)
            created.append(artifact_id)
            with os.fdopen(destination_fd, "wb") as output:
                output.write(content)
                output.flush()
                os.fsync(output.fileno())
            exported.append(
                {
                    "id": artifact_id,
                    "relative_path": str(path),
                    "size": len(content),
                    "sha256": hashlib.sha256(content).hexdigest(),
                    "mime_type": mime_type,
                }
            )
    except BaseException:
        for artifact_id in created:
            os.unlink(artifact_id, dir_fd=archive_fd)
        raise
    finally:
        os.close(root)
        os.close(archive_fd)
    return exported


def read_artifact(archive, artifact):
    """Recheck the immutable export immediately before delivery."""
    from uuid import UUID

    if str(UUID(artifact.id)) != artifact.id:
        raise ValueError("invalid archive reference")
    root = os.open(archive, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        fd = os.open(artifact.id, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=root)
        with os.fdopen(fd, "rb") as source:
            info = os.fstat(source.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size != artifact.size:
                raise ValueError("unsafe or changed artifact")
            content = source.read(20 * 1024 * 1024 + 1)
    finally:
        os.close(root)
    if len(content) != artifact.size or hashlib.sha256(content).hexdigest() != artifact.sha256:
        raise ValueError("artifact digest changed")
    return content
