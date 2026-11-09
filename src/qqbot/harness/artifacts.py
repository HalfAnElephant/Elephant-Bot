"""Export only regular, single-link files through descriptor-relative opens."""

import hashlib
import os
import stat
from pathlib import Path, PurePosixPath
from uuid import uuid4


def export_artifacts(workspace, archive, paths, max_bytes=20 * 1024 * 1024):
    if len(paths) > 20:
        raise ValueError("too many artifacts")
    root = os.open(workspace, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    archive = Path(archive)
    archive.mkdir(parents=True, exist_ok=True, mode=0o700)
    exported = []
    total = 0
    try:
        for relative in paths:
            path = PurePosixPath(relative)
            if path.is_absolute() or ".." in path.parts or not path.parts:
                raise ValueError("artifact path must stay in workspace")
            if path.suffix not in {".txt", ".md", ".json"}:
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
                    content.decode("utf-8")
                finally:
                    os.close(fd)
            finally:
                os.close(directory)
            artifact_id = str(uuid4())
            destination = archive / artifact_id
            with destination.open("xb") as output:
                output.write(content)
                output.flush()
                os.fsync(output.fileno())
            exported.append(
                {
                    "id": artifact_id,
                    "relative_path": str(path),
                    "size": len(content),
                    "sha256": hashlib.sha256(content).hexdigest(),
                    "mime_type": {".md": "text/markdown", ".txt": "text/plain", ".json": "application/json"}[
                        path.suffix
                    ],
                }
            )
    except BaseException:
        for artifact in exported:
            (archive / artifact["id"]).unlink(missing_ok=True)
        raise
    finally:
        os.close(root)
    return exported
