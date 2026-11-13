"""Export only regular, single-link files through descriptor-relative opens."""

import hashlib
import json
import os
import stat
from datetime import datetime, timedelta
from pathlib import Path, PurePosixPath
from uuid import UUID, uuid4

DEFAULT_ARTIFACT_MIME_TYPES = {
    ".txt": "text/plain", ".md": "text/markdown", ".json": "application/json",
    ".zip": "application/zip", ".pdf": "application/pdf", ".png": "image/png",
    ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".webp": "image/webp",
    ".csv": "text/csv", ".html": "text/html",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
}


def artifact_path(relative, *, mime_types=None):
    path = PurePosixPath(relative)
    if len(str(path)) > 512 or path.is_absolute() or ".." in path.parts or not path.parts:
        raise ValueError("artifact path must be a relative workspace file of at most 512 characters")
    mime_type = (DEFAULT_ARTIFACT_MIME_TYPES if mime_types is None else mime_types).get(path.suffix.lower())
    if not isinstance(mime_type, str) or not mime_type:
        raise ValueError("unsupported artifact type: " + str(path))
    return path, mime_type


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
            path, mime_type = artifact_path(relative, mime_types=mime_types)
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


def cleanup_artifacts(db, archive, now):
    """Expire old exports while preserving live task trees and pending result evidence."""
    from sqlalchemy import select

    from qqbot.jobs.service import TERMINAL
    from qqbot.storage.task_schema import runs, tasks

    try:
        directory = os.open(archive, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except FileNotFoundError:
        return 0
    cutoff = now - timedelta(days=30)
    removed = 0
    try:
        # Keep the reference snapshot valid until deletion, including concurrent finalizers.
        with db.transaction() as conn:
            task_rows = conn.execute(select(tasks.c.id, tasks.c.root_task_id, tasks.c.state,
                tasks.c.created_at, tasks.c.result_json)).mappings().all()
            run_rows = conn.execute(select(runs.c.task_id, runs.c.slot_reserved, runs.c.result_json)).mappings().all()
            resident = {row["task_id"] for row in run_rows if row["slot_reserved"] and row["task_id"]}
            active = {row["id"] for row in task_rows if row["state"] not in TERMINAL} | resident
            active.update(row["root_task_id"] for row in task_rows if row["id"] in active and row["root_task_id"])
            has_active = bool(active) or any(row["slot_reserved"] for row in run_rows)
            protected, referenced = set(), set()
            keep = {row["id"] for row in task_rows if row["id"] in active
                    or row["root_task_id"] in active or datetime.fromisoformat(row["created_at"]) >= cutoff}
            for row in [*task_rows, *run_rows]:
                result = json.loads(row["result_json"] or "{}")
                ids = {item["id"] for item in result.get("artifacts", [])}
                referenced.update(ids)
                task_id = row["task_id"] if "task_id" in row else row["id"]
                if task_id in keep:
                    protected.update(ids)
            for name in os.listdir(directory):
                try:
                    if str(UUID(name)) != name or name in protected:
                        continue
                except ValueError:
                    continue
                # shortcut: orphan exports await idle tasks; index spool references if traffic prevents cleanup.
                if has_active and name not in referenced:
                    continue
                try:
                    info = os.stat(name, dir_fd=directory, follow_symlinks=False)
                    if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                            or info.st_mtime >= cutoff.timestamp()):
                        continue
                    os.unlink(name, dir_fd=directory)
                    removed += 1
                except FileNotFoundError:
                    continue
    finally:
        os.close(directory)
    return removed
