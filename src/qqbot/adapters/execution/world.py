"""One local workspace resolver for model-run file tools and independent Jobs."""
from pathlib import Path


class LocalExecution:
    world_id = "local"

    def workspace(self, data_root, task_id, workspace_ref, bindings, *, create=False, strict=True):
        path = execution_workspace(data_root, task_id, workspace_ref, bindings, create=create, strict=strict)
        if create:
            (path / "outputs").mkdir(exist_ok=True, mode=0o700)
        return path

    def create_process_service(self):
        from qqbot.capabilities.process import ProcessService
        return ProcessService()

    def create_git_service(self, credentials):
        from qqbot.capabilities.git import GitService
        return GitService(credentials)

    def create_job_runtime(self, core, data_root):
        from .runtime import JobRuntime
        return JobRuntime(core, data_root)

    async def read_file(self, context, path, offset, limit):
        import asyncio
        import os

        from qqbot.capabilities.gateway import workspace_file

        def read():
            with os.fdopen(workspace_file(context, path, os.O_RDONLY), "rb") as source:
                source.seek(offset)
                return source.read(limit)
        return await asyncio.to_thread(read)

    async def write_file(self, context, path, content):
        import asyncio
        import os

        from qqbot.capabilities.gateway import workspace_file

        def write():
            # Exclusive creation preserves an existing document if a write fails.
            with os.fdopen(workspace_file(context, path, os.O_WRONLY | os.O_CREAT | os.O_EXCL), "wb") as target:
                target.write(content)
                target.flush()
                os.fsync(target.fileno())
        writing = asyncio.create_task(asyncio.to_thread(write))
        try:
            await asyncio.shield(writing)
        except asyncio.CancelledError:
            # The write remains active until its thread has flushed and closed the file.
            while not writing.done():
                try:
                    await asyncio.shield(writing)
                except asyncio.CancelledError:
                    pass
            writing.result()
            raise

    def export_artifacts(self, workspace, archive, paths):
        from qqbot.harness.artifacts import export_artifacts
        return export_artifacts(workspace, archive, paths)

    def snapshot_file(self, workspace, path, media_type):
        from qqbot.adapters.qq_official.local_media import snapshot_workspace_file
        return snapshot_workspace_file(workspace, path, media_type)


def execution_workspace(data_root, task_id, workspace_ref, bindings, *, create=False, strict=True):
    if not task_id or Path(task_id).name != task_id or task_id in {'.', '..'} or '\\' in task_id:
        raise ValueError('Invalid host task workspace identity')
    if workspace_ref:
        if workspace_ref not in bindings:
            raise ValueError('workspace reference has no configured binding')
        return Path(bindings[workspace_ref]).resolve(strict=strict)
    path = Path(data_root) / 'workspaces' / task_id
    if create:
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
    return path.resolve(strict=strict)
