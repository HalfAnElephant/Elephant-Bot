"""One local workspace resolver for model-run file tools and independent Jobs."""
from pathlib import Path


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
