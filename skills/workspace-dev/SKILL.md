---
name: workspace-dev
description: Read and change task workspace files, inspect logs, run tests and execute bundled skill scripts. Use for development, debugging and local data processing.
compatibility: Isolated script execution requires Linux bubblewrap and the requested interpreter installed on the host.
---

# Workspace development

Read task files with `read` and a workspace-relative `path`. The task working directory is `/workspace` inside a process. The pi profile/session directory is separate. Skills and their scripts are read-only under `/skills` and pinned to this task's version.

Start a bounded command with `capability_invoke`, `name` set to `process.start`, and `arguments` containing literal `argv`, optional `stdin`, and `timeout_seconds` from 1 to 1800. For example run a bundled Python script with `['python3', '/skills/example/scripts/work.py']`. The process receives the task workspace, the pinned skills and installed system runtimes. The environment is cleared, and public networking and account credentials remain in the host adapters.

Keep the returned `process_ref`. Use `process.read` to page output with `offset` and `limit`, `process.wait` with `wait_seconds` from 0 to 30 to await it, and `process.terminate` to stop it. Check the returned `exit_code`, `expired` and `truncated` fields. Keep related work in the same task. A process cannot outlive its run through an implicit handoff.

Use installed interpreters and existing repository commands. Check a required dependency with its normal import or version command before relying on it. A missing interpreter, module or executable is a dependency failure with that name. Report it accurately rather than claiming the command ran. Keep authenticated GitHub/Git and public web requests in their corresponding host adapters.

A new small UTF-8 file can use `workspace.write` with `path` and `text`. It creates a new file and rejects overwriting an existing path. For edits and binary generation, use a bounded process in the task workspace. Preserve existing data and validate the actual result through its normal entry point. Read generated outputs back before declaring them usable.

When files are requested, follow `goal_spec.requested_outputs` exactly and use `outputs/` for other artifacts. Pass their relative paths in `task_outcome.attachments`. The host verifies and archives their actual bytes. A successful command only proves that command's exit status. Requested tests and artifact checks still determine task completion.
