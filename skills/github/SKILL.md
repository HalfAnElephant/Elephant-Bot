---
name: github
description: Query or change GitHub repositories, pull requests, issues, reviews and CI. Use for repository work, pagination, collaboration and verified remote writes.
---

# GitHub work

Resolve the repository from explicit URLs, stable resource references and the user's established target. Verify ambiguous repository names with the installed GitHub account before choosing a target. Preserve the actual host, owner, repository, PR or issue number and reviewed head SHA. A follow-up such as “只看我创建的” refines the current task. Resolve “我” to the account bound to the requester, and ask when that binding is unknown.

When the available context and search results leave multiple possible repositories, use `task_ask_user` alone and keep the task waiting for the exact owner/repository. Save the candidate names and completed read-only discovery in its checkpoint. After the answer, continue the same task against the confirmed target and list its PRs or perform its requested work. An explicit owner/repository answer replaces the candidate scope. Stop candidate discovery and query and report only the confirmed repository unless the user requests a comparison.

Call the trusted account adapter through `capability_invoke` with `name` set to `gh.exec`. Its `arguments` accept literal `argv`, optional literal `stdin`, `stdin_ref`, `reviewed_head_sha`, and `timeout_ms` up to 120000. Pass argument arrays directly. Credentials and the account remain with the host.

For example, list PRs with `argv` equal to `['pr', 'list', '-R', 'owner/repo', '--json', 'number,title,url,author']`. Request all pages with GitHub API pagination, for example `['api', '--paginate', 'repos/owner/repo/pulls?state=open&per_page=100']`. Check truncation, exit status and returned object IDs. Use `content_read` to read complete archived results when a reference is returned.

Use `git.transport` for authenticated Git transport. Its arguments select `operation` from `clone`, `fetch`, or `push`, the workspace-relative `path`, and optional `branch` and `commit`. The repository and host come from the Task's trusted binding. Resolve a missing binding before authenticated Git transport. Read the operation's returned validation error if an argument or dependency is missing. Use `process.start/read/wait/terminate` for local work as described by workspace-dev. Keep authenticated transport with the host adapter.

For a bounded clone of a known branch, pass that `branch` with `operation` set to `clone`. This creates a shallow single-branch checkout. An omitted branch preserves the full clone.

For an authorized remote change, make exactly the requested change and preserve the returned operation ID. The host records PREPARED, DISPATCHED and the verified outcome. Check the actual remote object and reviewed commit. A draft review, accepted CI rerun and enabled auto-merge are distinct outcomes. Report only the achieved state.

Before retrying uncertain work, call `operation.list` with empty arguments. For an UNKNOWN operation use `operation.reconcile` with its `operation_id`. Preserve UNKNOWN when readback remains inconclusive. Resume the same operation history instead of dispatching another write. The host's existing GitHub/Git compatibility adapters provide the readback and semantic duplicate protection.

For a durable external wait, use `task_wait_external` alone in its tool batch. Supply a `condition` with `capability` equal to `gh.exec`, read-only `arguments`, a JSON `path`, expected `equals`, and optional bounded `interval_seconds`. The host validates the observation as read-only and polls deterministically. Preserve its checkpoint and task identity.
