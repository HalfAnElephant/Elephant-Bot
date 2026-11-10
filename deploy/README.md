# Ubuntu runner deployment

The production code path is `SystemdGeneralRun`, selected by `--runner-mode systemd`. It invokes the fixed sudo launcher with `start|stop LAUNCH_TASK_UUID PROCESS_UUID`. Protocol-1 research recovery retains its legacy run identity. Local mode remains the default for protocol development. Production startup rejects non-Linux hosts and root policies whose `isolation_verified` flag is false. Each target host requires Linux namespace, cgroup and credential acceptance before enabling the policy.

## Minimal installation

1. Install the locked Python project and npm runtime under root-owned `/opt/qqbot`. Python 3.13 and `/usr/bin/node` are required. The virtualenv interpreter must reside beneath a path the service account can traverse, with root ownership and no group/world write permission. An editable install pointing into a personal checkout is unsuitable.
2. Run the root-owned `deploy/install-runner-ubuntu.sh` from that checkout. It validates runtime ownership, creates dedicated supervisor/runner accounts, installs the fixed launcher and entry, prepares an empty runner root and installs an isolated network namespace. It does not start services. The policy is installed with `isolation_verified=false`.
3. Configure Core under its own `qqbot` account. Core owns the database and credentials. Expose its internal API only through `/run/qqbot/internal.sock`, with group access for `qqbot-worker`. Provision a separate thirty-two-character-or-longer worker token in `/etc/qqbot/worker.token`, readable by the supervisor account. Keep the same token on Core's internal listener.
4. Create `/var/lib/qqbot/artifacts`, owned by `qqbot-worker`. Give Core read/traverse access with a default ACL so newly exported files remain readable. Grant the worker only traversal on the Core parent directory, leaving the database and other files inaccessible. Set Core `TaskService.archive_root` to that archive path. The supervisor service uses the same path.
5. Store the explicitly authorized provider key in the supervisor's `/etc/qqbot/worker.env` as `QQBOT_RUNNER_OPENAI_KEY`. This variable name also supports the OpenAI-compatible OpenCode Go key. No operator Pi profile is imported. The provided service selects `deepseek-v4.1-flash` and `https://opencode.ai/inference/go/openai/v1/chat/completions`. Both are explicit CLI values. The root policy's approved-model list must match.
6. Run root-only `/usr/local/libexec/pi-isolation-check --enable-policy`. It uses the production workspace preparation under umask 0077 and enables the root policy only when all eighteen checks pass. Install/start only the new QQ Core and Worker services. Existing QQ transport services require no restart from this installer.

The fixed namespace uses host address `10.203.0.1/30` and runner address `10.203.0.2/30`, with no default route. Its firewall permits only the trusted proxy range `30000..30999`. The per-run listener binds that range. The isolated root mounts read-only runtime files, the fixed trusted extension/entry, one read-only manifest and only the current run workspace. Core paths, internal socket, personal homes and supervisor credentials are absent. The entry checks unprivileged UID, NoNewPrivileges, hidden private paths and absence of a default route before executing Pi with a replacement environment.

The worker service intentionally permits its tightly scoped sudo launcher, so it cannot set `NoNewPrivileges=true` itself. The launcher sets that property on every runner unit. No arbitrary systemd-run, executable, mount path, unit property or environment option is accepted from the caller.

## Acceptance and recovery

Verify systemd options on the target Ubuntu release. Test cross-run workspace access, Core/database/socket/key access, `/proc` credentials, background child survival, cancellation, lease loss and archive symlink/hardlink replacement. No claim of OS isolation follows from the offline tests alone.

A root launcher writes `/var/lib/qqbot-runner-stops/PROCESS_UUID.json` only after the unit is inactive and its cgroup is empty. Core checks root ownership, directory/file modes and canonical task/run identity before reconciliation. For general runs, Core also requires the authenticated Worker to have recorded STOPPED after draining gateway requests and host tool processes. A pi cgroup marker alone cannot prove host Git/gh cleanup. If the Worker crashes before reporting that proof, reconciliation remains blocked; verify and stop the Worker service cgroup and recover its ownership before releasing capacity. Never replace these proofs with a user-supplied boolean. An administrator can reconcile an expired spool result under new recovery ownership, or close the stopped task without a result. Old fencing never commits a new terminal state directly.

A host using UFW may require a narrow allowance from `10.203.0.2` to `10.203.0.1`, TCP ports `30000:30999`, on `qqbot-host`. The installer leaves host-wide firewall administration to the operator. Its network namespace helper and unit restore the isolated link before the Worker starts. Verify host reboot recovery on the target deployment before relying on automatic service restoration.

## Generic workspace and GitHub tools

The installer installs Ubuntu's `bubblewrap` package when `bwrap` is missing. Credentialed `gh.exec` and `process.start` require Linux user/PID/mount namespaces. Validate bubblewrap under the actual Worker service account and its AppArmor/systemd restrictions before enabling these capabilities. Installation alone does not establish isolation acceptance.

Workspace processes receive an empty environment, private `/proc`, `/dev` and `/tmp`, system binaries and their own workspace. Network access is disabled. Credentialed GitHub CLI runs have network access, private process/mount namespaces and a trusted empty working directory. They receive only the host-bound token and omit operator homes, Git helpers and service credential files. Install `gh` beneath `/usr` or `/bin` for the credentialed path. Keep installed extensions and executables administrator-owned.

Use `git.transport` for authenticated clone, fetch and push. It uses a trusted temporary repository and copies only bounded regular Git objects from a workspace, omitting its config, hooks and helpers. Pushes require a full commit SHA and explicit destination branch, then verify the remote ref. Fetch imports objects and returns `fetch_head` for an explicit subsequent workspace operation. `gh` checkout/clone/download/upload commands report the controlled Git provider requirement. Ordinary workspace commands receive no GitHub token.

The first implementation keeps processes owned by their originating run. A run with live processes must remain active or explicitly terminate them before becoming quiescent. Task-level process-lease transfer requires a separate implementation. Validate descendant cleanup, cancellation, timeout, symlink races, inaccessible service credentials and clone/edit/commit/push on the target Linux host. Local fixture tests cover protocol and evidence behavior but do not perform GitHub writes.

### Local gh authentication

Worker credentials prefer the configured host's local `gh` login, then fall back to
`QQBOT_GITHUB_TOKEN` (or `--github-token-env`). For a systemd Worker whose service
account cannot access the login user's home, use a service drop-in:

```ini
[Service]
LoadCredential=hosts.yml:/home/LOGIN_USER/.config/gh/hosts.yml
LoadCredential=config.yml:/home/LOGIN_USER/.config/gh/config.yml
Environment=GH_CONFIG_DIR=%d
```

Replace the source path with the existing `gh` configuration. systemd supplies a
read-only private copy at service startup; include `config.yml` so gh does not attempt
a configuration migration in that read-only directory; restart the Worker after changing that
login. The runner sandbox receives neither this file nor the user's home directory.

## iCloud Calendar

The optional `icloud.calendar` capability reads and manages events through the Worker.
Set `ICLOUD_CALENDAR_CONFIG` to a private JSON file containing `apple_id`,
`app_password` (an Apple app-specific password), `timezone`, `default_calendar`,
and an `owner` object with the exact `adapter_instance_id`, `identity_namespace`,
and `remote_user_id`. Every invocation and readback checks this owner binding.
Keep this file outside the repository and task workspaces, mode `0600`.

For systemd, provision `/etc/qqbot/icloud-calendar.json` as root and add a Worker drop-in:

```ini
[Service]
LoadCredential=icloud-calendar.json:/etc/qqbot/icloud-calendar.json
Environment=ICLOUD_CALENDAR_CONFIG=%d/icloud-calendar.json
```

Reload systemd and restart the Worker after provisioning. The Pi sandbox receives
neither the file nor its environment variable. No configuration means the
capability is absent. Calendar discovery and event reads are safe deployment
checks. Event create, update and delete use the durable operation journal and
remote readback. An uncertain write must be reconciled before any retry.
`list` requires a bounded date window. Preserve returned calendar/event IDs for
later edits. Recurrence edits apply to the full resource, including its series.
The legacy VTODO/Reminders scripts are reference material and have no capability
entry point.


## Immutable skill publication

The installer publishes the maintained `/opt/qqbot/skills` tree into root-owned,
read-only `/opt/qqbot/skillsets/<sha>` directories and atomically selects `current`.
To publish a skill-only update after placing the reviewed files under the same
root-owned source directory, use the following root command.

```sh
/opt/qqbot/.venv/bin/python -m qqbot.harness.skills /opt/qqbot/skills /opt/qqbot/skillsets
```

New tasks observe the new version. In-flight tasks, durable waits and retries keep
their pinned version. Keep old version directories while tasks or recovery evidence
refer to them. Worker retires obsolete idle pi processes and lets active processes
finish. General runner manifests require a content hash for startup, and old
manifests remain usable for stopping existing processes.

The launcher verifies root ownership, permissions and content before mounting the
selected directory read-only at `/skills`. The process service mounts the same
version there and uses `/workspace` for the task's execution directory. Installed
system runtimes provide script dependencies. Public HTTPS, authenticated Git and
calendar access use the existing trusted host adapters, preserving their account
boundaries, ledger and readback semantics. Target Linux mount and isolation checks
are required when deploying this change.
