# Ubuntu daemon deployment

The supported runtime is one Python daemon under `qqbot`, its on-demand Pi processes,
SQLite and OpenViking. QQ uses the official adapter. There is no separate Worker
listener or NapCat route. The daemon admits Leader, Worker and Hermes review turns
through the same execution pool and limits actual model requests separately.
Background Agent admission and task trees have no fixed count or total execution
time cap. Legacy count and background duration settings remain accepted for
configuration compatibility. Model requests retain upstream concurrency
backpressure, and individual tools and external waits retain their timeouts.
Interrupted or blocked Workers preserve results for a Leader decision. A Leader
takeover requires stopped related execution and reconciled external operations.

Install the locked Python and npm packages under root-owned `/opt/qqbot`, with
Python 3.13, Node, FFmpeg 6 or newer and the pinned Pi executable accessible to `qqbot`. The locked
Python dependency `pilk-nogil` decodes QQ SILK voice messages in a bounded child process. Install
`deploy/qqbot.service`, root-owned `scripts/elephant-daemon-stopped` at
`/usr/local/libexec/elephant-daemon-stopped`, and create root-owned mode 0755
`/var/lib/qqbot-daemon-stops`. The service grants the stop helper write access to this directory in its mount namespace. Keep `/var/lib/qqbot` owned by the daemon, mode 0700.

Configure `/etc/qqbot/core.yaml` and private `/etc/qqbot/core.env`. Enable the
model endpoint and its key, official QQ AppID/secret and verified owner OpenID,
and OpenViking's scoped user credentials. Do not infer an official identity from
an old OneBot numeric QQ ID. Preserve explicitly configured personal preferences
in `owner.preferences`. Remove retired social, Judge, expression and persona
settings before validating. `configs/example.yaml` and `configs/schema.json` are
the configuration contract. Configure `orchestration.pi_executable` with an absolute
path in production. Run `qqbot validate` before starting the service.

Run the normal Alembic migration before service startup. It bridges the recognized
experimental 0010 schema without modifying published revisions. `migrate(path,
backup=False)` supports an explicitly authorized deployment without a database
backup; the CLI otherwise keeps its existing pre-migration backup behavior. Drain
and stop the previous services first. Preserve Inbox, Outbox UNKNOWN states,
operation receipts and deletion barriers. Disable the old Core and Worker units;
start only this daemon.

The daemon service uses `KillMode=control-group`. Its privileged `ExecStopPost`
helper checks the cgroup is empty and fsyncs a root-owned receipt bound to the
systemd invocation. Recovery never treats an expired lease as proof of process
termination. Once stop is proven, a completed native boundary is committed through
the original result validation; unfinished tools and uncertain external writes
require readback. They are never automatically dispatched again. Keep the stop
helper and receipt directory unwritable by the daemon.

## Persistent Jobs and execution isolation

Provision the daemon account with root-only
`deploy/elephant-job-user-setup.sh qqbot`. This enables its systemd user manager
and linger, so Jobs survive daemon restarts. See
[Job operations](../docs/operations/persistent-jobs.md) for receipts and acceptance.
The setup script writes `/etc/qqbot/jobs.env` and a service drop-in with the actual account UID. The drop-in orders the daemon after that user manager and exposes only its `/run/user/UID` directory through the `ProtectHome=tmpfs` sandbox. Keep the remaining home directories hidden. Do not grant the daemon sudo.

Install `bubblewrap` and validate user namespaces under the actual service account.
Workspace processes and Jobs have a cleared environment, isolated mount/PID/network
namespaces, system binaries, and their single authorized workspace. Generic process
execution receives no GitHub or calendar credentials. GitHub CLI and Git transport
use their existing controlled credential providers. Pi has no built-in tools,
automatic skill scan, context-file injection or plugin mirror; its trusted extension
calls the host gateway.

To use an existing local GitHub login, add service credentials:

```ini
[Service]
LoadCredential=hosts.yml:/home/LOGIN_USER/.config/gh/hosts.yml
LoadCredential=config.yml:/home/LOGIN_USER/.config/gh/config.yml
Environment=GH_CONFIG_DIR=%d
```

For the optional calendar provider, use a private JSON configuration containing
`apple_id`, `app_password`, `timezone`, `default_calendar` and the exact authorized
`owner` identity. Mount it with `LoadCredential=icloud-calendar.json:/etc/qqbot/icloud-calendar.json`
and `Environment=ICLOUD_CALENDAR_CONFIG=%d/icloud-calendar.json`. A retired numeric
QQ identity does not authorize the official OpenID or host identity.

## Acceptance

Check readiness reports supervisor failures, actual provider calls, native Pi
continuation, two concurrent task trees, cancellation and cgroup cleanup, daemon
restart, durable Job survival, external UNKNOWN reconciliation and memory deletion
barriers. OpenViking must use authenticated scoped user requests and disable
working-memory/takeover and skills/tools/experiences extraction. Test a real write,
extraction, recall, correction, forget and restart. Run official QQ media, buttons,
streaming and all four surfaces on an authorized application separately. Missing
platform credentials mean those checks remain unverified even when unit contracts
pass. The full acceptance matrix lives in the implementation plan.

Database deletion-ledger exports include OpenViking owner epochs and source/URI
barriers. Restore applies them before recall, replaces old Leader session IDs and
starts memory in `RESTORE_REQUIRED`. The protected memory reconciliation endpoint
performs read-only checks of remote extraction/mutation state before reopening
recall. Until those checks succeed, chat can continue without recalled memory;
old synchronization work remains fenced. Restore needs the newest deletion ledger,
not one stored alongside the old snapshot.

## Automatic QQ onboarding

After the one-time `QQBOT_CREDENTIAL_KEY` environment setup, `/admin/qq-onboarding` defaults to automatic owner setup. The server validates the official scan response, saves encrypted credentials and owner-bound permissions, and connects through QQ WebSocket. Enabled configurations reconnect on daemon startup. Per-instance `transport` selects Webhook or WebSocket, and `intents` records the permitted subscription. Connection status is included in `/health/ready` and `/api/v1/qq/onboarding/status`. See [the onboarding workflow](../docs/operations/qq-onboarding.md).
