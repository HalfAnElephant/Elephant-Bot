# Ubuntu daemon deployment

The supported runtime is one Python daemon under `elephant`, its on-demand Pi processes,
SQLite and OpenViking. QQ uses the official adapter. There is no separate Worker
listener or NapCat route. The daemon admits Leader, Worker and Hermes review turns
through the same execution pool and limits actual model requests separately.
Background Agent admission and task trees have no fixed count or total execution
time cap. Legacy count and background duration settings remain accepted for
configuration compatibility. Model requests retain upstream concurrency
backpressure, and individual tools and external waits retain their timeouts.
Interrupted or blocked Workers preserve results for a Leader decision. A Leader
takeover requires stopped related execution and reconciled external operations.

## Guided Ubuntu installation

Use Ubuntu 24.04 or newer with systemd and cgroup v2. Download the complete
release bundle and verify its checksums and revision using the
[release instructions](../docs/runbooks/releases.md#手动部署). For a **new**
installation, stage the verified bundle as root under `/opt/elephant`; do not
extract over an existing install. From the directory containing the verified archive:

```sh
sudo mkdir -m 0755 /opt/elephant &&
  sudo tar -xzf elephant-bot.tar.gz -C /opt/elephant --strip-components=1 --no-same-owner &&
  sudo /opt/elephant/deploy/install-system.sh --apply
sudo /opt/elephant/.venv/bin/elephant setup --system
```

The installer prepares system dependencies, the locked runtime, the unprivileged
service account, and `elephant.service`. Run the installer without `--apply` to
print its plan without changing the system. The setup wizard collects the model
configuration, optionally displays the official QQ QR code in the terminal, and
asks whether to start the installed service and enable it at boot. No shell commands or config
paths are shown in the routine wizard. For managed service failures see
[Acceptance](#acceptance). The terminal
scan uses the service identity and the same database lock as the daemon. If the
daemon is already running, use its management page instead; setup does not stop
or restart it. The daemon listens on loopback. For manual QQ administration over
SSH, forward port 8080 as shown below, not through a public management listener.

For a supported update to an installer-managed unit, drain and stop it first,
back up its data, then rerun the installer from a verified replacement release at
`/opt/elephant`. Locally edited and unknown units are refused, not overwritten;
the installer retains private configuration and data. See the existing-installation
notes below before staging the replacement. macOS uses the local CLI and has no systemd setup.

## Fresh Ubuntu installation (manual)

The commands below prepare a new Ubuntu 24.04 or newer machine using systemd,
cgroup v2 and an amd64 or arm64 CPU. Run them in a root shell opened with `sudo -i`.
They install the daemon as `elephant.service`. Existing installations use the upgrade
section below and retain their current configuration and service name.

Install the system dependencies, uv and Node.js. The pinned Pi package requires
Node.js 22.19.0 or newer. Python 3.13 will be installed by uv in a root-owned,
service-readable directory. FFmpeg 6 or newer and the locked `pilk-nogil` Python
dependency decode QQ voice messages.

```sh
set -eu
apt-get update
apt-get install -y ca-certificates curl git xz-utils openssl sudo \
  bubblewrap ffmpeg dbus-user-session
install_tmp=$(mktemp -d)
curl -fLsS https://astral.sh/uv/0.11.8/install.sh -o "$install_tmp/uv-install.sh"
env UV_INSTALL_DIR=/usr/local/bin UV_NO_MODIFY_PATH=1 sh "$install_tmp/uv-install.sh"

case "$(dpkg --print-architecture)" in
  amd64) node_arch=x64 ;;
  arm64) node_arch=arm64 ;;
  *) echo 'Use a Node.js installation matching this CPU' >&2; exit 1 ;;
esac
node_archive="node-v22.19.0-linux-$node_arch.tar.xz"
curl -fLsS "https://nodejs.org/dist/v22.19.0/$node_archive" -o "$install_tmp/$node_archive"
curl -fLsS https://nodejs.org/dist/v22.19.0/SHASUMS256.txt -o "$install_tmp/SHASUMS256.txt"
(cd "$install_tmp" && sha256sum --check --ignore-missing SHASUMS256.txt)
install -d -o root -g root -m 0755 /opt/elephant-node
tar -xJf "$install_tmp/$node_archive" -C /opt/elephant-node --strip-components=1 --no-same-owner
ln -sf /opt/elephant-node/bin/node /usr/local/bin/node
ln -sf /opt/elephant-node/bin/npm /usr/local/bin/npm
rm -rf "$install_tmp"
```

Create the service account, both groups used by the service unit, and its writable
data directory. Install the locked packages and stop helper from the checkout.

```sh
getent group elephant-worker >/dev/null || groupadd --system elephant-worker
getent group elephant >/dev/null || groupadd --system elephant
getent passwd elephant >/dev/null || useradd --system --gid elephant-worker \
  --home-dir /var/lib/elephant --shell /usr/sbin/nologin elephant
install -d -o elephant -g elephant-worker -m 0700 /var/lib/elephant
install -d -o root -g elephant-worker -m 0750 /etc/elephant
install -d -o root -g root -m 0755 /var/lib/elephant-daemon-stops /usr/local/libexec /opt/elephant-python

git clone https://github.com/HalfAnElephant/Elephant-Bot.git /opt/elephant
cd /opt/elephant
UV_PYTHON_INSTALL_DIR=/opt/elephant-python uv sync --locked --no-dev --no-editable --python 3.13
npm ci --ignore-scripts
chmod -R go+rX /opt/elephant /opt/elephant-python /opt/elephant-node
install -o root -g root -m 0755 scripts/elephant-daemon-stopped /usr/local/libexec/elephant-daemon-stopped
install -o root -g root -m 0644 deploy/elephant.service /etc/systemd/system/elephant.service
if [ ! -e /etc/elephant/core.yaml ]; then
  install -o root -g elephant-worker -m 0640 configs/production.yaml /etc/elephant/core.yaml
fi
if [ ! -e /etc/elephant/core.env ]; then
  install -o root -g root -m 0600 .env.example /etc/elephant/core.env
fi
deploy/elephant-job-user-setup.sh elephant elephant.service
```

The production template puts SQLite, Pi sessions, workspaces and other durable
state under `/var/lib/elephant`. The Worker runs in process; the legacy
`/run/elephant/internal.sock` setting is retained for startup cleanup, not a listener.
Pi uses the absolute executable
`/opt/elephant/node_modules/.bin/pi`. Keep `/opt/elephant` and the Python installation
root-owned and readable by the service account.

Edit `/etc/elephant/core.yaml` and `/etc/elephant/core.env` before starting. Set
`model.endpoint` to the complete Chat Completions URL, `model.model` to your
provider's model name, and `model.context_window` to its supported context size.
Set `ELEPHANT_MODEL_KEY` in the environment file. Generate an admin token with
`openssl rand -hex 32` and a credential encryption key with
`openssl rand -base64 32`, then put those values in `ELEPHANT_ADMIN_TOKEN` and
`ELEPHANT_CREDENTIAL_KEY`. Generate the encryption key once and keep it with the
instance's secrets across restarts and upgrades. The existing-key dependency and
QQ scan steps are described in [QQ onboarding](../docs/operations/qq-onboarding.md).

The empty adapter list allows initial setup through the protected admin page.
After startup, use QQ onboarding to save credentials, verify the owner and connect
the official adapter. Long-term memory is optional and requires its own
[OpenViking configuration](../docs/openviking.md). Explicit personal
preferences belong in `owner.preferences`. The production template and
`configs/schema.json` describe the runtime configuration.

Validate and migrate with the daemon's identity and environment, then start the
service. The shell below reads the private environment file as root and passes
the variables to the service-account processes.

```sh
cd /opt/elephant
set -a
. /etc/elephant/core.env
set +a
runuser -u elephant -g elephant-worker -- /opt/elephant/.venv/bin/elephant validate --config /etc/elephant/core.yaml
runuser -u elephant -g elephant-worker -- /opt/elephant/.venv/bin/elephant doctor --config /etc/elephant/core.yaml
runuser -u elephant -g elephant-worker -- /opt/elephant/.venv/bin/elephant migrate --config /etc/elephant/core.yaml
runuser -u elephant -g elephant-worker -- /opt/elephant/node_modules/.bin/pi --version
runuser -u elephant -g elephant-worker -- bwrap --unshare-all --ro-bind / / -- /usr/bin/true
systemctl daemon-reload
systemctl enable --now elephant.service
systemctl status elephant.service --no-pager
curl --fail http://127.0.0.1:8080/health/live
curl --fail http://127.0.0.1:8080/health/ready
```

The server listens on loopback. On your own computer, run
`ssh -L 8080:127.0.0.1:8080 USER@SERVER` and open
`http://127.0.0.1:8080/admin` with the configured admin token. A failed service
reports its startup error through `journalctl -u elephant.service -n 100 --no-pager`.
Complete an actual model request and QQ message exchange during acceptance.
The bubblewrap probe must succeed as `elephant` for sandbox Workers. A user-namespace
denial requires the host's bubblewrap/AppArmor policy to permit this account's
sandbox execution before using that mode.

## Existing installations

The first release with the `elephant` command also uses a new package, service
name and directory layout. The installer refuses to adopt older `qqbot` or
`elephant-bot.service` deployments in place. Do not run the fresh installer to
migrate one: keep the old service and data together until a separate cutover has
been planned and tested. Existing environment names used for credentials remain
readable; do not regenerate the encryption key to change an environment variable name.

For a routine upgrade, preserve the installed service account, restrictions
and drop-ins. Use the [backup and staged restore checklist](../docs/runbooks/operations.md#升级前备份与回退演练)
before replacing code or running migrations. The fresh-install commands are not
an upgrade script and should not be used to regenerate secrets or overwrite an existing unit.

Drain and stop the current service before replacing its installed packages. Keep
its service name, environment file, encryption key, database and runtime-config
history. Use the existing configuration with the current schema and explicitly
review the production template's paths when adopting it. Remove retired social,
Judge, expression and persona settings before validating. Disable any retired
Core and Worker units and start the daemon after validation and migration.

The normal Alembic migration bridges the recognized
experimental 0010 schema without modifying published revisions. `migrate(path,
backup=False)` supports an explicitly authorized deployment without a database
backup. The CLI keeps its pre-migration backup behavior. Preserve Inbox, Outbox
UNKNOWN states, operation receipts and deletion barriers. See the
[release and deployment procedure](../docs/runbooks/releases.md) for the package
and backup steps.

The daemon service uses `KillMode=control-group`. Its privileged `ExecStopPost`
helper checks the cgroup is empty and fsyncs a root-owned receipt bound to the
systemd invocation. Recovery never treats an expired lease as proof of process
termination. Once stop is proven, a completed native boundary is committed through
the original result validation; unfinished tools and uncertain external writes
require readback. They are never automatically dispatched again. Keep the stop
helper and receipt directory unwritable by the daemon.

## Persistent Jobs and execution isolation

Provision the daemon account with root-only
`deploy/elephant-job-user-setup.sh elephant elephant.service`. This enables its systemd user manager
and linger, so Jobs survive daemon restarts. See
[Job operations](../docs/operations/persistent-jobs.md) for receipts and acceptance.
The setup script writes `/etc/elephant/jobs.env` and a service drop-in with the actual account UID. The drop-in orders the daemon after that user manager and exposes its `/run/user/UID` directory. Host execution uses the service account's existing filesystem permissions and does not grant sudo or root.
The optional second argument selects the installed service unit. One-argument
calls target `elephant.service`; another unit must be named explicitly.

Install `bubblewrap` and validate user namespaces under the actual service account.
`execution.leader_environment` defaults to `host`, and `execution.worker_environment`
defaults to `sandbox`. Leader delegation can select `environment: host` or
`environment: sandbox` for each child. The accepted mode is stored with the task and
kept across continuation and recovery. A sandbox Worker cannot delegate a host child.
Sandbox processes and Jobs have a cleared environment, isolated mount/PID/network
namespaces, system binaries, and their single authorized workspace. Host processes
and Jobs run directly under the service account, with an explicit environment and
an optional host working directory. Host file tools also accept absolute paths.
Foreground host processes require Linux cgroup v2 with `cgroup.kill` and the service
unit's `Delegate=yes`. Each command enters a child cgroup before exec; cleanup
kills and verifies the entire group, including detached descendants. These groups
remain inside the daemon service for the existing stop receipt. Explicitly starting
an external service is a host side effect, whose lifetime is independent of the run.
Existing `ProtectSystem`, `ProtectHome` or path restrictions may limit host mode.
Keep them during ordinary upgrades. Review the effective unit and grant only the
access specifically required for a deliberately enabled host workload; do not
switch a sandbox task to host just because sandbox preflight failed.
Generic process
execution receives no GitHub or calendar credentials. GitHub CLI and Git transport
use their existing controlled credential providers. Pi has no built-in tools,
automatic skill scan, context-file injection or plugin mirror; its trusted extension
calls the host gateway.

For explicitly authorized full host administration, configure the daemon account
with passwordless sudo and allow privilege elevation in the service. Use
`systemctl edit elephant.service` for a new installation, or the actual unit name for
an existing installation.

```ini
[Service]
NoNewPrivileges=false
```

Use `visudo -f /etc/sudoers.d/elephant-host-admin` to grant the account root access.

```sudoers
elephant ALL=(root) NOPASSWD: ALL
```

Validate with `visudo -c`, reload systemd, and restart the daemon. Verify
`sudo -n id -u` returns `0` through an actual host process and persistent Job.
This grants root access to the Leader and host Workers running as `elephant`.
Sandbox execution enforces its workspace isolation and privilege restrictions.

To use an existing local GitHub login, add service credentials:

```ini
[Service]
LoadCredential=hosts.yml:/home/LOGIN_USER/.config/gh/hosts.yml
LoadCredential=config.yml:/home/LOGIN_USER/.config/gh/config.yml
Environment=GH_CONFIG_DIR=%d
```

For the optional calendar provider, use a private JSON configuration containing
`apple_id`, `app_password`, `timezone`, `default_calendar` and the exact authorized
`owner` identity. Mount it with `LoadCredential=icloud-calendar.json:/etc/elephant/icloud-calendar.json`
and `Environment=ICLOUD_CALENDAR_CONFIG=%d/icloud-calendar.json`. A retired numeric
QQ identity does not authorize the official OpenID or host identity.

## Acceptance

Console-created tasks expose progress, questions and results through
`GET /api/v1/tasks/{public_id}`, including a bounded `notifications` list.
`progress.delivery.required=false` identifies this internal result channel.

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

After the one-time `ELEPHANT_CREDENTIAL_KEY` environment setup, `/admin/qq-onboarding` defaults to automatic owner setup. The server validates the official scan response, saves encrypted credentials and owner-bound permissions, and connects through QQ WebSocket. Enabled configurations reconnect on daemon startup. Per-instance `transport` selects Webhook or WebSocket, and `intents` records the permitted subscription. Connection status is included in `/health/ready` and `/api/v1/qq/onboarding/status`. See [the onboarding workflow](../docs/operations/qq-onboarding.md).
