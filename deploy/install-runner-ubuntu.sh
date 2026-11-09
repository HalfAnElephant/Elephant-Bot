#!/bin/sh
# Installation template. Run as root from a root-owned /opt/qqbot checkout.
set -eu
[ "$(id -u)" = 0 ] || { echo 'Run as root' >&2; exit 1; }
[ "$(uname -s)" = Linux ] || { echo 'Linux is required' >&2; exit 1; }
# Bubblewrap is also required by credentialed gh and isolated workspace processes.
if ! command -v bwrap >/dev/null 2>&1; then
  apt-get update
  apt-get install -y bubblewrap
fi
cd /opt/qqbot
[ -x .venv/bin/python ] && [ -x /usr/bin/node ]
# Validate before granting sudo access to the Python launcher/runtime.
/usr/bin/python3 -I - <<'PYTHON'
import os
import stat
from pathlib import Path
for root in (Path('/opt/qqbot/.venv'),Path('/opt/qqbot/src/qqbot'),Path('/opt/qqbot/node_modules')):
    for directory, directories, files in os.walk(root,followlinks=False):
        for item in [Path(directory),*[Path(directory)/name for name in directories+files]]:
            target = item.resolve(strict=True)
            for path in [target,*target.parents]:
                info = path.stat()
                if info.st_uid != 0 or info.st_mode & 0o022:
                    raise SystemExit('Runtime must be root-owned and non-writable by other users: '+str(path))
PYTHON
getent passwd qqbot-worker >/dev/null || useradd --system --home /var/lib/qqbot-worker --shell /usr/sbin/nologin qqbot-worker
getent passwd qqbot-runner >/dev/null || useradd --system --home /nonexistent --shell /usr/sbin/nologin qqbot-runner
install -d -o qqbot-worker -g qqbot-worker -m 0700 /var/lib/qqbot-worker /var/lib/qqbot-worker/launch /var/lib/qqbot-worker/spool
install -d -o root -g root -m 0755 /var/lib/qqbot-runner-stops
install -d -o root -g root -m 0700 /var/lib/qqbot-runner-manifests /var/lib/qqbot-runner-locks
install -d -o root -g root -m 0711 /var/lib/qqbot-runners
install -d -o root -g root -m 0755 /var/lib/qqbot-runner-root /etc/qqbot /opt/qqbot/bin /usr/local/libexec
for directory in usr lib lib64 opt/qqbot/node_modules opt/qqbot/bin run/qqbot workspace proc dev sys tmp var/tmp; do
  install -d -o root -g root -m 0755 "/var/lib/qqbot-runner-root/$directory"
done
install -o root -g root -m 0755 scripts/pi-runner-launch /usr/local/libexec/pi-runner-launch
install -o root -g root -m 0755 scripts/pi-runner-entry /opt/qqbot/bin/pi-runner-entry
install -o root -g root -m 0755 scripts/pi-isolation-probe /opt/qqbot/bin/pi-isolation-probe
install -o root -g root -m 0755 scripts/pi-isolation-check /usr/local/libexec/pi-isolation-check
install -o root -g root -m 0444 src/qqbot/harness/research_extension.ts /opt/qqbot/research_extension.ts
install -o root -g root -m 0444 src/qqbot/harness/pi_bridge.ts /opt/qqbot/pi_bridge.ts
install -o root -g root -m 0644 deploy/runner-policy.json /etc/qqbot/runner-policy.json
runner_sudoers_tmp=$(mktemp /etc/sudoers.d/.qqbot-runner.XXXXXX)
trap 'rm -f "$runner_sudoers_tmp"' EXIT
install -o root -g root -m 0440 deploy/pi-runner.sudoers "$runner_sudoers_tmp"
visudo -cf "$runner_sudoers_tmp"
mv -f "$runner_sudoers_tmp" /etc/sudoers.d/qqbot-runner
trap - EXIT
install -o root -g root -m 0755 scripts/pi-runner-network /usr/local/libexec/pi-runner-network
install -o root -g root -m 0644 deploy/qqbot-runner-network.service /etc/systemd/system/qqbot-runner-network.service
/usr/local/libexec/pi-runner-network
printf '%s\n' 'Installed with isolation_verified=false. Verify runtime ownership, archive ACLs and Linux boundary tests before enabling the policy.'
