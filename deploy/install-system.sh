#!/bin/sh
# Install only from a trusted, root-owned release staged at /opt/elephant.
set -eu

usage() {
    printf '%s\n' 'Usage: deploy/install-system.sh [--dry-run | --apply]' \
        'Dry run (default): print the plan without changing this host.' \
        'Apply: on Ubuntu 24.04+, provision dependencies and elephant.service from /opt/elephant.' \
        'The service is not enabled, started, restarted or migrated by this command.'
}

mode=${1:---dry-run}
[ "$#" -le 1 ] || { usage >&2; exit 2; }
case "$mode" in
    --help|-h) usage; exit 0 ;;
    --dry-run|--apply) ;;
    *) usage >&2; exit 2 ;;
esac

echo 'Plan: ensure Ubuntu dependencies, Elephant service account, locked runtime and elephant.service.'
echo 'Keep /etc/elephant/core.yaml, /etc/elephant/core.env and /var/lib/elephant intact.'
echo 'Do not import legacy qqbot services or data. Validate configuration before starting the service.'
[ "$mode" = --apply ] || exit 0

# Ignore a caller's PATH or tool environment when provisioning the system.
PATH=/usr/sbin:/usr/bin:/sbin:/bin
export PATH
unset ENV BASH_ENV CDPATH PYTHONPATH PYTHONHOME UV_CONFIG_FILE NPM_CONFIG_USERCONFIG
umask 077
fail() { printf '%s\n' "$*" >&2; exit 1; }
[ "$(id -u)" = 0 ] || fail 'Run --apply as root.'
[ "$(uname -s)" = Linux ] || fail 'Only Ubuntu Linux is supported.'
grep -Eq '^ID=(ubuntu|"ubuntu")$' /etc/os-release || fail 'Only Ubuntu is supported.'
version=$(awk -F= '$1=="VERSION_ID" {gsub(/"/,"",$2); print $2}' /etc/os-release)
case "$version" in ''|*[!0-9.]*) fail 'Cannot detect Ubuntu version.' ;; esac
dpkg --compare-versions "$version" ge 24.04 || fail 'Ubuntu 24.04 or newer is required.'
[ -d /run/systemd/system ] || fail 'A running systemd instance is required.'
[ -e /sys/fs/cgroup/cgroup.controllers ] || fail 'cgroup v2 is required.'

app=/opt/elephant
config=/etc/elephant
state=/var/lib/elephant
stops=/var/lib/elephant-daemon-stops
node=/opt/elephant-node
python=/opt/elephant-python
unit=elephant.service
unit_path=/etc/systemd/system/$unit
stamp=/etc/elephant/.installed-unit.sha256
uv=/usr/local/libexec/elephant-tools/uv
helper=/usr/local/libexec/elephant-daemon-stopped
command -v flock >/dev/null || fail 'Missing required base command: flock.'

for path in /etc/qqbot /var/lib/qqbot /opt/qqbot /etc/systemd/system/qqbot.service \
    /etc/systemd/system/elephant-bot.service; do
    if [ -e "$path" ] || [ -L "$path" ]; then
        fail "Legacy installation detected at $path. No changes made; use a separately reviewed migration."
    fi
done

# Privileged install reads only trusted release files and replaces only a previously managed unit.
trusted() {
    [ ! -L "$1" ] && [ "$(stat -c '%u' -- "$1")" = 0 ] &&
        [ -z "$(find "$1" -maxdepth 0 -perm /022 -print)" ]
}
# /run/lock may be writable by other accounts; use a private lock directory.
lock_dir=/run/elephant-installer
mkdir -m 0700 "$lock_dir" 2>/dev/null || {
    [ -d "$lock_dir" ] && trusted "$lock_dir" &&
        [ -z "$(find "$lock_dir" -maxdepth 0 -perm /077 -print)" ] ||
        fail 'Installer lock directory is not private and root-owned.'
}
[ ! -L "$lock_dir/install.lock" ] || fail 'Invalid installer lock file.'
exec 9>"$lock_dir/install.lock"
flock -n 9 || fail 'Another Elephant installer is running.'
for path in "$app" /opt /etc/systemd/system /usr/local/libexec; do
    [ ! -e "$path" ] || trusted "$path" || fail "Not a trusted root-owned path: $path"
done
[ -d "$app" ] && trusted "$app" || fail 'Stage a verified release at /opt/elephant first.'
# Dependencies have standard internal symlinks; the staged source must not.
unsafe=$(find "$app" -xdev \( -path "$app/.venv" -o -path "$app/node_modules" -o -path "$app/.git" \) -prune -o \
    \( ! -user root -o -perm /022 -o -type l \) -print -quit)
[ -z "$unsafe" ] || fail "Untrusted release path: $unsafe"
for path in "$app/.venv" "$app/node_modules" "$config" "$stops" "$node" "$python" "$stamp" "$unit_path" "$helper" "$uv" \
    "$unit_path.d" "$unit_path.d/jobs.conf" /usr/local/libexec/elephant-tools; do
    [ ! -L "$path" ] || fail "Refusing symbolic link: $path"
    [ ! -e "$path" ] || trusted "$path" || fail "Not a trusted root-owned path: $path"
done
for ext in core.yaml core.env jobs.env; do
    [ ! -L "$config/$ext" ] || fail "Refusing symbolic link: $config/$ext"
    [ ! -e "$config/$ext" ] || { [ -f "$config/$ext" ] && trusted "$config/$ext"; } ||
        fail "Not a trusted configuration file: $config/$ext"
done
if [ -f "$config/core.env" ]; then
    [ -z "$(find "$config/core.env" -maxdepth 0 -perm /077 -print)" ] ||
        fail 'Existing core.env must be private (0600); it was not changed.'
fi
if [ -f "$config/core.yaml" ]; then
    [ -z "$(find "$config/core.yaml" -maxdepth 0 -perm /007 -print)" ] ||
        fail 'Existing core.yaml must not be world-accessible; it was not changed.'
fi
# The Job helper writes its two files on fresh setup only. Preserve a complete
# pre-existing setup; stop before mutation if the existing setup is incomplete.
if [ -e "$config/jobs.env" ] || [ -e "$unit_path.d/jobs.conf" ]; then
    [ -f "$config/jobs.env" ] && [ -f "$unit_path.d/jobs.conf" ] ||
        fail 'Existing Job provisioning is incomplete. Review existing jobs.env and jobs.conf manually.'
    provision_jobs=no
else
    provision_jobs=yes
fi
[ ! -L "$state" ] || fail "Refusing symbolic link: $state"
for file in uv.lock package-lock.json configs/production.yaml .env.example deploy/elephant.service \
    deploy/elephant-job-user-setup.sh scripts/elephant-daemon-stopped; do
    [ -f "$app/$file" ] || fail "Missing release file: $file"
done

active=$(systemctl show -p ActiveState --value "$unit") || fail 'Could not inspect the existing unit.'
case "$active" in inactive|failed) ;; *) fail "Stop $unit and drain tasks before provisioning." ;; esac
fragment=$(systemctl show -p FragmentPath --value "$unit") || fail 'Could not inspect the unit fragment.'
[ -z "$fragment" ] || [ "$fragment" = "$unit_path" ] || fail 'An unmanaged Elephant unit already exists.'
if [ -e "$unit_path" ]; then
    [ -f "$unit_path" ] && [ -f "$stamp" ] && [ "$(cat "$stamp")" = "$(sha256sum "$unit_path" | cut -d ' ' -f 1)" ] ||
        fail 'Existing unit is unmanaged or locally edited. Review it manually; it was not changed.'
fi

if [ -e "$helper" ]; then
    [ -f "$helper" ] && { [ -f "$stamp" ] || cmp -s "$helper" "$app/scripts/elephant-daemon-stopped"; } ||
        fail 'An unmanaged stop helper already exists; review it manually.'
fi
if getent passwd elephant >/dev/null; then
    entry=$(getent passwd elephant)
    [ "$(echo "$entry" | cut -d: -f6)" = "$state" ] &&
        [ "$(echo "$entry" | cut -d: -f7)" = /usr/sbin/nologin ] &&
        [ "$(id -gn elephant)" = elephant-worker ] && [ "$(id -u elephant)" != 0 ] ||
        fail 'An incompatible elephant account already exists.'
fi
if [ -e "$state" ]; then
    [ -d "$state" ] && getent passwd elephant >/dev/null &&
        [ "$(stat -c %u "$state")" = "$(id -u elephant)" ] &&
        [ -z "$(find "$state" -maxdepth 0 -perm /077 -print)" ] ||
        fail 'Existing state must be private and owned by the expected service account.'
fi
# Reject existing external runtime paths writable by an unprivileged account.
for path in "$app/.venv" "$app/node_modules" "$node" "$python" /usr/local/libexec/elephant-tools; do
    if [ -d "$path" ]; then
        unsafe=$(find -P "$path" -xdev ! -type l \( ! -user root -o -perm /022 \) -print -quit)
        [ -z "$unsafe" ] || fail "Untrusted existing runtime path: $unsafe"
    fi
done
case "$(dpkg --print-architecture)" in amd64) arch=x64 ;; arm64) arch=arm64 ;; *) fail 'Unsupported CPU architecture.' ;; esac

stage=$(mktemp -d /run/elephant-install.XXXXXX)
trap 'rm -rf -- "$stage"' EXIT
env -i PATH="$PATH" HOME=/root apt-get update
env -i PATH="$PATH" HOME=/root apt-get install -y ca-certificates curl xz-utils bubblewrap ffmpeg dbus-user-session openssl sudo

archive="node-v22.19.0-linux-$arch.tar.xz"
node_url=https://nodejs.org/dist/v22.19.0
env -i PATH="$PATH" HOME=/root curl --disable -fLsS "$node_url/$archive" -o "$stage/$archive"
env -i PATH="$PATH" HOME=/root curl --disable -fLsS "$node_url/SHASUMS256.txt" -o "$stage/SHASUMS256.txt"
(cd "$stage" && awk -v f="$archive" '$2==f { print; n++ } END { if(n!=1) exit 1 }' SHASUMS256.txt > node.sha256 &&
    sha256sum --check --strict node.sha256)
install -d -o root -g root -m 0755 "$node" "$python" /usr/local/libexec/elephant-tools
tar -xJf "$stage/$archive" -C "$node" --strip-components=1 --no-same-owner --no-same-permissions
chmod -R a+rX "$node"
env -i PATH="$PATH" HOME=/root curl --disable -fLsS https://astral.sh/uv/0.11.8/install.sh -o "$stage/uv-install.sh"
env -i PATH="$PATH" HOME=/root UV_INSTALL_DIR=/usr/local/libexec/elephant-tools UV_NO_MODIFY_PATH=1 /bin/sh "$stage/uv-install.sh"
PATH=$node/bin:$PATH
export PATH
cd "$app"
chmod 0755 "$app"
env -i PATH="$PATH" HOME=/root UV_PYTHON_INSTALL_DIR="$python" "$uv" --no-config sync --locked --no-dev --no-editable --python 3.13.9
env -i PATH="$PATH" HOME=/root npm ci --ignore-scripts
chmod -R a+rX "$python" "$app/.venv" "$app/node_modules"

getent group elephant-worker >/dev/null || groupadd --system elephant-worker
getent group elephant >/dev/null || groupadd --system elephant
getent passwd elephant >/dev/null || useradd --system --gid elephant-worker --home-dir "$state" --shell /usr/sbin/nologin elephant
[ -e "$state" ] || install -d -o elephant -g elephant-worker -m 0700 "$state"
[ -e "$config" ] || install -d -o root -g elephant-worker -m 0750 "$config"
[ -e "$stops" ] || install -d -o root -g root -m 0755 "$stops"
[ -e "$config/core.yaml" ] || install -o root -g elephant-worker -m 0640 configs/production.yaml "$config/core.yaml"
[ -e "$config/core.env" ] || install -o root -g root -m 0600 .env.example "$config/core.env"

install -o root -g root -m 0755 scripts/elephant-daemon-stopped "$helper"
# Install the bundle unit and record its digest, without changing other unit drop-ins.
new_unit=$(mktemp /etc/systemd/system/.elephant.service.XXXXXX)
install -o root -g root -m 0644 deploy/elephant.service "$new_unit"
mv -f -- "$new_unit" "$unit_path"
sha256sum "$unit_path" | cut -d ' ' -f 1 > "$stamp"
[ "$provision_jobs" = no ] || env -i PATH="$PATH" HOME=/root deploy/elephant-job-user-setup.sh elephant "$unit"
systemctl daemon-reload
echo 'Installed. The Elephant daemon was not started.'
if [ -d "$unit_path.d" ]; then
    echo 'Review this unit and any existing overrides with: systemctl cat elephant.service'
fi
echo 'Next: sudo /opt/elephant/.venv/bin/elephant setup --system'
