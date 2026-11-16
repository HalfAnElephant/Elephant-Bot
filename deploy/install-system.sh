#!/bin/sh
# Install only from a trusted, root-owned release staged at /opt/elephant.
set -eu

usage() {
    printf '%s\n' 'Usage: deploy/install-system.sh [--dry-run | --apply [--install-pi]]' \
        'Dry run (default): print the plan without changing this host.' \
        'Apply: provision system dependencies and elephant.service from /opt/elephant.' \
        'Reuse a matching trusted Pi when available; --install-pi approves a private fallback.' \
        'Linux: Ubuntu 24.04+, Debian 13+, Fedora 43+, official Arch rolling.' \
        'The service is not enabled, started, restarted or migrated by this command.'
}

mode=
allow_pi=no
for flag do
    case "$flag" in
        --apply|--dry-run) [ -z "$mode" ] || { usage >&2; exit 2; }; mode=$flag ;;
        --install-pi) [ "$allow_pi" = no ] || { usage >&2; exit 2; }; allow_pi=yes ;;
        --help|-h) usage; exit 0 ;;
        *) usage >&2; exit 2 ;;
    esac
done
mode=${mode:---dry-run}
[ "$allow_pi" = no ] || [ "$mode" = --apply ] || { usage >&2; exit 2; }

echo 'Plan: ensure distribution dependencies, Elephant service account, locked runtime and elephant.service.'
echo 'Reuse Pi of the exact required version, or ask before installing it privately.'
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
[ "$(uname -s)" = Linux ] || fail 'Only Linux is supported by this system installer.'
# Read metadata, never source a file as shell code or treat derivatives as tested.
os_id=$(awk -F= '$1=="ID" {gsub(/"/,"",$2); print $2}' /etc/os-release)
version=$(awk -F= '$1=="VERSION_ID" {gsub(/"/,"",$2); print $2}' /etc/os-release)
case "$os_id" in
    ubuntu|debian|fedora)
        case "$version" in ''|*[!0-9.]*|.*|*.) fail "Cannot detect supported $os_id version." ;; esac
        ;;
    arch) ;;
    *) fail 'Unsupported distribution. Use Ubuntu, Debian, Fedora or official Arch Linux.' ;;
esac
case "$os_id" in
    ubuntu)
        command -v apt-get >/dev/null && command -v dpkg >/dev/null || fail 'Ubuntu requires apt-get and dpkg.'
        dpkg --compare-versions "$version" ge 24.04 || fail 'Ubuntu 24.04 or newer is required.'
        ;;
    debian)
        command -v apt-get >/dev/null && command -v dpkg >/dev/null || fail 'Debian requires apt-get and dpkg.'
        dpkg --compare-versions "$version" ge 13 || fail 'Debian 13 or newer is required.'
        ;;
    fedora)
        case "$version" in *.*) fail 'Expected a numbered Fedora release.' ;; esac
        [ "$version" -ge 43 ] || fail 'Fedora 43 or newer is required.'
        command -v dnf >/dev/null || fail 'Fedora requires dnf.'
        ;;
    arch)
        command -v pacman >/dev/null || fail 'Arch requires pacman.'
        ;;
esac
case "$(uname -m)" in
    x86_64) arch=x64 ;;
    aarch64|arm64)
        [ "$os_id" != arch ] || fail 'Official Arch Linux installation requires x86_64.'
        arch=arm64
        ;;
    *) fail 'Unsupported CPU architecture. Use x86_64 or arm64.' ;;
esac
[ -d /run/systemd/system ] || fail 'A running systemd instance is required.'
[ -e /sys/fs/cgroup/cgroup.controllers ] || fail 'cgroup v2 is required.'
# The root cgroup deliberately has no cgroup.kill. Inspect an existing child
# instead; this only checks the kernel interface, not service-level delegation.
kill_interface=$(find /sys/fs/cgroup -xdev -mindepth 2 -maxdepth 2 -name cgroup.kill -type f -print -quit) ||
    fail 'Could not inspect the cgroup v2 hierarchy.'
[ -n "$kill_interface" ] || fail 'cgroup v2 with cgroup.kill in a non-root cgroup is required.'

app=/opt/elephant
config=/etc/elephant
state=/var/lib/elephant
stops=/var/lib/elephant-daemon-stops
node=/opt/elephant-node
python=/opt/elephant-python
unit=elephant.service
unit_path=/etc/systemd/system/$unit
stamp=/etc/elephant/.installed-unit.sha256
pending=/etc/elephant/.pending-unit.sha256
uv=/usr/local/libexec/elephant-tools/uv
helper=/usr/local/libexec/elephant-daemon-stopped
for tool in flock runuser; do
    command -v "$tool" >/dev/null || fail "Missing required base command: $tool."
done

# Check effective access rather than assuming a numeric mode proves that the
# service account can traverse parent directories (ACLs can matter as well).
as_service() {
    runuser -u elephant -g elephant-worker -- /usr/bin/test "$@"
}
check_service_config() {
    if [ -e "$config" ]; then
        as_service -x "$config" ||
            fail "Service account cannot traverse $config. Existing configuration was not changed."
    fi
    if [ -e "$config/core.yaml" ]; then
        as_service -r "$config/core.yaml" ||
            fail "Service account cannot read $config/core.yaml. Existing configuration was not changed."
    fi
    # core.env is read by systemd as root; never make it service-readable.
}

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
for path in "$app/.venv" "$app/node_modules" "$config" "$stops" "$node" "$python" "$stamp" "$pending" "$unit_path" "$helper" "$uv" \
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
        fail 'Existing Job provisioning is incomplete. If the existing file is installer-generated, rerun deploy/elephant-job-user-setup.sh elephant elephant.service as root to complete it; custom files require manual review.'
    provision_jobs=no
else
    provision_jobs=yes
fi
[ ! -L "$state" ] || fail "Refusing symbolic link: $state"
for file in uv.lock package-lock.json configs/production.yaml .env.example deploy/elephant.service \
    deploy/elephant-job-user-setup.sh deploy/pi-install.sh scripts/elephant-daemon-stopped; do
    [ -f "$app/$file" ] || fail "Missing release file: $file"
done

require_inactive() {
    active=$(systemctl show -p ActiveState --value "$unit") || fail 'Could not inspect the existing unit.'
    case "$active" in inactive|failed) ;; *) fail "Stop $unit and drain tasks before provisioning." ;; esac
}
require_inactive
fragment=$(systemctl show -p FragmentPath --value "$unit") || fail 'Could not inspect the unit fragment.'
[ -z "$fragment" ] || [ "$fragment" = "$unit_path" ] || fail 'An unmanaged Elephant unit already exists.'

unit_hash() {
    digest=$(sha256sum -- "$1") || return 1
    printf '%s\n' "${digest%% *}"
}
trusted_digest() {
    [ -f "$1" ] && [ "$(wc -c < "$1")" -eq 65 ] && grep -Ex '[0-9a-f]{64}' "$1"
}
# The private pending digest is written before the unit is published. If the
# previous install stopped between renames, accept only the exact intended unit.
managed_unit=no
[ ! -e "$stamp" ] || [ -f "$stamp" ] || fail 'Installed unit digest is not a regular file.'
if [ -e "$pending" ]; then
    pending_hash=$(trusted_digest "$pending") ||
        fail 'Pending installer unit digest is invalid. Review it manually; no unit was changed.'
else
    pending_hash=
fi
if [ -e "$unit_path" ]; then
    [ -f "$unit_path" ] || fail 'Existing unit is not a regular file.'
    installed_hash=$(unit_hash "$unit_path") || fail 'Could not hash the existing unit.'
    if [ -e "$stamp" ]; then
        stamped_hash=$(trusted_digest "$stamp") || stamped_hash=
    else
        stamped_hash=
    fi
    if [ "$installed_hash" = "$stamped_hash" ] || [ "$installed_hash" = "$pending_hash" ]; then
        managed_unit=yes
    else
        fail 'Existing unit is unmanaged or locally edited. Review it manually; it was not changed.'
    fi
fi

if [ -e "$helper" ]; then
    [ -f "$helper" ] && { [ "$managed_unit" = yes ] || cmp -s "$helper" "$app/scripts/elephant-daemon-stopped"; } ||
        fail 'An unmanaged stop helper already exists; review it manually.'
fi
if getent passwd elephant >/dev/null; then
    entry=$(getent passwd elephant)
    [ "$(echo "$entry" | cut -d: -f6)" = "$state" ] &&
        [ "$(echo "$entry" | cut -d: -f7)" = /usr/sbin/nologin ] &&
        [ "$(id -gn elephant)" = elephant-worker ] && [ "$(id -u elephant)" != 0 ] ||
        fail 'An incompatible elephant account already exists.'
    check_service_config
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
        unsafe=$(find -P "$path" -xdev \( ! -user root -o \( ! -type l -perm /022 \) \) -print -quit)
        [ -z "$unsafe" ] || fail "Untrusted existing runtime path: $unsafe"
    fi
done

stage=$(mktemp -d /run/elephant-install.XXXXXX)
new_unit=
new_digest=
trap 'rm -rf -- "$stage"; [ -z "$new_unit" ] || rm -f -- "$new_unit"; [ -z "$new_digest" ] || rm -f -- "$new_digest"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
case "$os_id" in
    ubuntu|debian)
        env -i PATH="$PATH" HOME=/root apt-get update
        env -i PATH="$PATH" HOME=/root apt-get install -y \
            ca-certificates curl xz-utils bubblewrap ffmpeg dbus-user-session openssl sudo
        ;;
    fedora)
        env -i PATH="$PATH" HOME=/root dnf install -y \
            ca-certificates curl xz bubblewrap ffmpeg-free openssl sudo
        ;;
    arch)
        # Never perform a partial database refresh or silently upgrade the OS.
        # The operator must begin with a fully updated official Arch installation.
        env -i PATH="$PATH" HOME=/root pacman -S --needed --noconfirm \
            ca-certificates curl xz bubblewrap ffmpeg openssl sudo ||
            fail 'Arch package installation failed. Update the whole system first using the official Arch process.'
        ;;
esac

archive="node-v22.19.0-linux-$arch.tar.xz"
node_url=https://nodejs.org/dist/v22.19.0
env -i PATH="$PATH" HOME=/root curl --disable -fLsS "$node_url/$archive" -o "$stage/$archive"
env -i PATH="$PATH" HOME=/root curl --disable -fLsS "$node_url/SHASUMS256.txt" -o "$stage/SHASUMS256.txt"
(cd "$stage" && awk -v f="$archive" '$2==f { print; n++ } END { if(n!=1) exit 1 }' SHASUMS256.txt > node.sha256 &&
    sha256sum --check --strict node.sha256)
require_inactive
install -d -o root -g root -m 0755 "$node" /usr/local/libexec/elephant-tools
tar -xJf "$stage/$archive" -C "$node" --strip-components=1 --no-same-owner --no-same-permissions
chmod -R a+rX "$node"
env -i PATH="$PATH" HOME=/root curl --disable -fLsS https://astral.sh/uv/0.11.8/install.sh -o "$stage/uv-install.sh"
env -i PATH="$PATH" HOME=/root UV_INSTALL_DIR=/usr/local/libexec/elephant-tools UV_NO_MODIFY_PATH=1 /bin/sh "$stage/uv-install.sh"
PATH=$node/bin:$PATH
export PATH
cd "$app"
require_inactive
# uv's platform archive and wheel modes are not a deployment permissions
# policy. Keep both trusted parents inaccessible while generating files;
# the two trees stay closed if installation or verification fails.
[ -e "$python" ] || install -d -o root -g root -m 0700 "$python"
chmod 0700 "$app" "$python"
verify_managed_interpreter() {
    interpreter=$(realpath -e -- "$app/.venv/bin/python") || fail 'Could not resolve the installed Python interpreter.'
    case "$interpreter" in
        "$python"/*) ;;
        *) fail 'Installed Python interpreter does not resolve into the managed runtime.' ;;
    esac
    [ -f "$interpreter" ] && [ -x "$interpreter" ] && trusted "$interpreter" ||
        fail 'Installed Python interpreter is not a trusted root-owned executable.'
}
# uv may inspect the pre-existing venv interpreter during sync. Do not
# execute a symlink to an outside or writable interpreter even as root.
if [ -e "$app/.venv/bin/python" ] || [ -L "$app/.venv/bin/python" ]; then
    verify_managed_interpreter
fi
echo 'Preparing private managed Python and virtual environment.'
env -i PATH="$PATH" HOME=/root UV_PYTHON_INSTALL_DIR="$python" "$uv" --no-config sync \
    --locked --no-dev --no-editable --managed-python --link-mode copy --reinstall --python 3.13.9 ||
    fail 'Managed Python installation failed. Runtime directories remain private; review the installer diagnostics before retrying.'

# Verify ownership before changing mode, and never chmod a symlink target or
# a file hard-linked into a cache/other installation. Copy mode + reinstall
# replaces older uv cache hardlinks before this normalization.
for directory in "$python" "$app/.venv"; do
    [ -d "$directory" ] && [ ! -L "$directory" ] || fail "Missing managed runtime directory: $directory"
    unsafe=$(find -P "$directory" -xdev ! -user root -print -quit) ||
        fail "Could not inspect managed Python ownership: $directory"
    [ -z "$unsafe" ] || fail "Non-root-owned managed Python path: $unsafe"
    unsafe=$(find -P "$directory" -xdev -type f -links +1 -print -quit) ||
        fail "Could not inspect managed Python file links: $directory"
    [ -z "$unsafe" ] || fail "Hard-linked managed Python file cannot be normalized safely: $unsafe"
    # Operate on children explicitly, leaving the public interpreter root
    # closed until the full verification has succeeded.
    find -P "$directory" -xdev -mindepth 1 ! -type l -exec chmod a+rX,go-w -- {} + ||
        fail "Could not normalize managed runtime modes: $directory"
    if [ "$directory" != "$python" ]; then
        chmod a+rX,go-w -- "$directory"
    fi
    unsafe=$(find -P "$directory" -xdev ! -type l -perm /022 -print -quit) ||
        fail "Could not verify managed runtime modes: $directory"
    [ -z "$unsafe" ] || fail "Group/world-writable managed Python path: $unsafe"
done
verify_managed_interpreter
chmod 0755 "$python" "$app"
echo 'Managed Python interpreter verified and made available to the service account.'
getent group elephant-worker >/dev/null || groupadd --system elephant-worker
getent group elephant >/dev/null || groupadd --system elephant
getent passwd elephant >/dev/null || useradd --system --gid elephant-worker --home-dir "$state" --shell /usr/sbin/nologin elephant
# The sourced helper is within the root-owned release checked above. It probes
# only trusted system paths and asks before falling back to a private install.
. "$app/deploy/pi-install.sh"
elephant_prepare_pi "$app" "$allow_pi" system
IFS= read -r selected_pi < "$app/.elephant-pi" || fail 'Could not read the Pi installation selection.'
case "$selected_pi" in
    bundled) selected_pi=$app/node_modules/.bin/pi ;;
    /*) ;;
    *) fail 'The Pi installation selection is invalid.' ;;
esac
[ ! -d "$app/node_modules" ] || chmod -R a+rX "$app/node_modules"

[ -e "$state" ] || install -d -o elephant -g elephant-worker -m 0700 "$state"
[ -e "$config" ] || install -d -o root -g elephant-worker -m 0750 "$config"
[ -e "$stops" ] || install -d -o root -g root -m 0755 "$stops"
[ -e "$config/core.yaml" ] || install -o root -g elephant-worker -m 0640 configs/production.yaml "$config/core.yaml"
[ -e "$config/core.env" ] || install -o root -g root -m 0600 .env.example "$config/core.env"
check_service_config
for path in "$app" "$app/.venv/bin/python" "$app/.venv/bin/elephant" "$selected_pi" "$node/bin/node"; do
    as_service -x "$path" || fail "Service account cannot execute or traverse $path. The daemon unit was not changed."
done
# Alembic uses the deployment working directory, not the installed wheel.
for path in "$app/alembic.ini" "$app/migrations/env.py" "$app"/migrations/versions/*.py; do
    as_service -r "$path" || fail "Service account cannot read release migration files at $path. The daemon unit was not changed."
done
as_service -x "$state" && as_service -w "$state" ||
    fail "Service account cannot use $state. The daemon unit was not changed."

require_inactive
# Finish an interrupted unit publication before replacing its pending intent.
if [ "$managed_unit" = yes ] && [ "$installed_hash" = "$pending_hash" ]; then
    mv -f -- "$pending" "$stamp"
fi
# Publish helper and unit only after the account checks above. Keep the unit and
# digest staging files on their respective destination filesystems.
install -o root -g root -m 0755 scripts/elephant-daemon-stopped "$helper"
new_unit=$(mktemp /etc/systemd/system/.elephant.service.XXXXXX)
install -o root -g root -m 0644 deploy/elephant.service "$new_unit"
new_digest=$(mktemp "$config/.unit-digest.XXXXXX")
unit_hash "$new_unit" > "$new_digest" || fail 'Could not hash the new unit.'
mv -f -- "$new_digest" "$pending"
new_digest=
mv -f -- "$new_unit" "$unit_path"
new_unit=
mv -f -- "$pending" "$stamp"
[ "$provision_jobs" = no ] || env -i PATH="$PATH" HOME=/root deploy/elephant-job-user-setup.sh elephant "$unit"
systemctl daemon-reload
echo 'Installed. The Elephant daemon was not started.'
if [ -d "$unit_path.d" ]; then
    echo 'Review this unit and any existing overrides with: systemctl cat elephant.service'
fi
echo 'Installation complete; open the Elephant setup wizard when ready.'
