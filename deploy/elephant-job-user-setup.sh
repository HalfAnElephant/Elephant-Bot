#!/bin/sh
# Administrative provisioning only; the daemon never receives sudo/systemd-run privileges.
set -eu

if [ "$(id -u)" -ne 0 ] || [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
    echo "Usage as root: $0 EXISTING_SERVICE_ACCOUNT [SERVICE_UNIT]" >&2
    exit 2
fi
account=$1
service_unit=${2-elephant.service}
case "$service_unit" in
    *[!a-zA-Z0-9_.@-]*|.service)
        echo "SERVICE_UNIT must be a service name without a path." >&2
        exit 2 ;;
    *.service) ;;
    *) echo "SERVICE_UNIT must end with .service." >&2; exit 2 ;;
esac
service_uid=$(id -u -- "$account")
if [ "$service_uid" -eq 0 ]; then
    echo "Jobs must run under an ordinary service account." >&2
    exit 2
fi
for dependency in loginctl systemctl systemd-run journalctl bwrap runuser; do
    command -v "$dependency" >/dev/null || {
        echo "Missing dependency: $dependency" >&2
        exit 1
    }
done
config=/etc/elephant
dropin=/etc/systemd/system/$service_unit.d
bus_address="unix:path=/run/user/$service_uid/bus"
umask 077
for directory in "$config" "$dropin"; do
    [ ! -L "$directory" ] || { echo "Refusing linked directory: $directory" >&2; exit 1; }
    if [ -e "$directory" ]; then
        [ -d "$directory" ] && [ "$(stat -c %u -- "$directory")" = 0 ] &&
            [ -z "$(find "$directory" -maxdepth 0 -perm /022 -print)" ] ||
            { echo "Directory is not trusted: $directory" >&2; exit 1; }
    fi
done
validate_existing() {
    expected=$1
    target=$2
    [ ! -L "$target" ] || { echo "Refusing linked file: $target" >&2; exit 1; }
    if [ -e "$target" ]; then
        [ -f "$target" ] && [ "$(stat -c %u -- "$target")" = 0 ] &&
            [ -z "$(find "$target" -maxdepth 0 -perm /022 -print)" ] &&
            cmp -s "$expected" "$target" ||
            { echo "Existing Job file differs from generated defaults. Review manually: $target" >&2; exit 1; }
    fi
}
env_tmp=
dropin_tmp=
trap '[ -z "$env_tmp" ] || rm -f -- "$env_tmp"; [ -z "$dropin_tmp" ] || rm -f -- "$dropin_tmp"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
[ -d "$config" ] || install -d -o root -g "$(id -gn -- "$account")" -m 0750 "$config"
[ -d "$dropin" ] || install -d -o root -g root -m 0755 "$dropin"
env_tmp=$(mktemp "$config/.jobs-env.XXXXXX")
dropin_tmp=$(mktemp "$dropin/.jobs-conf.XXXXXX")
printf 'XDG_RUNTIME_DIR=/run/user/%s\nDBUS_SESSION_BUS_ADDRESS=%s\n' "$service_uid" "$bus_address" > "$env_tmp"
printf '[Unit]\nRequires=user@%s.service\nAfter=user@%s.service\n[Service]\nBindReadOnlyPaths=/run/user/%s\n' \
    "$service_uid" "$service_uid" "$service_uid" > "$dropin_tmp"
chmod 0644 "$env_tmp" "$dropin_tmp"
validate_existing "$env_tmp" "$config/jobs.env"
validate_existing "$dropin_tmp" "$dropin/jobs.conf"

loginctl enable-linger -- "$account"
systemctl start "user@$service_uid.service"
manager_state=$(runuser -u "$account" -- env \
    "XDG_RUNTIME_DIR=/run/user/$service_uid" "DBUS_SESSION_BUS_ADDRESS=$bus_address" \
    systemctl --user is-system-running) || true
case "$manager_state" in
    running|degraded) ;;
    *) echo "The service account's systemd user manager is unavailable." >&2; exit 1 ;;
esac
# Hard-link each fully written file into place without replacing a concurrent or
# operator-owned destination. Exact generated survivors are safe on rerun.
if [ ! -e "$config/jobs.env" ]; then
    ln -- "$env_tmp" "$config/jobs.env"
fi
if [ ! -e "$dropin/jobs.conf" ]; then
    ln -- "$dropin_tmp" "$dropin/jobs.conf"
fi
systemctl daemon-reload
echo "User manager provisioned for $account and $service_unit. Run the documented isolated Job acceptance before enabling tools."
