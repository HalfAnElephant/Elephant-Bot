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
loginctl enable-linger -- "$account"
systemctl start "user@$service_uid.service"
bus_address="unix:path=/run/user/$service_uid/bus"
manager_state=$(runuser -u "$account" -- env \
    "XDG_RUNTIME_DIR=/run/user/$service_uid" "DBUS_SESSION_BUS_ADDRESS=$bus_address" \
    systemctl --user is-system-running) || true
case "$manager_state" in
    running|degraded) ;;
    *) echo "The service account's systemd user manager is unavailable." >&2; exit 1 ;;
esac
mkdir -p /etc/elephant
install -m 0644 /dev/null /etc/elephant/jobs.env
printf 'XDG_RUNTIME_DIR=/run/user/%s\nDBUS_SESSION_BUS_ADDRESS=%s\n' "$service_uid" "$bus_address" > /etc/elephant/jobs.env
mkdir -p "/etc/systemd/system/$service_unit.d"
printf '[Unit]\nRequires=user@%s.service\nAfter=user@%s.service\n[Service]\nBindReadOnlyPaths=/run/user/%s\n' \
    "$service_uid" "$service_uid" "$service_uid" > "/etc/systemd/system/$service_unit.d/jobs.conf"
systemctl daemon-reload
echo "User manager provisioned for $account and $service_unit. Run the documented isolated Job acceptance before enabling tools."
