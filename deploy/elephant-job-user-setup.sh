#!/bin/sh
# Administrative provisioning only; the daemon never receives sudo/systemd-run privileges.
set -eu

if [ "$(id -u)" -ne 0 ] || [ "$#" -ne 1 ]; then
    echo "Usage as root: $0 EXISTING_SERVICE_ACCOUNT" >&2
    exit 2
fi
account=$1
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
mkdir -p /etc/qqbot
install -m 0644 /dev/null /etc/qqbot/jobs.env
printf 'XDG_RUNTIME_DIR=/run/user/%s\nDBUS_SESSION_BUS_ADDRESS=%s\n' "$service_uid" "$bus_address" > /etc/qqbot/jobs.env
mkdir -p /etc/systemd/system/elephant-bot.service.d
printf '[Unit]\nRequires=user@%s.service\nAfter=user@%s.service\n[Service]\nBindReadOnlyPaths=/run/user/%s\n' \
    "$service_uid" "$service_uid" "$service_uid" > /etc/systemd/system/elephant-bot.service.d/jobs.conf
systemctl daemon-reload
echo "User manager provisioned for $account. Run the documented isolated Job acceptance before enabling tools."
