#!/bin/sh
# Sourced by the installers. Loading this file does not inspect or change the host.

elephant_pi_root_path() (
    path=$1
    # The system installer runs only on Linux and uses GNU realpath/stat.
    # Verify both the executable's PATH entry and its resolved target.
    resolved=$(realpath -e -- "$path") || exit 1
    for item in "$path" "$resolved"; do
        while [ "$item" != / ]; do
            [ "$(stat -c '%u' -- "$item" 2>/dev/null)" = 0 ] || exit 1
            if [ ! -L "$item" ]; then
                [ -z "$(find "$item" -maxdepth 0 -perm /022 -print)" ] || exit 1
            fi
            item=${item%/*}
            [ -n "$item" ] || item=/
        done
    done
)

elephant_pi_version() (
    candidate=$1
    expected=$2
    scope=$3
    case "$candidate" in /*) ;; *) exit 1 ;; esac
    [ -f "$candidate" ] && [ -x "$candidate" ] || exit 1
    node_path=$(command -v node) || exit 1
    case "$node_path" in /*) ;; *) exit 1 ;; esac
    # Match the daemon's Pi RPC process environment, not arbitrary interactive
    # shell directories. The selected native Node is the first runtime entry.
    probe_path=${node_path%/*}:/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin
    if [ "$scope" = system ]; then
        elephant_pi_root_path "$candidate" || exit 1
        candidate=$(realpath -e -- "$candidate") || exit 1
        runuser -u elephant -g elephant-worker -- /usr/bin/test -x "$candidate" || exit 1
        probe_home=$(mktemp -d /run/elephant-pi-probe.XXXXXX) || exit 1
    else
        probe_home=$(mktemp -d) || exit 1
    fi
    trap 'rm -rf -- "$probe_home"' EXIT
    if [ "$scope" = system ]; then
        chown elephant:elephant-worker "$probe_home" || exit 1
        set -- runuser -u elephant -g elephant-worker --
    else
        set --
    fi
    # Read only an offline version and synthetic RPC state; no model calls,
    # extension loading, configured profile, or credentials are needed.
    "$@" env -i PATH="$probe_path" HOME="$probe_home" XDG_CONFIG_HOME="$probe_home" \
        XDG_DATA_HOME="$probe_home" XDG_CACHE_HOME="$probe_home" PI_CODING_AGENT_DIR="$probe_home" \
        PI_OFFLINE=1 PI_TELEMETRY=0 CI=1 NO_COLOR=1 "$node_path" -e '
const { spawnSync } = require("node:child_process");
const [executable, expected] = process.argv.slice(1);
const options = { timeout: 5000, maxBuffer: 32768, killSignal: "SIGKILL",
  stdio: ["pipe", "pipe", "ignore"], cwd: process.env.HOME, encoding: "utf-8" };
const version = spawnSync(executable, ["--offline", "--version"], options);
if (version.error || version.status !== 0 || version.stdout.trim() !== expected) process.exit(1);
const request = { id: "elephant-install-check", type: "get_state" };
const flags = ["--offline", "--no-tools", "--no-extensions", "--no-mcp", "--no-skills",
  "--no-prompt-templates", "--no-context-files", "--no-themes", "--no-approve", "--mode", "rpc"];
const rpc = spawnSync(executable, flags, {
  ...options, input: JSON.stringify(request) + "\n",
});
if (rpc.error || rpc.status !== 0) process.exit(1);
try {
  const response = rpc.stdout.split("\n").filter(Boolean).map(JSON.parse)
    .find(value => value.id === request.id && value.type === "response" &&
      value.command === request.type && value.success === true && value.data &&
      Object.hasOwn(value.data, "isStreaming"));
  process.exit(response ? 0 : 1);
} catch { process.exit(1); }
' "$candidate" "$expected" </dev/null >/dev/null 2>&1

)

elephant_pi_write_marker() (
    path=$1/.elephant-pi
    choice=$2
    scope=$3
    [ ! -L "$path" ] && { [ ! -e "$path" ] || [ -f "$path" ]; } || {
        printf '%s\n' 'Pi 配置不是普通文件，未修改。' >&2
        exit 1
    }
    temp=$(mktemp "$1/.elephant-pi.XXXXXX") || exit 1
    trap 'rm -f -- "$temp"' EXIT
    printf '%s\n' "$choice" >"$temp" || exit 1
    if [ "$scope" = system ]; then chmod 0644 "$temp" || exit 1; fi
    mv -f -- "$temp" "$path"
)

elephant_prepare_pi() (
    base=$1
    allow_pi=$2
    scope=$3
    case "$allow_pi:$scope" in yes:personal|no:personal|yes:system|no:system) ;; *) exit 2 ;; esac
    cd -- "$base" || exit 1
    marker=$base/.elephant-pi
    [ ! -L "$marker" ] && { [ ! -e "$marker" ] || [ -f "$marker" ]; } || {
        printf '%s\n' 'Pi 配置不是普通文件，未修改。' >&2
        exit 1
    }
    expected=$(env -i PATH="$PATH" node -p 'require("./package.json").dependencies["@earendil-works/pi-coding-agent"]') || exit 1
    if ! printf '%s\n' "$expected" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$'; then
        printf '%s\n' '锁定的 Pi 版本无效。' >&2
        exit 1
    fi
    # Prefer the active PATH choice. For a root install also check the usual
    # local directory without exporting it as a path for privileged commands.
    system_pi=$(command -v pi 2>/dev/null || true)
    extra_pi=
    [ "$scope" != system ] || extra_pi=/usr/local/bin/pi
    for candidate in "$system_pi" "$extra_pi"; do
        case "$candidate" in
            "$base/node_modules/"*) ;;
            /*)
                [ -x "$candidate" ] || continue
                if [ "$scope" = system ]; then
                    elephant_pi_root_path "$candidate" || continue
                    candidate=$(realpath -e -- "$candidate") || continue
                fi
                if elephant_pi_version "$candidate" "$expected" "$scope"; then
                    printf '使用系统已有 Pi %s。\n' "$expected"
                    elephant_pi_write_marker "$base" "$candidate" "$scope"
                    exit $?
                fi
                printf '系统 Pi 无法验证为兼容的 %s。\n' "$expected"
                ;;
        esac
    done
    if elephant_pi_version "$base/node_modules/.bin/pi" "$expected" "$scope"; then
        printf '复用 Elephant 私有的 Pi %s。\n' "$expected"
        elephant_pi_write_marker "$base" bundled "$scope"
        exit $?
    fi
    if [ "$allow_pi" != yes ]; then
        if [ -t 0 ] && [ -t 1 ]; then
            printf '没有兼容的 Pi，是否仅为 Elephant 安装锁定版本 %s？[y/N] ' "$expected"
            if IFS= read -r answer; then
                case "$answer" in y|Y|yes|YES|Yes) allow_pi=yes ;; esac
            fi
        fi
        if [ "$allow_pi" != yes ]; then
            printf '%s\n' '未安装 Pi，也未变更已有 Pi 配置。可先安装兼容 Pi，或用 --install-pi 明确批准私有安装。' >&2
            exit 1
        fi
    fi
    if ! command -v npm >/dev/null 2>&1 || ! env -i PATH="$PATH" npm --version >/dev/null 2>&1; then
        printf '%s\n' '私有安装 Pi 需要可用的 Node.js/npm；请修正 PATH 后重试。' >&2
        exit 1
    fi
    printf '正在安装 Elephant 私有的 Pi %s…\n' "$expected"
    if [ "$scope" = system ]; then
        env -i PATH="$PATH" HOME=/root npm ci --ignore-scripts --no-audit --no-fund || exit 1
        chmod -R a+rX "$base/node_modules" || exit 1
    else
        npm ci --ignore-scripts --no-audit --no-fund || exit 1
    fi
    elephant_pi_version "$base/node_modules/.bin/pi" "$expected" "$scope" || {
        printf '%s\n' '未生成锁定版本的可执行 Pi，安装未完成。' >&2
        exit 1
    }
    elephant_pi_write_marker "$base" bundled "$scope"
)
