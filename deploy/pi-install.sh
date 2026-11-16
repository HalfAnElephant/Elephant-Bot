#!/bin/sh
# Sourced by the installers. Loading this file does not inspect or change the host.

elephant_pi_root_path() (
    # The system installer has already verified this managed interpreter.
    "$2" -I -B - "$1" <<'PYTHON'
import os
import stat
import sys
from pathlib import Path

checked = {}
active = set()


def trusted_path(path):
    if path in checked:
        return checked[path]
    if path in active:
        raise ValueError("cyclic Pi path")
    active.add(path)
    parent = path if path == Path("/") else trusted_path(path.parent) / path.name
    info = parent.lstat()
    if info.st_uid != 0:
        raise ValueError("Pi path is not root-owned")
    if stat.S_ISLNK(info.st_mode):
        target = Path(os.readlink(parent))
        resolved = trusted_path(target if target.is_absolute() else parent.parent / target)
    else:
        if info.st_mode & 0o022:
            raise ValueError("Pi path is writable by another account")
        resolved = Path(os.path.normpath(parent))
    active.remove(path)
    checked[path] = resolved
    return resolved


def walk_error(error):
    raise error


try:
    executable = trusted_path(Path(sys.argv[1]))
    modules = next(parent for parent in reversed(executable.parents) if parent.name == "node_modules")
    # npm can hoist imports beside Pi. Check the complete dependency root before
    # executing anything, including each intermediate link and its ancestors.
    for directory, directories, files in os.walk(modules, onerror=walk_error):
        for name in directories + files:
            target = trusted_path(Path(directory) / name)
            if not target.is_relative_to(modules):
                raise ValueError("Pi dependency link leaves its npm tree")
except (OSError, ValueError, StopIteration, RecursionError) as error:
    print(f"Pi installation is not trusted: {error}", file=sys.stderr)
    sys.exit(1)
PYTHON
)

elephant_pi_version() (
    candidate=$1
    expected=$2
    scope=$3
    case "$candidate" in /*) ;; *) exit 10 ;; esac
    [ -f "$candidate" ] && [ -x "$candidate" ] || exit 10
    node_path=$(command -v node) || exit 11
    case "$node_path" in /*) ;; *) exit 11 ;; esac
    # Match the daemon's Pi RPC process environment, not arbitrary interactive
    # shell directories. The selected native Node is the first runtime entry.
    probe_path=${node_path%/*}:/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin
    if [ "$scope" = system ]; then
        elephant_pi_root_path "$candidate" "$base/.venv/bin/python" || exit 12
        candidate=$(realpath -e -- "$candidate") || exit 12
        runuser -u elephant -g elephant-worker -- /usr/bin/test -x "$candidate" || exit 13
        runuser -u elephant -g elephant-worker -- /usr/bin/test -r "$candidate" || exit 13
        probe_home=$(mktemp -d /run/elephant-pi-probe.XXXXXX) || exit 14
    else
        probe_home=$(mktemp -d) || exit 14
    fi
    trap 'rm -rf -- "$probe_home"' EXIT
    if [ "$scope" = system ]; then
        chown elephant:elephant-worker "$probe_home" || exit 14
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
if (version.error || version.status !== 0) process.exit(21);
if (version.stdout.trim() !== expected) process.exit(22);
const request = { id: "elephant-install-check", type: "get_state" };
const flags = ["--offline", "--no-tools", "--no-extensions", "--no-mcp", "--no-skills",
  "--no-prompt-templates", "--no-context-files", "--no-themes", "--no-approve", "--mode", "rpc"];
const rpc = spawnSync(executable, flags, {
  ...options, input: JSON.stringify(request) + "\n",
});
if (rpc.error || rpc.status !== 0) process.exit(23);
try {
  const response = rpc.stdout.split("\n").filter(Boolean).map(JSON.parse)
    .find(value => value.id === request.id && value.type === "response" &&
      value.command === request.type && value.success === true && value.data &&
      Object.hasOwn(value.data, "isStreaming"));
  process.exit(response ? 0 : 24);
} catch { process.exit(24); }
' "$candidate" "$expected" </dev/null >/dev/null 2>&1

)

elephant_pi_probe_reason() {
    case "$1" in
        10) reason='未找到可执行文件' ;;
        11) reason='未找到 Node.js' ;;
        12) reason='安装路径不满足系统权限要求' ;;
        13) reason='服务账号无法读取或执行 Pi' ;;
        14) reason='无法准备临时检查目录' ;;
        21) reason='离线版本命令未能执行' ;;
        22) reason='版本与锁定版本不一致' ;;
        23) reason='离线 RPC 未启动' ;;
        24) reason='离线 RPC 响应不兼容' ;;
        *) reason='无法以运行账号启动检查进程' ;;
    esac
    printf 'Pi 检查：%s。\n' "$reason" >&2
}

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

# System npm can publish bin-link directories with permissive modes even
# under umask 077. Never run it directly in the daemon-readable install.
elephant_install_private_system_pi() (
    base=$1
    expected=$2
    target=$base/node_modules
    parent=${base%/*}
    umask 077
    stage=$(mktemp -d -- "$parent/.elephant-pi-stage.XXXXXX") || exit 1
    keep_stage=no
    trap '[ "$keep_stage" = yes ] || rm -rf -- "$stage"' EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
    # Reject separate /opt and application filesystems before provisioning.
    [ "$(stat -c %d -- "$stage")" = "$(stat -c %d -- "$base")" ] || {
        printf '%s\n' 'Pi 暂存目录与安装目录不在同一文件系统，未替换运行环境。' >&2
        exit 1
    }

    cp -- "$base/package.json" "$base/package-lock.json" "$stage/" || exit 1
    (
        cd -- "$stage" || exit 1
        env -i PATH="$PATH" HOME=/root npm ci --ignore-scripts --no-audit --no-fund
    ) || exit 1
    new=$stage/node_modules
    [ -d "$new" ] && [ ! -L "$new" ] || exit 1
    foreign=$(find -P "$new" -xdev ! -user root -print -quit) || exit 1
    [ -z "$foreign" ] || {
        printf '%s\n' 'Pi 暂存依赖不是 root 所有，未替换运行环境。' >&2
        exit 1
    }
    chmod -R go-w,a+rX -- "$new" || exit 1

    # Allow traversal (not listing) only after sanitizing the tree, so the
    # normal validation still executes as elephant, not as the installer.
    chmod 0711 -- "$stage" || exit 1
    elephant_pi_version "$new/.bin/pi" "$expected" system || {
        elephant_pi_probe_reason "$?"
        printf '%s\n' '未生成锁定版本的可执行 Pi，安装未完成。' >&2
        exit 1
    }

    [ ! -L "$target" ] && { [ ! -e "$target" ] || [ -d "$target" ]; } || exit 1
    previous=$stage/previous
    if [ -d "$target" ]; then
        # Don't make a retired runtime readable through the staging path.
        mkdir -m 0700 -- "$previous" || exit 1
        keep_stage=yes
        mv -T -- "$target" "$previous/node_modules" || exit 1
    fi
    if ! mv -T -- "$new" "$target"; then
        if [ -d "$previous/node_modules" ]; then
            mv -T -- "$previous/node_modules" "$target" || {
                printf '旧 Pi 目录保存在 %s。\n' "$previous/node_modules" >&2
                exit 1
            }
        fi
        keep_stage=no
        exit 1
    fi
    if ! elephant_pi_write_marker "$base" bundled system; then
        if ! mv -T -- "$target" "$new" ||
            { [ -d "$previous/node_modules" ] && ! mv -T -- "$previous/node_modules" "$target"; }; then
            printf '无法恢复原 Pi 目录，请检查 %s。\n' "$stage" >&2
            exit 1
        fi
        keep_stage=no
        exit 1
    fi
    keep_stage=no
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
                if elephant_pi_version "$candidate" "$expected" "$scope"; then
                    if [ "$scope" = system ]; then
                        candidate=$(realpath -e -- "$candidate") || continue
                    fi
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
        elephant_install_private_system_pi "$base" "$expected"
        exit $?
    fi
    npm ci --ignore-scripts --no-audit --no-fund || exit 1
    elephant_pi_version "$base/node_modules/.bin/pi" "$expected" "$scope" || {
        elephant_pi_probe_reason "$?"
        printf '%s\n' '未生成锁定版本的可执行 Pi，安装未完成。' >&2
        exit 1
    }
    elephant_pi_write_marker "$base" bundled "$scope"
)
