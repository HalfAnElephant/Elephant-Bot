#!/bin/sh
# Install only from a trusted, persistent source checkout or verified release.
set -eu

usage() {
    printf '%s\n' '用法：deploy/install-macos.sh [--dry-run | --apply [--with-deps]]' \
        '默认仅预览；--apply 在当前源码目录安装锁定依赖。' \
        '无需 sudo，不会安装 Homebrew、修改配置或启动服务。' \
        '加 --with-deps 可使用已安装的 Homebrew 补齐工具；--install-pi 明确批准私有 Pi。'
}

fail() { printf 'Elephant：%s\n' "$*" >&2; exit 1; }

mode=dry
with_deps=no
allow_pi=no
for option in "$@"; do
    case "$option" in
        --help|-h) usage; exit 0 ;;
        --dry-run|--apply)
            [ "$mode" = dry ] || { usage >&2; exit 2; }
            mode=$option ;;
        --install-pi)
            [ "$allow_pi" = no ] || { usage >&2; exit 2; }
            allow_pi=yes ;;
        --with-deps)
            [ "$with_deps" = no ] || { usage >&2; exit 2; }
            with_deps=yes ;;
        *) usage >&2; exit 2 ;;
    esac
done
[ "$with_deps:$allow_pi" = no:no ] || [ "$mode" = --apply ] || { usage >&2; exit 2; }

printf '%s\n' 'Elephant · macOS 本地安装' \
    '  1. 检查原生终端、uv、Node.js 和 npm。' \
    '  2. 在此源码目录安装锁定的 Python 与 Node.js 依赖。' \
    '  3. 手动运行 ./elephant，按需前台启动。' \
    '  保留已有配置；不写入系统目录，不安装后台服务。'
[ "$mode" = --apply ] || exit 0

[ "$(uname -s)" = Darwin ] || fail '此安装入口只用于 macOS。'
[ "$(id -u)" != 0 ] || fail '请退出 sudo，以普通用户运行。'

arch=$(uname -m)
case "$arch" in
    arm64) node_arch=arm64; brew_bin=/opt/homebrew/bin ;;
    x86_64)
        translated=$(sysctl -n sysctl.proc_translated 2>/dev/null || true)
        [ "$translated" != 1 ] || fail '请关闭终端的「使用 Rosetta 打开」，改用原生 Apple Silicon 终端。'
        node_arch=x64; brew_bin=/usr/local/bin ;;
    *) fail "不支持此 macOS 架构：$arch。" ;;
esac

root=$(CDPATH='' cd -P -- "$(dirname -- "$0")/.." && pwd -P)
cd -- "$root"
for path in .python-version pyproject.toml uv.lock package.json package-lock.json; do
    [ -f "$path" ] && [ ! -L "$path" ] || fail "缺少可信发布文件：$path。"
done
for path in .venv node_modules; do
    if [ -L "$path" ] || { [ -e "$path" ] && [ ! -d "$path" ]; }; then
        fail "拒绝写入非目录或符号链接：$path。"
    fi
done

# The version check follows package.json and rejects an architecture-mismatched
# Node runtime. npm is checked only if the operator chooses a private Pi.
valid_node() {
    node -e '
const minimum = require("./package.json").engines.node.match(/^>=(\d+)\.(\d+)\.(\d+)$/);
const current = process.versions.node.split(".").map(Number);
if (!minimum || process.platform !== "darwin" || process.arch !== process.argv[1]) process.exit(1);
const expected = minimum.slice(1).map(Number);
const difference = current.findIndex((value, index) => value !== expected[index]);
if (difference !== -1 && current[difference] < expected[difference]) process.exit(1);
' "$node_arch" >/dev/null 2>&1
}

node_path_help() {
    fail "当前 PATH 上的 Node.js/npm 不完整、版本过低或架构不匹配。请将兼容工具所在目录（如 $brew_bin）放在 PATH 前面，再重试。此安装不会自动替换已有工具。"
}

# Prefer the caller's already-installed tools. Expose the standard Homebrew
# prefix only as a fallback, without shadowing intentionally selected binaries.
if [ -x "$brew_bin/brew" ]; then
    PATH="$PATH:$brew_bin"
    export PATH
fi
node_present=no
if command -v node >/dev/null 2>&1; then
    node_present=yes
    valid_node || node_path_help
elif command -v npm >/dev/null 2>&1; then
    # Do not let Homebrew replace an apparently partial existing installation.
    node_path_help
fi

if [ "$arch" = x86_64 ]; then
    # The locked pilk-nogil release ships a Darwin arm64 but no Intel wheel.
    # Toolchain detection is read-only; do not invoke xcode-select --install.
    if ! xcode-select -p >/dev/null 2>&1 || ! xcrun --find clang >/dev/null 2>&1; then
        fail 'Intel Mac 需要从源码构建音频依赖。请先手动安装 Apple Command Line Tools（xcode-select --install）。'
    fi
    printf '%s\n' '  Intel Mac：音频依赖将尝试从锁定源码构建。'
fi

if [ "$with_deps" = yes ]; then
    brew="$brew_bin/brew"
    [ -x "$brew" ] && [ "$("$brew" --prefix)" = "${brew_bin%/bin}" ] ||
        fail '未检测到标准原生 Homebrew。请先从 brew.sh 安装，或手动准备工具后去掉 --with-deps。'
    if [ "$node_present" = no ]; then
        HOMEBREW_NO_AUTO_UPDATE=1 "$brew" install node
    fi
fi
command -v node >/dev/null 2>&1 || fail '缺少原生 Node.js。请安装官方 Node.js 后重试。'
[ "$node_present" = yes ] || valid_node || node_path_help
if [ "$with_deps" = no ]; then
    command -v uv >/dev/null 2>&1 || fail '缺少 uv。请安装官方 uv 后重试。'
fi

IFS= read -r python_version < .python-version || fail '无法读取锁定的 Python 版本。'
case "$python_version" in
    3.13.[0-9]|3.13.[0-9][0-9]) ;;
    *) fail '不支持 .python-version 中的版本；预期为 3.13.x。' ;;
esac

# Keep installation scoped to this source tree, not a shell's activated venv.
unset VIRTUAL_ENV CONDA_PREFIX UV_PROJECT_ENVIRONMENT UV_WORKING_DIRECTORY UV_PROJECT
umask 077
lock=.elephant-install.lock
mkdir "$lock" 2>/dev/null || fail '本目录已有安装在进行；若上次异常退出，请确认无安装进程后删除 .elephant-install.lock。'
trap 'rmdir "$lock"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# shellcheck source=deploy/pi-install.sh
. "$root/deploy/pi-install.sh"
elephant_prepare_pi "$root" "$allow_pi" personal
if [ "$with_deps" = yes ]; then
    set --
    command -v uv >/dev/null 2>&1 || set -- "$@" uv
    command -v ffmpeg >/dev/null 2>&1 || set -- "$@" ffmpeg
    if [ "$#" -gt 0 ]; then
        HOMEBREW_NO_AUTO_UPDATE=1 "$brew" install "$@"
    fi
fi
command -v uv >/dev/null 2>&1 || fail '缺少 uv。请安装官方 uv 后重试。'
if ! command -v ffmpeg >/dev/null 2>&1; then
    printf '%s\n' '  提示：未检测到 FFmpeg，语音功能需要 FFmpeg 6 或更新版本。'
elif ffmpeg_output=$(ffmpeg -version 2>/dev/null); then
    ffmpeg_major=$(printf '%s\n' "$ffmpeg_output" |
        awk '/^ffmpeg version / { v=$3; sub(/^n/,"",v); split(v,a,"."); if (a[1] ~ /^[0-9]+$/) print a[1]; exit }')
    if [ -z "$ffmpeg_major" ]; then
        printf '%s\n' '  提示：无法确认 FFmpeg 版本；请使用 FFmpeg 6 或更新版本以支持语音。'
    elif [ "$ffmpeg_major" -lt 6 ]; then
        printf '%s\n' '  提示：当前 FFmpeg 版本低于 6；语音功能需要更新或调整 PATH。'
    fi
else
    printf '%s\n' '  提示：FFmpeg 无法运行；请检查 PATH 和原生工具安装，语音暂不可用。'
fi

uv sync --locked --no-dev --no-editable --python "$python_version"
[ -f .venv/bin/elephant ] && [ -x .venv/bin/elephant ] || fail '依赖安装未生成 Elephant 命令。'

printf '%s\n' '  安装完成。请在本目录运行：' \
    '    ./elephant'
