#!/bin/sh
# Distribution-neutral local runtime. No system packages, root or service changes.
set -eu
fail() { printf '%s\n' "$*" >&2; exit 1; }
mode=
allow_pi=no
for option do
    case "$option" in
        --dry-run|--apply) [ -z "$mode" ] || fail '安装选项无效。'; mode=$option ;;
        --install-pi) [ "$allow_pi" = no ] || fail '安装选项无效。'; allow_pi=yes ;;
        *) fail '安装选项无效。' ;;
    esac
done
mode=${mode:---dry-run}
[ "$allow_pi" = no ] || [ "$mode" = --apply ] || fail '--install-pi 需要搭配 --apply。'
printf '%s\n' '个人安装：准备锁定的 Python 和 Pi 运行环境，不修改系统服务或已有配置。'
[ "$mode" = --apply ] || exit 0
[ "$(id -u)" -ne 0 ] || fail '个人安装请使用普通用户，不要使用管理员权限。'
[ "$(uname -s)" = Linux ] || fail '此安装方式仅适用于 Linux。'
case "$(uname -m)" in
    x86_64|aarch64) ;;
    *) fail '此架构尚未纳入个人安装支持范围。' ;;
esac
getconf GNU_LIBC_VERSION >/dev/null 2>&1 ||
    fail '通用个人安装目前需要 glibc。基于 musl 的系统尚未验收。'
base=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd -P)
cd -- "$base"
[ -f .python-version ] && [ -f uv.lock ] && [ -f package-lock.json ] ||
    fail '安装文件不完整，请使用已校验的完整发布包。'
umask 077
lock=.elephant-install.lock
mkdir "$lock" 2>/dev/null || fail '此目录中已有安装未结束。'
trap 'rmdir "$lock"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
# A previous version of the daemon need not hold any installer lock. Never
# rewrite a bundle that may already supply a foreground or login daemon.
if [ -e .venv ] || [ -L .venv ]; then
    fail '此目录已有 Python 运行环境，不会就地覆盖。请先停止旧实例（含登录启动项），再在新目录安装已校验的发布包。'
fi
[ ! -L node_modules ] && { [ ! -e node_modules ] || [ -d node_modules ]; } ||
    fail '发现非目录的安装目标，未修改现有文件。'
for tool in uv node; do
    command -v "$tool" >/dev/null 2>&1 ||
        fail "缺少 ${tool}，请按部署文档补齐系统依赖后重试。"
done
node -e 'const [m,n] = process.versions.node.split(".").map(Number); process.exit(m > 22 || (m === 22 && n >= 19) ? 0 : 1)' ||
    fail 'Node.js 版本过低，需要 22.19 或更新版本。'
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
unset VIRTUAL_ENV CONDA_PREFIX UV_PROJECT_ENVIRONMENT UV_WORKING_DIRECTORY UV_PROJECT
# shellcheck source=deploy/pi-install.sh
. "$base/deploy/pi-install.sh"
elephant_prepare_pi "$base" "$allow_pi" personal
printf '%s\n' '正在安装 Python 运行环境…'
uv sync --locked --no-dev --no-editable --python "$(cat .python-version)"
[ -f .venv/bin/elephant ] && [ -x .venv/bin/elephant ] ||
    fail '安装未完成，未找到完整运行环境。'
printf '%s\n' '安装完成。已保留现有配置，可打开 Elephant 向导。'
