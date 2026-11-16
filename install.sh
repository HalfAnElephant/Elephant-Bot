#!/bin/sh
# Common entry point for a verified Elephant source bundle or checkout.
set -eu

usage() {
    cat <<'EOF'
Elephant 安装
  --dry-run    只查看安装计划（默认，不改动系统）
  --apply      安装此目录内的运行环境
  --setup      安装完成后打开配置向导（与 --apply 一起使用）
  --system     在受支持的 Linux 上安装系统服务（需管理员权限）
  --with-deps  在 macOS 上用现有的 Homebrew 补齐所需依赖
  --install-pi 在找不到兼容 Pi 时，允许安装 Elephant 专用副本
EOF
}

mode=
system=no
setup=no
with_deps=no
allow_pi=no
fail() { printf '%s\n' "$*" >&2; exit 1; }
for flag do
    case "$flag" in
        --apply|--dry-run)
            [ -z "$mode" ] || fail '请选择一种安装方式。'
            mode=$flag ;;
        --system) system=yes ;;
        --setup) setup=yes ;;
        --with-deps) with_deps=yes ;;
        --install-pi) allow_pi=yes ;;
        --help|-h) usage; exit 0 ;;
        *) fail "未知安装选项：$flag（可查看帮助）" ;;
    esac
done
mode=${mode:---dry-run}
if [ "$mode" != --apply ] && { [ "$setup" = yes ] || [ "$with_deps" = yes ] || [ "$allow_pi" = yes ]; }; then
    fail '启动向导或安装依赖需要同时选择正式安装。'
fi

platform=$(uname -s)
base=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd -P)
set -- "$mode"
if [ "$allow_pi" = yes ]; then set -- "$@" --install-pi; fi
case "$platform:$system:$with_deps" in
    Linux:yes:no)
        if [ "$mode" = --apply ] && [ "$base" != /opt/elephant ]; then
            fail 'Linux 系统安装要求先把已校验的发布包放到受保护的系统目录，详见部署文档。'
        fi
        "$base/deploy/install-system.sh" "$@"
        if [ "$setup" = yes ]; then
            cd -- "$base"
            set --
            if [ "${NO_COLOR+x}" = x ]; then set -- NO_COLOR=; fi
            exec /usr/bin/env -i "TERM=${TERM:-dumb}" LANG=C.UTF-8 HOME=/root \
                PATH=/opt/elephant-node/bin:/usr/bin:/bin "$@" \
                "$base/.venv/bin/python" -I -c 'from elephant.cli import main; raise SystemExit(main())' setup --system
        fi
        ;;
    Linux:no:no)
        "$base/deploy/install-local-linux.sh" "$@"
        ;;
    Darwin:no:*)
        if [ "$with_deps" = yes ]; then set -- "$@" --with-deps; fi
        "$base/deploy/install-macos.sh" "$@"
        ;;
    *)
        fail '此平台不支持所选安装方式。macOS 请使用个人安装；Linux 依赖可参考部署文档。'
        ;;
esac
if [ "$setup" = yes ]; then
    exec "$base/elephant" setup
fi
