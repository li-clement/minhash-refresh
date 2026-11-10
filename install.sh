#!/usr/bin/env bash
# 把本仓库里的 skill 安装到 ~/.agents/skills/（默认复制；--link 用软链接便于 git pull 生效）。
#
#   ./install.sh                复制到 ${AGENTS_SKILLS:-~/.agents/skills}/minhash-slim-dedup
#   ./install.sh --link         建软链接
#   ./install.sh --force        目标已存在时先删掉再装
set -euo pipefail
SRC=$(cd "$(dirname "$0")" && pwd)/minhash-slim-dedup
DEST_ROOT=${AGENTS_SKILLS:-$HOME/.agents/skills}
DEST=$DEST_ROOT/minhash-slim-dedup
MODE=copy
FORCE=0
for a in "$@"; do
  case "$a" in
    --link) MODE=link ;;
    --force) FORCE=1 ;;
    -h|--help) sed -n '2,7p' "$0"; exit 0 ;;
    *) echo "未知参数: $a" >&2; exit 2 ;;
  esac
done
[ -f "$SRC/SKILL.md" ] || { echo "✗ 找不到 $SRC/SKILL.md" >&2; exit 1; }
mkdir -p "$DEST_ROOT"
if [ -e "$DEST" ] || [ -L "$DEST" ]; then
  if [ "$FORCE" -eq 1 ]; then rm -rf "$DEST"; else
    echo "✗ $DEST 已存在；加 --force 覆盖，或先手动移走" >&2; exit 1
  fi
fi
if [ "$MODE" = link ]; then
  ln -s "$SRC" "$DEST"
  echo "已软链: $DEST -> $SRC"
else
  cp -R "$SRC" "$DEST"
  echo "已复制: $DEST"
fi
for f in "$DEST"/scripts/*.sh; do bash -n "$f"; done && echo "脚本语法检查通过"
