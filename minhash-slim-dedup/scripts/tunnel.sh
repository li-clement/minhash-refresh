#!/usr/bin/env bash
# 开 SSH 隧道：本地 :8501 -> node1 :8501（前台，Ctrl-C 结束）。
#   tunnel.sh          8501
#   tunnel.sh 8503     面板演练用的备用端口
set -euo pipefail
HOST=${RAYTEST_HOST:-node1}
PORT=${1:-8501}
if nc -z 127.0.0.1 "$PORT" 2>/dev/null; then
  echo "本地 $PORT 已有服务在监听（隧道可能已开）：http://127.0.0.1:$PORT/"
  exit 0
fi
echo "隧道已开：http://127.0.0.1:$PORT/  （Ctrl-C 结束）"
exec ssh -N -L "$PORT:127.0.0.1:$PORT" "$HOST"
