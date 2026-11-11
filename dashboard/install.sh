#!/usr/bin/env bash
# =============================================================================
# raytest 任务监控面板 —— 幂等安装 / 升级脚本
#
#   1) 远端 venv 里装 fastapi / uvicorn / psutil（清华镜像）
#   2) rsync dashboard/ → node1:~/code/raytest/dashboard/
#   3) 渲染 dashboard.service（替换 @PROJECT_DIR@）→ ~/.config/systemd/user/
#   4) systemctl --user enable --now raytest-dashboard.service（再 restart 让新代码生效）
#   5) 健康检查 curl 127.0.0.1:8501/api/health
#
# 反复执行是安全的：不碰 Ray 进程、不碰 6379/8265、不写实验目录。
#
# 用法:
#   ./install.sh                 # 完整安装/升级
#   ./install.sh --skip-deps     # 跳过 pip（离线/已装好）
#   ./install.sh --dry-run       # 只打印将要做什么
#   RAYTEST_REMOTE=node2 ./install.sh
# =============================================================================
set -euo pipefail

LOCAL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REMOTE="${RAYTEST_REMOTE:-node1}"
REMOTE_REL="${RAYTEST_REMOTE_DIR:-code/raytest}"          # 相对 $HOME
PIP_INDEX="${RAYTEST_PIP_INDEX:-https://pypi.tuna.tsinghua.edu.cn/simple}"
UNIT_NAME="raytest-dashboard.service"
PORT="${RAYTEST_PORT:-8501}"

SKIP_DEPS=0
SKIP_RESTART=0
DRY_RUN=0

while [ $# -gt 0 ]; do
  case "$1" in
    --skip-deps)    SKIP_DEPS=1 ;;
    --no-restart)   SKIP_RESTART=1 ;;
    --dry-run)      DRY_RUN=1 ;;
    -h|--help)      sed -n '2,22p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "未知参数: $1（-h 看用法）" >&2; exit 2 ;;
  esac
  shift
done

if [ -t 1 ]; then
  C_CYAN=$'\033[36m'; C_GREEN=$'\033[32m'; C_YELLOW=$'\033[33m'; C_RED=$'\033[31m'; C_OFF=$'\033[0m'
else
  C_CYAN=""; C_GREEN=""; C_YELLOW=""; C_RED=""; C_OFF=""
fi
step() { printf '%s==>%s %s\n' "$C_CYAN" "$C_OFF" "$*"; }
ok()   { printf '%s  ok%s %s\n' "$C_GREEN" "$C_OFF" "$*"; }
warn() { printf '%s  !!%s %s\n' "$C_YELLOW" "$C_OFF" "$*"; }
die()  { printf '%s  xx%s %s\n' "$C_RED" "$C_OFF" "$*" >&2; exit 1; }

remote() { ssh -o ConnectTimeout=15 "$REMOTE" "$@"; }
run() { if [ "$DRY_RUN" = 1 ]; then printf '     [dry-run] %s\n' "$*"; else "$@"; fi; }

TMPFILES=()
cleanup() { [ ${#TMPFILES[@]} -eq 0 ] || rm -f "${TMPFILES[@]}"; }
trap cleanup EXIT

# ---------------------------------------------------------------------------
# 0. 前置检查
# ---------------------------------------------------------------------------
step "前置检查（本机 rsync / ssh ${REMOTE}）"
command -v rsync >/dev/null 2>&1 || die "本机缺少 rsync"
[ -f "$LOCAL_DIR/app.py" ] || die "找不到 $LOCAL_DIR/app.py —— 请在 dashboard/ 目录下运行"
[ -f "$LOCAL_DIR/dashboard.service" ] || die "找不到 dashboard.service 模板"

remote true >/dev/null 2>&1 || die "无法 ssh 到 $REMOTE"
REMOTE_HOME="$(remote 'echo $HOME')"
[ -n "$REMOTE_HOME" ] || die "无法取得 $REMOTE 的 \$HOME"
REMOTE_PROJECT="$REMOTE_HOME/$REMOTE_REL"
PY="$REMOTE_PROJECT/.venv/bin/python"
UNIT_PATH="$REMOTE_HOME/.config/systemd/user/$UNIT_NAME"
ok "远端 $REMOTE:$REMOTE_PROJECT"

remote "test -x '$PY'" >/dev/null 2>&1 || die "远端 venv 不存在：$PY"

# 端口占用（首次安装时 8501 应是空的；升级时是被自己占用）
if remote "ss -ltn 2>/dev/null | grep -q ':$PORT '"; then
  if remote "systemctl --user is-active --quiet '$UNIT_NAME'" >/dev/null 2>&1; then
    ok "端口 $PORT 由 $UNIT_NAME 自己占用（升级场景）"
  else
    warn "端口 $PORT 已被其它进程占用，安装可能失败："
    remote "ss -ltnp 2>/dev/null | grep ':$PORT ' || true"
  fi
else
  ok "端口 $PORT 空闲"
fi

# linger（没有它，用户级服务在 SSH 断开/重启后不会常驻）
LINGER="$(remote 'loginctl show-user "$USER" -p Linger --value 2>/dev/null || echo unknown')"
if [ "$LINGER" = "yes" ]; then
  ok "linger 已开启（用户服务可开机自启）"
else
  warn "linger=${LINGER}，尝试开启 loginctl enable-linger"
  run remote "loginctl enable-linger \"\$USER\"" || warn "开启 linger 失败（需要 root 或 polkit）"
fi

# ---------------------------------------------------------------------------
# 1. 依赖
# ---------------------------------------------------------------------------
step "安装 Python 依赖（fastapi / uvicorn / psutil，镜像 ${PIP_INDEX}）"
if [ "$SKIP_DEPS" = 1 ]; then
  warn "--skip-deps：跳过 pip"
else
  run remote "'$PY' -m pip install --disable-pip-version-check -q -i '$PIP_INDEX' --upgrade fastapi uvicorn psutil" \
    || die "pip 安装失败（检查 $REMOTE 到清华镜像的网络）"
  if [ "$DRY_RUN" = 1 ]; then
    ok "dry-run：跳过依赖版本探测"
  else
    VER="$(remote "'$PY' -c 'import fastapi, uvicorn, psutil; print(\"fastapi\", fastapi.__version__, \"uvicorn\", uvicorn.__version__, \"psutil\", psutil.__version__)'")"
    ok "依赖就绪：$VER"
  fi
fi

# ---------------------------------------------------------------------------
# 2. rsync
# ---------------------------------------------------------------------------
step "同步 dashboard/ → $REMOTE:$REMOTE_PROJECT/dashboard/"
RSYNC=(rsync -az --delete --itemize-changes --human-readable
       --exclude '.DS_Store' --exclude '__pycache__' --exclude '*.pyc' --exclude '.venv'
       "$LOCAL_DIR/" "$REMOTE:$REMOTE_PROJECT/dashboard/")
if [ "$DRY_RUN" = 1 ]; then
  printf '     [dry-run] %s\n' "${RSYNC[*]}"
else
  "${RSYNC[@]}" || die "rsync 失败"
  remote "mkdir -p '$REMOTE_PROJECT/dashboard'" >/dev/null
  ok "已同步（$(remote "find '$REMOTE_PROJECT/dashboard' -type f | wc -l") 个文件）"
fi

# ---------------------------------------------------------------------------
# 3. systemd user 单元
# ---------------------------------------------------------------------------
step "安装 unit：$UNIT_PATH"
TMP_UNIT="$(mktemp)"; TMPFILES+=("$TMP_UNIT")
sed -e "s|@PROJECT_DIR@|$REMOTE_PROJECT|g" "$LOCAL_DIR/dashboard.service" > "$TMP_UNIT"
if [ "$DRY_RUN" = 1 ]; then
  printf '     [dry-run] 渲染后的 unit：\n'; sed 's/^/       /' "$TMP_UNIT"
else
  remote "mkdir -p '$REMOTE_HOME/.config/systemd/user'"
  remote "cat > '$UNIT_PATH'" < "$TMP_UNIT"
  ok "已写入（@PROJECT_DIR@ → ${REMOTE_PROJECT}）"
fi

# ---------------------------------------------------------------------------
# 4. enable --now / restart
# ---------------------------------------------------------------------------
step "启用并（重）启动 $UNIT_NAME"
run remote "systemctl --user daemon-reload"
run remote "systemctl --user enable --now '$UNIT_NAME'" || die "enable --now 失败"
if [ "$SKIP_RESTART" = 1 ]; then
  warn "--no-restart：跳过 restart（新代码可能未生效）"
else
  # enable --now 对已在运行的服务不重启，这里显式 restart 保证加载新代码
  run remote "systemctl --user restart '$UNIT_NAME'" || die "restart 失败"
fi

# ---------------------------------------------------------------------------
# 5. 健康检查
# ---------------------------------------------------------------------------
step "健康检查 http://127.0.0.1:$PORT/api/health"
HEALTH=""
for i in $(seq 1 30); do
  if [ "$DRY_RUN" = 1 ]; then HEALTH='{"status":"dry-run"}'; break; fi
  if OUT="$(remote "curl -sf -m 3 'http://127.0.0.1:$PORT/api/health'" 2>/dev/null)"; then
    HEALTH="$OUT"; break
  fi
  sleep 1
done

if [ -z "$HEALTH" ]; then
  remote "systemctl --user status --no-pager -l '$UNIT_NAME' | tail -25" || true
  remote "journalctl --user -u '$UNIT_NAME' --no-pager -n 40" || true
  die "健康检查失败：$PORT/api/health 没有返回 200"
fi

CODE="$(remote "curl -s -o /dev/null -w '%{http_code}' -m 3 'http://127.0.0.1:$PORT/api/health'")"
ACTIVE="$(remote "systemctl --user is-active '$UNIT_NAME'" || true)"

# ---------------------------------------------------------------------------
# 6. 汇总
# ---------------------------------------------------------------------------
echo
printf '%s==========================================================%s\n' "$C_GREEN" "$C_OFF"
printf ' %-18s %s\n' "systemctl --user" "is-active = $ACTIVE"
printf ' %-18s %s\n' "health" "HTTP $CODE  $HEALTH"
printf ' %-18s %s\n' "unit" "$UNIT_PATH"
printf ' %-18s %s\n' "监听" "0.0.0.0:${PORT}（页面内 API 全为相对路径）"
printf ' %-18s %s\n' "隧道" "ssh -N -L $PORT:127.0.0.1:$PORT $REMOTE"
printf ' %-18s %s\n' "打开" "http://127.0.0.1:$PORT/"
printf '%s==========================================================%s\n' "$C_GREEN" "$C_OFF"

[ "$ACTIVE" = "active" ] && [ "$CODE" = "200" ] || die "服务未就绪（active=$ACTIVE code=${CODE}）"
ok "安装完成"
