#!/usr/bin/env bash
# 跑一次瘦身 minhash：远端 nohup 起进程，本地每 5s 打一行进度，跑完把结果拉回归档。
#
#   run_slim.sh --mode local --table subset_10k --tag smoke10k
#   run_slim.sh --mode ray --table subset_1000k --pivot-over 1000000000 --tag f4b_1m
#
# 包装参数：--mode local|ray  --table T  --tag TAG  --isolated  --timeout S  --stop
# `--` 之后的参数原样传给 dedup_slim.py（--threshold/--bands/--rows/--k/--cpu/--pivot-over ...）。
set -euo pipefail
HOST=${RAYTEST_HOST:-node1}
# 注意：不要用 ~/... 作默认值 —— bash 会在 ${VAR:-word} 里做波浪号展开，把本地路径塞给远端。
# 默认按「远端 HOME 下的相对路径」处理；给绝对路径则原样使用。
REPO=${RAYTEST_REPO:-code/raytest}
case "$REPO" in
  /*) REMOTE_CD="cd $(printf '%q' "$REPO")" ;;
  *)  REMOTE_CD="cd \"\$HOME\"/$(printf '%q' "$REPO")" ;;
esac
RUN_DIR=${RAYTEST_RUN_DIR:-/tmp/raytest}
HERE=$(cd "$(dirname "$0")" && pwd)

MODE=local
TABLE=subset_10k
TAG=""
ISOLATED=0
TIMEOUT=3600
STOP=0
ARGS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --mode) MODE=${2:?--mode 需要值}; shift 2 ;;
    --table) TABLE=${2:?--table 需要值}; shift 2 ;;
    --tag) TAG=${2:?--tag 需要值}; shift 2 ;;
    --isolated) ISOLATED=1; shift ;;
    --timeout) TIMEOUT=${2:?--timeout 需要值}; shift 2 ;;
    --stop) STOP=1; shift ;;
    --) shift; ARGS+=("$@"); break ;;
        -h|--help) sed -n '2,8p' "$0"; exit 0 ;;
    *) ARGS+=("$1"); shift ;;
  esac
done
[ -n "$TAG" ] || TAG="${MODE}_${TABLE}_$(date +%H%M%S)"

PDIR=$RUN_DIR
[ "$ISOLATED" -eq 1 ] && PDIR="$RUN_DIR/$TAG-progress"
OUT=$RUN_DIR/result.json
LOG=$RUN_DIR/$TAG.log
PIDFILE=$RUN_DIR/$TAG.pid

# ---- --stop：收掉这个 tag 的远端进程 -------------------------------------
if [ "$STOP" -eq 1 ]; then
  PID=$(ssh "$HOST" "cat $(printf '%q' "$PIDFILE") 2>/dev/null || true")
  [ -n "$PID" ] || { echo "没有 ${PIDFILE}（没在跑，或换过 tag）" >&2; exit 1; }
  ssh "$HOST" "kill -TERM $PID 2>/dev/null; sleep 1; kill -KILL $PID 2>/dev/null || true; rm -f $(printf '%q' "$PIDFILE")"
  echo "已停止 ${TAG}（pid ${PID}）；Ray 上已启动的 task 会在 driver 退出后自行结束"
  exit 0
fi

# ---- 防撞车：同一个 progress_dir 上不要并行 ---------------------------------
if [ "$ISOLATED" -eq 0 ]; then
  INFO=$(ssh "$HOST" "python3 - <<'PY' 2>/dev/null || true
import json, time
try:
    d = json.load(open('$RUN_DIR/progress.json'))
except Exception:
    raise SystemExit(0)
print(d.get('stage', '?'), int(time.time() - float(d.get('updated_at', 0))))
PY
")
  ST=${INFO%% *}
  AGE=${INFO##* }
  if [ -n "${ST:-}" ] && [ "$ST" != "done" ] && [ "${AGE:-999}" -lt 120 ]; then
    echo "⚠️  $RUN_DIR/progress.json 显示 stage=${ST}、${AGE}s 前还在更新：有实验正在跑。" >&2
    echo "    要么等它结束，要么加 --isolated（progress 写到 $RUN_DIR/$TAG-progress，面板默认看不到）。" >&2
    exit 2
  fi
fi

# ---- 远端启动 ---------------------------------------------------------------
Q=""
for a in ${ARGS[@]+"${ARGS[@]}"}; do Q+="$(printf '%q' "$a") "; done

REMOTE_CMD="$REMOTE_CD || exit 3
mkdir -p $(printf '%q' "$RUN_DIR")
rm -f $(printf '%q' "$OUT")
nohup .venv/bin/python -u dedup_slim.py \
  --mode $(printf '%q' "$MODE") --table $(printf '%q' "$TABLE") \
  --progress-dir $(printf '%q' "$PDIR") --out $(printf '%q' "$OUT") \
  $Q> $(printf '%q' "$LOG") 2>&1 < /dev/null &
echo \$! > $(printf '%q' "$PIDFILE")
cat $(printf '%q' "$PIDFILE")"

PID=$(ssh "$HOST" "$REMOTE_CMD")
[ -n "$PID" ] || { echo "✗ 启动失败（没拿到 pid），看 $LOG" >&2; exit 1; }
echo "▶ $TAG  mode=$MODE table=$TABLE  pid=$PID  log=$HOST:$LOG  progress=$PDIR"
[ "$ISOLATED" -eq 1 ] && echo "  隔离模式：面板要指 RAYTEST_RUN_DIR=$PDIR 才看得到"

# ---- 轮询 -------------------------------------------------------------------
START=$(date +%s)
while :; do
  sleep 5
  "$HERE/watch.sh" --once --brief || true
  if ! ssh -o ConnectTimeout=8 "$HOST" "kill -0 $PID 2>/dev/null"; then break; fi
  if [ $(( $(date +%s) - START )) -gt "$TIMEOUT" ]; then
    echo "⏱  超过 ${TIMEOUT}s 仍在跑；远端继续，用 $0 --stop --tag $TAG 收掉" >&2
    exit 3
  fi
done

# ---- 收结果 -----------------------------------------------------------------
if ! ssh "$HOST" "test -f $(printf '%q' "$OUT")"; then
  echo "✗ 进程结束但没有 ${OUT}；日志尾部：" >&2
  ssh "$HOST" "tail -30 $(printf '%q' "$LOG")" >&2 || true
  exit 1
fi
ssh "$HOST" "mkdir -p $(printf '%q' "$RUN_DIR/runs") && cp $(printf '%q' "$OUT") $(printf '%q' "$RUN_DIR/runs/$TAG.json") && rm -f $(printf '%q' "$PIDFILE")"

LOCAL_OUT=${LOCAL_OUT:-results/$TAG}
mkdir -p "$LOCAL_OUT"
scp -q "$HOST:$OUT" "$LOCAL_OUT/result_slim_$TAG.json"
scp -q "$HOST:$LOG" "$LOCAL_OUT/$TAG.log" || true
echo "📦 归档: $LOCAL_OUT/{result_slim_$TAG.json,$TAG.log}  （远端副本 $RUN_DIR/runs/$TAG.json）"

python3 - "$LOCAL_OUT/result_slim_$TAG.json" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))
m = r.get("metrics", {})
print(f"== {r.get('run_id')}  {r.get('mode')}/{r.get('table')}  docs={r.get('docs')} unique={r.get('unique')} ==")
print("sec   : " + "  ".join(f"{k}={v}" for k, v in r.get("sec", {}).items()))
print("bytes : " + f"slim={r.get('bytes', {}).get('slim_bytes')} "
      f"({r.get('bytes', {}).get('slim_bytes_per_doc')} B/篇) "
      f"ratio={r.get('bytes', {}).get('ratio')}×")
b = r.get("bucket", {})
print("bucket: " + f"buckets={b.get('buckets')} compared={b.get('compared')} "
      f"max={b.get('max_bucket')} pivot={b.get('pivot_buckets')}")
keys = ("kept", "dropped", "groups_missing", "components_merged", "neg_false_drop_rate",
        "border_dropped", "pair_recall", "pair_recall_edge_canon")
print("指标  : " + "  ".join(f"{k}={m.get(k)}" for k in keys if k in m))
if b.get("pivot_buckets"):
    print("⚠️  触发了 pivot 星形：1M 上应给 --pivot-over 1000000000（会多留几篇）")
print("判读  : 召回看 pair_recall_edge_canon；kept/dropped 要和 fat 版逐项相等才算等价")
PY
