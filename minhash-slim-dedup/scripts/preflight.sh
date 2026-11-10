#!/usr/bin/env bash
# 瘦身流水线 + 监控体检。默认只读，不动集群。
#   preflight.sh               只检查（有 ❌ 时退出码 1）
#   preflight.sh --stop-jobs   额外停掉 RUNNING/PENDING 的 Ray job（确认上一轮没停干净才用）
set -euo pipefail
HOST=${RAYTEST_HOST:-node1}
STOP=0
case "${1:-}" in
  --stop-jobs) STOP=1 ;;
  "" ) ;;
  -h|--help) sed -n '2,4p' "$0"; exit 0 ;;
  *) echo "未知参数: $1" >&2; exit 2 ;;
esac

OUT=$(ssh "$HOST" "STOP=$STOP bash -s" <<'REMOTE'
set -uo pipefail
DIR=${RAYTEST_DIR:-$HOME/code/raytest}
RAY=$DIR/.venv/bin/ray
PY=$DIR/.venv/bin/python
RUN=${RAYTEST_RUN_DIR:-/tmp/raytest}
FAIL=0
ok()   { printf '  ok    %s\n' "$*"; }
warn() { printf '  warn  %s\n' "$*"; }
bad()  { printf '  FAIL  %s\n' "$*"; FAIL=$((FAIL + 1)); }

echo "== 服务 =="
for svc in ray-head raytest-dashboard; do
  st=$(systemctl --user is-active "$svc" 2>/dev/null || echo unknown)
  [ "$st" = active ] && ok "$svc active" || bad "$svc = $st"
done

echo "== Ray 资源 =="
$RAY status 2>/dev/null | sed -n '/Total Usage/,/object_store/p' | head -4 || warn "ray status 无输出"

echo "== 作业 =="
$PY - <<'PY' || true
import re, subprocess
out = subprocess.run(["/usr/bin/env", "bash", "-lc",
                      "cd ~/code/raytest && .venv/bin/ray job list 2>/dev/null"],
                     capture_output=True, text=True).stdout
jobs = re.findall(r"submission_id='(raysubmit_[A-Za-z0-9]+)'.*?status=<JobStatus\.([A-Z]+)", out, re.S)
live = [(j, s) for j, s in jobs if s in ("RUNNING", "PENDING")]
print(f"  共 {len(jobs)} 个作业，运行/排队 {len(live)} 个")
for j, s in live:
    print(f"    {s:9} {j}")
if live:
    print("  warn  有作业在跑：新实验会和它抢 CPU（compare 段是单核为主）")
print("LIVE_IDS=" + " ".join(j for j, _ in live))
PY

if [ "${STOP:-0}" = "1" ]; then
  for j in $(cd "$DIR" && .venv/bin/ray job list 2>/dev/null | grep -o 'raysubmit_[A-Za-z0-9]*' | sort -u); do
    st=$(cd "$DIR" && .venv/bin/ray job status "$j" 2>/dev/null | grep -o 'RUNNING\|PENDING' | head -1 || true)
    [ -n "$st" ] && { echo "  停止 $j"; (cd "$DIR" && .venv/bin/ray job stop "$j" >/dev/null 2>&1) || true; }
  done
fi

echo "== LanceDB 表 =="
$PY - <<'PY' || bad "读不了 LanceDB"
import lancedb, os
uri = os.environ.get("RAYTEST_LANCE_DIR", "/tmp/raytest/lance")
try:
    names = set(lancedb.connect(uri).list_tables())
except Exception as e:
    print(f"  FAIL  {uri}: {e}")
    raise SystemExit(1)
need = {"corpus_1m", "subset_1000k", "subset_10k"}
print(f"  表: {', '.join(sorted(names))}")
missing = need - names
print("  FAIL  缺表: " + ", ".join(sorted(missing)) if missing else "  ok    1M/10K 子集表齐")
PY

echo "== 语料 =="
for f in "$RUN/corpus/1m/corpus_1m.jsonl" "$RUN/corpus/corpus_200k.jsonl"; do
  if [ -f "$f" ]; then ok "$(ls -lh "$f" | awk '{print $5" "$9}')"; else warn "缺失 $f（只影响需要它的规模）"; fi
done

echo "== 进度 / 面板 =="
$PY - <<'PY' || warn "没有 progress.json（还没跑过，或用了别的 --progress-dir）"
import json, os, time
p = os.path.join(os.environ.get("RAYTEST_RUN_DIR", "/tmp/raytest"), "progress.json")
d = json.load(open(p))
age = time.time() - float(d.get("updated_at", 0))
print(f"  run_id={d.get('run_id')} algo={d.get('algorithm')} stage={d.get('stage')} "
      f"percent={d.get('percent')} docs={d.get('docs_done')}/{d.get('docs_total')} age={age:.0f}s")
if age > 120 and d.get("stage") != "done":
    print("  warn  进度超过 120s 没更新，且没到 done：上一轮可能异常退出")
elif d.get("stage") == "done":
    print("  ok    上一轮已收尾")
else:
    print("  warn  有实验正在写进度（别在同一个 progress_dir 上再开一个）")
PY
curl -fsS --max-time 4 "http://127.0.0.1:8501/api/health" >/dev/null 2>&1 \
  && ok "dashboard /api/health 正常（本地需 ssh -N -L 8501:127.0.0.1:8501 node1）" \
  || warn "dashboard :8501 在本机不可达（可能没开隧道）；在 node1 上可直接 curl"

echo "== 代码 =="
for f in dedup_slim.py progress.py minhash.py dashboard/app.py; do
  [ -f "$DIR/$f" ] && ok "$(stat -c '%y' "$DIR/$f" | cut -c1-19)  $f" || bad "缺 $DIR/$f"
done
$PY -c "import sys; sys.path.insert(0, '$DIR'); import dedup_slim, progress; print('  ok    dedup_slim/progress 可导入')" \
  || bad "import dedup_slim 失败"

echo
if [ "$FAIL" -eq 0 ]; then echo "结论: 可以开跑"; else echo "结论: 先处理上面 $FAIL 处 FAIL"; fi
REMOTE
) || true

printf '%s\n' "$OUT"
printf '%s' "$OUT" | grep -q '^  FAIL' && exit 1
exit 0
