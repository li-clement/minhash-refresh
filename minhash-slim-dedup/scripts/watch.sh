#!/usr/bin/env bash
# 看瘦身实验进度：读远端 progress.json（不碰 Ray、不加锁）。
#
#   watch.sh                    每 5s 一行（brief），stage=done 自动退出
#   watch.sh --full             每轮多打阶段条 + 最后一条事件
#   watch.sh --once             只打一次
#   watch.sh --json             机器可读（单行 JSON，含 stages/events 摘要）
#   watch.sh --interval 2       改刷新间隔
set -euo pipefail
HOST=${RAYTEST_HOST:-node1}
RUN_DIR=${RAYTEST_RUN_DIR:-/tmp/raytest}
INTERVAL=5
MODE=loop
FORMAT=brief
while [ $# -gt 0 ]; do
  case "$1" in
    --once) MODE=once; shift ;;
    --interval) INTERVAL=${2:?--interval 需要值}; shift 2 ;;
    --json) FORMAT=json; shift ;;
    --full) FORMAT=full; shift ;;
    --brief) FORMAT=brief; shift ;;
        -h|--help) sed -n '2,8p' "$0"; exit 0 ;;
    *) echo "未知参数: $1" >&2; exit 2 ;;
  esac
done

exec ssh -o ConnectTimeout=8 "$HOST" \
  "RUN_DIR=$(printf '%q' "$RUN_DIR") INTERVAL=$INTERVAL MODE=$MODE FORMAT=$FORMAT python3 -" <<'PY'
import json
import os
import sys
import time

RUN_DIR = os.environ["RUN_DIR"]
INTERVAL = float(os.environ["INTERVAL"])
MODE = os.environ["MODE"]
FORMAT = os.environ["FORMAT"]


def human(n):
    if n is None:
        return "—"
    try:
        n = int(n)
    except (TypeError, ValueError):
        return str(n)
    if n >= 1_000_000:
        return f"{n / 1e6:.1f}M"
    return f"{n:,}"


def snapshot():
    try:
        with open(os.path.join(RUN_DIR, "progress.json")) as f:
            p = json.load(f)
    except FileNotFoundError:
        return None, "等待 progress.json（%s 里还没有）" % RUN_DIR
    except Exception as exc:
        return None, f"progress.json 读不了：{exc}"
    return p, None


def line(p):
    age = time.time() - float(p.get("updated_at") or 0)
    stages = p.get("stages") or []
    stage = p.get("stage") or "?"
    hint = p.get("stage_hint") or ""
    pct = p.get("percent")
    docs, total = p.get("docs_done"), p.get("docs_total")
    thr = p.get("throughput_docs_per_sec")
    alive, wtotal = p.get("workers_alive"), p.get("workers_total")
    head = (f"[{time.strftime('%H:%M:%S')}] {p.get('algorithm') or '?'} · {stage}"
            + (f"(hint={hint})" if hint and hint != stage else "")
            + (f" · {pct:.1f}%" if isinstance(pct, (int, float)) else "")
            + f" · 文档 {human(docs)}/{human(total)}"
            + (f" · {thr:,.0f} 篇/s" if isinstance(thr, (int, float)) else "")
            + (f" · workers {alive}/{wtotal}" if alive is not None else "")
            + f" · 更新 {age:.0f}s 前")
    if FORMAT == "brief":
        return head
    rows = [head]
    for st in stages:
        done, tot = st.get("done"), st.get("total")
        pct_s = "—" if not tot else f"{min(100.0, 100.0 * done / tot):.0f}%"
        rows.append(f"   {st.get('name'):<10} {pct_s:>4}  [{human(done)}/{human(tot)}]"
                    + (f"  w={st.get('weight')}" if st.get("weight") is not None else ""))
    events = p.get("events") or []
    if events:
        rows.append(f"   · {events[-1].get('msg')}")
    return "\n".join(rows)


def emit(p):
    if FORMAT == "json":
        stages = p.get("stages") or []
        out = {
            "run_id": p.get("run_id"), "algorithm": p.get("algorithm"),
            "stage": p.get("stage"), "stage_hint": p.get("stage_hint"),
            "percent": p.get("percent"), "docs_done": p.get("docs_done"),
            "docs_total": p.get("docs_total"),
            "throughput": p.get("throughput_docs_per_sec"),
            "workers": [p.get("workers_alive"), p.get("workers_total")],
            "age_sec": round(time.time() - float(p.get("updated_at") or 0), 1),
            "stages": [{k: s.get(k) for k in ("name", "done", "total", "weight")} for s in stages],
            "last_event": (p.get("events") or [{}])[-1].get("msg"),
        }
        print(json.dumps(out, ensure_ascii=False), flush=True)
    else:
        print(line(p), flush=True)


while True:
    p, err = snapshot()
    if p is None:
        print(err, flush=True)
        return_code = 0
        if MODE == "once":
            sys.exit(return_code)
    else:
        emit(p)
        if MODE == "once" or (p.get("stage") == "done"):
            sys.exit(0)
    time.sleep(INTERVAL)
PY
