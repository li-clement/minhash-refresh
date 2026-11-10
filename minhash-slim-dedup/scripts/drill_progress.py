#!/usr/bin/env python3
"""不上集群演练监控：用真实 progress.py 的写端 API 造一份假 run（不 import ray）。

    python3 drill_progress.py --repo ~/code/raytest --run-dir /tmp/raytest-drill --algo slim

产出 progress.json / workers/*.json / result.json，跑完做契约自检（阶段齐全、percent=100、
docs_done=total、stage=done）。想用面板看这份假数据，就在 node1 上跑同一命令，再另起一个
指向该目录的面板（见 references/monitoring.md §5）。
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path


def plan(algo: str, docs: int) -> list[dict]:
    """返回 [{name,total,weight,doc}]；doc=True 表示 done 按篇计数。"""
    if algo == "slim":
        u = max(1, int(docs * 0.667))
        rows = 12 * u
        return [
            {"name": "exact", "total": docs, "weight": 0.06, "doc": True},
            {"name": "signature", "total": u, "weight": 0.24, "doc": True},
            {"name": "band", "total": rows, "weight": 0.08, "doc": False},
            {"name": "compare", "total": rows, "weight": 0.44, "doc": False},
            {"name": "resolve", "total": 1, "weight": 0.18, "doc": False},
        ]
    if algo == "minhash":
        rows = 12 * docs
        return [
            {"name": "signature", "total": docs, "weight": 0.30, "doc": True},
            {"name": "shuffle", "total": docs, "weight": 0.10, "doc": True},
            {"name": "compare", "total": rows, "weight": 0.45, "doc": False},
            {"name": "verify", "total": max(1, docs // 4), "weight": 0.10, "doc": False},
            {"name": "resolve", "total": 1, "weight": 0.05, "doc": False},
        ]
    rows = 8 * docs
    return [
        {"name": "fingerprint", "total": docs, "weight": 0.30, "doc": True},
        {"name": "shuffle", "total": docs, "weight": 0.10, "doc": True},
        {"name": "compare", "total": rows, "weight": 0.45, "doc": False},
        {"name": "verify", "total": max(1, docs // 4), "weight": 0.10, "doc": False},
        {"name": "resolve", "total": 1, "weight": 0.05, "doc": False},
    ]


def simulate(prog, root: Path, stages: list[dict], speed: float, workers: int) -> None:
    prog.declare([{k: s[k] for k in ("name", "total", "weight")} for s in stages])
    prog.start_monitor()
    prog.event(f"drill 启动: {len(stages)} 阶段, {prog.docs_total:,} 篇（不碰 Ray）")
    chunk_sec = max(0.01, 0.6 / max(speed, 0.1) / 10)
    for st in stages:
        total = st["total"]
        prog.set_stage(st["name"], total=total)
        prog.event(f"{st['name']}: 0/{total:,}")
        reporters = [prog_reporter(root, st["name"]) for _ in range(workers)]
        per = max(1, total // 10)
        done = 0
        while done < total:
            step = min(per, total - done)
            done += step
            reporters[(done // per) % workers].report(step, done - step, done)
            prog.set_stage(st["name"], done=done)
            if st["doc"]:
                prog.set_docs_done(done)
            time.sleep(chunk_sec)
        prog.event(f"{st['name']}: 完成 {total:,}")
    prog.update(edges=12_345, verify_total=12_345, verify_kept=9_001,
                docs_kept=int(prog.docs_total * 0.4), docs_dropped=int(prog.docs_total * 0.6),
                unique=int(prog.docs_total * 0.667))
    prog.set_stage("resolve", done=1)
    prog.finish()
    prog.stop_monitor()


def prog_reporter(root: Path, stage: str):
    from progress import WorkerReporter  # 延迟导入：--repo 先插 sys.path

    return WorkerReporter(stage=stage, root=root)


def write_result(root: Path, algo: str, docs: int) -> None:
    result = {
        "run_id": f"drill_{algo}",
        "algorithm": algo,
        "mode": "drill",
        "docs": docs,
        "bands": 12, "rows": 10, "threshold": 0.8, "shingle": 3, "pivot_over": 1_000_000_000,
        "metrics": {"kept": int(docs * 0.4), "dropped": int(docs * 0.6),
                    "groups_missing": 0, "components_merged": 0,
                    "neg_false_drop_rate": 0.0, "border_dropped": 0,
                    "pair_recall_edge_canon": 0.99},
        "sec": {"total": 1.0},
    }
    (root / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=1))


def check(root: Path, docs: int, stages: list[dict]) -> list[str]:
    problems: list[str] = []
    try:
        p = json.loads((root / "progress.json").read_text())
    except Exception as exc:
        return [f"progress.json 读不了: {exc}"]
    if p.get("stage") != "done":
        problems.append(f"stage={p.get('stage')}（应为 done）")
    if not isinstance(p.get("percent"), (int, float)) or abs(p["percent"] - 100.0) > 1e-6:
        problems.append(f"percent={p.get('percent')}（应为 100.0）")
    if p.get("docs_done") != docs:
        problems.append(f"docs_done={p.get('docs_done')}（应为 {docs}）")
    got = [s.get("name") for s in p.get("stages", [])]
    want = [s["name"] for s in stages]
    if got != want:
        problems.append(f"stages={got}（应为 {want}）")
    for s in p.get("stages", []):
        if s.get("total") and s.get("done") != s.get("total"):
            problems.append(f"阶段 {s.get('name')} 未收满: {s.get('done')}/{s.get('total')}")
    if not (root / "result.json").exists():
        problems.append("缺 result.json（面板会一直显示「尚无 result.json」）")
    if not list((root / "workers").glob("*.json")):
        problems.append("workers/ 为空（任务分配表会是空的）")
    return problems


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default=str(Path.home() / "code" / "raytest"),
                    help="raytest 仓库路径（import progress.py）")
    ap.add_argument("--run-dir", default="/tmp/raytest-drill", help="假 run 的目录（别指向正在跑的 /tmp/raytest）")
    ap.add_argument("--algo", choices=["slim", "minhash", "simhash"], default="slim")
    ap.add_argument("--docs", type=int, default=1000)
    ap.add_argument("--speed", type=float, default=4.0, help="倍速，越大越快")
    ap.add_argument("--workers", type=int, default=4, help="模拟几个 worker（每阶段）")
    args = ap.parse_args()

    repo = Path(args.repo).expanduser()
    if not (repo / "progress.py").exists():
        print(f"✗ {repo}/progress.py 不存在；用 --repo 指向 raytest 仓库", file=sys.stderr)
        return 2
    sys.path.insert(0, str(repo))
    from progress import ProgressReporter

    root = Path(args.run_dir)
    root.mkdir(parents=True, exist_ok=True)
    stages = plan(args.algo, args.docs)
    prog = ProgressReporter(root=root, docs_total=args.docs, algorithm=args.algo)
    simulate(prog, root, stages, args.speed, args.workers)
    write_result(root, args.algo, args.docs)

    problems = check(root, args.docs, stages)
    print(f"run 目录: {root}")
    if problems:
        print("✗ 契约自检失败：")
        for p in problems:
            print("  -", p)
        return 1
    print("✅ 契约自检通过：5 阶段齐全、percent=100、docs_done=docs_total、stage=done、"
          "workers/result 都在")
    print("下一步（在 node1 上另起一个面板看这份假数据）：")
    print(f"  ssh node1 'cd ~/code/raytest && RAYTEST_RUN_DIR={root} RAYTEST_PORT=8503 "
          f"RAYTEST_LANCE_DIR=/tmp/nonexistent .venv/bin/python dashboard/app.py'")
    print("  ssh -N -L 8503:127.0.0.1:8503 node1   # → http://127.0.0.1:8503/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
