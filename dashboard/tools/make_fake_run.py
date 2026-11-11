#!/usr/bin/env python3
"""生成一份「假实验」运行时目录 / 假历史 run 列表，用来演练监控页面（绝不写真实的 /tmp/raytest）。

用法（在 node1 上）：
  # 一次性静态快照（simhash 的 5 个阶段、5 个 worker、若干漏斗字段、非空日志）
  .venv/bin/python tools/make_fake_run.py --run-dir /tmp/raytest-fake

  # 换成 minhash 的阶段词表与参数（signature / 分带入桶 / Jaccard 估计、bands×rows×threshold）
  .venv/bin/python tools/make_fake_run.py --run-dir /tmp/raytest-fake --algo minhash

  # 持续写入（每 2s 推进一次，模拟主实验进程；Ctrl-C 停止）
  .venv/bin/python tools/make_fake_run.py --run-dir /tmp/raytest-fake --algo minhash --animate --duration 600

  # 演练「日志 0 字节」
  .venv/bin/python tools/make_fake_run.py --run-dir /tmp/raytest-fake --empty-log

  # 只造一份假历史 run 列表（两种算法都有），供没有 lancedb 的环境测 /api/runs
  .venv/bin/python tools/make_fake_run.py --runs-json /tmp/raytest-fake/runs.json

然后在另一个端口起一份面板指向它（真实服务不受影响）：
  RAYTEST_RUN_DIR=/tmp/raytest-fake RAYTEST_PORT=8503 \
  RAYTEST_LANCE_DIR=/tmp/raytest-fake/lance RAYTEST_RUNS_JSON=/tmp/raytest-fake/runs.json \
    .venv/bin/python dashboard/app.py
"""
from __future__ import annotations

import argparse
import json
import random
import time
from pathlib import Path

# 与 progress.py 里真实上报的阶段词表一致：simhash 首段 fingerprint，minhash 首段 signature
ALGO_STAGES: dict[str, list[tuple[str, float]]] = {
    "simhash": [("fingerprint", 0.30), ("shuffle", 0.10), ("compare", 0.45),
                ("verify", 0.10), ("resolve", 0.05)],
    "minhash": [("signature", 0.32), ("shuffle", 0.10), ("compare", 0.43),
                ("verify", 0.10), ("resolve", 0.05)],
}
ALGO_PARAMS: dict[str, dict] = {
    "simhash": {"bits": 128, "band_bits": 16, "hamming": 12, "shingle": 3, "verify": 0.0},
    "minhash": {"bands": 8, "rows": 16, "threshold": 0.8, "shingle": 3, "verify": 0.0},
}
STAGE_TEXT = {
    "fingerprint": "指纹计算", "signature": "签名计算", "shuffle": "分带入桶",
    "compare": "桶内比较", "verify": "Jaccard 估计", "resolve": "并查集归并",
}


def atomic(path: Path, obj) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(path)


def build_progress(docs_done: int, docs_total: int, stage: str, now: float,
                   events: list[dict] | None = None, tick: int = 0,
                   algo: str = "simhash", run_id: str = "") -> dict:
    """按 weight 加权算 percent（与主实验的加权算法一致），并让「当前阶段」随 tick 推进。"""
    stages_cfg = ALGO_STAGES.get(algo, ALGO_STAGES["simhash"])
    names = [n for n, _ in stages_cfg]
    first = names[0]
    totals = {first: docs_total, "shuffle": docs_total, "compare": 40,
              "verify": 41233, "resolve": 40}
    cur = names.index(stage) if stage in names else len(names)

    stages, percent = [], 0.0
    for i, (name, weight) in enumerate(stages_cfg):
        total = totals.get(name) or 1
        if stage == "done" or cur > i:
            done = total
        elif cur == i:
            done = {
                first: docs_done,
                "shuffle": docs_done,
                "compare": min(total, 6 + tick * 2),
                "verify": min(total, int(total * 0.7) + tick * 120),
                "resolve": min(total, max(1, tick)),
            }.get(name, 0)
        else:
            done = 0
        # 还没轮到的阶段：total 留空，页面显示「未开始」
        pending = cur < i and done == 0
        stages.append({"name": name, "weight": weight, "done": done,
                       "total": None if pending else total})
        if not pending:
            percent += weight * (done / total)

    return {
        "run_id": run_id or f"20261110-141500-{algo}",
        "algorithm": algo,
        "updated_at": now,
        "stage": stage,
        "stages": stages,
        "percent": round(percent * 100, 2),
        "docs_total": docs_total,
        "docs_done": docs_done,
        "docs_kept": 96100 if stage in ("resolve", "done") else None,
        "docs_dropped": 103900 if stage in ("resolve", "done") else None,
        "verify_total": 39880 if stage in ("verify", "resolve", "done") else None,
        "verify_kept": 39880 if stage in ("resolve", "done") else None,
        "edges": 39880 if stage in ("resolve", "done") else None,
        "throughput_docs_per_sec": 1520.5,
        "events": (events or [])[-40:],
    }


def build_workers(now: float, docs_done: int, algo: str = "simhash") -> list[dict]:
    first = ALGO_STAGES.get(algo, ALGO_STAGES["simhash"])[0][0]
    specs = [
        ("actor-7f3a", 12345, "192.168.2.51", first, 37, 4736, 980.2, 1.0),
        ("actor-91c2", 12388, "192.168.2.51", first, 12, 1536, 610.4, 3.0),
        ("actor-4d10", 12401, "192.168.2.51", "shuffle", 9, 1152, 420.0, 2.0),
        ("actor-b7e3", 12520, "192.168.2.52", "compare", 28, 3584, 780.5, 1.0),
        # 故意一直不刷新 updated_at：演示「疑似空闲」
        ("actor-c05e", 12633, "192.168.2.52", "verify", 5, 640, 240.0, 45.0),
    ]
    out = []
    for wid, pid, node, stage, batches, docs, rate, lag in specs:
        out.append({
            "worker_id": wid, "pid": pid, "node": node, "stage": stage,
            "status": "running", "batches_done": batches + docs_done // 2000,
            "docs_done": docs + docs_done // 4, "docs_per_sec": round(rate, 1),
            "last_batch": {"start": docs + docs_done // 4 - 36, "end": docs + docs_done // 4, "docs": 36},
            "updated_at": now - lag,
        })
    return out


def build_result(algo: str = "simhash", run_id: str = "") -> dict:
    params = dict(ALGO_PARAMS.get(algo, ALGO_PARAMS["simhash"]))
    result = {
        "run_id": run_id or f"20261110-141500-{algo}",
        "algorithm": algo,
        "docs": 200000, "chars": 61639900,
        "seed": 7, "corpus_sha256": "f" * 64, "subset_table": None, "subset_sha256": None,
        "ray": {"dup_total": 119800, "dup_recall": 0.9987, "neg_total": 6700,
                "neg_false_drop_rate": 0.0003, "extra_kept_groups": 2,
                "missing_kept_groups": 0, "kept": 96100, "dropped": 103900,
                "edges": 41233, "pair_recall": 0.9689, "pair_precision": 0.9802,
                "edge_fp_cross_base": 28, "components_merged": 7,
                "seconds": 128.4, "docs_per_sec": 1557.6, "mode": "ray"},
        "local": {"dup_total": 119800, "dup_recall": 0.5451, "neg_total": 6700,
                  "neg_false_drop_rate": 0.0, "extra_kept_groups": 226,
                  "missing_kept_groups": 0, "kept": 95900, "dropped": 104100,
                  "edges": 39880, "pair_recall": 0.9689, "pair_precision": 0.9802,
                  "edge_fp_cross_base": 28, "components_merged": 7,
                  "seconds": 553.2, "docs_per_sec": 361.3, "mode": "local"},
    }
    result.update(params)
    return result


def build_log(algo: str = "simhash") -> str:
    params = ALGO_PARAMS.get(algo, ALGO_PARAMS["simhash"])
    cfg = " ".join(f"{k}={v}" for k, v in params.items())
    first_stage = ALGO_STAGES.get(algo, ALGO_STAGES["simhash"])[0][0]
    first_text = STAGE_TEXT.get(first_stage, "指纹")
    lines = [f"[{time.strftime('%H:%M:%S')}] raytest fake run 启动 "
             f"(algo={algo}, docs=200000, {cfg})"]
    for i in range(36):
        if i == 30:
            lines += ["Traceback (most recent call last):",
                      '  File "dedup_ray.py", line 118, in _compare_group',
                      "    dist = np.bitwise_count(fps[i+1:] ^ fps[i])",
                      "ValueError: operands could not be broadcast together"]
        lines.append(f"[ray] batch {i:03d} {first_text}完成 "
                     f"docs={i * 128 + 128:>6} 1520.5 docs/s")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------- 假历史 run
# (run_id, algo, mode, docs, hamming, bands, rows, threshold, verify,
#  recall, precision, cross_base, merged, missing, kept, edges, seconds, rate)
FAKE_RUNS: list[tuple] = [
    ("fake_simhash_1k_ham12", "simhash", "local", 1002, 12, None, None, None, 0.0,
     0.9683, 0.9794, 0, 0, 0, 395, 869, 2.67, 375.3),
    ("fake_simhash_1k_ham12", "simhash", "ray", 1002, 12, None, None, None, 0.0,
     0.9683, 0.9794, 0, 0, 0, 397, 872, 6.21, 161.4),
    ("fake_simhash_10k_ham12", "simhash", "local", 10002, 12, None, None, None, 0.0,
     0.9689, 0.9802, 28, 7, 7, 3868, 8891, 28.13, 355.6),
    ("fake_simhash_10k_ham12", "simhash", "ray", 10002, 12, None, None, None, 0.0,
     0.9689, 0.9802, 28, 7, 7, 3872, 8900, 12.55, 797.3),
    ("fake_simhash_10k_verify08", "simhash", "ray", 10002, 12, None, None, None, 0.8,
     0.9689, 0.9867, 0, 0, 0, 3895, 8842, 13.39, 747.0),
    ("fake_simhash_10k_ham10", "simhash", "ray", 10002, 10, None, None, None, 0.0,
     0.9205, 0.9909, 0, 0, 0, 4134, 8364, 12.66, 790.3),
    ("fake_minhash_1k_b8r16", "minhash", "ray", 1002, None, 8, 16, 0.8, 0.0,
     0.9412, 0.9736, 0, 0, 0, 421, 834, 5.88, 170.4),
    ("fake_minhash_10k_b8r16", "minhash", "ray", 10002, None, 8, 16, 0.8, 0.0,
     0.9534, 0.9771, 3, 1, 1, 3948, 8612, 12.98, 770.4),
    ("fake_minhash_10k_verify08", "minhash", "ray", 10002, None, 8, 16, 0.8, 0.8,
     0.9534, 0.9849, 0, 0, 0, 3988, 8540, 13.71, 729.5),
    ("fake_minhash_10k_b16r8", "minhash", "ray", 10002, None, 16, 8, 0.8, 0.0,
     0.9788, 0.9701, 5, 2, 2, 3902, 8790, 13.02, 768.1),
]


def build_fake_runs() -> list[dict]:
    t0 = time.mktime(time.strptime("2026-11-10 09:00:00", "%Y-%m-%d %H:%M:%S"))
    rows = []
    for i, (run_id, algo, mode, docs, ham, bands, nrows, threshold, verify,
            recall, precision, cross, merged, missing, kept, edges, secs, rate) in enumerate(FAKE_RUNS):
        created = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(t0 + i * 900))
        if algo == "minhash":
            params = {"bands": bands, "rows": nrows, "threshold": threshold}
        else:
            params = {"bits": 128, "band_bits": 16, "hamming": ham}
        rows.append({
            "run_id": run_id, "algo": algo, "mode": mode, "created_at": created,
            "docs": docs, "chars": docs * 308, "shingle": 3,
            "bits": params.get("bits"), "band_bits": params.get("band_bits"),
            "hamming": params.get("hamming"), "bands": params.get("bands"),
            "rows": params.get("rows"), "threshold": params.get("threshold"),
            "verify": verify,
            "corpus_sha256": "f" * 64,
            "subset_table": f"subset_{max(1, docs // 1000)}k",
            "subset_sha256": "a" * 64,
            "edges": edges, "band_hits": None, "kept": kept, "dropped": docs - kept,
            "pair_recall": recall, "pair_precision": precision,
            "edge_fp_cross_base": cross,
            "groups_collapsed_rate": round((docs - kept) / docs, 4),
            "groups_extra_kept": 0, "groups_missing": missing,
            "components_merged": merged, "neg_false_drop_rate": 0.0,
            "border_dropped": 3, "border_total": 45,
            "seconds": secs, "docs_per_sec": rate,
        })
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", default="/tmp/raytest-fake")
    ap.add_argument("--algo", choices=["simhash", "minhash"], default="simhash",
                    help="假实验的算法（决定阶段词表与参数）")
    ap.add_argument("--docs-total", type=int, default=200000)
    ap.add_argument("--stage", default="", help="当前阶段；默认取该算法的第 3 个阶段")
    ap.add_argument("--docs-done", type=int, default=48000)
    ap.add_argument("--animate", action="store_true", help="持续写入，模拟运行中的实验")
    ap.add_argument("--interval", type=float, default=2.0)
    ap.add_argument("--duration", type=float, default=0, help="秒；0=一直跑")
    ap.add_argument("--empty-log", action="store_true", help="把 experiment.log 写成 0 字节")
    ap.add_argument("--no-result", action="store_true", help="不写 result.json")
    ap.add_argument("--runs-json", default="",
                    help="只生成假历史 run 列表到该文件（供 RAYTEST_RUNS_JSON），不写运行目录")
    args = ap.parse_args()

    if args.runs_json:
        out = Path(args.runs_json).resolve()
        if str(out).startswith("/tmp/raytest/"):
            raise SystemExit(f"拒绝写入真实实验目录: {out}（假历史数据请放 /tmp/raytest-fake）")
        out.parent.mkdir(parents=True, exist_ok=True)
        runs = build_fake_runs()
        out.write_text(json.dumps({"runs": runs}, ensure_ascii=False, indent=1), encoding="utf-8")
        algos = sorted({r["algo"] for r in runs})
        print(f"[fake] 写入 {out}：{len(runs)} 行 / {len({r['run_id'] for r in runs})} 个 run，"
              f"algo={','.join(algos)}", flush=True)
        return

    algo = args.algo
    stage = args.stage or ALGO_STAGES[algo][2][0]
    if stage not in [n for n, _ in ALGO_STAGES[algo]]:
        raise SystemExit(f"{algo} 没有阶段 {stage}；可选 {[n for n, _ in ALGO_STAGES[algo]]}")

    run = Path(args.run_dir).resolve()
    if str(run) in ("/tmp/raytest", "/") or run.name == "raytest":
        raise SystemExit(f"拒绝写入真实实验目录: {run}（本脚本只写 fake 目录）")
    (run / "workers").mkdir(parents=True, exist_ok=True)

    now = time.time()
    docs = args.docs_done
    run_id = f"fake-{time.strftime('%Y%m%d-%H%M%S')}-{algo}"
    params = ALGO_PARAMS[algo]
    cfg = " ".join(f"{k}={v}" for k, v in params.items())
    events = [
        {"ts": now - 300, "level": "info", "msg": f"run {run_id} 启动，algo={algo}，docs={args.docs_total}"},
        {"ts": now - 240, "level": "info", "msg": f"算法参数 {cfg}"},
        {"ts": now - 120, "level": "info", "msg": f"stage {ALGO_STAGES[algo][0][0]} 进度 48000/{args.docs_total}"},
        {"ts": now - 60, "level": "warn", "msg": "actor-91c2 吞吐下降 (980 -> 610 docs/s)"},
        {"ts": now - 20, "level": "error", "msg": "actor-c05e 45s 无心跳，疑似空闲"},
    ]

    end = now + args.duration if args.duration else float("inf")
    first = True
    tick = 0
    while True:
        now = time.time()
        atomic(run / "progress.json",
               build_progress(docs, args.docs_total, stage, now, events, tick, algo, run_id))
        for w in build_workers(now, docs, algo):
            atomic(run / "workers" / (w["worker_id"] + ".json"), w)
        if first:
            if not args.no_result:
                result = build_result(algo, run_id)
                atomic(run / "result.json", result)
                (run / "runs").mkdir(parents=True, exist_ok=True)
                atomic(run / "runs" / f"{run_id}.json", result)
            (run / "experiment.log").write_text("" if args.empty_log else build_log(algo),
                                                encoding="utf-8")
            first = False
        elif not args.empty_log:
            with open(run / "experiment.log", "a", encoding="utf-8") as fh:
                fh.write(f"[ray] batch {docs // 128:05d} 完成 docs={docs:>6} 1520.5 docs/s\n")
        print(f"[fake] {time.strftime('%H:%M:%S')} algo={algo} docs={docs} "
              f"stage={stage} dir={run}", flush=True)
        if not args.animate or time.time() >= end:
            break
        docs = min(args.docs_total, docs + random.randint(2400, 3600))
        tick += 1
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
