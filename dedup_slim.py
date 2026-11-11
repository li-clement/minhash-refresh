"""瘦身版去重流水线（为 600M 形态做的 1M 验证实现）。

每个阶段都是一个 Ray task（`stages.py`），驱动只做编排。相对老 fat 链路的三处差别：

  阶段 0  精确预聚合：按「归一化文本哈希」把精确重复聚成一类，只对类代表算签名。
          1M 篇 -> 0.67M 个唯一签名（本语料 exact 占 33.3%）。
  阶段 2  瘦身分带行：只带 (band_key, uid) = 12 B，不再每条带行塞 512B 全签名。
          中间态 12 行 × 12 B = 144 B/篇（fat 版是 6,336 B/篇）。
  阶段 3  排序 + 连续扫描：不做 `groupby` + `map_groups`（逐桶物化），
          按 band_key 排完序线性扫，桶内用一张广播的 (U,k) 签名矩阵做 numpy 估计。

不变量（本实验的核心校验）：**带键仍由全精度签名算 blake2b**，
所以候选桶与 fat 版完全一致，只是「每条带行带什么」和「怎么扫」变了。
质量指标直接调 `metrics.evaluate`，与历史阶段同一套口径。

用法：
  python dedup_slim.py --mode local --table subset_10k
  python dedup_slim.py --mode ray   --table subset_1000k --cpu 20
"""
from __future__ import annotations

import argparse
import hashlib
import json
import time
from collections import Counter
from pathlib import Path

import numpy as np

from minhash import band_key, band_slices, estimate_many, permutations, signature
from text import shingles, ws_key

K = 128
BANDS = 12
ROWS = 10


# ---------------------------------------------------------------- 阶段 0
# ---------------------------------------------------------------- 阶段 1/2
# ---------------------------------------------------------------- 阶段 3
def scan_buckets(keys: np.ndarray, uids: np.ndarray, sigs: np.ndarray, threshold: float,
                 pivot_over: int = 64, progress_cb=None,
                 progress_every: int = 250_000) -> tuple[np.ndarray, dict]:
    """按 band_key 排序后线性扫描；桶内用签名矩阵估计 Jaccard。

    小桶两两比较；大桶（> pivot_over）改「一个 pivot 打星」，避免 C(m,2) 爆炸，
    被星形替代的桶数量如实记账（对应「丢传递边」的风险）。
    """
    order = np.argsort(keys, kind="stable")
    keys, uids = keys[order], uids[order]
    n = len(keys)
    if n == 0:
        return np.empty((0, 2), np.int32), dict(buckets=0, compared=0, max_bucket=0,
                                                pivot_buckets=0, mean_bucket=0.0)
    starts = np.flatnonzero(np.r_[True, keys[1:] != keys[:-1]])
    ends = np.r_[starts[1:], n]
    sizes = ends - starts
    pairs: list[tuple[int, int]] = []
    compared = 0
    pivot_buckets = 0
    seen_rows = 0
    next_mark = progress_every
    for s, e, m in zip(starts, ends, sizes):
        seen_rows += int(m)
        if progress_cb is not None and seen_rows >= next_mark:
            progress_cb(seen_rows)
            next_mark += progress_every
        if m < 2:
            continue
        idx = uids[s:e]
        if m > pivot_over:
            pivot_buckets += 1
            head, rest = idx[:1], idx[1:]
            est = estimate_many(sigs[head[0]], sigs[rest])
            compared += len(rest)
            for j in np.flatnonzero(est >= threshold):
                pairs.append((int(head[0]), int(rest[j])))
            continue
        for i in range(m - 1):
            est = estimate_many(sigs[idx[i]], sigs[idx[i + 1:]])
            compared += len(est)
            for j in np.flatnonzero(est >= threshold):
                pairs.append((int(idx[i]), int(idx[i + 1 + j])))
    if progress_cb is not None:
        progress_cb(int(len(keys)))
    edges = (np.array(pairs, dtype=np.int32).reshape(-1, 2) if pairs
             else np.empty((0, 2), np.int32))
    return edges, dict(buckets=int(len(starts)), compared=int(compared),
                       max_bucket=int(sizes.max()), pivot_buckets=int(pivot_buckets),
                       mean_bucket=round(float(sizes.mean()), 3))


def scan_buckets_fast(keys: np.ndarray, uids: np.ndarray, sigs: np.ndarray, threshold: float,
                      chunk: int = 100_000, progress_cb=None,
                      progress_every: int = 500_000) -> tuple[np.ndarray, dict]:
    """排序 + 分段广播（向量化版 `scan_buckets`，语义完全等价：全程两两比较）。

    旧版对每个桶做一次 Python 调用（5.9M 个桶 × ~3.5 µs = 20s，全是调用开销）。
    这里把「桶内所有对」一次性展开成两个索引数组，再分块做签名比较：

        left  = 每一行（除桶内最后一行）
        right = 同一桶里排在它后面的行

    展开用 repeat + cumsum 的经典写法，内存 O(对数)，1M 上约 3.64M 对 ≈ 30 MB。
    签名比较分块（默认 10 万对/块），块内是 (chunk, k) 的逐位相等求和。
    """
    order = np.argsort(keys, kind="stable")
    keys = keys[order]
    uids = uids[order]
    n = len(keys)
    if n < 2:
        return np.empty((0, 2), np.int32), dict(buckets=0, compared=0, max_bucket=0,
                                                pivot_buckets=0, mean_bucket=0.0)

    starts = np.flatnonzero(np.r_[True, keys[1:] != keys[:-1]])
    ends = np.r_[starts[1:], n]
    sizes = ends - starts

    off = np.arange(n, dtype=np.int64) - np.repeat(starts, sizes)   # 桶内偏移
    bid = np.repeat(np.arange(len(starts), dtype=np.int64), sizes)  # 桶号
    cnt = sizes[bid] - 1 - off                                      # 该行要比较的伙伴数
    keep = cnt > 0
    if not keep.any():
        return np.empty((0, 2), np.int32), dict(buckets=int(len(starts)), compared=0,
                                                max_bucket=int(sizes.max()),
                                                pivot_buckets=0,
                                                mean_bucket=round(float(sizes.mean()), 3))

    left = np.flatnonzero(keep)
    cnt = cnt[keep]
    left_rep = np.repeat(left, cnt)
    # 桶起点 = 行号 - 桶内偏移；伙伴 = 桶起点 + 偏移 + 1 + 块内序号
    base = np.repeat(left - off[left], cnt)
    cum = np.cumsum(cnt)
    pos = np.arange(len(left_rep), dtype=np.int64) - np.repeat(cum - cnt, cnt)
    right = base + np.repeat(off[left], cnt) + 1 + pos

    total = len(left_rep)
    # 用「一致位数」做整数比较，避免 float32 在阈值附近抖动：
    # t*k 通常不是整数（0.8*128=102.4），整数 vs float64 的比较是精确的。
    counts = np.empty(total, dtype=np.int16)
    for s in range(0, total, chunk):
        e = min(s + chunk, total)
        a = uids[left_rep[s:e]]
        b = uids[right[s:e]]
        counts[s:e] = (sigs[a] == sigs[b]).sum(axis=1)
        if progress_cb is not None and e % progress_every < chunk:
            progress_cb(int(n * e / total))
    if progress_cb is not None:
        progress_cb(n)

    hit = counts >= (float(threshold) * sigs.shape[1])
    edges = (np.stack([uids[left_rep[hit]], uids[right[hit]]], axis=1).astype(np.int32)
             if hit.any() else np.empty((0, 2), np.int32))
    return edges, dict(buckets=int(len(starts)), compared=int(total),
                       max_bucket=int(sizes.max()), pivot_buckets=0,
                       mean_bucket=round(float(sizes.mean()), 3),
                       est_mean=round(float(counts.mean()) / sigs.shape[1], 4))


# ---------------------------------------------------------------- 阶段 4
def components(edges: np.ndarray, n: int) -> np.ndarray:
    """numpy 并查集（父指针 + 路径压缩），根恒为分量内最小 uid。"""
    parent = np.arange(n, dtype=np.int32)

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = int(parent[x])
        return x

    for a, b in edges:
        ra, rb = find(int(a)), find(int(b))
        if ra != rb:
            lo, hi = (ra, rb) if ra < rb else (rb, ra)
            parent[hi] = lo
    return np.array([find(i) for i in range(n)], dtype=np.int32)


def to_decision(docs, canon_ids, roots, alias, edges):
    """代表级结果 -> 原始 doc_id 级的 (kept, dropped, 文档级边)，交给参考口径评估。"""
    surv: dict[int, int] = {}
    for ci, cid in enumerate(canon_ids):
        r = int(roots[ci])
        sid = min(alias[cid])
        surv[r] = min(surv.get(r, 10**18), sid)
    dropped: dict[int, int] = {}
    for ci, cid in enumerate(canon_ids):
        rep = surv[int(roots[ci])]
        for m in alias[cid]:
            if m != rep:
                dropped[m] = rep
    kept = [d.doc_id for d in docs if d.doc_id not in dropped]

    pair_list: list[tuple[int, int]] = []
    for a, b in edges:
        for x in alias[canon_ids[int(a)]]:
            for y in alias[canon_ids[int(b)]]:
                pair_list.append((x, y) if x < y else (y, x))
    pairs = np.array(pair_list, dtype=np.int64) if pair_list else np.empty((0, 2), np.int64)
    return kept, dropped, pairs


def recall_decomposition(docs, canon_ids, alias, metrics) -> dict:
    """把成对召回拆开：精确预聚合会让「同一类内部」的真值对不可能有边。

    参考口径的 pair_recall 分母包含 (orig, exact) 这种「同一个类内部」的对，
    它们在瘦身版里被并成一类、天然没有边 —— 所以那个数会假性下跌。
    这里把它扣掉，给出**可比口径**的召回，并显式记录被扣掉多少对。
    """
    pos = {int(d.doc_id): i for i, d in enumerate(docs)}
    uid_of = {cid: i for i, cid in enumerate(canon_ids)}
    n = len(docs)
    base = np.array([int(d.base_id) for d in docs])
    kind = np.array([d.kind for d in docs], dtype=object)
    cu = np.empty(n, dtype=np.int32)
    for ci, cid in enumerate(canon_ids):
        for m in alias[cid]:
            cu[pos[m]] = ci

    sel = (base >= 0) & np.isin(kind, ["orig", "exact", "near"])
    key = np.stack([base[sel], cu[sel]], axis=1)
    uniq, cnt = np.unique(key, axis=0, return_counts=True)
    intra = int((cnt * (cnt - 1) // 2).sum())
    true_pairs = int(metrics.get("true_pairs") or 0)
    tp = int(metrics.get("edge_tp") or 0)
    denom = max(1, true_pairs - intra)
    return {
        "true_pairs_intra_canonical": intra,
        "pair_recall_edge_canon": round(tp / denom, 4),
        "pair_precision_note": "边界边数不变，但分母因同类合并变薄，故与 fat 版不可直接比",
    }


def _chunks(seq, n):
    size = max(1, (len(seq) + n - 1) // n)
    return [seq[i: i + size] for i in range(0, len(seq), size)]


def _gather(refs, on_done=None, poll: float = 0.2):
    """按完成顺序收结果（保持提交顺序），每完成一个就回调一次用于推进进度。"""
    import ray

    pending, out = list(refs), [None] * len(refs)
    done = 0
    while pending:
        ready, pending = ray.wait(pending, num_returns=1, timeout=poll)
        if not ready:
            continue
        r = ready[0]
        out[refs.index(r)] = ray.get(r)
        done += 1
        if on_done:
            on_done(done, len(refs))
    return out


def run(mode: str = "ray", uri: str = "/tmp/raytest/lance", table: str = "subset_10k",
        k: int = K, bands: int = BANDS, rows: int = ROWS, threshold: float = 0.8,
        shingle_n: int = 3, seed: int = 7, cpu: int = 20, pivot_over: int = 64,
        progress_dir: str | None = None, scan: str = "fast",
        eval_metrics: bool = True, run_id: str = "", bundle_samples: int = 24) -> dict:
    """整条流水线：每个阶段都是一个 Ray task（mode="local" 时在进程内顺序调用同一批函数）。"""
    import stages as S
    from lancedb_io import load_subset
    from progress import ProgressReporter

    use_ray = mode == "ray"
    if use_ray:
        import ray

        ray.init(address="auto", log_to_driver=False, ignore_reinit_error=True)
        call = lambda f, *a: ray.get(f.remote(*a))            # noqa: E731
        submit = lambda f, *a: f.remote(*a)                   # noqa: E731
        job_id = ""
        try:
            job_id = ray.get_runtime_context().get_job_id() or ""
        except Exception:
            pass
    else:
        call = lambda f, *a: f._function(*a)                  # noqa: E731
        submit = None
        job_id = ""

    t_all = time.perf_counter()
    docs = load_subset(uri, table)
    t_load = time.perf_counter() - t_all
    run_id = run_id or f"{time.strftime('%Y%m%d-%H%M%S')}-{(job_id or 'local')[:8]}"
    n = len(docs)
    nchunk = max(4, min(24, cpu))

    dwr = None
    prog = None
    if progress_dir:
        prog = ProgressReporter(root=Path(progress_dir), docs_total=n, algorithm="slim")
        prog.declare([
            {"name": "exact", "total": n, "weight": 0.06},
            {"name": "signature", "total": None, "weight": 0.26},
            {"name": "band", "total": None, "weight": 0.10},
            {"name": "compare", "total": None, "weight": 0.40},
            {"name": "resolve", "total": 1, "weight": 0.18},
        ])
        prog.start_monitor()
        prog.event(f"载入 {n:,} 篇（{t_load:.1f}s）· run_id={run_id}"
                   + (f" · ray_job={job_id}" if job_id else "") + f" · 模式={mode}")

        from progress import WorkerReporter as _WR

        class _DriverWorker:
            def __init__(self, stage: str):
                self.r = _WR(stage=stage, root=prog.root,
                             worker_id="driver", node=__import__("socket").gethostname())
                self.n = 0

            def stage(self, name: str, total: int | None = None) -> None:
                self.r.stage = name
                self.r.docs_done = 0
                self.r.batches_done = 0
                self.r.t_start = __import__("time").time()
                self._total = total or 0
                self.n = 0
                self.r._write()

            def report(self, docs: int) -> None:
                docs = int(docs)
                if docs <= 0:
                    return
                start = self.n
                self.n += docs
                self.r.report(docs, start, self.n)

        dwr = _DriverWorker("exact")

    # ---------------- 阶段 0：精确预聚合（并行分片哈希 -> 单 task 归并） ----------------
    t0 = time.perf_counter()
    texts = [d.text for d in docs]
    ids = [int(d.doc_id) for d in docs]
    parts = _chunks(list(zip(texts, ids)), nchunk)
    def _on_exact(done_chunks, total_chunks):
        frac = done_chunks / max(1, total_chunks)
        prog.set_stage("exact", done=min(n, int(n * frac)))
        if dwr:
            dwr.report(max(0, int(n * frac) - dwr.n))

    if use_ray:
        refs = [submit(S.exact_chunk, [t for t, _ in p], [i for _, i in p]) for p in parts]
        hashed = _gather(refs, on_done=_on_exact if prog else None)
    else:
        hashed = [call(S.exact_chunk, [t for t, _ in p], [i for _, i in p]) for p in parts]
        if prog:
            prog.set_stage("exact", done=n)
    hashes = [h for part in hashed for h in part]
    canon_ids, alias = call(S.dedupe_hashes, hashes, ids)
    del hashed, hashes, texts
    t_exact = time.perf_counter() - t0
    by_id = {int(d.doc_id): d for d in docs}
    canon = [by_id[c] for c in canon_ids]
    if prog:
        prog.set_stage("exact", done=n)
        prog.update(unique=len(canon), exact_sec=round(t_exact, 2))
        prog.event(f"精确预聚合: {n:,} → {len(canon):,} 篇（{t_exact:.2f}s，{len(parts)} 个分片）")

    # ---------------- 阶段 1：签名（并行分片） ----------------
    if prog:
        prog.set_stage("signature", total=len(canon))
    if dwr:
        dwr.stage("signature", total=len(canon))
    t1 = time.perf_counter()
    ctexts = [d.text for d in canon]
    cparts = _chunks(ctexts, nchunk)
    if use_ray:
        refs = [submit(S.sign_chunk, p, k, shingle_n, seed) for p in cparts]
        done_off = [0]

        def _on_sign(d, tot):
            off = sum(len(cparts[i]) for i in range(len(cparts)))
            prog.set_stage("signature", done=min(len(canon), int(len(canon) * d / tot)))
            prog.set_docs_done(int(len(canon) * d / tot))
            if dwr:
                dwr.report(max(0, int(len(canon) * d / tot) - dwr.n))
        got = _gather(refs, on_done=_on_sign if prog else None)
    else:
        got = [call(S.sign_chunk, p, k, shingle_n, seed) for p in cparts]
        if prog:
            prog.set_stage("signature", done=len(canon))
            prog.set_docs_done(len(canon))
    sigs = np.vstack(got) if got else np.zeros((0, k), np.uint32)
    t_sign = time.perf_counter() - t1
    if prog:
        prog.set_stage("signature", done=len(canon))
        prog.set_docs_done(len(canon))
        prog.event(f"签名完成: {t_sign:.1f}s（{len(cparts)} 个分片）")

    # ---------------- 阶段 2：分带行（并行分片） ----------------
    if prog:
        prog.set_stage("band", total=bands * len(canon))
    if dwr:
        dwr.stage("band", total=bands * len(canon))
    t2 = time.perf_counter()
    if use_ray:
        sigs_ref = ray.put(sigs)
        refs, offs = [], 0
        for p in _chunks(list(range(len(sigs))), nchunk):
            refs.append(submit(S.band_chunk, sigs[p[0]: p[-1] + 1], p[0], bands, rows))
        got = _gather(refs)
    else:
        got = []
        for p in _chunks(list(range(len(sigs))), nchunk):
            got.append(call(S.band_chunk, sigs[p[0]: p[-1] + 1], p[0], bands, rows))
    keys = np.concatenate([g[0] for g in got]) if got else np.empty(0, np.int64)
    uids = np.concatenate([g[1] for g in got]) if got else np.empty(0, np.int32)
    t_band = time.perf_counter() - t2
    if prog:
        prog.set_stage("band", done=bands * len(canon))
        prog.event(f"分带行: {len(keys):,} 行 × 12 B = {(keys.nbytes + uids.nbytes) / 1e6:.1f} MB")

    band_bytes = int(keys.nbytes + uids.nbytes)
    fat_bytes = int(len(keys) * (8 + 8 + k * 4))
    stats_bytes = {
        "band_rows": int(len(keys)),
        "slim_bytes": band_bytes,
        "slim_bytes_per_doc": round(band_bytes / n, 1),
        "fat_bytes": fat_bytes,
        "fat_bytes_per_doc": round(fat_bytes / n, 1),
        "ratio": round(fat_bytes / max(1, band_bytes), 1),
        "sigs_bytes": int(sigs.nbytes),
    }

    # ---------------- 阶段 3：桶内比较（按带并行，12 个 task） ----------------
    if prog:
        prog.set_stage("compare", total=len(keys))
        prog.event(f"桶内比较: 估计 Jaccard >= {threshold}，按 {bands} 个带并行")
    if dwr:
        dwr.stage("compare", total=len(keys))
    t3 = time.perf_counter()
    def _on_compare(done_bands, total_bands):
        frac = done_bands / max(1, total_bands)
        prog.set_stage("compare", done=min(len(keys), int(len(keys) * frac)))
        if dwr:
            dwr.report(max(0, int(len(keys) * frac) - dwr.n))

    if use_ray:
        refs = [submit(S.compare_band, keys, uids, sigs_ref, b, threshold, 100_000, scan)
                for b in range(bands)]
        got = _gather(refs, on_done=_on_compare if prog else None)
    else:
        got = [call(S.compare_band, keys, uids, sigs, b, threshold, 100_000, scan)
               for b in range(bands)]
        if prog:
            prog.set_stage("compare", done=len(keys))
    edges = np.concatenate([g[0] for g in got if len(g[0])]) if any(len(g[0]) for g in got) \
        else np.empty((0, 2), np.int32)
    compared = int(sum(g[1] for g in got))
    max_bucket = int(max((g[2] for g in got), default=0))
    t_scan = time.perf_counter() - t3
    bstats = {"buckets": int(bands and (len(keys) / bands) or 0), "compared": compared,
              "max_bucket": max_bucket, "pivot_buckets": 0,
              "mean_bucket": round(len(keys) / max(1, len(np.unique(keys))), 3) if len(keys) else 0.0}
    if prog:
        prog.set_stage("compare", done=len(keys))
        prog.event(f"候选边 {int(len(edges)):,} 条（{compared:,} 次比较，最大桶 {max_bucket}）")
        prog.set_stage("resolve")

    # ---------------- 阶段 4：并查集 ----------------
    t4 = time.perf_counter()
    roots = call(S.resolve, edges, len(canon))
    t_uf = time.perf_counter() - t4

    # ---------------- 阶段 5：文档级决策 + 指标 ----------------
    t5 = time.perf_counter()
    kept, dropped, doc_pairs = to_decision(docs, canon_ids, roots, alias, edges)
    if eval_metrics:
        base_ids = np.array([int(d.base_id) for d in docs], dtype=np.int64)
        kind_codes = np.array([S.KIND_CODES.get(d.kind, 9) for d in docs], dtype=np.int8)
        metrics = call(S.measure, np.array([int(d.doc_id) for d in docs], dtype=np.int64),
                       base_ids, kind_codes, kept, dropped, doc_pairs)
        metrics.update(recall_decomposition(docs, canon_ids, alias, metrics))
    else:
        metrics = {"kept": len(kept), "dropped": len(dropped),
                   "note": "--no-eval：跳过成对/组口径指标（生产路径，不用于验收）"}
    t_eval = time.perf_counter() - t5

    sec_dict = {"load": round(t_load, 2), "exact": round(t_exact, 2), "sign": round(t_sign, 2),
                "band_rows": round(t_band, 2), "scan": round(t_scan, 2), "uf": round(t_uf, 2),
                "eval": round(t_eval, 2), "total": round(time.perf_counter() - t_all, 2),
                "chunks": nchunk, "bands_parallel": bands}

    bundle_path = None
    if progress_dir:
        try:
            bundle = build_bundle(
                docs, canon_ids, alias, roots, sigs, edges, metrics, sec_dict, stats_bytes,
                bstats, {"k": k, "bands": bands, "rows": rows, "threshold": threshold,
                         "shingle": shingle_n, "seed": seed, "mode": mode, "table": table,
                         "pivot_over": pivot_over, "scan": scan, "job_id": job_id},
                samples=bundle_samples)
            bundle["run_id"] = run_id
            bundle["ray_job_id"] = job_id
            root = Path(progress_dir)
            root.mkdir(parents=True, exist_ok=True)
            for name in (f"result_slim_{run_id}.json", "result_slim_latest.json"):
                (root / name).write_text(json.dumps(bundle, ensure_ascii=False), encoding="utf-8")
            bundle_path = str(root / "result_slim_latest.json")
            append_run_index(root, bundle, bundle_path)
            if prog:
                prog.event(f"结果包已写: {bundle_path}")
        except Exception as exc:
            if prog:
                prog.event(f"结果包生成失败: {exc.__class__.__name__}: {exc}", "error")

    if prog:
        prog.set_stage("resolve", done=1)
        prog.update(edges=int(len(edges)), kept=metrics.get("kept"),
                    docs_kept=metrics.get("kept"), docs_dropped=metrics.get("dropped"),
                    band_rows=int(len(keys)), slim_bytes=band_bytes,
                    run_id=run_id, ray_job_id=job_id)
        prog.event(f"完成: kept={metrics.get('kept')}, dropped={metrics.get('dropped')}, "
                   f"edges={int(len(edges))}")
        prog.finish()
        prog.stop_monitor()

    return {
        "run_id": run_id, "ray_job_id": job_id,
        "mode": mode, "table": table, "docs": n, "unique": len(canon),
        "k": k, "bands": bands, "rows": rows, "threshold": threshold,
        "shingle": shingle_n, "seed": seed, "scan_impl": scan, "pivot_over": pivot_over,
        "bundle": bundle_path,
        "sec": sec_dict,
        "bytes": stats_bytes,
        "bucket": bstats,
        "edges_canonical": int(len(edges)),
        "metrics": metrics,
    }


def append_run_index(root: Path, bundle: dict, bundle_path: str) -> None:
    """把本次 run 追加进 runs_index.json：run_id + ray job 编码 + 独立时间戳 + 关键指标。"""
    idx = Path(root) / "runs_index.json"
    items: list[dict] = []
    if idx.exists():
        try:
            items = json.loads(idx.read_text(encoding="utf-8"))
        except Exception:
            items = []
    p_ = bundle.get("params") or {}
    m = bundle.get("metrics") or {}
    b_ = bundle.get("bytes") or {}
    items.append({
        "run_id": bundle.get("run_id"),
        "ray_job_id": bundle.get("ray_job_id") or "",
        "ts": bundle.get("ts"),
        "finished_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "mode": p_.get("mode"), "table": p_.get("table"),
        "docs": (bundle.get("funnel") or {}).get("docs"),
        "unique": (bundle.get("funnel") or {}).get("unique"),
        "kept": m.get("kept"), "dropped": m.get("dropped"),
        "seconds": (bundle.get("sec") or {}).get("total"),
        "slim_bytes": b_.get("slim_bytes"),
        "bands": p_.get("bands"), "rows": p_.get("rows"),
        "threshold": p_.get("threshold"), "k": p_.get("k"),
        "bundle": bundle_path,
    })
    seen, out = set(), []
    for it in reversed(items):
        if it.get("run_id") in seen:
            continue
        seen.add(it.get("run_id"))
        out.append(it)
    out.reverse()
    idx.write_text(json.dumps(out[-200:], ensure_ascii=False, indent=1), encoding="utf-8")


def build_bundle(docs, canon_ids, alias, roots, sigs, edges, metrics, sec, bytes_, bucket,
                 params, samples: int = 24) -> dict:
    """把一次 run 的结果整理成面板能直接画的一包 JSON（不依赖 LanceDB）。

    内容：漏斗/耗时/体积/桶/验收项 + 分量大小分布 + 估计 Jaccard 直方图 +
    「代表 vs 被删」的文本对照样本（按分量大小分层抽样）。
    """
    import random as _random
    from collections import Counter as _Counter

    n = len(docs)
    pos = {int(d.doc_id): i for i, d in enumerate(docs)}
    comp = np.empty(n, dtype=np.int64)
    cu = np.empty(n, dtype=np.int64)
    uid_of = {cid: i for i, cid in enumerate(canon_ids)}
    for ci, cid in enumerate(canon_ids):
        for m in alias[cid]:
            j = pos[m]
            comp[j] = int(roots[ci])
            cu[j] = ci

    members: dict[int, list[int]] = {}
    for i in range(n):
        members.setdefault(int(comp[i]), []).append(i)
    sizes = np.array([len(v) for v in members.values()], dtype=np.int64)
    size_hist = _Counter(np.clip(sizes, 1, 9).tolist())

    kind = [d.kind for d in docs]
    kept_rows = [min(members[c]) for c in members]

    # 估计 Jaccard 直方图：按候选边（canonical 级，去重后）算一次
    uniq = np.unique(np.sort(edges, axis=1), axis=0) if len(edges) else np.empty((0, 2), np.int32)
    if len(uniq):
        est = (sigs[uniq[:, 0]] == sigs[uniq[:, 1]]).mean(axis=1)
        ehist, ebin = np.histogram(est, bins=np.arange(0.75, 1.001, 0.025))
        jacc = {"bins": [round(float(x), 3) for x in ebin[:-1]],
                "counts": [int(x) for x in ehist],
                "mean": round(float(est.mean()), 4), "n": int(len(est)),
                "min": round(float(est.min()), 4), "max": round(float(est.max()), 4)}
    else:
        jacc = {"bins": [], "counts": [], "mean": None, "n": 0, "min": None, "max": None}

    def doc_view(i: int) -> dict:
        d = docs[i]
        return {"doc_id": int(d.doc_id), "kind": d.kind, "base_id": int(d.base_id),
                "chars": len(d.text), "text": d.text[:480],
                "truncated": len(d.text) > 480}

    # 分层抽样：大的分量（多篇被删）+ 随机的（常见形态）
    pool = [(c, v) for c, v in members.items() if len(v) > 1]
    pool.sort(key=lambda kv: -len(kv[1]))
    picked: list[tuple[int, list[int]]] = pool[: max(1, samples // 3)]
    rest = pool[len(picked):]
    rng = _random.Random(7)
    if rest:
        picked += rng.sample(rest, min(len(rest), samples - len(picked)))
    # 再补几组「被拆开没合的」（组内还有多篇幸存），看漏召回长什么样
    per_base = {}
    for i in range(n):
        b = int(docs[i].base_id)
        if b >= 0:
            per_base.setdefault(b, {}).setdefault(int(comp[i]), []).append(i)
    split = [(b, v) for b, v in per_base.items() if len(v) > 1]
    for b, v in (rng.sample(split, min(len(split), 4)) if split else []):
        picked.append((-b - 1, [x for g in v.values() for x in g][:4]))

    out_samples = []
    for c, idxs in picked:
        idxs = sorted(idxs, key=lambda i: int(docs[i].doc_id))
        rep = idxs[0]
        others = []
        for i in idxs[1:4]:
            same = cu[i] == cu[rep]
            e = 1.0 if same else float((sigs[cu[i]] == sigs[cu[rep]]).mean())
            others.append({**doc_view(i), "est": round(e, 4), "same_class": bool(same)})
        out_samples.append({"component": int(c), "size": len(idxs),
                            "rep": doc_view(rep), "dropped": others,
                            "split_group": int(c) < 0})

    return {
        "run_id": None, "ts": __import__("time").time(),
        "params": params, "sec": sec, "bytes": bytes_, "bucket": bucket,
        "metrics": metrics,
        "funnel": {"docs": n, "unique": len(canon_ids), "kept": int(metrics.get("kept") or 0),
                   "dropped": int(metrics.get("dropped") or 0),
                   "components": int(len(members)),
                   "collapsed": int((sizes > 1).sum())},
        "component_sizes": {str(k): int(v) for k, v in sorted(size_hist.items())},
        "jaccard": jacc,
        "kinds": {"all": dict(_Counter(kind)),
                  "kept": dict(_Counter(kind[i] for i in kept_rows))},
        "samples": out_samples,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["local", "ray"], default="ray",
                    help="ray=每个阶段都是 Ray task（默认）；local=同函数在进程内顺序跑，仅调试用")
    ap.add_argument("--uri", default="/tmp/raytest/lance")
    ap.add_argument("--table", default="subset_10k")
    ap.add_argument("--k", type=int, default=K)
    ap.add_argument("--bands", type=int, default=BANDS)
    ap.add_argument("--rows", type=int, default=ROWS)
    ap.add_argument("--threshold", type=float, default=0.8)
    ap.add_argument("--shingle", type=int, default=3)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--cpu", type=int, default=20)
    ap.add_argument("--run-id", default="", help="不给则用 <时间戳>-<ray job 前 8 位>")
    ap.add_argument("--out", default="")
    ap.add_argument("--progress-dir", default="", help="给监控面板上报进度（一般 /tmp/raytest）")
    ap.add_argument("--bundle-samples", type=int, default=24,
                    help="结果包里的文本对照样本条数")
    ap.add_argument("--scan", choices=["fast", "loop"], default="fast",
                    help="fast=分段广播向量化（默认）；loop=逐桶 Python 循环（对照用）")
    ap.add_argument("--no-eval", action="store_true",
                    help="跳过参考口径指标（生产计时用；验收必须开）")
    ap.add_argument("--pivot-over", type=int, default=64,
                    help="桶内多于该值时改用 pivot 星形；给 1e9 等于全程两两比较")
    args = ap.parse_args()

    if args.mode == "ray":
        import ray

        ray.init(address="auto", log_to_driver=False, ignore_reinit_error=True)
        cpu = max(3, int(ray.cluster_resources().get("CPU", 22)) - 2)
    else:
        cpu = args.cpu

    res = run(args.mode, args.uri, args.table, args.k, args.bands, args.rows,
              args.threshold, shingle_n=args.shingle, seed=args.seed, cpu=cpu,
              pivot_over=args.pivot_over, progress_dir=args.progress_dir or None,
              scan=args.scan, eval_metrics=not args.no_eval, run_id=args.run_id,
              bundle_samples=args.bundle_samples)
    print(json.dumps(res, ensure_ascii=False, indent=1, default=str))
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(res, f, ensure_ascii=False, indent=1, default=str)
        print(f"written {args.out}")


if __name__ == "__main__":
    main()
