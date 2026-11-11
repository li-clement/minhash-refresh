"""把瘦身流水线的每个阶段都做成 Ray task（`@ray.remote`），驱动只负责编排。

设计原则：
  * 每个阶段吃/吐 numpy 数组或纯 python 容器，边界清晰；
  * 大对象（签名矩阵）用 `ray.put` 放对象存储后按引用传参，同节点上零拷贝共享；
  * 分片粒度按「带」切（12 带天然独立），这也是 600M 时按带分区的同一套形状。

数值口径必须与串行版完全一致（kept/dropped 逐字节相同），所以每个函数体
就是原来驱动里的同一段代码，只是搬进了 task。
"""
from __future__ import annotations

import hashlib

import numpy as np
import ray

from minhash import band_key, band_slices, permutations, signature
from text import shingles, ws_key


# ---------------------------------------------------------------- 阶段 0：精确预聚合
@ray.remote(num_cpus=1)
def exact_chunk(texts: list[str], ids: list[int]) -> list[bytes]:
    """去空白后的 sha1（与串行版同一个键）。"""
    return [hashlib.sha1(ws_key(t).encode("utf-8")).digest() for t in texts]


@ray.remote(num_cpus=1)
def dedupe_hashes(hashes: list[bytes], ids: list[int]) -> tuple[list[int], dict[int, list[int]]]:
    """按 hash 归类；类代表取组内最小 doc_id（与串行版一致）。"""
    first: dict[bytes, int] = {}
    alias: dict[int, list[int]] = {}
    for h, did in zip(hashes, ids):
        c = first.get(h)
        if c is None:
            first[h] = did
            alias[did] = [did]
        else:
            alias[c].append(did)
    canon = sorted(alias)
    for c in alias:
        alias[c].sort()
    return canon, alias


# ---------------------------------------------------------------- 阶段 1：签名
@ray.remote(num_cpus=1)
def sign_chunk(texts: list[str], k: int, shingle_n: int, seed: int) -> np.ndarray:
    ab = permutations(k, seed)
    out = np.zeros((len(texts), k), dtype=np.uint32)
    for i, t in enumerate(texts):
        out[i] = signature(shingles(t, shingle_n), k, seed, ab)
    return out


# ---------------------------------------------------------------- 阶段 2：分带行
@ray.remote(num_cpus=1)
def band_chunk(sigs: np.ndarray, offset: int, bands: int, rows: int):
    """(m,k) 签名 -> 12m 行 (band_key, uid)。uid 是全局下标（offset 起）。"""
    m = len(sigs)
    keys = np.empty(bands * m, dtype=np.int64)
    uids = np.empty(bands * m, dtype=np.int32)
    for i, sig in enumerate(sigs):
        for b, chunk in band_slices(sig, bands, rows):
            keys[i * bands + b] = band_key(b, chunk)
            uids[i * bands + b] = offset + i
    return keys, uids


# ---------------------------------------------------------------- 阶段 3：桶内比较（按带并行）
@ray.remote(num_cpus=1)
def compare_band(keys: np.ndarray, uids: np.ndarray, sigs: np.ndarray, band: int,
                 threshold: float, chunk: int = 100_000, impl: str = "fast"):
    """只处理某一个带的桶：排序 -> 展开桶内对 -> 分块比较。与串行版同函数同参数。"""
    from dedup_slim import scan_buckets, scan_buckets_fast  # 复用同一段实现，避免两份逻辑漂移

    mask = (keys >> 40) == band
    if not mask.any():
        return np.empty((0, 2), np.int32), 0, 0
    if impl == "loop":
        edges, st = scan_buckets(keys[mask], uids[mask], sigs, threshold, pivot_over=1 << 62)
    else:
        edges, st = scan_buckets_fast(keys[mask], uids[mask], sigs, threshold, chunk=chunk)
    return edges, int(st["compared"]), int(st["max_bucket"])


# ---------------------------------------------------------------- 阶段 4/5：并查集与指标
@ray.remote(num_cpus=1)
def resolve(edges: np.ndarray, n: int) -> np.ndarray:
    from dedup_slim import components

    return components(edges, n)


KIND_CODES = {"orig": 0, "exact": 1, "near": 2, "border": 3, "neg": 4}
KIND_NAMES = {v: k for k, v in KIND_CODES.items()}


@ray.remote(num_cpus=1)
def measure(doc_ids: np.ndarray, base_ids: np.ndarray, kind_codes: np.ndarray,
            kept: list[int], dropped: dict, pairs: np.ndarray) -> dict:
    """参考口径指标（组口径 + 成对口径）。用三个数组传元数据，避免 pickle 一百万个元组。"""

    class _D:
        __slots__ = ("doc_id", "base_id", "kind")

        def __init__(self, did, base, kind):
            self.doc_id, self.base_id, self.kind = did, base, kind

    from metrics import evaluate

    docs = [_D(int(a), int(b), KIND_NAMES[int(c)])
            for a, b, c in zip(doc_ids, base_ids, kind_codes)]
    return evaluate(docs, kept, dropped, pairs)
