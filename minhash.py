"""MinHash 文本去重核心实现（与 simhash 共用 shingle、并查集与评估口径）。

思路（和 simhash 一一对应，方便逐项对照）:
1. 特征：同一套字符 3-gram shingle（`simhash.shingles`），两算法看的是同一批字节；
2. 签名：每个 shingle 只做**一次** 31bit 哈希，再用 k 组 (a,b) 做仿射置换
   `h_i = (a_i * h + b_i) mod P`（P = 2^31-1），每个位置取最小值 → k 维签名。
   一次哈希 + numpy 批量，避免「每个 shingle 调 k 次 blake2b」把指纹阶段拖成瓶颈。
   P 取 2^31-1 是为了 a*h < 2^62，uint64 里不会溢出。
3. 索引：b 带 × r 行（b*r <= k，用签名前 b*r 位），同带同值即候选对；
   带键 = blake2b(该带 r 个值) 的低 40 位 + 带号，避免 Python `hash()` 的进程随机化
   （多 worker 下会算出不同的桶）。
4. 判定：用**全部 k 位**的签名一致比例估计 Jaccard，>= t 判重。
   估计方差 ≈ sqrt(J(1-J)/k)：k=128、J≈0.9 时 σ≈0.027，阈值附近会有少量抖动
   （这是 MinHash 相对 simhash 汉明距离的固有差别，对比时要单独说明）。
5. 单机基线 `dedup()` 与 Ray 版共用同一套签名/分带/估计，保证 ray ≡ local。
"""
from __future__ import annotations

import hashlib
from itertools import combinations
from typing import Iterable, Sequence

import numpy as np

from text import shingles


class DedupResult:  # 单机基线的返回结构（原 simhash.py 里的同名类）
    def __init__(self, kept=None, dropped=None, candidate_pairs=0, pairs=None):
        self.kept = kept if kept is not None else []
        self.dropped = dropped if dropped is not None else {}
        self.candidate_pairs = candidate_pairs
        self.pairs = pairs if pairs is not None else set()

P = (1 << 31) - 1           # Mersenne 质数：a*h < 2^31*2^31 = 2^62，uint64 不溢出
BAND_KEY_BITS = 40          # 带键只用 40 位，剩下留给带号拼 int64


def hash31(tokens: Sequence[str]) -> np.ndarray:
    """每个 token 一次 blake2b(8B) → 31bit 正整数（0 保留给空文档）。"""
    if not tokens:
        return np.empty(0, dtype=np.uint64)
    raw = b"".join(
        hashlib.blake2b(t.encode("utf-8"), digest_size=8).digest() for t in tokens
    )
    vals = np.frombuffer(raw, dtype=">u8").astype(np.uint64) & np.uint64(P - 1)
    vals[vals == 0] = 1     # 0 在模乘里是不动点，会把签名钉死，挪开
    return vals


def permutations(k: int = 128, seed: int = 7) -> tuple[np.ndarray, np.ndarray]:
    """k 组 (a,b)：a 取 [1, P) 的非零乘子，b 取 [0, P)。固定 seed ⇒ 跨进程/跨 run 可复现。"""
    rng = np.random.default_rng(seed)
    a = rng.integers(1, P, size=k, dtype=np.uint64)
    b = rng.integers(0, P, size=k, dtype=np.uint64)
    return a, b


def signature(tokens: Sequence[str], k: int = 128, seed: int = 7,
              ab: tuple[np.ndarray, np.ndarray] | None = None) -> np.ndarray:
    """k 维 MinHash 签名（uint32，31bit 值）。空文档返回全 0（判重时自然互不相同）。"""
    hs = hash31(tokens)
    if len(hs) == 0:
        return np.zeros(k, dtype=np.uint32)
    a, b = ab if ab is not None else permutations(k, seed)
    # (n,k) 的仿射置换矩阵：uint64 乘加后取模，再按列取最小值
    proj = (hs[:, None] * a[None, :]) % np.uint64(P)
    proj = (proj + b[None, :]) % np.uint64(P)
    return proj.min(axis=0).astype(np.uint32)


def signature_of_text(text: str, shingle_n: int = 3, k: int = 128, seed: int = 7,
                      ab: tuple[np.ndarray, np.ndarray] | None = None) -> np.ndarray:
    return signature(shingles(text, shingle_n), k, seed, ab)


def estimate(sa: np.ndarray, sb: np.ndarray) -> float:
    """两条签名的一致比例 = Jaccard 的无偏估计。"""
    if sa.size == 0:
        return 0.0
    return float(np.count_nonzero(sa == sb)) / float(sa.size)


def estimate_many(sig: np.ndarray, others: np.ndarray) -> np.ndarray:
    """sig: (k,)；others: (m,k) → (m,) 的估计 Jaccard（给桶内比较用）。"""
    if others.size == 0:
        return np.empty(0, dtype=np.float64)
    return (others == sig[None, :]).sum(axis=1) / float(sig.size)


def band_slices(sig: np.ndarray, bands: int, rows: int) -> list[tuple[int, np.ndarray]]:
    """把签名切成 bands 段 × rows 行（要求 bands*rows <= len(sig)）。"""
    if bands * rows > len(sig):
        raise ValueError(f"bands*rows={bands * rows} 超过签名维度 {len(sig)}")
    return [(i, sig[i * rows : (i + 1) * rows]) for i in range(bands)]


def band_key(band: int, chunk: np.ndarray) -> int:
    """(带号, 该带 r 个值) → int64 桶键。用 blake2b 而不是 hash()：后者按进程随机化。"""
    h = int.from_bytes(hashlib.blake2b(chunk.tobytes(), digest_size=8).digest(), "big")
    return (band << BAND_KEY_BITS) | (h & ((1 << BAND_KEY_BITS) - 1))


def band_rows_for(sig: np.ndarray, bands: int, rows: int) -> list[tuple[int, int]]:
    """签名 → [(band_key, 带号), ...]（写进 shuffle 阶段的行）。"""
    return [(band_key(i, chunk), i) for i, chunk in band_slices(sig, bands, rows)]


def dedup(ids: Sequence[int], texts: Sequence[str], threshold: float = 0.8,
          bands: int = 12, rows: int = 10, k: int = 128, shingle_n: int = 3, seed: int = 7,
          collect_pairs: bool = True) -> DedupResult:
    """单机版 MinHash 去重：与 Ray 版同签名、同分带、同估计、同并查集语义。"""
    ab = permutations(k, seed)
    sigs = [signature_of_text(t, shingle_n, k, seed, ab) for t in texts]

    buckets: dict[int, list[int]] = {}
    for idx, sig in enumerate(sigs):
        for key, _band in band_rows_for(sig, bands, rows):
            buckets.setdefault(key, []).append(idx)

    parent = list(range(len(ids)))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    hit_pairs: set[tuple[int, int]] = set()
    for members in buckets.values():
        if len(members) < 2:
            continue
        for i, j in combinations(members, 2):
            if estimate(sigs[i], sigs[j]) < threshold:
                continue
            if collect_pairs:
                ia, ib = ids[i], ids[j]
                hit_pairs.add((ia, ib) if ia < ib else (ib, ia))
            ra, rb = find(i), find(j)
            if ra != rb:
                parent[max(ra, rb)] = min(ra, rb)

    res = DedupResult()
    for idx, doc_id in enumerate(ids):
        root = find(idx)
        if root != idx:
            res.dropped[doc_id] = ids[root]
    res.kept = [doc_id for idx, doc_id in enumerate(ids) if find(idx) == idx]
    res.pairs = hit_pairs
    return res


def expected_collision(j: float, bands: int, rows: int) -> float:
    """理论召回曲线：真实 Jaccard = j 时至少撞上一个带的概率。画参数曲线用。"""
    return 1.0 - (1.0 - j ** rows) ** bands


def threshold_of(bands: int, rows: int) -> float:
    """50% 碰撞点：t ≈ (1/b)^(1/r)。"""
    return (1.0 / bands) ** (1.0 / rows)


if __name__ == "__main__":
    # 自检：完全相同的两篇 → 估计 1.0；不改写的两篇 → 估计≈真实 Jaccard
    t = "报告显示, 公司营收在本季度同比增长12.3亿元, 高于行业平均水平。"
    s = signature_of_text(t)
    assert estimate(s, s) == 1.0
    t2 = t.replace("12.3", "13.1")
    e = estimate(signature_of_text(t2), s)
    ex = len(set(shingles(t)) & set(shingles(t2))) / len(set(shingles(t)) | set(shingles(t2)))
    print(f"估计 Jaccard={e:.3f} 真实 Jaccard={ex:.3f} k=128")
    for b, r in ((12, 10), (16, 8), (8, 16)):
        print(f"bands={b} rows={r}: 50% 阈值 ≈ {threshold_of(b, r):.3f}, "
              f"J=0.85 召回 ≈ {expected_collision(0.85, b, r):.3f}")
