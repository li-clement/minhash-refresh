"""去重质量指标（组口径 + 成对口径）——与阶段 A–F 同一套口径，独立成模块。

`evaluate(docs, kept, dropped, pairs)`：
  docs  需要有 .doc_id / .base_id / .kind 三个属性（lancedb_io.load_subset 的 dataclass 即是）
  kept  保留的 doc_id 列表；dropped  dict: doc_id -> 代表 doc_id
  pairs 文档级候选边 ndarray (m,2)，用于成对召回/精度

口径要点（别踩过的坑）：
  * 真值对只统计 kind in (orig, exact, near) 且 base_id >= 0 的同组对，**不含 border**；
  * 召回 = 真值对里有边的比例（edge 口径），不是「dup 是否被删」；
  * groups_collapsed_rate 用「组里还剩几篇」算，代表文档取分量内最小 doc_id。
"""
from __future__ import annotations

from collections import Counter

import numpy as np

def evaluate(docs, kept, dropped, pairs=None) -> dict:
    """按「组」和「成对」两个口径评估。

    组口径回答「去重结果对不对」：每个 base 组应恰好剩 1 篇（neg 单独一篇）。
    成对口径回答「LSH 索引好不好」：同组文档对有没有被召回、有没有跨组误判。
    注意别把「代表文档是谁」当成召回：代表取分量内最小 doc_id，orig 常常不是那个最小 id，
    于是「dup 是否被丢」这个指标会被代表选择稀释（1K 试验里同一份结果 0.62 vs 0.99 就是这么来的）。
    """
    kind = {d.doc_id: d.kind for d in docs}
    base = {d.doc_id: d.base_id for d in docs}

    def gid(doc_id: int):
        b = base[doc_id]
        return b if b >= 0 else ("neg", doc_id)

    kept_set = set(kept)
    dups = [d.doc_id for d in docs if d.kind in ("exact", "near")]
    negs = [d.doc_id for d in docs if d.kind == "neg"]
    border = [d.doc_id for d in docs if d.kind == "border"]

    # 组口径
    comp = {i: r for i, r in dropped.items()}
    for i in kept_set:
        comp[i] = i
    groups_of: dict = {}            # 分量根 -> 该分量里的组
    for i, r in comp.items():
        groups_of.setdefault(r, set()).add(gid(i))
    merged_components = sum(1 for gs in groups_of.values() if len(gs) > 1)
    kept_per_group = Counter(gid(i) for i in kept_set)
    all_groups = {gid(d.doc_id) for d in docs}
    extra = sum(1 for g, c in kept_per_group.items() if c > 1)
    missing = sum(1 for g in all_groups if kept_per_group.get(g, 0) == 0)

    # 成对口径（pairs: 去重后的候选边）
    pair_stats = {}
    if pairs is not None and len(pairs):
        true_pairs = set()
        by_base: dict = {}
        for d in docs:
            if d.base_id >= 0 and d.kind in ("orig", "exact", "near"):
                by_base.setdefault(d.base_id, []).append(d.doc_id)
        for members in by_base.values():
            for i in range(len(members)):
                for j in range(i + 1, len(members)):
                    a, b = members[i], members[j]
                    true_pairs.add((a, b) if a < b else (b, a))
        edges = {(int(a), int(b)) if a < b else (int(b), int(a)) for a, b in pairs}
        tp = edges & true_pairs
        fp_cross = sum(1 for a, b in edges - true_pairs if base[a] >= 0 and base[a] == base[b])
        pair_stats = {
            "edge_total": len(edges),
            "true_pairs": len(true_pairs),
            "edge_tp": len(tp),
            "edge_fp_cross_base": len(edges - true_pairs) - fp_cross,
            "edge_fp_border": fp_cross,   # 同组成员的 edge，但至少一方是 border（灰区）
            "pair_recall": round(len(tp) / len(true_pairs), 4) if true_pairs else None,
            "pair_precision": round(len(tp) / len(edges), 4) if edges else None,
        }

    dups_kept = sum(1 for i in dups if i in kept_set)
    return {
        "dup_total": len(dups),
        "dup_kept": dups_kept,
        # 组是否被正确压成 1 篇（≠「dup 是否被丢」：代表取最小 doc_id，幸存者可能是 near/exact）
        "groups_collapsed_rate": round(1 - extra / len(all_groups), 4) if all_groups else None,
        "neg_total": len(negs),
        "neg_false_drop_rate": round(sum(1 for i in negs if i in dropped) / len(negs), 4) if negs else None,
        "border_total": len(border),
        "border_dropped": sum(1 for i in border if i in dropped),
        "groups_total": len(all_groups),
        "groups_extra_kept": extra,
        "groups_missing": missing,
        "components_merged": merged_components,
        "survivor_is_orig_rate": round(
            sum(1 for d in docs if d.kind == "orig" and d.doc_id in kept_set)
            / max(1, sum(1 for d in docs if d.kind == "orig")), 4),
        "kept": len(kept),
        "dropped": len(dropped),
        **pair_stats,
    }


