#!/usr/bin/env python3
"""对拍两次 run 的决策：fat minhash 报告 与 slim 结果是否逐项一致。

    compare_runs.py results/phaseF_1m/phaseF_fat_minhash_1m.json \
                    results/f4b_1m/result_slim_f4b_1m.json

fat 报告的形状是 {"ray": {...}, "local": {...}}；slim 是 {"metrics": {...}}。
默认取 fat 的 ray 模式，可用 --mode local 换。

退出码：0 = 硬指标一致；1 = 有差异；2 = 文件/形状不认识。
"""
from __future__ import annotations

import argparse
import json
import sys

# 必须逐项相等：不一致就说明「瘦身改坏了决策」，其余数字都不用看
HARD = ("kept", "dropped", "groups_missing", "components_merged",
        "neg_false_drop_rate", "border_dropped")
# 只展示，不判死：口径不同（edge_total 的边定义、召回分母）或本来就允许取整
SOFT = ("edge_total", "true_pairs", "pair_recall", "pair_recall_edge_canon",
        "groups_extra_kept", "groups_collapsed_rate", "true_pairs_intra_canonical",
        "dup_total", "dup_kept")


def load(path: str, mode: str) -> tuple[dict, str]:
    with open(path, encoding="utf-8") as f:
        doc = json.load(f)
    if isinstance(doc.get("metrics"), dict):
        return doc["metrics"], "slim"
    for key in (mode, "ray", "local"):
        block = doc.get(key)
        if isinstance(block, dict) and "kept" in block:
            return block, key
    raise SystemExit(f"✗ {path}: 找不到 metrics / {mode} 指标块")


def fmt(v) -> str:
    if isinstance(v, float):
        return f"{v:.4f}"
    if isinstance(v, int):
        return f"{v:,}"
    return "—" if v is None else str(v)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("a", help="基准（fat）报告")
    ap.add_argument("b", help="对照（slim）结果")
    ap.add_argument("--mode", default="ray", choices=["ray", "local"])
    args = ap.parse_args()

    try:
        ma, ka = load(args.a, args.mode)
        mb, kb = load(args.b, args.mode)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"✗ 读不了: {exc}", file=sys.stderr)
        return 2

    print(f"A = {args.a}  [{ka}]")
    print(f"B = {args.b}  [{kb}]")
    print(f"{'指标':<28}{'A':>14}{'B':>14}  判定")
    bad = []
    for key in HARD + SOFT:
        va, vb = ma.get(key), mb.get(key)
        if va is None and vb is None:
            continue
        if key in HARD:
            same = va == vb
            flag = "✅" if same else "❌ 不一致"
            if not same:
                bad.append(key)
        else:
            same = va == vb
            flag = "·" if same else "· 口径/取整差异（不判死）"
        print(f"{key:<28}{fmt(va):>14}{fmt(vb):>14}  {flag}")

    if ma.get("pair_recall") != mb.get("pair_recall") and mb.get("pair_recall_edge_canon") is not None:
        print("\n注：pair_recall 不同是预期差异 —— slim 的精确预聚合让「同一类内部」的真值对不可能有边；"
              f"扣掉 {fmt(mb.get('true_pairs_intra_canonical'))} 对后看 pair_recall_edge_canon"
              f"（{fmt(mb.get('pair_recall_edge_canon'))}）。")

    print()
    if bad:
        print(f"✗ 决策不一致：{', '.join(bad)}")
        return 1
    print("✅ 决策一致（kept/dropped/groups_missing/components_merged/neg_false_drop/border_dropped）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
