"""固定语料快照：一次生成、多次复用，保证 simhash / minhash 跑的是同一批数据。

用法:
  python corpus_io.py --docs 200000 --out /tmp/raytest/corpus/corpus_200k.jsonl   # 生成快照
  python corpus_io.py --inspect /tmp/raytest/corpus/corpus_200k.jsonl              # 看分布/校验和

快照是 jsonl，一行一篇：{"doc_id","text","base_id","kind"}；同目录写一份 manifest.json
（docs / seed / sha256 / chars），实验报告里带上 sha256 就能证明两次跑的是同一批数据。
大的快照只生成一次，小规模实验按「文件前 N 篇」取子集（顺序已按 seed 洗牌）。
"""
from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path

from gen_corpus import Doc, build_corpus


def dump_corpus(path: str | Path, n_docs: int = 200000, seed: int = 7) -> dict:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    docs = build_corpus(n_docs, seed=seed)
    digest = hashlib.sha256()
    tmp = path.with_name(path.name + ".tmp")
    chars = 0
    with open(tmp, "w", encoding="utf-8") as f:
        for d in docs:
            line = json.dumps({"doc_id": d.doc_id, "text": d.text,
                               "base_id": d.base_id, "kind": d.kind}, ensure_ascii=False)
            f.write(line + "\n")
            digest.update(line.encode("utf-8"))
            chars += len(d.text)
    tmp.replace(path)
    meta = {
        "path": str(path),
        "docs": len(docs),
        "seed": seed,
        "chars": chars,
        "sha256": digest.hexdigest(),
        "kinds": dict(Counter(d.kind for d in docs)),
        "bytes": path.stat().st_size,
    }
    path.with_name("manifest.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return meta


def _group_key(rec: dict) -> object:
    return int(rec["base_id"]) if int(rec["base_id"]) >= 0 else f"doc-{rec['doc_id']}"


def _select_groups(path: str | Path, limit: int) -> set:
    """第一遍扫描：按组大小贪心选组，直到累计篇数 >= limit，返回入选组。"""
    sizes: dict[object, int] = {}
    order: list[object] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            key = _group_key(json.loads(line))
            if key not in sizes:
                sizes[key] = 0
                order.append(key)
            sizes[key] += 1
    chosen: set = set()
    total = 0
    for k in order:
        if total >= limit:
            break
        chosen.add(k)
        total += sizes[k]
    return chosen


def load_corpus(path: str | Path, limit: int | None = None, by_group: bool = True) -> list[Doc]:
    """读取前 limit 篇（limit=None 读全部）。

    by_group=True（默认）：按「整组」取样 —— 一个 base 组的 orig/exact/near/border 必须一起进来。
    语料生成时整体洗牌过，直接取文件前 N 行会把一个组的 3 篇切散，子集里几乎没有完整重复对，
    recall 会被算成 0（1K 试验踩过这个坑）。所以先扫一遍统计组大小、按组选够 limit 篇，
    再扫第二遍把入选组的全部文档读出来（200K 篇两遍约 6s）。
    """
    if limit is not None and by_group:
        keys = _select_groups(path, limit)
        out: list[Doc] = []
        with open(path, encoding="utf-8") as f:
            for line in f:
                r = json.loads(line)
                if _group_key(r) in keys:
                    out.append(Doc(r["doc_id"], r["text"], int(r["base_id"]), r["kind"]))
        return out

    out = []
    with open(path, encoding="utf-8") as f:
        for i, line in enumerate(f):
            if limit is not None and i >= limit:
                break
            r = json.loads(line)
            out.append(Doc(r["doc_id"], r["text"], int(r["base_id"]), r["kind"]))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--docs", type=int, default=200000)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--out", default="/tmp/raytest/corpus/corpus_200k.jsonl")
    ap.add_argument("--inspect", default="")
    args = ap.parse_args()

    if args.inspect:
        docs = load_corpus(args.inspect)
        print(json.dumps({"docs": len(docs), "chars": sum(len(d.text) for d in docs),
                          "kinds": dict(Counter(d.kind for d in docs))},
                         ensure_ascii=False, indent=2))
        return

    meta = dump_corpus(args.out, args.docs, args.seed)
    print(json.dumps(meta, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
