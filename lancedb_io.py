"""LanceDB 侧的数据进出口：语料入库、按组取子集、去重结果回写。

为什么要有这一层：
  * 输入统一走 LanceDB —— 200K 快照只入库一次（`corpus` 表），任何规模实验都用
    「整组子集表」（`subset_10k` 等）喂给 Ray，Ray 侧用 `ray.data.read_lance` 直接扫
    Lance 数据集，不再把 10K 篇文本 pickle 进 driver 参数。
  * 输出统一走 LanceDB —— 每篇的 kept/代表文档、候选边、本次 run 的配置与指标分别落到
    `dedup_result` / `dedup_edges` / `dedup_runs`，可以直接用 duckdb/pandas 复核，
    不用再从 json 报告里二次解析。
  * 组完整性是铁律：子集按「整组」选（同 corpus_io.load_corpus），子集里必须有完整的
    orig/exact/near 对，否则 recall 恒为 0。这里把同一套选组逻辑放在 LanceDB 上实现，
    并把 doc_id 摘要写进 `catalog`，事后能证明 LanceDB 路径和 jsonl 路径取到的是同一批文档。

表结构:
  corpus         doc_id int64, text string, base_id int64, kind string, group_key string
  subset_<n>     与 corpus 同构，只含选中的整组
  catalog        table_name, kind(corpus/subset), docs, chars, sha256(源 jsonl), doc_ids_sha256, ...
  dedup_result   run_id, mode, doc_id, base_id, kind, kept bool, rep_id int64
  dedup_edges    run_id, mode, a, b
  dedup_runs     run_id, mode, 配置/指标各列 + metrics_json（append，历史可查）

用法:
  python lancedb_io.py --uri /tmp/raytest/lance --ingest --src /tmp/raytest/corpus/corpus_200k.jsonl
  python lancedb_io.py --uri /tmp/raytest/lance --materialize 10000
  python lancedb_io.py --uri /tmp/raytest/lance --inspect
  python lancedb_io.py --uri /tmp/raytest/lance --inspect --table dedup_result
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from collections import Counter
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from gen_corpus import Doc

DEFAULT_URI = os.environ.get("RAYTEST_LANCE_ROOT", "/tmp/raytest/lance")
CORPUS_TABLE = "corpus"
CATALOG_TABLE = "catalog"

CORPUS_SCHEMA = pa.schema([
    ("doc_id", pa.int64()),
    ("text", pa.string()),
    ("base_id", pa.int64()),
    ("kind", pa.string()),
    ("group_key", pa.string()),
])

CATALOG_SCHEMA = pa.schema([
    ("table_name", pa.string()),
    ("kind", pa.string()),
    ("docs", pa.int64()),
    ("chars", pa.int64()),
    ("sha256", pa.string()),          # 源 jsonl 快照的 sha256（200K 那份）
    ("doc_ids_sha256", pa.string()),  # 子集 doc_id 集合的摘要（和 jsonl 路径选出的集合可比）
    ("subset_limit", pa.int64()),
    ("by_group", pa.bool_()),
    ("source_table", pa.string()),
    ("source_path", pa.string()),
    ("created_at", pa.string()),
    ("note", pa.string()),
])


def group_key(base_id: int, doc_id: int) -> str:
    """一个 base 组的所有成员共享同一个 key；neg 每篇自成一组。

    与 corpus_io._group_key 同语义（那边用 f"doc-{doc_id}"，这里用 "d:" 前缀，只影响字符串本身，
    不影响分组结果）。
    """
    return f"g:{base_id}" if int(base_id) >= 0 else f"d:{doc_id}"


def table_path(uri: str, name: str) -> str:
    """LanceDB 里一张表就是一个 <name>.lance 目录，Ray Data 需要这份路径。"""
    return str(Path(uri) / f"{name}.lance")


def connect(uri: str = DEFAULT_URI):
    import lancedb

    Path(uri).mkdir(parents=True, exist_ok=True)
    return lancedb.connect(uri)


# ---------------------------------------------------------------- 入库
def _iter_jsonl_batches(src: str | Path, batch: int = 20000, stats: dict | None = None):
    """逐批读 jsonl，返回 (arrow 表, 累计统计)。

    顺带算一遍「语料摘要」：注意 corpus_io.dump_corpus 当初是对 `line`（不含换行）累加哈希的，
    所以这里也必须按「去掉行尾换行」的方式累加，否则同一份文件会算出两个 sha256，
    和 RESULTS.md / manifest.json 里的 53518f48… 对不上。最终值写进 stats["sha256"]（生成器没法 return）。
    """
    chars = 0
    kinds: Counter = Counter()
    digest = hashlib.sha256()
    rows = {k: [] for k in ("doc_id", "text", "base_id", "kind", "group_key")}
    n = 0
    total = 0
    with open(src, "rb") as fb:
        for raw in fb:
            digest.update(raw[:-1] if raw.endswith(b"\n") else raw)
            rec = json.loads(raw)
            rows["doc_id"].append(int(rec["doc_id"]))
            rows["text"].append(rec["text"])
            rows["base_id"].append(int(rec["base_id"]))
            rows["kind"].append(rec["kind"])
            rows["group_key"].append(group_key(int(rec["base_id"]), int(rec["doc_id"])))
            chars += len(rec["text"])
            kinds[rec["kind"]] += 1
            total += 1
            n += 1
            if n >= batch:
                yield pa.table(rows, schema=CORPUS_SCHEMA), dict(
                    n=total, chars=chars, kinds=dict(kinds))
                rows = {k: [] for k in ("doc_id", "text", "base_id", "kind", "group_key")}
                n = 0
    if n:
        yield pa.table(rows, schema=CORPUS_SCHEMA), dict(n=total, chars=chars, kinds=dict(kinds))
    if stats is not None:
        stats["sha256"] = digest.hexdigest()
        stats["n"] = total


def sha256_file(path: str | Path, chunk: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            buf = f.read(chunk)
            if not buf:
                break
            digest.update(buf)
    return digest.hexdigest()


def ingest_corpus(uri: str = DEFAULT_URI, src: str = "/tmp/raytest/corpus/corpus_200k.jsonl",
                  table: str = CORPUS_TABLE, batch: int = 20000, force: bool = False) -> dict:
    """jsonl 快照 -> LanceDB `corpus` 表。已存在则直接复用（一份语料只入库一次）。"""
    db = connect(uri)
    if not force and table in _table_names(db):
        t = db.open_table(table)
        meta = _catalog_get(db, table) or {}
        n = t.count_rows()
        print(f"[lance] 复用已有 {table}: {n} 行 ({table_path(uri, table)})")
        return {**meta, "docs": n, "reused": True}
    if force and table in _table_names(db):
        db.drop_table(table)

    t0 = time.perf_counter()
    tbl = None
    info: dict = {}
    stats: dict = {}
    for i, (batch_tbl, info) in enumerate(_iter_jsonl_batches(src, batch, stats)):
        if tbl is None:
            tbl = db.create_table(table, data=batch_tbl, mode="overwrite")
        else:
            tbl.add(batch_tbl)
        print(f"[lance] 写入 corpus 分片 {i + 1}: 累计 {info['n']} 行")
    dt = time.perf_counter() - t0
    meta = {
        "table_name": table,
        "kind": "corpus",
        "docs": info["n"],
        "chars": info["chars"],
        "sha256": stats.get("sha256") or sha256_file(src),
        "doc_ids_sha256": None,
        "subset_limit": None,
        "by_group": None,
        "source_table": None,
        "source_path": str(src),
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "note": json.dumps({"kinds": info["kinds"], "ingest_sec": round(dt, 2),
                            "sha256_raw_bytes": sha256_file(src)}, ensure_ascii=False),
    }
    manifest = Path(src).with_name("manifest.json")
    if manifest.exists():
        want = json.loads(manifest.read_text()).get("sha256")
        if want and want != meta["sha256"]:
            raise SystemExit(f"语料摘要不一致: LanceDB 侧 {meta['sha256'][:16]}… != manifest {want[:16]}…")
        print(f"[lance] 语料摘要与 manifest 一致: {meta['sha256'][:16]}…")
    _catalog_put(db, meta)
    print(f"[lance] corpus 入库完成: {info['n']} 行 / {info['chars'] / 1e6:.1f}M 字 "
          f"/ {dt:.1f}s -> {table_path(uri, table)}")
    return meta


# ---------------------------------------------------------------- 子集
def select_group_keys(doc_ids: np.ndarray, base_ids: np.ndarray, limit: int) -> set[str]:
    """按组大小贪心选组，直到累计篇数 >= limit（与 corpus_io._select_groups 同语义）。

    语料整体洗牌过，直接取前 N 行会把一个组的 orig/exact/near 切散，子集里没有完整重复对，
    recall 会恒为 0。所以必须先扫一遍分组、按「首次出现顺序」把整组选进来。
    """
    sizes: dict[str, int] = {}
    order: list[str] = []
    for doc_id, base_id in zip(doc_ids.tolist(), base_ids.tolist()):
        k = group_key(base_id, doc_id)
        if k not in sizes:
            sizes[k] = 0
            order.append(k)
        sizes[k] += 1
    chosen: set[str] = set()
    total = 0
    for k in order:
        if total >= limit:
            break
        chosen.add(k)
        total += sizes[k]
    return chosen


def subset_table_name(limit: int) -> str:
    return f"subset_{limit // 1000}k" if limit % 1000 == 0 else f"subset_{limit}"


def prepare_subset(uri: str = DEFAULT_URI, limit: int = 10000, table: str | None = None,
                   source: str = CORPUS_TABLE, force: bool = False) -> dict:
    """从 corpus 表按整组取出子集，物化成 `subset_<n>` 表（已存在且参数一致则复用）。"""
    db = connect(uri)
    table = table or subset_table_name(limit)
    names = _table_names(db)
    if source not in names:
        raise SystemExit(f"corpus 表不存在: {source}（先 --ingest）")
    src_meta = _catalog_get(db, source) or {}
    if not force and table in names:
        meta = _catalog_get(db, table) or {}
        if meta.get("subset_limit") == limit and meta.get("sha256") == src_meta.get("sha256"):
            n = db.open_table(table).count_rows()
            print(f"[lance] 复用子集 {table}: {n} 行")
            return {**meta, "docs": n, "reused": True}

    t0 = time.perf_counter()
    src = db.open_table(source)
    light = src.to_lance().to_table(columns=["doc_id", "base_id"])
    doc_ids = light["doc_id"].to_numpy(zero_copy_only=False)
    base_ids = light["base_id"].to_numpy(zero_copy_only=False)
    keys = select_group_keys(doc_ids, base_ids, limit)
    key_arr = pa.array([group_key(b, d) for d, b in zip(doc_ids.tolist(), base_ids.tolist())])
    mask = pc.is_in(key_arr, value_set=pa.array(sorted(keys)))
    sub = src.to_lance().to_table().filter(mask)   # 整表 200K 行约 175MB，读一遍后只留子集
    sub = sub.select(CORPUS_SCHEMA.names).cast(CORPUS_SCHEMA)
    if table in names:
        db.drop_table(table)
    db.create_table(table, data=sub, mode="overwrite")
    dt = time.perf_counter() - t0

    ids = sorted(int(x) for x in sub["doc_id"].to_pylist())
    digest = hashlib.sha256(",".join(map(str, ids)).encode()).hexdigest()
    chars = int(pc.sum(pc.utf8_length(sub["text"])).as_py())
    kinds = Counter(sub["kind"].to_pylist())
    meta = {
        "table_name": table,
        "kind": "subset",
        "docs": len(ids),
        "chars": chars,
        "sha256": src_meta.get("sha256"),
        "doc_ids_sha256": digest,
        "subset_limit": limit,
        "by_group": True,
        "source_table": source,
        "source_path": src_meta.get("source_path"),
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "note": json.dumps({"groups": len(keys), "kinds": dict(kinds),
                            "materialize_sec": round(dt, 2)}, ensure_ascii=False),
    }
    _catalog_put(db, meta)
    print(f"[lance] 子集 {table}: {len(ids)} 篇 / {chars / 1e6:.2f}M 字 / {len(keys)} 组 "
          f"/ {dt:.1f}s -> {table_path(uri, table)}")
    return meta


def load_subset(uri: str = DEFAULT_URI, table: str = "subset_10k") -> list[Doc]:
    """把子集表读成 Doc 列表（单机基线、指标评估、verify 复核都用它）。"""
    db = connect(uri)
    t = db.open_table(table)
    cols = t.to_lance().to_table(columns=["doc_id", "text", "base_id", "kind"]).to_pydict()
    return [Doc(int(d), txt, int(b), k) for d, txt, b, k in
            zip(cols["doc_id"], cols["text"], cols["base_id"], cols["kind"])]


# ---------------------------------------------------------------- 结果回写
RESULT_SCHEMA = pa.schema([
    ("run_id", pa.string()),
    ("algo", pa.string()),      # simhash / minhash：两个算法的结果同表累积，靠这一列区分
    ("mode", pa.string()),
    ("doc_id", pa.int64()),
    ("base_id", pa.int64()),
    ("kind", pa.string()),
    ("kept", pa.bool_()),
    ("rep_id", pa.int64()),
])

EDGE_SCHEMA = pa.schema([
    ("run_id", pa.string()),
    ("algo", pa.string()),
    ("mode", pa.string()),
    ("a", pa.int64()),
    ("b", pa.int64()),
])

RUN_COLUMNS = [
    ("run_id", pa.string()), ("algo", pa.string()), ("mode", pa.string()),
    ("created_at", pa.string()),
    ("docs", pa.int64()), ("chars", pa.int64()), ("bits", pa.int64()),
    ("band_bits", pa.int64()), ("hamming", pa.int64()), ("shingle", pa.int64()),
    ("bands", pa.int64()), ("rows", pa.int64()), ("threshold", pa.float64()),
    ("verify", pa.float64()), ("corpus_sha256", pa.string()),
    ("subset_table", pa.string()), ("subset_sha256", pa.string()),
    ("edges", pa.int64()), ("band_hits", pa.int64()), ("kept", pa.int64()),
    ("dropped", pa.int64()), ("pair_recall", pa.float64()), ("pair_precision", pa.float64()),
    ("edge_fp_cross_base", pa.int64()), ("groups_collapsed_rate", pa.float64()),
    ("groups_extra_kept", pa.int64()), ("groups_missing", pa.int64()),
    ("components_merged", pa.int64()), ("neg_false_drop_rate", pa.float64()),
    ("border_dropped", pa.int64()), ("border_total", pa.int64()),
    ("seconds", pa.float64()), ("docs_per_sec", pa.float64()),
    ("pools", pa.string()), ("metrics_json", pa.string()),
]
RUN_SCHEMA = pa.schema(RUN_COLUMNS)


def write_results(uri: str, run_id: str, docs: list[Doc], results: dict[str, dict],
                  config: dict, mode_metrics: dict[str, dict] | None = None) -> dict:
    """把去重结果写进 LanceDB。

    docs:    本次子集的全部文档（带 base_id/kind，便于直接 SQL 复核）
    results: {"ray": {"kept": [...], "dropped": {doc_id: rep}, "pairs": ...}, ...}
    config:  本次 run 的配置（bits/band_bits/hamming/shingle/verify/corpus_sha256/...）
    返回 {"tables": {...}, "rows": {...}}。
    """
    db = connect(uri)
    mode_metrics = mode_metrics or {}
    names = _table_names(db)
    created = time.strftime("%Y-%m-%dT%H:%M:%S")

    algo = str(config.get("algo") or "simhash")
    res_rows = {k: [] for k in ("run_id", "algo", "mode", "doc_id", "base_id", "kind", "kept", "rep_id")}
    edge_rows = {k: [] for k in ("run_id", "algo", "mode", "a", "b")}
    run_rows = {name: [] for name, _ in RUN_COLUMNS}

    base = {int(d.doc_id): int(d.base_id) for d in docs}
    kind = {int(d.doc_id): d.kind for d in docs}
    ids = [int(d.doc_id) for d in docs]

    for mode, res in results.items():
        kept = {int(i) for i in res["kept"]}
        dropped = {int(k): int(v) for k, v in (res.get("dropped") or {}).items()}
        for i in ids:
            res_rows["run_id"].append(run_id)
            res_rows["algo"].append(algo)
            res_rows["mode"].append(mode)
            res_rows["doc_id"].append(i)
            res_rows["base_id"].append(base[i])
            res_rows["kind"].append(kind[i])
            res_rows["kept"].append(i in kept)
            res_rows["rep_id"].append(i if i in kept else dropped.get(i, i))
        pairs = res.get("pairs")
        if pairs is not None:
            # ray 给的是 (n,2) numpy 数组，local 给的是 set[(a,b)] —— 两种都要能落表
            seq = sorted(pairs) if isinstance(pairs, (set, frozenset)) else pairs
            arr = (np.asarray(seq, dtype=np.int64).reshape(-1, 2) if len(seq)
                   else np.empty((0, 2), dtype=np.int64))
            edge_rows["run_id"].extend([run_id] * len(arr))
            edge_rows["algo"].extend([algo] * len(arr))
            edge_rows["mode"].extend([mode] * len(arr))
            edge_rows["a"].extend(arr[:, 0].tolist())
            edge_rows["b"].extend(arr[:, 1].tolist())

        m = mode_metrics.get(mode, {})
        row = {
            "run_id": run_id, "algo": algo, "mode": mode, "created_at": created,
            "docs": len(ids), "chars": int(sum(len(d.text) for d in docs)),
            "bits": config.get("bits"), "band_bits": config.get("band_bits"),
            "hamming": config.get("hamming"), "shingle": config.get("shingle"),
            "bands": config.get("bands"), "rows": config.get("rows"),
            "threshold": config.get("threshold"),
            "verify": float(config.get("verify") or 0.0),
            "corpus_sha256": config.get("corpus_sha256"),
            "subset_table": config.get("subset_table"),
            "subset_sha256": config.get("subset_sha256"),
            "edges": m.get("edges"), "band_hits": m.get("band_hits"),
            "kept": m.get("kept"), "dropped": m.get("dropped"),
            "pair_recall": m.get("pair_recall"), "pair_precision": m.get("pair_precision"),
            "edge_fp_cross_base": m.get("edge_fp_cross_base"),
            "groups_collapsed_rate": m.get("groups_collapsed_rate"),
            "groups_extra_kept": m.get("groups_extra_kept"),
            "groups_missing": m.get("groups_missing"),
            "components_merged": m.get("components_merged"),
            "neg_false_drop_rate": m.get("neg_false_drop_rate"),
            "border_dropped": m.get("border_dropped"), "border_total": m.get("border_total"),
            "seconds": m.get("seconds"), "docs_per_sec": m.get("docs_per_sec"),
            "pools": json.dumps(m.get("pools"), ensure_ascii=False) if m.get("pools") else None,
            "metrics_json": json.dumps(m, ensure_ascii=False, default=str),
        }
        for name, _ in RUN_COLUMNS:
            run_rows[name].append(row.get(name))

    _migrate_if_needed(db, "dedup_result", RESULT_SCHEMA)
    _migrate_if_needed(db, "dedup_edges", EDGE_SCHEMA)
    _migrate_if_needed(db, "dedup_runs", RUN_SCHEMA)
    _append_run(db, "dedup_result", pa.table(res_rows, schema=RESULT_SCHEMA), run_id)
    _append_run(db, "dedup_edges", pa.table(edge_rows, schema=EDGE_SCHEMA), run_id)
    new_runs = pa.table(run_rows, schema=RUN_SCHEMA)
    if "dedup_runs" in _table_names(db):
        (db.open_table("dedup_runs").merge_insert(["run_id", "mode"])
         .when_matched_update_all().when_not_matched_insert_all().execute(new_runs))
    else:
        db.create_table("dedup_runs", data=new_runs, mode="overwrite")

    rows = {"dedup_result": len(res_rows["doc_id"]), "dedup_edges": len(edge_rows["a"]),
            "dedup_runs": new_runs.num_rows}
    print(f"[lance] 结果写入 {uri}: " + ", ".join(f"{k}={v}" for k, v in rows.items()))
    return {"tables": {k: table_path(uri, k) for k in rows}, "rows": rows}


def _migrate_if_needed(db, name: str, schema: pa.Schema) -> None:
    """老表补列：结果表是累积的（历史 run 不能丢），但 schema 会随算法维度演进。

    缺列时「读出来 → 补默认值 → 重建表」，量级只有几万行，比在表上做 add_columns 更好控。
    """
    if name not in _table_names(db):
        return
    t = db.open_table(name)
    have = set(t.schema.names)
    missing = [f for f in schema if f.name not in have]
    if not missing:
        return
    df = t.to_lance().to_table().to_pandas()
    for f in missing:
        df[f.name] = "simhash" if f.name == "algo" else None
    rebuilt = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    db.drop_table(name)
    db.create_table(name, data=rebuilt, mode="overwrite")
    print(f"[lance] {name} 补列 {[f.name for f in missing]}（历史 {rebuilt.num_rows} 行保留）")


def _append_run(db, name: str, data: pa.Table, run_id: str) -> None:
    """按 run_id 幂等追加：先删掉同名 run 的旧行再写入，历史 run 保留可查。"""
    if name not in _table_names(db):
        db.create_table(name, data=data, mode="overwrite")
        return
    t = db.open_table(name)
    t.delete(f"run_id = '{run_id}'")
    t.add(data)


# ---------------------------------------------------------------- catalog
def _table_names(db) -> list[str]:
    listing = db.list_tables()
    return list(getattr(listing, "tables", listing))


def _catalog_get(db, name: str) -> dict | None:
    if CATALOG_TABLE not in _table_names(db):
        return None
    t = db.open_table(CATALOG_TABLE)
    rows = t.to_lance().to_table().to_pylist()
    hit = [r for r in rows if r["table_name"] == name]
    return hit[-1] if hit else None


def _catalog_put(db, meta: dict) -> None:
    row = {k: meta.get(k) for k in CATALOG_SCHEMA.names}
    data = pa.table({k: [v] for k, v in row.items()}, schema=CATALOG_SCHEMA)
    if CATALOG_TABLE not in _table_names(db):
        db.create_table(CATALOG_TABLE, data=data, mode="overwrite")
        return
    t = db.open_table(CATALOG_TABLE)
    t.delete(f"table_name = '{row['table_name']}'")
    t.add(data)


def read_table(uri: str, name: str, where: str | None = None):
    """按 SQL where 读回一张结果表（lancedb 的 to_arrow 不支持 filter，走 lance 数据集）。"""
    db = connect(uri)
    if name not in _table_names(db):
        raise SystemExit(f"表不存在: {name} @ {uri}")
    return db.open_table(name).to_lance().to_table(filter=where).to_pandas()


def verify_results(uri: str, run_id: str) -> dict:
    """从 LanceDB 回读本次 run 的结果，复核「写进去的东西」自洽且两模式一致。"""
    df = read_table(uri, "dedup_result", f"run_id = '{run_id}'")
    edges = read_table(uri, "dedup_edges", f"run_id = '{run_id}'")
    db = connect(uri)
    q = f"run_id = '{run_id}'"
    out = {
        "rows": int(len(df)),
        "modes": sorted(df["mode"].unique().tolist()) if len(df) else [],
        "kept_by_mode": {m: int(g["kept"].sum()) for m, g in df.groupby("mode")},
        "edges_by_mode": {m: int(len(g)) for m, g in edges.groupby("mode")} if len(edges) else {},
        "rows_by_mode": {m: int(len(g)) for m, g in df.groupby("mode")},
        "count_rows_by_mode": {m: int(db.open_table("dedup_result").count_rows(filter=f"{q} AND mode = '{m}'"))
                               for m in sorted(df["mode"].unique().tolist())} if len(df) else {},
        "kept_rows_by_mode": {m: int(db.open_table("dedup_result").count_rows(filter=f"{q} AND mode = '{m}' AND kept"))
                              for m in sorted(df["mode"].unique().tolist())} if len(df) else {},
    }
    if {"ray", "local"} <= set(out["modes"]):
        piv = df.pivot_table(index="doc_id", columns="mode", values="kept", aggfunc="first")
        out["ray_vs_local_kept_diff"] = int((piv["ray"].fillna(False) != piv["local"].fillna(False)).sum())
        prep = df.pivot_table(index="doc_id", columns="mode", values="rep_id", aggfunc="first")
        out["ray_vs_local_rep_diff"] = int((prep["ray"].fillna(-1) != prep["local"].fillna(-1)).sum())
    return out


# ---------------------------------------------------------------- CLI
def describe(uri: str, table: str = "", limit_rows: int = 3) -> dict:
    db = connect(uri)
    names = _table_names(db)
    out: dict = {"uri": uri, "tables": {}}
    for name in names:
        t = db.open_table(name)
        info = {"rows": t.count_rows(), "columns": [(f.name, str(f.type)) for f in t.schema],
                "catalog": _catalog_get(db, name)}
        if table and name == table:
            info["head"] = t.head(limit_rows).to_pylist()
        out["tables"][name] = info
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", default=DEFAULT_URI)
    ap.add_argument("--ingest", action="store_true")
    ap.add_argument("--src", default="/tmp/raytest/corpus/corpus_200k.jsonl")
    ap.add_argument("--materialize", type=int, default=0)
    ap.add_argument("--table", default="")
    ap.add_argument("--source", default="", help="物化子集时的来源表（默认 corpus）")
    ap.add_argument("--inspect", action="store_true")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--rows", type=int, default=3)
    ap.add_argument("--compare-jsonl", default="", help="把子集和 jsonl 路径选出的 doc_id 集合对比")
    args = ap.parse_args()

    if args.ingest:
        print(json.dumps(ingest_corpus(args.uri, args.src, args.table or CORPUS_TABLE, force=args.force),
                         ensure_ascii=False, indent=2))
    if args.materialize:
        print(json.dumps(prepare_subset(args.uri, args.materialize, args.table or None,
                                        source=args.source or CORPUS_TABLE, force=args.force),
                         ensure_ascii=False, indent=2))
    if args.compare_jsonl:
        from corpus_io import load_corpus

        meta = prepare_subset(args.uri, args.materialize or 10000, args.table or None)
        lance_ids = {d.doc_id for d in load_subset(args.uri, meta["table_name"])}
        jsonl_ids = {d.doc_id for d in load_corpus(args.compare_jsonl, limit=meta["subset_limit"])}
        print(json.dumps({"lance": len(lance_ids), "jsonl": len(jsonl_ids),
                          "same": lance_ids == jsonl_ids,
                          "only_lance": sorted(lance_ids - jsonl_ids)[:5],
                          "only_jsonl": sorted(jsonl_ids - lance_ids)[:5]},
                         ensure_ascii=False, indent=2))
    if args.inspect or not (args.ingest or args.materialize or args.compare_jsonl):
        print(json.dumps(describe(args.uri, args.table, args.rows), ensure_ascii=False, indent=2,
                         default=str))


if __name__ == "__main__":
    main()
