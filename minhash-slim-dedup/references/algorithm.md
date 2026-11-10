# 瘦身 MinHash 流水线：设计、数字、扩展点

代码：`~/code/raytest/dedup_slim.py`（流水线）、`minhash.py`（签名/分带/估计）、`simhash.py`（shingles）。
质量评估复用 `run_experiment.evaluate`，与阶段 A–D 同一套口径。

## 0. 语料边界

| 快照 | 规模 | 用途 |
|---|---|---|
| `corpus_200k.jsonl` | 199,998 篇 / 61.3M 字 | 阶段 A–E（1K / 10K / 100K 子集） |
| `corpus_1m.jsonl` | 1,000,002 篇 / 306.6M 字 / 880.9 MB | 阶段 F（`subset_1000k`） |

1M 是**新快照**（200K 里没有 100 万篇），kinds：orig 333,334 / exact 333,334 / near 266,667 /
neg 33,334 / border 33,333。两份并存、互不影响；跨快照比指标没有意义。
`subset_1000k` = 1,000,000 篇 / 366,666 组 / `doc_ids_sha256 cd31d53b…`。

## 1. 数据流

```
load_subset(uri, table)                      # LanceDB 表 -> Doc(doc_id, text, base_id, kind)
  └─ exact_aggregate(docs)                   # 去空白 -> sha1 -> 类；代表 = 类内最小 doc_id
       └─ sign_{local,ray}(canon)            # k=128 minhash 签名，uint32 (U,k)
            └─ band_rows_of(sigs)            # 12 带 × U 行 -> (band_key int64, uid int32)
                 └─ scan_buckets(...)        # argsort(band_key) + 连续扫描，桶内估计 Jaccard
                      └─ components(edges)   # numpy 并查集，根 = 分量内最小 uid
                           └─ to_decision()  # 代表级 -> 原始 doc_id 级 kept/dropped/边
                                └─ evaluate()# 参考口径 + 召回分解
```

### 1.1 阶段 0：精确预聚合

- 归一化只做一件事：`re.sub(r"\s+", "", text)`。本语料的 exact 副本只差换行/空格；
  换别的归一化（全半角、大小写）会改变「唯一数」，进而改变所有下游数字。
- `sha1(归一化文本)` → 类；`alias[类代表] = 类内全部 doc_id`（升序）。
- 1M 上 1,000,000 → 666,506 个唯一签名（33.3% 精确重复），这一步 8.2s。

### 1.2 阶段 1/2：签名与瘦分带行

- 签名：每个 shingle 一次 `blake2b(8B) → 31bit`，再 k 组仿射置换 `(a·h+b) mod (2^31−1)` 取最小值。
  `P=2^31−1` 保证 `a·h < 2^62`，uint64 不溢出；`seed=7` 固定，跨进程可复现。
- 分带：`band_key = (band << 40) | (blake2b(chunk)[:8] 的低 40 位)`，`chunk` 是该带 r 个签名值。
  **必须 blake2b**：Python `hash()` 按进程随机化，多 worker 会算出不同桶，召回静默变 0。
- 瘦行：每条带行只存 `(band_key, uid)` = 8 B + 4 B = **12 B**；fat 版每条带行还塞 512 B 全签名（528 B/行）。
- 注意 `b·r ≤ k`（12×10=120 ≤ 128）；带外的那 8 位不参与分带，但仍参与 Jaccard 估计。

### 1.3 阶段 3：排序 + 连续扫描

- `np.argsort(keys, kind="stable")`，`flatnonzero` 找桶边界，线性扫；不做 `groupby` 物化。
- 桶内：`estimate_many(sig, others) = count(others == sig) / k`，用**全部 k 位**。
- 桶 > `pivot_over`（默认 64）时改「一个 pivot 打星」：只连 `idx[0]` 与其余，比较数从 C(m,2) 降到 m−1。
  代价是丢传递边 —— 1M 上 83 个桶触发、丢 76 条真值对、多留 7 篇。
- 1M 实测：5,889,424 个桶、平均 1.358、最大 411、比较 3,635,992 次、canonical 边 1,917,310。

### 1.4 阶段 4/5：并查集与决策换算

- `components()`：父指针 + 路径压缩，合并时让小根当父 → 根 = 分量内最小 uid。
- `to_decision()`：分量内先按类取最小 doc_id 作为存活代表（`surv[root]`），类内其余成员都算丢给该代表；
  分量内其它类全部丢给同一个代表。这与 fat 版「并查集根取最小 doc_id」等价，是 kept 能逐字节相同的原因。
- 候选边按类展开成文档级边，交给 `evaluate` 算成对指标（`true_pairs` 等）。

### 1.5 召回分解（最容易误判的一步）

`pair_recall` 的分母包含 `(orig, exact)` 这类**同一个精确类内部**的真值对。预聚合后它们被并成一类，
天然没有边，所以 slim 的 `pair_recall` 会「假性下跌」：

```
1M: true_pairs 866,668, intra_canonical 333,654, edge_tp 532,938
    pair_recall            = 532,938 / 866,668      = 0.6149   ← 不可比
    pair_recall_edge_canon = 532,938 / (866,668−333,654) = 0.9999  ← 可比口径
```

报告里两个数都写，判读只用后者；`true_pairs_intra_canonical` 明确记下扣掉了多少对。

## 2. 1M 实测（同一 `subset_1000k`）

| run | payload | compare | 核 | 耗时 | 中间态 | 桶内比较 | kept |
|---|---|---|---|---|---|---|---|
| F1 fat minhash | 528 B/行 | Ray `groupby`+`map_groups` | 20 | 1,227.6s | 6.34 GB（12.0M 行） | 7,838,468（band_hits） | 383,708 |
| F2b slim local | 12 B/行 | 排序扫描，全程两两 | 1 | 259.2s | 96.0 MB（8.0M 行） | 3,635,992 | 383,708 |
| F4b slim+Ray 签名 | 12 B/行 | 同上，pivot 关 | 20 | **78.7s** | 96.0 MB | 3,635,992 | 383,708 |
| F4 slim+星形 | 12 B/行 | 同上，桶>64 打星 | 20 | 78.5s | 96.0 MB | 2,988,269 | 383,715 |

- 中间态 6.34 GB → 96 MB（66×），耗时 15.6×；**单核 slim 比 20 核 fat 快 4.7×** —— fat 的成本是每桶固定开销，
  跟并行度无关（core-second：≈24,500 → ≈460，53×）。
- 决策完全一致：F2b/F4b 的 kept/dropped = 383,708 / 616,292，与 F1 逐个相等；
  `groups_missing / components_merged / edge_fp_cross_base / neg_false_drop` 全 0；灰区 10,758 = F1 同值。
- 阶段耗时（ray）：load 4.1 / exact 8.2 / sign 20.2 / band_rows 8.3 / scan 19.8 / uf 2.6 / eval 15.4。
- 签名成本 0.31 ms/篇（单核），1M 单核 201s；Ray 20 核 20.2s。

## 3. 成本模型（用来预判，不要用来下结论）

- 中间态字节：`rows = b × U`，`bytes = rows × 12`。
  1M：12 × 666,506 × 12 B = 96.0 MB（逐字节命中模型）。
- 比较次数：`Σ_buckets C(m,2)`（pivot 桶为 `m−1`）。随机桶假设给的是下界，
  真实语料的有效键熵可能远低于 40 bit —— simhash 8×16bit 在 1M 上只用了 33% 桶空间，候选对是模型的 87×。
- 扫描成本 ≈ 3 µs/次比较 + **每桶 ~3.5 µs** 的 Python 循环开销；1M 上 5.9M 个桶 = 20s，
  所以下一步优化是「桶内批量向量化」（`np.repeat` + 分块广播），不是换算法。
- 签名/指纹：minhash 0.31 ms/篇；simhash 批处理 0.53 ms/篇/核（纯 Python 单机 2.65 ms/篇是另一个口径）。

## 4. 参数与取舍

| 参数 | 默认 | 说明 |
|---|---|---|
| `k` | 128 | 估计标准差 `sqrt(J(1−J)/k)`；J≈0.9 时 σ≈0.027 |
| `bands` × `rows` | 12 × 10 | 50% 碰撞点 `t ≈ (1/b)^(1/r)` = 0.816；`b·r ≤ k` |
| `threshold` | 0.8 | 10K 上 0.85 精度 0.9819/压并 0.9254；0.8 精度 0.9644/压并 0.9493 |
| `pivot_over` | 64 | 1M 上应当给 `1000000000`（关掉）；只有桶里几千篇时才需要 |
| `shingle` | 3 | 字符 3-gram，与 simhash 同一套 |
| `seed` | 7 | 决定置换与签名；换了就不是同一批结果 |

10K 上的算法对比（同一 shingle / 同一并查集 / `verify=0`）：

| 配置 | 召回 | 精度 | 跨组误边 | 组压成 1 篇 |
|---|---|---|---|---|
| minhash 12×10 t=0.8 | 1.0000 | 0.9644 | 0 | 0.9493 |
| minhash 12×10 t=0.85 | 1.0000 | 0.9819 | 0 | 0.9254 |
| simhash hamming 12 | 0.9689 | 0.9802 | **28** | 0.8828 |

## 5. 扩展点（按收益/成本排序）

1. **桶内向量化**（P0）：把 `scan_buckets` 的逐桶 Python 循环换成「按桶大小分组 → `np.repeat` 展开 →
   分块广播比较」。1M 上每桶 3.5 µs × 5.9M ≈ 20s，目标是压到 2–3s。
2. **按带分区**（600M 必需）：`band_key` 排序后按哈希区间切分区，分区内独立扫描 + 局部并查集，
   最后小规模合并分量。中间态 57.6 GB 必须落盘，不能全进内存。
3. **签名分布式化**：现在 `sign_ray` 只把签名放进 Ray，分带/扫描仍在驱动进程；
   600M 要把 band_rows 也做成 Ray dataset（`MinHashSigner` 已支持 `progress_dir`）。
4. **pivot 策略**：只有 `max_bucket` 上千时才开，且要记账「丢了多少边、多留几篇」。
5. **换估计器**（如 b-bit minhash）：只需替换 `signature` / `estimate_many`，
   但带键必须继续用「能唯一化桶」的稳定哈希。

改任何一条，都要先过「决策逐项相等 + groups_missing=0 + 可比召回」三道线，否则不算成功。
