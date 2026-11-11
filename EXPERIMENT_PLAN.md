# 实验设计：同一批数据上 SimHash vs MinHash 批量文本去重

> 目标：在**同一份固定语料**上，把 simhash 与 minhash 放进**同一条 Ray 流水线骨架**里，
> 用同一套指标、同一套监控页面比较：召回、误杀、边界行为、候选对数量、吞吐、伸缩性。
>
> 执行顺序：先 simhash（1K 冒烟 → 1K 参数曲线 → 10K/100K），再 minhash（同规模同参数网格），
> 最后汇总成对比报告。

## 1. 要回答的问题

| # | 问题 | 对应指标 |
|---|---|---|
| Q1 | 谁更能召回「近重复」（1% 字符改写） | `dup_recall`、`recall_near` |
| Q2 | 谁更容易误杀「硬负样本」（同模板不同内容） | `neg_false_drop_rate` |
| Q3 | 对「边界样本」（换 1 句 + 打乱语序）的判定差异 | `border_dropped` |
| Q4 | LSH 索引的代价：候选对数量 / 桶内比较开销 | `candidate_pairs`、`edges`、各阶段耗时 |
| Q5 | Ray 并行是否线性：同样数据 1K→10K→100K→200K | `docs_per_sec`、加速比、面板阶段耗时 |
| Q6 | 分布式结果是否等于单机结果（正确性） | ray vs local 的 `kept`/`dropped` 集合 |

## 2. 数据：固定快照（关键设计）

两套算法必须跑**同一批字节**，所以语料只生成一次，落盘固化：

```
/tmp/raytest/corpus/corpus_200k.jsonl   199,998 篇 / 61.3M 字 / 175 MB
sha256 = 53518f48edf75cd3898aca835ea710bf19ba513ff639d0685f6caf5333638940
```

一行一篇 `{"doc_id","text","base_id","kind"}`，同目录 `manifest.json` 记录 sha256、seed、分布；
每次实验的报告里写入 `corpus_sha256`，事后可证明「两次跑的是同一批数据」。

类别（`gen_corpus.py` 按 seed=7 生成，已洗牌）：

| kind | 数量 | 含义 | 判定 |
|---|---|---|---|
| `orig` | 66,666 | 原始文档（每组 1 篇，代表保留） | 每组必须留 1 篇 |
| `exact` | 66,666 | 仅空白差异 | 必须判重 |
| `near` | 53,332 | ~2% 字符替换/增删（转述、OCR、转录误差） | 必须判重（召回主战场） |
| `border` | 6,667 | 换掉 1 句 + 打乱语序，同 base | 灰区，只报比例不判对错 |
| `neg` | 6,667 | 同模板不同内容，与某 orig 共享约一半句子 | **必须不判重**（误杀陷阱） |

规模阶梯用**同一快照的整组子集**：取 1,000 / 10,000 / 100,000 篇。
注意不能直接取文件前 N 行 —— 生成时整体洗牌过，一个组的 orig/exact/near 会被切到不同子集里，
子集里几乎没有完整重复对，recall 会恒为 0（1K 试验踩过）。`corpus_io.load_corpus` 先扫一遍统计组大小、
按组选够 N 篇、再扫第二遍取文档（总数会有 0~2 篇溢出，组完整性优先）。
两套算法在同一 N 上取到的是同一批文档，且 1K ⊂ 10K ⊂ 100K。

## 3. 算法与统一流水线

两者共用 `dedup_ray.py` 的 5 阶段骨架，只替换「指纹」与「比较」两步：

```
文档 → map_batches(指纹) → flat_map 切带 → groupby(band) → 桶内比较 → union-find → kept/dropped
        fingerprint          shuffle          compare        verify(可选)   resolve
```

| | SimHash | MinHash |
|---|---|---|
| 表示 | 128 bit：shingle 哈希按位加权投票 | 128 维签名：每个 shingle 取 128 个置换哈希的最小值 |
| 索引 | 切 8 段 × 16 bit 分带 | 分 b 带 × r 行（b·r = 128） |
| 候选 | 同段同值即入桶 | 同带同签名即入桶 |
| 判定 | 汉明距离 ≤ k | 估计 Jaccard ≥ t（或带碰撞即判重） |
| 参数 | k = 8/10/12/14 | (b,r) = (16,8)/(32,4)；t = 0.7/0.8 |

**公平性约束**（必须全部满足，否则对比无效）：

1. 同一 shingle 提取器（字符 3-gram，NFKC + 去标点空白，中文按字切）；
2. 同一份语料快照、同一 N、同一 seed；
3. 同一 union-find（分量取最小 doc_id 为代表）；
4. 同一 CPU 配额（Ray 22 CPU，`ActorPoolStrategy(size=22)`）、同一节点、同一时间窗内跑；
5. **都不开 Jaccard 后处理复核**（`--verify 0`）：minhash 的阈值本身就是判定规则，
   若给 simhash 也加 Jaccard 复核，两者的差异会被复核抹平，测的就不再是 LSH 本身。
   需要「上线口径」时另跑一组 `verify=0.8` 做对照。

## 4. 参数网格与执行顺序

| 阶段 | 数据量 | 内容 | 目的 |
|---|---|---|---|
| A 冒烟 | 1K | simhash 单点：128bit / 8 带×16bit / hamming 12，ray+local 双跑 | 流水线正确性、面板通路、ray≡local |
| B 曲线 | 1K | simhash k=8/10/12/14；minhash (16,8)(32,4) × t=0.7/0.8 | 质量-代价曲线，定工作点 |
| B+ 规模 | 10K | simhash：hamming 8/10/12 × verify 0/0.8，LanceDB 进出 | 规模效应、阈值安全区、Ray vs 单机 |
| C minhash | 1K / 10K | minhash：k=128，b×r 与 t 网格，ray+local 对拍 | 同口径与 simhash 对比、双算法同框 |
| D 规模 | 100K / 200K | 两算法各自最优点 | 吞吐、加速比、候选对增长 |
| D 复核 | 100K | 加 `verify=0.8` | 上线口径对照（可选） |

工作点初值来自 `calibration.json`（同语料 4K 抽样）：hamming 12 时 recall≈0.633、precision 1.0；
hamming 14 开始出现负样本误杀（neg_fp 1.2%），所以 simhash 默认取 **k=12**，8 带×16 bit。

## 5. 指标定义（每个 run 都产出）

**成对口径**（回答「LSH 索引好不好」）：

- `pair_recall` = 真同组文档对里被召回的比例（真值 = 同 base 且双方都是 orig/exact/near）；
- `pair_precision` = 候选边里真同组的比例；
- `edge_fp_cross_base` = 跨组误判边数（最危险，必须 0）；
- `edge_fp_border` = 同组但有一方是 border 的边（灰区，单列）；
- `edges` / `band_hits` = 去重后的候选对 / 分带命中次数（一对可命中多个带）。

**组口径**（回答「去重结果对不对」）：

- `groups_collapsed_rate` = 被压成恰好 1 篇的组比例；
- `groups_extra_kept` = 组内留下 >1 篇的组数（漏合并）；
- `groups_missing` = 一篇都没留下的组数（过度合并）；
- `components_merged` = 连通分量里出现多个组的个数（跨组串味）；
- `neg_false_drop_rate` = 硬负样本被误删比例（目标 0）；
- `border_dropped / border_total` = 灰区被删比例（只看趋势）；
- `survivor_is_orig_rate` = 幸存者是 orig 的比例（仅信息性：代表取最小 doc_id，与算法质量无关）。

**成本**：`seconds` / `docs_per_sec`（1K 不看吞吐，只看跑通）、`band_hits`、pool 预算。

**复现凭据**：`corpus_sha256`、`bits`、`band_bits`、`hamming`、`shingle`、`seed`、`verify`。

> 别用「dup 是否被删」当召回：代表文档取分量内最小 doc_id，`orig` 常常不是最小 id，
> 组被正确压成 1 篇时幸存者仍可能是 near/exact，该指标会把「谁活下来」误当成「有没有召回」
> （实测同一份结果：0.62 vs 0.97）。

## 6. 运行方式

集群：node1（22 CPU / 111 GiB）Ray 2.60，job submission 走 8265；监控页走 8501。

```bash
# 生成/复用固定语料（已生成，勿重复）
.venv/bin/python corpus_io.py --docs 200000 --out /tmp/raytest/corpus/corpus_200k.jsonl

# 阶段 A：1K 冒烟（simhash）
DOCS=1000 BITS=128 HAMMING=12 BAND_BITS=16 ./submit_experiment.sh \
    --corpus /tmp/raytest/corpus/corpus_200k.jsonl --mode both --verify 0
```

面板：`ssh -N -L 8501:127.0.0.1:8501 node1` → <http://127.0.0.1:8501/>
（阶段圆点、任务分配表、批次矩阵、吞吐折线、漏斗卡片、日志尾、result.json 指标卡）

## 7. 阶段 A（1K）验收标准

1. 5 个阶段在面板上依次点亮，`result.json` 落盘；
2. ray 与 local 的 19 个指标逐项相等（分布式不改变语义）；
3. `neg_false_drop_rate == 0` 且 `edge_fp_cross_base == 0`；
4. `groups_missing == 0`、`components_merged == 0`；
5. 记录端到端耗时与调度开销占比（1K 下预计开销 > 计算，仅作正确性冒烟）。

**阶段 A 已通过（2026-11-10 15:02）**：pair_recall 0.9683 / pair_precision 0.9794 / 跨组误边 0 /
负样本 0 误杀 / 85.6% 组压成 1 篇 / ray 5.95s ≈ local 2.86s×2（开销全在启动）。详见 `RESULTS.md`。

## 8. 已知风险与对策

| 风险 | 影响 | 对策 |
|---|---|---|
| 1K 太小，22 actor 启动开销占大头 | 吞吐数字无意义 | 只用于正确性；吞吐看 10K/100K |
| `near` 改写比例 2% 偏重，simhash 召回上限只有 ~0.63 | 两算法都偏低，差异被压缩 | 加一组 `near_ratio=0.005` 的轻改写语料作为补充通道 |
| MinHash 128 置换逐 shingle 哈希很贵 | 吞吐被指纹阶段拖垮 | 单置换 + 128 个奇偶乘加（一次 64bit 哈希推 128 维），或 numpy 批量 |
| 两者都容易在「同模板」语料上过拟合 | 结论不可外推 | 明确只在合成语料上下结论，真实语料另做抽样验证 |
| 面板无鉴权 | 安全 | 只经 SSH 隧道访问，不暴露公网 |

## 9. 阶段 B（10K + LanceDB 输入/输出）——设计变更与结果

原计划里阶段 B 是「1K 参数曲线」、阶段 C 才是 10K。实际执行改成**直接在 10K 上做参数与复核的
四组对照**（1K 的参数曲线信息量很低：22 个 actor 喂不满，跨组误判也还没长出来），
同时把数据进出的底座换成 LanceDB。

### 9.1 数据进出（新增 `lancedb_io.py`）

| 环节 | 旧 | 新 | 说明 |
|---|---|---|---|
| 语料 | `/tmp/raytest/corpus/corpus_200k.jsonl` | `corpus.lance`（一次性入库） | 快照仍然只生成一次，入库时按 manifest 口径校验 sha256 |
| 取子集 | `corpus_io.load_corpus(limit=N)` 两遍扫描 jsonl | `subset_<n>.lance`（按整组物化） | 与 jsonl 路径逐 doc_id 相等（1K/10K 都验过） |
| Ray 输入 | `rd.from_items(list[dict])` | `rd.read_lance("subset_10k.lance")` | driver 不再搬运 10K 篇文本；行数校验兜底 |
| 结果 | `/tmp/raytest/result.json` | `dedup_result` / `dedup_edges` / `dedup_runs` | 逐篇 kept/rep_id + 边 + 每次 run 的配置与指标 |
| 诊断 | 读 json / 打印 | `analyze_run.py` 直接查表 | 合并分量、跨组边的 Jaccard、按 kind 的 kept |

口径不变的部分：同一 shingle、同一 union-find、同一 `evaluate()`、同一进度上报，
`corpus_sha256` 与 `subset_sha256` 都写进报告，`run_id` 作为结果表的主键维度。
算法维度用 `algo` 列（`simhash` / `minhash`）贯穿 `progress.json` → `result.json` → LanceDB 三张表，
面板据此切换阶段词表与参数卡（minhash：`bands`/`rows`/`threshold`；simhash：`bits`/`band_bits`/`hamming`）。

### 9.2 四组对照（10,002 篇 / 3,472 组，真值对 9,004）

| run | hamming | verify | recall | precision | 跨组误边 | 合并分量 | 丢失组 | kept | 耗时 |
|---|---|---|---|---|---|---|---|---|---|
| B1 | 12 | 0 | 0.9689 | 0.9802 | 28 | 7 | 7 | 3,872 | 12.55s |
| B2 | 12 | 0.8 | 0.9689 | 0.9867 | 0 | 0 | 0 | 3,895 | 13.39s |
| B3 | 10 | 0 | 0.9205 | 0.9909 | 0 | 0 | 0 | 4,134 | 12.66s |
| B4 | 8 | 0 | 0.8101 | 0.9970 | 0 | 0 | 0 | 4,658 | 12.73s |

### 9.3 结论（对后续 minhash 对照的约束）

1. **工作点要按 10K 重新定**：hamming=12 在 1K 上看着干净（跨组边 0），10K 上就冒出 28 条
   （Jaccard 0.29–0.42，汉明全部 =12）。凡是「1K 上跨组边 0」的结论都不能当作阈值安全的证据。
2. **simhash 的对照口径改成两组**：算法对比仍用 `verify=0`（B1/B3/B4 的 hamming 曲线）；
   「上线口径」用 B2（hamming=12 + `verify=0.8`），minhash 也要按同样的两种口径各跑一遍，
   否则 compare 阶段的对齐不公平（给 simhash 加复核、不给 minhash 加，会把差异抹平）。
3. **吞吐口径**：10K 起 Ray 才有意义（797 docs/s vs 单机 356 docs/s）；1K 的 168 docs/s 不能外推。
4. **100K 之前先算候选边规模**：`band_hits` 45,198（≈4.5/篇×8 带），桶内比较随桶大小平方增长，
   100K 需要先看 `band_hits` 与最大桶的分布，再决定是否要抬高 band 数或换 32bit 带。

## 10. 阶段 C：MinHash 落地（1K → 10K）

### 10.1 实现要点

| 环节 | 做法 | 为什么 |
|---|---|---|
| 签名 | 一次 31bit blake2b + 128 组仿射置换 `(a·h+b) mod (2^31-1)`，按位置取最小 | 逐 shingle 调 k 次哈希会把指纹阶段拖成瓶颈；实测 0.30 ms/篇（simhash 2.65 ms/篇） |
| 存储 | 签名 = 128×uint32 = 512B，bytes 列随 band 行走 | compare 阶段要算「全部 k 位」的估计 Jaccard，不能只带 band 里的 r 个值 |
| 带键 | `blake2b(带上 r 个值)` 的低 40 位 + 带号 | `hash()` 按进程随机化，多 worker 会算出不同桶（静默错的经典坑） |
| 判定 | 估计 Jaccard ≥ t（用全部 k 位） | 阈值直接作用在相似度上，跨组「模板相似但对不同」J≈0.4 直接被挡 |
| 参数 | k=128，b=12，r=10（b·r=120≤k），t=0.8 | 50% 碰撞点≈0.78；1K 上 b×r 与 t 网格比较后选定 |
| 复用 | 同 shingle、同 union-find、同 evaluate、同 LanceDB 三张表（`algo` 列区分） | 两个算法必须同口径才可比 |

### 10.2 结果（同一 `subset_10k`：10,002 篇 / 3,472 组 / 真值对 9,004）

| | 召回 | 精度 | 跨组误边 | 合并分量 | 丢失组 | 组压成1篇 | 灰区被删 | ray | local |
|---|---|---|---|---|---|---|---|---|---|
| minhash b12r10 t=0.8 | **1.0000** | 0.9644 | 0 | 0 | 0 | **0.9493** | 104/342 | 19.05s | **3.35s** |
| minhash t=0.85 | 1.0000 | 0.9819 | 0 | 0 | 0 | 0.9254 | 49/342 | 19.20s | — |
| minhash t=0.8 + verify 0.8 | 1.0000 | 0.9678 | 0 | 0 | 0 | 0.9447 | 98/342 | 20.51s | — |
| simhash hamming 12 | 0.9689 | 0.9802 | 28 | 7 | 7 | 0.8828 | 40/342 | 12.55s | 28.13s |
| simhash + verify 0.8 | 0.9689 | 0.9867 | 0 | 0 | 0 | 0.8782 | 30/342 | 13.39s | — |

### 10.3 对 100K 的约束

1. **compare 是 minhash 的瓶颈**：`band_hits` 10K 已 81,374（simhash 45,198），桶内对数随桶大小平方增长；
   100K 前先量「最大桶 / 桶内对总数」，必要时把 b 从 12 降到 8、或 t 提到 0.85 压候选。
2. **Ray 在 10K 对 minhash 不划算**（19.05s vs 单机 3.35s）：收益来自「计算重、共享少」的阶段，
   签名阶段太便宜反而不够摊 actor/shuffle 开销。100K 上需要先估算 compare 的并行收益再决定。
3. **签名内存**：512B × b 行/篇 → 10K 约 61MB，100K 约 610MB，100K 前要么降到 64 维、要么改成
   「先出候选对、再 join 签名」的两阶段，别让 shuffle 把内存吃满。
4. **灰区口径要固定**：minhash 天然更愿意合并 border（t=0.8 时 104/342）。做算法对比时把 t 与 hamming
   都记进报告，并同时报「裸阈值」与「+verify」两组，避免用口径差异冒充质量差异。

---

## 11. 阶段 F：1M 中间实验的设计（2026-11-10）

前五个阶段暴露的问题不是「算法选错」，而是**实验设计里缺了三个约束**：中间态没记账、
变量一次改太多、模型预测事后才写。阶段 F 按下面八条重做。

### 11.1 设计原则

| # | 原则 | 反例（前面踩过的） |
|---|---|---|
| 1 | **中间态字节是一等公民**：每个 run 必须记 `band_rows × row_bytes`，并在报告里与耗时并列 | 阶段 C 只报秒和召回，3.8 TB shuffle 是事后算出来的 |
| 2 | **一个 run 只改一个变量**（payload / compare 架构 / 是否预聚合），否则收益无法归因 | 阶段 B 同时改 hamming 和 verify，只能整组比较 |
| 3 | **质量口径要能跨实现对拍**：kept/dropped 逐字节相等 + 组缺失/合并/负样本；成对口径要用「扣除同类内部对」的可比版本 | 瘦身版把 (orig, exact) 并成一类后，`pair_recall` 从 1.0 假性跌到 0.63 |
| 4 | **不允许物化候选对**：用桶直方图代替边表 | 1M 上 simhash 的候选对是 5.3e9，物化必炸，但直方图只要 O(桶数) 内存 |
| 5 | **预测先写、偏差后算**，>±20% 必须给解释 | 阶段 E 的 6.1e7 是事后算的，没被证伪过 |
| 6 | **失败要记账**：pivot 桶数、丢的传递边、最大桶，全部进报告 | 星形合并的代价以前没人量 |
| 7 | **单核基线必跑**：证明并行不是必要条件，避免把「多核跑得快」误当成架构改进 | 现状链路 20 核跑 1227s，单核瘦身 259s |
| 8 | **复现凭据进报告**：语料 sha256、子集 doc_ids_sha256、全部参数 | — |

### 11.2 变量表（同一 `subset_1000k`）

| run | 数据 | payload | compare 实现 | 目的 |
|---|---|---|---|---|
| F1 | subset_1000k | fat 528 B/行 | Ray `groupby`+`map_groups` | 现状基线 |
| F2 | 同上（预聚合 666,506） | slim 12 B/行 | 排序 + 连续扫描，单核 | payload + 架构 |
| F2b | 同上 | slim | 同上，`--pivot-over 1e9`（关星形） | 消掉星形的准确性误差 |
| F4 | 同上 | slim | Ray 签名 + 排序扫描 | 分布式签名版的成本 |
| F3 | subset_1000k | — | 只算桶直方图 | 验证 N²，不物化 |

### 11.3 通过线

1. **决策一致**：F2b 的 `kept/dropped` 与 F1 逐字节相等；`groups_missing / components_merged /
   edge_fp_cross_base / neg_false_drop_rate` 全 0。
2. **中间态**：slim = `12 × U × 12 B`（U = 预聚合后唯一篇数），实测与理论差 ≤1%。
3. **耗时**：F4 ≤ F1 的 1/5；F2b（单核）≤ F1（20 核）。
4. **模型**：F1 的 `band_hits` 落在 `9 × 真值对` 的 ±20% 内。
5. **失败记账**：星形触发的桶数、丢的真值对数、最大桶，全部写出。

### 11.4 结果

见 `RESULTS.md` 阶段 F。五条通过线里 **4 条通过**（决策一致、中间态 0 偏差、耗时 15.6×、band_hits −3%），
第 5 条（模型）在 simhash 侧被证伪 —— 候选对实测比模型高 87 倍，已写回 `SCALING_600M.md`。
