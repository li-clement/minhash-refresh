# minhash-refresh — 瘦身 MinHash 去重流水线（node1 Ray 集群）

单机 Ray 集群（node1，Ubuntu 24.04 / 24 核 / 125 GiB）上的一套**全阶段 Ray 化**文本去重流水线，
外加实时监控页与去重结果页。1M 篇端到端 **50.8s**，kept/dropped 逐字节可复现。

仓库同时放两样东西：

| 目录/文件 | 是什么 |
|---|---|
| `dedup_slim.py`、`stages.py`、`minhash.py`、`metrics.py`、`text.py`、`progress.py`、`corpus_io.py`、`gen_corpus.py`、`lancedb_io.py`、`dashboard/` | **流水线本体**（Ray task 编排 + 监控/结果面板） |
| `minhash-slim-dedup/`、`install.sh` | **Agent skill `minhash-slim-dedup`**：操作这套流水线的指令、脚本、参考文档 |
| `README.md` / `RESULTS.md` / `EXPERIMENT_PLAN.md` / `SCALING_600M.md` | 用法、逐阶段实验结果、设计决策、600M 外推 |

## 安装 skill（可选）

```bash
./install.sh                 # 复制到 ~/.agents/skills/minhash-slim-dedup
# 或者软链，git pull 后自动生效：
ln -s "$PWD/minhash-slim-dedup" ~/.agents/skills/minhash-slim-dedup
```

装好后会话里以 `minhash-slim-dedup` 出现；触发场景：跑瘦身 minhash / 1M 去重 / 看或改监控面板 /
接进度上报 / 判断两次 run 的 kept 是否逐字节一致。

## 1. 集群

| 项目 | 值 |
|---|---|
| 主机 | node1 (`192.168.2.51`，Tailscale `100.102.205.59`) |
| Ray 版本 | 2.60.0（venv：`/home/lee/code/raytest/.venv`） |
| GCS 端口 | `6379` |
| Dashboard | `http://127.0.0.1:8265`（隧道见下） |
| 资源 | 22 CPU（留 2 核给系统）、8 GiB object store、111 GiB 可用内存 |
| 常驻方式 | systemd **user** 单元 `ray-head.service`（已 enable，linger 已开，开机会自启） |

```bash
ssh node1 'systemctl --user status ray-head --no-pager'   # 看状态
ssh node1 '~/code/raytest/start_ray_head.sh'              # 重启
ssh node1 '~/code/raytest/stop_ray.sh'                    # 停
ssh node1 '~/code/raytest/.venv/bin/ray status'           # 资源/节点
```

**加更多节点**（同一局域网即可，节点上需同版本 Ray 与同样的 Python 依赖）：

```bash
# 在 worker 上（建议用同一个 venv 布局）
ray start --address='192.168.2.51:6379' --num-cpus=<核数> --disable-usage-stats
```

本地看 Dashboard / 监控页：

```bash
ssh -N -L 8265:127.0.0.1:8265 -L 8501:127.0.0.1:8501 node1
# 然后浏览器打开 http://127.0.0.1:8265 （Ray）与 http://127.0.0.1:8501 （监控页）
```

## 2. 去重流水线

```
LanceDB 表 ──▶ read_lance(subset_10k.lance)   Ray 直接分布式扫 Lance 数据集
          ──▶ map_batches(Fingerprinter)     每篇算 128bit simhash（批内 numpy 向量化）
             （--algo minhash 时换成 MinHashSigner：128 维签名，0.30ms/篇）
          ──▶ flat_map                       切 8 段 × 16bit，展开成 (band, doc_id, fp)
          ──▶ groupby(band).map_groups       同桶内两两算汉明距离 ≤ k ⇒ 重复边
          ──▶ verify(可选)                   Jaccard ≥ 0.8 复核，剔掉低相似度误边
          ──▶ union-find                     连通分量合并，取最小 doc_id 为代表
          ──▶ LanceDB                        dedup_result / dedup_edges / dedup_runs
```

文件：

| 文件 | 作用 |
|---|---|
| `dedup_slim.py` | **瘦身 minhash 流水线**（唯一实现）：精确预聚合 + 12 B 带行 + 按带并行排序扫描；`--mode ray/local` |
| `stages.py` | 五个阶段的 Ray task：`exact_chunk` / `dedupe_hashes` / `sign_chunk` / `band_chunk` / `compare_band` / `resolve` / `measure` |
| `minhash.py` | k 维 MinHash 签名（31bit + 仿射置换）、b×r 分带、估计 Jaccard、单机基线 `dedup()` |
| `text.py` | 文本规范化（NFKC/小写/去标点）与字符 3-gram shingle、`ws_key`（精确重复判等键） |
| `metrics.py` | 组口径 + 成对口径指标（`evaluate`），与历史阶段同一套定义 |
| `progress.py` | 写 `/tmp/raytest/progress.json` 与 `workers/*.json`（监控页数据源） |
| `corpus_io.py` | 固定语料快照：一次生成、按整组取子集（200K 与 1M 两份快照，sha256 入 manifest） |
| `lancedb_io.py` | LanceDB 进出口：语料入库、按整组物化子集（`subset_1k/10k/1000k`） |
| `gen_corpus.py` | 合成中文语料：orig、exact（仅空白差异）、near（~2% 字符改写）、border（换 1 句）、neg（同模板不同内容） |
| `dashboard/` | 监控页（FastAPI + 原生 canvas，端口 8501）与 `/result` 去重结果可视化页 |
| `start_ray_head.sh` / `stop_ray.sh` | Ray head 起停（生产上由 systemd `ray-head.service` 托管） |

## 3. 跑实验

```bash
ssh node1
cd ~/code/raytest

# 参数标定（单机，几十秒）
.venv/bin/python calibrate.py

# 小规模验证（用固定语料，按整组取前 N 篇）
.venv/bin/python run_experiment.py --docs 1000 --mode both --bits 128 --band-bits 16 \
    --hamming 12 --corpus /tmp/raytest/corpus/corpus_200k.jsonl --verify 0

# minhash：k=128 / 12 带 × 10 行 / 估计 Jaccard >= 0.8（自动写 algo=minhash）
.venv/bin/python run_experiment.py --docs 1000 --mode both --algo minhash \
    --bands 12 --rows 10 --threshold 0.8 --verify 0 \
    --corpus-uri /tmp/raytest/lance --corpus /tmp/raytest/corpus/corpus_200k.jsonl

# 规模化（报告写到 /tmp/raytest/result.json）
.venv/bin/python run_experiment.py --docs 200000 --mode ray --hamming 6 --band-bits 8 | tee /tmp/raytest/experiment.log
```

参数含义：

- `--band-bits`：指纹切带粒度。64 bit 切 8 段（`8`）召回几乎无损但候选对多；切 4 段（`16`）候选对少 30 倍但漏检明显。
- `--hamming`：判定阈值。阈值越大召回越高、误杀越多。
- `--shingle`：字符 n-gram 长度，中文默认 3。

## 4. LanceDB 输入/输出（10K 起的默认路径）

```bash
# 0) 依赖（已装在 venv 里）：lancedb + pylance（ray.data.read_lance 需要 pylance 读 Lance 数据集）
# 1) 语料入库（只做一次；200K 快照约 2s，sha256 与 manifest 对齐后写 catalog 表）
.venv/bin/python lancedb_io.py --uri /tmp/raytest/lance --ingest \
    --src /tmp/raytest/corpus/corpus_200k.jsonl

# 2) 按整组物化子集（可复用；--force 重建），并和 jsonl 路径对拍 doc_id 集合
.venv/bin/python lancedb_io.py --uri /tmp/raytest/lance --materialize 10000 \
    --compare-jsonl /tmp/raytest/corpus/corpus_200k.jsonl

# 3) 跑实验：输入 read_lance(subset_10k)，结果写回同一 uri
DOCS=10000 MODE=both BITS=128 HAMMING=12 BAND_BITS=16 VERIFY=0 \
  CORPUS=/tmp/raytest/corpus/corpus_200k.jsonl ./submit_experiment.sh \
  --corpus-uri /tmp/raytest/lance --run-id phaseB_simhash_10k_lance

# 4) 查表：诊断 / 重建报告 / 一览
.venv/bin/python lancedb_io.py --uri /tmp/raytest/lance --inspect
.venv/bin/python analyze_run.py --run-id phaseB_simhash_10k_lance
.venv/bin/python analyze_run.py --run-id phaseB_simhash_10k_lance --export-report /tmp/r.json
```

`--corpus-uri` 给了就走 LanceDB：整组子集从表里取（与 jsonl 路径逐 doc_id 相等才继续），
Ray 侧读 `subset_<n>.lance`，跑完把逐篇 kept/代表文档、候选边、run 指标写回三张结果表
（`dedup_result` / `dedup_edges` / `dedup_runs`，按 `run_id` 累积，历史可查）。
不给 `--corpus-uri` 时仍是老路子（`from_items` + 只写 `result.json`）。

## 4.1 出对比图

```bash
python3 viz_compare.py            # 读 results/ 下所有 run 报告，写 results/figures/
```

产出 6 张图（质量 / 召回-精度权衡 / 成本 / 参数与阈值 / 10K 双算法对拍 / 10K 记分卡）和
`summary.md`（图里用到的数字表）；双算法对拍表另写一份 `results/phaseD_10k/comparison.md`。
脚本内置了中文字形缺失检查与「文字是否被画布裁掉」的版面自检，换机器跑会直接报错而不是悄悄出方块。

## 5. 结果（node1 单机 22 CPU）

见 `RESULTS.md`。实验设计（simhash vs minhash 对照、指标口径、参数网格）见 `EXPERIMENT_PLAN.md`。

```bash
# 固定语料（已生成一次，勿重复覆盖）
.venv/bin/python corpus_io.py --docs 200000 --out /tmp/raytest/corpus/corpus_200k.jsonl

# 阶段 A：1K 冒烟（simhash），走 job submission，面板可实时看
DOCS=1000 MODE=both BITS=128 HAMMING=12 BAND_BITS=16 VERIFY=0 \
  CORPUS=/tmp/raytest/corpus/corpus_200k.jsonl ./submit_experiment.sh
```

`submit_experiment.sh` 支持 `DOCS / MODE / BITS / HAMMING / BAND_BITS / CORPUS / VERIFY / LOG` 环境变量，
其余参数（如 `--corpus-uri` / `--subset-table` / `--run-id`）原样透传给 `run_experiment.py`。
