---
name: minhash-slim-dedup
description: Run, monitor, and extend the slim MinHash dedup pipeline (dedup_slim.py — exact pre-aggregation, 12-byte band rows, sorted bucket scan) together with the progress.json + workers/*.json monitoring protocol behind the :8501 dashboard. Use when asked to 跑瘦身 minhash / 1M 去重 / 对比 fat 与 slim / 看或改监控面板 / 接进度上报 / 判断两次 run 的 kept 是否逐字节一致。Not for the legacy fat Ray Data pipeline or plain Ray cluster chores — use ray-dedup-experiment for those.
---

# 瘦身 MinHash 去重 + 进度监控（node1）

两件东西捆在一起，改一边要顺手检查另一边：

1. **算法**：`dedup_slim.py` —— 1M 验证过的 minhash 流水线。三条改动：精确预聚合（只对唯一文本算签名）、
   带行瘦身（每条 12 B）、排序 + 连续扫描（不 `groupby` + `map_groups`）。
2. **监控**：`progress.py`（写端）+ `dashboard/app.py`（读端）。驱动写 `progress.json`，worker 各写
   `workers/<id>.json`，面板只读 `/tmp/raytest`，不碰 Ray。

## 何时用

- 「跑一下瘦身版 / 1M 去重」「fat 和 slim 的 kept 对得上吗」「1M 到底多少钱」
- 「面板 percent 卡住了」「任务分配里一堆 0 篇的 worker」「docs_done 一直是 0」
- 「给流水线加一个阶段/算法，进度要能在面板上看到」「换端口演练面板」
- 需要判读「这次结果能不能和 fat 版逐项比」

不适用：老 fat 流水线（`run_experiment.py` + Ray Data）、simhash 参数标定、语料生成 → 用 `ray-dedup-experiment`。

## 环境事实（先确认，再动手）

| 项目 | 值 |
|---|---|
| 集群 | `node1`（ssh 别名），节点 24 核 / 125 GiB，Ray 可用 22 CPU（2 个留给驱动/系统） |
| 仓库 | 远端 `~/code/raytest`；Mac 上 `/Users/lee/code/raytest` 是同一份，改完要 rsync 过去 |
| venv | `~/code/raytest/.venv/bin/{python,ray}` |
| Ray | systemd user `ray-head.service`；GCS `6379`；Job Submission `http://127.0.0.1:8265` |
| 面板 | systemd user `raytest-dashboard.service`，`:8501`，需 SSH 隧道 |
| 运行目录 | `/tmp/raytest/{progress.json,workers/,experiment.log,result.json}` |
| 结果页 | 同一服务下的 `/result`（指标可视化 + 文本对照）；历史 run 在 `/tmp/raytest/runs_index.json` |
| 1M 数据 | `/tmp/raytest/corpus/1m/corpus_1m.jsonl`（1,000,002 篇 / 880.9 MB / sha256 `8a6ad577…`） |
| LanceDB | `/tmp/raytest/lance`：`corpus_1m`、`subset_1000k`（1,000,000 篇 / 366,666 组 / `cd31d53b…`）、`subset_10k`、`subset_1k` |

`subset_1000k` 不是 200K 语料的子集，是另一份快照：任何跨快照的对比都不成立（见 `references/algorithm.md` §0）。

## 五阶段与不变量

| 阶段 | 做什么 | done/total 单位 | 权重 |
|---|---|---|---|
| `exact` | 去空白 → sha1 → 精确重复聚成一类，只留类代表 | 篇（原文数） | 0.06 |
| `signature` | 对代表算 k=128 维 minhash 签名 | 篇（唯一数） | 0.26 |
| `band` | 12 带 × 10 行 → `(band_key, uid)`，每条 12 B | 带行数 | 0.10 |
| `compare` | 按 `band_key` 排序后线性扫桶，桶内估计 Jaccard ≥ t | 带行数 | 0.40 |
| `resolve` | numpy 并查集 → 代表级 → 文档级决策，调参考 `evaluate` | 1 | 0.18 |

**每个阶段都是一个 `@ray.remote` task**（`stages.py`），驱动只做编排：`exact` 20 分片、
`signature` 20 分片、`band` 12 分片、`compare` **按带并行**（12 个 task，各自只扫本带的桶）、
`resolve`/`measure` 单 task。按带并行同时也是 600M 的形状。

不变量（破了就不是「瘦身」，是「换算法」）：

- **带键仍由全精度签名算 blake2b**（`minhash.band_key`，40 bit + `band<<40`），所以候选桶与 fat 版完全一致；
  变的只是「每行带什么」和「怎么扫」。
- **估计用全部 k 位**，不是桶里那 r 位。
- 精确预聚合只按「去掉所有空白后的 sha1」合并；类代表与分量存活者都取**最小 doc_id**，
  正好等于 fat 版并查集根的语义 —— 所以决策能逐字节相同。
- 质量指标直接调 `run_experiment.evaluate`，与阶段 A–D 同一套口径（但召回要看可比口径，见下）。

## 起手

```bash
S=~/.agents/skills/minhash-slim-dedup/scripts
$S/preflight.sh          # 服务 / 资源 / 残留 job / 表 / 语料 / 面板 / progress 新鲜度；只读
$S/preflight.sh --stop-jobs   # 确认上一轮没停干净时，停掉 RUNNING/PENDING 的 job
```

看到 `有作业在跑` 就先决定：让它在跑就别开新实验（compare 段是纯 Python 单核为主，抢 CPU 会污染计时）。

## 跑

```bash
# 10K 冒烟：先 local（单核，约 5s），确认 kept 与 fat 10K 相等再上 Ray
$S/run_slim.sh --mode local --table subset_10k --tag smoke10k

# 1M：全阶段 Ray——1M 实测 50.8s（exact 5.2 / sign 16.3 / band 0.83 / compare 0.63 / uf 2.5 / eval 15.9）
$S/run_slim.sh --mode ray --table subset_1000k --tag f4b_1m

# 关掉 pivot 星形（1M 上的正确选择），其余参数用默认
$S/run_slim.sh --mode ray --table subset_1000k --pivot-over 1000000000 --tag f4b_1m_nopivot
```

规则：

- **`--pivot-over 1000000000`**：1M 上 max_bucket 只有 411，星形只省 18% 比较次数、墙钟几乎不变（78.7s → 78.5s），
  却丢 76 条真值对、多留 7 篇。除非桶里出现几千篇（600M 分带后才可能），否则全程两两比较。
- 默认 `b=12, r=10, t=0.8, k=128, shingle=3, seed=7`，与阶段 C/D 一致才可对比；改任何一项都要在报告里记。
- `--mode ray`（默认）每个阶段都是 Ray task；`--mode local` 用同一批函数在进程内顺序跑，只用于调试。
- `--scan loop` 保留旧的逐桶 Python 循环做对照（1M 上 21.5s vs fast 的 2.1s）。
- `--no-eval` 跳过参考口径指标（省 ~16s，生产计时用；验收必须开）。
- 想在不污染面板的情况下试跑：加 `--isolated`（progress 写到 `/tmp/raytest/<tag>-progress`，面板默认看不到）。

脚本行为：远端 `nohup` 起进程、本地每 5s 打一行进度、跑完把 `result_slim_<tag>.json` 和日志拉回
`results/<tag>/`，并打印 kept/dropped/口径提示。中断：`Ctrl-C` 只停本地轮询，远端继续；
`$S/run_slim.sh --stop --tag <tag>` 收掉。

## 边跑边看

```bash
$S/watch.sh                  # 每 5s 一行：阶段 / percent / docs / 吞吐 / 活跃 worker；stage=done 自动退出
$S/watch.sh --once --json    # 一次性，机器可读（挑字段，不是整份 JSON）
$S/tunnel.sh                 # 若没有：ssh -N -L 8501:127.0.0.1:8501 node1
```

浏览器 <http://127.0.0.1:8501/>；同一份数据走 API：

```bash
ssh node1 'curl -s 127.0.0.1:8501/api/health'
ssh node1 'curl -s "127.0.0.1:8501/api/state?lines=5"' | head -c 800
```

面板字段、写端契约、演练方法见 `references/monitoring.md`。

## 怎么判读（通过线）

一次 slim run 只有同时满足下面几条才算「与 fat 等价、且真的瘦了」：

1. **决策逐项相等**：`kept` / `dropped` 与 fat 完全相同（10K：3,648 / 6,354；1M：383,708 / 616,292）。
2. `groups_missing = 0`、`components_merged = 0`、`neg_false_drop_rate = 0`、`edge_fp_cross_base = 0`。
3. `border_dropped` 与 fat 相等（1M：10,758）——灰区只看趋势，但两边必须同值。
4. **召回用可比口径** `pair_recall_edge_canon`（1M：0.9999），不要用 `pair_recall`（0.6149）。
   差值来自 333,654 条「同一个精确类内部」的真值对：预聚合后它们天然没有边，不是召回掉了。
5. 中间态：`bytes.slim_bytes_per_doc = 96.0 B`（含预聚合），同篇数 fat 对照 4,223 B/篇 → **44×**；
   按原始 1M 篇算 96 B/篇 vs 6,336 B/篇 → **66×**。两个数都对，别混着讲。
6. 成本看 `sec{}`：1M ray **50.8s**（load 4.1 / exact 5.2 / sign 16.3 / band 0.83 / compare 0.63 /
   uf 2.5 / eval 15.9）；`--no-eval` 约 35s；`--scan loop` 对照 80.1s（compare 21.5s）。
   10K 冒烟应看到 unique 6,630、kept 3,648 / dropped 6,354、`pair_recall_edge_canon=1.0`。

自动对拍：

```bash
$S/compare_runs.py <fat 报告.json> results/<tag>/result_slim_<tag>.json
```

## run 标识、历史索引与结果页

- `run_id = <YYYYmmdd-HHMMSS>-<ray job 前 8 位>`；`ray_job_id` 取自 `ray.get_runtime_context().get_job_id()`。
- 每次跑完向 `/tmp/raytest/runs_index.json` 追加一行（run_id / job / 时间戳 / kept / 耗时 / 体积 / 参数），
  同时写 `result_slim_<run_id>.json` 与 `result_slim_latest.json` 两个结果包。
- 面板：`GET /api/run_index`（索引）、`GET /api/result[?run=<id>]`（结果包）、`GET /result`（可视化页：
  漏斗、验收项、耗时/体积、分量大小分布、Jaccard 直方图、代表 vs 被删的文本对照 + 差异标红）。
- 监控页「历史对比」也读这个索引（`runs_source=index`），表格里的 `ray job` 列就是 job 编码。

## 监控协议：写端三条硬规则

1. **先 `declare([...])` 再干活**：阶段顺序与权重一次定死，`percent` 不会随阶段推进跳变。
2. **`total` 的单位必须等于 `done` 的来源**。fat minhash 的 `compare` total 是「带行数」
   （12,000,000），worker 报的却是「篇数」——percent 爬到 68% 就再也上不去，看起来像卡死。
   slim 用带行计数，两边一致。
3. **按篇计数的阶段名要登记**：`progress.DOC_STAGE_NAMES` 决定 `docs_done` 从哪个阶段取
   （`fingerprint` / `signature`），`dashboard/app.py` 的 `DOC_STAGES` 决定 ETA 是否用全局篇/秒。
   新增按篇阶段（如 `exact`）要同步这两处，否则面板头部恒 0 或 ETA 乱跳。

另外：worker 侧**不要在 `__init__` 里落盘**（Ray 会预启动一堆空 actor → 「任务分配」里全是 0 篇行），
首批再写、之后每 0.5s 最多一次；写 `progress.json` 必须原子替换且临时名唯一（pid+thread+uuid），
否则多线程并发写会半截 JSON / `FileNotFoundError`。

## 改监控 / 加阶段

1. 选阶段名（小写、与现有词表不冲突）；面板文案加在 `dashboard/app.py` 的 `STAGE_LABELS`，
   slim 专属文案加在 `ALGO_STAGE_LABELS["slim"]`；参数条加在 `ALGO_PARAM_KEYS["slim"]`。
2. 驱动侧 `prog.declare([...])` + `prog.set_stage(name, total=...)`；收尾 `prog.finish()`、`prog.stop_monitor()`。
3. worker 侧 `WorkerReporter(stage="<name>", root=...)`，每批 `report(docs, start, end)`；
   Ray 的每个 `map_batches` 都要把 `progress_dir` 通过 `fn_constructor_kwargs` 传进去。
4. 按篇计数的阶段 → 同步改 `progress.DOC_STAGE_NAMES` 与 `dashboard/app.py: DOC_STAGES`。
5. 改完 `rsync -az dashboard/app.py node1:code/raytest/dashboard/`，再
   `ssh node1 'systemctl --user restart raytest-dashboard'`。
6. 不上集群先验收：`scripts/drill_progress.py`（见 `references/monitoring.md` §5）。

## 已知坑（详见 `references/pitfalls.md`）

| 现象 | 根因 |
|---|---|
| percent 爬到 ~68% 不动，compare 其实在跑 | 阶段 total 与 worker 计数单位不一致（行 vs 篇） |
| 头部阶段是 compare，日志/当前阶段写「签名计算 99.5」 | `stage` 取最下游未完成阶段，`stage_hint` 是最后显式 set 的；流式流水线下本来就会分叉 |
| `docs_done` 恒 0 | 旧代码写死 `fingerprint`；minhash 要读 `signature` |
| 任务分配里一堆 0 篇空 worker | 构造时就落盘；Ray 预启动的 actor 一次批次都没跑 |
| 面板数字是上一轮的 | 旧 `result.json` 还在；先看 `progress.json.run_id` 与 `updated_at` 年龄 |
| slim 的 `pair_recall` 只有 0.61 | 预聚合让类内真值对不可能有边；看 `pair_recall_edge_canon` |
| slim 比 fat 多留 7 篇 | 开了 pivot 星形；`--pivot-over 1000000000` |
| `--pivot-over 1e9` 一启动就退出 | 参数是 `type=int`，只吃 `1000000000`，`1e9` 会 argparse 报错 |
| ray 模式报「签名数 != 唯一文档数」 | `MinHashSigner` 要 `doc_id` 列（传 canon 下标）；列名写成 `uid` 就丢行 |
| 10K 上 Ray 比单机慢 5.7×，1M 却快 3.3× | 10K 的 compare 是纯 Python 逐桶，固定开销占比大；别用 10K 外推 |
| `run_slim.sh` 报 `ST: unbound variable` | `$VAR` 紧跟中文标点（`$ST、`），`set -u` 下被当成变量名；已改成 `${ST}、` |
| 结果包报 `build_bundle is not defined` | 函数被追加到 `if __name__ == "__main__"` 之后；脚本模式下 `main()` 先执行 |
| 面板「任务分配」全是 0 行 | 单进程阶段没有 actor 上报；驱动会写一条 `worker_id=driver` 的伪 worker 行 |

## 600M 摘要（只做结论，细节见仓库）

按 1M 实测修正后的 600M 预算：minhash 候选对 7.2e9（约 6 core-h compare）、签名 52 core-h、
瘦身后中间态 57.6 GB；simhash 在 1M 上实测候选对已是 5.33e9（随机桶模型的 87×），
外推 600M ≈ 1.9e15，**物理不可行**。
所以 600M 的路线是 minhash 瘦身 + 按带分区，而不是「加宽 simhash」。
完整推导与预算表在仓库 `SCALING_600M.md` §10（1M 实测修正）与 §9（P0/P1 清单）。

## 收尾

- 结果归档到 `results/<tag>/{result_slim_<tag>.json, <tag>.log}`；结论写 `RESULTS.md`，设计变更写
  `EXPERIMENT_PLAN.md`。
- 集群别留作业：`$S/preflight.sh` 看 CPU 是否回到 0/22；跑完确认 `stage=done`。
- 面板「任务分配」每行只覆盖该 actor 存活期间的量（Ray Data 会回收 actor），阶段总计才是准的。
