# 进度监控：写端契约、读端行为、演练与运维

写端 `~/code/raytest/progress.py`，读端 `dashboard/app.py`（FastAPI + 静态页），
两者只通过 `/tmp/raytest` 下的文件耦合。改任何一侧前先读这份。

## 1. 文件契约

| 路径 | 谁写 | 内容 | 缺失时面板 |
|---|---|---|---|
| `progress.json` | 驱动进程（1s 刷新 + 阶段切换） | run_id、algorithm、stage、stages[]、percent、吞吐、events、自定义 extra | 「等待实验启动」 |
| `workers/<id>.json` | 每个 worker/actor（首批立即、之后 ≤2/s） | 自己的 stage、批次、篇数、吞吐、last_batch | 「等待 worker 上报」 |
| `experiment.log` | 启动脚本 `tail -F` 落盘 | driver stdout | 「等待 experiment.log」 |
| `result.json` | 实验收尾（`--out`） | 最终报告（含 algorithm 与算法参数） | 「尚无 result.json」 |
| `runs/<run_id>.json` | `run_experiment.py` 收尾 | 每次 run 的报告副本 | 无影响 |
| `lance`（`dedup_runs` 表） | `run_experiment.py`（slim **不写**） | 历史 run 列表 | 历史面板显示提示 |

`progress.json` 关键字段：

| 字段 | 语义 |
|---|---|
| `run_id` | **时间戳**（`%Y%m%d-%H%M%S`），不是 `--run-id`；与 `result.json.run_id` 不同属正常 |
| `algorithm` | `simhash` / `minhash` / `slim`；决定阶段文案与参数条 |
| `stage` | `_active_stage()` 的结果：从后往前找「最后一个已开始但未完成」的阶段 |
| `stage_hint` | 最后一次显式 `set_stage()` 的名字（与 `stage` 分叉是正常的，见 §3） |
| `stages[]` | 顺序即面板渲染顺序；`done` 单调不减，`total` 可为 null |
| `percent` | 按 `weight` 加权的总进度；`total=null` 的阶段贡献 0（所以 100% 只在 `finish()` 后出现） |
| `docs_done / docs_total` | 头部「篇」进度；`docs_done` 只从 `DOC_STAGE_NAMES`（`fingerprint`/`signature`）取 |
| `throughput_docs_per_sec` | `docs_done / 运行秒数`（不是瞬时值） |
| `events[]` | 写端保留 200 条（`flush` 只发 50 条），面板取倒序 |
| `workers_alive / workers_total` | 监控线程统计（`updated_at` 距今 >15s 视为 stale） |
| extra（`edges` / `verify_total` / `verify_kept` / `docs_kept` / `docs_dropped` / `unique` …） | 原样进 JSON，面板的漏斗卡片按字段名取 |

## 2. 写端规则（`progress.py`）

1. **原子写**：`tmp = path.with_suffix(f".tmp{pid}-{thread}-{uuid}")` → `os.replace`。
   临时名必须唯一：监控线程和主线程会并发 `flush()`；旧实现用固定 `progress.tmp<pid>`，
   两个线程同时 replace 会 `FileNotFoundError: progress.tmp16052 -> progress.json`（真实事故）。
2. **worker 不在 `__init__` 落盘**，首批 `report()` 才写。Ray Data 的 actor pool 会预启动一批
   actor，其中一些一次批次都没跑；构造时写文件 → 面板「任务分配」出现一堆 0 篇/0 批的空行。
3. **写频率**：首批立即写，之后 ≥0.5s 才写一次；另有 5s 心跳（仅 `batches_done > 0`），
   防止一批算很久被面板误判「陈旧/空闲」。
4. **监控线程**（`start_monitor`，1s）：扫 `workers/*.json` → 按 stage 累加 → 取历史最大值（只增不减）；
   忽略 `updated_at` 早于 120s 的残留文件；同步 `docs_done`、`workers_alive/total`。
5. **`declare()` 先登记全部阶段**（顺序 + 权重），再 `set_stage()` 推进；`set_stage` 的 `done` 取 `max`
   （不允许回退）。收尾必须 `finish()` + `stop_monitor()`，否则阶段永远「进行中」。
6. `ProgressReporter.__init__` 会**清空** `workers/*.json`（上一次 run 的残留），所以同一 `progress_dir`
   上不要并行跑两个实验。
7. 目录：`RAYTEST_PROGRESS_DIR` 或构造参数；Ray 侧每个 actor 都要拿到同一个 `progress_dir`
   （`fn_constructor_kwargs={"progress_dir": pdir}`），漏了就只有全局条、没有任务分配表。

### 2.1 阶段单位必须自洽

`percent` 的分子分母都来自 `stages[].done/total`。fat minhash 的 `compare` 里，`total = 篇数 × 带数`
（带行数），而 `BandComparer` 上报的是**篇数**，于是 `done` 最多到 `10M/12M`，percent 爬到 ~68% 就停，
看起来像卡死。slim 改成「带行计数」（`total = b×U`，`done = 已扫行数`），两边同单位。
**新增阶段时先问一句：done 是谁数的，和 total 是同一个东西吗？**

### 2.2 按篇计数的阶段要登记两处

- `progress.DOC_STAGE_NAMES = ("fingerprint", "signature")`：决定 `docs_done` 从哪个阶段取。
  旧代码写死 `fingerprint`，minhash 下头部恒 `0/10002`。
- `dashboard/app.py: DOC_STAGES`：决定 ETA 是否用全局篇/秒估。

新增按篇阶段（例如把 `exact` 也算进文档进度）要同时改这两处。

## 3. 读端行为（`dashboard/app.py`）

- 只读文件，不调用 Ray、不加锁；实验跑着也能开。JSON 按 `(mtime, size)` 缓存后解析；
  `invalid_json`（并发写/半截）时保留上一次成功数据，只在状态里标记错误。
- 算法：`progress.json.algorithm` → 其次 `result.json.algorithm` → 都没有显示「未知」。
- 阶段文案：`STAGE_LABELS`（通用）+ `ALGO_STAGE_LABELS[algorithm]`；**未知阶段名原样显示**，
  所以加阶段不一定会崩，但文案会变成英文 key。
- 参数条：`ALGO_PARAM_KEYS`（simhash: bits/band_bits/hamming/shingle/verify；
  minhash: bands/rows/threshold/shingle/verify；slim: bands/rows/threshold/shingle/pivot_over）。
  参数的来源是 `progress.json` → 同一次 run 的报告副本 → `result.json`（**仅当 run 相同**）；
  progress.json 里没带参数时，面板的参数条就是空的（drill 场景就是这样，不是 bug）。
- `ALGO_META` 只登记了 simhash / minhash，所以 `/api/state` 里 slim 的 `algorithm_label` 是 null；
  前端 `app.js` 自带一份 slim 文案（「Slim MinHash / 预聚合 · 瘦带行 · 排序扫描」），页面显示正常。
  要让 API 消费者也拿到标签，在 `ALGO_META` 补一行 `"slim": {"label": "Slim MinHash", "desc": ...}`。
- 新鲜度：「数据陈旧」阈值 `RAYTEST_IDLE_SEC=15`；`stage=done` 且 `result.json` 存在时显示「已完成」。
- 历史面板：`dedup_runs` 表（`run_experiment.py` 写）；`RAYTEST_RUNS_JSON` 可整体替换成假数据。
  **slim 不写 LanceDB**，所以 slim 的 run 不会出现在历史对比里 —— 要留档就自己归档结果 JSON。
- 环境变量：`RAYTEST_RUN_DIR`（默认 `/tmp/raytest`）、`RAYTEST_LANCE_DIR`、`RAYTEST_RUNS_JSON`、
  `RAYTEST_PORT`（默认 8501）、`RAYTEST_HOST`、`RAYTEST_IDLE_SEC`。

## 4. 运维

```bash
ssh node1 'systemctl --user is-active ray-head raytest-dashboard'
ssh node1 'systemctl --user restart raytest-dashboard'      # 改完 app.py
ssh node1 'curl -s 127.0.0.1:8501/api/health'
ssh node1 'curl -s "127.0.0.1:8501/api/state?lines=5"' | head -c 800
ssh node1 'tail -50 /tmp/raytest/experiment.log'
ssh -N -L 8501:127.0.0.1:8501 node1                          # 隧道；或 scripts/tunnel.sh
```

改 dashboard 的流程：本地改 → `rsync -az dashboard/app.py node1:code/raytest/dashboard/` →
`systemctl --user restart raytest-dashboard` → 刷新页面（静态页 `index.html`/`app.js` 同样要 rsync）。

## 5. 不上集群演练（值得每次改面板都做）

`scripts/drill_progress.py` 直接用 `progress.py` 的写端 API 生成一份假 run（不 import ray）：

```bash
python3 ~/.agents/skills/minhash-slim-dedup/scripts/drill_progress.py \
  --repo /Users/lee/code/raytest --run-dir /tmp/raytest-drill --algo slim --speed 8
```

它按 slim 的 5 阶段推进、写 `progress.json` / `workers/*.json` / `result.json`，跑完做契约自检
（阶段齐全、percent=100、docs_done=total、stage=done）。

另起一个面板看这份假数据（真实服务不动）：

```bash
ssh node1 'cd ~/code/raytest && RAYTEST_RUN_DIR=/tmp/raytest-drill RAYTEST_PORT=8503 \
  RAYTEST_LANCE_DIR=/tmp/nonexistent .venv/bin/python dashboard/app.py'
# 本地再开一条隧道
ssh -N -L 8503:127.0.0.1:8503 node1     # → http://127.0.0.1:8503/
```

仓库还有 `dashboard/tools/make_fake_run.py`（支持 `--algo minhash --animate`），用来演练「没有表」「0 字节日志」
等降级路径；两者互补：本 skill 的 drill 走真实写端 API，仓库工具走静态数据。

## 6. 故障对照

| 症状 | 先查 |
|---|---|
| 页面「等待实验启动」 | `progress.json` 是否存在；`--progress-dir` 是否指到别处 |
| 数字像上一轮 | `progress.json.run_id` + `updated_at` 年龄；`result.json` 的 mtime |
| 有总进度、没有任务分配 | Ray actor 没拿到 `progress_dir`，或 worker 文件超过 15s 没更新（stale） |
| 指标卡不见 | `--out` 不是 `/tmp/raytest/result.json`（面板只认这个文件名） |
| 历史面板空 | `dedup_runs` 表不存在 / 没装 lancedb / slim run 本来就不写表 |
| 502 / 连不上 | 隧道断了，或 `raytest-dashboard` 挂了 |
