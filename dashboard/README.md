# raytest 任务监控面板

**simhash / minhash 双算法**文本去重实验的**只读**监控页面：FastAPI + uvicorn 常驻在
`node1`（systemd user 服务 `raytest-dashboard.service`），浏览器通过 SSH 隧道访问。

- 算法跟着数据走：头部 badge 与参数条按 `progress.json.algorithm`（运行中）或
  `result.json.algorithm`（收尾后）切换，两者都没有就显示「未知」
- 阶段区按 `progress.json` 里的 `stages` 动态渲染（simhash 首段 `fingerprint`、
  minhash 首段 `signature`，未知阶段名原样显示）
- 历史对比面板读 LanceDB `dedup_runs`（每次 run × mode 一行），没装 lancedb /
  没有表 / 路径不可读时只显示提示文案，不报错
- 只读 `/tmp/raytest/`：不写入、不删除实验产物（真实实验在跑时也安全）
- 不碰 Ray：不发 `ray start/stop`，不动 6379 / 8265，`ray status` 只读且 ≥5s 一次、超时 5s
- 前端零公网依赖：不用任何 CDN，图表是手写 canvas，字体用系统字体
- 所有 API 走相对路径（`./api/...`），所以隧道端口随便换都能用

## 一、安装（在 Mac 上执行）

```bash
cd /Users/lee/code/raytest/dashboard
./install.sh
```

脚本是幂等的，重复执行 = 升级（pip → rsync → daemon-reload → enable --now → restart → 健康检查）。它按顺序做：

1. 远端 `~/code/raytest/.venv` 里装 `fastapi uvicorn psutil`（`-i https://pypi.tuna.tsinghua.edu.cn/simple`）
2. `rsync -az --delete dashboard/ → node1:~/code/raytest/dashboard/`
3. 把 `dashboard.service` 里的 `@PROJECT_DIR@` 换成远端绝对路径，写到
   `~/.config/systemd/user/raytest-dashboard.service`
4. `systemctl --user enable --now raytest-dashboard.service`（随后 `restart` 让新代码生效）
5. `curl 127.0.0.1:8501/api/health` 轮询到 200，并打印 `is-active`

可选参数：`--skip-deps`（跳过 pip）、`--no-restart`、`--dry-run`（只打印，不执行）。
环境变量：`RAYTEST_REMOTE`（默认 `node1`）、`RAYTEST_REMOTE_DIR`（默认 `code/raytest`）、
`RAYTEST_PIP_INDEX`、`RAYTEST_PORT`。

## 二、访问：SSH 隧道

node1 在 NAT 后，页面只能经隧道打开：

```bash
ssh -N -L 8501:127.0.0.1:8501 node1      # 前台保持
# 或带自动重连：autossh -M 0 -N -L 8501:127.0.0.1:8501 node1
```

然后浏览器打开 <http://127.0.0.1:8501/>。

服务监听 `0.0.0.0:8501`（供隧道/内网直连），页面内所有请求都是相对路径，不写死 host。
**注意**：面板没有鉴权，靠内网 + 隧道隔离，不要把 8501 暴露到公网。

## 三、运维命令

```bash
ssh node1 'systemctl --user is-active raytest-dashboard'
ssh node1 'systemctl --user status -l raytest-dashboard'
ssh node1 'journalctl --user -u raytest-dashboard -f'
ssh node1 'systemctl --user restart raytest-dashboard'    # 改完代码后（或直接重跑 install.sh）
ssh node1 'curl -s 127.0.0.1:8501/api/health'
ssh node1 'curl -s 127.0.0.1:8501/api/state' | python3 -m json.tool | head -40
```

## 四、目录 / 接口

```
dashboard/
├── app.py                 FastAPI 应用 + 2s 资源采样 + ray status 采样（≥5s/超时 5s）
├── static/
│   ├── index.html         区域结构（含历史对比表 + 两张散点图）
│   ├── style.css          暗色主题、栅格、响应式（无外部字体/图标）
│   └── app.js             SSE 订阅 + 300 点环形采样 + 手写 canvas 折线/散点图
├── dashboard.service      systemd user 单元模板（@PROJECT_DIR@ 占位）
├── install.sh             幂等安装/升级脚本
├── tools/make_fake_run.py 生成假运行目录 / 假历史 run 列表（绝不写 /tmp/raytest）
└── README.md
```

| 接口 | 说明 |
| --- | --- |
| `GET /api/health` | 永远 200，只表示面板自身存活（含 `run_dir_exists` / `ray_available`） |
| `GET /api/state?lines=N` | 一次性合并状态（§1–§5 的全部数据），`N∈[1,200]` |
| `GET /api/stream?lines=N` | SSE，默认 1s 一帧，带 `retry: 3000`；断开后 EventSource 自动重连 |
| `GET /api/log?lines=N` | 只读 `experiment.log` 尾部 N 行（≤200）；0 字节时 `empty: true` |
| `GET /api/runs` | 历史 run 列表 `{runs, algo_summary, runs_error, runs_source, runs_path, runs_count}`；可选 `?algo=simhash\|minhash` 过滤、`?refresh=1` 跳缓存 |
| `GET /` `GET /static/*` | 页面与静态资源（`Cache-Control: no-store`，改完刷新即生效） |

状态 JSON 顶层：`app` / `experiment` / `workers` / `result` / `log` / `resources`，
外加 `algorithm` / `algorithm_source` / `algorithm_label` / `algorithm_desc` / `params` /
`params_source`（同样的内容在 `experiment` 里也有一份，老字段一个没删）。

`/api/runs` 的每一行是 `dedup_runs` 的扁平列（`run_id/algo/mode/docs/bits/band_bits/hamming/
bands/rows/threshold/verify/edges/kept/dropped/pair_recall/pair_precision/edge_fp_cross_base/
groups_extra_kept/groups_missing/components_merged/neg_false_drop_rate/border_dropped/border_total/
seconds/docs_per_sec/subset_table/corpus_sha256/...`），不存在的能力字段是 `null`；
`algo_summary` 按 algo 汇总 `rows/runs/modes/docs_max/docs_min/max_docs_per_sec/
best_pair_recall/best_pair_precision/hamming/bands/verify/latest_run/latest_at`。

读表失败（没装 lancedb / 没有 `dedup_runs` 表 / 目录不可读）时：HTTP 仍是 200，
`runs` 为空数组、`runs_error` 是中文原因，页面只显示提示文案。

## 五、数据契约（严格按主实验进程写入格式读取）

| 文件 | 用途 | 缺失/损坏时 |
| --- | --- | --- |
| `/tmp/raytest/progress.json` | run_id、**algorithm**、阶段、加权 percent、吞吐、漏斗字段、events | 页面显示「等待实验启动」，不报错 |
| `/tmp/raytest/workers/*.json` | 每 worker 的 stage/批次/文档/吞吐 → 任务分配表 + 批次矩阵 | 矩阵显示「等待 worker 上报」 |
| `/tmp/raytest/experiment.log` | 尾部 200 行（seek 到末尾回退 ≤256KB） | 0 字节 → 「暂无日志」；不存在 → 「等待 experiment.log」 |
| `/tmp/raytest/result.json` | 最终报告（含 algorithm + 算法参数）→ 「任务输出」指标卡 | 显示「尚无 result.json」 |
| `/tmp/raytest/runs/<run_id>.json` | 每次 run 的报告副本（形状同 result.json） | 只在收尾后用于对照，缺失无影响 |
| `/tmp/raytest/lance`（`RAYTEST_LANCE_DIR`） | LanceDB：`dedup_runs` 历史表 → 「历史对比」面板 | 没装 lancedb / 没表 → 面板显示提示，不报错 |

`progress.json` 字段用法：

| 字段 | 说明 |
| --- | --- |
| `algorithm` | `simhash` / `minhash`；头部 badge 与阶段文案据此切换。缺失时回落到 `result.json.algorithm`，再缺就显示「未知」 |
| `percent` | **优先**用于顶部总进度条（已按 weight 加权）；缺失才用 `docs_done/docs_total` 兜底 |
| `stages[]` | 阶段列表**按文件里的顺序动态渲染**（不假定固定 5 段）；`fingerprint`/`signature` 是按篇计数的阶段，用全局 docs/s 估 ETA |
| `stages[].weight` | 0~1 权重，页面在阶段行上以 `w=` 小标签显示 |
| `stages[].total` | 为 `null` 时该阶段显示「进行中」+ 流动条，**不显示 0%** |
| `verify_total` / `verify_kept` / `edges` / `docs_kept` / `docs_dropped` | 「任务输出」区的漏斗卡片（候选对 → 复核后 → 最终丢弃）；都可缺失，缺失显示 `—` |
| `updated_at` | 数据新鲜度（秒级；也兼容毫秒） |

算法参数（头部「算法参数」条与任务输出里的「参数」行）：

| 算法 | 参数 | 来源 |
| --- | --- | --- |
| simhash | `bits` / `band_bits` / `hamming` / `shingle` / `verify` | progress.json 有就用，否则 result.json |
| minhash | `bands` / `rows` / `threshold` / `shingle` / `verify` | 同上 |

环境变量：

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `RAYTEST_RUN_DIR` | `/tmp/raytest` | 运行时目录（progress/workers/log/result） |
| `RAYTEST_LANCE_DIR` | `/tmp/raytest/lance` | LanceDB 根目录（`dedup_runs` 历史表） |
| `RAYTEST_RUNS_JSON` | 空 | 假历史 run 列表；设置后 `/api/runs` **优先**读它，完全不碰 LanceDB |
| `RAYTEST_RUNS_CACHE_SEC` | `15` | `/api/runs` 缓存秒数（表只在 run 收尾时变化） |
| `RAYTEST_RAY_BIN` | venv 里的 ray | `ray status` 用的可执行文件 |
| `RAYTEST_HOST` / `RAYTEST_PORT` | `0.0.0.0` / `8501` | 监听地址 |
| `RAYTEST_IDLE_SEC` | `15` | 「疑似空闲」阈值 |

容错策略：JSON 按 `(mtime, size)` 缓存后解析；`invalid_json`（并发写/半截）时保留上一次成功
解析的数据、只在状态里标记错误；所有数值字段做 `null`/类型校验，异常一律显示 `—`。

## 六、页面说明

1. **顶部状态条**
   - 算法 badge（`simhash` 青 / `minhash` 紫 / `未知` 灰虚线）+ 中文副标题（指纹·汉明距离 /
     签名·相似度阈值）；旁边「算法参数」条按算法只显示相关参数
     （simhash：`bits · band · ham · sh · verify`；minhash：`bands · rows · t · sh · verify`）；
   - run_id、当前阶段（中文映射）、**阶段 x/N 圆点**（N 来自 progress.json 的阶段数；
     灰=未开始 / 蓝=进行中 / 绿=已完成）、总进度条（`percent` 优先）、整体吞吐、
     数据新鲜度（>15s 黄「数据陈旧」、>60s 红）、面板健康、Ray 集群连接状态。
2. **任务分配**
   - worker 表**按 stage 分组**，每组一行表头：阶段中文名 + 原始名 + `N 个 worker · 平均 X docs/s · 合计 Y 篇 / Z 批`；
   - 表格列：worker / pid / node / 阶段 / 批次 / 文档 / 吞吐条形 / 最近批次 / 更新时间；
   - 阶段筛选 chips（中文名，`data-stage` 用原始值）；表尾合计；
   - **批次矩阵**同样按 stage 分组，每 worker 一行、方块数 = 已处理批次、颜色 = 是否有进展；
     >15s 无更新标「疑似空闲」（若 *全部* worker 一起过期，提示「疑似写入端未刷新」而不是逐个误报）。
3. **进展**
   - 整体进度条（`percent`）+ 各阶段独立进度条：中文名、原始名、`w=`权重、`done / total · %`、
     按**该阶段自己的实测速率**算的 ETA；状态色灰/蓝/绿；
   - 吞吐/进度折线图（近 300 点，含近 1 分钟均值线）；
   - ETA 卡片：整体 ETA、当前阶段 ETA、近 1 分钟吞吐、已处理。
4. **任务输出**
   - **漏斗**：候选对 `verify_total` → 复核后保留边 `verify_kept` → 最终丢弃 `docs_dropped`，
     另附最终保留 `docs_kept`、重复边 `edges`；
   - `result.json` 的 ray/local 指标卡；`experiment.log` 尾部日志（等宽、自动滚动开关、50/100/200 行可切，
     0 字节显示「暂无日志」）；`progress.json` 事件流（倒序、按 level 着色）。
5. **服务器资源**：CPU 总+每核、内存、load average、磁盘、网络收发速率、`ray status` 解析出的
   Ray CPU / object_store / 节点数；最近 300 点（2s 采样）历史曲线。
6. **历史对比**（读 LanceDB `dedup_runs`，或 `RAYTEST_RUNS_JSON` 假数据）
   - 表格 16 列：run_id / algo / mode / docs / 参数（simhash 显示 `128b · band 16 · ham≤12`，
     minhash 显示 `8×16 t=0.80`）/ verify / pair_recall / pair_precision / 跨组误边 /
     合并分量 / 丢失组 / kept / **Δkept** / **Δedges**（同一 run 的 ray − local）/ seconds / docs_per_sec；
     点表头排序（再点一次反向），chips 按 algo 过滤（全部 / simhash / minhash，带行数）；
     同一 run 的 ray 与 local 两行相邻，local 行 run_id 颜色更淡；
   - 图 1：`pair_recall × pair_precision` 散点——颜色 = 算法，实心 = ray / 空心 = local，
     半径 = docs（对数），鼠标悬停显示该 run 的关键数字；
   - 图 2：`docs_per_sec × docs`——横轴对数、点同图 1，同算法同 mode 的点按 docs 连线（看规模效应）；
   - 降级：`runs_error` 非空时面板只显示「历史数据不可用：<原因>」，其余区域照常工作。

布局：≤1180px 单列（进展与资源上下堆叠），日志与事件流 ≤980px 上下堆叠；`main` 用
`minmax(0,1fr)` 栅格 + 逐层 `min-width:0` 收口，卡片内部自己滚动，所以在 1400×900 与 900×700
下页面都不会横向溢出（已用 Chrome CDP 量化验证：`document.scrollWidth == clientWidth`、0 个越界元素）。

## 七、用假数据演练（不碰真实实验）

`tools/make_fake_run.py` 只写你给的目录，并显式拒绝 `/tmp/raytest`：

```bash
# 1) 造一份静态假数据（默认 simhash；--algo minhash 换成签名/分带入桶/Jaccard 估计 + b×r,t）
ssh node1 'cd ~/code/raytest && .venv/bin/python dashboard/tools/make_fake_run.py --run-dir /tmp/raytest-fake'
ssh node1 'cd ~/code/raytest && .venv/bin/python dashboard/tools/make_fake_run.py --run-dir /tmp/raytest-fake --algo minhash'
#    想要"活"的（每 2s 推进，模拟运行中的实验）：
ssh node1 'cd ~/code/raytest && .venv/bin/python dashboard/tools/make_fake_run.py --run-dir /tmp/raytest-fake --algo minhash --animate --duration 600'
#    演练 0 字节日志：
ssh node1 'cd ~/code/raytest && .venv/bin/python dashboard/tools/make_fake_run.py --run-dir /tmp/raytest-fake --empty-log'

# 2) 造一份假历史 run 列表（simhash 7 行 + minhash 4 行），给没有 lancedb 的环境测 /api/runs
ssh node1 'cd ~/code/raytest && .venv/bin/python dashboard/tools/make_fake_run.py --runs-json /tmp/raytest-fake/runs.json'

# 3) 另起一份面板指向它（真实服务/真实数据都不动）
#    RAYTEST_LANCE_DIR 指向不存在的目录 = 演练「没有表」的降级；
#    RAYTEST_RUNS_JSON 优先级高于 LanceDB，历史面板改用假数据
ssh node1 'cd ~/code/raytest && RAYTEST_RUN_DIR=/tmp/raytest-fake RAYTEST_PORT=8503 \
  RAYTEST_LANCE_DIR=/tmp/raytest-fake/lance RAYTEST_RUNS_JSON=/tmp/raytest-fake/runs.json \
  nohup .venv/bin/python dashboard/app.py > /tmp/dash-8503.log 2>&1 &'

# 4) 验收
ssh node1 'curl -s 127.0.0.1:8503/api/state | python3 -m json.tool | head -40'
ssh node1 'curl -s 127.0.0.1:8503/api/runs | head -c 800'
ssh node1 'curl -s -o /dev/null -w "%{http_code}\n" 127.0.0.1:8503/'

# 5) 本地再开一条隧道看效果
ssh -N -L 8503:127.0.0.1:8503 node1     # 浏览器打开 http://127.0.0.1:8503/
```

演练「没装 lancedb」的降级（本机 Mac 就没有这个依赖）：在远端 venv 里临时屏蔽模块即可——

```bash
ssh node1 'cd ~/code/raytest && .venv/bin/python -c "
import sys; sys.modules["lancedb"] = None; sys.path.insert(0, "dashboard")
import app; print(app.read_runs()["runs_error"])"'
```

切回真实实验：删掉 8503 那个进程即可（真实服务一直指向 `/tmp/raytest`，从未被改动）。
清理假数据：`rm -rf /tmp/raytest-fake`。

## 八、注意事项 / 假设

- 服务只监听 `0.0.0.0:8501`，**没有鉴权**——靠内网/SSH 隧道隔离。
- `ray status` 用 `RAYTEST_RAY_BIN` 指向 venv 里的 ray（默认
  `/home/lee/code/raytest/.venv/bin/ray`）；实测单次约 1.2s，因此放在独立线程、最短 5s 一次，
  失败只把状态标成「不可用」，不影响其它区域。
- 「疑似空闲」阈值由 `RAYTEST_IDLE_SEC`（默认 15s）控制。若主实验的 worker 文件不是周期性刷新，
  会看到「全部 worker 无更新（疑似写入端未刷新）」的提示——这不是页面 bug。
- 数据新鲜度基于 `progress.json` 的 `updated_at`；整体陈旧时暂停 worker 空闲判定，避免全表误报。
- 前端折线图默认 1s 一帧、保留 300 点（≈5 分钟）；服务端资源环形缓冲 300 点 / 2s（≈10 分钟）。
- 阶段中文名映射：fingerprint=指纹计算、signature=签名计算、compare=桶内比较、
  resolve=并查集归并、done=完成；随算法变化的两个：
  simhash 的 shuffle=切带分发、verify=Jaccard 复核；minhash 的 shuffle=分带入桶、verify=Jaccard 估计。
  未知阶段一律原样显示（不假定只有 5 个固定阶段）。
- `fingerprint` / `signature` 是**按篇计数**的阶段，只有它们的 `done/total` 能直接用全局 docs/s 估 ETA；
  其余阶段（桶/边/分量）由前端按该阶段自己的实测速率算。
- 历史对比默认读 `RAYTEST_LANCE_DIR/dedup_runs`；实测 8 行的读表耗时约 5ms（含 lancedb 首次
  import 约 1s），因此做了 15s 的进程内缓存（`RAYTEST_RUNS_CACHE_SEC`），换 run 后最多 15s 可见。
- `dedup_runs` 里 minhash 行只用 `bands/rows/threshold`，simhash 行只用 `bits/band_bits/hamming`；
  缺的就是 `null`，页面显示 `—`，不做 0 填充。
- systemd 单元里 **`PrivateTmp=false` 必须保留**，否则面板读不到 `/tmp/raytest`。
