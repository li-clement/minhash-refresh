# 坑：现象 → 根因 → 修法

每条都带实测证据。遇到「数字不对」先在这里找，再去翻代码。

## 一、口径类（最容易把结论做废）

### 1. slim 的 `pair_recall` 只有 0.61，看起来召回崩了

- **现象**：`metrics.pair_recall = 0.6149`，但 kept 与 fat 完全一致、`groups_missing = 0`。
- **根因**：1M 真值对 866,668 里有 333,654 对是「同一个精确类内部」的 `(orig, exact)`。
  预聚合把它们并成一类后天然没有边，分母却还包含它们。
- **修法**：看 `pair_recall_edge_canon`（0.9999）；报告里同时写 `true_pairs_intra_canonical`。
  这个分解在 `dedup_slim.recall_decomposition()` 里，别删。

### 2. 瘦身倍数一会儿 44×、一会儿 66×

- 44× = 同篇数（等价于「预聚合后的 666,506 篇」）：fat 4,223 B/篇 vs slim 96.0 B/篇。
- 66× = 按原始 1M 篇：fat 6,336 B/篇（12 行 × 528 B）vs slim 96 B/篇。
- 两个都对，混着讲会被追问「到底几倍」。默认说 66×（含预聚合），并注明 44× 是同篇数口径。

### 3. 拿 10K 的 Ray 吞吐外推 100K/1M

- 10K：minhash Ray 19.05s（525 篇/s）比单机 3.35s 慢 5.7×；
- 1M：slm Ray 78.7s 比单机 259.2s 快 3.3×。
- 根因：10K 的 compare 只有 81k 次比较，固定开销（actor 启动、shuffle）占大头；
  1M 的 compare 是 3.6M 次、单核 Python 循环，Ray 只帮了签名那一段。
- 判读：先看 `bucket`（buckets / compared / max_bucket）再谈吞吐；`band_hits` 和真值对同量级才正常。

### 4. 用随机桶模型算 simhash 的 600M 成本

- 模型给 6.1e7 候选对，1M 实测 **5.33e9**（87×）：桶空间只用了 1/3（175,550 / 524,288），
  最大桶 26,828 篇（C(26,828,2)=3.6e8 对，一个桶占 7%）。
- 结论：模板语料的「有效键熵」远低于位宽，任何基于均匀桶假设的容量规划都要除以一个 30–100 的经验因子，
  或者直接用 `skew_1m.py` 量。

### 5. 两份语料混用

`corpus_200k.jsonl` 与 `corpus_1m.jsonl` 是不同快照（seed/组结构都不同）。
`subset_1000k` 不是 200K 的子集。跨快照对比 `pair_recall`、`groups_total`、`kept` 一律无效。

### 6. 空文档

`minhash.signature()` 对空文档返回全 0；两条全 0 签名的估计 Jaccard = 1.0，会互相判重。
本语料没有空文档，所以没暴露；接真实数据前先定策略（丢弃 / 单独成类），否则一批空文本会并成一个大分量。

## 二、监控类

### 7. percent 爬到 ~68% 就不动了，但 compare 还在跑

- **证据**：fat minhash 1M 的 `progress.json`：`compare done=8,544,066 total=12,000,000`、`percent=71.8`，
  而 `BandComparer` 实际已处理约 99 万篇。
- **根因**：`total` 是「带行数」（篇数 × 带数），worker 上报的是「篇数」。done 永远到不了 total。
- **修法**：让 total 与 done 同单位。slim 用 `total = b × U`、`done = 已扫带行数`。
  不要在面板上「改大 percent」，那是把错的数据画得好看。

### 8. 顶部阶段是 compare，日志却写「签名计算 99.5」

- **根因**：`stage`（`_active_stage`）取「最下游、已开始、未完成」的阶段；`stage_hint` 是最后一次
  `set_stage()`。Ray Data 是流式的，上游 actor 先收尾、下游 compare 已经在跑，两个字段本来就会分叉。
- **判读**：`stage_hint` 表示「代码执行到哪一行」，`stage` 表示「数据流到哪一阶段」。不要据此认为卡住。

### 9. `docs_done` 恒为 0（minhash）

- **根因**：旧 `_scan_workers` 写死 `high["fingerprint"]`；minhash 的阶段叫 `signature`。
- **修法**：`DOC_STAGE_NAMES = ("fingerprint", "signature")`，按顺序取第一个有值的。
  新增按篇阶段时同步加进去。

### 10. 「任务分配」里一堆 0 篇、0 批的空 worker

- **根因**：`WorkerReporter.__init__` 就落盘；Ray Data 的 actor pool 预启动的 actor 有些一次批次都没跑。
- **修法**：首批 `report()` 时才写（`batches_done == 0` 时心跳也跳过）。

### 11. 作业失败：`FileNotFoundError: progress.tmp16052 -> progress.json`

- **根因**：原子写的临时文件名固定为 `progress.tmp<pid>`；监控线程和主线程同时 `flush()`，
  一个线程刚 replace 完，另一个线程的 tmp 已被换走。
- **修法**：临时名带 `pid + thread_ident + uuid`，`os.replace` 前不共享路径。

### 12. 面板显示的是上一轮的数字

- **根因**：作业 failed/被停时，旧的 `result.json` 还在；或者有人用固定的 `--run-id` 覆写了 `runs/<id>.json`。
- **判读**：先看 `progress.json.run_id` 与 `updated_at` 年龄（>15s 且 `stage != done` 就是没在跑）；
  再对 `result.json` 的 mtime。`run_experiment.sh` 在作业失败时会显式提示「result.json 是上一轮的」。

### 13. 指标卡一直「尚无 result.json」

- **根因**：面板只认 `/tmp/raytest/result.json`；slim 用 `--out result_slim_<tag>.json` 时它读不到。
- **修法**：`run_slim.sh` 统一写 `/tmp/raytest/result.json`，收尾再复制到 `runs/<tag>.json` + 本地归档。

### 14. 历史对比面板里找不到 slim 的 run

- **根因**：历史列表来自 LanceDB `dedup_runs`，只有 `run_experiment.py` 写；`dedup_slim.py` 不写表。
- **修法**：要么接受（结果 JSON 归档到本地 `results/`），要么自己按 `write_results` 的形状补写；
  不要以为「没写表 = 没跑成功」。

### 15. `progress.json.run_id` 与报告 `run_id` 不一致

- 前者是启动时间戳（`ProgressReporter` 生成），后者是 `--run-id`（slim 默认 `slim_<mode>`）。
- 面板用前者标识「这次运行」，历史面板用后者。对不上不是 bug，但诊断时要说明用的是哪个。

## 三、算法实现类

### 16. Ray 模式报「签名数 6 万 != 唯一文档数 66 万」

- **根因**：`sign_ray` 把「代表在 canon 里的下标」当 `doc_id` 传给 `MinHashSigner`；
  列名写成 `uid` 或忘了带 `doc_id`，Ray 侧拿不到主键，行会丢/错位。
- **修法**：保持 `{"doc_id": i, "text": ...}`；收尾断言 `got == len(canon)` 不能删。

### 17. pivot 星形开着，kept 比 fat 多 7 篇

- 1M：`--pivot-over 64` → kept 383,715；关掉（`1000000000`）→ 383,708，与 fat 相等。
- 星形只省 18% 比较次数（3.64M → 2.99M），墙钟 78.7s vs 78.5s，却丢 76 条真值对。
- **结论**：1M 上关掉；只有 `max_bucket` 上千（600M 分带后）才考虑，且必须记账损失。

### 18. 精确预聚合后「唯一数」与预期不符

1M 的 exact 占 33.3%，唯一数 666,506；10K 上是 6,630。归一化只去掉空白（`\s+`）。
如果换成正则更激进的归一化，唯一数会变少、下游所有指标都会变 —— 那就不是同一份实验了。

### 19. `--pivot-over 1e9` 一启动就退出

- **现象**：进程 0.2s 结束，日志里只有 argparse 帮助 + `invalid int value: '1e9'`。
- **根因**：`dedup_slim.py --pivot-over` 是 `type=int`，只吃十进制整数。
- **修法**：用 `1000000000`（关掉星形）。同理 `--bands/--rows/--k/--cpu` 是 int，
  只有 `--threshold` 是 float。

### 20. 判断「跑完了」不能只看进程退出

`slim` 的 `--out` 是在 `run()` 返回后才写；进程被 kill / 异常退出时没有 JSON。
用 `run_slim.sh` 的轮询（pid + result 双条件），或看 `progress.json.stage == done` 且 `updated_at` 新鲜。

## 四、运维类

### 21. 上一轮的 job 还在抢 CPU

`preflight.sh` 会列 RUNNING/PENDING；确认要停再 `--stop-jobs`。两个实验并行时，
compare 段都是单核 Python，互相抢 1 个核就能把计时打歪 10%+。

### 22. 改了代码但集群上没变

本地改完必须 `rsync -az <file> node1:code/raytest/`；改 `dashboard/` 后还要
`systemctl --user restart raytest-dashboard`。静态页（`index.html`、`app.js`）同样要 rsync。

### 23. 在 Mac 上直接跑

Mac 上没有 venv、没有 LanceDB 表、也没有 Ray 集群。所有实验命令都在 `node1` 上执行，
本地只放 skill、脚本和归档结果。
