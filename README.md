# minhash-refresh

Agent skill：**瘦身 MinHash 去重流水线（1M 验证）+ 进度监控协议**。

对应 skill 标识 `minhash-slim-dedup`。仓库只放 skill 本体（指令 + 脚本 + 参考文档）；
它操作的流水线代码在另一处（`raytest` 仓库的 `dedup_slim.py` / `progress.py` / `dashboard/`），
运行环境是 `node1` Ray 集群。

## 安装

```bash
git clone git@github.com:li-clement/minhash-refresh.git
cd minhash-refresh
./install.sh                 # 复制到 ~/.agents/skills/minhash-slim-dedup
```

或者手动软链（`git pull` 后自动生效）：

```bash
ln -s "$PWD/minhash-slim-dedup" ~/.agents/skills/minhash-slim-dedup
```

装好后在会话里会以 `minhash-slim-dedup` 出现。触发场景：跑瘦身 minhash / 1M 去重 /
对比 fat 与 slim / 看或改监控面板 / 接进度上报 / 判断两次 run 的 kept 是否逐字节一致。

## 里面有什么

```
minhash-slim-dedup/
├── SKILL.md                 何时用、环境事实、五阶段与不变量、跑/看/判读、坑表
├── references/
│   ├── algorithm.md         数据流、预聚合/瘦带行/排序扫描、1M 四组实测、成本模型、扩展点
│   ├── monitoring.md        progress.json / workers/*.json 契约、写端规则、面板读端行为、演练
│   └── pitfalls.md          23 条「现象 → 根因 → 修法」，带实测证据
└── scripts/
    ├── preflight.sh         服务/资源/残留 job/表/语料/面板/progress 体检（只读）
    ├── run_slim.sh          远端起跑 + 本地轮询 + 结果归档（支持 --isolated / --stop）
    ├── watch.sh             进度单行输出 / --full 阶段条 / --json
    ├── compare_runs.py      fat vs slim 决策对拍（硬指标不一致退出 1）
    ├── drill_progress.py    用真实写端 API 造假 run，验证面板契约，不碰 Ray
    └── tunnel.sh            8501 / 8503 隧道
```

## 快速开始

```bash
S=~/.agents/skills/minhash-slim-dedup/scripts
$S/preflight.sh                                          # 只读体检
$S/run_slim.sh --mode local --table subset_10k --tag smoke10k
$S/watch.sh                                              # 另开终端，stage=done 自动退出
$S/run_slim.sh --mode ray --table subset_1000k --pivot-over 1000000000 --tag f4b_1m
```

通过线（缺一条就不算「与 fat 等价」）：

- `kept` / `dropped` 与 fat 逐项相等（10K：3,648 / 6,354；1M：383,708 / 616,292）
- `groups_missing = 0`、`components_merged = 0`、`neg_false_drop_rate = 0`、`edge_fp_cross_base = 0`
- `border_dropped` 与 fat 同值（1M：10,758）
- 召回看 `pair_recall_edge_canon`（1M：0.9999），不是 `pair_recall`（0.6149）

## 实测数字（2026-11-10，node1，`subset_1000k`）

| | fat minhash | slim local（1 核） | slim ray（20 核） |
|---|---|---|---|
| 端到端 | 1,227.6 s | 259.2 s | 78.7 s（复跑 81.5 s） |
| 带行中间态 | 6.34 GB | 96.0 MB | 96.0 MB |
| kept / dropped | 383,708 / 616,292 | 同 | 同 |
| 桶内比较 | 7.84e6（band_hits） | 3.64e6 | 3.64e6 |

## 前置条件

- `ssh node1` 可用；`ray-head` 与 `raytest-dashboard`（`:8501`）两个 systemd user 服务在跑
- `/tmp/raytest` 下有 `corpus_1m.jsonl`、LanceDB 表（`corpus_1m`、`subset_1000k`、`subset_10k`）
- raytest 仓库里有 `dedup_slim.py`、`progress.py`、`dashboard/app.py`

缺任何一项，`preflight.sh` 会直接指出来。

## 维护

- 改完 `SKILL.md` 或脚本先自测：`scripts/drill_progress.py --algo slim`（契约自检）+ 10K 冒烟对拍。
- 数字更新后同步三处：`SKILL.md` 的通过线、`references/algorithm.md` 的实测表、本 README 的表格。
