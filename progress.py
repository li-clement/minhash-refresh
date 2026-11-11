"""实验进度上报：驱动进程写全局状态，Ray worker 各自写自己的文件。

目录布局（与 dashboard 的读取端约定）:
  /tmp/raytest/progress.json          全局状态（原子写）
  /tmp/raytest/workers/<id>.json      每个 worker/actor 一份（各自写，无锁竞争）
  /tmp/raytest/experiment.log         实验 stdout（由启动脚本重定向）
  /tmp/raytest/result.json            最终报告
"""
from __future__ import annotations

import json
import os
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_DIR = Path(os.environ.get("RAYTEST_PROGRESS_DIR", "/tmp/raytest"))

# 「按篇计数」的阶段名：simhash 的指纹、minhash 的签名。两算法共用同一套进度上报。
DOC_STAGE_NAMES = ("fingerprint", "signature")


_WRITE_LOCK = threading.Lock()


def _atomic_write_json(path: Path, obj) -> None:
    # 临时文件名必须唯一：监控线程和主线程会并发写同一份 progress.json
    tmp = path.with_suffix(f".tmp{os.getpid()}-{threading.get_ident()}-{uuid.uuid4().hex[:6]}")
    with _WRITE_LOCK:
        with open(tmp, "w") as f:
            json.dump(obj, f, ensure_ascii=False)
        os.replace(tmp, path)


@dataclass
class WorkerReporter:
    """Ray worker 侧：每个批次写一次自己的文件。"""

    stage: str
    root: Path = DEFAULT_DIR
    worker_id: str = field(default_factory=lambda: f"w-{uuid.uuid4().hex[:8]}")
    pid: int = field(default_factory=os.getpid)
    node: str = field(default_factory=lambda: os.environ.get("RAY_NODE_IP", ""))
    batches_done: int = 0
    docs_done: int = 0
    t_start: float = field(default_factory=time.time)
    last_batch: dict | None = None
    _t_write: float = 0.0

    def __post_init__(self):
        try:  # 优先用 Ray actor id，便于和 dashboard 对齐
            import ray

            self.worker_id = ray.get_runtime_context().get_actor_id() or self.worker_id
        except Exception:
            pass
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        # 注意：这里不落盘。Ray Data 的 actor pool 会预启动一批 actor，其中一部分一次都没跑过批次；
        # 如果构造时就写文件，面板的「任务分配」里会出现一堆 0 篇/0 批的空 worker。
        # 心跳：即使一批算很久，页面上也不会被误判为"陈旧/疑似空闲"
        threading.Thread(target=self._heartbeat, daemon=True).start()

    def _heartbeat(self, interval: float = 5.0) -> None:
        while True:
            time.sleep(interval)
            if self.batches_done == 0:
                continue
            try:
                self._write()
            except Exception:
                pass

    def report(self, docs: int, start: int, end: int) -> None:
        with self._lock:
            self.batches_done += 1
            self.docs_done += docs
            self.last_batch = {"start": start, "end": end, "docs": docs}
            # 第一批立即落盘（让面板尽快看到这个 worker），之后每 0.5s 最多一次：
            # 分组可能上百万个，逐次写文件会把时间全花在 IO 上
            if self.batches_done == 1 or time.time() - self._t_write >= 0.5:
                self._write_locked()

    def _write(self) -> None:
        with self._lock:
            self._write_locked()

    def _write_locked(self) -> None:
        self._t_write = time.time()
        elapsed = max(1e-6, time.time() - self.t_start)
        _atomic_write_json(
            self.root / "workers" / f"{self.worker_id}.json",
            {
                "worker_id": self.worker_id,
                "pid": self.pid,
                "node": self.node,
                "stage": self.stage,
                "status": "running",
                "batches_done": self.batches_done,
                "docs_done": self.docs_done,
                "docs_per_sec": round(self.docs_done / elapsed, 1),
                "last_batch": self.last_batch,
                "updated_at": time.time(),
            },
        )


class ProgressReporter:
    """驱动测：全局阶段、吞吐、事件。"""

    def __init__(self, root: Path = DEFAULT_DIR, docs_total: int = 0, algorithm: str = "simhash"):
        self.root = Path(root)
        wdir = self.root / "workers"
        wdir.mkdir(parents=True, exist_ok=True)
        for f in wdir.glob("*.json"):  # 清掉上一次运行的 worker 文件，避免污染本次统计
            try:
                f.unlink()
            except OSError:
                pass
        self.run_id = time.strftime("%Y%m%d-%H%M%S")
        self.algorithm = algorithm
        self.docs_total = docs_total
        self.docs_done = 0
        self.stage = "init"
        self.stages: list[dict] = []
        self.events: list[dict] = []
        self.extra: dict = {}
        self.t0 = time.time()
        self._t_last = self.t0
        self.flush()

    def event(self, msg: str, level: str = "info") -> None:
        self.events.append({"ts": time.time(), "level": level, "msg": msg})
        self.events = self.events[-200:]
        print(f"[progress] {level}: {msg}", flush=True)

    def declare(self, stages: list[dict]) -> None:
        """开工前把全部阶段登记好（进度条顺序、权重固定，不会随阶段推进跳变）。"""
        self.stages = [
            {"name": s["name"], "done": s.get("done", 0), "total": s.get("total"),
             "weight": float(s.get("weight", 1.0))}
            for s in stages
        ]
        self.flush()

    def set_stage(self, stage: str, total: int | None = None, done: int | None = None,
                  weight: float | None = None) -> None:
        """切换当前阶段；已登记的阶段只更新传入的字段。"""
        self.stage = stage
        for st in self.stages:
            if st["name"] == stage:
                if total is not None:
                    st["total"] = total
                if done is not None:
                    st["done"] = max(st["done"], done)
                if weight is not None:
                    st["weight"] = weight
                break
        else:
            self.stages.append({"name": stage, "done": done or 0, "total": total,
                                "weight": weight if weight is not None else 1.0})
        self._t_last = time.time()
        self.flush()

    def add_stage_total(self, stage: str, total: int) -> None:
        for s in self.stages:
            if s["name"] == stage and not s.get("total"):
                s["total"] = total
                self.flush()

    def add_stage_done(self, stage: str, n: int = 1) -> None:
        for s in self.stages:
            if s["name"] == stage:
                s["done"] = max(s["done"], s["done"] + n)
        self._t_last = time.time()
        self.flush()

    def set_docs_done(self, n: int) -> None:
        """按篇计数的阶段直接推进文档进度（本地单进程模式没有 worker 文件可汇总）。"""
        self.docs_done = max(self.docs_done, int(n))
        self.flush()

    def update(self, **kwargs) -> None:
        self.extra.update(kwargs)
        self.flush()

    # ---- 监控线程：把各 worker 文件汇总成全局进度 ----
    def _scan_workers(self) -> dict:
        out = {}
        wdir = self.root / "workers"
        if not wdir.is_dir():
            return out
        now = time.time()
        for f in wdir.glob("*.json"):
            try:
                data = json.loads(f.read_text())
            except Exception:
                continue
            data["stale"] = (now - float(data.get("updated_at", 0))) > 15
            out[data.get("worker_id", f.stem)] = data
        return out

    def start_monitor(self, interval: float = 1.0) -> None:
        if getattr(self, "_monitor", None):
            return

        def loop():
            high = {}
            while not self._stop.is_set():
                workers = self._scan_workers()
                per_stage: dict[str, int] = {}
                for w in workers.values():
                    if float(w.get("updated_at", 0)) < time.time() - 120:
                        continue  # 上一次运行的残留
                    per_stage[w.get("stage", "?")] = per_stage.get(w.get("stage", "?"), 0) + int(w.get("docs_done", 0))
                for name, v in per_stage.items():
                    high[name] = max(high.get(name, 0), v)  # 只增不减（actor 停止上报后不回落）
                for st in self.stages:
                    if st["name"] in high:
                        st["done"] = max(st["done"], high[st["name"]])
                # 文档进度取自「按篇计数」的那个阶段：simhash 是 fingerprint，minhash 是 signature。
                # 之前写死 fingerprint，minhash 跑起来 docs_done 恒为 0（面板头部 0/10002）。
                for name in DOC_STAGE_NAMES:
                    if name in high:
                        self.docs_done = max(self.docs_done, high[name])
                        break
                self.extra["workers_alive"] = sum(1 for w in workers.values() if not w["stale"])
                self.extra["workers_total"] = len(workers)
                self.flush()
                self._stop.wait(interval)

        self._stop = threading.Event()
        self._monitor = threading.Thread(target=loop, daemon=True)
        self._monitor.start()

    def stop_monitor(self) -> None:
        stop = getattr(self, "_stop", None)
        if stop:
            stop.set()

    def finish(self) -> None:
        """收尾：把所有阶段标记为完成，避免面板停在半路（worker 文件在收尾时会掉线）。"""
        for st in self.stages:
            st["total"] = st["total"] or 1
            st["done"] = st["total"]
        if self.docs_total:
            self.docs_done = self.docs_total
        self.stage = "done"
        self.flush()

    def _overall_percent(self) -> float:
        wsum = tot = 0.0
        for st in self.stages:
            w = float(st.get("weight", 1.0))
            wsum += w
            if st.get("total"):
                tot += w * min(1.0, st["done"] / st["total"])
        return round(100.0 * tot / wsum, 1) if wsum else 0.0

    def _active_stage(self) -> str:
        """当前阶段：优先报「最靠下游、且已经在跑」的阶段。

        Ray Data 是流式的，上游 actor 收尾时它的 done 会停在 `worker 存活期间的量`（< total），
        而下游 compare 已经在推进。按「第一个未完成的阶段」报会让面板一直卡在 signature，
        所以从后往前找最后一个「已开始但未完成」的阶段。
        """
        started = [st for st in self.stages if (st.get("done") or 0) > 0]
        for st in reversed(started):
            if st.get("total") is None or st["done"] < st["total"]:
                return st["name"]
        for st in self.stages:
            if st.get("total") is None or st["done"] < st["total"]:
                return st["name"]
        return "done"

    def flush(self) -> None:
        now = time.time()
        elapsed = max(1e-6, now - self.t0)
        _atomic_write_json(
            self.root / "progress.json",
            {
                "run_id": self.run_id,
                "algorithm": self.algorithm,
                "updated_at": now,
                "stage": self._active_stage(),
                "stage_hint": self.stage,
                "stages": self.stages,
                "docs_total": self.docs_total,
                "docs_done": self.docs_done,
                "percent": self._overall_percent(),
                "throughput_docs_per_sec": round(self.docs_done / elapsed, 1),
                "events": self.events[-50:],
                **self.extra,
            },
        )
