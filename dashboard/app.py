#!/usr/bin/env python3
"""raytest 任务监控服务（FastAPI + uvicorn，端口 8501）。

同时适配两种去重算法（simhash / minhash）：阶段词表、参数展示、历史对比都跟着
progress.json / result.json 里的 `algorithm` 字段切换，缺字段时降级为「未知」。

设计约束
--------
* **只读**：只读取 /tmp/raytest 下的实验产物，绝不写入/删除实验目录，也绝不向
  Ray 集群发起任何写操作（不 start/stop/restart，不碰 6379/8265）。
* **容错**：progress.json / workers/*.json / result.json / experiment.log 缺失、
  半截、损坏、类型异常都不抛错，最多体现在状态里的 *_error 字段上；前端会显示
  「等待实验启动」。历史 run 读 LanceDB 时没装 lancedb / 表不存在 / 路径不可读都
  只返回空列表 + `runs_error`，不 500。
* **采样**：后台线程每 2s 用 psutil 采 CPU/内存/load/磁盘/网络；另一个线程跑
  `ray status`（最短间隔 5s、超时 5s）解析集群资源，失败只标记不可用。
* **接口**：/api/stream（SSE）、/api/state、/api/runs、/api/health、/api/log。

环境变量（都有默认值，systemd 单元里显式给出）：
  RAYTEST_RUN_DIR     运行时目录，默认 /tmp/raytest
  RAYTEST_LANCE_DIR   LanceDB 根目录（dedup_runs 历史表），默认 /tmp/raytest/lance
  RAYTEST_RUNS_JSON   假历史数据文件；设置后 /api/runs 优先读它（不碰 LanceDB）
  RAYTEST_RUNS_CACHE_SEC  /api/runs 缓存秒数，默认 15
  RAYTEST_RAY_BIN     ray 可执行文件，默认 /home/lee/code/raytest/.venv/bin/ray
  RAYTEST_HOST/PORT   监听地址，默认 0.0.0.0:8501
  RAYTEST_SAMPLE_INTERVAL / RAYTEST_HISTORY_LEN / RAYTEST_SSE_INTERVAL / RAYTEST_IDLE_SEC
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import socket
import subprocess
import threading
import time
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional

import psutil
import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse

APP_VERSION = "1.1.0"
BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

RUN_DIR = Path(os.environ.get("RAYTEST_RUN_DIR", "/tmp/raytest"))
LANCE_DIR = Path(os.environ.get("RAYTEST_LANCE_DIR", "/tmp/raytest/lance"))
RUNS_JSON = os.environ.get("RAYTEST_RUNS_JSON", "").strip()
RUNS_CACHE_SEC = float(os.environ.get("RAYTEST_RUNS_CACHE_SEC", "15"))
RAY_BIN = os.environ.get("RAYTEST_RAY_BIN", "/home/lee/code/raytest/.venv/bin/ray")
HOST = os.environ.get("RAYTEST_HOST", "0.0.0.0")
PORT = int(os.environ.get("RAYTEST_PORT", "8501"))

SAMPLE_INTERVAL = float(os.environ.get("RAYTEST_SAMPLE_INTERVAL", "2.0"))
HISTORY_LEN = int(os.environ.get("RAYTEST_HISTORY_LEN", "300"))
SSE_INTERVAL = float(os.environ.get("RAYTEST_SSE_INTERVAL", "1.0"))
IDLE_SEC = float(os.environ.get("RAYTEST_IDLE_SEC", "15"))

RAY_MIN_INTERVAL = 5.0          # `ray status` 最短调用间隔（硬性要求 >= 5s）
RAY_TIMEOUT = 5.0               # 单次 `ray status` 超时
FRESH_WARN_SEC = 15.0           # 数据新鲜度：>15s 黄色
FRESH_STALE_SEC = 60.0          # >60s 红色
LOG_TAIL_MAX = 200              # 只读日志尾部 200 行
LOG_READ_BYTES = 256 * 1024     # 读日志时最多回退的字节数
START_TS = time.time()

# --------------------------------------------------------------------------- #
# 通用小工具
# --------------------------------------------------------------------------- #


def _num(v: Any) -> Optional[float]:
    """把 JSON 里的数字安全地转成 float；bool/str/None 都返回 None。"""
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, (int, float)):
        try:
            f = float(v)
        except (TypeError, ValueError):
            return None
        if f != f or f in (float("inf"), float("-inf")):
            return None
        return f
    return None


def _int(v: Any) -> Optional[int]:
    f = _num(v)
    return int(f) if f is not None else None


def _str(v: Any, default: str = "") -> str:
    if isinstance(v, str):
        return v
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return str(v)
    return default


class JsonFile:
    """按 (mtime, size) 缓存的 JSON 读取器：容忍缺失/半截/损坏，不重复解析。"""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._key: Optional[tuple[int, int]] = None
        self._data: Any = None
        self._err: Optional[str] = "missing"
        self._mtime: Optional[float] = None

    def read(self) -> tuple[Any, Optional[str]]:
        try:
            st = self.path.stat()
        except FileNotFoundError:
            self._key, self._data, self._err, self._mtime = None, None, "missing", None
            return None, "missing"
        except OSError as exc:
            self._err = f"stat_error: {exc.__class__.__name__}"
            return None, self._err

        key = (st.st_mtime_ns, st.st_size)
        if key == self._key:
            return self._data, self._err

        self._key, self._mtime = key, st.st_mtime
        if st.st_size == 0:
            self._data, self._err = None, "empty"
            return None, self._err
        try:
            raw = self.path.read_bytes()
        except OSError as exc:
            self._data, self._err = None, f"read_error: {exc.__class__.__name__}"
            return None, self._err
        if not raw.strip():
            self._data, self._err = None, "empty"
            return None, self._err
        try:
            self._data = json.loads(raw.decode("utf-8", "replace"))
            self._err = None
        except (json.JSONDecodeError, ValueError):
            # 半截/并发写：保留旧数据（如果上一次解析成功），只报告错误
            self._err = "invalid_json"
        except Exception as exc:  # pragma: no cover - 兜底
            self._data, self._err = None, f"parse_error: {exc.__class__.__name__}"
        return self._data, self._err


_PROGRESS = JsonFile(RUN_DIR / "progress.json")
_RESULT = JsonFile(RUN_DIR / "result.json")
_WORKER_CACHE: dict[str, JsonFile] = {}
_LOG_CACHE: dict[str, Any] = {"key": None, "payload": None}


# --------------------------------------------------------------------------- #
# 资源采样
# --------------------------------------------------------------------------- #


def _mem_bytes(text: str) -> Optional[float]:
    """解析 '111.09GiB' / '0B' / '1.5GB' 这类尺寸字符串为字节数。"""
    m = re.match(r"^\s*([\d.]+)\s*([KMGTPE]?i?B)\s*$", text, re.I)
    if not m:
        return None
    try:
        val = float(m.group(1))
    except ValueError:
        return None
    unit = m.group(2).upper().replace("IB", "I")
    mult = {
        "B": 1, "KB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12, "PB": 1e15,
        "KI": 1024, "MI": 1024 ** 2, "GI": 1024 ** 3, "TI": 1024 ** 4, "PI": 1024 ** 5,
    }.get(unit)
    return val * mult if mult else None


_CPU_LINE = re.compile(r"^\s*([\d.]+)/([\d.]+)\s+CPU\s*$", re.M)
_MEM_LINE = re.compile(r"^\s*(\S+)/(\S+)\s+memory\s*$", re.M)
_OBJ_LINE = re.compile(r"^\s*(\S+)/(\S+)\s+object_store_memory\s*$", re.M)


def parse_ray_status(text: str) -> dict[str, Any]:
    """从 `ray status` 文本里抽出集群资源。解析失败只返回 available=False。"""
    out: dict[str, Any] = {
        "cpu": {"used": None, "total": None, "util": None},
        "memory": {"used": None, "total": None},
        "object_store": {"used": None, "total": None},
        "nodes_active": None,
        "nodes_pending": None,
    }
    m = _CPU_LINE.search(text)
    if m:
        used, total = float(m.group(1)), float(m.group(2))
        out["cpu"] = {"used": used, "total": total, "util": (used / total) if total else None}
    m = _MEM_LINE.search(text)
    if m:
        out["memory"] = {"used": _mem_bytes(m.group(1)), "total": _mem_bytes(m.group(2))}
    m = _OBJ_LINE.search(text)
    if m:
        out["object_store"] = {"used": _mem_bytes(m.group(1)), "total": _mem_bytes(m.group(2))}

    # Node status 段落里的 Active / Pending 计数
    try:
        lines = text.splitlines()
        idx = next(i for i, ln in enumerate(lines) if ln.strip().startswith("Node status"))
        section, cur = {}, None
        for ln in lines[idx: idx + 24]:
            s = ln.strip()
            if s.startswith("Active:"):
                cur = "active"
                continue
            if s.startswith("Pending:"):
                cur = "pending"
                continue
            if s.startswith(("Recent failures", "Resources", "----")):
                if cur and s.startswith(("Recent failures", "Resources")):
                    cur = None
                continue
            if cur and s.startswith("("):
                continue
            if cur and s:
                cnt = 0
                mm = re.match(r"^(\d+)\s+\S+", s)
                cnt = int(mm.group(1)) if mm else 1
                section[cur] = section.get(cur, 0) + cnt
        if "active" in section:
            out["nodes_active"] = section["active"]
        if "pending" in section:
            out["nodes_pending"] = section["pending"]
    except Exception:
        pass
    return out


class ResourceSampler(threading.Thread):
    """每 2s 采一次主机资源，维护 300 点环形缓冲。"""

    def __init__(self) -> None:
        super().__init__(name="resource-sampler", daemon=True)
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._snapshot: dict[str, Any] = {}
        self._history: deque[dict[str, Any]] = deque(maxlen=HISTORY_LEN)
        self._ncpu = psutil.cpu_count(logical=True) or os.cpu_count() or 1

    # --- 采样主体 --------------------------------------------------------
    def _sample(self) -> dict[str, Any]:
        now = time.time()
        try:
            per = psutil.cpu_percent(interval=None, percpu=True)
        except Exception:
            per = []
        per = [round(float(x), 1) for x in per][: self._ncpu]
        total = round(sum(per) / len(per), 1) if per else 0.0
        try:
            vm = psutil.virtual_memory()
            mem = {
                "total": vm.total,
                "used": vm.total - vm.available,   # 与 free -h 的 used 口径一致
                "available": vm.available,
                "percent": round(vm.percent, 1),
            }
        except Exception:
            mem = {"total": None, "used": None, "available": None, "percent": None}
        try:
            la = os.getloadavg()
            load = {"1": round(la[0], 2), "5": round(la[1], 2), "15": round(la[2], 2)}
        except Exception:
            load = {"1": None, "5": None, "15": None}
        try:
            du = psutil.disk_usage("/")
            disk = {"total": du.total, "used": du.used, "free": du.free, "percent": round(du.percent, 1)}
        except Exception:
            disk = {"total": None, "used": None, "free": None, "percent": None}
        try:
            per_nic = psutil.net_io_counters(pernic=True)
            sent = sum(c.bytes_sent for n, c in per_nic.items() if n != "lo")
            recv = sum(c.bytes_recv for n, c in per_nic.items() if n != "lo")
        except Exception:
            sent, recv = 0, 0
        try:
            uptime = now - psutil.boot_time()
        except Exception:
            uptime = None
        return {
            "ts": now,
            "cpu": {"total": total, "per_core": per, "count": self._ncpu},
            "mem": mem,
            "load": load,
            "disk": disk,
            "net": {"bytes_sent": sent, "bytes_recv": recv, "rx_rate": 0.0, "tx_rate": 0.0},
            "uptime_sec": round(uptime, 0) if uptime else None,
        }

    def run(self) -> None:  # pragma: no cover - 线程体
        try:
            psutil.cpu_percent(interval=None, percpu=True)   # 预热基线
        except Exception:
            pass
        prev_net = None
        prev_ts = None
        while not self._stop.wait(SAMPLE_INTERVAL):
            try:
                snap = self._sample()
            except Exception:
                continue
            net = snap["net"]
            if prev_net is not None and prev_ts is not None:
                dt = max(1e-6, snap["ts"] - prev_ts)
                net["rx_rate"] = max(0.0, (net["bytes_recv"] - prev_net[0]) / dt)
                net["tx_rate"] = max(0.0, (net["bytes_sent"] - prev_net[1]) / dt)
            prev_net = (net["bytes_recv"], net["bytes_sent"])
            prev_ts = snap["ts"]
            hist = {
                "t": round(snap["ts"], 3),
                "cpu": snap["cpu"]["total"],
                "mem": snap["mem"]["percent"],
                "load1": snap["load"]["1"],
                "net_rx": round(net["rx_rate"], 1),
                "net_tx": round(net["tx_rate"], 1),
            }
            with self._lock:
                self._snapshot = snap
                self._history.append(hist)
        return

    def stop(self) -> None:
        self._stop.set()

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            snap = dict(self._snapshot)
            snap["history"] = list(self._history)
        return snap


class RaySampler(threading.Thread):
    """独立线程跑 `ray status`：最短 5s 一次、超时 5s，绝不阻塞资源采样。"""

    def __init__(self) -> None:
        super().__init__(name="ray-sampler", daemon=True)
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._state: dict[str, Any] = {
            "available": False, "checked_at": None, "duration_ms": None,
            "error": "尚未采样", "cpu": {"used": None, "total": None, "util": None},
            "memory": {"used": None, "total": None},
            "object_store": {"used": None, "total": None},
            "nodes_active": None, "nodes_pending": None,
        }
        self._last_call = 0.0

    def _call(self) -> None:
        t0 = time.perf_counter()
        env = dict(os.environ)
        env.setdefault("RAY_DATA_DISABLE_PROGRESS_BARS", "1")
        env.setdefault("RAY_USAGE_STATS_ENABLED", "0")
        base = {"available": False, "checked_at": time.time(), "duration_ms": None,
                "cpu": {"used": None, "total": None, "util": None},
                "memory": {"used": None, "total": None},
                "object_store": {"used": None, "total": None},
                "nodes_active": None, "nodes_pending": None}
        try:
            proc = subprocess.run(
                [RAY_BIN, "status"], capture_output=True, text=True,
                timeout=RAY_TIMEOUT, env=env, cwd=str(BASE_DIR),
            )
        except FileNotFoundError:
            base.update(error=f"找不到 ray 可执行文件：{RAY_BIN}")
        except subprocess.TimeoutExpired:
            base.update(error=f"ray status 超时（>{RAY_TIMEOUT:.0f}s）")
        except Exception as exc:  # pragma: no cover
            base.update(error=f"ray status 调用失败：{exc.__class__.__name__}")
        else:
            dur = round((time.perf_counter() - t0) * 1000, 1)
            base["duration_ms"] = dur
            if proc.returncode != 0:
                err = (proc.stderr or proc.stdout or "").strip().splitlines()
                base.update(available=False,
                            error=f"ray status 退出码 {proc.returncode}：{(err[-1] if err else '无输出')[:200]}")
            else:
                parsed = parse_ray_status(proc.stdout or "")
                if parsed["cpu"]["total"] is None:
                    base.update(available=False, error="ray status 输出无法解析（集群可能未启动）")
                else:
                    parsed.update(available=True, checked_at=time.time(), duration_ms=dur, error=None)
                    with self._lock:
                        self._state = parsed
                    return
        with self._lock:
            self._state = base

    def run(self) -> None:  # pragma: no cover - 线程体
        while not self._stop.is_set():
            now = time.time()
            if now - self._last_call >= RAY_MIN_INTERVAL:
                self._last_call = now
                try:
                    self._call()
                except Exception:
                    pass
            self._stop.wait(1.0)

    def stop(self) -> None:
        self._stop.set()

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._state)


SAMPLER = ResourceSampler()
RAY = RaySampler()


# --------------------------------------------------------------------------- #
# 实验产物读取
# --------------------------------------------------------------------------- #


def _worker_file(name: str) -> JsonFile:
    jf = _WORKER_CACHE.get(name)
    if jf is None:
        jf = JsonFile(RUN_DIR / "workers" / name)
        _WORKER_CACHE[name] = jf
    return jf


def read_progress(now: float) -> dict[str, Any]:
    data, err = _PROGRESS.read()
    info: dict[str, Any] = {
        "exists": data is not None,
        "error": err,
        "mtime": _PROGRESS._mtime,
        "data": None,
        "age_sec": None,
    }
    if not isinstance(data, dict):
        if data is not None:
            info["error"] = "not_an_object"
        return info

    progress = dict(data)
    progress.pop("events", None)  # 事件单独走 events 字段，避免重复体积
    updated_at = _num(data.get("updated_at"))
    if updated_at is not None and updated_at > 1e12:      # 容忍毫秒时间戳
        updated_at = updated_at / 1000.0
    age = None
    if updated_at is not None:
        age = max(0.0, now - updated_at)
    info.update(data=progress, updated_at=updated_at, age_sec=age)
    return info


# 阶段名 → 中文（前端也有一份，这里给 API 消费者用）。
# simhash 与 minhash 共用 shuffle/compare/verify/resolve，首段分别是 fingerprint / signature；
# 只有 shuffle / verify 的文案随算法变化，其余共用。未知阶段名一律原样显示。
STAGE_LABELS = {
    "signature": "签名计算", "shuffle": "切带分发",
    "compare": "桶内比较", "verify": "Jaccard 复核", "resolve": "并查集归并",
    "exact": "精确预聚合", "band": "分带入桶",
    "init": "初始化", "done": "完成",
}
ALGO_STAGE_LABELS: dict[str, dict[str, str]] = {
    "minhash": {"shuffle": "分带入桶", "verify": "Jaccard 估计"},
    "slim": {"exact": "精确预聚合", "signature": "签名计算", "band": "分带入桶",
             "compare": "桶内比较（排序扫描）", "resolve": "并查集归并"},
}
# 只有这些阶段的 done/total 是「文档数」，才能用全局 docs/s 估 ETA（按篇计数）
DOC_STAGES = ("signature",)

# 各算法关心的参数：simhash 用位宽/分带/汉明阈值，minhash 用带数×行数/相似度阈值
ALGO_PARAM_KEYS = {
    "minhash": ("bands", "rows", "threshold", "shingle", "verify"),
    "slim": ("bands", "rows", "threshold", "shingle", "pivot_over"),
}
PARAM_KEYS = ("bits", "band_bits", "hamming", "bands", "rows", "threshold",
              "shingle", "verify", "seed")


def stage_labels(algorithm: Optional[str]) -> dict[str, str]:
    """当前算法的阶段中文名：通用表 + 算法覆盖。未知算法只用通用表。"""
    labels = dict(STAGE_LABELS)
    labels.update(ALGO_STAGE_LABELS.get((algorithm or "").lower(), {}))
    return labels


def _normalize_stage(raw: Any, idx: int, current: Optional[str] = None,
                     labels: Optional[dict[str, str]] = None) -> dict[str, Any]:
    """规范化一个 stage：weight/percent/status 都可能缺失，缺失要能容忍。"""
    labels = labels or STAGE_LABELS
    if not isinstance(raw, dict):
        return {"name": f"stage-{idx}", "label": f"stage-{idx}", "done": None, "total": None,
                "percent": None, "weight": None, "status": "pending", "eta_sec": None,
                "doc_counted": False}
    name = _str(raw.get("name"), f"stage-{idx}")
    done, total = _int(raw.get("done")), _int(raw.get("total"))
    weight = _num(raw.get("weight"))
    pct = _num(raw.get("percent"))
    if pct is None and done is not None and total:
        pct = done * 100.0 / total
    if pct is not None:
        pct = max(0.0, min(100.0, pct))
    # 状态：已完成(done>=total) / 进行中(当前阶段或已有产出) / 未开始
    if done is not None and total and done >= total:
        status = "done"
    elif name == current or (done is not None and done > 0) or (total is not None and done):
        status = "running"
    else:
        status = "pending"
    return {"name": name, "label": labels.get(name, name), "done": done, "total": total,
            "percent": round(pct, 1) if pct is not None else None,
            "weight": weight, "status": status, "eta_sec": None,
            "doc_counted": name in DOC_STAGES}


def read_events(now: float, limit: int = 60) -> list[dict[str, Any]]:
    """从 progress.json 原始文件里取 events（倒序，最多 limit 条）。"""
    data, _ = _PROGRESS.read()
    if not isinstance(data, dict):
        return []
    raw = data.get("events")
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for ev in raw[-limit:]:
        if isinstance(ev, dict):
            ts = _num(ev.get("ts"))
            out.append({
                "ts": ts,
                "age_sec": round(max(0.0, now - ts), 1) if ts else None,
                "level": _str(ev.get("level"), "info").lower(),
                "msg": _str(ev.get("msg")),
            })
        elif isinstance(ev, str):
            out.append({"ts": None, "age_sec": None, "level": "info", "msg": ev})
    out.reverse()   # 最新在前
    return out


def read_workers(now: float, run_live: bool = True,
                 algorithm: Optional[str] = None) -> dict[str, Any]:
    wdir = RUN_DIR / "workers"
    items: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    try:
        names = sorted(p.name for p in wdir.glob("*.json") if p.is_file())[:512]
    except (FileNotFoundError, NotADirectoryError, OSError):
        names = []

    for name in names:
        jf = _worker_file(name)
        data, err = jf.read()
        if data is None:
            if err != "missing":
                errors.append({"file": name, "error": err or "unknown"})
            continue
        if not isinstance(data, dict):
            errors.append({"file": name, "error": "not_an_object"})
            continue
        updated_at = _num(data.get("updated_at"))
        if updated_at is not None and updated_at > 1e12:
            updated_at = updated_at / 1000.0
        age = max(0.0, now - updated_at) if updated_at is not None else None
        lb = data.get("last_batch")
        last_batch = None
        if isinstance(lb, dict):
            last_batch = {"start": _int(lb.get("start")), "end": _int(lb.get("end")),
                          "docs": _int(lb.get("docs"))}
        items.append({
            "worker_id": _str(data.get("worker_id"), Path(name).stem),
            "file": name,
            "pid": _int(data.get("pid")),
            "node": _str(data.get("node")).strip() or "—",
            "stage": _str(data.get("stage"), "unknown") or "unknown",
            "status": _str(data.get("status"), "unknown") or "unknown",
            "batches_done": _int(data.get("batches_done")),
            "docs_done": _int(data.get("docs_done")),
            "docs_per_sec": _num(data.get("docs_per_sec")),
            "last_batch": last_batch,
            "updated_at": updated_at,
            "age_sec": round(age, 1) if age is not None else None,
            # 「疑似空闲」：worker 自己长时间没更新，且全局进度仍在推进（否则是整体陈旧，不能怪 worker）
            "idle": bool((run_live and age is not None and age > IDLE_SEC)
                         or data.get("status") in ("idle", "dead")),
        })

    # 清理已消失文件的缓存
    if len(_WORKER_CACHE) > len(names) + 64:
        keep = set(names)
        for k in list(_WORKER_CACHE):
            if k not in keep:
                _WORKER_CACHE.pop(k, None)

    stages: dict[str, dict[str, Any]] = {}
    for w in items:
        s = stages.setdefault(w["stage"], {"stage": w["stage"], "workers": 0, "idle": 0,
                                           "docs_done": 0, "docs_per_sec": 0.0,
                                           "batches_done": 0})
        s["workers"] += 1
        s["idle"] += 1 if w["idle"] else 0
        s["docs_done"] += w["docs_done"] or 0
        s["docs_per_sec"] = round(s["docs_per_sec"] + (w["docs_per_sec"] or 0.0), 1)
        s["batches_done"] += w["batches_done"] or 0

    for st in stages.values():
        st["label"] = stage_labels(algorithm).get(st["stage"], st["stage"])
        st["avg_docs_per_sec"] = round(st["docs_per_sec"] / st["workers"], 1) if st["workers"] else 0.0
        st["avg_docs_done"] = round(st["docs_done"] / st["workers"], 1) if st["workers"] else 0.0

    items.sort(key=lambda w: (w["stage"], -int(w["docs_done"] or 0), w["worker_id"]))
    return {
        "count": len(items),
        "idle_count": sum(1 for w in items if w["idle"]),
        "items": items,
        "by_stage": sorted(stages.values(), key=lambda s: s["stage"]),
        "errors": errors,
        "dir_exists": wdir.is_dir(),
        "idle_threshold_sec": IDLE_SEC,
        # 本次是否真正做了空闲判定（全局数据陈旧时不做，避免误报）
        "idle_judged": bool(run_live),
        "stale_count": sum(1 for w in items
                           if w["age_sec"] is not None and w["age_sec"] > IDLE_SEC),
        # 全部 worker 一起过期 = 写入端没刷新（不是某个 worker 卡住），前端要区别提示
        "all_stale": bool(items) and all(
            w["age_sec"] is not None and w["age_sec"] > IDLE_SEC for w in items),
    }


def read_result(now: float) -> dict[str, Any]:
    data, err = _RESULT.read()
    info: dict[str, Any] = {"exists": False, "error": err, "mtime": _RESULT._mtime,
                            "data": None, "age_sec": None}
    if isinstance(data, dict):
        info["exists"] = True
        info["error"] = None
        info["data"] = data
        mt = _RESULT._mtime
        if mt:
            info["age_sec"] = round(max(0.0, now - mt), 1)
    return info


def read_log(max_lines: int = LOG_TAIL_MAX) -> dict[str, Any]:
    """只读日志尾部：seek 到末尾回退最多 256KB，取最后 max_lines 行。"""
    path = RUN_DIR / "experiment.log"
    try:
        st = path.stat()
    except (FileNotFoundError, OSError):
        _LOG_CACHE.update(key=None, payload=None)
        return {"exists": False, "size": 0, "mtime": None, "tail": [], "lines": 0,
                "empty": True}
    key = (st.st_mtime_ns, st.st_size)
    if _LOG_CACHE["key"] != key:
        try:
            with open(path, "rb") as fh:
                back = min(st.st_size, LOG_READ_BYTES)
                fh.seek(st.st_size - back)
                raw = fh.read()
            text = raw.decode("utf-8", "replace")
            lines = text.splitlines()
            if back < st.st_size and lines:
                lines = lines[1:]        # 首行可能被截断，丢弃
            _LOG_CACHE.update(key=key, payload=lines)
        except OSError as exc:
            return {"exists": True, "size": st.st_size, "mtime": st.st_mtime,
                    "tail": [], "lines": 0, "empty": st.st_size == 0,
                    "error": f"读取失败: {exc.__class__.__name__}"}
    lines = _LOG_CACHE["payload"] or []
    n = max(1, min(int(max_lines or LOG_TAIL_MAX), LOG_TAIL_MAX))
    return {"exists": True, "size": st.st_size, "mtime": st.st_mtime,
            "tail": lines[-n:], "lines": len(lines), "showing": min(n, len(lines)),
            # 0 字节（日志跟随进程被杀）不是错误，前端显示「暂无日志」
            "empty": st.st_size == 0}


def _stage_eta(done: Optional[int], total: Optional[int], rate: Optional[float]) -> Optional[float]:
    if done is None or not total or rate is None or rate <= 0:
        return None
    remain = max(0, total - done)
    return round(remain / rate, 1)


# --------------------------------------------------------------------------- #
# 算法识别与参数（simhash / minhash）
# --------------------------------------------------------------------------- #

ALGO_META: dict[str, dict[str, str]] = {
    "simhash": {"label": "SimHash", "desc": "指纹 + 汉明距离"},
    "minhash": {"label": "MinHash", "desc": "签名 + 相似度阈值"},
}


def _clean_algo(v: Any) -> Optional[str]:
    s = _str(v).strip().lower()
    return s or None


def _resolve_algorithm(raw: dict, result_data: dict) -> tuple[Optional[str], Optional[str]]:
    """运行中看 progress.json.algorithm；结束后（或 progress 缺失）看 result.json.algorithm。"""
    algo = _clean_algo(raw.get("algorithm")) if isinstance(raw, dict) else None
    if algo:
        return algo, "progress"
    algo = _clean_algo(result_data.get("algorithm")) if isinstance(result_data, dict) else None
    if algo:
        return algo, "result"
    return None, None


def _params_from(src: Any) -> dict[str, Any]:
    """从 progress/result 里挑出算法相关参数，数值字段做类型收敛。"""
    if not isinstance(src, dict):
        return {}
    out: dict[str, Any] = {}
    for k in PARAM_KEYS:
        if k not in src:
            continue
        v = _num(src.get(k)) if k in ("verify", "threshold") else _int(src.get(k))
        if v is not None:
            out[k] = v
    return out


_RUN_REPORTS: dict[str, JsonFile] = {}


def _run_report(name: str) -> JsonFile:
    """runs/<name>.json：每次 run 的报告副本（run_experiment.py 收尾时写）。"""
    jf = _RUN_REPORTS.get(name)
    if jf is None:
        if len(_RUN_REPORTS) > 128:
            _RUN_REPORTS.clear()
        jf = JsonFile(RUN_DIR / "runs" / f"{name}.json")
        _RUN_REPORTS[name] = jf
    return jf


def _recent_run_reports(limit: int = 40) -> list[tuple[float, Path]]:
    """runs/ 下最近写出的报告副本（按 mtime 倒序，最多 limit 个）。"""
    rdir = RUN_DIR / "runs"
    try:
        items = [(p.stat().st_mtime, p) for p in rdir.glob("*.json") if p.is_file()]
    except (FileNotFoundError, NotADirectoryError, OSError):
        return []
    items.sort(key=lambda t: t[0], reverse=True)
    return items[:limit]


def _report_matches(raw: Any, report: Any, report_mtime: Optional[float]) -> bool:
    """这份报告是否属于「本次 run」。

    run_experiment.py 的报告 run_id 是 --run-id（可自定义），progress.json 里是时间戳，
    两者经常不同：所以用「报告不比 progress 旧 + docs/algo 对得上」兜底。
    """
    if not isinstance(report, dict) or not report:
        return False
    if not _str(report.get("run_id")):
        return False
    if "ray" not in report and "local" not in report:
        return False                     # 排除 analyze_run 产出的诊断/分析 json
    p_run = _str(raw.get("run_id")) if isinstance(raw, dict) else ""
    if p_run and p_run == _str(report.get("run_id")):
        return True
    ua = _num(raw.get("updated_at")) if isinstance(raw, dict) else None
    if ua is not None and ua > 1e12:
        ua = ua / 1000.0
    if ua is None or report_mtime is None or report_mtime + 1.0 < ua:
        return False
    p_docs, r_docs = _int(raw.get("docs_total")), _int(report.get("docs"))
    if p_docs is not None and r_docs is not None and p_docs != r_docs:
        return False
    p_algo = _clean_algo(raw.get("algorithm"))
    r_algo = _clean_algo(report.get("algorithm"))
    if p_algo and r_algo and p_algo != r_algo:
        return False
    return True


def _resolve_params(raw: dict, result_data: dict,
                    result_mtime: Optional[float] = None) -> tuple[dict[str, Any], Optional[str]]:
    """参数来源优先级：progress.json → runs/ 里的本次报告副本 → result.json（仅同一 run）。

    只认「本次 run」的参数：result.json 常常是上一次 run 留下的，直接拿来会张冠李戴。
    """
    p_run = _str(raw.get("run_id")) if isinstance(raw, dict) else ""
    live = _params_from(raw)
    if live:
        return live, "progress"

    # 1) runs/<progress.run_id>.json（--run-id 省略时文件名就是 progress 的时间戳）
    if p_run and "/" not in p_run and "\\" not in p_run:
        jf = _run_report(p_run)
        report, _err = jf.read()
        if _report_matches(raw, report, jf._mtime):
            rp = _params_from(report)
            if rp:
                return rp, f"runs/{p_run}.json"

    # 2) runs/ 里最近写出的报告副本（自定义 --run-id 的情况）
    for mtime, path in _recent_run_reports():
        report, _err = _run_report(path.stem).read()
        if _report_matches(raw, report, mtime):
            rp = _params_from(report)
            if rp:
                return rp, f"runs/{path.name}"

    # 3) result.json（上次 run 的残留会被 _report_matches 挡掉）
    final = _params_from(result_data)
    if final and _report_matches(raw, result_data, result_mtime):
        r_run = _str(result_data.get("run_id"))
        return final, (f"result:{r_run}" if r_run else "result")
    return {}, None


def collect_state(log_lines: int = LOG_TAIL_MAX) -> dict[str, Any]:
    """把 §1–§5 需要的所有数据合成一份 JSON（同步、无 await，事件循环内安全）。"""
    now = time.time()
    prog = read_progress(now)
    result = read_result(now)
    raw = prog.get("data") or {}
    result_data = result.get("data") if isinstance(result.get("data"), dict) else {}

    algorithm, algorithm_source = _resolve_algorithm(raw, result_data)
    params, params_source = _resolve_params(raw, result_data, result.get("mtime"))
    labels = stage_labels(algorithm)

    p_age = prog.get("age_sec")
    run_live = p_age is not None and p_age <= FRESH_WARN_SEC
    workers = read_workers(now, run_live=run_live, algorithm=algorithm)
    log = read_log(log_lines)
    res = SAMPLER.snapshot()
    ray = RAY.snapshot()

    docs_total = _int(raw.get("docs_total"))
    docs_done = _int(raw.get("docs_done"))
    tput = _num(raw.get("throughput_docs_per_sec"))
    cur_stage = _str(raw.get("stage"), "") or None

    stages = [_normalize_stage(st, i, cur_stage, labels)
              for i, st in enumerate(raw.get("stages") or [])] \
        if isinstance(raw.get("stages"), list) else []

    # 总进度：优先用契约新给的加权 percent，缺失才按 docs_done/docs_total 兜底
    overall_pct = _num(raw.get("percent"))
    pct_source = "percent"
    if overall_pct is None:
        pct_source = "computed"
        if docs_done is not None and docs_total:
            overall_pct = docs_done * 100.0 / docs_total
    overall_pct = round(max(0.0, min(100.0, overall_pct)), 2) if overall_pct is not None else None

    eta = _stage_eta(docs_done, docs_total, tput)
    # 阶段 ETA：只有按篇计数的阶段能用全局 docs/s 估；其余交给前端按阶段实测速率算
    for st in stages:
        if st["doc_counted"]:
            st["eta_sec"] = _stage_eta(st["done"], st["total"], tput)

    age = prog.get("age_sec")
    if not prog.get("exists"):
        freshness, status = "unknown", "waiting"
    elif age is None:
        freshness, status = "unknown", "live" if not prog.get("error") else "error"
    elif age > FRESH_STALE_SEC:
        freshness, status = "stale", "stale"
    elif age > FRESH_WARN_SEC:
        freshness, status = "warn", "stale"
    else:
        freshness, status = "ok", "live"
    if result.get("exists") and raw.get("stage") in ("done", "finished", "complete"):
        status = "finished"
        freshness = "ok"   # 实验跑完后不再刷新 progress.json，别把「跑完了」显示成「数据陈旧」

    return {
        "ts": now,
        "app": {"version": APP_VERSION, "pid": os.getpid(), "hostname": socket.gethostname(),
                "uptime_sec": round(now - START_TS, 1), "run_dir": str(RUN_DIR),
                "started_at": START_TS, "sse_interval": SSE_INTERVAL,
                "sample_interval": SAMPLE_INTERVAL, "history_len": HISTORY_LEN,
                "idle_threshold_sec": IDLE_SEC,
                "fresh_warn_sec": FRESH_WARN_SEC, "fresh_stale_sec": FRESH_STALE_SEC},
        "experiment": {
            "run_dir_exists": RUN_DIR.is_dir(),
            "status": status,
            "freshness": freshness,
            "run_id": _str(raw.get("run_id"), "") or None,
            # 算法：优先 progress.json，其次 result.json；都没有就是 None（前端显示「未知」）
            "algorithm": algorithm,
            "algorithm_source": algorithm_source,
            "algorithm_label": ALGO_META.get(algorithm or "", {}).get("label"),
            "algorithm_desc": ALGO_META.get(algorithm or "", {}).get("desc"),
            "params": params,
            "params_source": params_source,
            "stage": _str(raw.get("stage"), "") or None,
            "updated_at": prog.get("updated_at"),
            "data_age_sec": round(age, 1) if age is not None else None,
            "progress_error": prog.get("error"),
            "progress_exists": bool(prog.get("exists")),
            "docs_total": docs_total,
            "docs_done": docs_done,
            "docs_kept": _int(raw.get("docs_kept")),
            "docs_dropped": _int(raw.get("docs_dropped")),
            "edges": _int(raw.get("edges")),
            # 漏斗字段：可能都不存在
            "verify_total": _int(raw.get("verify_total")),
            "verify_kept": _int(raw.get("verify_kept")),
            "throughput_docs_per_sec": tput,
            "percent": overall_pct,
            "percent_source": pct_source if overall_pct is not None else None,
            "stage_count": len(stages),
            "eta_sec": eta,
            "stages": stages,
            "events": read_events(now),
            "worker_count": workers["count"],
        },
        # 顶层也给一份（外部消费者不用挖 experiment 就能拿到算法）
        "algorithm": algorithm,
        "algorithm_source": algorithm_source,
        "algorithm_label": ALGO_META.get(algorithm or "", {}).get("label"),
        "algorithm_desc": ALGO_META.get(algorithm or "", {}).get("desc"),
        "params": params,
        "params_source": params_source,
        "workers": workers,
        "result": result,
        "log": log,
        "resources": {"host": res, "ray": ray},
    }


# --------------------------------------------------------------------------- #
# 历史对比：LanceDB `dedup_runs`（或 RAYTEST_RUNS_JSON 假数据）
# --------------------------------------------------------------------------- #

# 只暴露面板真正会用到的列，避免把 metrics_json / pools 这种大字段塞进每帧
_RUN_INT = {"docs", "chars", "bits", "band_bits", "hamming", "shingle", "bands", "rows",
            "edges", "band_hits", "kept", "dropped", "edge_fp_cross_base",
            "groups_extra_kept", "groups_missing", "components_merged",
            "border_dropped", "border_total"}
_RUN_RATIO = {"threshold", "verify", "pair_recall", "pair_precision",
              "groups_collapsed_rate", "neg_false_drop_rate"}
_RUN_FLOAT = {"seconds", "docs_per_sec"}
_RUN_STR = {"run_id", "algo", "mode", "created_at", "corpus_sha256",
            "subset_table", "subset_sha256", "ray_job_id", "finished_at"}

_RUNS_CACHE: dict[str, Any] = {"key": None, "ts": 0.0, "payload": None}


def _r(v: Optional[float], nd: int) -> Optional[float]:
    return round(v, nd) if v is not None else None


def _norm_run_row(row: Any) -> Optional[dict[str, Any]]:
    """把 lance/JSON 的一行收敛成前端友好的扁平 dict；缺列一律 None。"""
    if not isinstance(row, dict):
        return None
    out: dict[str, Any] = {}
    for k in _RUN_INT:
        out[k] = _int(row.get(k))
    for k in _RUN_RATIO:
        out[k] = _r(_num(row.get(k)), 6)
    for k in _RUN_FLOAT:
        out[k] = _r(_num(row.get(k)), 3)
    for k in _RUN_STR:
        out[k] = _str(row.get(k)).strip() or None
    out["algo"] = (out["algo"] or "unknown").lower()
    out["mode"] = out["mode"].lower() if out["mode"] else None
    if not out["run_id"]:
        return None
    return out


def _run_sort_key(row: dict[str, Any]) -> tuple:
    """新的 run 在前；同一 run 里 ray 在 local 之前（两行相邻便于对照）。"""
    return (row.get("created_at") or "", row.get("run_id") or "", row.get("mode") or "")


def _later(a: Optional[str], b: Optional[str]) -> bool:
    return bool(a) and (not b or a > b)


def summarize_runs(runs: list[dict[str, Any]]) -> dict[str, Any]:
    """按 algo 汇总：行数/run 数/docs 区间/最好的召回精度/最大吞吐。"""
    out: dict[str, Any] = {}
    for row in runs:
        algo = row.get("algo") or "unknown"
        s = out.setdefault(algo, {
            "rows": 0, "runs": 0, "modes": [], "docs_max": None, "docs_min": None,
            "max_docs_per_sec": None, "best_pair_recall": None, "best_pair_precision": None,
            "hamming": [], "bands": [], "verify": [], "latest_run": None, "latest_at": None,
        })
        s["rows"] += 1
        s.setdefault("_ids", [])
        if row.get("run_id") and row["run_id"] not in s["_ids"]:
            s["_ids"].append(row["run_id"])
        mode = row.get("mode")
        if mode and mode not in s["modes"]:
            s["modes"].append(mode)
        docs = row.get("docs")
        if docs is not None:
            s["docs_max"] = docs if s["docs_max"] is None else max(s["docs_max"], docs)
            s["docs_min"] = docs if s["docs_min"] is None else min(s["docs_min"], docs)
        rate = row.get("docs_per_sec")
        if rate is not None:
            s["max_docs_per_sec"] = rate if s["max_docs_per_sec"] is None else max(s["max_docs_per_sec"], rate)
        for key, best in (("pair_recall", "best_pair_recall"),
                          ("pair_precision", "best_pair_precision")):
            v = row.get(key)
            if v is not None:
                s[best] = v if s[best] is None else max(s[best], v)
        for key in ("hamming", "bands"):
            v = row.get(key)
            if v is not None and v not in s[key]:
                s[key].append(v)
        verify = row.get("verify")
        if verify is not None and verify not in s["verify"]:
            s["verify"].append(verify)
        if _later(row.get("created_at"), s["latest_at"]):
            s["latest_at"] = row.get("created_at")
            s["latest_run"] = row.get("run_id")

    for s in out.values():
        s["runs"] = len(s.pop("_ids", []))
        s["modes"] = sorted(s["modes"])
        for key in ("hamming", "bands", "verify"):
            s[key] = sorted(s[key])
    return dict(sorted(out.items()))


def _runs_from_json(path: str) -> tuple[list[dict[str, Any]], Optional[str]]:
    p = Path(path)
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return [], f"RAYTEST_RUNS_JSON 指向的文件不存在：{path}"
    except (OSError, ValueError) as exc:
        return [], f"读取 {path} 失败：{exc.__class__.__name__}"
    items = raw.get("runs") if isinstance(raw, dict) else raw
    if not isinstance(items, list):
        return [], f"{path} 里没有 runs 列表（应为 [...] 或 {{\"runs\": [...]}}）"
    rows = [r for r in (_norm_run_row(x) for x in items) if r]
    return rows, None


def _runs_from_lance() -> tuple[list[dict[str, Any]], Optional[str]]:
    try:
        import lancedb   # 只有远端 venv 装了；本机没有 → ImportError 走降级
    except Exception:
        return [], "未安装 lancedb（本机没有该依赖，历史对比面板只能显示空状态）"
    if not LANCE_DIR.is_dir():
        return [], f"LanceDB 目录不存在：{LANCE_DIR}"
    try:
        db = lancedb.connect(str(LANCE_DIR))
    except Exception as exc:
        return [], f"连接 LanceDB 失败：{exc.__class__.__name__}: {exc}"
    try:
        listing = db.list_tables()
        names = list(getattr(listing, "tables", listing) or [])
    except Exception:
        try:
            names = list(db.table_names())
        except Exception as exc:
            return [], f"读取表清单失败：{exc.__class__.__name__}: {exc}"
    if "dedup_runs" not in names:
        return [], f"{LANCE_DIR} 里没有 dedup_runs 表（还没跑过 --corpus-uri 的实验？）"
    try:
        rows = db.open_table("dedup_runs").to_arrow().to_pylist()
    except Exception as exc:
        return [], f"读取 dedup_runs 失败：{exc.__class__.__name__}: {exc}"
    return [r for r in (_norm_run_row(x) for x in rows) if r], None


RUN_INDEX = RUN_DIR / "runs_index.json"


def _load_bundle(path: str) -> Optional[dict]:
    try:
        p = Path(path)
        if p.is_file():
            return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None
    return None


def _runs_from_index() -> tuple[list[dict[str, Any]], Optional[str]]:
    """瘦身流水线的历史：/tmp/raytest/runs_index.json（每次 run 追加一行）。

    索引里只有汇总字段；召回/精度这类指标从对应的结果包里取（bundle 就在同目录），
    取不到就留 None —— 不影响表格展示。
    """
    if not RUN_INDEX.is_file():
        return [], f"没有 run 索引：{RUN_INDEX}（跑一次 dedup_slim.py --progress-dir /tmp/raytest 就会生成）"
    try:
        items = json.loads(RUN_INDEX.read_text(encoding="utf-8"))
    except Exception as exc:
        return [], f"读取 {RUN_INDEX.name} 失败：{exc.__class__.__name__}: {exc}"

    rows: list[dict[str, Any]] = []
    for it in items if isinstance(items, list) else []:
        if not isinstance(it, dict) or not it.get("run_id"):
            continue
        bundle = _load_bundle(it.get("bundle") or "")
        m = (bundle or {}).get("metrics") or {}
        b = (bundle or {}).get("bytes") or {}
        sec = (bundle or {}).get("sec") or {}
        docs, seconds = it.get("docs"), it.get("seconds")
        rate = round(docs / seconds, 1) if docs and seconds else None
        rows.append({
            "run_id": it.get("run_id"),
            "ray_job_id": it.get("ray_job_id") or None,
            "algo": "minhash",
            "mode": it.get("mode"),
            "created_at": it.get("finished_at"),
            "finished_at": it.get("finished_at"),
            "docs": docs,
            "kept": it.get("kept"),
            "dropped": it.get("dropped"),
            "seconds": seconds,
            "docs_per_sec": rate,
            "pair_recall": m.get("pair_recall_edge_canon", m.get("pair_recall")),
            "pair_precision": m.get("pair_precision"),
            "groups_missing": m.get("groups_missing"),
            "components_merged": m.get("components_merged"),
            "edge_fp_cross_base": m.get("edge_fp_cross_base"),
            "border_dropped": m.get("border_dropped"),
            "bands": it.get("bands"), "rows": it.get("rows"),
            "threshold": it.get("threshold"), "k": it.get("k"),
            "edges": m.get("edge_total"), "band_hits": None,
            "slim_bytes": b.get("slim_bytes"), "slim_bytes_per_doc": b.get("slim_bytes_per_doc"),
            "scan": (bundle or {}).get("params", {}).get("scan"),
            "stage_sec": {kk: sec.get(kk) for kk in
                          ("load", "exact", "sign", "band_rows", "scan", "uf", "eval", "total")},
        })
    return [r for r in (_norm_run_row(x) for x in rows) if r], None


def _runs_cache_key() -> tuple:
    """数据源指纹：假数据看文件 mtime，真表看 dedup_runs.lance 目录 mtime。"""
    if RUNS_JSON:
        try:
            st = Path(RUNS_JSON).stat()
            return ("json", RUNS_JSON, st.st_mtime_ns, st.st_size)
        except OSError:
            return ("json", RUNS_JSON, None, None)
    if RUN_INDEX.is_file():
        try:
            st = RUN_INDEX.stat()
            return ("index", str(RUN_INDEX), st.st_mtime_ns, st.st_size)
        except OSError:
            pass
    try:
        st = (LANCE_DIR / "dedup_runs.lance").stat()
        return ("lance", str(LANCE_DIR), st.st_mtime_ns, st.st_size)
    except OSError:
        return ("lance", str(LANCE_DIR), None, None)


def read_runs(force: bool = False) -> dict[str, Any]:
    """读历史 run；任何异常都只体现在 runs_error 上，永远返回可序列化的结构。"""
    now = time.time()
    key = _runs_cache_key()
    cached = _RUNS_CACHE.get("payload")
    if (not force and cached is not None and _RUNS_CACHE.get("key") == key
            and now - float(_RUNS_CACHE.get("ts") or 0.0) < RUNS_CACHE_SEC):
        return dict(cached)

    if RUNS_JSON:
        source = "json"
    elif RUN_INDEX.is_file():
        source = "index"
    else:
        source = "lance"
    try:
        if source == "json":
            runs, err = _runs_from_json(RUNS_JSON)
        elif source == "index":
            runs, err = _runs_from_index()
        else:
            runs, err = _runs_from_lance()
    except Exception as exc:      # 兜底：绝不让 /api/runs 抛 500
        runs, err = [], f"读取历史 run 失败：{exc.__class__.__name__}: {exc}"

    runs.sort(key=_run_sort_key, reverse=True)
    payload = {
        "runs": runs,
        "algo_summary": summarize_runs(runs),
        "runs_error": err,
        "runs_source": source,
        "runs_path": RUNS_JSON or (str(RUN_INDEX) if source == "index" else str(LANCE_DIR)),
        "runs_count": len(runs),
        "runs_cache_sec": RUNS_CACHE_SEC,
        "generated_at": now,
    }
    _RUNS_CACHE.update(key=key, ts=now, payload=payload)
    return dict(payload)


# --------------------------------------------------------------------------- #
# FastAPI 应用
# --------------------------------------------------------------------------- #


@asynccontextmanager
async def lifespan(_app: FastAPI):
    SAMPLER.start()
    RAY.start()
    yield
    SAMPLER.stop()
    RAY.stop()


app = FastAPI(title="raytest 任务监控", version=APP_VERSION, lifespan=lifespan)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"],
)

_NO_STORE = {"Cache-Control": "no-store, max-age=0"}


@app.get("/api/health")
async def api_health() -> JSONResponse:
    """永远 200：只表示监控服务本身活着（不依赖实验/Ray 状态）。"""
    return JSONResponse({
        "status": "ok",
        "version": APP_VERSION,
        "uptime_sec": round(time.time() - START_TS, 1),
        "ts": time.time(),
        "pid": os.getpid(),
        "run_dir": str(RUN_DIR),
        "run_dir_exists": RUN_DIR.is_dir(),
        "ray_available": bool(RAY.snapshot().get("available")),
    }, headers=_NO_STORE)


@app.get("/api/state")
async def api_state(lines: int = Query(LOG_TAIL_MAX, ge=1, le=LOG_TAIL_MAX)) -> JSONResponse:
    return JSONResponse(await asyncio.to_thread(collect_state, lines), headers=_NO_STORE)


@app.get("/api/runs")
async def api_runs(algo: Optional[str] = Query(None),
                   refresh: bool = Query(False)) -> JSONResponse:
    """历史 run 列表：默认读 LanceDB `dedup_runs`；RAYTEST_RUNS_JSON 设置了就优先读假数据。

    `?algo=simhash|minhash` 只过滤返回行（algo_summary 也跟着重算）；`?refresh=1` 跳缓存。
    读表失败（没装 lancedb / 没有表 / 路径不可读）只返回空列表 + `runs_error`，不 500。
    """
    payload = await asyncio.to_thread(read_runs, refresh)
    want = (algo or "").strip().lower()
    if want and want not in ("all", "*"):
        rows = [r for r in payload.get("runs") or []
                if (r.get("algo") or "").lower() == want]
        payload = {**payload, "runs": rows, "algo_summary": summarize_runs(rows),
                   "runs_count": len(rows), "algo_filter": want}
    else:
        payload["algo_filter"] = None
    return JSONResponse(payload, headers=_NO_STORE)


@app.get("/api/log")
async def api_log(lines: int = Query(LOG_TAIL_MAX, ge=1, le=LOG_TAIL_MAX)) -> JSONResponse:
    return JSONResponse(read_log(lines), headers=_NO_STORE)


@app.get("/api/stream")
async def api_stream(request: Request,
                     lines: int = Query(LOG_TAIL_MAX, ge=1, le=LOG_TAIL_MAX)) -> StreamingResponse:
    """SSE：每条消息是 §1–§5 的合并状态。前端断线后 EventSource 自动重连。"""

    async def gen():
        yield "retry: 3000\n\n"
        while True:
            if await request.is_disconnected():
                break
            try:
                payload = await asyncio.to_thread(collect_state, lines)
                yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # 采集异常也不能断开连接
                err = {"ts": time.time(), "error": f"采集失败: {exc.__class__.__name__}"}
                yield f"data: {json.dumps(err, ensure_ascii=False)}\n\n"
            await asyncio.sleep(SSE_INTERVAL)

    return StreamingResponse(gen(), media_type="text/event-stream", headers={
        "Cache-Control": "no-store",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    })


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html", headers=_NO_STORE)


@app.get("/result")
async def result_page() -> FileResponse:
    """去重结果可视化页（与监控页同源、同主题）。"""
    return FileResponse(STATIC_DIR / "result.html", headers=_NO_STORE)


@app.get("/api/run_index")
async def api_run_index() -> Response:
    """瘦身流水线的历史 run 索引：run_id / ray job 编码 / 独立时间戳 / 关键指标。

    （老的 /api/runs 读 LanceDB `dedup_runs`，那是 fat 链路写的表；这里单独一个端点，
      不破坏监控页现有的历史表格。）
    """
    idx = RUN_DIR / "runs_index.json"
    if not idx.is_file():
        return JSONResponse({"runs": []}, headers=_NO_STORE)
    try:
        runs = json.loads(idx.read_text(encoding="utf-8"))
    except Exception as exc:
        return JSONResponse({"runs": [], "error": f"{exc.__class__.__name__}: {exc}"},
                            headers=_NO_STORE)
    return JSONResponse({"runs": runs}, headers=_NO_STORE)


@app.get("/api/result")
async def api_result(run: str = "") -> Response:
    """某次 run 的结果包；默认取最近一次（run 为空）。

    dedup_slim.py 会写 /tmp/raytest/result_slim_<run_id>.json 与 ..._latest.json。
    """
    if run:
        safe = re.sub(r"[^0-9A-Za-z_.\-]", "", run)[:64]
        path = RUN_DIR / f"result_slim_{safe}.json"
    else:
        path = RUN_DIR / "result_slim_latest.json"
    if not path.is_file():
        return JSONResponse({"empty": True, "looked_for": str(path)}, headers=_NO_STORE)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return JSONResponse({"empty": True, "error": f"{exc.__class__.__name__}: {exc}"},
                            status_code=200, headers=_NO_STORE)
    data["mtime"] = path.stat().st_mtime
    data["age_sec"] = round(time.time() - path.stat().st_mtime, 1)
    return JSONResponse(data, headers=_NO_STORE)


@app.get("/favicon.ico")
async def favicon() -> Response:
    return Response(status_code=204)


@app.get("/static/{fname}")
async def static_file(fname: str) -> FileResponse:
    target = (STATIC_DIR / fname).resolve()
    try:
        target.relative_to(STATIC_DIR)
    except ValueError:
        raise HTTPException(status_code=404)
    if not target.is_file():
        raise HTTPException(status_code=404)
    return FileResponse(target, headers=_NO_STORE)


def main() -> None:
    uvicorn.run(app, host=HOST, port=PORT, log_level="info", access_log=False,
                timeout_keep_alive=30)


if __name__ == "__main__":
    main()
