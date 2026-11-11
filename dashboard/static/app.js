/* ============================================================
   raytest 任务监控 — 前端逻辑
   * 只用原生 API：EventSource + canvas，无任何外部依赖
   * 所有请求都走相对路径（./api/...），便于 SSH 隧道访问
   * 保留最近 300 个采样点（约 5 分钟 @1s）
   ============================================================ */
'use strict';

(function () {
  /* ----------------------------- 工具函数 ----------------------------- */
  const $ = (sel, root) => (root || document).querySelector(sel);
  const $$ = (sel, root) => Array.from((root || document).querySelectorAll(sel));

  const NF0 = new Intl.NumberFormat('zh-CN', { maximumFractionDigits: 0 });
  const NF1 = new Intl.NumberFormat('zh-CN', { minimumFractionDigits: 1, maximumFractionDigits: 1 });
  const NF2 = new Intl.NumberFormat('zh-CN', { maximumFractionDigits: 2 });

  const isNum = (v) => typeof v === 'number' && isFinite(v);
  const num = (v, d) => (!isNum(v) ? '—' : d === 0 ? NF0.format(v) : d === 2 ? NF2.format(v) : NF1.format(v));
  const int = (v) => (!isNum(v) ? '—' : NF0.format(v));

  function bytes(v) {
    if (!isNum(v)) return '—';
    const u = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'];
    let i = 0, x = Math.abs(v);
    while (x >= 1024 && i < u.length - 1) { x /= 1024; i++; }
    const s = i === 0 ? String(Math.round(x)) : (x < 10 ? x.toFixed(2) : x.toFixed(1));
    return s + ' ' + u[i];
  }
  const rate = (v) => bytes(v) + '/s';

  function dur(sec) {
    if (!isNum(sec) || sec < 0) return '—';
    const s = Math.round(sec);
    if (s < 60) return s + '秒';
    if (s < 3600) return Math.floor(s / 60) + '分' + String(s % 60).padStart(2, '0') + '秒';
    const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60);
    if (h < 24) return h + '小时' + String(m).padStart(2, '0') + '分';
    return Math.floor(h / 24) + '天' + (h % 24) + '小时';
  }

  function age(sec) {
    if (!isNum(sec)) return '—';
    if (sec < 1) return '刚刚';
    if (sec < 60) return Math.round(sec) + 's 前';
    if (sec < 3600) return Math.floor(sec / 60) + 'm' + String(Math.round(sec % 60)).padStart(2, '0') + 's 前';
    return Math.floor(sec / 3600) + 'h 前';
  }

  const clock = (ts) => (isNum(ts) ? new Date(ts * 1000).toLocaleTimeString('zh-CN', { hour12: false }) : '—');
  const hhmm = (ts) => (isNum(ts) ? new Date(ts * 1000).toLocaleTimeString('zh-CN', { hour12: false }).slice(0, 5) : '');

  function pctText(part, total) {
    if (!isNum(part) || !isNum(total) || total <= 0) return '—';
    return (part * 100 / total).toFixed(part * 100 / total >= 10 ? 1 : 2) + '%';
  }
  function pctNum(part, total) {
    if (!isNum(part) || !isNum(total) || total <= 0) return null;
    return Math.max(0, Math.min(100, part * 100 / total));
  }

  const esc = (s) => String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');

  const clampPct = (v) => (isNum(v) ? Math.max(0, Math.min(100, v)) : 0);

  /* ----------------------------- 全局状态 ----------------------------- */
  const MAX_POINTS = 300;

  const CFG = {
    logLines: 200,
    autoscroll: true,
    stage: 'all',
    streamLines: 200,
  };

  const S = {
    last: null,          // 最近一次完整状态
    samples: [],         // [{t, done, total, pct, tput, cpu, mem, rx, tx}]
    connected: false,
    lastMsgAt: 0,
    logSig: '',
    health: null,
    pollTimer: null,
    es: null,
    redrawQueued: false,
    lastRunId: undefined, // 用于「新 run 结束 → 刷新历史对比」
    scatterHits: [],      // 召回/精度散点的命中框（鼠标悬停用）
    scaleHits: [],        // 吞吐/规模散点的命中框
  };

  /* ----------------------------- 画布 / 图表 ----------------------------- */
  function fitCanvas(canvas) {
    const dpr = window.devicePixelRatio || 1;
    const rect = canvas.getBoundingClientRect();
    const w = Math.max(60, Math.round(rect.width || canvas.clientWidth || 300));
    const h = Math.max(40, Math.round(rect.height || canvas.clientHeight || 140));
    if (canvas.width !== Math.round(w * dpr) || canvas.height !== Math.round(h * dpr)) {
      canvas.width = Math.round(w * dpr);
      canvas.height = Math.round(h * dpr);
    }
    const ctx = canvas.getContext('2d');
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, w, h);
    return { ctx, w, h };
  }

  const GRID = 'rgba(44,63,97,.55)';
  const AXIS_TXT = '#61789a';
  const FONT = '10px ui-monospace, SFMono-Regular, Menlo, Consolas, monospace';

  function niceMax(v) {
    if (!isNum(v) || v <= 0) return 1;
    const exp = Math.floor(Math.log10(v));
    const base = Math.pow(10, exp);
    const n = v / base;
    const step = n <= 1 ? 1 : n <= 2 ? 2 : n <= 2.5 ? 2.5 : n <= 5 ? 5 : 10;
    return step * base;
  }

  /**
   * 通用折线图。spec = {
   *   times: [ts...],
   *   leftMax, rightMax,
   *   leftFmt, rightFmt,   // 轴标签格式化
   *   series: [{ values:[], color, axis:'left'|'right', width, dash, fill }]
   * }
   */
  function drawChart(canvas, spec) {
    const { ctx, w, h } = fitCanvas(canvas);
    const pad = { l: 36, r: 36, t: 9, b: 16 };
    const iw = Math.max(10, w - pad.l - pad.r);
    const ih = Math.max(10, h - pad.t - pad.b);
    const times = spec.times || [];
    const n = times.length;

    ctx.font = FONT;
    ctx.textBaseline = 'middle';

    // 网格 + 左右轴刻度
    const lMax = spec.leftMax > 0 ? spec.leftMax : 1;
    const rMax = spec.rightMax > 0 ? spec.rightMax : 1;
    const rows = 4;
    for (let i = 0; i <= rows; i++) {
      const y = pad.t + ih * i / rows;
      ctx.strokeStyle = GRID;
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(pad.l, Math.round(y) + 0.5);
      ctx.lineTo(pad.l + iw, Math.round(y) + 0.5);
      ctx.stroke();

      ctx.fillStyle = AXIS_TXT;
      ctx.textAlign = 'right';
      ctx.fillText(spec.leftFmt ? spec.leftFmt(lMax * (1 - i / rows)) : '', pad.l - 4, y);
      ctx.textAlign = 'left';
      ctx.fillText(spec.rightFmt ? spec.rightFmt(rMax * (1 - i / rows)) : '', pad.l + iw + 4, y);
    }

    if (n === 0) {
      ctx.fillStyle = '#4d6383';
      ctx.textAlign = 'center';
      ctx.font = '11px -apple-system, "PingFang SC", sans-serif';
      ctx.fillText('暂无采样数据', pad.l + iw / 2, pad.t + ih / 2);
      return;
    }

    const t0 = times[0], t1 = times[n - 1];
    const span = Math.max(1, t1 - t0);
    const xOf = (t) => pad.l + (n === 1 ? iw / 2 : iw * (t - t0) / span);
    const yOf = (v, axis) => {
      const max = axis === 'right' ? rMax : lMax;
      return pad.t + ih * (1 - Math.max(0, Math.min(1, v / max)));
    };

    // 横向时间刻度（首 / 中 / 末）
    ctx.fillStyle = AXIS_TXT;
    ctx.textBaseline = 'top';
    const ticks = n === 1 ? [0] : [0, Math.floor((n - 1) / 2), n - 1];
    ticks.forEach((idx, k) => {
      ctx.textAlign = k === 0 ? 'left' : k === ticks.length - 1 ? 'right' : 'center';
      ctx.fillText(clock(times[idx]).slice(0, 8), xOf(times[idx]), pad.t + ih + 4);
    });
    ctx.textBaseline = 'middle';

    // 曲线
    spec.series.forEach((s) => {
      const pts = [];
      for (let i = 0; i < n; i++) {
        const v = s.values[i];
        if (isNum(v)) pts.push([xOf(times[i]), yOf(v, s.axis)])
      }
      if (!pts.length) return;

      if (s.fill) {
        const grad = ctx.createLinearGradient(0, pad.t, 0, pad.t + ih);
        grad.addColorStop(0, s.fill);
        grad.addColorStop(1, 'rgba(0,0,0,0)');
        ctx.beginPath();
        ctx.moveTo(pts[0][0], pad.t + ih);
        pts.forEach((p) => ctx.lineTo(p[0], p[1]));
        ctx.lineTo(pts[pts.length - 1][0], pad.t + ih);
        ctx.closePath();
        ctx.fillStyle = grad;
        ctx.fill();
      }

      ctx.beginPath();
      pts.forEach((p, i) => (i ? ctx.lineTo(p[0], p[1]) : ctx.moveTo(p[0], p[1])));
      ctx.strokeStyle = s.color;
      ctx.lineWidth = s.width || 1.4;
      ctx.lineJoin = 'round';
      ctx.setLineDash(s.dash ? [3, 3] : []);
      ctx.stroke();
      ctx.setLineDash([]);

      // 末端点
      const last = pts[pts.length - 1];
      ctx.beginPath();
      ctx.arc(last[0], last[1], 2, 0, Math.PI * 2);
      ctx.fillStyle = s.color;
      ctx.fill();
    });
  }

  /* ----------------------------- 采样累积 ----------------------------- */

  function pushSample(st) {
    const ex = st.experiment || {};
    const host = (st.resources && st.resources.host) || {};
    const t = isNum(st.ts) ? st.ts : Date.now() / 1000;
    const stageDone = {};
    (ex.stages || []).forEach((s) => { if (isNum(s.done)) stageDone[s.name] = s.done; });
    const point = {
      t,
      done: isNum(ex.docs_done) ? ex.docs_done : null,
      total: isNum(ex.docs_total) ? ex.docs_total : null,
      pct: isNum(ex.percent) ? ex.percent : null,
      tput: isNum(ex.throughput_docs_per_sec) ? ex.throughput_docs_per_sec : null,
      cpu: isNum((host.cpu || {}).total) ? host.cpu.total : null,
      mem: isNum((host.mem || {}).percent) ? host.mem.percent : null,
      rx: isNum((host.net || {}).rx_rate) ? host.net.rx_rate : null,
      tx: isNum((host.net || {}).tx_rate) ? host.net.tx_rate : null,
      stages: stageDone,
    };
    const last = S.samples[S.samples.length - 1];
    if (last && Math.abs(t - last.t) < 0.5) {
      S.samples[S.samples.length - 1] = point;   // 同一时刻重复推送：覆盖
    } else {
      S.samples.push(point);
      if (S.samples.length > MAX_POINTS) S.samples.splice(0, S.samples.length - MAX_POINTS);
    }
  }

  /** 近 60s 的实测吞吐（用 docs_done 差分，比单点吞吐更稳） */
  function rateLastMinute() {
    const pts = S.samples.filter((p) => isNum(p.done));
    if (pts.length < 2) return null;
    const tEnd = pts[pts.length - 1].t;
    const win = pts.filter((p) => p.t >= tEnd - 60);
    if (win.length >= 2) {
      const dt = win[win.length - 1].t - win[0].t;
      const dd = win[win.length - 1].done - win[0].done;
      if (dt >= 2 && dd >= 0) return dd / dt;
    }
    const rates = pts.filter((p) => isNum(p.tput)).map((p) => p.tput);
    if (!rates.length) return null;
    return rates.reduce((a, b) => a + b, 0) / rates.length;
  }

  /** 近 60s 的加权总进度斜率（percent/秒），docs_done 不推进时兜底估 ETA */
  function pctRatePerMinute() {
    const pts = S.samples.filter((p) => isNum(p.pct));
    if (pts.length < 2) return null;
    const tEnd = pts[pts.length - 1].t;
    const win = pts.filter((p) => p.t >= tEnd - 60);
    if (win.length < 2) return null;
    const dt = win[win.length - 1].t - win[0].t;
    const dp = win[win.length - 1].pct - win[0].pct;
    if (dt < 2 || dp < 0) return null;
    return dp / dt;
  }

  /** 某阶段自己的 done 变化速率（单位随阶段：文档/桶/边），用于阶段 ETA */
  function stageRate(name) {
    const pts = S.samples.filter((p) => p.stages && isNum(p.stages[name]));
    if (pts.length < 2) return null;
    const tEnd = pts[pts.length - 1].t;
    const win = pts.filter((p) => p.t >= tEnd - 60);
    if (win.length < 2) return null;
    const dt = win[win.length - 1].t - win[0].t;
    const dd = win[win.length - 1].stages[name] - win[0].stages[name];
    if (dt < 2 || dd < 0) return null;
    const v = dd / dt;
    return v > 0 ? v : null;
  }

  function movingAvg(values, times, winSec) {
    const out = new Array(values.length).fill(null);
    let sum = 0, cnt = 0, j = 0;
    for (let i = 0; i < values.length; i++) {
      const v = values[i];
      if (isNum(v)) { sum += v; cnt++; }
      while (isNum(times[i]) && isNum(times[j]) && times[i] - times[j] > winSec) {
        if (isNum(values[j])) { sum -= values[j]; cnt--; }
        j++;
      }
      out[i] = cnt ? sum / cnt : null;
    }
    return out;
  }

  /* ----------------------------- 阶段元数据 ----------------------------- */
  // 两种算法共用 shuffle/compare/verify/resolve，首段分别是 fingerprint / signature；
  // 只有 shuffle / verify 的文案随算法变化，其余共用；未知阶段名一律原样显示。
  const STAGE_LABEL = {
    signature: '签名计算', shuffle: '切带分发',
    compare: '桶内比较', verify: 'Jaccard 复核', resolve: '并查集归并',
    exact: '精确预聚合', band: '分带入桶',
    init: '初始化', done: '完成',
  };
  const ALGO_STAGE_LABEL = {
    minhash: { shuffle: '分带入桶', verify: 'Jaccard 估计' },
    slim: {
      exact: '精确预聚合', signature: '签名计算', band: '分带入桶',
      compare: '桶内比较（排序扫描）', resolve: '并查集归并',
    },
  };
  const ALGO_DEFAULT_ORDER = {
    minhash: ['signature', 'shuffle', 'compare', 'verify', 'resolve'],
    slim: ['exact', 'signature', 'band', 'compare', 'resolve'],
  };
  const STAGE_ORDER_FALLBACK = ['fingerprint', 'shuffle', 'compare', 'verify', 'resolve'];
  const STATUS_TEXT = { pending: '未开始', running: '进行中', done: '已完成' };

  const ALGO_META = {
    minhash: { label: 'minhash', title: 'MinHash', sub: '签名 · 相似度阈值', cls: 'minhash' },
    slim: { label: 'slim', title: 'Slim MinHash', sub: '预聚合 · 瘦带行 · 排序扫描', cls: 'minhash' },
  };
  // 头部参数条：simhash 看 bits/band/hamming，minhash 看 bands×rows/阈值
  const PARAM_ORDER = {
    minhash: ['bands', 'rows', 'threshold', 'shingle', 'verify'],
    slim: ['bands', 'rows', 'threshold', 'shingle', 'pivot_over'],
  };

  let _stageOrder = STAGE_ORDER_FALLBACK;   // 当前状态的阶段顺序（来自 progress.json，动态）

  function algoOf(st) {
    const ex = (st && st.experiment) || {};
    const a = String(ex.algorithm || (st && st.algorithm) || '').trim().toLowerCase();
    return a || null;
  }

  function stageLabel(name, algo) {
    if (!name) return '—';
    const ov = ALGO_STAGE_LABEL[algo] || {};
    return ov[name] || STAGE_LABEL[name] || name;
  }

  const stageRank = (name) => {
    const i = _stageOrder.indexOf(name);
    return i === -1 ? 99 : i;
  };

  /** 阶段顺序：progress.json 里有 stages 就按它的顺序（不硬编码只有 5 个固定阶段） */
  function stageOrder(st) {
    const ex = (st && st.experiment) || {};
    const order = [];
    (ex.stages || []).forEach((s) => {
      const n = s && s.name;
      if (n && n !== 'done' && n !== 'init' && !order.includes(n)) order.push(n);
    });
    if (order.length) return order;
    return ALGO_DEFAULT_ORDER[algoOf(st)] || STAGE_ORDER_FALLBACK;
  }

  /** 参数条文案，例如 bits=128 · band=16 · ham≤12 · sh=3 · verify=0.80 */
  function paramText(algo, params) {
    if (!params || typeof params !== 'object') return '';
    const order = PARAM_ORDER[algo] || Object.keys(params);
    const nameOf = (k) => (k === 'band_bits' ? 'band' : k === 'shingle' ? 'sh'
      : k === 'threshold' ? 't' : k === 'hamming' ? 'ham' : k);
    const parts = [];
    order.forEach((k) => {
      const v = params[k];
      if (!isNum(v)) return;
      const isRate = k === 'verify' || k === 'threshold';
      parts.push(nameOf(k) + '=' + (isRate ? num(v, 2) : int(v)));
    });
    return parts.join(' · ');
  }

  function stageStatuses(st) {
    const ex = st.experiment || {};
    const map = new Map();
    (ex.stages || []).forEach((s) => map.set(s.name, s.status || 'pending'));
    if (ex.stage === 'done') _stageOrder.forEach((n) => map.set(n, 'done'));
    return _stageOrder.map((n) => map.get(n) || 'pending');
  }

  /** 「阶段 x/N」里的 x：第一个未完成阶段的位置（全部完成 = N） */
  function stageIndex(statuses) {
    const i = statuses.findIndex((s) => s !== 'done');
    return i === -1 ? statuses.length : i + 1;
  }

  /* ----------------------------- 顶部状态条 ----------------------------- */
  function renderAlgo(ex, st) {
    const algo = algoOf(st);
    const meta = ALGO_META[algo] || null;
    const badge = $('#sb-algo');
    if (badge) {
      badge.className = 'algobadge ' + (meta ? meta.cls : 'unknown');
      badge.textContent = meta ? meta.label : '未知';
      badge.title = meta
        ? '算法：' + meta.title + '（来源 ' + (ex.algorithm_source || '?') + '）'
        : 'progress.json / result.json 都没有 algorithm 字段';
    }
    const sub = $('#sb-algo-sub');
    if (sub) sub.textContent = meta ? meta.sub : '算法未上报';

    const pv = $('#sb-params');
    if (pv) {
      const text = paramText(algo, ex.params || {});
      pv.textContent = text || '—';
      pv.title = ex.params_source ? '参数来源：' + ex.params_source : '实验还没写入参数';
    }
  }

  function renderStatus(st) {
    const ex = st.experiment || {};
    const ray = (st.resources && st.resources.ray) || {};
    const app = st.app || {};
    const algo = algoOf(st);

    renderAlgo(ex, st);

    $('#sb-run').textContent = ex.run_id || '—';
    $('#sb-stage').textContent = ex.stage ? stageLabel(ex.stage, algo) : '—';
    $('#sb-stage').title = ex.stage || '';

    const done = ex.docs_done, total = ex.docs_total;
    const p = isNum(ex.percent) ? ex.percent : pctNum(done, total);
    $('#sb-prog-text').textContent =
      (isNum(p) ? num(p) + '%  ·  ' : '') +
      (isNum(done) ? int(done) : '—') + ' / ' + (isNum(total) ? int(total) : '—');
    $('#sb-prog-fill').style.width = clampPct(p) + '%';
    $('#sb-prog-fill').title = ex.percent_source === 'computed' ? '按 docs_done/total 估算（progress.json 无 percent）' : '';

    // 阶段 x/N 圆点（阶段数随算法的阶段词表变化）
    const statuses = stageStatuses(st);
    $('#sb-dots').innerHTML = _stageOrder.map((name, i) =>
      '<i class="sdot ' + statuses[i] + '" title="' + (i + 1) + '. ' + esc(stageLabel(name, algo)) +
      '（' + name + '）· ' + (STATUS_TEXT[statuses[i]] || statuses[i]) + '"></i>').join('');
    const hasStageInfo = (ex.stages || []).length > 0 || !!ex.stage;
    $('#sb-stage-n').textContent = hasStageInfo
      ? stageIndex(statuses) + '/' + _stageOrder.length : '—';

    $('#sb-tput').textContent = isNum(ex.throughput_docs_per_sec)
      ? num(ex.throughput_docs_per_sec) + ' docs/s' : '—';

    // 数据新鲜度
    const fresh = $('#sb-fresh');
    const a = ex.data_age_sec;
    fresh.className = 'pill ' + (
      !isNum(a) ? 'unknown'
        : a > (app.fresh_stale_sec || 60) ? 'err'
          : a > (app.fresh_warn_sec || 15) ? 'warn' : 'ok');
    if (!ex.progress_exists) fresh.textContent = '无数据';
    else if (!isNum(a)) fresh.textContent = '未知';
    else fresh.textContent = (a > (app.fresh_stale_sec || 60) ? '数据陈旧 · '
      : a > (app.fresh_warn_sec || 15) ? '数据陈旧 · ' : '') + age(a);

    // 服务健康
    const hp = $('#sb-health');
    const up = isNum(app.uptime_sec) ? dur(app.uptime_sec) : '';
    if (S.connected) { hp.className = 'pill ok'; hp.textContent = '正常' + (up ? ' · ' + up : ''); }
    else { hp.className = 'pill err'; hp.textContent = '连接中断'; }

    // Ray
    const rp = $('#sb-ray');
    if (ray.available && ray.cpu && isNum(ray.cpu.total)) {
      rp.className = 'pill ok';
      rp.textContent = '已连接 · ' + num(ray.cpu.used, 1) + '/' + num(ray.cpu.total, 0) + ' CPU';
      rp.title = '';
    } else {
      rp.className = 'pill warn';
      rp.textContent = ray.error ? '不可用' : '未采样';
      rp.title = ray.error || '';
    }

    $('#conn-dot').className = 'dot ' + (S.connected ? 'live' : 'down');
    document.title = (ex.run_id ? ex.run_id + ' · ' : '') +
      (isNum(p) ? num(p) + '% · ' : '') + '任务监控';
  }

  /* ----------------------------- ① 任务分配 ----------------------------- */
  let _idleThreshold = 15;                 // 由服务端 app.idle_threshold_sec 下发

  function freshnessClass(w) {
    const a = w.age_sec;
    if (!isNum(a)) return 'idle';
    if (a <= 5) return 'ok';
    if (a <= _idleThreshold) return 'warn';
    return 'idle';
  }

  function workerRows(st) {
    const items = ((st.workers || {}).items) || [];
    return CFG.stage === 'all' ? items : items.filter((w) => w.stage === CFG.stage);
  }

  /** 按 stage 分组（指纹 -> 切带 -> 比较 -> 复核 -> 归并 -> 其它） */
  function groupByStage(rows) {
    const g = new Map();
    rows.forEach((w) => {
      const k = w.stage || 'unknown';
      if (!g.has(k)) g.set(k, []);
      g.get(k).push(w);
    });
    return Array.from(g.entries()).sort((a, b) =>
      stageRank(a[0]) - stageRank(b[0]) || String(a[0]).localeCompare(String(b[0])));
  }

  function groupStats(list) {
    const sum = (f) => list.reduce((a, w) => a + (isNum(w[f]) ? w[f] : 0), 0);
    const n = list.length;
    const tput = sum('docs_per_sec');
    return {
      n,
      tput,
      avg: n ? tput / n : 0,
      docs: sum('docs_done'),
      batches: sum('batches_done'),
      idle: list.filter((w) => w.idle).length,
    };
  }

  function renderAlloc(st) {
    const wk = st.workers || {};
    const app = st.app || {};
    const algo = algoOf(st);
    _idleThreshold = isNum(app.idle_threshold_sec) ? app.idle_threshold_sec : 15;

    const all = wk.items || [];
    const rows = workerRows(st);
    const idle = rows.filter((w) => w.idle).length;

    // 摘要
    const sub = $('#alloc-sub');
    if (!all.length) {
      sub.textContent = wk.dir_exists ? '等待 worker 上报…' : '等待实验启动（无 workers 目录）';
    } else {
      const stages = new Set(all.map((w) => w.stage));
      let line = rows.length + ' 个 worker · ' + stages.size + ' 个阶段';
      if (wk.idle_judged === false && wk.stale_count) {
        line += ' · ' + wk.stale_count + ' 个长时间无更新（整体进度陈旧，暂停空闲判定）';
      } else if (wk.all_stale) {
        line += ' · 全部超过 ' + Math.round(_idleThreshold) + 's 未更新（疑似写入端未刷新）';
      } else {
        line += ' · 疑似空闲 ' + idle;
      }
      sub.textContent = line;
    }

    // 阶段筛选 chips
    const byStage = {};
    all.forEach((w) => { byStage[w.stage] = (byStage[w.stage] || 0) + 1; });
    const chips = ['<button class="chip' + (CFG.stage === 'all' ? ' active' : '') +
      '" data-stage="all">全部<span class="n">' + all.length + '</span></button>'];
    Object.keys(byStage).sort((a, b) => stageRank(a) - stageRank(b)).forEach((s) => {
      chips.push('<button class="chip' + (CFG.stage === s ? ' active' : '') + '" data-stage="' +
        esc(s) + '" title="' + esc(s) + '">' + esc(stageLabel(s, algo)) +
        '<span class="n">' + byStage[s] + '</span></button>');
    });
    $('#stage-filter').innerHTML = chips.join('');

    // 吞吐相对值（全表最大值，保证条形可横向比较）
    let maxTput = 0;
    rows.forEach((w) => { if (isNum(w.docs_per_sec)) maxTput = Math.max(maxTput, w.docs_per_sec); });

    const tbody = $('#worker-body');
    if (!rows.length) {
      tbody.innerHTML = '<tr><td colspan="9" class="empty">' +
        (all.length ? '当前筛选无 worker' : '等待实验启动…') + '</td></tr>';
    } else {
      const groups = groupByStage(rows);
      tbody.innerHTML = groups.map(([stage, list]) => {
        const g = groupStats(list);
        const head = '<tr class="grp"><td colspan="9">' +
          '<i class="gdot ' + (g.idle === g.n ? 'idle' : 'on') + '"></i>' +
          '<span class="gname">' + esc(stageLabel(stage, algo)) + '</span>' +
          '<span class="graw mono">' + esc(stage) + '</span>' +
          '<span class="gstat">' + g.n + ' 个 worker · 平均 ' + num(g.avg) + ' docs/s · 合计 ' +
          int(g.docs) + ' 篇 / ' + int(g.batches) + ' 批' +
          (g.idle ? ' · <b class="dim">' + g.idle + ' 个无更新</b>' : '') + '</span>' +
          '</td></tr>';
        return head + list.map((w) => {
          const fr = freshnessClass(w);
          const dotCls = fr === 'ok' ? '' : fr === 'warn' ? ' warn' : ' idle';
          const lb = w.last_batch || {};
          const lbTxt = isNum(lb.start) && isNum(lb.end)
            ? int(lb.start) + '–' + int(lb.end) + (isNum(lb.docs) ? ' · ' + int(lb.docs) : '')
            : '—';
          const tputW = isNum(w.docs_per_sec) && maxTput > 0
            ? Math.max(2, Math.round(w.docs_per_sec * 100 / maxTput)) : 0;
          return '<tr class="' + (w.idle ? 'idle' : '') + '">' +
            '<td><span class="wname"><i class="dot-mini' + dotCls + '"></i><span class="txt">' +
              esc(w.worker_id) + '</span>' +
              (w.idle && !wk.all_stale ? '<span class="badge-idle">疑似空闲</span>' : '') +
            '</span></td>' +
            '<td class="mono">' + (isNum(w.pid) ? w.pid : '—') + '</td>' +
            '<td class="mono">' + esc(w.node || '—') + '</td>' +
            '<td>' + esc(stageLabel(w.stage, algo)) + '</td>' +
            '<td class="r">' + int(w.batches_done) + '</td>' +
            '<td class="r">' + int(w.docs_done) + '</td>' +
            '<td><span class="tput-cell"><span class="mini"><i style="width:' + tputW + '%"></i></span>' +
              '<span class="val">' + (isNum(w.docs_per_sec) ? num(w.docs_per_sec) : '—') + '</span></span></td>' +
            '<td class="r dim">' + lbTxt + '</td>' +
            '<td class="r dim">' + (isNum(w.age_sec) ? Math.round(w.age_sec) + 's' : '—') + '</td>' +
            '</tr>';
        }).join('');
      }).join('');
    }

    // 合计行
    const g = groupStats(rows);
    $('#worker-foot').innerHTML = rows.length
      ? '<tr><td colspan="4">合计 ' + g.n + ' 个 worker（空闲 ' + g.idle + '）</td>' +
        '<td class="r">' + int(g.batches) + '</td>' +
        '<td class="r">' + int(g.docs) + '</td>' +
        '<td class="mono">' + num(g.tput) + ' docs/s</td>' +
        '<td colspan="2"></td></tr>'
      : '';

    // 批次矩阵（同样按 stage 分组）
    const matrix = $('#matrix');
    if (!rows.length) {
      matrix.innerHTML = '<div class="empty">' + (all.length ? '当前筛选无 worker' : '等待 worker 上报…') + '</div>';
      return;
    }
    const CAP = 60;
    const keepTop = matrix.scrollTop;
    matrix.innerHTML = groupByStage(rows).map(([stage, list]) => {
      const gs = groupStats(list);
      const head = '<div class="mgroup"><span class="gname">' + esc(stageLabel(stage, algo)) + '</span>' +
        '<span class="graw mono">' + esc(stage) + '</span>' +
        '<span class="gstat">' + gs.n + ' worker · 平均 ' + num(gs.avg) + ' docs/s</span></div>';
      const body = list.map((w) => {
        const fr = freshnessClass(w);
        const batches = isNum(w.batches_done) ? Math.max(0, Math.round(w.batches_done)) : 0;
        const shown = Math.min(batches, CAP);
        let cells = '';
        for (let i = 0; i < shown; i++) {
          const isLast = i === shown - 1;
          let cls;
          if (fr === 'ok' || fr === 'warn') {
            const depth = shown <= 1 ? 0 : i / (shown - 1);
            cls = (fr === 'warn' ? 'warn' : depth > 0.66 ? 'on' : depth > 0.33 ? 'on2' : 'on3');
          } else {
            cls = 'idle';
          }
          cells += '<i class="sq ' + cls + (isLast && fr !== 'idle' ? ' last' : '') + '"></i>';
        }
        if (!shown) cells = '<i class="sq idle"></i>';
        const more = batches > CAP ? '<span class="mmore">+' + (batches - CAP) + '</span>' : '';
        return '<div class="mrow"><span class="mlabel' + (w.idle ? ' idle' : '') + '" title="' +
          esc(w.worker_id) + ' · ' + batches + ' 批">' + esc(w.worker_id) + '</span>' +
          '<span class="msq-wrap">' + cells + '</span>' + more + '</div>';
      }).join('');
      return head + body;
    }).join('');
    matrix.scrollTop = keepTop;
  }

  /* ----------------------------- ② 进展 ----------------------------- */
  function stageEta(s) {
    if (!isNum(s.total) || !isNum(s.done)) return null;
    if (s.done >= s.total) return 0;
    const r = stageRate(s.name);
    if (isNum(r) && r > 0) return (s.total - s.done) / r;
    if (isNum(s.eta_sec)) return s.eta_sec;
    return null;
  }

  function renderProgress(st) {
    const ex = st.experiment || {};
    const app = st.app || {};
    const algo = algoOf(st);

    const p = isNum(ex.percent) ? ex.percent : pctNum(ex.docs_done, ex.docs_total);
    $('#prog-docs').textContent =
      (isNum(ex.docs_done) ? int(ex.docs_done) : '—') + ' / ' +
      (isNum(ex.docs_total) ? int(ex.docs_total) : '—') + ' 篇';
    $('#prog-pct').textContent = isNum(p) ? num(p) + '%' : '—';
    $('#prog-fill').style.width = clampPct(p) + '%';

    const stages = ex.stages || [];
    const cur = ex.stage;
    $('#prog-sub').textContent = stages.length
      ? '共 ' + stages.length + ' 个阶段' + (cur ? ' · 当前 ' + stageLabel(cur, algo) : '')
      : (ex.progress_exists ? '进度文件无阶段信息' : '等待实验启动');

    const list = $('#stage-list');
    if (!stages.length) {
      list.innerHTML = '<div class="empty">' + (ex.progress_exists ? '暂无阶段数据' : '等待实验启动…') + '</div>';
    } else {
      list.innerHTML = stages.map((s) => {
        const status = s.status || 'pending';
        const hasTotal = isNum(s.total) && s.total > 0;
        const pct = hasTotal && isNum(s.done)
          ? Math.max(0, Math.min(100, s.done * 100 / s.total)) : null;
        const eta = stageEta(s);
        let nums;
        if (status === 'pending') nums = '未开始';
        else if (!hasTotal) nums = int(s.done) + ' / — · 进行中';
        else nums = int(s.done) + ' / ' + int(s.total) + ' · ' + num(pct) + '%' +
          (isNum(eta) ? (eta > 0 ? ' · ETA ' + dur(eta) : ' · 已完成') : '');
        // total 未知时不画 0%：进行中 -> 流动条，未开始 -> 静态灰条
        let bar;
        if (pct !== null) {
          bar = '<div class="bar"><i class="' + status + '" style="width:' +
            clampPct(pct) + '%"></i></div>';
        } else if (status === 'running') {
          bar = '<div class="bar"><i class="indet running"></i></div>';
        } else {
          bar = '<div class="bar"><i class="pending" style="width:0%"></i></div>';
        }
        return '<div class="stage ' + status + '"' +
          (s.name === cur ? ' data-current="1"' : '') + '>' +
          '<div class="stage-top"><i class="sdot ' + status + '"></i>' +
          '<span class="stage-name">' + esc(s.name === 'done' ? '完成' : (s.label || stageLabel(s.name, algo))) + '</span>' +
          '<span class="stage-raw mono">' + esc(s.name) + '</span>' +
          (isNum(s.weight) ? '<span class="wtag" title="加权系数 weight">w=' + num(s.weight, 2) + '</span>' : '') +
          '<span class="stage-nums">' + nums + '</span></div>' + bar + '</div>';
      }).join('');
    }

    // ETA 卡片
    const rm = rateLastMinute();
    const pr = pctRatePerMinute();
    let overallEta = null;
    if (isNum(rm) && rm > 0 && isNum(ex.docs_total) && isNum(ex.docs_done)) {
      overallEta = (ex.docs_total - ex.docs_done) / rm;
    }
    if (overallEta === null && isNum(pr) && pr > 0 && isNum(p)) overallEta = (100 - p) / pr;

    const running = stages.filter((s) => s.status === 'running');
    const curStage = running.find((s) => s.name === cur) || running[0] || null;
    const curEta = curStage ? stageEta(curStage) : null;

    const etaCards = [
      { k: '整体 ETA', v: isNum(overallEta) ? dur(overallEta) : '—' },
      { k: '当前阶段 ETA', v: isNum(curEta) && curEta > 0 ? dur(curEta) : '—' },
      { k: '近 1 分钟吞吐', v: isNum(rm) ? num(rm) + ' /s' : '—' },
      { k: '已处理', v: int(ex.docs_done) },
    ];
    $('#etas').innerHTML = etaCards.map((c) =>
      '<div class="eta"><div class="k">' + c.k + '</div><div class="v">' + c.v + '</div></div>').join('');

    $('#chart-progress-count').textContent = S.samples.length + ' 点 / ' +
      dur(app.sample_interval ? app.sample_interval * S.samples.length : 0) + '（上限 ' + MAX_POINTS + ' 点）';
  }

  /* ----------------------------- ③ 任务输出 ----------------------------- */
  const RESULT_KEYS = [
    ['dup_recall', '召回率', 'rate'], ['neg_false_drop_rate', '负样本误删', 'rate'],
    ['docs_per_sec', '吞吐 docs/s', 'num'], ['seconds', '耗时 s', 'num'],
    ['kept', '保留', 'int'], ['dropped', '丢弃', 'int'],
    ['edges', '重复边', 'int'], ['dup_total', '重复总数', 'int'],
    ['neg_total', '负样本数', 'int'], ['extra_kept_groups', '多留组', 'int'],
    ['missing_kept_groups', '漏留组', 'int'], ['fp_sec', '指纹 s', 'num'],
    ['dedup_sec', '去重 s', 'num'], ['docs', '文档数', 'int'],
  ];

  function fmtMetric(v, kind) {
    if (!isNum(v)) return '—';
    if (kind === 'rate') return (v * 100).toFixed(2) + '%';
    if (kind === 'int') return int(v);
    return num(v, 2);
  }

  /** 漏斗：候选对 → 复核后 → 最终丢弃（字段可能都不存在） */
  function renderFunnel(ex) {
    const box = $('#funnel');
    if (!box) return;
    const main = [
      { k: '候选对', v: ex.verify_total, tip: 'verify_total' },
      { k: '复核后保留边', v: ex.verify_kept, tip: 'verify_kept' },
      { k: '最终丢弃', v: ex.docs_dropped, tip: 'docs_dropped', cls: 'fdrop' },
    ];
    const extra = [
      { k: '最终保留', v: ex.docs_kept, tip: 'docs_kept' },
      { k: '重复边', v: ex.edges, tip: 'edges' },
    ];
    if (!main.concat(extra).some((c) => isNum(c.v))) {
      box.innerHTML = '<div class="empty">尚无漏斗数据' +
        '（verify_total / verify_kept / docs_kept / docs_dropped / edges 都还没写入）</div>';
      return;
    }
    const cell = (c) => '<div class="fcell ' + (c.cls || '') + '" title="' + esc(c.tip) + '">' +
      '<div class="k">' + esc(c.k) + '</div><div class="v mono">' + int(c.v) + '</div></div>';
    box.innerHTML = '<div class="funnel-row">' +
      main.map(cell).join('<span class="farrow">→</span>') +
      '<span class="fgap"></span>' + extra.map(cell).join('') + '</div>';
  }

  function renderOutput(st) {
    const rs = st.result || {};
    const ex = st.experiment || {};

    renderFunnel(ex);

    const block = $('#result-block');
    if (!rs.exists || !rs.data) {
      block.innerHTML = '<div class="empty">' +
        (rs.error === 'invalid_json' ? 'result.json 解析失败（可能正在写入）'
          : '尚无 result.json —— 实验未产出最终报告') + '</div>';
      $('#out-sub').textContent = '';
    } else {
      const modes = ['ray', 'local'].filter((k) => rs.data[k] && typeof rs.data[k] === 'object');
      const others = Object.keys(rs.data).filter((k) =>
        !['ray', 'local'].includes(k) && rs.data[k] && typeof rs.data[k] === 'object');
      const allModes = modes.concat(others);

      const cards = allModes.map((mode) => {
        const d = rs.data[mode] || {};
        const keys = RESULT_KEYS.filter(([k]) => k in d);
        Object.keys(d).forEach((k) => {
          if (!keys.find(([kk]) => kk === k) && isNum(d[k])) keys.push([k, k, 'num']);
        });
        return '<div class="rmode"><div class="rmode-head">' +
          (mode === 'ray' ? 'Ray 集群' : mode === 'local' ? '单机基线' : esc(mode)) +
          '<span class="tag">' + esc(mode) + '</span>' +
          (isNum(d.docs_per_sec) ? '<span class="tag" style="margin-left:auto">' +
            num(d.docs_per_sec) + ' docs/s</span>' : '') +
          '</div><div class="metrics">' +
          keys.map(([k, label, kind]) => {
            const hl = (k === 'dup_recall' || k === 'neg_false_drop_rate') ? ' hl' : '';
            return '<div class="metric' + hl + '"><div class="k" title="' + esc(k) + '">' +
              esc(label) + '</div><div class="v">' + fmtMetric(d[k], kind) + '</div></div>';
          }).join('') +
          '</div></div>';
      }).join('');

      const algo = algoOf(st);
      const algoMeta = ALGO_META[algo] || null;
      const pv = paramText(algo, ex.params || {});
      const paramsRow = pv
        ? '<div class="res-note mono" style="margin-top:2px" title="' +
          esc(ex.params_source || '') + '">参数：' + esc(pv) + '</div>'
        : '';
      const meta = '生成于 ' + (isNum(rs.mtime) ? clock(rs.mtime) : '—') +
        (algoMeta ? ' · ' + algoMeta.title : '') +
        (isNum(rs.data.docs) ? ' · ' + int(rs.data.docs) + ' 篇' : '') +
        (isNum(rs.data.seed) ? ' · seed ' + rs.data.seed : '');
      block.innerHTML = (cards ? '<div class="result-modes">' + cards + '</div>'
        : '<div class="empty">result.json 已存在，但没有 ray / local 指标字典</div>') +
        paramsRow +
        '<div class="res-note mono" style="margin-top:6px">' + esc(meta) + '</div>';
      $('#out-sub').textContent = '最终报告已生成' + (algoMeta ? ' · ' + algoMeta.title : '');
    }

    // 日志（0 字节 = 「暂无日志」，不是错误）
    const lg = st.log || {};
    const empty = !!lg.exists && (lg.empty || !lg.size);
    const sig = (lg.exists ? 1 : 0) + ':' + (lg.size || 0) + ':' + (lg.mtime || 0) + ':' + CFG.logLines;
    const pre = $('#log');
    if (sig !== S.logSig) {
      S.logSig = sig;
      const atBottom = pre.scrollHeight - pre.scrollTop - pre.clientHeight < 48;
      if (!lg.exists) {
        pre.innerHTML = '<span class="muted">等待 experiment.log…</span>';
      } else if (empty) {
        pre.innerHTML = '<span class="muted">暂无日志（experiment.log 当前 0 字节；' +
          '日志跟随实验进程，进程被回收后可能为空）</span>';
      } else if (!lg.tail || !lg.tail.length) {
        pre.innerHTML = '<span class="muted">暂无日志</span>';
      } else {
        pre.innerHTML = lg.tail.map((line) => {
          const t = String(line);
          if (/traceback|error|exception|failed|失败|错误/i.test(t)) return '<span class="err">' + esc(t) + '</span>';
          if (/warn|警告/i.test(t)) return '<span class="warn">' + esc(t) + '</span>';
          return esc(t);
        }).join('\n');
      }
      if (CFG.autoscroll || atBottom) pre.scrollTop = pre.scrollHeight;
    }
    $('#log-meta').textContent = !lg.exists ? '文件不存在'
      : empty ? '0 B · 暂无日志'
        : bytes(lg.size) + ' · 尾部 ' + ((lg.tail || []).length) + ' 行';

    // 事件流
    const evs = ex.events || [];
    $('#ev-count').textContent = evs.length ? evs.length + ' 条' : '';
    const ul = $('#events');
    if (!evs.length) {
      ul.innerHTML = '<li class="empty">暂无事件</li>';
    } else {
      ul.innerHTML = evs.map((e) => {
        const lv = (e.level || 'info').toLowerCase();
        return '<li><span class="ts">' + (isNum(e.ts) ? clock(e.ts) : '—') + '</span>' +
          '<span class="lv ' + esc(lv) + '">' + esc(lv) + '</span>' +
          '<span class="msg">' + esc(e.msg || '') + '</span></li>';
      }).join('');
    }
  }
  /* ----------------------------- ⑤ 服务器资源 ----------------------------- */
  let coresBuilt = 0;

  function renderResources(st) {
    const host = (st.resources && st.resources.host) || {};
    const ray = (st.resources && st.resources.ray) || {};
    const cpu = host.cpu || {};
    const mem = host.mem || {};
    const load = host.load || {};
    const disk = host.disk || {};
    const net = host.net || {};

    $('#res-cpu').textContent = isNum(cpu.total) ? num(cpu.total) + '%' : '—';
    $('#res-cpu-fill').style.width = clampPct(cpu.total) + '%';

    // 每核
    const per = Array.isArray(cpu.per_core) ? cpu.per_core : [];
    const cores = $('#cores');
    if (coresBuilt !== per.length) {
      cores.innerHTML = per.map((_, i) => '<span class="core" title="核心 ' + i + '"><i></i></span>').join('');
      coresBuilt = per.length;
    }
    $$('#cores .core').forEach((el, i) => {
      const v = per[i];
      const bar = el.firstChild;
      bar.style.height = clampPct(v) + '%';
      bar.className = v >= 90 ? 'hot' : v >= 60 ? 'warn' : '';
      el.title = '核心 ' + i + ' · ' + (isNum(v) ? v.toFixed(0) + '%' : '—');
    });

    $('#res-mem').textContent = isNum(mem.percent) ? num(mem.percent) + '%' : '—';
    $('#res-mem-fill').style.width = clampPct(mem.percent) + '%';
    $('#res-mem-note').textContent = (isNum(mem.used) && isNum(mem.total))
      ? bytes(mem.used) + ' / ' + bytes(mem.total) : '—';

    const l1 = load['1'], l5 = load['5'], l15 = load['15'];
    $('#res-load').textContent = isNum(l1) ? num(l1, 2) + ' / ' + num(l5, 2) + ' / ' + num(l15, 2) : '—';
    const ncpu = isNum(cpu.count) ? cpu.count : (per.length || 1);
    $('#res-load-note').textContent = '1 / 5 / 15 分钟 · ' + ncpu + ' 核' +
      (isNum(l1) ? '，负载率 ' + num(l1 * 100 / ncpu, 0) + '%' : '');

    $('#res-disk').textContent = isNum(disk.percent) ? num(disk.percent) + '%' : '—';
    $('#res-disk-fill').style.width = clampPct(disk.percent) + '%';
    $('#res-disk-note').textContent = (isNum(disk.used) && isNum(disk.total))
      ? bytes(disk.used) + ' / ' + bytes(disk.total) + '（余 ' + bytes(disk.free) + '）' : '—';

    $('#res-net').textContent = (isNum(net.rx_rate) ? rate(net.rx_rate) : '—');
    $('#res-net-note').textContent = '↓ ' + (isNum(net.rx_rate) ? rate(net.rx_rate) : '—') +
      ' · ↑ ' + (isNum(net.tx_rate) ? rate(net.tx_rate) : '—');

    const rc = ray.cpu || {};
    const os_ = ray.object_store || {};
    const memR = ray.memory || {};
    if (ray.available && isNum(rc.total)) {
      $('#res-ray').textContent = num(rc.used, 1) + ' / ' + num(rc.total, 0) + ' CPU';
      $('#res-ray-fill').style.width = clampPct(isNum(rc.util) ? rc.util * 100 : pctNum(rc.used, rc.total)) + '%';
      $('#res-ray-note').textContent = 'obj ' + bytes(os_.used || 0) + ' / ' + bytes(os_.total) +
        ' · mem ' + bytes(memR.used || 0) + ' / ' + bytes(memR.total) +
        (isNum(ray.nodes_active) ? ' · ' + ray.nodes_active + ' 节点' : '');
    } else {
      $('#res-ray').textContent = '不可用';
      $('#res-ray-fill').style.width = '0%';
      $('#res-ray-note').textContent = ray.error || '尚未采样';
    }

    const rs = $('#res-sub');
    rs.textContent = isNum(host.ts) ? '采样于 ' + clock(host.ts) : '尚未采样';
    if (isNum(ray.duration_ms)) rs.textContent += ' · ray status ' + Math.round(ray.duration_ms) + 'ms';

    $('#chart-res-count').textContent = (host.history ? host.history.length : 0) + ' 点（服务端环形缓冲）';
  }

  /* ----------------------------- ④ 历史对比 ----------------------------- */
  const ALGO_COLOR = { simhash: '#38bdf8', minhash: '#a78bfa' };

  const HIST = {
    data: null,                       // /api/runs 整包（runs / algo_summary / runs_error）
    error: null,
    algo: 'all',                      // 过滤：all / simhash / minhash
    sort: { key: 'run_id', dir: -1 }, // 默认最新 run 在前
    hover: null,
    loading: false,
  };

  const fmtRatio = (v) => (isNum(v) ? (v * 100).toFixed(2) + '%' : '—');
  const signed = (v) => (!isNum(v) ? '—' : (v > 0 ? '+' : '') + NF0.format(v));
  const fmtDocs = (v) => (!isNum(v) ? '—' : v >= 1000 ? Math.round(v / 1000) + 'k' : String(Math.round(v)));

  /** 参数列：simhash 显示 bits/band/hamming，minhash 显示 bands×rows t */
  function configText(r) {
    if (isNum(r.bands) || isNum(r.rows)) {
      return int(r.bands) + '×' + int(r.rows) + (isNum(r.threshold) ? ' t=' + num(r.threshold, 2) : '');
    }
    const parts = [];
    if (isNum(r.bits)) parts.push(int(r.bits) + 'b');
    if (isNum(r.band_bits)) parts.push('band ' + int(r.band_bits));
    if (isNum(r.hamming)) parts.push('ham≤' + int(r.hamming));
    return parts.join(' · ') || '—';
  }

  const HIST_SORT = {
    run_id: (r) => r.run_id || '',
    algo: (r) => r.algo || '',
    mode: (r) => r.mode || '',
    docs: (r) => r.docs,
    // 参数本身没有天然大小，按「阈值越松越靠前」排：simhash 看 hamming，minhash 看 bands
    config: (r) => (isNum(r.hamming) ? 1000 - r.hamming
      : isNum(r.bands) ? -r.bands * 100 - (isNum(r.rows) ? r.rows : 0) : null),
    verify: (r) => r.verify,
    pair_recall: (r) => r.pair_recall,
    pair_precision: (r) => r.pair_precision,
    edge_fp_cross_base: (r) => r.edge_fp_cross_base,
    components_merged: (r) => r.components_merged,
    groups_missing: (r) => r.groups_missing,
    kept: (r) => r.kept,
    dkept: (r) => r.dKept,
    dedges: (r) => r.dEdges,
    seconds: (r) => r.seconds,
    docs_per_sec: (r) => r.docs_per_sec,
  };

  /** 同一 run 的 ray/local 差值（kept 差、edges 差），两行都显示 */
  function decorateRuns(rows) {
    const groups = new Map();
    rows.forEach((r) => {
      if (!groups.has(r.run_id)) groups.set(r.run_id, {});
      groups.get(r.run_id)[r.mode || '?'] = r;
    });
    return rows.map((r) => {
      const g = groups.get(r.run_id) || {};
      const ray = g.ray, loc = g.local;
      const dKept = (ray && loc && isNum(ray.kept) && isNum(loc.kept)) ? ray.kept - loc.kept : null;
      const dEdges = (ray && loc && isNum(ray.edges) && isNum(loc.edges)) ? ray.edges - loc.edges : null;
      return Object.assign({}, r, { dKept, dEdges });
    });
  }

  function historyRows() {
    const all = (HIST.data && HIST.data.runs) || [];
    return HIST.algo === 'all' ? all : all.filter((r) => (r.algo || '') === HIST.algo);
  }

  function sortRuns(rows) {
    const key = HIST.sort.key;
    const dir = HIST.sort.dir;
    const val = (r) => (HIST_SORT[key] ? HIST_SORT[key](r) : r[key]);
    return rows.slice().sort((a, b) => {
      const va = val(a), vb = val(b);
      const ea = va === null || va === undefined || (typeof va === 'number' && isNaN(va));
      const eb = vb === null || vb === undefined || (typeof vb === 'number' && isNaN(vb));
      if (ea && eb) return 0;
      if (ea) return 1;            // 空值永远排在末尾
      if (eb) return -1;
      if (va < vb) return -dir;
      if (va > vb) return dir;
      const ka = (a.run_id || '') + '|' + (a.mode || '');   // 同值时保持 run 相邻、ray 在前
      const kb = (b.run_id || '') + '|' + (b.mode || '');
      return ka < kb ? -1 : ka > kb ? 1 : 0;
    });
  }

  function renderHistory() {
    const body = $('#hist-body');
    if (!body) return;

    const all = (HIST.data && HIST.data.runs) || [];
    const rows = decorateRuns(historyRows());
    const sorted = sortRuns(rows);
    const src = !HIST.data ? '—'
      : HIST.data.runs_source === 'json' ? 'RAYTEST_RUNS_JSON'
      : HIST.data.runs_source === 'index' ? 'runs_index.json' : 'LanceDB dedup_runs';

    const runCount = new Set(rows.map((r) => r.run_id)).size;
    const byAlgo = {};
    rows.forEach((r) => { byAlgo[r.algo] = (byAlgo[r.algo] || 0) + 1; });
    const algoBits = Object.keys(byAlgo).sort().map((a) => a + ' ' + byAlgo[a] + ' 行').join(' / ');
    const sub = $('#hist-sub');
    if (!HIST.data) sub.textContent = '读取中…';
    else if (!rows.length) sub.textContent = src + ' · 0 行';
    else sub.textContent = src + ' · ' + rows.length + ' 行 / ' + runCount + ' 个 run' +
      (algoBits ? ' · ' + algoBits : '');

    // 数据源提示 / 降级错误（面板其它区域不受影响）
    const note = $('#hist-note');
    if (HIST.error) {
      note.className = 'hist-note err';
      note.textContent = '历史数据不可用：' + HIST.error + '（实时监控不受影响）';
    } else if (HIST.data && !all.length) {
      note.className = 'hist-note';
      note.textContent = '暂无历史 run：' + (HIST.data.runs_path || '') +
        '（跑一次带 --progress-dir /tmp/raytest 的 dedup_slim.py 就会自动出现）';
    } else if (HIST.data) {
      note.className = 'hist-note';
      note.textContent = (HIST.data.runs_source === 'json' ? '假数据演练：' : '数据源：') +
        (HIST.data.runs_path || '') + ' · 缓存 ' + (HIST.data.runs_cache_sec || '?') + 's';
    } else {
      note.className = 'hist-note';
      note.textContent = '';
    }

    // algo 过滤 chips（数量为 0 也显示，便于知道「还没有 minhash 数据」）
    const counts = { all: all.length, minhash: 0 };
    all.forEach((r) => { counts[r.algo] = (counts[r.algo] || 0) + 1; });
    const chip = (key, text) => '<button class="chip' + (HIST.algo === key ? ' active' : '') +
      '" data-halgo="' + key + '">' + text + '<span class="n">' + (counts[key] || 0) + '</span></button>';
    $('#hist-filter').innerHTML = chip('all', '全部') + chip('minhash', 'minhash');

    // 排序指示
    $$('#hist-thead th[data-sort]').forEach((th) => {
      const on = th.getAttribute('data-sort') === HIST.sort.key;
      th.classList.toggle('sorted', on);
      const ind = th.querySelector('.sort-ind');
      if (ind) ind.textContent = on ? (HIST.sort.dir > 0 ? '▲' : '▼') : '';
    });

    // 表格：每个 run 的 ray / local 两行相邻，颜色按 run 交替
    if (!sorted.length) {
      body.innerHTML = '<tr><td colspan="16" class="empty">' +
        (HIST.error ? '历史数据不可用（原因见上方提示）'
          : all.length ? '当前过滤条件下没有 run' : '暂无历史 run') + '</td></tr>';
    } else {
      const runIndex = new Map();
      all.forEach((r) => { if (!runIndex.has(r.run_id)) runIndex.set(r.run_id, runIndex.size); });
      body.innerHTML = sorted.map((r) => {
        const idx = runIndex.has(r.run_id) ? runIndex.get(r.run_id) : 0;
        const cls = (idx % 2 ? 'runodd' : 'runeven') + (r.mode === 'local' ? ' runlocal' : '');
        return '<tr class="' + cls + '">' +
          '<td class="mono hrun" title="' + esc(r.run_id || '') + '">' + esc(r.run_id || '—') + '</td>' +
          '<td><span class="atag ' + esc(r.algo || 'unknown') + '">' + esc(r.algo || '—') + '</span></td>' +
          '<td class="mono">' + esc(r.mode || '—') +
            (r.created_at ? '<span class="dim" style="margin-left:6px">' + esc(String(r.created_at).slice(11, 19)) + '</span>' : '') +
            '</td>' +
          '<td class="r mono">' + int(r.docs) + '</td>' +
          '<td class="mono dim">' + esc(configText(r)) + '</td>' +
          '<td class="mono dim" title="' + esc(r.ray_job_id || '') + '">' + esc(r.ray_job_id || '—') + '</td>' +
          '<td class="r mono">' + fmtRatio(r.pair_recall) + '</td>' +
          '<td class="r mono">' + fmtRatio(r.pair_precision) + '</td>' +
          '<td class="r mono' + (r.edge_fp_cross_base ? ' bad' : '') + '">' + int(r.edge_fp_cross_base) + '</td>' +
          '<td class="r mono' + (r.components_merged ? ' bad' : '') + '">' + int(r.components_merged) + '</td>' +
          '<td class="r mono' + (r.groups_missing ? ' bad' : '') + '">' + int(r.groups_missing) + '</td>' +
          '<td class="r mono">' + int(r.kept) + '</td>' +
          '<td class="r mono dim">' + signed(r.dKept) + '</td>' +
          '<td class="r mono dim">' + signed(r.dEdges) + '</td>' +
          '<td class="r mono">' + num(r.seconds, 1) + '</td>' +
          '<td class="r mono">' + num(r.docs_per_sec, 1) + '</td>' +
          '</tr>';
      }).join('');
    }

    drawHistoryCharts();
  }

  async function fetchRuns(force) {
    if (HIST.loading) return;
    HIST.loading = true;
    try {
      const r = await fetch('./api/runs' + (force ? '?refresh=1' : ''), { cache: 'no-store' });
      if (!r.ok) throw new Error('HTTP ' + r.status);
      const data = await r.json();
      HIST.data = data;
      HIST.error = data.runs_error || null;
    } catch (e) {
      HIST.error = '请求 /api/runs 失败：' + ((e && e.message) || e);
    } finally {
      HIST.loading = false;
      renderHistory();
    }
  }

  /* --- 散点 / 规模图（手写 canvas，无外部依赖） --- */
  function drawScatter(canvas, spec) {
    const { ctx, w, h } = fitCanvas(canvas);
    const pad = { l: 42, r: 12, t: 10, b: 24 };
    const iw = Math.max(10, w - pad.l - pad.r);
    const ih = Math.max(10, h - pad.t - pad.b);
    ctx.font = FONT;
    ctx.textBaseline = 'middle';

    const xLog = !!spec.xLog;
    const tx = (v) => (xLog ? Math.log10(Math.max(1e-9, v)) : v);
    const x0 = tx(spec.xMin), x1 = tx(spec.xMax);
    const y0 = spec.yMin, y1 = spec.yMax;
    const xOf = (v) => pad.l + iw * ((tx(v) - x0) / Math.max(1e-9, x1 - x0));
    const yOf = (v) => pad.t + ih * (1 - (v - y0) / Math.max(1e-9, y1 - y0));

    // 横向网格 + 纵轴刻度
    const rows = 4;
    for (let i = 0; i <= rows; i++) {
      const y = pad.t + ih * i / rows;
      ctx.strokeStyle = GRID;
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(pad.l, Math.round(y) + 0.5);
      ctx.lineTo(pad.l + iw, Math.round(y) + 0.5);
      ctx.stroke();
      ctx.fillStyle = AXIS_TXT;
      ctx.textAlign = 'right';
      ctx.fillText(spec.yFmt ? spec.yFmt(y1 - (y1 - y0) * i / rows) : '', pad.l - 4, y);
    }
    // 纵向网格 + 横轴刻度
    const cols = 4;
    ctx.textBaseline = 'top';
    for (let i = 0; i <= cols; i++) {
      const x = pad.l + iw * i / cols;
      ctx.strokeStyle = GRID;
      ctx.beginPath();
      ctx.moveTo(Math.round(x) + 0.5, pad.t);
      ctx.lineTo(Math.round(x) + 0.5, pad.t + ih);
      ctx.stroke();
      const v = xLog ? Math.pow(10, x0 + (x1 - x0) * i / cols) : x0 + (x1 - x0) * i / cols;
      ctx.fillStyle = AXIS_TXT;
      ctx.textAlign = i === 0 ? 'left' : i === cols ? 'right' : 'center';
      ctx.fillText(spec.xFmt ? spec.xFmt(v) : String(v), x, pad.t + ih + 5);
    }
    ctx.textBaseline = 'middle';

    const pts = (spec.points || []).filter((p) => isNum(p.x) && isNum(p.y));
    if (!pts.length) {
      ctx.fillStyle = '#4d6383';
      ctx.textAlign = 'center';
      ctx.font = '11px -apple-system, "PingFang SC", sans-serif';
      ctx.fillText(spec.empty || '暂无历史数据', pad.l + iw / 2, pad.t + ih / 2);
      return [];
    }

    // 连线（规模曲线：同算法同 mode 按 docs 排序）
    (spec.lines || []).forEach((ln) => {
      const ps = (ln.points || []).filter((p) => isNum(p[0]) && isNum(p[1]));
      if (ps.length < 2) return;
      ctx.beginPath();
      ps.forEach((p, i) => (i ? ctx.lineTo(xOf(p[0]), yOf(p[1])) : ctx.moveTo(xOf(p[0]), yOf(p[1]))));
      ctx.strokeStyle = ln.color;
      ctx.lineWidth = 1.1;
      ctx.setLineDash(ln.dash ? [3, 3] : []);
      ctx.stroke();
      ctx.setLineDash([]);
    });

    // 数据点：颜色=算法，实心=ray / 空心=local，半径=docs
    const hits = [];
    pts.forEach((p) => {
      const x = xOf(p.x), y = yOf(p.y);
      const rad = isNum(p.r) ? p.r : 3.5;
      ctx.beginPath();
      ctx.arc(x, y, rad, 0, Math.PI * 2);
      if (p.hollow) {
        ctx.fillStyle = 'rgba(10,16,28,.85)';
        ctx.fill();
        ctx.strokeStyle = p.color;
        ctx.lineWidth = 1.6;
        ctx.stroke();
      } else {
        ctx.globalAlpha = 0.85;
        ctx.fillStyle = p.color;
        ctx.fill();
        ctx.globalAlpha = 1;
        ctx.strokeStyle = 'rgba(10,16,28,.9)';
        ctx.lineWidth = 1;
        ctx.stroke();
      }
      if (p.hl) {
        ctx.beginPath();
        ctx.arc(x, y, rad + 3, 0, Math.PI * 2);
        ctx.strokeStyle = '#e6edf7';
        ctx.lineWidth = 1.4;
        ctx.stroke();
      }
      hits.push({ x, y, r: Math.max(6, rad), point: p });
    });
    return hits;
  }

  function drawRecallChart(rows) {
    const canvas = $('#chart-scatter');
    if (!canvas) return;
    const pts = rows.filter((r) => isNum(r.pair_recall) && isNum(r.pair_precision)).map((r) => ({
      x: r.pair_recall,
      y: r.pair_precision,
      r: 3 + 5 * Math.min(1, Math.log10(Math.max(10, isNum(r.docs) ? r.docs : 10)) / 5),
      color: ALGO_COLOR[r.algo] || '#8ea3c0',
      hollow: r.mode === 'local',
      key: (r.run_id || '') + '|' + (r.mode || ''),
      run: r,
      hl: HIST.hover === (r.run_id || '') + '|' + (r.mode || ''),
    }));

    let xMin = 0, xMax = 1, yMin = 0, yMax = 1;
    if (pts.length) {
      const xs = pts.map((p) => p.x), ys = pts.map((p) => p.y);
      xMin = Math.min.apply(null, xs); xMax = Math.max.apply(null, xs);
      yMin = Math.min.apply(null, ys); yMax = Math.max.apply(null, ys);
      const padX = Math.max(0.004, (xMax - xMin) * 0.18);
      const padY = Math.max(0.002, (yMax - yMin) * 0.22);
      xMin = Math.max(0, xMin - padX); xMax = Math.min(1, xMax + padX);
      yMin = Math.max(0, yMin - padY); yMax = Math.min(1, yMax + padY);
      if (xMax - xMin < 0.02) { const m = (xMax + xMin) / 2; xMin = Math.max(0, m - 0.01); xMax = Math.min(1, m + 0.01); }
      if (yMax - yMin < 0.01) { const m = (yMax + yMin) / 2; yMin = Math.max(0, m - 0.005); yMax = Math.min(1, m + 0.005); }
    }
    S.scatterHits = drawScatter(canvas, {
      points: pts, xMin, xMax, yMin, yMax,
      xFmt: (v) => (v * 100).toFixed(1) + '%',
      yFmt: (v) => (v * 100).toFixed(1) + '%',
      empty: HIST.error ? '历史数据不可用' : '暂无 run 级召回/精度',
    });
  }

  function drawScaleChart(rows) {
    const canvas = $('#chart-scale');
    if (!canvas) return;
    const pts = rows.filter((r) => isNum(r.docs) && isNum(r.docs_per_sec)).map((r) => ({
      x: r.docs,
      y: r.docs_per_sec,
      r: 3 + 4 * Math.min(1, Math.log10(Math.max(10, r.docs)) / 5),
      color: ALGO_COLOR[r.algo] || '#8ea3c0',
      hollow: r.mode === 'local',
      key: (r.run_id || '') + '|' + (r.mode || ''),
      run: r,
      hl: HIST.hover === (r.run_id || '') + '|' + (r.mode || ''),
    }));

    let xMin = 1, xMax = 10, yMax = 1;
    if (pts.length) {
      const xs = pts.map((p) => p.x);
      const lo = Math.min.apply(null, xs), hi = Math.max.apply(null, xs);
      xMin = Math.pow(10, Math.floor(Math.log10(Math.max(1, lo))));
      xMax = Math.pow(10, Math.ceil(Math.log10(Math.max(10, hi))));
      if (xMax <= xMin) xMax = xMin * 10;
      let rateMax = 0;
      pts.forEach((p) => { rateMax = Math.max(rateMax, p.y); });
      yMax = niceMax(rateMax * 1.15 || 1);
    }

    const groups = new Map();
    pts.forEach((p) => {
      const k = (p.run.algo || '?') + '|' + (p.run.mode || '?');
      if (!groups.has(k)) groups.set(k, []);
      groups.get(k).push([p.x, p.y]);
    });
    const lines = [];
    groups.forEach((arr, k) => {
      if (arr.length < 2) return;
      const parts = k.split('|');
      arr.sort((a, b) => a[0] - b[0]);
      lines.push({
        points: arr,
        color: ALGO_COLOR[parts[0]] || '#8ea3c0',
        dash: parts[1] === 'local',
      });
    });

    S.scaleHits = drawScatter(canvas, {
      points: pts, lines, xLog: true,
      xMin, xMax, yMin: 0, yMax,
      xFmt: (v) => fmtDocs(v),
      yFmt: (v) => (v >= 1000 ? (v / 1000).toFixed(1) + 'k' : Math.round(v)),
      empty: HIST.error ? '历史数据不可用' : '暂无吞吐/规模数据',
    });
    const maxEl = $('#scale-max');
    if (maxEl) maxEl.textContent = pts.length ? '峰值 ' + num(yMax, 0) + ' docs/s' : '—';
  }

  function drawHistoryCharts() {
    const rows = decorateRuns(historyRows());
    drawRecallChart(rows);
    drawScaleChart(rows);
  }

  /** 鼠标悬停：在图下方说明行显示该 run 的关键数字（不新增依赖、不改 DOM 结构） */
  function bindHistHover(canvasId, hitsOf, captionId, describe) {
    const c = $(canvasId);
    const cap = $(captionId);
    if (!c || !cap) return;
    c.addEventListener('mousemove', (e) => {
      const rect = c.getBoundingClientRect();
      const mx = e.clientX - rect.left;
      const my = e.clientY - rect.top;
      let best = null, bestD = 1e9;
      (hitsOf() || []).forEach((hit) => {
        const d = Math.hypot(hit.x - mx, hit.y - my);
        if (d <= Math.max(9, hit.r + 3) && d < bestD) { best = hit; bestD = d; }
      });
      const key = best ? best.point.key : null;
      if (key !== HIST.hover) {
        HIST.hover = key;
        drawHistoryCharts();
      }
      cap.textContent = best ? describe(best.point.run) : (cap.getAttribute('data-default') || '');
    });
    c.addEventListener('mouseleave', () => {
      HIST.hover = null;
      cap.textContent = cap.getAttribute('data-default') || '';
      drawHistoryCharts();
    });
  }

  /* ----------------------------- 图表绘制 ----------------------------- */
  function drawProgressChart() {
    const canvas = $('#chart-progress');
    if (!canvas) return;
    const times = S.samples.map((p) => p.t);
    const tput = S.samples.map((p) => p.tput);
    const pctv = S.samples.map((p) => (isNum(p.pct) ? p.pct : pctNum(p.done, p.total)));
    const avg = movingAvg(tput, times, 60);

    let lMax = 0;
    tput.concat(avg).forEach((v) => { if (isNum(v)) lMax = Math.max(lMax, v); });
    lMax = niceMax(lMax * 1.15 || 1);

    drawChart(canvas, {
      times,
      leftMax: lMax,
      rightMax: 100,
      leftFmt: (v) => (v >= 1000 ? (v / 1000).toFixed(1) + 'k' : Math.round(v)),
      rightFmt: (v) => Math.round(v) + '%',
      series: [
        { values: tput, color: '#38bdf8', width: 1.1, fill: 'rgba(56,189,248,.22)' },
        { values: avg, color: '#34d399', width: 2 },
        { values: pctv, color: '#a78bfa', width: 1.3, dash: true, axis: 'right' },
      ],
    });
  }

  function drawResChart() {
    const canvas = $('#chart-res');
    if (!canvas) return;
    const hist = ((S.last && S.last.resources && S.last.resources.host && S.last.resources.host.history) || []);
    let times, cpuArr, memArr, netArr;
    if (hist.length >= 2) {
      times = hist.map((p) => p.t);
      cpuArr = hist.map((p) => p.cpu);
      memArr = hist.map((p) => p.mem);
      netArr = hist.map((p) => ((p.net_rx || 0) + (p.net_tx || 0)) / 1048576);
    } else {
      times = S.samples.map((p) => p.t);
      cpuArr = S.samples.map((p) => p.cpu);
      memArr = S.samples.map((p) => p.mem);
      netArr = S.samples.map((p) => (isNum(p.rx) && isNum(p.tx) ? (p.rx + p.tx) / 1048576 : null));
    }
    let nMax = 0;
    netArr.forEach((v) => { if (isNum(v)) nMax = Math.max(nMax, v); });
    nMax = niceMax(nMax * 1.2 || 1);

    drawChart(canvas, {
      times,
      leftMax: 100,
      rightMax: nMax,
      leftFmt: (v) => Math.round(v) + '%',
      rightFmt: (v) => (v >= 10 ? Math.round(v) : v.toFixed(1)) + 'M',
      series: [
        { values: cpuArr, color: '#34d399', width: 1.4, fill: 'rgba(52,211,153,.20)' },
        { values: memArr, color: '#60a5fa', width: 1.2 },
        { values: netArr, color: '#fbbf24', width: 1.2, dash: true, axis: 'right' },
      ],
    });
  }

  function queueRedraw() {
    if (S.redrawQueued) return;
    S.redrawQueued = true;
    requestAnimationFrame(() => {
      S.redrawQueued = false;
      drawProgressChart();
      drawResChart();
      if (HIST.data) drawHistoryCharts();   // 窗口尺寸变化时历史图也跟着重排
    });
  }

  /* ----------------------------- 总渲染 ----------------------------- */
  function render(st) {
    if (!st || st.error) {
      $('#foot-meta').textContent = (st && st.error) ? '采集异常：' + st.error : '—';
      return;
    }
    S.last = st;
    _stageOrder = stageOrder(st);       // 阶段顺序来自 progress.json，不硬编码
    pushSample(st);
    const rid = (st.experiment || {}).run_id || null;
    if (S.lastRunId === undefined) S.lastRunId = rid;
    else if (rid && rid !== S.lastRunId) {
      S.lastRunId = rid;
      setTimeout(() => fetchRuns(true), 1500);   // 新 run 出现：过一会儿重读历史
    }
    renderStatus(st);
    renderAlloc(st);
    renderProgress(st);
    renderOutput(st);
    renderResources(st);
    queueRedraw();

    const app = st.app || {};
    $('#foot-meta').textContent =
      'v' + (app.version || '?') + ' · ' + (app.hostname || '') + ':' + (app.run_dir || '') +
      ' · 采样 ' + (app.sample_interval || '?') + 's · SSE ' + (app.sse_interval || '?') + 's' +
      ' · 已运行 ' + dur(app.uptime_sec) + ' · 接收 ' + clock(st.ts);
  }

  /* ----------------------------- 连接层 ----------------------------- */
  function setConnected(v) {
    S.connected = v;
    const hp = $('#sb-health');
    if (!v && hp) { hp.className = 'pill err'; hp.textContent = '连接中断'; }
    const dot = $('#conn-dot');
    if (dot) dot.className = 'dot ' + (v ? 'live' : 'down');
  }

  function startPolling() {
    if (S.pollTimer) return;
    S.pollTimer = setInterval(async () => {
      if (S.connected) return;
      try {
        const r = await fetch('./api/state?lines=' + CFG.logLines, { cache: 'no-store' });
        if (!r.ok) throw new Error('HTTP ' + r.status);
        const st = await r.json();
        S.lastMsgAt = Date.now();
        if (!S.connected) setConnected(true);   // 轮询可用也算健康
        render(st);
      } catch (e) {
        S.lastMsgAt = S.lastMsgAt || 0;
        if (Date.now() - S.lastMsgAt > 8000) setConnected(false);
      }
    }, 3000);
  }

  function connectStream() {
    if (S.es) { try { S.es.close(); } catch (e) {} S.es = null; }
    try {
      const es = new EventSource('./api/stream?lines=' + CFG.logLines);
      S.es = es;
      es.onopen = () => { setConnected(true); };
      es.onmessage = (ev) => {
        let st;
        try { st = JSON.parse(ev.data); } catch (e) { return; }
        S.lastMsgAt = Date.now();
        if (!S.connected) setConnected(true);
        render(st);
      };
      es.onerror = () => {
        setConnected(false);
        startPolling();       // EventSource 自己会重连；轮询只是兜底
      };
    } catch (e) {
      setConnected(false);
      startPolling();
    }
  }

  async function pingHealth() {
    try {
      const r = await fetch('./api/health', { cache: 'no-store' });
      S.health = r.ok ? await r.json() : null;
    } catch (e) { S.health = null; }
  }

  /* ----------------------------- 事件绑定 ----------------------------- */
  function bind() {
    $('#stage-filter').addEventListener('click', (e) => {
      const btn = e.target.closest('.chip');
      if (!btn) return;
      const s = btn.getAttribute('data-stage');
      if (!s || s === CFG.stage) return;
      CFG.stage = s;
      if (S.last) { renderAlloc(S.last); }
    });

    const auto = $('#autoscroll');
    auto.checked = CFG.autoscroll;
    auto.addEventListener('change', () => { CFG.autoscroll = auto.checked; });

    $('#loglines').addEventListener('change', (e) => {
      CFG.logLines = parseInt(e.target.value, 10) || 200;
      S.logSig = '';              // 强制重绘
      connectStream();            // 重新订阅，让服务端只推 N 行
      if (S.last) renderOutput(S.last);
    });

    $('#logbottom').addEventListener('click', () => {
      const pre = $('#log');
      pre.scrollTop = pre.scrollHeight;
    });

    // 历史对比：algo 过滤
    $('#hist-filter').addEventListener('click', (e) => {
      const btn = e.target.closest('.chip');
      if (!btn) return;
      const a = btn.getAttribute('data-halgo');
      if (!a || a === HIST.algo) return;
      HIST.algo = a;
      HIST.hover = null;
      renderHistory();
    });

    // 历史对比：点表头排序（再点一次反向）
    $('#hist-thead').addEventListener('click', (e) => {
      const th = e.target.closest('th[data-sort]');
      if (!th) return;
      const key = th.getAttribute('data-sort');
      if (HIST.sort.key === key) HIST.sort.dir = -HIST.sort.dir;
      else HIST.sort = { key: key, dir: -1 };
      renderHistory();
    });

    bindHistHover('#chart-scatter', () => S.scatterHits, '#scatter-note', (r) =>
      (r.run_id || '—') + ' · ' + (r.algo || '?') + '/' + (r.mode || '?') +
      ' · ' + int(r.docs) + ' 篇 · recall ' + fmtRatio(r.pair_recall) +
      ' / precision ' + fmtRatio(r.pair_precision));
    bindHistHover('#chart-scale', () => S.scaleHits, '#scale-note', (r) =>
      (r.run_id || '—') + ' · ' + (r.algo || '?') + '/' + (r.mode || '?') +
      ' · ' + int(r.docs) + ' 篇 → ' + num(r.docs_per_sec, 1) + ' docs/s');

    let rt = null;
    window.addEventListener('resize', () => {
      clearTimeout(rt);
      rt = setTimeout(queueRedraw, 150);
    });

    document.addEventListener('visibilitychange', () => {
      if (!document.hidden) { queueRedraw(); if (!S.es) connectStream(); }
    });
  }

  /* ----------------------------- 启动 ----------------------------- */
  function boot() {
    bind();
    pingHealth();
    setInterval(pingHealth, 30000);
    connectStream();
    startPolling();               // 始终开着：SSE 断了会用它兜底
    fetchRuns(false);             // 历史对比（LanceDB 或 RAYTEST_RUNS_JSON）
    setInterval(() => fetchRuns(false), 15000);
    setInterval(() => {
      // SSE 静默超过 8s 视为断开
      if (S.connected && S.lastMsgAt && Date.now() - S.lastMsgAt > 8000) setConnected(false);
    }, 3000);
    // 首屏即使还没数据也画一次空图
    queueRedraw();
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
  else boot();

  // 暴露给调试 / 自动化检查
  window.__raytestDashboard = { state: S, config: CFG, redraw: queueRedraw };
})();
