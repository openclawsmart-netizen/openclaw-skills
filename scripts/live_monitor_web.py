#!/usr/bin/env python3
"""Web live monitor dashboard (中英對照) for openclaw-skills."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

BASE_DIR = Path(__file__).resolve().parent.parent
LOGS_DIR = BASE_DIR / "logs"
REPORTS_DIR = BASE_DIR / "reports"

DATA_DIR_CANDIDATES = [
    BASE_DIR / "data",
    BASE_DIR / "scripts" / "data",
    BASE_DIR / "scripts" / "../data",
    BASE_DIR / "../data",
]


def _resolve_data_dir() -> Path:
    for cand in DATA_DIR_CANDIDATES:
        p = cand.resolve()
        if not p.exists() or not p.is_dir():
            continue
        if (p / "trade_logs.json").exists() or (p / "trade_logs.csv").exists() or (p / "trade_snapshot.json").exists():
            return p
    return (BASE_DIR / "data").resolve()


DATA_DIR = _resolve_data_dir()

TRADE_LOG_JSON = DATA_DIR / "trade_logs.json"
CRON_LOG = LOGS_DIR / "cron_trade.log"
MANUAL_APPRENTICE_LOG = LOGS_DIR / "cron_apprentice_manual.log"

TRADE_ANALYST_CRON_TAG = "run-skill.sh trade-analyst"
TRADE_ANALYST_SAFE_CRON = f"*/30 * * * * cd {BASE_DIR} && ./run-skill.sh trade-analyst >> logs/cron_trade.log 2>&1"

REGISTRY_JSON = BASE_DIR / "registry.json"
RUN_SKILL = BASE_DIR / "run-skill.sh"
TRADE_ANALYST_LOCK = DATA_DIR / "trade_analyst.lock"

JOB_SCHEDULES: Dict[str, str] = {
    "trade-analyst": f"*/30 * * * * cd {BASE_DIR} && ./run-skill.sh trade-analyst >> logs/cron_trade.log 2>&1",
    "daily-brief": f"0 9 * * * cd {BASE_DIR} && ./run-skill.sh daily-brief >> logs/cron_daily_brief.log 2>&1",
    "report-generator": f"30 9 * * * cd {BASE_DIR} && ./run-skill.sh report-generator >> logs/cron_report_generator.log 2>&1",
}

JOB_LAST_RUN_HINTS: Dict[str, List[Path]] = {
    "trade-analyst": [DATA_DIR / "trade_logs.json", DATA_DIR / "trade_logs.csv", LOGS_DIR / "cron_trade.log"],
    "daily-brief": [LOGS_DIR / "cron_daily_brief.log"],
    "report-generator": [REPORTS_DIR, LOGS_DIR / "cron_report_generator.log"],
    "trade-health-check": [LOGS_DIR / "cron_trade_health.log", DATA_DIR / "trade_logs.json"],
    "monitor-dashboard": [LOGS_DIR / "cron_monitor_dashboard.log"],
    "monitor-dashboard-web": [LOGS_DIR / "cron_monitor_dashboard_web.log"],
}

HTML_PAGE = """<!doctype html>
<html lang="zh-Hant">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>監控儀表板（Monitor Dashboard）</title>
  <style>
    :root {
      --bg: #0b1220;
      --panel: #121a2b;
      --text: #e5e7eb;
      --muted: #9ca3af;
      --accent: #22d3ee;
      --ok: #22c55e;
      --warn: #f59e0b;
      --bad: #ef4444;
      --border: #243149;
      --hero-bg: #0f172a;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Inter, ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Noto Sans TC", Arial, sans-serif;
      background: var(--bg);
      color: var(--text);
    }
    header {
      padding: 16px 20px;
      border-bottom: 1px solid var(--border);
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
    }
    h1 { margin: 0; font-size: 20px; }
    #updatedAt { color: var(--muted); font-size: 13px; }
    .hero-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 12px;
      padding: 12px 12px 0 12px;
    }
    .hero {
      background: var(--hero-bg);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 14px;
      min-height: 130px;
    }
    .hero h2 {
      margin: 0;
      color: var(--accent);
      font-size: 14px;
    }
    .hero .big {
      margin-top: 10px;
      font-size: 30px;
      font-weight: 800;
      line-height: 1.1;
    }
    .hero .sub {
      margin-top: 6px;
      color: var(--muted);
      font-size: 12px;
    }
    .ok { color: var(--ok); }
    .warn { color: var(--warn); }
    .bad { color: var(--bad); }

    .action-bar {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      padding: 12px;
    }
    button {
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 10px 12px;
      font-size: 13px;
      color: var(--text);
      background: #1f2937;
      cursor: pointer;
    }
    button:hover { filter: brightness(1.1); }
    button.danger { background: #3b1111; border-color: #7f1d1d; }
    button.safe { background: #0f2b19; border-color: #166534; }
    .action-msg {
      width: 100%;
      font-size: 13px;
      color: var(--muted);
    }

    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      gap: 12px;
      padding: 0 12px 12px 12px;
    }
    .card {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 12px;
      min-height: 160px;
    }
    .card h2 {
      margin: 0 0 10px 0;
      font-size: 15px;
      color: var(--accent);
    }
    ul { margin: 0; padding-left: 18px; }
    li { margin: 4px 0; line-height: 1.35; }
    .kv { width: 100%; border-collapse: collapse; }
    .kv td {
      border-bottom: 1px solid var(--border);
      padding: 5px 0;
      vertical-align: top;
      font-size: 13px;
    }
    .k { color: var(--muted); width: 42%; }
    .v { word-break: break-word; }
    .missing { color: #fda4af; font-weight: 600; }
    .small { font-size: 12px; color: var(--muted); }
    .jobs-wrap {
      padding: 0 12px 12px 12px;
    }
    .jobs-table {
      width: 100%;
      border-collapse: collapse;
      font-size: 12px;
    }
    .jobs-table th, .jobs-table td {
      border: 1px solid var(--border);
      padding: 6px;
      vertical-align: top;
      text-align: left;
    }
    .jobs-table th {
      color: var(--accent);
      background: #0f172a;
    }
    .jobs-actions {
      display: flex;
      gap: 6px;
      flex-wrap: wrap;
    }
    .jobs-actions button {
      padding: 6px 8px;
      font-size: 12px;
      border-radius: 8px;
    }
  </style>
</head>
<body>
  <header>
    <h1>交易監控儀表板（Trading Monitor Dashboard）</h1>
    <div id="updatedAt">更新時間（Updated At）: -</div>
  </header>

  <section class="hero-grid">
    <article class="hero">
      <h2>目前動作（Current Action）</h2>
      <div id="heroAction" class="big">-</div>
      <div id="heroActionSub" class="sub">-</div>
    </article>
    <article class="hero">
      <h2>系統狀態（System Status）</h2>
      <div id="heroSystem" class="big">-</div>
      <div id="heroSystemSub" class="sub">-</div>
    </article>
    <article class="hero">
      <h2>下次更新預估（Next Update ETA）</h2>
      <div id="heroEta" class="big">-</div>
      <div id="heroEtaSub" class="sub">-</div>
    </article>
  </section>

  <section class="action-bar">
    <button class="danger" onclick="runAction('/api/actions/pause-trade-analyst')">暫停交易排程（Pause Trade Analyst）</button>
    <button class="safe" onclick="runAction('/api/actions/restore-trade-analyst')">恢復交易排程（Restore Trade Analyst */30）</button>
    <button onclick="runAction('/api/actions/run-apprentice')">手動跑 Apprentice（Run Apprentice）</button>
    <button onclick="runAction('/api/actions/export-latest-report')">匯出最新報告（Export Latest Report）</button>
    <div id="actionMsg" class="action-msg">-</div>
  </section>

  <section class="jobs-wrap">
    <section class="card">
      <h2>工作清單（Jobs Table）</h2>
      <div id="jobMsg" class="action-msg">-</div>
      <table class="jobs-table">
        <thead>
          <tr>
            <th>工作名稱<br/>Job Name</th>
            <th>說明<br/>Description</th>
            <th>指令<br/>Command</th>
            <th>類型<br/>Type</th>
            <th>狀態<br/>Status</th>
            <th>最後執行<br/>Last Run</th>
            <th>操作<br/>Actions</th>
          </tr>
        </thead>
        <tbody id="jobsTableBody"></tbody>
      </table>
    </section>
  </section>

  <main class="grid">
    <section class="card">
      <h2>最近任務（Recent Tasks）</h2>
      <div class="small">Cron</div>
      <ul id="recentCron"></ul>
      <div class="small">Reports</div>
      <ul id="recentReports"></ul>
      <div class="small">Trades</div>
      <ul id="recentTrades"></ul>
    </section>

    <section class="card">
      <h2>排程狀態（Scheduler）</h2>
      <ul id="scheduler"></ul>
    </section>

    <section class="card">
      <h2>健康摘要（Health Summary）</h2>
      <table class="kv" id="healthSummary"></table>
    </section>

    <section class="card">
      <h2>健康分（Health Score）</h2>
      <table class="kv" id="healthScore"></table>
      <div class="small">四維度（4 factors）</div>
      <ul id="healthFactors"></ul>
    </section>

    <section class="card">
      <h2>任務透明度（Task Transparency）</h2>
      <table class="kv" id="taskTransparency"></table>
    </section>

    <section class="card">
      <h2>交易摘要（Trade Snapshot）</h2>
      <table class="kv" id="tradeSnapshot"></table>
    </section>

    <section class="card">
      <h2>風控摘要（Risk Control）</h2>
      <table class="kv" id="risk"></table>
    </section>

    <section class="card">
      <h2>模式與路由（Mode & Routing）</h2>
      <table class="kv" id="modeRouting"></table>
    </section>
  </main>

  <script>
    const MISSING = 'MISSING';

    function classForLevel(level) {
      if (level === 'ok') return 'ok';
      if (level === 'warn') return 'warn';
      return 'bad';
    }

    function setList(id, items) {
      const ul = document.getElementById(id);
      ul.innerHTML = '';
      (items || []).forEach((it) => {
        const li = document.createElement('li');
        li.textContent = it;
        ul.appendChild(li);
      });
    }

    function setTable(id, obj) {
      const table = document.getElementById(id);
      table.innerHTML = '';
      Object.entries(obj || {}).forEach(([k, v]) => {
        const tr = document.createElement('tr');
        const tdK = document.createElement('td');
        const tdV = document.createElement('td');
        tdK.className = 'k';
        tdV.className = 'v';
        tdK.textContent = k;
        tdV.textContent = String(v ?? MISSING);
        if (String(v ?? MISSING) === MISSING) tdV.classList.add('missing');
        tr.appendChild(tdK);
        tr.appendChild(tdV);
        table.appendChild(tr);
      });
    }

    function setHealthFactors(factors) {
      const ul = document.getElementById('healthFactors');
      ul.innerHTML = '';
      Object.values(factors || {}).forEach((f) => {
        const li = document.createElement('li');
        li.textContent = `${f.zh || '-'} (${f.en || '-'})：-${f.penalty ?? 0} | ${f.detail || '-'}`;
        ul.appendChild(li);
      });
    }

    async function runAction(path) {
      const msg = document.getElementById('actionMsg');
      msg.textContent = '執行中（Running）...';
      try {
        const res = await fetch(path, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ source: 'dashboard' }),
        });
        const data = await res.json();
        msg.textContent = `${data.ok ? '✅' : '❌'} ${data.message || '-'}${data.path ? ' | ' + data.path : ''}`;
      } catch (err) {
        msg.textContent = `❌ 請求失敗（Request Failed）: ${err}`;
      }
      await refresh();
    }

    async function runJobAction(path, jobName) {
      const msg = document.getElementById('jobMsg');
      msg.textContent = `執行中（Running）... ${jobName}`;
      try {
        const res = await fetch(path, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ jobName }),
        });
        const data = await res.json();
        msg.textContent = `${data.ok ? '✅' : '❌'} ${data.message || '-'} (${jobName})`;
      } catch (err) {
        msg.textContent = `❌ 請求失敗（Request Failed）: ${err}`;
      }
      await refreshJobs();
    }

    function renderJobs(jobs) {
      const body = document.getElementById('jobsTableBody');
      body.innerHTML = '';
      (jobs || []).forEach((job) => {
        const tr = document.createElement('tr');
        const cols = [
          job.jobName,
          job.description,
          job.command,
          job.type,
          job.status,
          job.lastRun,
        ];
        cols.forEach((c) => {
          const td = document.createElement('td');
          td.textContent = String(c ?? MISSING);
          if (String(c ?? MISSING) === MISSING) td.classList.add('missing');
          tr.appendChild(td);
        });

        const tdAction = document.createElement('td');
        tdAction.className = 'jobs-actions';

        const runBtn = document.createElement('button');
        runBtn.textContent = 'Run Now';
        runBtn.onclick = () => runJobAction('/api/jobs/run', job.jobName);
        tdAction.appendChild(runBtn);

        const enBtn = document.createElement('button');
        enBtn.textContent = 'Enable';
        enBtn.className = 'safe';
        enBtn.onclick = () => runJobAction('/api/jobs/enable', job.jobName);
        tdAction.appendChild(enBtn);

        const disBtn = document.createElement('button');
        disBtn.textContent = 'Disable';
        disBtn.className = 'danger';
        disBtn.onclick = () => runJobAction('/api/jobs/disable', job.jobName);
        tdAction.appendChild(disBtn);

        tr.appendChild(tdAction);
        body.appendChild(tr);
      });
    }

    async function refreshJobs() {
      try {
        const res = await fetch('/api/jobs?_=' + Date.now(), { cache: 'no-store' });
        const data = await res.json();
        renderJobs(data.jobs || []);
      } catch (err) {
        document.getElementById('jobMsg').textContent = '❌ 讀取工作清單失敗';
      }
    }

    async function refresh() {
      try {
        const res = await fetch('/api/snapshot?_=' + Date.now(), { cache: 'no-store' });
        const data = await res.json();

        document.getElementById('updatedAt').textContent = '更新時間（Updated At）: ' + (data.generated_at || '-');

        const heroAction = document.getElementById('heroAction');
        heroAction.textContent = data?.hero?.current_action?.text || '-';
        heroAction.className = 'big ' + classForLevel(data?.hero?.current_action?.level || 'warn');
        document.getElementById('heroActionSub').textContent = data?.hero?.current_action?.detail || '-';

        const heroSystem = document.getElementById('heroSystem');
        heroSystem.textContent = data?.hero?.system_status?.text || '-';
        heroSystem.className = 'big ' + classForLevel(data?.hero?.system_status?.level || 'warn');
        document.getElementById('heroSystemSub').textContent = data?.hero?.system_status?.detail || '-';

        const heroEta = document.getElementById('heroEta');
        heroEta.textContent = data?.hero?.next_update_eta?.text || '-';
        heroEta.className = 'big ' + classForLevel(data?.hero?.next_update_eta?.level || 'warn');
        document.getElementById('heroEtaSub').textContent = data?.hero?.next_update_eta?.detail || '-';

        setList('recentCron', data?.recent_tasks?.cron || []);
        setList('recentReports', data?.recent_tasks?.reports || []);
        setList('recentTrades', data?.recent_tasks?.trades || []);
        setList('scheduler', data?.scheduler || []);

        setTable('healthSummary', data?.health_summary || {});
        setTable('healthScore', {
          '健康分 Health Score': data?.health_score ?? MISSING,
          '摘要 Summary': data?.health_summary_message ?? MISSING,
          '資料路徑 Data Source': data?.data_source_path ?? MISSING,
        });
        setHealthFactors(data?.health_factors || {});
        setTable('taskTransparency', {
          '發生什麼事 What happened': data?.what_happened ?? MISSING,
          '現在做什麼 What is the job': data?.whats_the_job ?? MISSING,
          '進度 Progressing': data?.progressing ?? MISSING,
          'AI 路由 AI routing': data?.ai_routing ?? MISSING,
        });
        setTable('tradeSnapshot', data?.trade_snapshot || {});
        setTable('risk', data?.risk || {});
        setTable('modeRouting', data?.mode_routing || {});
      } catch (err) {
        document.getElementById('updatedAt').textContent = '更新失敗（Fetch failed）';
      }
    }

    refresh();
    refreshJobs();
    setInterval(refresh, 3000);
    setInterval(refreshJobs, 5000);
  </script>
</body>
</html>
"""


def _safe_read_json(path: Path, default: Any) -> Any:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _tail_lines(path: Path, limit: int = 120) -> List[str]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        return [ln.rstrip("\n") for ln in lines[-limit:]]
    except Exception:
        return []


def _last_trade() -> Dict[str, Any]:
    logs = _safe_read_json(TRADE_LOG_JSON, [])
    if isinstance(logs, list) and logs:
        item = logs[-1]
        return item if isinstance(item, dict) else {}
    return {}


def _fmt_missing(v: Any) -> Any:
    if v is None or v == "":
        return "MISSING"
    return v


def _parse_log_ts(line: str) -> Optional[datetime]:
    if not line.startswith("[") or "]" not in line:
        return None
    raw = line[1 : line.index("]")]
    try:
        return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=datetime.now().astimezone().tzinfo)
    except Exception:
        return None


def _run_crontab(args: List[str], input_text: Optional[str] = None) -> Tuple[bool, str]:
    try:
        res = subprocess.run(
            ["crontab", *args],
            input=input_text,
            capture_output=True,
            text=True,
            check=False,
        )
        if res.returncode != 0:
            return False, (res.stderr or res.stdout or "crontab command failed").strip()
        return True, (res.stdout or "ok").strip()
    except Exception as exc:
        return False, str(exc)


def _get_crontab_raw() -> Tuple[bool, List[str], str]:
    ok, out = _run_crontab(["-l"])
    if not ok:
        # no crontab for user might appear as stderr text
        if "no crontab" in out.lower():
            return True, [], "no crontab"
        return False, [], out
    return True, out.splitlines(), "ok"


def _get_crontab_lines() -> List[str]:
    ok, lines, _ = _get_crontab_raw()
    if not ok:
        return ["MISSING"]
    clean = [ln.strip() for ln in lines if ln.strip() and not ln.strip().startswith("#")]
    keys = ("run-skill.sh", "trade", "batch", "daily", "system-status", "health", "report")
    filtered = [ln for ln in clean if any(k in ln for k in keys)]
    return filtered[:8] if filtered else ["MISSING"]


def _trade_analyst_interval_minutes(lines: List[str]) -> int:
    for ln in lines:
        if TRADE_ANALYST_CRON_TAG not in ln:
            continue
        m = re.match(r"^\s*\*/(\d+)\s+", ln)
        if m:
            try:
                return max(1, int(m.group(1)))
            except Exception:
                pass
    return 30


def _status_from_context(last_trade: Dict[str, Any], cron_tail: List[str]) -> Tuple[str, str, bool]:
    risk = last_trade.get("risk_control") if isinstance(last_trade.get("risk_control"), dict) else {}
    if risk.get("circuit_breaker_active") is True:
        return "Paused", "暫停", False

    latest_ts: Optional[datetime] = None
    for ln in reversed(cron_tail):
        ts = _parse_log_ts(ln)
        if ts:
            latest_ts = ts
            break

    if latest_ts:
        delta = datetime.now().astimezone() - latest_ts
        if delta.total_seconds() <= 1800:
            return "Running", "執行中", False
        if delta.total_seconds() > 7200:
            return "Stuck", "疑似卡住", True

    return "Idle", "閒置", False


def _latest_report_path() -> Optional[Path]:
    if not REPORTS_DIR.exists():
        return None
    files = [p for p in REPORTS_DIR.iterdir() if p.is_file()]
    if not files:
        return None
    return max(files, key=lambda p: p.stat().st_mtime)


def _collect_health(cron_tail: List[str]) -> Dict[str, Any]:
    success_kw = ("success", "completed", "done", "report generated", "exit code 0")
    error_kw = ("error", "failed", "traceback", "exception")

    last_success = None
    for ln in reversed(cron_tail):
        low = ln.lower()
        if any(k in low for k in success_kw):
            ts = _parse_log_ts(ln)
            if ts:
                last_success = ts.strftime("%Y-%m-%d %H:%M:%S")
            else:
                last_success = ln
            break

    recent_error = None
    for ln in reversed(cron_tail):
        low = ln.lower()
        if any(k in low for k in error_kw):
            recent_error = ln
            break

    return {
        "last_success_task_time": _fmt_missing(last_success),
        "recent_error_summary": _fmt_missing(recent_error),
    }


def _count_recent_matches(lines: List[str], keywords: Tuple[str, ...], lookback: int = 120) -> int:
    count = 0
    for ln in lines[-lookback:]:
        low = ln.lower()
        if any(k in low for k in keywords):
            count += 1
    return count


def _human_delta(delta: timedelta) -> str:
    mins = int(max(0, delta.total_seconds()) // 60)
    if mins < 60:
        return f"{mins} 分鐘"
    return f"{mins // 60} 小時 {mins % 60} 分鐘"


def _compute_alerts(last_trade: Dict[str, Any], status_en: str) -> Tuple[str, str, str, List[str]]:
    risk = last_trade.get("risk_control") if isinstance(last_trade.get("risk_control"), dict) else {}
    tri = last_trade.get("tri_brain_status") if isinstance(last_trade.get("tri_brain_status"), dict) else {}

    alerts: List[str] = []
    level = "ok"

    if risk.get("circuit_breaker_active") is True:
        alerts.append("Circuit breaker active")
        level = "bad"

    losing_streak = risk.get("losing_streak")
    try:
        if losing_streak is not None and int(losing_streak) >= 3:
            alerts.append("連虧 >= 3")
            level = "bad"
    except Exception:
        pass

    cooldown = risk.get("gemini_cooldown") if isinstance(risk.get("gemini_cooldown"), dict) else {}
    if cooldown.get("active") is True:
        alerts.append("Gemini cooldown active")
        if level != "bad":
            level = "warn"

    gemini = tri.get("gemini") if isinstance(tri.get("gemini"), dict) else {}
    groq = tri.get("groq") if isinstance(tri.get("groq"), dict) else {}
    openai = tri.get("openai") if isinstance(tri.get("openai"), dict) else {}

    all_fallback = all((x.get("status") in ("fallback", "failed", "unavailable") for x in (gemini, groq, openai) if isinstance(x, dict)))
    if all_fallback and any((gemini, groq, openai)):
        alerts.append("fallback all")
        level = "bad"

    if status_en in ("Idle", "Stuck") and level == "ok":
        level = "warn"

    if not alerts:
        return level, "正常（Normal）", "系統運作正常（System healthy）", alerts

    return level, "警示（Alert）" if level != "bad" else "危險（Danger）", "；".join(alerts), alerts


def _build_health_and_transparency(
    last_trade: Dict[str, Any],
    cron_tail: List[str],
    last_seen: Optional[datetime],
    interval: int,
    en_status: str,
    is_stuck: bool,
) -> Tuple[int, Dict[str, Any], str, Dict[str, str]]:
    now = datetime.now().astimezone()
    tri = last_trade.get("tri_brain_status") if isinstance(last_trade.get("tri_brain_status"), dict) else {}
    risk = last_trade.get("risk_control") if isinstance(last_trade.get("risk_control"), dict) else {}

    gemini = tri.get("gemini") if isinstance(tri.get("gemini"), dict) else {}
    groq = tri.get("groq") if isinstance(tri.get("groq"), dict) else {}
    openai = tri.get("openai") if isinstance(tri.get("openai"), dict) else {}

    recent_429 = _count_recent_matches(cron_tail, ("429", "quota", "rate limit", "http_retryable"), lookback=160)
    recent_conn_err = _count_recent_matches(cron_tail, ("connection", "timeout", "network", "dns", "unreachable"), lookback=160)
    anomaly_penalty = min(30, recent_429 * 6 + recent_conn_err * 5)

    lock_file = DATA_DIR / "trade_analyst.lock"
    lock_age_mins: Optional[int] = None
    if lock_file.exists():
        try:
            lock_age = now - datetime.fromtimestamp(lock_file.stat().st_mtime, tz=timezone.utc).astimezone()
            lock_age_mins = int(lock_age.total_seconds() // 60)
        except Exception:
            lock_age_mins = None

    backlog_penalty = 0
    backlog_reasons: List[str] = []
    if is_stuck or en_status == "Stuck":
        backlog_penalty += 18
        backlog_reasons.append("排程疑似卡住")
    if lock_age_mins is not None and lock_age_mins > max(45, interval * 2):
        backlog_penalty += min(12, lock_age_mins // 20)
        backlog_reasons.append(f"lock 檔存在 {lock_age_mins} 分鐘")
    cron_fail = _count_recent_matches(cron_tail, ("failed", "traceback", "exception", "command not found"), lookback=120)
    if cron_fail > 0:
        backlog_penalty += min(10, cron_fail * 3)
        backlog_reasons.append(f"近期可疑失敗 {cron_fail} 次")
    backlog_penalty = min(30, backlog_penalty)

    stale_penalty = 0
    stale_msg = "K線更新正常"
    trade_ts: Optional[datetime] = None
    date_raw = last_trade.get("date")
    if isinstance(date_raw, str) and date_raw.strip():
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z"):
            try:
                trade_ts = datetime.strptime(date_raw, fmt)
                if trade_ts.tzinfo is None:
                    trade_ts = trade_ts.replace(tzinfo=now.tzinfo)
                break
            except Exception:
                continue
    if trade_ts and trade_ts.tzinfo is not None:
        stale_age = now - trade_ts.astimezone(now.tzinfo)
        stale_mins = int(stale_age.total_seconds() // 60)
        if stale_mins > 90:
            stale_penalty = min(25, 8 + stale_mins // 30)
            stale_msg = f"K線 {stale_mins} 分鐘未更新"
        else:
            stale_msg = f"最近更新 {_human_delta(stale_age)} 前"
    else:
        stale_penalty = 12
        stale_msg = "無法判定 K線時間，套用保守扣分"

    budget_proxy = (
        (1 if gemini.get("status") in ("fallback", "failed", "unavailable") else 0)
        + (1 if groq.get("status") in ("fallback", "failed", "unavailable") else 0)
        + (1 if openai.get("status") in ("fallback", "failed", "unavailable") else 0)
    )
    cooldown_active = bool((risk.get("gemini_cooldown") or {}).get("active")) if isinstance(risk.get("gemini_cooldown"), dict) else False
    budget_penalty = min(25, budget_proxy * 5 + min(10, recent_429 * 2) + (6 if cooldown_active else 0))

    health_score = max(0, 100 - anomaly_penalty - backlog_penalty - stale_penalty - budget_penalty)

    anomaly_bad = (recent_429 + recent_conn_err) > 0
    anomaly_text = "AI 休息中，數學模型代班" if anomaly_bad else "AI 連線穩定"

    health_factors = {
        "anomaly_stagnation": {
            "zh": "異常停滯",
            "en": "Anomaly/Stagnation",
            "penalty": anomaly_penalty,
            "status": "warn" if anomaly_bad else "ok",
            "detail": f"429={recent_429}, conn_error={recent_conn_err}; {anomaly_text}",
        },
        "task_backlog": {
            "zh": "任務積壓",
            "en": "Task Backlog",
            "penalty": backlog_penalty,
            "status": "warn" if backlog_penalty > 0 else "ok",
            "detail": "；".join(backlog_reasons) if backlog_reasons else "排程節奏正常",
        },
        "stale_execution": {
            "zh": "無效執行",
            "en": "Stale Execution",
            "penalty": stale_penalty,
            "status": "warn" if stale_penalty > 0 else "ok",
            "detail": stale_msg,
        },
        "budget_risk_proxy": {
            "zh": "預算風險",
            "en": "Budget Risk (Proxy)",
            "penalty": budget_penalty,
            "status": "warn" if budget_penalty > 0 else "ok",
            "detail": f"proxy=fallback({budget_proxy}) cooldown={cooldown_active} 429={recent_429}；無官方額度 API，採代理指標估算",
        },
    }

    summary_bits = []
    if anomaly_bad:
        summary_bits.append("AI 休息中，數學模型代班")
    if backlog_penalty > 0:
        summary_bits.append("排程有積壓風險")
    if stale_penalty > 0:
        summary_bits.append("K線更新延遲")
    if budget_penalty > 0:
        summary_bits.append("額度壓力偏高（代理）")
    health_summary_message = "；".join(summary_bits) if summary_bits else "系統健康，持續監控中（Healthy and monitoring）"

    now_ref = last_seen or now
    eta = now_ref + timedelta(minutes=interval)
    countdown = eta - now
    if countdown.total_seconds() < 0:
        countdown_txt = f"已超時 {int(abs(countdown.total_seconds()) // 60)} 分鐘"
    else:
        countdown_txt = _human_delta(countdown)

    connectivity = "穩定" if (recent_conn_err == 0 and recent_429 == 0) else f"波動（429={recent_429}, conn={recent_conn_err}）"
    major_event = " / ".join([ln for ln in cron_tail[-20:] if "gemini_" in ln or "error" in ln.lower()][-1:]) or "近期無重大異常"
    major_event = major_event[:220]

    routing = "Fallback/數學模型"
    for name, node in (("Gemini", gemini), ("Groq", groq), ("OpenAI", openai)):
        if isinstance(node, dict) and node.get("status") not in ("fallback", "failed", "unavailable", None, ""):
            routing = name
            break

    transparency = {
        "what_happened": f"連線品質：{connectivity}；最後重大事件：{major_event}",
        "whats_the_job": "標的：YM=F；任務：15m K線 + EMA/RSI/MACD 分析、風控與交易計畫輸出",
        "progressing": f"下次看盤倒數：{countdown_txt}；排程間隔：{interval} 分鐘；執行狀態：{en_status}",
        "ai_routing": f"目前決策路由：{routing}（Gemini/Groq/OpenAI + fallback）",
    }

    return health_score, health_factors, health_summary_message, transparency


def collect_snapshot() -> Dict[str, Any]:
    last_trade = _last_trade()
    cron_tail = _tail_lines(CRON_LOG, 200)
    scheduler = _get_crontab_lines()

    recent_log_events = [ln for ln in cron_tail if "[run]" in ln or "gemini_" in ln or "run_skipped" in ln]
    recent_log_events = recent_log_events[-5:] or ["MISSING"]

    report_items: List[str] = []
    if REPORTS_DIR.exists():
        files = sorted([p for p in REPORTS_DIR.iterdir() if p.is_file()], key=lambda p: p.stat().st_mtime, reverse=True)[:3]
        for p in files:
            ts = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc).astimezone().strftime("%m-%d %H:%M")
            report_items.append(f"{p.name} ({ts})")
    if not report_items:
        report_items = ["MISSING"]

    trade_items: List[str] = []
    all_logs = _safe_read_json(TRADE_LOG_JSON, [])
    if isinstance(all_logs, list):
        for item in all_logs[-5:]:
            if not isinstance(item, dict):
                continue
            date_v = item.get('date')
            entry_v = item.get('entry_price')
            exp_v = item.get('expected_profit_points')
            if date_v is None and entry_v is None and exp_v is None:
                continue
            trade_items.append(
                f"{date_v or '-'} | entry={entry_v if entry_v is not None else '-'} | exp={exp_v if exp_v is not None else '-'}"
            )
    if not trade_items:
        trade_items = ["MISSING"]

    en_status, zh_status, is_stuck = _status_from_context(last_trade, cron_tail)

    risk = last_trade.get("risk_control") if isinstance(last_trade.get("risk_control"), dict) else {}
    strategy_mode = last_trade.get("strategy_mode") if isinstance(last_trade.get("strategy_mode"), dict) else {}
    tri = last_trade.get("tri_brain_status") if isinstance(last_trade.get("tri_brain_status"), dict) else {}

    gemini = tri.get("gemini") if isinstance(tri.get("gemini"), dict) else {}
    groq = tri.get("groq") if isinstance(tri.get("groq"), dict) else {}
    openai = tri.get("openai") if isinstance(tri.get("openai"), dict) else {}

    crontab_ok, crontab_lines, _ = _get_crontab_raw()
    interval = _trade_analyst_interval_minutes(crontab_lines if crontab_ok else [])

    last_seen = None
    for ln in reversed(cron_tail):
        ts = _parse_log_ts(ln)
        if ts:
            last_seen = ts
            break

    if last_seen:
        eta = last_seen + timedelta(minutes=interval)
        eta_text = eta.strftime("%H:%M:%S")
        eta_detail = f"約每 {interval} 分鐘（Every {interval} min）"
        eta_level = "warn" if is_stuck else "ok"
    else:
        eta_text = "MISSING"
        eta_detail = "無法估計（No data）"
        eta_level = "warn"

    alert_level, action_text, action_detail, alerts = _compute_alerts(last_trade, en_status)

    health = _collect_health(cron_tail)
    health_score, health_factors, health_summary_message, transparency = _build_health_and_transparency(
        last_trade=last_trade,
        cron_tail=cron_tail,
        last_seen=last_seen,
        interval=interval,
        en_status=en_status,
        is_stuck=is_stuck,
    )

    return {
        "generated_at": datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z"),
        "data_source_path": str(DATA_DIR),
        "status": {"en": en_status, "zh": zh_status},
        "health_score": health_score,
        "health_factors": health_factors,
        "health_summary_message": health_summary_message,
        "what_happened": transparency["what_happened"],
        "whats_the_job": transparency["whats_the_job"],
        "progressing": transparency["progressing"],
        "ai_routing": transparency["ai_routing"],
        "hero": {
            "current_action": {"text": action_text, "detail": action_detail, "level": alert_level},
            "system_status": {
                "text": f"{zh_status}（{en_status}）",
                "detail": "系統疑似卡住（Possibly stuck）" if is_stuck else "系統更新中（System active/idle）",
                "level": "bad" if is_stuck else ("ok" if en_status == "Running" else "warn"),
            },
            "next_update_eta": {"text": eta_text, "detail": eta_detail, "level": eta_level},
        },
        "recent_tasks": {
            "cron": recent_log_events,
            "reports": report_items,
            "trades": trade_items,
        },
        "scheduler": scheduler,
        "health_summary": {
            **health,
            "health_score（健康分）": health_score,
            "health_summary_message（摘要）": health_summary_message,
        },
        "alerts": alerts,
        "trade_snapshot": {
            "date": _fmt_missing(last_trade.get("date")),
            "active_contract": _fmt_missing(last_trade.get("active_contract")),
            "entry_price": _fmt_missing(last_trade.get("entry_price")),
            "take_profit_price": _fmt_missing(last_trade.get("take_profit_price")),
            "stop_loss_price": _fmt_missing(last_trade.get("stop_loss_price")),
            "expected_profit_points": _fmt_missing(last_trade.get("expected_profit_points")),
            "prior_trade_status": _fmt_missing((last_trade.get("prior_trade_status") or {}).get("status") if isinstance(last_trade.get("prior_trade_status"), dict) else None),
        },
        "risk": {
            "circuit_breaker": _fmt_missing(risk.get("circuit_breaker_active")),
            "cooldown": _fmt_missing(((risk.get("gemini_cooldown") or {}).get("active")) if isinstance(risk.get("gemini_cooldown"), dict) else risk.get("cooldown")),
            "cooldown_until": _fmt_missing(((risk.get("gemini_cooldown") or {}).get("until")) if isinstance(risk.get("gemini_cooldown"), dict) else None),
            "losing_streak": _fmt_missing(risk.get("losing_streak")),
            "daily_pnl": _fmt_missing(risk.get("daily_pnl")),
        },
        "mode_routing": {
            "strategy_mode": _fmt_missing(strategy_mode.get("mode")),
            "run_mode": _fmt_missing(strategy_mode.get("run_mode")),
            "gemini_status": _fmt_missing(gemini.get("status")),
            "gemini_fallback_reason": _fmt_missing(gemini.get("reason")),
            "groq_status": _fmt_missing(groq.get("status")),
            "openai_status": _fmt_missing(openai.get("status")),
        },
    }


def pause_trade_analyst_cron() -> Tuple[bool, str]:
    ok, lines, msg = _get_crontab_raw()
    if not ok:
        return False, f"讀取 crontab 失敗（read failed）: {msg}"

    changed = False
    out: List[str] = []
    for ln in lines:
        stripped = ln.lstrip()
        if TRADE_ANALYST_CRON_TAG in stripped and not stripped.startswith("#"):
            out.append(f"# PAUSED_BY_LIVE_MONITOR {ln}")
            changed = True
        else:
            out.append(ln)

    if not changed:
        return True, "沒有可暫停的 trade-analyst 排程（no active entry found）"

    ok_set, msg_set = _run_crontab(["-"], "\n".join(out) + "\n")
    if not ok_set:
        return False, f"寫入 crontab 失敗（write failed）: {msg_set}"
    return True, "已暫停 trade-analyst 排程（paused）"


def restore_trade_analyst_cron() -> Tuple[bool, str]:
    ok, lines, msg = _get_crontab_raw()
    if not ok:
        return False, f"讀取 crontab 失敗（read failed）: {msg}"

    out: List[str] = []
    inserted = False
    for ln in lines:
        if TRADE_ANALYST_CRON_TAG in ln:
            if not inserted:
                out.append(TRADE_ANALYST_SAFE_CRON)
                inserted = True
            continue
        out.append(ln)

    if not inserted:
        out.append(TRADE_ANALYST_SAFE_CRON)

    ok_set, msg_set = _run_crontab(["-"], "\n".join(out).rstrip() + "\n")
    if not ok_set:
        return False, f"寫入 crontab 失敗（write failed）: {msg_set}"
    return True, "已恢復 trade-analyst 排程（restored at */30）"


def run_apprentice_once() -> Tuple[bool, str]:
    try:
        MANUAL_APPRENTICE_LOG.parent.mkdir(parents=True, exist_ok=True)
        logf = MANUAL_APPRENTICE_LOG.open("a", encoding="utf-8")
        subprocess.Popen(
            ["bash", "-lc", f"cd {BASE_DIR} && ./run-skill.sh trade-apprentice"],
            stdout=logf,
            stderr=logf,
        )
        return True, f"已觸發 apprentice（triggered）; log={MANUAL_APPRENTICE_LOG}"
    except Exception as exc:
        return False, f"觸發失敗（trigger failed）: {exc}"


def export_latest_report() -> Tuple[bool, str, Optional[str]]:
    p = _latest_report_path()
    if not p:
        return False, "找不到 reports 檔案（no report found）", None
    return True, "已找到最新報告（latest report found）", str(p)


def _load_registry_jobs() -> Dict[str, Dict[str, str]]:
    zh_desc = {
        "trade-analyst": "交易分析主程式（15m 技術指標 + 風控 + 訊息）",
        "daily-brief": "每日簡報（新聞摘要 + Telegram 推送）",
        "report-generator": "報表產生器（交易紀錄轉 Excel 並推送）",
        "trade-health-check": "交易健康檢查（檢查最新交易狀態）",
        "monitor-dashboard": "終端機監控儀表板",
        "monitor-dashboard-web": "Web 監控儀表板",
    }
    data = _safe_read_json(REGISTRY_JSON, {})
    if not isinstance(data, dict):
        return {}
    out: Dict[str, Dict[str, str]] = {}
    for job_name, meta in data.items():
        if not isinstance(meta, dict):
            continue
        description = str(meta.get("description") or "MISSING")
        if not re.search(r"[\u4e00-\u9fff]", description) and job_name in zh_desc:
            description = f"{zh_desc[job_name]} / {description}"
        out[str(job_name)] = {
            "description": description,
            "command": str(meta.get("command") or f"./run-skill.sh {job_name}"),
        }
    return out


def _parse_cron_job_states(lines: List[str]) -> Dict[str, str]:
    states: Dict[str, str] = {}
    for raw in lines:
        stripped = raw.strip()
        if not stripped:
            continue
        is_comment = stripped.startswith("#")
        target = stripped[1:].strip() if is_comment else stripped
        if "run-skill.sh" not in target:
            continue
        m = re.search(r"run-skill\.sh\s+([a-zA-Z0-9_-]+)", target)
        if not m:
            continue
        job = m.group(1)
        states[job] = "disabled" if is_comment else "enabled"
    return states


def _guess_last_run(job_name: str) -> str:
    candidates = JOB_LAST_RUN_HINTS.get(job_name, [])
    if not candidates:
        candidates = [LOGS_DIR / f"cron_{job_name.replace('-', '_')}.log"]
    latest: Optional[float] = None
    for p in candidates:
        try:
            rp = p.resolve()
            if rp.is_dir():
                files = [x for x in rp.iterdir() if x.is_file()]
                if not files:
                    continue
                mt = max(x.stat().st_mtime for x in files)
            elif rp.exists():
                mt = rp.stat().st_mtime
            else:
                continue
            latest = mt if latest is None else max(latest, mt)
        except Exception:
            continue
    if latest is None:
        return "MISSING"
    return datetime.fromtimestamp(latest, tz=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def _is_job_running(job_name: str) -> bool:
    if job_name == "trade-analyst" and TRADE_ANALYST_LOCK.exists():
        return True
    try:
        res = subprocess.run(
            ["bash", "-lc", f"pgrep -af 'run-skill\\.sh {job_name}|python3 .*{job_name.replace('-', '_')}'"],
            capture_output=True,
            text=True,
            check=False,
        )
        return res.returncode == 0 and bool((res.stdout or "").strip())
    except Exception:
        return False


def get_jobs_table() -> List[Dict[str, str]]:
    registry = _load_registry_jobs()
    ok, lines, _ = _get_crontab_raw()
    cron_states = _parse_cron_job_states(lines if ok else [])

    jobs: List[Dict[str, str]] = []
    for job_name, meta in sorted(registry.items()):
        has_schedule = job_name in JOB_SCHEDULES or job_name in cron_states
        jtype = "cron" if has_schedule else "manual"
        base_status = cron_states.get(job_name, "disabled" if has_schedule else "idle")
        if _is_job_running(job_name):
            status = "running"
        elif jtype == "cron":
            status = base_status
        else:
            status = "idle"

        jobs.append(
            {
                "jobName": job_name,
                "description": meta.get("description") or "MISSING",
                "command": meta.get("command") or f"./run-skill.sh {job_name}",
                "type": jtype,
                "status": status,
                "lastRun": _guess_last_run(job_name),
            }
        )
    return jobs


def run_job_now(job_name: str) -> Tuple[bool, str]:
    registry = _load_registry_jobs()
    if job_name not in registry:
        return False, f"找不到工作（job not found）: {job_name}"
    if not RUN_SKILL.exists():
        return False, f"找不到 run-skill.sh: {RUN_SKILL}"
    try:
        log_path = LOGS_DIR / f"manual_{job_name.replace('-', '_')}.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        logf = log_path.open("a", encoding="utf-8")
        subprocess.Popen(
            ["bash", "-lc", f"cd {BASE_DIR} && ./run-skill.sh {job_name}"],
            stdout=logf,
            stderr=logf,
        )
        return True, f"已觸發（triggered）{job_name}，log={log_path}"
    except Exception as exc:
        return False, f"執行失敗（run failed）: {exc}"


def _toggle_job_schedule(job_name: str, enable: bool) -> Tuple[bool, str]:
    if job_name not in JOB_SCHEDULES:
        return False, f"not scheduled: {job_name}"

    ok, lines, msg = _get_crontab_raw()
    if not ok:
        return False, f"讀取 crontab 失敗（read failed）: {msg}"

    schedule_line = JOB_SCHEDULES[job_name]
    out: List[str] = []
    found = False
    changed = False
    for ln in lines:
        candidate = ln.strip().lstrip("#").strip()
        if f"run-skill.sh {job_name}" in candidate:
            found = True
            if enable:
                out.append(schedule_line)
                if ln.strip() != schedule_line:
                    changed = True
            else:
                if not ln.strip().startswith("#"):
                    out.append(f"# {candidate}")
                    changed = True
                else:
                    out.append(ln)
            continue
        out.append(ln)

    if enable and not found:
        out.append(schedule_line)
        changed = True

    if not changed:
        return True, f"{job_name} 無需變更（no change）"

    ok_set, msg_set = _run_crontab(["-"], "\n".join(out).rstrip() + "\n")
    if not ok_set:
        return False, f"寫入 crontab 失敗（write failed）: {msg_set}"
    action = "enabled" if enable else "disabled"
    return True, f"{job_name} 已{action}"


class Handler(BaseHTTPRequestHandler):
    server_version = "LiveMonitorWeb/2.0"

    def _read_json_body(self) -> Dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except Exception:
            length = 0
        if length <= 0:
            return {}
        try:
            raw = self.rfile.read(length)
            data = json.loads(raw.decode("utf-8"))
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _send_json(self, payload: Dict[str, Any], status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, html: str, status: int = 200) -> None:
        body = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _forbid_non_localhost(self) -> bool:
        if self.client_address[0] != "127.0.0.1":
            self._send_json(
                {
                    "ok": False,
                    "message": "拒絕：僅允許 127.0.0.1 呼叫變更操作（only localhost 127.0.0.1 allowed）",
                    "client": self.client_address[0],
                },
                status=403,
            )
            return True
        return False

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path == "/":
            self._send_html(HTML_PAGE)
            return
        if path == "/api/snapshot":
            self._send_json(collect_snapshot())
            return
        if path == "/api/jobs":
            self._send_json({"ok": True, "jobs": get_jobs_table()})
            return
        if path == "/healthz":
            self._send_json({"ok": True})
            return
        self._send_json({"error": "not found"}, status=404)

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path

        if (path.startswith("/api/actions/") or path.startswith("/api/jobs/")) and self._forbid_non_localhost():
            return

        if path == "/api/actions/pause-trade-analyst":
            ok, msg = pause_trade_analyst_cron()
            self._send_json({"ok": ok, "message": msg}, status=200 if ok else 500)
            return

        if path == "/api/actions/restore-trade-analyst":
            ok, msg = restore_trade_analyst_cron()
            self._send_json({"ok": ok, "message": msg}, status=200 if ok else 500)
            return

        if path == "/api/actions/run-apprentice":
            ok, msg = run_apprentice_once()
            self._send_json({"ok": ok, "message": msg}, status=200 if ok else 500)
            return

        if path == "/api/actions/export-latest-report":
            ok, msg, report_path = export_latest_report()
            payload: Dict[str, Any] = {"ok": ok, "message": msg}
            if report_path:
                payload["path"] = report_path
            self._send_json(payload, status=200 if ok else 404)
            return

        if path == "/api/jobs/run":
            body = self._read_json_body()
            job_name = str(body.get("jobName") or "").strip()
            if not job_name:
                self._send_json({"ok": False, "message": "缺少 jobName"}, status=400)
                return
            ok, msg = run_job_now(job_name)
            self._send_json({"ok": ok, "message": msg}, status=200 if ok else 400)
            return

        if path == "/api/jobs/enable":
            body = self._read_json_body()
            job_name = str(body.get("jobName") or "").strip()
            if not job_name:
                self._send_json({"ok": False, "message": "缺少 jobName"}, status=400)
                return
            ok, msg = _toggle_job_schedule(job_name, enable=True)
            self._send_json({"ok": ok, "message": msg}, status=200 if ok else 400)
            return

        if path == "/api/jobs/disable":
            body = self._read_json_body()
            job_name = str(body.get("jobName") or "").strip()
            if not job_name:
                self._send_json({"ok": False, "message": "缺少 jobName"}, status=400)
                return
            ok, msg = _toggle_job_schedule(job_name, enable=False)
            self._send_json({"ok": ok, "message": msg}, status=200 if ok else 400)
            return

        self._send_json({"ok": False, "error": "not found"}, status=404)

    def log_message(self, fmt: str, *args: Any) -> None:
        return


def main() -> int:
    parser = argparse.ArgumentParser(description="中英對照網頁監控儀表板 / Bilingual web live monitor dashboard")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8787, help="Bind port (default: 8787)")
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"[live-monitor-web] serving http://{args.host}:{args.port}")
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
