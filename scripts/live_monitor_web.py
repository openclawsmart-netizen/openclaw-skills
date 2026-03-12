#!/usr/bin/env python3
"""Web live monitor dashboard (中英對照) for openclaw-skills."""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
REPORTS_DIR = BASE_DIR / "reports"

TRADE_LOG_JSON = DATA_DIR / "trade_logs.json"
CRON_LOG = LOGS_DIR / "cron_trade.log"

HTML_PAGE = """<!doctype html>
<html lang="zh-Hant">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>監控儀表板 / Monitor Dashboard</title>
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
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      gap: 12px;
      padding: 12px;
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
    .status {
      font-size: 24px;
      font-weight: 700;
      margin-top: 8px;
    }
    .ok { color: var(--ok); }
    .warn { color: var(--warn); }
    .bad { color: var(--bad); }
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
  </style>
</head>
<body>
  <header>
    <h1>網頁儀表板 / Web Monitor Dashboard</h1>
    <div id="updatedAt">Updated At: -</div>
  </header>

  <main class="grid">
    <section class="card">
      <h2>目前狀態 / Current Status</h2>
      <div id="statusText" class="status">-</div>
    </section>

    <section class="card">
      <h2>最近任務 / Recent Tasks</h2>
      <div class="small">Cron</div>
      <ul id="recentCron"></ul>
      <div class="small">Reports</div>
      <ul id="recentReports"></ul>
      <div class="small">Trades</div>
      <ul id="recentTrades"></ul>
    </section>

    <section class="card">
      <h2>排程狀態 / Scheduler</h2>
      <ul id="scheduler"></ul>
    </section>

    <section class="card">
      <h2>交易摘要 / Trade Snapshot</h2>
      <table class="kv" id="tradeSnapshot"></table>
    </section>

    <section class="card">
      <h2>風控摘要 / Risk Control</h2>
      <table class="kv" id="risk"></table>
    </section>

    <section class="card">
      <h2>模式與路由 / Mode & Routing</h2>
      <table class="kv" id="modeRouting"></table>
    </section>
  </main>

  <script>
    const MISSING = 'MISSING';

    function classForStatus(en) {
      if (en === 'Running') return 'ok';
      if (en === 'Paused') return 'bad';
      return 'warn';
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

    async function refresh() {
      try {
        const res = await fetch('/api/snapshot?_=' + Date.now(), { cache: 'no-store' });
        const data = await res.json();

        document.getElementById('updatedAt').textContent = 'Updated At: ' + (data.generated_at || '-');
        const status = document.getElementById('statusText');
        status.textContent = `${data?.status?.zh || '-'} / ${data?.status?.en || '-'}`;
        status.className = 'status ' + classForStatus(data?.status?.en);

        setList('recentCron', data?.recent_tasks?.cron || []);
        setList('recentReports', data?.recent_tasks?.reports || []);
        setList('recentTrades', data?.recent_tasks?.trades || []);
        setList('scheduler', data?.scheduler || []);

        setTable('tradeSnapshot', data?.trade_snapshot || {});
        setTable('risk', data?.risk || {});
        setTable('modeRouting', data?.mode_routing || {});
      } catch (err) {
        document.getElementById('updatedAt').textContent = 'Updated At: fetch failed';
      }
    }

    refresh();
    setInterval(refresh, 3000);
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


def _get_crontab_lines() -> List[str]:
    try:
        res = subprocess.run(["crontab", "-l"], capture_output=True, text=True, check=False)
        if res.returncode != 0:
            return ["MISSING"]
        lines = [ln.strip() for ln in res.stdout.splitlines() if ln.strip() and not ln.strip().startswith("#")]
        keys = ("run-skill.sh", "trade", "batch", "daily", "system-status", "health", "report")
        filtered = [ln for ln in lines if any(k in ln for k in keys)]
        return filtered[:8] if filtered else ["MISSING"]
    except Exception:
        return ["MISSING"]


def _status_from_context(last_trade: Dict[str, Any]) -> Tuple[str, str]:
    risk = last_trade.get("risk_control") if isinstance(last_trade.get("risk_control"), dict) else {}
    if risk.get("circuit_breaker_active") is True:
        return "Paused", "暫停"

    recent = _tail_lines(CRON_LOG, 200)
    latest_ts: Optional[datetime] = None
    for ln in reversed(recent):
        if ln.startswith("[") and "]" in ln:
            t = ln[1 : ln.index("]")]
            try:
                latest_ts = datetime.strptime(t, "%Y-%m-%d %H:%M:%S").replace(tzinfo=datetime.now().astimezone().tzinfo)
                break
            except Exception:
                continue
    if latest_ts:
        delta = datetime.now().astimezone() - latest_ts
        if delta.total_seconds() <= 1800:
            return "Running", "執行中"

    return "Idle", "閒置"


def collect_snapshot() -> Dict[str, Any]:
    last_trade = _last_trade()
    cron_tail = _tail_lines(CRON_LOG, 160)
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
        for item in all_logs[-3:]:
            if not isinstance(item, dict):
                continue
            trade_items.append(
                f"{item.get('date', 'MISSING')} | entry={item.get('entry_price', 'MISSING')} | exp={item.get('expected_profit_points', 'MISSING')}"
            )
    if not trade_items:
        trade_items = ["MISSING"]

    en_status, zh_status = _status_from_context(last_trade)

    risk = last_trade.get("risk_control") if isinstance(last_trade.get("risk_control"), dict) else {}
    strategy_mode = last_trade.get("strategy_mode") if isinstance(last_trade.get("strategy_mode"), dict) else {}
    tri = last_trade.get("tri_brain_status") if isinstance(last_trade.get("tri_brain_status"), dict) else {}

    gemini = tri.get("gemini") if isinstance(tri.get("gemini"), dict) else {}
    groq = tri.get("groq") if isinstance(tri.get("groq"), dict) else {}
    openai = tri.get("openai") if isinstance(tri.get("openai"), dict) else {}

    return {
        "generated_at": datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z"),
        "status": {"en": en_status, "zh": zh_status},
        "recent_tasks": {
            "cron": recent_log_events,
            "reports": report_items,
            "trades": trade_items,
        },
        "scheduler": scheduler,
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


class Handler(BaseHTTPRequestHandler):
    server_version = "LiveMonitorWeb/1.0"

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

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path == "/":
            self._send_html(HTML_PAGE)
            return
        if path == "/api/snapshot":
            self._send_json(collect_snapshot())
            return
        if path == "/healthz":
            self._send_json({"ok": True})
            return
        self._send_json({"error": "not found"}, status=404)

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
