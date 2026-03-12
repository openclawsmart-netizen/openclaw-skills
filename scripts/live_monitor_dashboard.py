#!/usr/bin/env python3
"""Live monitor dashboard (中英對照) for openclaw-skills."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
REPORTS_DIR = BASE_DIR / "reports"

TRADE_LOG_JSON = DATA_DIR / "trade_logs.json"
CRON_LOG = LOGS_DIR / "cron_trade.log"


def _safe_read_json(path: Path, default: Any) -> Any:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _tail_lines(path: Path, limit: int = 80) -> List[str]:
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


def _parse_iso(ts: str) -> Optional[datetime]:
    try:
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        return datetime.fromisoformat(ts)
    except Exception:
        return None


def _fmt_missing(v: Any) -> str:
    if v is None:
        return "缺少 / MISSING"
    if v == "":
        return "缺少 / MISSING"
    return str(v)


def _human_dt(dt: Optional[datetime]) -> str:
    if not dt:
        return "未知 / Unknown"
    local = dt.astimezone()
    return local.strftime("%Y-%m-%d %H:%M:%S %Z")


def _get_crontab_lines() -> List[str]:
    try:
        res = subprocess.run(["crontab", "-l"], capture_output=True, text=True, check=False)
        if res.returncode != 0:
            return ["(無 crontab 或無法讀取) / (No crontab or cannot read)"]
        lines = [ln.strip() for ln in res.stdout.splitlines() if ln.strip() and not ln.strip().startswith("#")]
        keys = ("run-skill.sh", "trade", "batch", "daily", "system-status", "health", "report")
        filtered = [ln for ln in lines if any(k in ln for k in keys)]
        return filtered[:8] if filtered else ["(無關鍵排程) / (No key scheduler entries)"]
    except Exception as e:
        return [f"(讀取失敗) / (Read failed): {e}"]


def _status_from_context(last_trade: Dict[str, Any], cron_lines: List[str]) -> Tuple[str, str]:
    risk = last_trade.get("risk_control") if isinstance(last_trade.get("risk_control"), dict) else {}
    if risk.get("circuit_breaker_active") is True:
        return "Paused", "暫停"

    # infer running if recent cron log activity within 30 minutes
    recent = _tail_lines(CRON_LOG, 120)
    latest_ts = None
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
    cron_tail = _tail_lines(CRON_LOG, 120)
    scheduler = _get_crontab_lines()

    # recent tasks
    recent_log_events = [ln for ln in cron_tail if "[run]" in ln or "gemini_" in ln or "run_skipped" in ln]
    recent_log_events = recent_log_events[-5:]

    report_items: List[str] = []
    if REPORTS_DIR.exists():
        files = sorted([p for p in REPORTS_DIR.iterdir() if p.is_file()], key=lambda p: p.stat().st_mtime, reverse=True)[:3]
        for p in files:
            ts = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc).astimezone().strftime("%m-%d %H:%M")
            report_items.append(f"{p.name} ({ts})")

    trade_items: List[str] = []
    all_logs = _safe_read_json(TRADE_LOG_JSON, [])
    if isinstance(all_logs, list):
        for item in all_logs[-3:]:
            if not isinstance(item, dict):
                continue
            trade_items.append(
                f"{item.get('date', 'N/A')} | entry={item.get('entry_price', 'N/A')} | exp={item.get('expected_profit_points', 'N/A')}"
            )

    en_status, zh_status = _status_from_context(last_trade, scheduler)

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
            "cron": recent_log_events or ["(無近期事件) / (No recent events)"],
            "reports": report_items or ["(無近期報告) / (No recent reports)"],
            "trades": trade_items or ["(無交易紀錄) / (No trade logs)"],
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


def _build_plain_text(s: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(f"更新時間 / Updated At: {s['generated_at']}")
    lines.append("=" * 88)

    lines.append("目前狀態 / Current Status")
    lines.append(f"  {s['status']['zh']} / {s['status']['en']}")
    lines.append("")

    lines.append("最近任務 / Recent Tasks")
    lines.append("  [Cron]")
    for x in s["recent_tasks"]["cron"]:
        lines.append(f"    - {x}")
    lines.append("  [Reports]")
    for x in s["recent_tasks"]["reports"]:
        lines.append(f"    - {x}")
    lines.append("  [Trades]")
    for x in s["recent_tasks"]["trades"]:
        lines.append(f"    - {x}")
    lines.append("")

    lines.append("排程狀態 / Scheduler")
    for x in s["scheduler"]:
        lines.append(f"  - {x}")
    lines.append("")

    lines.append("交易摘要 / Trade Snapshot")
    for k, v in s["trade_snapshot"].items():
        lines.append(f"  - {k}: {v}")
    lines.append("")

    lines.append("風控摘要 / Risk Control")
    for k, v in s["risk"].items():
        lines.append(f"  - {k}: {v}")
    lines.append("")

    lines.append("模式與路由 / Mode & Routing")
    for k, v in s["mode_routing"].items():
        lines.append(f"  - {k}: {v}")

    return "\n".join(lines)


def run_plain(refresh_sec: float) -> None:
    while True:
        snap = collect_snapshot()
        os.system("clear")
        print(_build_plain_text(snap), flush=True)
        time.sleep(refresh_sec)


def run_rich(refresh_sec: float) -> None:
    from rich.console import Console
    from rich.layout import Layout
    from rich.live import Live
    from rich.panel import Panel
    from rich.table import Table

    console = Console()

    def render() -> Layout:
        snap = collect_snapshot()

        root = Layout()
        root.split_column(
            Layout(name="header", size=3),
            Layout(name="body"),
        )
        root["body"].split_row(Layout(name="left"), Layout(name="right"))
        root["left"].split_column(Layout(name="status", size=6), Layout(name="recent"), Layout(name="scheduler"))
        root["right"].split_column(Layout(name="trade"), Layout(name="risk"), Layout(name="mode"))

        root["header"].update(Panel(f"中英監控視窗 / Live Monitor Dashboard\n更新時間 / Updated At: {snap['generated_at']}", border_style="cyan"))

        root["status"].update(
            Panel(
                f"目前狀態 / Current Status\n[bold]{snap['status']['zh']} / {snap['status']['en']}[/bold]",
                border_style="green",
            )
        )

        t_recent = Table(show_header=True, header_style="bold magenta", expand=True)
        t_recent.add_column("類型 / Type", width=16)
        t_recent.add_column("內容 / Content")
        for c in snap["recent_tasks"]["cron"]:
            t_recent.add_row("Cron", c)
        for c in snap["recent_tasks"]["reports"]:
            t_recent.add_row("Reports", c)
        for c in snap["recent_tasks"]["trades"]:
            t_recent.add_row("Trades", c)
        root["recent"].update(Panel(t_recent, title="最近任務 / Recent Tasks", border_style="magenta"))

        t_sched = Table(show_header=False, expand=True)
        t_sched.add_column("entry")
        for x in snap["scheduler"]:
            t_sched.add_row(x)
        root["scheduler"].update(Panel(t_sched, title="排程狀態 / Scheduler", border_style="yellow"))

        t_trade = Table(show_header=False, expand=True)
        t_trade.add_column("k", width=26)
        t_trade.add_column("v")
        for k, v in snap["trade_snapshot"].items():
            t_trade.add_row(k, str(v))
        root["trade"].update(Panel(t_trade, title="交易摘要 / Trade Snapshot", border_style="blue"))

        t_risk = Table(show_header=False, expand=True)
        t_risk.add_column("k", width=26)
        t_risk.add_column("v")
        for k, v in snap["risk"].items():
            t_risk.add_row(k, str(v))
        root["risk"].update(Panel(t_risk, title="風控摘要 / Risk Control", border_style="red"))

        t_mode = Table(show_header=False, expand=True)
        t_mode.add_column("k", width=26)
        t_mode.add_column("v")
        for k, v in snap["mode_routing"].items():
            t_mode.add_row(k, str(v))
        root["mode"].update(Panel(t_mode, title="模式與路由 / Mode & Routing", border_style="cyan"))

        return root

    with Live(render(), console=console, refresh_per_second=max(1, int(round(1 / max(refresh_sec, 0.2))))) as live:
        while True:
            time.sleep(refresh_sec)
            live.update(render())


def main() -> int:
    parser = argparse.ArgumentParser(description="中英對照即時監控視窗 / Bilingual live monitor dashboard")
    parser.add_argument("--refresh-sec", type=float, default=3.0, help="Refresh interval seconds (default: 3)")
    args = parser.parse_args()

    try:
        try:
            import rich  # noqa: F401

            run_rich(args.refresh_sec)
        except Exception:
            run_plain(args.refresh_sec)
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
