#!/usr/bin/env python3
import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

BASE_DIR = Path(__file__).resolve().parents[1]
TRADE_LOGS_PATH = BASE_DIR / "data" / "trade_logs.json"
TELEGRAM_SENDER = BASE_DIR / "proactive-agent" / "send_telegram.py"


def _to_bool_text(v: Any) -> str:
    return "YES" if bool(v) else "NO"


def _load_trade_logs(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"trade log not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("trade_logs.json must be a JSON array")

    if not data:
        raise ValueError("trade_logs.json is empty")

    last = data[-1]
    if not isinstance(last, dict):
        raise ValueError("latest trade log item is not an object")

    return data


def _extract_cooldown(latest: Dict[str, Any]) -> Dict[str, Any]:
    risk_control = latest.get("risk_control") if isinstance(latest.get("risk_control"), dict) else {}
    tri_brain = latest.get("tri_brain_status") if isinstance(latest.get("tri_brain_status"), dict) else {}

    gemini_cd = risk_control.get("gemini_cooldown") if isinstance(risk_control.get("gemini_cooldown"), dict) else None
    if gemini_cd is None:
        gemini_node = tri_brain.get("gemini") if isinstance(tri_brain.get("gemini"), dict) else {}
        gemini_cd = gemini_node.get("cooldown") if isinstance(gemini_node.get("cooldown"), dict) else {}

    if not isinstance(gemini_cd, dict):
        gemini_cd = {}

    return {
        "active": bool(gemini_cd.get("active", False)),
        "until": gemini_cd.get("until"),
        "reason": gemini_cd.get("reason"),
    }


def _extract_tri_brain_fallback(latest: Dict[str, Any]) -> Tuple[bool, List[str]]:
    tri_brain = latest.get("tri_brain_status") if isinstance(latest.get("tri_brain_status"), dict) else {}
    fallback_nodes: List[str] = []

    for name in ("groq", "gemini", "openai"):
        node = tri_brain.get(name)
        if isinstance(node, dict) and str(node.get("status", "")).lower() == "fallback":
            fallback_nodes.append(name)

    return (len(fallback_nodes) > 0, fallback_nodes)


def _fmt_num(v: Any) -> str:
    if isinstance(v, (int, float)):
        return f"{v:.2f}" if isinstance(v, float) else str(v)
    return "N/A"


def build_summary(latest: Dict[str, Any]) -> str:
    action_missing = ("action" not in latest) or (latest.get("action") is None)

    risk_control = latest.get("risk_control") if isinstance(latest.get("risk_control"), dict) else {}
    cooldown = _extract_cooldown(latest)
    tri_fallback, tri_nodes = _extract_tri_brain_fallback(latest)

    monthly = latest.get("monthly_progress") if isinstance(latest.get("monthly_progress"), dict) else {}

    date = latest.get("date", "N/A")
    circuit_breaker = risk_control.get("circuit_breaker_active", False)
    losing_streak = risk_control.get("losing_streak", "N/A")

    # Accept both existing key names and desired labels
    current = monthly.get("current")
    if current is None:
        current = monthly.get("current_pnl")
    target = monthly.get("target")
    if target is None:
        target = monthly.get("target_points")
    achievement = monthly.get("achievement")
    if achievement is None:
        achievement = monthly.get("achievement_pct")

    lines = [
        "Trade Health Check",
        f"- latest_log_time: {date}",
        f"- action_missing_or_none: {_to_bool_text(action_missing)}",
        (
            f"- gemini_cooldown: {'ACTIVE' if cooldown['active'] else 'inactive'}"
            + (f" (until: {cooldown['until']})" if cooldown.get("until") else "")
            + (f" | reason: {cooldown['reason']}" if cooldown.get("reason") else "")
        ),
        f"- tri_brain_fallback: {_to_bool_text(tri_fallback)}"
        + (f" ({', '.join(tri_nodes)})" if tri_nodes else ""),
        f"- circuit_breaker_active: {_to_bool_text(circuit_breaker)}",
        f"- losing_streak: {losing_streak}",
        f"- monthly_progress: current={_fmt_num(current)} / target={_fmt_num(target)} / achievement={_fmt_num(achievement)}%",
    ]

    return "\n".join(lines)


def maybe_notify(summary: str) -> int:
    if not TELEGRAM_SENDER.exists():
        print(f"[skip] notifier not found: {TELEGRAM_SENDER}")
        return 0

    result = subprocess.run(
        [sys.executable, str(TELEGRAM_SENDER), "--message", summary],
        capture_output=True,
        text=True,
    )

    if result.stdout.strip():
        print(result.stdout.strip())
    if result.stderr.strip():
        print(result.stderr.strip(), file=sys.stderr)

    # send_telegram.py returns 2 when env missing -> graceful skip
    if result.returncode == 2:
        return 0
    return result.returncode


def main() -> int:
    parser = argparse.ArgumentParser(description="Check trade system health from latest trade_logs.json")
    parser.add_argument("--notify", action="store_true", help="Send summary via proactive-agent/send_telegram.py")
    args = parser.parse_args()

    try:
        logs = _load_trade_logs(TRADE_LOGS_PATH)
    except Exception as e:
        print(f"[error] {e}", file=sys.stderr)
        return 1

    latest = logs[-1]
    summary = build_summary(latest)
    print(summary)

    if args.notify:
        rc = maybe_notify(summary)
        if rc != 0:
            print(f"[error] notify failed with code {rc}", file=sys.stderr)
            return rc

    return 0


if __name__ == "__main__":
    sys.exit(main())
