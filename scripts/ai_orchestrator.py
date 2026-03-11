#!/usr/bin/env python3
"""AI orchestrator for YM=F 15m.

Flow:
1) Fetch YM=F 15m candles via yfinance.
2) Ask Gemini for deep strategy (role: 道瓊量化交易員).
3) Ask Groq for low-latency risk check.
4) Aggregate market summary + Gemini + Groq into final report.
5) Send report via proactive-agent/send_telegram.py (graceful skip).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

SYMBOL = "YM=F"
INTERVAL = "15m"
PERIOD = "2d"

BASE_DIR = Path(__file__).resolve().parents[1]
TELEGRAM_SCRIPT = BASE_DIR / "proactive-agent" / "send_telegram.py"


GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")


def eprint(msg: str) -> None:
    print(msg, file=sys.stderr)


def ensure_yfinance_dep():
    try:
        import yfinance as yf  # type: ignore
    except ImportError:
        eprint("[error] Missing dependency: yfinance. Install with: pip install yfinance")
        sys.exit(3)
    return yf


def fetch_market_snapshot(yf_module) -> Tuple[Dict[str, Any], str]:
    try:
        df = yf_module.download(SYMBOL, period=PERIOD, interval=INTERVAL, progress=False, auto_adjust=False)
    except Exception as exc:
        raise RuntimeError(f"failed to fetch market data: {exc}") from exc

    if df is None or df.empty:
        raise RuntimeError("no market data returned from yfinance")

    if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1:
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

    required = ["Open", "High", "Low", "Close", "Volume"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"market data missing columns: {missing}")

    df = df.dropna().copy()
    if len(df) < 10:
        raise RuntimeError("not enough candles for 15m analysis")

    latest = df.iloc[-1]
    prev = df.iloc[-2]
    latest_ts = str(df.index[-1])

    close = float(latest["Close"])
    open_ = float(latest["Open"])
    high = float(latest["High"])
    low = float(latest["Low"])
    vol = float(latest["Volume"])
    prev_close = float(prev["Close"])

    change = close - prev_close
    change_pct = (change / prev_close * 100.0) if prev_close else 0.0

    tail = df.tail(12)
    trend_up = int((tail["Close"].diff().fillna(0) > 0).sum())

    snapshot = {
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "timestamp": latest_ts,
        "open": round(open_, 2),
        "high": round(high, 2),
        "low": round(low, 2),
        "close": round(close, 2),
        "prev_close": round(prev_close, 2),
        "change": round(change, 2),
        "change_pct": round(change_pct, 3),
        "volume": round(vol, 2),
        "recent_12_up_candles": trend_up,
        "recent_12_total": 12,
    }

    summary = (
        f"{SYMBOL} {INTERVAL} @ {latest_ts} | O:{open_:.2f} H:{high:.2f} L:{low:.2f} C:{close:.2f} "
        f"({change:+.2f}, {change_pct:+.2f}%) Vol:{vol:.0f} | last12 up-candles={trend_up}/12"
    )
    return snapshot, summary


def call_gemini(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    key = (os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or "").strip()
    if not key:
        return {"status": "unavailable", "reason": "missing GEMINI_API_KEY/GOOGLE_API_KEY", "text": ""}

    try:
        import requests  # type: ignore
    except ImportError:
        return {"status": "unavailable", "reason": "missing requests dependency", "text": ""}

    prompt = (
        "你是『道瓊量化交易員』。請基於以下 YM=F 15m 快照，輸出深度策略。"
        "請用繁中，結構包含：\n"
        "1) 市場結構判讀\n"
        "2) 交易方向與條件\n"
        "3) 進出場（entry/stop/take-profit）\n"
        "4) 倉位與風險控制\n"
        "5) 失敗情境與應對\n"
        "最後給出一句結論。\n"
        f"市場快照: {json.dumps(snapshot, ensure_ascii=False)}"
    )

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.2, "maxOutputTokens": 900},
    }

    try:
        resp = requests.post(url, params={"key": key}, json=payload, timeout=25)
        if resp.status_code != 200:
            return {"status": "unavailable", "reason": f"Gemini HTTP {resp.status_code}", "text": ""}
        body = resp.json()
        text = (
            body.get("candidates", [{}])[0]
            .get("content", {})
            .get("parts", [{}])[0]
            .get("text", "")
            .strip()
        )
        if not text:
            return {"status": "unavailable", "reason": "empty Gemini response", "text": ""}
        return {"status": "ok", "reason": "", "text": text}
    except Exception as exc:
        return {"status": "unavailable", "reason": f"Gemini exception: {exc}", "text": ""}


def call_groq(snapshot: Dict[str, Any], gemini_text: str) -> Dict[str, Any]:
    key = (os.getenv("GROQ_API_KEY") or "").strip()
    if not key:
        return {"status": "unavailable", "reason": "missing GROQ_API_KEY", "text": ""}

    try:
        import requests  # type: ignore
    except ImportError:
        return {"status": "unavailable", "reason": "missing requests dependency", "text": ""}

    system_prompt = (
        "You are a low-latency futures risk checker. "
        "Assess risk quickly and output concise Traditional Chinese bullet points."
    )
    user_prompt = (
        "請對以下交易策略做 Risk Check，輸出：\n"
        "- 主要風險(最多3點)\n"
        "- 是否允許進場(YES/NO/CONDITIONAL)\n"
        "- 若進場，最大可承受風險與必要條件\n\n"
        f"行情: {json.dumps(snapshot, ensure_ascii=False)}\n\n"
        f"策略: {gemini_text[:3000]}"
    )

    payload = {
        "model": GROQ_MODEL,
        "temperature": 0.1,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }

    try:
        resp = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json=payload,
            timeout=20,
        )
        if resp.status_code != 200:
            return {"status": "unavailable", "reason": f"Groq HTTP {resp.status_code}", "text": ""}

        body = resp.json()
        text = body.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
        if not text:
            return {"status": "unavailable", "reason": "empty Groq response", "text": ""}
        return {"status": "ok", "reason": "", "text": text}
    except Exception as exc:
        return {"status": "unavailable", "reason": f"Groq exception: {exc}", "text": ""}


def build_final_report(market_summary: str, snapshot: Dict[str, Any], gemini: Dict[str, Any], groq: Dict[str, Any]) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    partial_mode = gemini.get("status") != "ok" or groq.get("status") != "ok"
    mode = "partial" if partial_mode else "full"

    fallback_decision = "觀望(HOLD)"
    if snapshot.get("change_pct", 0) > 0.3:
        fallback_decision = "偏多但等回踩"
    elif snapshot.get("change_pct", 0) < -0.3:
        fallback_decision = "偏空但等反彈"

    lines = [
        "🤖 AI Orchestrator 報告（YM=F 15m）",
        f"時間: {now}",
        f"模式: {mode}",
        "",
        "【基本行情摘要】",
        market_summary,
        "",
        "【Gemini 深度策略】",
    ]

    if gemini.get("status") == "ok":
        lines.append(gemini.get("text", ""))
    else:
        lines.append(f"unavailable: {gemini.get('reason', 'unknown')}")

    lines += ["", "【Groq 風險確認】"]
    if groq.get("status") == "ok":
        lines.append(groq.get("text", ""))
    else:
        lines.append(f"unavailable: {groq.get('reason', 'unknown')}")

    lines += [
        "",
        "【最終決策】",
    ]

    if gemini.get("status") == "ok" and groq.get("status") == "ok":
        lines.append("依 Gemini 策略 + Groq 風險確認執行（請人工二次確認倉位與風控）。")
    else:
        lines.append(f"部分服務不可用，採 fallback/partial mode：{fallback_decision}。")

    return "\n".join(lines).strip()


def send_telegram(report: str) -> str:
    if not TELEGRAM_SCRIPT.exists():
        return "skip: sender script not found"

    proc = subprocess.run(
        [sys.executable, str(TELEGRAM_SCRIPT), "--message", report],
        cwd=str(BASE_DIR),
        capture_output=True,
        text=True,
    )

    if proc.returncode == 0:
        return "sent"
    if proc.returncode == 2:
        return "skip: telegram not configured"
    err = (proc.stderr or proc.stdout or "").strip()
    return f"warn: telegram failed rc={proc.returncode} {err}"


def main() -> int:
    yf = ensure_yfinance_dep()

    try:
        snapshot, market_summary = fetch_market_snapshot(yf)
    except Exception as exc:
        eprint(f"[error] {exc}")
        return 4

    gemini = call_gemini(snapshot)
    groq = call_groq(snapshot, gemini.get("text", ""))

    report = build_final_report(market_summary, snapshot, gemini, groq)

    print(report)
    print("\n---")
    print(f"[info] Gemini status={gemini.get('status')} reason={gemini.get('reason', '')}")
    print(f"[info] Groq status={groq.get('status')} reason={groq.get('reason', '')}")

    tg = send_telegram(report)
    print(f"[info] Telegram: {tg}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
