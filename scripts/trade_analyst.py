#!/usr/bin/env python3
"""Trade analyst skill for YM=F (15m).

Features:
- Pull latest 2 days of 15m bars from yfinance
- Compute EMA20/EMA50/RSI14/MACD(12,26,9)/Bollinger(20,2) using pandas_ta
- Multi-factor analysis and simple AI plan (OpenAI if available, fallback rule engine)
- SQLite paper-trading engine with SL/TP auto-close
- Telegram summary notification (graceful skip if not configured)
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

SYMBOL = "YM=F"
INTERVAL = "15m"
PERIOD = "2d"

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "trading_v1.db"
TELEGRAM_SENDER = ROOT / "proactive-agent" / "send_telegram.py"


@dataclass
class MarketSnapshot:
    price: float
    ema20: float
    ema50: float
    rsi14: float
    macd: float
    macd_signal: float
    bb_lower: float
    bb_middle: float
    bb_upper: float
    prev_macd: float
    prev_macd_signal: float


@dataclass
class AIPlan:
    sentiment: int
    action: str
    entry: float
    sl: float
    tp: float
    note: str = ""


def eprint(msg: str) -> None:
    print(msg, file=sys.stderr)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_dependencies():
    try:
        import yfinance as yf  # type: ignore
    except ImportError:
        eprint("[error] Missing dependency: yfinance. Install with: pip install yfinance")
        sys.exit(3)

    try:
        import pandas_ta as ta  # type: ignore  # noqa: F401
    except ImportError:
        eprint("[error] Missing dependency: pandas_ta. Install with: pip install pandas_ta")
        sys.exit(3)

    try:
        import pandas as pd  # type: ignore  # noqa: F401
    except ImportError:
        eprint("[error] Missing dependency: pandas. Install with: pip install pandas")
        sys.exit(3)

    return yf


def fetch_and_compute(yf_module) -> MarketSnapshot:
    try:
        df = yf_module.download(SYMBOL, period=PERIOD, interval=INTERVAL, progress=False, auto_adjust=False)
    except Exception as exc:
        eprint(f"[error] Failed to fetch market data: {exc}")
        sys.exit(4)

    if df is None or df.empty:
        eprint("[error] No market data returned for YM=F")
        sys.exit(4)

    # yfinance can return MultiIndex columns; flatten if needed.
    if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1:
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

    required = {"Close"}
    missing = [c for c in required if c not in df.columns]
    if missing:
        eprint(f"[error] Missing required columns from data: {missing}")
        sys.exit(4)

    try:
        import pandas_ta as ta  # type: ignore

        close = df["Close"]
        df["EMA20"] = ta.ema(close, length=20)
        df["EMA50"] = ta.ema(close, length=50)
        df["RSI14"] = ta.rsi(close, length=14)
        macd = ta.macd(close, fast=12, slow=26, signal=9)
        bb = ta.bbands(close, length=20, std=2)
    except Exception as exc:
        eprint(f"[error] Indicator calculation failed: {exc}")
        sys.exit(5)

    if macd is None or macd.empty:
        eprint("[error] MACD indicator returned empty data")
        sys.exit(5)
    if bb is None or bb.empty:
        eprint("[error] Bollinger Bands indicator returned empty data")
        sys.exit(5)

    # pandas_ta columns are usually MACD_12_26_9, MACDs_12_26_9 and BBL_20_2.0 etc.
    macd_col = next((c for c in macd.columns if c.startswith("MACD_")), None)
    macd_signal_col = next((c for c in macd.columns if c.startswith("MACDs_")), None)
    bbl_col = next((c for c in bb.columns if c.startswith("BBL_")), None)
    bbm_col = next((c for c in bb.columns if c.startswith("BBM_")), None)
    bbu_col = next((c for c in bb.columns if c.startswith("BBU_")), None)

    if not all([macd_col, macd_signal_col, bbl_col, bbm_col, bbu_col]):
        eprint("[error] Unexpected indicator column names from pandas_ta")
        sys.exit(5)

    df["MACD"] = macd[macd_col]
    df["MACD_SIGNAL"] = macd[macd_signal_col]
    df["BBL"] = bb[bbl_col]
    df["BBM"] = bb[bbm_col]
    df["BBU"] = bb[bbu_col]

    df = df.dropna().copy()
    if len(df) < 2:
        eprint("[error] Not enough rows after indicator calculation")
        sys.exit(5)

    latest = df.iloc[-1]
    prev = df.iloc[-2]

    return MarketSnapshot(
        price=float(latest["Close"]),
        ema20=float(latest["EMA20"]),
        ema50=float(latest["EMA50"]),
        rsi14=float(latest["RSI14"]),
        macd=float(latest["MACD"]),
        macd_signal=float(latest["MACD_SIGNAL"]),
        bb_lower=float(latest["BBL"]),
        bb_middle=float(latest["BBM"]),
        bb_upper=float(latest["BBU"]),
        prev_macd=float(prev["MACD"]),
        prev_macd_signal=float(prev["MACD_SIGNAL"]),
    )


def get_trend(s: MarketSnapshot) -> str:
    if s.price > s.ema20 > s.ema50:
        return "強多頭"
    if s.price < s.ema20 < s.ema50:
        return "強空頭"
    return "中性"


def get_signals(s: MarketSnapshot) -> Dict[str, str]:
    if s.rsi14 >= 70:
        rsi_signal = "RSI超買"
    elif s.rsi14 <= 30:
        rsi_signal = "RSI超賣"
    else:
        rsi_signal = "RSI中性"

    cross_up = s.prev_macd <= s.prev_macd_signal and s.macd > s.macd_signal
    cross_down = s.prev_macd >= s.prev_macd_signal and s.macd < s.macd_signal
    if cross_up:
        macd_signal = "MACD金叉"
    elif cross_down:
        macd_signal = "MACD死叉"
    else:
        macd_signal = "MACD無明確交叉"

    return {"rsi": rsi_signal, "macd": macd_signal}


def ensure_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            entry_price REAL NOT NULL,
            sl REAL NOT NULL,
            tp REAL NOT NULL,
            status TEXT NOT NULL,
            opened_at TEXT NOT NULL,
            closed_at TEXT,
            close_price REAL,
            pnl REAL,
            ai_sentiment INTEGER,
            ai_action TEXT,
            note TEXT
        )
        """
    )
    conn.commit()


def settle_open_trades(conn: sqlite3.Connection, latest_price: float) -> int:
    rows = conn.execute(
        "SELECT id, side, entry_price, sl, tp FROM trades WHERE status='open'"
    ).fetchall()

    closed = 0
    for trade_id, side, entry_price, sl, tp in rows:
        side_u = str(side).upper()
        hit = None
        close_px = None

        if side_u == "BUY":
            if latest_price <= sl:
                hit = "sl"
                close_px = sl
            elif latest_price >= tp:
                hit = "tp"
                close_px = tp
            pnl = (close_px - entry_price) if close_px is not None else None
        else:  # SELL
            if latest_price >= sl:
                hit = "sl"
                close_px = sl
            elif latest_price <= tp:
                hit = "tp"
                close_px = tp
            pnl = (entry_price - close_px) if close_px is not None else None

        if hit and close_px is not None and pnl is not None:
            conn.execute(
                """
                UPDATE trades
                SET status='closed', closed_at=?, close_price=?, pnl=?, note=COALESCE(note,'') || ?
                WHERE id=?
                """,
                (now_iso(), float(close_px), float(pnl), f" | auto-close:{hit}", trade_id),
            )
            closed += 1

    conn.commit()
    return closed


def has_open_trade(conn: sqlite3.Connection) -> bool:
    row = conn.execute("SELECT 1 FROM trades WHERE status='open' LIMIT 1").fetchone()
    return row is not None


def cumulative_pnl(conn: sqlite3.Connection) -> float:
    row = conn.execute("SELECT COALESCE(SUM(pnl), 0) FROM trades WHERE status='closed'").fetchone()
    return float(row[0] if row and row[0] is not None else 0.0)


def clamp(n: float, a: float, b: float) -> float:
    return max(a, min(b, n))


def fallback_plan(trend: str, signals: Dict[str, str], s: MarketSnapshot) -> AIPlan:
    sentiment = 50
    action = "HOLD"

    if trend == "強多頭":
        sentiment += 20
    elif trend == "強空頭":
        sentiment -= 20

    if signals["rsi"] == "RSI超賣":
        sentiment += 12
    elif signals["rsi"] == "RSI超買":
        sentiment -= 12

    if signals["macd"] == "MACD金叉":
        sentiment += 10
    elif signals["macd"] == "MACD死叉":
        sentiment -= 10

    sentiment = int(clamp(sentiment, 0, 100))

    if sentiment >= 60:
        action = "BUY"
    elif sentiment <= 40:
        action = "SELL"

    entry = s.price
    width = max(20.0, abs(s.bb_upper - s.bb_lower) * 0.6)
    if action == "BUY":
        sl = entry - width
        tp = entry + width * 1.5
    elif action == "SELL":
        sl = entry + width
        tp = entry - width * 1.5
    else:
        sl = entry - width
        tp = entry + width

    return AIPlan(
        sentiment=sentiment,
        action=action,
        entry=round(entry, 2),
        sl=round(sl, 2),
        tp=round(tp, 2),
        note="rule-engine",
    )


def try_openai_plan(trend: str, signals: Dict[str, str], s: MarketSnapshot) -> Optional[AIPlan]:
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        return None

    try:
        import requests  # type: ignore
    except ImportError:
        eprint("[warn] OPENAI_API_KEY present but requests missing; fallback to rule engine")
        return None

    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    url = "https://api.openai.com/v1/chat/completions"
    prompt = {
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "trend": trend,
        "signals": signals,
        "price": s.price,
        "ema20": s.ema20,
        "ema50": s.ema50,
        "rsi14": s.rsi14,
        "macd": s.macd,
        "macd_signal": s.macd_signal,
        "bb_lower": s.bb_lower,
        "bb_middle": s.bb_middle,
        "bb_upper": s.bb_upper,
    }

    sys_msg = (
        "You are a futures trading planning assistant. Return only JSON with keys: "
        "sentiment(0-100 int), action(BUY/SELL/HOLD), entry(number), sl(number), tp(number), note(string)."
    )
    user_msg = f"Create one short-term plan using this market snapshot: {json.dumps(prompt, ensure_ascii=False)}"

    payload = {
        "model": model,
        "temperature": 0.2,
        "messages": [
            {"role": "system", "content": sys_msg},
            {"role": "user", "content": user_msg},
        ],
        "response_format": {"type": "json_object"},
    }

    try:
        resp = requests.post(
            url,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=payload,
            timeout=20,
        )
        if resp.status_code != 200:
            eprint(f"[warn] OpenAI API HTTP {resp.status_code}; fallback to rule engine")
            return None
        data = resp.json()
        content = data["choices"][0]["message"]["content"]
        parsed: Dict[str, Any] = json.loads(content)

        sentiment = int(clamp(float(parsed.get("sentiment", 50)), 0, 100))
        action = str(parsed.get("action", "HOLD")).upper()
        if action not in {"BUY", "SELL", "HOLD"}:
            action = "HOLD"

        entry = float(parsed.get("entry", s.price))
        sl = float(parsed.get("sl", s.price))
        tp = float(parsed.get("tp", s.price))
        note = str(parsed.get("note", "llm"))

        return AIPlan(sentiment, action, round(entry, 2), round(sl, 2), round(tp, 2), note)
    except Exception as exc:
        eprint(f"[warn] OpenAI plan failed ({exc}); fallback to rule engine")
        return None


def maybe_open_trade(conn: sqlite3.Connection, plan: AIPlan, trend: str) -> bool:
    if has_open_trade(conn):
        return False

    if plan.action not in {"BUY", "SELL"}:
        return False

    # Keep a simple guard to avoid opening against strong opposite trend.
    if trend == "強空頭" and plan.action == "BUY":
        return False
    if trend == "強多頭" and plan.action == "SELL":
        return False

    conn.execute(
        """
        INSERT INTO trades
        (symbol, side, entry_price, sl, tp, status, opened_at, ai_sentiment, ai_action, note)
        VALUES (?, ?, ?, ?, ?, 'open', ?, ?, ?, ?)
        """,
        (SYMBOL, plan.action, plan.entry, plan.sl, plan.tp, now_iso(), plan.sentiment, plan.action, plan.note),
    )
    conn.commit()
    return True


def send_telegram_summary(trend: str, rsi: float, action: str, price: float, total_pnl: float) -> None:
    message = (
        "📊 道瓊期貨分析 (15m)\n"
        f"📈 趨勢：{trend} | RSI：{rsi:.2f}\n"
        f"🤖 AI 建議：{action} (Entry: {price:.2f})\n"
        f"💰 累計模擬損益：{total_pnl:.2f}"
    )

    if not TELEGRAM_SENDER.exists():
        print("[skip] Telegram sender script not found")
        return

    result = subprocess.run(
        [sys.executable, str(TELEGRAM_SENDER), "--message", message],
        capture_output=True,
        text=True,
    )

    if result.returncode == 0:
        print("[ok] Telegram summary sent")
        return

    # send_telegram.py uses 2 for missing env; skip gracefully.
    if result.returncode == 2:
        print("[skip] Telegram not configured")
        return

    stderr = (result.stderr or "").strip()
    print(f"[warn] Telegram send failed (code={result.returncode}) {stderr}")


def main() -> int:
    yf = ensure_dependencies()
    snapshot = fetch_and_compute(yf)

    trend = get_trend(snapshot)
    signals = get_signals(snapshot)

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        ensure_db(conn)
        closed_count = settle_open_trades(conn, snapshot.price)

        plan = try_openai_plan(trend, signals, snapshot) or fallback_plan(trend, signals, snapshot)
        opened = maybe_open_trade(conn, plan, trend)
        total_pnl = cumulative_pnl(conn)
    finally:
        conn.close()

    print(f"[info] Symbol={SYMBOL} Price={snapshot.price:.2f} Trend={trend}")
    print(f"[info] RSI={snapshot.rsi14:.2f} MACD={snapshot.macd:.4f}/{snapshot.macd_signal:.4f}")
    print(f"[info] Signals: {signals['rsi']}, {signals['macd']}")
    print(
        f"[info] Plan: sentiment={plan.sentiment} action={plan.action} "
        f"entry={plan.entry:.2f} sl={plan.sl:.2f} tp={plan.tp:.2f} source={plan.note}"
    )
    print(f"[info] Trades updated: closed={closed_count}, opened_new={opened}")
    print(f"[info] Cumulative closed PnL={total_pnl:.2f}")

    send_telegram_summary(trend, snapshot.rsi14, plan.action, plan.entry, total_pnl)
    return 0


if __name__ == "__main__":
    sys.exit(main())
