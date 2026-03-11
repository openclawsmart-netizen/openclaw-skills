#!/usr/bin/env python3
"""YM=F 15m trade analyst skill.

- Downloads recent 15m candles via yfinance (period=5d)
- Computes EMA20/EMA50/RSI14/MACD(12,26,9) via pandas_ta
- Settles/open paper trades in SQLite: data/trading_v1.db
- Builds self-reflection from latest 5 closed trades
- Generates plan with LLM (if OPENAI_API_KEY exists) or rule fallback
- Sends Telegram summary via proactive-agent/send_telegram.py (graceful skip)
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
from typing import Any, Dict, List, Optional, Tuple

SYMBOL = "YM=F"
INTERVAL = "15m"
PERIOD = "5d"

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "trading_v1.db"
TELEGRAM_SCRIPT = BASE_DIR / "proactive-agent" / "send_telegram.py"


@dataclass
class Snapshot:
    ts: str
    close: float
    ema20: float
    ema50: float
    rsi14: float
    macd: float
    macd_signal: float


@dataclass
class Plan:
    sentiment_score: int
    reflection_one_liner: str
    action: str
    entry: float
    sl: float
    tp: float
    reason: str
    raw: str


def eprint(msg: str) -> None:
    print(msg, file=sys.stderr)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_deps():
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


def fetch_snapshot(yf_module) -> Snapshot:
    try:
        df = yf_module.download(SYMBOL, period=PERIOD, interval=INTERVAL, progress=False, auto_adjust=False)
    except Exception as exc:
        eprint(f"[error] Failed to fetch market data from yfinance: {exc}")
        sys.exit(4)

    if df is None or df.empty:
        eprint("[error] No market data returned for YM=F")
        sys.exit(4)

    if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1:
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

    if "Close" not in df.columns:
        eprint("[error] Market data missing required column: Close")
        sys.exit(4)

    try:
        import pandas_ta as ta  # type: ignore

        close = df["Close"]
        df["EMA20"] = ta.ema(close, length=20)
        df["EMA50"] = ta.ema(close, length=50)
        df["RSI14"] = ta.rsi(close, length=14)
        macd = ta.macd(close, fast=12, slow=26, signal=9)
    except Exception as exc:
        eprint(f"[error] Indicator calculation failed: {exc}")
        sys.exit(5)

    if macd is None or macd.empty:
        eprint("[error] MACD indicator returned empty data")
        sys.exit(5)

    macd_col = next((c for c in macd.columns if c.startswith("MACD_")), None)
    macds_col = next((c for c in macd.columns if c.startswith("MACDs_")), None)
    if not macd_col or not macds_col:
        eprint("[error] Unexpected MACD column names from pandas_ta")
        sys.exit(5)

    df["MACD"] = macd[macd_col]
    df["MACD_SIGNAL"] = macd[macds_col]

    df = df.dropna().copy()
    if df.empty:
        eprint("[error] Not enough rows after indicators (dropna produced empty dataframe)")
        sys.exit(5)

    latest = df.iloc[-1]
    ts = str(df.index[-1])
    return Snapshot(
        ts=ts,
        close=float(latest["Close"]),
        ema20=float(latest["EMA20"]),
        ema50=float(latest["EMA50"]),
        rsi14=float(latest["RSI14"]),
        macd=float(latest["MACD"]),
        macd_signal=float(latest["MACD_SIGNAL"]),
    )


def ensure_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            opened_at TEXT,
            side TEXT,
            entry_price REAL,
            sl REAL,
            tp REAL,
            reason TEXT,
            status TEXT,
            closed_at TEXT,
            close_price REAL,
            pnl REAL,
            ai_reflection TEXT,
            ai_plan_raw TEXT
        )
        """
    )
    conn.commit()


def settle_open_trades(conn: sqlite3.Connection, price: float) -> int:
    rows = conn.execute(
        "SELECT id, side, entry_price, sl, tp FROM trades WHERE status='OPEN'"
    ).fetchall()
    closed = 0

    for trade_id, side, entry, sl, tp in rows:
        side = str(side).upper()
        outcome = None
        close_price = None

        if side == "LONG":
            if price <= float(sl):
                outcome = "LOSS"
                close_price = float(sl)
            elif price >= float(tp):
                outcome = "WIN"
                close_price = float(tp)
            pnl = (close_price - float(entry)) if close_price is not None else None
        elif side == "SHORT":
            if price >= float(sl):
                outcome = "LOSS"
                close_price = float(sl)
            elif price <= float(tp):
                outcome = "WIN"
                close_price = float(tp)
            pnl = (float(entry) - close_price) if close_price is not None else None
        else:
            continue

        if outcome and close_price is not None and pnl is not None:
            conn.execute(
                """
                UPDATE trades
                SET status=?, closed_at=?, close_price=?, pnl=?
                WHERE id=?
                """,
                (outcome, now_iso(), close_price, float(pnl), trade_id),
            )
            closed += 1

    conn.commit()
    return closed


def get_reflection(conn: sqlite3.Connection) -> Tuple[float, List[str], str]:
    rows = conn.execute(
        """
        SELECT reason, pnl, status
        FROM trades
        WHERE status IN ('WIN','LOSS','CLOSED') AND closed_at IS NOT NULL
        ORDER BY datetime(closed_at) DESC
        LIMIT 5
        """
    ).fetchall()

    if not rows:
        return 0.0, [], "No closed trade history yet."

    win_count = sum(1 for _, _, st in rows if str(st).upper() == "WIN")
    total = len(rows)
    winrate = (win_count / total) * 100.0 if total else 0.0

    items: List[str] = []
    for reason, pnl, status in rows:
        reason_s = (reason or "(no reason)").strip()
        pnl_v = float(pnl or 0.0)
        status_s = str(status or "").upper()
        items.append(f"- {status_s} | PnL={pnl_v:.2f} | reason={reason_s}")

    summary = (
        f"Last {total} closed trades winrate={winrate:.1f}%. "
        + " ".join([f"[{i}]" for i in items])
    )
    return winrate, items, summary


def trend_summary(s: Snapshot) -> str:
    trend = "震盪"
    if s.close > s.ema20 > s.ema50:
        trend = "多頭"
    elif s.close < s.ema20 < s.ema50:
        trend = "空頭"
    return (
        f"{trend} | Close={s.close:.2f}, EMA20={s.ema20:.2f}, EMA50={s.ema50:.2f}, "
        f"RSI14={s.rsi14:.2f}, MACD={s.macd:.3f}/{s.macd_signal:.3f}"
    )


def safe_float(v: Any, default: float) -> float:
    try:
        return float(v)
    except Exception:
        return default


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def rule_plan(s: Snapshot, winrate: float, history_summary: str) -> Plan:
    score = 50
    if s.close > s.ema20 > s.ema50:
        score += 18
    elif s.close < s.ema20 < s.ema50:
        score -= 18

    if s.rsi14 < 30:
        score += 10
    elif s.rsi14 > 70:
        score -= 10

    if s.macd > s.macd_signal:
        score += 7
    else:
        score -= 7

    score = int(clamp(score, 0, 100))

    action = "HOLD"
    if score >= 60:
        action = "LONG"
    elif score <= 40:
        action = "SHORT"

    entry = s.close
    band = max(s.close * 0.002, 25.0)
    if action == "LONG":
        sl = entry - band
        tp = entry + band * 1.6
    elif action == "SHORT":
        sl = entry + band
        tp = entry - band * 1.6
    else:
        sl = entry - band
        tp = entry + band

    refl = "依規則引擎：延續優勢訊號"
    if winrate < 50:
        refl = "近期勝率偏低，已收斂風險並修正進場條件"

    reason = f"Fallback engine based on EMA/RSI/MACD. history={history_summary}"
    raw = json.dumps(
        {
            "source": "rule_engine",
            "sentiment_score": score,
            "reflection_one_liner": refl,
            "action": action,
            "entry": round(entry, 2),
            "sl": round(sl, 2),
            "tp": round(tp, 2),
            "reason": reason,
        },
        ensure_ascii=False,
    )

    return Plan(score, refl, action, round(entry, 2), round(sl, 2), round(tp, 2), reason, raw)


def llm_plan(s: Snapshot, winrate: float, history_summary: str) -> Optional[Plan]:
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        return None

    try:
        import requests  # type: ignore
    except ImportError:
        eprint("[warn] OPENAI_API_KEY is set but requests is missing; fallback to rule engine")
        return None

    model = (os.getenv("OPENAI_MODEL") or "gpt-4o-mini").strip()
    sys_prompt = (
        "You are a futures trading strategist for YM=F 15m paper trading. "
        "Output ONLY JSON with keys: sentiment_score, reflection_one_liner, action, entry, sl, tp, reason. "
        "action must be LONG, SHORT, or HOLD. sentiment_score must be 0-100 integer."
    )

    extra = ""
    if winrate < 50:
        extra = "必須輸出：失敗檢討與策略修正，並放在 reflection_one_liner/reason 內。"

    user_prompt = (
        f"歷史反思：最近5筆勝率 {winrate:.1f}%\n"
        f"歷史摘要：{history_summary}\n"
        f"{extra}\n"
        "當下決策：根據最新K線與指標，結合歷史教訓，給 direction/entry/sl/tp/action。\n"
        f"最新資料: ts={s.ts}, close={s.close}, EMA20={s.ema20}, EMA50={s.ema50}, "
        f"RSI14={s.rsi14}, MACD={s.macd}, MACD_SIGNAL={s.macd_signal}"
    )

    payload = {
        "model": model,
        "temperature": 0.2,
        "messages": [
            {"role": "system", "content": sys_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "response_format": {"type": "json_object"},
    }

    try:
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=payload,
            timeout=30,
        )
        if resp.status_code != 200:
            eprint(f"[warn] OpenAI HTTP {resp.status_code}; fallback to rule engine")
            return None

        body = resp.json()
        text = body["choices"][0]["message"]["content"]
        parsed = json.loads(text)

        sentiment = int(clamp(safe_float(parsed.get("sentiment_score"), 50), 0, 100))
        action = str(parsed.get("action", "HOLD")).upper().strip()
        if action not in {"LONG", "SHORT", "HOLD"}:
            action = "HOLD"

        entry = round(safe_float(parsed.get("entry"), s.close), 2)
        sl = round(safe_float(parsed.get("sl"), s.close), 2)
        tp = round(safe_float(parsed.get("tp"), s.close), 2)
        reflection = str(parsed.get("reflection_one_liner", ""))[:220] or "N/A"
        reason = str(parsed.get("reason", ""))[:800] or "N/A"

        return Plan(sentiment, reflection, action, entry, sl, tp, reason, text)
    except Exception as exc:
        eprint(f"[warn] OpenAI planning failed ({exc}); fallback to rule engine")
        return None


def has_open_trade(conn: sqlite3.Connection) -> bool:
    row = conn.execute("SELECT 1 FROM trades WHERE status='OPEN' LIMIT 1").fetchone()
    return row is not None


def maybe_open_trade(conn: sqlite3.Connection, p: Plan) -> bool:
    if has_open_trade(conn):
        return False

    if p.action not in {"LONG", "SHORT"}:
        return False

    conn.execute(
        """
        INSERT INTO trades (
            symbol, opened_at, side, entry_price, sl, tp, reason, status,
            ai_reflection, ai_plan_raw
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'OPEN', ?, ?)
        """,
        (
            SYMBOL,
            now_iso(),
            p.action,
            p.entry,
            p.sl,
            p.tp,
            p.reason,
            p.reflection_one_liner,
            p.raw,
        ),
    )
    conn.commit()
    return True


def telegram_summary(winrate: float, reflection: str, trend: str, p: Plan) -> str:
    return (
        "📊 道瓊期貨 AI 交易員 (15m)\n"
        f"🧠 自我檢討：{winrate:.1f}% - {reflection}\n"
        f"📈 當前盤勢：{trend}\n"
        f"🤖 最新計畫：{p.action} (Entry: {p.entry:.2f}, SL: {p.sl:.2f}, TP: {p.tp:.2f})"
    )


def send_telegram(msg: str) -> str:
    if not TELEGRAM_SCRIPT.exists():
        return "skip: sender script not found"

    proc = subprocess.run(
        [sys.executable, str(TELEGRAM_SCRIPT), "--message", msg],
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
    yf = ensure_deps()
    s = fetch_snapshot(yf)

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        ensure_db(conn)
        closed = settle_open_trades(conn, s.close)
        winrate, _, history_summary = get_reflection(conn)
        plan = llm_plan(s, winrate, history_summary) or rule_plan(s, winrate, history_summary)
        opened = maybe_open_trade(conn, plan)
    finally:
        conn.close()

    trend = trend_summary(s)
    tg_state = send_telegram(telegram_summary(winrate, plan.reflection_one_liner, trend, plan))

    print(f"[info] Symbol={SYMBOL} interval={INTERVAL} ts={s.ts}")
    print(f"[info] Indicators: Close={s.close:.2f} EMA20={s.ema20:.2f} EMA50={s.ema50:.2f} RSI14={s.rsi14:.2f} MACD={s.macd:.3f}/{s.macd_signal:.3f}")
    print(f"[info] Reflection winrate(last5)={winrate:.1f}%")
    print(
        f"[info] Plan: sentiment_score={plan.sentiment_score} action={plan.action} "
        f"entry={plan.entry:.2f} sl={plan.sl:.2f} tp={plan.tp:.2f}"
    )
    print(f"[info] Trades: settled_open={closed}, opened_new={opened}")
    print(f"[info] Telegram: {tg_state}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
