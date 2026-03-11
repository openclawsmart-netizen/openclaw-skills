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
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "trading_v1.db"
TRADE_LOG_JSON = DATA_DIR / "trade_logs.json"
TRADE_LOG_CSV = DATA_DIR / "trade_logs.csv"
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
    candle_body: float
    upper_wick_ratio: float
    lower_wick_ratio: float
    bias_ema20: float
    bias_ema50: float


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
    new_skill_proposal: Optional[Dict[str, str]] = None


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

    price_range = (df["High"] - df["Low"]).astype(float)
    safe_range = price_range.where(price_range != 0.0)
    candle_top = df[["Open", "Close"]].max(axis=1)
    candle_bottom = df[["Open", "Close"]].min(axis=1)

    df["CANDLE_BODY"] = (df["Close"] - df["Open"]).abs().astype(float)
    df["UPPER_WICK_RATIO"] = ((df["High"] - candle_top) / safe_range).fillna(0.0)
    df["LOWER_WICK_RATIO"] = ((candle_bottom - df["Low"]) / safe_range).fillna(0.0)
    df["BIAS_EMA20"] = ((df["Close"] - df["EMA20"]) / df["EMA20"].replace(0, float("nan")) * 100.0).fillna(0.0)
    df["BIAS_EMA50"] = ((df["Close"] - df["EMA50"]) / df["EMA50"].replace(0, float("nan")) * 100.0).fillna(0.0)

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
        candle_body=float(latest["CANDLE_BODY"]),
        upper_wick_ratio=float(latest["UPPER_WICK_RATIO"]),
        lower_wick_ratio=float(latest["LOWER_WICK_RATIO"]),
        bias_ema20=float(latest["BIAS_EMA20"]),
        bias_ema50=float(latest["BIAS_EMA50"]),
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
        f"RSI14={s.rsi14:.2f}, MACD={s.macd:.3f}/{s.macd_signal:.3f}, "
        f"Body={s.candle_body:.2f}, UpperWick={s.upper_wick_ratio:.3f}, LowerWick={s.lower_wick_ratio:.3f}, "
        f"BiasEMA20={s.bias_ema20:.2f}%, BiasEMA50={s.bias_ema50:.2f}%"
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
            "new_skill_proposal": None,
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
        "Output ONLY JSON with keys: sentiment_score, reflection_one_liner, action, entry, sl, tp, reason, new_skill_proposal. "
        "action must be LONG, SHORT, or HOLD. sentiment_score must be 0-100 integer. "
        "new_skill_proposal must be null or an object with skill_name and reason."
    )

    extra = ""
    if winrate < 50:
        extra = "必須輸出：失敗檢討與策略修正，並放在 reflection_one_liner/reason 內。"

    user_prompt = (
        f"歷史反思：最近5筆勝率 {winrate:.1f}%\n"
        f"歷史摘要：{history_summary}\n"
        f"{extra}\n"
        "當下決策：根據最新K線與指標，結合歷史教訓，給 direction/entry/sl/tp/action。\n"
        "你不必拘泥於傳統指標。你可以觀察這些基礎特徵的組合。如果發現特殊的 K 線型態勝率更高，你可以直接定義這個新形態作為進場理由。\n"
        f"當下盤勢輸入: ts={s.ts}, close={s.close}, EMA20={s.ema20}, EMA50={s.ema50}, "
        f"RSI14={s.rsi14}, MACD={s.macd}, MACD_SIGNAL={s.macd_signal}, "
        f"candle_body={s.candle_body}, upper_wick_ratio={s.upper_wick_ratio}, lower_wick_ratio={s.lower_wick_ratio}, "
        f"bias_ema20={s.bias_ema20}, bias_ema50={s.bias_ema50}\n"
        "為了達成月獲利 1000 點的 KPI，如果你發現當前的數據來源（只有 K 線）不足以提高勝率（例如你需要 VIX 恐慌指數，或新聞情緒），你可以提出開發新 Skill 的需求。"
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

        proposal = None
        raw_proposal = parsed.get("new_skill_proposal")
        if isinstance(raw_proposal, dict):
            skill_name = str(raw_proposal.get("skill_name", "")).strip()
            proposal_reason = str(raw_proposal.get("reason", "")).strip()
            if skill_name and proposal_reason:
                proposal = {"skill_name": skill_name[:80], "reason": proposal_reason[:300]}

        return Plan(sentiment, reflection, action, entry, sl, tp, reason, text, proposal)
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
    msg = (
        "📊 道瓊期貨 AI 交易員 (15m)\n"
        f"🧠 自我檢討：{winrate:.1f}% - {reflection}\n"
        f"📈 當前盤勢：{trend}\n"
        f"🤖 最新計畫：{p.action} (Entry: {p.entry:.2f}, SL: {p.sl:.2f}, TP: {p.tp:.2f})"
    )
    if p.new_skill_proposal:
        skill_name = p.new_skill_proposal.get("skill_name", "(unknown)")
        reason = p.new_skill_proposal.get("reason", "(no reason)")
        msg += (
            "\n"
            f"💡 [AI 研發提案]：我需要擴充新能力 - {skill_name}。"
            f"理由：{reason}。請建友協助開發或授權。"
        )
    return msg


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


def trade_status_from_prices(side: str, entry: float, sl: float, tp: float, current_price: float) -> Tuple[str, float]:
    side_u = str(side or "").upper()
    if side_u == "LONG":
        if current_price >= tp:
            return "WIN", float(tp - entry)
        if current_price <= sl:
            return "LOSS", float(sl - entry)
        return "OPEN", float(current_price - entry)
    if side_u == "SHORT":
        if current_price <= tp:
            return "WIN", float(entry - tp)
        if current_price >= sl:
            return "LOSS", float(entry - sl)
        return "OPEN", float(entry - current_price)
    return "NA", 0.0


def get_prior_trade_status(conn: sqlite3.Connection, current_price: float) -> Dict[str, Any]:
    row = conn.execute(
        """
        SELECT opened_at, side, entry_price, sl, tp, status, close_price, pnl
        FROM trades
        ORDER BY datetime(opened_at) DESC, id DESC
        LIMIT 1
        """
    ).fetchone()

    if not row:
        return {
            "date": "NA",
            "entry_price": 0.0,
            "status": "NA",
            "current_price": float(current_price),
            "profit_loss_points": 0.0,
        }

    opened_at, side, entry_price, sl, tp, db_status, close_price, pnl = row
    entry_v = safe_float(entry_price, 0.0)
    sl_v = safe_float(sl, entry_v)
    tp_v = safe_float(tp, entry_v)

    db_status_s = str(db_status or "").upper()
    if db_status_s in {"WIN", "LOSS"}:
        current_v = safe_float(close_price, current_price)
        pnl_v = safe_float(pnl, 0.0)
        status = db_status_s
    elif db_status_s == "OPEN":
        current_v = float(current_price)
        status, pnl_v = trade_status_from_prices(str(side), entry_v, sl_v, tp_v, current_v)
    else:
        current_v = float(current_price)
        status, pnl_v = "NA", 0.0

    return {
        "date": str(opened_at or "NA"),
        "entry_price": float(entry_v),
        "status": status,
        "current_price": float(current_v),
        "profit_loss_points": float(pnl_v),
    }


def append_trade_log_and_export_csv(record: Dict[str, Any]) -> Tuple[int, str, str]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    records: List[Dict[str, Any]] = []
    if TRADE_LOG_JSON.exists():
        try:
            with TRADE_LOG_JSON.open("r", encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, list):
                records = loaded
        except Exception:
            records = []

    records.append(record)

    with TRADE_LOG_JSON.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    try:
        import pandas as pd  # type: ignore

        pd.read_json(str(TRADE_LOG_JSON)).to_csv(TRADE_LOG_CSV, index=False, encoding="utf-8-sig")
    except Exception as exc:
        eprint(f"[warn] CSV export failed: {exc}")

    return len(records), str(TRADE_LOG_JSON), str(TRADE_LOG_CSV)


def main() -> int:
    yf = ensure_deps()
    s = fetch_snapshot(yf)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        ensure_db(conn)
        closed = settle_open_trades(conn, s.close)
        winrate, _, history_summary = get_reflection(conn)
        plan = llm_plan(s, winrate, history_summary) or rule_plan(s, winrate, history_summary)
        prior_trade_status = get_prior_trade_status(conn, s.close)
        opened = maybe_open_trade(conn, plan)
    finally:
        conn.close()

    trend = trend_summary(s)
    tg_state = send_telegram(telegram_summary(winrate, plan.reflection_one_liner, trend, plan))

    expected_profit_points = plan.tp - plan.entry if plan.action != "SHORT" else plan.entry - plan.tp
    log_record: Dict[str, Any] = {
        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "entry_price": float(plan.entry),
        "take_profit_price": float(plan.tp),
        "stop_loss_price": float(plan.sl),
        "expected_profit_points": float(expected_profit_points),
        "analysis_reasoning": str(plan.reason),
        "error_review": str(plan.reflection_one_liner),
        "optimization_suggestion": "依近期勝率動態調整進場過濾條件與停損帶寬。",
        "new_skill_proposal": plan.new_skill_proposal,
        "prior_trade_status": prior_trade_status,
    }
    log_count, json_path, csv_path = append_trade_log_and_export_csv(log_record)

    print(f"[info] Symbol={SYMBOL} interval={INTERVAL} ts={s.ts}")
    print(
        "[info] Indicators: "
        f"Close={s.close:.2f} EMA20={s.ema20:.2f} EMA50={s.ema50:.2f} RSI14={s.rsi14:.2f} "
        f"MACD={s.macd:.3f}/{s.macd_signal:.3f} Body={s.candle_body:.2f} "
        f"UpperWick={s.upper_wick_ratio:.3f} LowerWick={s.lower_wick_ratio:.3f} "
        f"BiasEMA20={s.bias_ema20:.2f}% BiasEMA50={s.bias_ema50:.2f}%"
    )
    print(f"[info] Reflection winrate(last5)={winrate:.1f}%")
    print(
        f"[info] Plan: sentiment_score={plan.sentiment_score} action={plan.action} "
        f"entry={plan.entry:.2f} sl={plan.sl:.2f} tp={plan.tp:.2f}"
    )
    print(f"[info] new_skill_proposal={json.dumps(plan.new_skill_proposal, ensure_ascii=False)}")
    print(f"[info] Trades: settled_open={closed}, opened_new={opened}")
    print(f"[info] Log export: records={log_count} json={json_path} csv={csv_path}")
    print(f"[info] Telegram: {tg_state}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
