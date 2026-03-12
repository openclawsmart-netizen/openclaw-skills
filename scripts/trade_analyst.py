#!/usr/bin/env python3
"""YM=F 15m trade analyst skill (single quant entrypoint).

Architecture (tri-brain):
1) Time Guard (TW maintenance + weekend + US holiday) => hit then exit(0)
2) Groq risk check => normalized risk_level / volatility_flag
3) Gemini strategy => action/entry/sl/tp/reason/new_skill_proposal
4) OpenAI arbitration => final risk review + JSON normalization
   - No OPENAI key => deterministic fallback with same output schema

Unified persistence:
- SQLite: data/trading_v1.db (trades)
- JSON:   data/trade_logs.json (append valid JSON array)
- CSV:    data/trade_logs.csv (utf-8-sig)
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

MONTHLY_TARGET_POINTS = 1000.0
# 達成率 >= 90% 視為接近目標，切換防守模式
DEFENSIVE_PROGRESS_THRESHOLD = 90.0
# 近期勝率穩定門檻：最近 N 筆已平倉，勝率 >= 55%
STABLE_WINRATE_LOOKBACK = 8
STABLE_WINRATE_THRESHOLD = 55.0
from zoneinfo import ZoneInfo

SYMBOL = "YM=F"
INTERVAL = "15m"
PERIOD = "5d"

YM_ROOT = "YM"
YM_EXCHANGE_SUFFIX = ".CBT"
YM_QUARTER_MONTH_CODES = [(3, "H"), (6, "M"), (9, "U"), (12, "Z")]
CONSULTANT_COMMANDER_ROLLOVER_TAG = "🚨 [諮詢-指揮官]：目前處於換倉週，價差波動可能導致技術指標失真，請指揮官覆核策略。"

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
LOG_DIR = BASE_DIR / "logs"
CRON_TRADE_LOG = LOG_DIR / "cron_trade.log"
DB_PATH = DATA_DIR / "trading_v1.db"
TRADE_LOG_JSON = DATA_DIR / "trade_logs.json"
TRADE_LOG_CSV = DATA_DIR / "trade_logs.csv"
TELEGRAM_SCRIPT = BASE_DIR / "proactive-agent" / "send_telegram.py"

GROQ_MODEL = (os.getenv("GROQ_MODEL") or "llama-3.1-8b-instant").strip()
GEMINI_MODEL = (os.getenv("GEMINI_MODEL") or "gemini-2.0-flash").strip()
OPENAI_MODEL = (os.getenv("OPENAI_MODEL") or "gpt-4o-mini").strip()

CIRCUIT_BREAKER_DAILY_LOSS_LIMIT = -150.0
CIRCUIT_BREAKER_MAX_LOSS_STREAK = 3

CONSULTANT_COMMANDER_TAG = "🚨 [諮詢-指揮官]：策略觸發警報，請將此報表 Excel 丟給指揮官 Gem 進行深度覆盤。"
CONSULTANT_ENGINEER_TAG = "⚙️ [諮詢-工程師]：運算效能遇到瓶頸，請將需求丟給工程師 Gem 實作 OpenCL 優化。"
CONSULTANT_SPECIALIST_TAG = "🏗 [諮詢-專員]：系統架構或環境異常，請將 Log 丟給專員 Gem 進行修復或架構升級。"


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
    sentiment_score: float  # 0~1
    reflection_one_liner: str
    action: str  # BUY/SELL/HOLD (external schema)
    entry: float
    sl: float
    tp: float
    reason: str
    raw: str
    new_skill_proposal: Optional[Dict[str, str]] = None


@dataclass
class MonthlyProgress:
    current_pnl: float
    remaining: float
    achievement_pct: float


@dataclass
class StrategyModeContext:
    mode: str
    context: str


@dataclass
class RolloverDecision:
    active_contract: str
    near_contract: str
    far_contract: str
    near_volume: Optional[float]
    far_volume: Optional[float]
    switched_to_far: bool
    in_rollover_week: bool
    reason: str
    adjustment_method: str


def eprint(msg: str) -> None:
    print(msg, file=sys.stderr)


def append_cron_trade_log(message: str) -> None:
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        with CRON_TRADE_LOG.open("a", encoding="utf-8") as f:
            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}\n")
    except Exception as exc:
        eprint(f"[warn] failed writing cron trade log: {exc}")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(v: Any, default: float) -> float:
    try:
        return float(v)
    except Exception:
        return default


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def normalize_sentiment_0_1(v: Any, default: float = 0.5) -> float:
    fv = safe_float(v, default)
    # backward-compatible: if model still returns 0~100, convert to 0~1
    if fv > 1.0:
        fv = fv / 100.0
    return round(clamp(fv, 0.0, 1.0), 4)


def normalize_action_external(v: Any) -> str:
    a = str(v or "HOLD").upper().strip()
    mapping = {
        "BUY": "BUY",
        "SELL": "SELL",
        "HOLD": "HOLD",
        "LONG": "BUY",
        "SHORT": "SELL",
    }
    return mapping.get(a, "HOLD")


def external_to_internal_side(action: str) -> Optional[str]:
    a = normalize_action_external(action)
    if a == "BUY":
        return "LONG"
    if a == "SELL":
        return "SHORT"
    return None


def extract_json_object(text: str) -> Dict[str, Any]:
    text = (text or "").strip()
    if not text:
        return {}
    try:
        loaded = json.loads(text)
        if isinstance(loaded, dict):
            return loaded
    except Exception:
        pass

    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        try:
            loaded = json.loads(text[start : end + 1])
            if isinstance(loaded, dict):
                return loaded
        except Exception:
            pass
    return {}


def get_taipei_now(override_iso: Optional[str] = None) -> datetime:
    tz = ZoneInfo("Asia/Taipei")
    if override_iso:
        parsed = datetime.fromisoformat(override_iso)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=tz)
        return parsed.astimezone(tz)
    return datetime.now(tz)


def is_futures_market_closed_taipei(now_tpe: datetime) -> bool:
    if now_tpe.tzinfo is None:
        now_tpe = now_tpe.replace(tzinfo=ZoneInfo("Asia/Taipei"))
    else:
        now_tpe = now_tpe.astimezone(ZoneInfo("Asia/Taipei"))

    weekday = now_tpe.weekday()  # Mon=0 ... Sun=6
    hhmmss = now_tpe.time()

    if 5 <= now_tpe.hour < 6:  # 每日保養
        return True
    if weekday == 5 and hhmmss >= datetime.strptime("05:00:00", "%H:%M:%S").time():  # 週六 05:00 後
        return True
    if weekday == 6:  # 週日
        return True
    if weekday == 0 and hhmmss < datetime.strptime("06:00:00", "%H:%M:%S").time():  # 週一 06:00 前
        return True
    return False


def get_us_eastern_zone() -> Optional[ZoneInfo]:
    for tz_name in ("US/Eastern", "America/New_York"):
        try:
            return ZoneInfo(tz_name)
        except Exception:
            continue
    eprint("[warn] 無法載入 US/Eastern 時區資料，將略過美國國定假日檢查並繼續執行。")
    return None


def ensure_holidays_module():
    try:
        import holidays  # type: ignore

        return holidays
    except ImportError:
        eprint("[warn] Missing dependency: holidays. Trying to install via pip3...")

    try:
        proc = subprocess.run(
            ["pip3", "install", "holidays", "--break-system-packages"],
            capture_output=True,
            text=True,
            timeout=120,
        )
    except Exception as exc:
        eprint(f"[warn] Failed to run pip3 install holidays: {exc}")
        return None

    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        eprint(f"[warn] pip3 install holidays failed (rc={proc.returncode}): {err}")
        return None

    try:
        import holidays  # type: ignore

        return holidays
    except ImportError:
        eprint("[warn] holidays 安裝後仍無法載入，將略過美國國定假日檢查並繼續執行。")
        return None


def get_us_holiday_name_from_taipei(
    now_tpe: datetime,
    holidays_module=None,
    holidays_calendar: Optional[Dict[Any, Any]] = None,
) -> Optional[str]:
    if now_tpe.tzinfo is None:
        now_tpe = now_tpe.replace(tzinfo=ZoneInfo("Asia/Taipei"))
    else:
        now_tpe = now_tpe.astimezone(ZoneInfo("Asia/Taipei"))

    eastern_tz = get_us_eastern_zone()
    if eastern_tz is None:
        return None

    now_eastern = now_tpe.astimezone(eastern_tz)
    eastern_date = now_eastern.date()

    if holidays_calendar is None:
        if holidays_module is None:
            return None
        try:
            holidays_calendar = holidays_module.US(years=now_eastern.year)
        except Exception as exc:
            eprint(f"[warn] 美國假日資料初始化失敗: {exc}")
            return None

    if eastern_date in holidays_calendar:
        return str(holidays_calendar[eastern_date])
    return None


def _run_holiday_guard_selftest() -> None:
    now_tpe = datetime.fromisoformat("2026-07-03T12:00:00+08:00")
    fake_holidays = {
        now_tpe.astimezone(ZoneInfo("America/New_York")).date(): "Independence Day (observed)"
    }
    holiday_name = get_us_holiday_name_from_taipei(now_tpe=now_tpe, holidays_module=None, holidays_calendar=fake_holidays)
    if holiday_name != "Independence Day (observed)":
        raise AssertionError(f"holiday guard selftest failed: got={holiday_name}")


def _run_time_guard_selftest() -> None:
    tz = ZoneInfo("Asia/Taipei")
    cases = [
        ("2026-03-12T04:59:59+08:00", False),
        ("2026-03-12T05:00:00+08:00", True),
        ("2026-03-12T05:59:59+08:00", True),
        ("2026-03-12T06:00:00+08:00", False),
        ("2026-03-14T04:59:59+08:00", False),
        ("2026-03-14T05:00:00+08:00", True),
        ("2026-03-15T12:00:00+08:00", True),
        ("2026-03-16T05:59:59+08:00", True),
        ("2026-03-16T06:00:00+08:00", False),
    ]
    for iso_s, expected in cases:
        dt = datetime.fromisoformat(iso_s).astimezone(tz)
        got = is_futures_market_closed_taipei(dt)
        if got != expected:
            raise AssertionError(f"time guard selftest failed: {iso_s} expected={expected} got={got}")


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


def _normalize_download_df(df):
    if df is None or df.empty:
        return df
    if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1:
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    return df


def build_ym_contract_ticker(year: int, month: int) -> str:
    month_code = {m: c for m, c in YM_QUARTER_MONTH_CODES}.get(month)
    if not month_code:
        raise ValueError(f"Unsupported YM quarter month: {month}")
    yy = year % 100
    return f"{YM_ROOT}{month_code}{yy:02d}{YM_EXCHANGE_SUFFIX}"


def infer_near_far_ym_contracts(now_tpe: datetime) -> Tuple[str, str, int, int]:
    y = now_tpe.year
    m = now_tpe.month
    months = [x[0] for x in YM_QUARTER_MONTH_CODES]
    near_month = next((mm for mm in months if mm >= m), months[0])
    near_year = y if near_month >= m else y + 1
    idx = months.index(near_month)
    far_month = months[(idx + 1) % len(months)]
    far_year = near_year if far_month > near_month else near_year + 1
    return build_ym_contract_ticker(near_year, near_month), build_ym_contract_ticker(far_year, far_month), near_month, far_month


def fetch_contract_volume(yf_module, ticker: str) -> Optional[float]:
    try:
        vdf = _normalize_download_df(yf_module.download(ticker, period="10d", interval="1d", progress=False, auto_adjust=False))
    except Exception:
        return None
    if vdf is None or vdf.empty or "Volume" not in vdf.columns:
        return None
    vols = vdf["Volume"].dropna()
    if vols.empty:
        return None
    return float(vols.tail(3).mean())


def detect_rollover_and_active_contract(yf_module, now_tpe: datetime) -> RolloverDecision:
    near_contract, far_contract, near_month, _ = infer_near_far_ym_contracts(now_tpe)
    near_volume = fetch_contract_volume(yf_module, near_contract)
    far_volume = fetch_contract_volume(yf_module, far_contract)

    switched_to_far = False
    active_contract = SYMBOL
    reason = "fallback_continuous_symbol"

    if near_volume is not None and far_volume is not None:
        switched_to_far = far_volume > near_volume
        active_contract = far_contract if switched_to_far else near_contract
        reason = f"volume_compare far({far_volume:.0f}) {'>' if switched_to_far else '<='} near({near_volume:.0f})"
    elif near_volume is not None:
        active_contract = near_contract
        reason = "far_volume_missing_use_near"
    elif far_volume is not None:
        active_contract = far_contract
        reason = "near_volume_missing_use_far"

    in_rollover_week = now_tpe.month == near_month and 8 <= now_tpe.day <= 21

    return RolloverDecision(
        active_contract=active_contract,
        near_contract=near_contract,
        far_contract=far_contract,
        near_volume=near_volume,
        far_volume=far_volume,
        switched_to_far=switched_to_far,
        in_rollover_week=in_rollover_week,
        reason=reason,
        adjustment_method="back-adjust spread stitching",
    )


def build_continuous_adjusted_df(yf_module, decision: RolloverDecision):
    # 連續合約處理：
    # 1) 抓取近月與遠月資料
    # 2) 用重疊區段的收盤價差做 back-adjust（把近月歷史平移到遠月價位）
    # 3) 若已換倉，拼接為「前段近月(已校正) + 後段遠月」避免換月跳空污染 EMA/RSI/MACD
    near_df = _normalize_download_df(yf_module.download(decision.near_contract, period=PERIOD, interval=INTERVAL, progress=False, auto_adjust=False))
    far_df = _normalize_download_df(yf_module.download(decision.far_contract, period=PERIOD, interval=INTERVAL, progress=False, auto_adjust=False))

    if near_df is None or near_df.empty:
        base = _normalize_download_df(yf_module.download(SYMBOL, period=PERIOD, interval=INTERVAL, progress=False, auto_adjust=False))
        return base, "fallback: near_contract_missing"
    if far_df is None or far_df.empty:
        return near_df, "fallback: far_contract_missing"

    overlap_idx = near_df.index.intersection(far_df.index)
    if len(overlap_idx) == 0:
        return far_df if decision.switched_to_far else near_df, "fallback: no_overlap"

    spread = float((far_df.loc[overlap_idx, "Close"] - near_df.loc[overlap_idx, "Close"]).median())
    adjusted_near = near_df.copy()
    for col in ("Open", "High", "Low", "Close"):
        if col in adjusted_near.columns:
            adjusted_near[col] = adjusted_near[col].astype(float) + spread

    if decision.switched_to_far:
        switch_ts = overlap_idx[-1]
        stitched = adjusted_near[adjusted_near.index < switch_ts].copy()
        stitched = stitched.combine_first(far_df[far_df.index >= switch_ts].copy())
        stitched = stitched.sort_index()
        return stitched, f"back-adjust spread={spread:.2f} switched_at={switch_ts}"

    return adjusted_near.sort_index(), f"back-adjust spread={spread:.2f} near-active"


def fetch_snapshot(yf_module, decision: RolloverDecision) -> Snapshot:
    try:
        df, _ = build_continuous_adjusted_df(yf_module, decision)
        if decision.active_contract not in {decision.near_contract, decision.far_contract}:
            df = _normalize_download_df(yf_module.download(SYMBOL, period=PERIOD, interval=INTERVAL, progress=False, auto_adjust=False))
    except Exception as exc:
        eprint(f"[error] Failed to fetch market data from yfinance: {exc}")
        sys.exit(4)

    if df is None or df.empty:
        eprint("[error] No market data returned for YM")
        sys.exit(4)

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

    # 必須保留的微觀特徵
    df["CANDLE_BODY"] = (df["Close"] - df["Open"]).abs().astype(float)
    df["UPPER_WICK_RATIO"] = ((df["High"] - candle_top) / safe_range).fillna(0.0)
    df["LOWER_WICK_RATIO"] = ((candle_bottom - df["Low"]) / safe_range).fillna(0.0)
    df["BIAS_EMA20"] = ((df["Close"] - df["EMA20"]) / df["EMA20"].replace(0, float("nan")) * 100.0).fillna(0.0)
    df["BIAS_EMA50"] = ((df["Close"] - df["EMA50"]) / df["EMA50"].replace(0, float("nan")) * 100.0).fillna(0.0)

    df = df.dropna().copy()
    if df.empty:
        eprint("[error] Not enough rows after indicators")
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
    rows = conn.execute("SELECT id, side, entry_price, sl, tp FROM trades WHERE status='OPEN'").fetchall()
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


def ensure_risk_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS risk_daily_stats (
            trade_date TEXT PRIMARY KEY,
            cumulative_pnl REAL NOT NULL DEFAULT 0,
            losing_streak INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS risk_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            circuit_breaker_active INTEGER NOT NULL DEFAULT 0,
            breaker_reason TEXT,
            triggered_at TEXT,
            active_date TEXT,
            gemini_cooldown_until TEXT,
            gemini_cooldown_reason TEXT,
            gemini_cooldown_set_at TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )
    # backward-compatible migration for existing DBs
    for col, col_type in [
        ("gemini_cooldown_until", "TEXT"),
        ("gemini_cooldown_reason", "TEXT"),
        ("gemini_cooldown_set_at", "TEXT"),
    ]:
        try:
            conn.execute(f"ALTER TABLE risk_state ADD COLUMN {col} {col_type}")
        except sqlite3.OperationalError:
            pass

    conn.execute(
        """
        INSERT OR IGNORE INTO risk_state (
            id, circuit_breaker_active, breaker_reason, triggered_at, active_date,
            gemini_cooldown_until, gemini_cooldown_reason, gemini_cooldown_set_at, updated_at
        )
        VALUES (1, 0, '', NULL, '', NULL, '', NULL, ?)
        """,
        (now_iso(),),
    )
    conn.commit()


def get_taipei_date_str(ts: Any) -> Optional[str]:
    dt = _parse_db_ts_to_taipei(ts)
    if dt is None:
        return None
    return dt.strftime("%Y-%m-%d")


def compute_daily_pnl_and_losing_streak(conn: sqlite3.Connection, date_str: str) -> Tuple[float, int]:
    rows = conn.execute(
        """
        SELECT closed_at, pnl, status
        FROM trades
        WHERE status IN ('WIN','LOSS','CLOSED') AND closed_at IS NOT NULL
        ORDER BY datetime(closed_at) DESC, id DESC
        """
    ).fetchall()

    daily_pnl = 0.0
    for closed_at, pnl, _ in rows:
        if get_taipei_date_str(closed_at) == date_str:
            daily_pnl += safe_float(pnl, 0.0)

    streak = 0
    for closed_at, pnl, status in rows:
        if get_taipei_date_str(closed_at) != date_str:
            continue
        st_u = str(status or "").upper()
        pnl_v = safe_float(pnl, 0.0)
        is_loss = st_u == "LOSS" or (st_u == "CLOSED" and pnl_v < 0)
        if is_loss:
            streak += 1
        else:
            break

    return float(daily_pnl), int(streak)


def upsert_risk_daily_stats(conn: sqlite3.Connection, date_str: str, cumulative_pnl: float, losing_streak: int) -> None:
    conn.execute(
        """
        INSERT INTO risk_daily_stats (trade_date, cumulative_pnl, losing_streak, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(trade_date) DO UPDATE SET
            cumulative_pnl=excluded.cumulative_pnl,
            losing_streak=excluded.losing_streak,
            updated_at=excluded.updated_at
        """,
        (date_str, float(cumulative_pnl), int(losing_streak), now_iso()),
    )
    conn.commit()


def get_circuit_breaker_state(conn: sqlite3.Connection) -> Dict[str, Any]:
    row = conn.execute(
        "SELECT circuit_breaker_active, breaker_reason, triggered_at, active_date FROM risk_state WHERE id=1"
    ).fetchone()
    if not row:
        return {"active": False, "reason": "", "triggered_at": None, "active_date": ""}
    active, reason, triggered_at, active_date = row
    return {
        "active": bool(active),
        "reason": str(reason or ""),
        "triggered_at": triggered_at,
        "active_date": str(active_date or ""),
    }


def set_circuit_breaker_state(
    conn: sqlite3.Connection,
    *,
    active: bool,
    reason: str,
    active_date: str,
    triggered_at: Optional[str] = None,
) -> None:
    conn.execute(
        """
        UPDATE risk_state
        SET circuit_breaker_active=?, breaker_reason=?, triggered_at=?, active_date=?, updated_at=?
        WHERE id=1
        """,
        (1 if active else 0, reason, triggered_at, active_date, now_iso()),
    )
    conn.commit()


def evaluate_circuit_breaker(cumulative_pnl: float, losing_streak: int) -> Optional[str]:
    reasons = []
    if cumulative_pnl <= CIRCUIT_BREAKER_DAILY_LOSS_LIMIT:
        reasons.append(f"當日累計損益 {cumulative_pnl:.2f} 點 <= {CIRCUIT_BREAKER_DAILY_LOSS_LIMIT:.0f} 點")
    if losing_streak >= CIRCUIT_BREAKER_MAX_LOSS_STREAK:
        reasons.append(f"連續虧損 {losing_streak} 筆 >= {CIRCUIT_BREAKER_MAX_LOSS_STREAK} 筆")
    if not reasons:
        return None
    return "；".join(reasons)


def _parse_iso_utc(ts: Any) -> Optional[datetime]:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(str(ts))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def get_gemini_cooldown_state(conn: sqlite3.Connection) -> Dict[str, Any]:
    row = conn.execute(
        "SELECT gemini_cooldown_until, gemini_cooldown_reason, gemini_cooldown_set_at FROM risk_state WHERE id=1"
    ).fetchone()
    if not row:
        return {"active": False, "until": None, "reason": "", "set_at": None}

    until_raw, reason, set_at_raw = row
    now_utc = datetime.now(timezone.utc)
    until_dt = _parse_iso_utc(until_raw)
    set_at_dt = _parse_iso_utc(set_at_raw)
    active = bool(until_dt and now_utc < until_dt)
    return {
        "active": active,
        "until": until_dt.isoformat() if until_dt else None,
        "reason": str(reason or ""),
        "set_at": set_at_dt.isoformat() if set_at_dt else None,
    }


def set_gemini_cooldown(conn: sqlite3.Connection, *, until: datetime, reason: str) -> None:
    until_utc = until.astimezone(timezone.utc)
    now_s = now_iso()
    conn.execute(
        """
        UPDATE risk_state
        SET gemini_cooldown_until=?, gemini_cooldown_reason=?, gemini_cooldown_set_at=?, updated_at=?
        WHERE id=1
        """,
        (until_utc.isoformat(), reason[:300], now_s, now_s),
    )
    conn.commit()
    append_cron_trade_log(
        f"gemini_cooldown_triggered until={until_utc.isoformat()} reason={reason[:180]}"
    )


def clear_gemini_cooldown(conn: sqlite3.Connection, *, end_reason: str) -> None:
    prev = get_gemini_cooldown_state(conn)
    if prev.get("until"):
        append_cron_trade_log(
            f"gemini_cooldown_ended at={datetime.now(timezone.utc).isoformat()} previous_until={prev.get('until')} reason={end_reason[:180]}"
        )
    conn.execute(
        """
        UPDATE risk_state
        SET gemini_cooldown_until=NULL, gemini_cooldown_reason='', gemini_cooldown_set_at=NULL, updated_at=?
        WHERE id=1
        """,
        (now_iso(),),
    )
    conn.commit()


def _contains_any(text: str, keywords: List[str]) -> bool:
    lowered = (text or "").lower()
    return any(k in lowered for k in keywords)


def build_consultant_routing(
    *,
    risk_control: Dict[str, Any],
    error_review: str,
    tri_brain_status: Dict[str, Any],
    new_skill_proposal: Optional[Dict[str, str]],
    optimization_suggestion: str,
    indicator_calc_seconds: float,
    in_rollover_week: bool,
    perf_anomaly: Dict[str, Any],
) -> Tuple[List[str], List[str]]:
    tags: List[str] = []
    notes: List[str] = []

    # A) 諮詢-指揮官（風控斷路器/連三虧/嚴重邏輯偏離）
    if bool(risk_control.get("circuit_breaker_active")):
        tags.append(CONSULTANT_COMMANDER_TAG)
        notes.append("觸發原因：circuit_breaker_active=true")
    if int(safe_float(risk_control.get("losing_streak"), 0)) >= 3:
        if CONSULTANT_COMMANDER_TAG not in tags:
            tags.append(CONSULTANT_COMMANDER_TAG)
        notes.append("觸發原因：losing_streak >= 3")

    severe_logic_keywords = [
        "severe", "critical", "重大", "嚴重", "偏離", "錯誤", "hallucination", "inconsistent", "矛盾", "失真",
    ]
    if _contains_any(error_review, severe_logic_keywords):
        if CONSULTANT_COMMANDER_TAG not in tags:
            tags.append(CONSULTANT_COMMANDER_TAG)
        notes.append("觸發原因：error_review 顯示策略邏輯嚴重偏離")

    if in_rollover_week and bool(perf_anomaly.get("abnormal")):
        tags.append(CONSULTANT_COMMANDER_ROLLOVER_TAG)
        notes.append(f"觸發原因：換倉週績效異常 ({perf_anomaly.get('reason', 'unknown')})")

    # B) 諮詢-工程師（複雜數學/大量資料/低延遲/指標計算 > 3 秒）
    proposal_text = ""
    if isinstance(new_skill_proposal, dict):
        proposal_text = f"{new_skill_proposal.get('skill_name', '')} {new_skill_proposal.get('reason', '')}"
    engineer_keywords = [
        "complex", "math", "matrix", "regression", "optimization", "大量", "比對", "低延遲", "latency", "real-time",
        "high frequency", "vector", "gpu", "opencl", "backtest", "million", "big data",
    ]
    if _contains_any(proposal_text, engineer_keywords):
        tags.append(CONSULTANT_ENGINEER_TAG)
        notes.append("觸發原因：new_skill_proposal 涉及複雜運算/資料量/低延遲需求")
    if indicator_calc_seconds > 3.0:
        if CONSULTANT_ENGINEER_TAG not in tags:
            tags.append(CONSULTANT_ENGINEER_TAG)
        notes.append(f"觸發原因：indicator_calc_seconds={indicator_calc_seconds:.3f} > 3.0")

    # C) 諮詢-專員（路徑/API key/DB 寫入/系統環境/模擬轉實盤(IB)）
    tri_text = json.dumps(tri_brain_status or {}, ensure_ascii=False)
    specialist_keywords = [
        "path", "not found", "api key", "missing", "db", "sqlite", "write failed", "permission", "env", "環境", "憑證", "失效",
        "exception", "failed", "error", "timeout",
    ]
    migration_keywords = [
        "ib", "interactive brokers", "paper", "live", "實盤", "模擬轉實盤", "gateway", "tws", "下單橋接", "execution adapter",
    ]
    if _contains_any(tri_text, specialist_keywords):
        tags.append(CONSULTANT_SPECIALIST_TAG)
        notes.append("觸發原因：檢測到 API/DB/環境錯誤訊號")

    migration_hint = f"{proposal_text} {optimization_suggestion}"
    if _contains_any(migration_hint, migration_keywords):
        if CONSULTANT_SPECIALIST_TAG not in tags:
            tags.append(CONSULTANT_SPECIALIST_TAG)
        notes.append("觸發原因：檢測到模擬轉實盤(IB)架構調整需求")

    unique_tags: List[str] = []
    for tag in tags:
        if tag not in unique_tags:
            unique_tags.append(tag)
    return unique_tags, notes


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="YM=F trade analyst")
    parser.add_argument("--reset-circuit-breaker", action="store_true", help="手動解除風控斷路器")
    return parser.parse_args()


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

    summary = f"Last {total} closed trades winrate={winrate:.1f}%. " + " ".join([f"[{i}]" for i in items])
    return winrate, items, summary


def _parse_db_ts_to_taipei(ts: Any) -> Optional[datetime]:
    if not ts:
        return None
    s = str(ts).strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        return None
    tz_tpe = ZoneInfo("Asia/Taipei")
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc).astimezone(tz_tpe)
    return dt.astimezone(tz_tpe)


def get_monthly_progress(conn: sqlite3.Connection, now_tpe: datetime) -> MonthlyProgress:
    rows = conn.execute(
        """
        SELECT closed_at, pnl
        FROM trades
        WHERE status IN ('WIN','LOSS','CLOSED') AND closed_at IS NOT NULL
        """
    ).fetchall()

    month_start = now_tpe.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if now_tpe.month == 12:
        month_end = now_tpe.replace(year=now_tpe.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        month_end = now_tpe.replace(month=now_tpe.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)

    current_pnl = 0.0
    for closed_at, pnl in rows:
        dt_tpe = _parse_db_ts_to_taipei(closed_at)
        if dt_tpe is None:
            continue
        if month_start <= dt_tpe < month_end:
            current_pnl += safe_float(pnl, 0.0)

    achievement_pct = (current_pnl / MONTHLY_TARGET_POINTS * 100.0) if MONTHLY_TARGET_POINTS else 0.0
    remaining = MONTHLY_TARGET_POINTS - current_pnl
    return MonthlyProgress(current_pnl=float(current_pnl), remaining=float(remaining), achievement_pct=float(achievement_pct))


def get_recent_closed_stats(conn: sqlite3.Connection, lookback: int = STABLE_WINRATE_LOOKBACK) -> Dict[str, Any]:
    rows = conn.execute(
        """
        SELECT status, pnl
        FROM trades
        WHERE status IN ('WIN','LOSS','CLOSED') AND closed_at IS NOT NULL
        ORDER BY datetime(closed_at) DESC
        LIMIT ?
        """,
        (int(max(1, lookback)),),
    ).fetchall()

    if not rows:
        return {"count": 0, "winrate": 0.0, "stable": False}

    wins = 0
    for st, pnl in rows:
        st_u = str(st or "").upper()
        pnl_v = safe_float(pnl, 0.0)
        if st_u == "WIN" or (st_u == "CLOSED" and pnl_v > 0):
            wins += 1

    count = len(rows)
    winrate = (wins / count) * 100.0 if count else 0.0
    stable = count >= min(lookback, 3) and winrate >= STABLE_WINRATE_THRESHOLD
    return {"count": count, "winrate": float(winrate), "stable": bool(stable)}


def get_recent_performance_anomaly(conn: sqlite3.Connection) -> Dict[str, Any]:
    rows = conn.execute(
        """
        SELECT status, pnl
        FROM trades
        WHERE status IN ('WIN','LOSS','CLOSED') AND closed_at IS NOT NULL
        ORDER BY datetime(closed_at) DESC
        LIMIT 30
        """
    ).fetchall()
    if len(rows) < 8:
        return {"abnormal": False, "reason": "insufficient_history"}

    def stat(seg):
        wins = 0
        total_pnl = 0.0
        for st, pnl in seg:
            st_u = str(st or "").upper()
            pnl_v = safe_float(pnl, 0.0)
            if st_u == "WIN" or (st_u == "CLOSED" and pnl_v > 0):
                wins += 1
            total_pnl += pnl_v
        cnt = len(seg)
        return (wins / cnt * 100.0 if cnt else 0.0), (total_pnl / cnt if cnt else 0.0)

    recent = rows[:5]
    baseline = rows[5:25] if len(rows) >= 15 else rows[5:]
    rw, rexp = stat(recent)
    bw, bexp = stat(baseline)

    abnormal = (bw - rw >= 20.0) or (rexp < 0 and bexp > 0)
    reason = f"recent_winrate={rw:.1f} baseline_winrate={bw:.1f} recent_exp={rexp:.2f} baseline_exp={bexp:.2f}"
    return {"abnormal": abnormal, "reason": reason, "recent_winrate": rw, "baseline_winrate": bw, "recent_expectancy": rexp, "baseline_expectancy": bexp}


def get_last_logged_active_contract() -> Optional[str]:
    if not TRADE_LOG_JSON.exists():
        return None
    try:
        with TRADE_LOG_JSON.open("r", encoding="utf-8") as f:
            arr = json.load(f)
        if isinstance(arr, list) and arr:
            return str(arr[-1].get("active_contract") or "").strip() or None
    except Exception:
        return None
    return None


def recent_three_closed_all_loss(conn: sqlite3.Connection) -> bool:
    rows = conn.execute(
        """
        SELECT status, pnl
        FROM trades
        WHERE status IN ('WIN','LOSS','CLOSED') AND closed_at IS NOT NULL
        ORDER BY datetime(closed_at) DESC
        LIMIT 3
        """
    ).fetchall()

    if len(rows) < 3:
        return False

    for st, pnl in rows:
        st_u = str(st or "").upper()
        pnl_v = safe_float(pnl, 0.0)
        is_loss = st_u == "LOSS" or (st_u == "CLOSED" and pnl_v < 0)
        if not is_loss:
            return False
    return True


def build_strategy_mode_context(monthly: MonthlyProgress, recent_stats: Dict[str, Any]) -> StrategyModeContext:
    stable = bool(recent_stats.get("stable", False))

    # 防守模式：已達標或接近達標（>=90%）
    if monthly.current_pnl >= MONTHLY_TARGET_POINTS or monthly.achievement_pct >= DEFENSIVE_PROGRESS_THRESHOLD:
        return StrategyModeContext(
            mode="Defensive",
            context=(
                "月目標已達成或接近，請優先風險過濾與鎖利，避免回吐；"
                "若訊號品質不足，傾向 HOLD。"
            ),
        )

    # 進攻模式：進度仍落後，但近期勝率穩定
    if monthly.achievement_pct < DEFENSIVE_PROGRESS_THRESHOLD and stable:
        return StrategyModeContext(
            mode="Aggressive",
            context=(
                "月目標進度落後但近期勝率穩定，可稍微放寬進場條件以捕捉更多波動；"
                "仍需維持合理停損紀律。"
            ),
        )

    return StrategyModeContext(
        mode="Balanced",
        context="在風險與機會間維持中性配置，依訊號品質決定 BUY/SELL/HOLD。",
    )


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


def _get_requests_module():
    try:
        import requests  # type: ignore

        return requests
    except ImportError:
        return None


def _summarize_error_message(msg: str, limit: int = 220) -> str:
    clean = " ".join(str(msg or "").split())
    return clean[:limit]


def _is_retryable_gemini_http(status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code <= 599


def _is_retryable_gemini_exception(exc: Exception) -> bool:
    name = exc.__class__.__name__.lower()
    msg = str(exc).lower()
    retryable_hints = [
        "timeout",
        "timed out",
        "temporarily unavailable",
        "connection reset",
        "connection aborted",
        "connection refused",
        "name resolution",
        "dns",
        "ssl",
        "readtimeout",
        "connecttimeout",
    ]
    return ("timeout" in name) or any(h in msg for h in retryable_hints)


def normalize_risk(parsed: Dict[str, Any], s: Snapshot, degraded: bool = False) -> Dict[str, Any]:
    fallback_score = 5 + (1 if abs(s.bias_ema20) > 0.8 else 0) + (1 if abs(s.bias_ema50) > 1.2 else 0)
    risk_level = int(clamp(round(safe_float(parsed.get("risk_level"), fallback_score)), 0, 10))

    vol_raw = parsed.get("volatility_flag", "normal")
    vol = "normal"
    if isinstance(vol_raw, bool):
        vol = "high" if vol_raw else "normal"
    elif isinstance(vol_raw, str):
        v = vol_raw.strip().lower()
        if v in {"high", "true", "1"}:
            vol = "high"
        elif v in {"normal", "false", "0", "low"}:
            vol = "normal"
        else:
            degraded = True
    else:
        degraded = True

    return {
        "risk_level": risk_level,
        "volatility_flag": vol,
        "degraded": degraded,
    }


def step1_groq_risk_check(s: Snapshot, mode_ctx: StrategyModeContext, monthly: MonthlyProgress) -> Dict[str, Any]:
    key = (os.getenv("GROQ_API_KEY") or "").strip()
    requests = _get_requests_module()

    fallback = {
        "risk_level": int(clamp(round(4 + abs(s.bias_ema20) * 0.6 + abs(s.bias_ema50) * 0.4), 0, 10)),
        "volatility_flag": "high" if abs(s.bias_ema20) > 1.2 or abs(s.bias_ema50) > 1.5 else "normal",
        "risk_notes": ["fallback_risk_model"],
    }

    if not key or requests is None:
        reason = "missing GROQ_API_KEY" if not key else "missing requests dependency"
        return {"status": "fallback", "reason": reason, "raw_json": fallback, "normalized": normalize_risk(fallback, s, True)}

    prompt = (
        "You are a futures risk sentinel. Output strict JSON only. "
        "Required keys: risk_level(0-10 int), volatility_flag(high/normal). "
        "Optional: risk_notes(array).\n"
        "If mode is Defensive, strengthen risk filters and profit-protection to avoid giving back gains.\n"
        f"Mode: {mode_ctx.mode}, Context: {mode_ctx.context}\n"
        f"MonthlyProgress: current_pnl={monthly.current_pnl:.2f}, target={MONTHLY_TARGET_POINTS:.0f}, achievement_pct={monthly.achievement_pct:.2f}, remaining={monthly.remaining:.2f}\n"
        f"Snapshot: {json.dumps(s.__dict__, ensure_ascii=False)}"
    )
    payload = {
        "model": GROQ_MODEL,
        "temperature": 0.1,
        "messages": [
            {"role": "system", "content": "Strict JSON output only."},
            {"role": "user", "content": prompt},
        ],
        "response_format": {"type": "json_object"},
    }

    try:
        resp = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json=payload,
            timeout=20,
        )
        if resp.status_code != 200:
            return {
                "status": "fallback",
                "reason": f"Groq HTTP {resp.status_code}",
                "raw_json": fallback,
                "normalized": normalize_risk(fallback, s, True),
            }
        text = resp.json().get("choices", [{}])[0].get("message", {}).get("content", "")
        parsed = extract_json_object(text)
        if not parsed:
            parsed = fallback
            return {"status": "degraded", "reason": "non-json response", "raw_json": parsed, "normalized": normalize_risk(parsed, s, True)}
        return {"status": "ok", "reason": "", "raw_json": parsed, "normalized": normalize_risk(parsed, s, False)}
    except Exception as exc:
        return {"status": "fallback", "reason": f"Groq exception: {exc}", "raw_json": fallback, "normalized": normalize_risk(fallback, s, True)}


def step2_gemini_strategy(
    s: Snapshot,
    winrate: float,
    history_summary: str,
    groq_norm: Dict[str, Any],
    monthly: MonthlyProgress,
    mode_ctx: StrategyModeContext,
    force_goal_optimization: bool,
    gemini_cooldown_state: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    key = (os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or "").strip()
    requests = _get_requests_module()

    risk_level = int(groq_norm.get("risk_level", 5))
    vol_flag = str(groq_norm.get("volatility_flag", "normal"))

    def fallback(reason: str, degrade_cause: Optional[str] = None) -> Dict[str, Any]:
        score = 50
        if s.close > s.ema20 > s.ema50:
            score += 15
        elif s.close < s.ema20 < s.ema50:
            score -= 15
        score += 8 if s.macd > s.macd_signal else -8
        if s.rsi14 > 70:
            score -= 8
        elif s.rsi14 < 30:
            score += 8
        if risk_level >= 7:
            score = max(35, min(65, score))

        action = "HOLD"
        if score >= 60 and risk_level <= 8:
            action = "BUY"
        elif score <= 40 and risk_level <= 8:
            action = "SELL"

        band = max(s.close * 0.002, 25.0)
        if action == "BUY":
            sl, tp = s.close - band, s.close + band * 1.6
        elif action == "SELL":
            sl, tp = s.close + band, s.close - band * 1.6
        else:
            sl, tp = s.close - band, s.close + band

        proposal = None
        if winrate < 45:
            proposal = {
                "skill_name": "market-context-sensor",
                "reason": "近期勝率偏低，建議加入 VIX/新聞情緒作為濾網以降低假突破。",
            }

        optimization_text = (
            "連三虧後先縮減可交易情境：僅在 EMA20/EMA50 同向且 MACD 同向時進場，"
            "停損縮至原先 0.8 倍、單筆報酬風險比需 >=1.8；"
            "若本月距 1000 點仍落後，改採分批出場先鎖 50% 利潤，避免再次回吐。"
            if force_goal_optimization
            else "依近期勝率動態調整進場過濾條件與停損帶寬。"
        )
        payload = {
            "sentiment_score": round(clamp(score, 0, 100) / 100.0, 4),
            "reflection_one_liner": "Gemini fallback：已整合風險哨兵與微觀特徵進行保守決策。",
            "action": action,
            "entry": round(s.close, 2),
            "sl": round(sl, 2),
            "tp": round(tp, 2),
            "reason": (
                f"fallback_strategy(reason={reason}, risk_level={risk_level}, volatility={vol_flag}, "
                f"mode={mode_ctx.mode}, achievement={monthly.achievement_pct:.2f}%, history={history_summary})"
            ),
            "optimization_suggestion": optimization_text,
            "new_skill_proposal": proposal,
        }
        return {
            "status": "fallback",
            "reason": reason,
            "raw_json": payload,
            "degraded": bool(degrade_cause),
            "degrade_cause": degrade_cause or "",
            "trigger_cooldown": False,
            "cooldown_seconds": 0,
        }

    cooldown_state = gemini_cooldown_state or {}
    if bool(cooldown_state.get("active")):
        until = str(cooldown_state.get("until") or "")
        reason = str(cooldown_state.get("reason") or "gemini_429_cooldown")
        append_cron_trade_log(
            f"gemini_skip_during_cooldown until={until} reason={reason}"
        )
        return fallback(f"Gemini cooldown active until {until}", degrade_cause="gemini_cooldown")

    if (os.getenv("TRADE_ANALYST_SIMULATE_GEMINI_429") or "").strip() == "1":
        append_cron_trade_log("gemini_error status=429 error_type=simulated attempt=0/0 message=TRADE_ANALYST_SIMULATE_GEMINI_429")
        cooldown_seconds = int((os.getenv("TRADE_ANALYST_GEMINI_COOLDOWN_SECONDS") or "900").strip() or 900)
        return fallback("Gemini HTTP 429 (simulated)", degrade_cause="gemini_429") | {
            "trigger_cooldown": True,
            "cooldown_seconds": max(60, cooldown_seconds),
        }

    if not key or requests is None:
        reason = "missing GEMINI_API_KEY/GOOGLE_API_KEY" if not key else "missing requests dependency"
        append_cron_trade_log(
            f"gemini_error status=NA error_type=precheck_non_retryable attempt=0/0 message={reason}"
        )
        return fallback(reason)

    optimization_requirement = (
        "optimization_suggestion 欄位必填，且需針對『月獲利1000點目標』提供可執行修正方案（不得空白、不得泛談）。"
        if force_goal_optimization
        else "optimization_suggestion 欄位建議提供簡短優化方向。"
    )

    prompt = (
        "你是『道瓊 20 年華爾街量化策略師』，專做 YM=F 15m。請只輸出單一 JSON。\n"
        "必要欄位: sentiment_score(0~1 float), reflection_one_liner, action(BUY/SELL/HOLD), entry, sl, tp, reason, optimization_suggestion, new_skill_proposal(null或{skill_name,reason})。\n"
        "核心策略約束(必須明確納入 reason):\n"
        "1) Price Action：重點看 15m candle_body、wick_ratio，特別留意整數關卡的假突破/回踩。\n"
        "2) bias_ema50 過大時，優先考慮均值回歸逆勢。\n"
        "3) candle_body 連續縮小後的突破，視為趨勢啟動線索。\n"
        "4) 趨勢過濾：EMA20>EMA50 且斜率上行才偏多；若 RSI 在 30~70 震盪帶，偏高拋低吸。\n"
        "5) 勝率優先，且 RR 必須 > 1.5；若不滿足則 HOLD。\n"
        "6) 若連續虧損，必須檢視市場風格是否切換並降低進攻性。\n"
        "7) 持續評估是否需要成分股權重/VIX 資訊，必要時於 new_skill_proposal 提案。\n"
        "你必須結合：K線+微觀特徵(candle_body/upper_wick_ratio/lower_wick_ratio/bias_ema20/bias_ema50)+Groq風險訊號。\n"
        f"策略模式: mode={mode_ctx.mode}, context={mode_ctx.context}\n"
        f"月目標進度: current_pnl={monthly.current_pnl:.2f}, target={MONTHLY_TARGET_POINTS:.0f}, achievement_pct={monthly.achievement_pct:.2f}, remaining={monthly.remaining:.2f}\n"
        f"{optimization_requirement}\n"
        f"Groq normalized: {json.dumps(groq_norm, ensure_ascii=False)}\n"
        f"winrate={winrate:.1f}, history={history_summary}\n"
        f"snapshot={json.dumps(s.__dict__, ensure_ascii=False)}"
    )

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.15, "maxOutputTokens": 600},
    }

    max_attempts = 4
    request_timeout_seconds = 45
    base_backoff_seconds = 1.2
    last_error_reason = ""

    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.post(url, params={"key": key}, json=payload, timeout=request_timeout_seconds)
            status = int(getattr(resp, "status_code", 0) or 0)

            if status == 200:
                text = (
                    resp.json().get("candidates", [{}])[0]
                    .get("content", {})
                    .get("parts", [{}])[0]
                    .get("text", "")
                )
                parsed = extract_json_object(text)
                if not parsed:
                    reason = "Gemini non-json response"
                    append_cron_trade_log(
                        f"gemini_error status=200 error_type=non_json attempt={attempt}/{max_attempts} message={reason}"
                    )
                    return fallback(reason)
                return {"status": "ok", "reason": "", "raw_json": parsed}

            body_msg = ""
            try:
                body_msg = _summarize_error_message(resp.text)
            except Exception:
                body_msg = ""

            error_type = "http_retryable" if _is_retryable_gemini_http(status) else "http_non_retryable"
            last_error_reason = f"Gemini HTTP {status}"
            append_cron_trade_log(
                f"gemini_error status={status} error_type={error_type} attempt={attempt}/{max_attempts} message={body_msg or last_error_reason}"
            )

            if _is_retryable_gemini_http(status) and attempt < max_attempts:
                sleep_s = base_backoff_seconds * (2 ** (attempt - 1)) + random.uniform(0.0, 0.6)
                time.sleep(sleep_s)
                continue

            if status == 429:
                cooldown_seconds = int((os.getenv("TRADE_ANALYST_GEMINI_COOLDOWN_SECONDS") or "900").strip() or 900)
                return fallback(last_error_reason, degrade_cause="gemini_429") | {
                    "trigger_cooldown": True,
                    "cooldown_seconds": max(60, cooldown_seconds),
                }

            return fallback(last_error_reason)
        except Exception as exc:
            retryable = _is_retryable_gemini_exception(exc)
            error_type = "exception_retryable" if retryable else "exception_non_retryable"
            last_error_reason = f"Gemini exception: {exc}"
            append_cron_trade_log(
                f"gemini_error status=NA error_type={error_type} attempt={attempt}/{max_attempts} message={_summarize_error_message(str(exc))}"
            )

            if retryable and attempt < max_attempts:
                sleep_s = base_backoff_seconds * (2 ** (attempt - 1)) + random.uniform(0.0, 0.6)
                time.sleep(sleep_s)
                continue
            return fallback(last_error_reason)

    return fallback(last_error_reason or "Gemini retry exhausted")


def normalize_plan_payload(payload: Dict[str, Any], s: Snapshot) -> Plan:
    sentiment = normalize_sentiment_0_1(payload.get("sentiment_score"), 0.5)
    action = normalize_action_external(payload.get("action", "HOLD"))

    entry = round(safe_float(payload.get("entry"), s.close), 2)
    sl = round(safe_float(payload.get("sl"), s.close), 2)
    tp = round(safe_float(payload.get("tp"), s.close), 2)

    reflection = str(payload.get("reflection_one_liner", "N/A"))[:220]
    reason = str(payload.get("reason", "N/A"))[:900]

    proposal = None
    raw = payload.get("new_skill_proposal")
    if isinstance(raw, dict):
        name = str(raw.get("skill_name", "")).strip()
        rs = str(raw.get("reason", "")).strip()
        if name and rs:
            proposal = {"skill_name": name[:80], "reason": rs[:300]}

    normalized_payload = {
        "sentiment_score": sentiment,
        "action": action,
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "reason": reason,
        "new_skill_proposal": proposal,
        "reflection_one_liner": reflection,
    }

    return Plan(
        sentiment_score=sentiment,
        reflection_one_liner=reflection,
        action=action,
        entry=entry,
        sl=sl,
        tp=tp,
        reason=reason,
        raw=json.dumps(normalized_payload, ensure_ascii=False),
        new_skill_proposal=proposal,
    )


def step3_openai_arbitrate(s: Snapshot, groq_norm: Dict[str, Any], gemini_payload: Dict[str, Any]) -> Tuple[Plan, Dict[str, Any]]:
    key = (os.getenv("OPENAI_API_KEY") or "").strip()
    requests = _get_requests_module()

    def fallback(reason: str) -> Tuple[Plan, Dict[str, Any]]:
        p = normalize_plan_payload(gemini_payload, s)
        risk_level = int(groq_norm.get("risk_level", 5))
        vol = str(groq_norm.get("volatility_flag", "normal"))

        # deterministic risk arbitration while preserving same output schema
        if p.action == "BUY" and (risk_level >= 9 or vol == "high") and p.sentiment_score < 0.55:
            p.action = "HOLD"
        if p.action == "SELL" and (risk_level >= 9 or vol == "high") and p.sentiment_score > 0.45:
            p.action = "HOLD"

        if p.action == "HOLD":
            band = max(s.close * 0.0015, 20.0)
            p.entry = round(s.close, 2)
            p.sl = round(s.close - band, 2)
            p.tp = round(s.close + band, 2)

        p.reason = f"{p.reason} | openai_arbitration=fallback({reason})"
        p.raw = json.dumps(
            {
                "sentiment_score": p.sentiment_score,
                "reflection_one_liner": p.reflection_one_liner,
                "action": p.action,
                "entry": p.entry,
                "sl": p.sl,
                "tp": p.tp,
                "reason": p.reason,
                "new_skill_proposal": p.new_skill_proposal,
                "arbitration": {"status": "fallback", "reason": reason},
            },
            ensure_ascii=False,
        )
        return p, {"status": "fallback", "reason": reason}

    if not key or requests is None:
        return fallback("missing OPENAI_API_KEY" if not key else "missing requests dependency")

    system_prompt = (
        "You are the final risk arbitrator and must act as a Dow 20-year Wall Street quant strategist. "
        "Return strict JSON only with keys: sentiment_score(0~1), reflection_one_liner, action(BUY/SELL/HOLD), "
        "entry, sl, tp, reason, new_skill_proposal(null or {skill_name,reason}). "
        "Enforce strategy constraints: 15m price action(candle_body/wick_ratio/integer-level false break-retest), "
        "mean-reversion priority when bias_ema50 is too large, shrinking candle_body breakout clue, trend filter "
        "(EMA20>EMA50 and upward slope => long bias only), range regime(RSI 30~70 => buy low sell high), "
        "win-rate first with RR>1.5, and force market-regime-switch review after consecutive losses. "
        "Always consider whether component-weight/VIX context should be proposed in new_skill_proposal."
    )
    user_prompt = (
        f"Groq normalized risk: {json.dumps(groq_norm, ensure_ascii=False)}\n"
        f"Gemini candidate decision: {json.dumps(gemini_payload, ensure_ascii=False)}\n"
        f"Snapshot: {json.dumps(s.__dict__, ensure_ascii=False)}\n"
        "請做最終風險審核與 JSON 標準化，輸出單一 JSON。"
    )
    payload = {
        "model": OPENAI_MODEL,
        "temperature": 0.1,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "response_format": {"type": "json_object"},
    }

    try:
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json=payload,
            timeout=30,
        )
        if resp.status_code != 200:
            return fallback(f"OpenAI HTTP {resp.status_code}")

        text = resp.json().get("choices", [{}])[0].get("message", {}).get("content", "")
        parsed = extract_json_object(text)
        if not parsed:
            return fallback("OpenAI non-json response")

        plan = normalize_plan_payload(parsed, s)
        plan.raw = text
        return plan, {"status": "ok", "reason": ""}
    except Exception as exc:
        return fallback(f"OpenAI exception: {exc}")


def has_open_trade(conn: sqlite3.Connection) -> bool:
    row = conn.execute("SELECT 1 FROM trades WHERE status='OPEN' LIMIT 1").fetchone()
    return row is not None


def maybe_open_trade(conn: sqlite3.Connection, p: Plan, symbol: str, readonly_mode: bool = False) -> bool:
    if readonly_mode:
        return False

    if has_open_trade(conn):
        return False

    side = external_to_internal_side(p.action)
    if side not in {"LONG", "SHORT"}:
        return False

    conn.execute(
        """
        INSERT INTO trades (
            symbol, opened_at, side, entry_price, sl, tp, reason, status,
            ai_reflection, ai_plan_raw
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'OPEN', ?, ?)
        """,
        (symbol, now_iso(), side, p.entry, p.sl, p.tp, p.reason, p.reflection_one_liner, p.raw),
    )
    conn.commit()
    return True


def telegram_summary(
    winrate: float,
    reflection: str,
    trend: str,
    p: Plan,
    monthly: MonthlyProgress,
    active_contract: str,
    rollover_status: str,
    consultant_tags: Optional[List[str]] = None,
    quota_degraded: bool = False,
) -> str:
    msg = (
        "📊 道瓊期貨 AI 交易員 (15m)\n"
        f"🧠 自我檢討：{winrate:.1f}% - {reflection}\n"
        f"📈 本月進度：{monthly.current_pnl:.2f} / {MONTHLY_TARGET_POINTS:.0f} 點 ({monthly.achievement_pct:.1f}%)\n"
        f"📈 當前盤勢：{trend}\n"
        f"🧾 Active Contract：{active_contract} ({rollover_status})\n"
        f"🤖 最新計畫：{p.action} (Entry: {p.entry:.2f}, SL: {p.sl:.2f}, TP: {p.tp:.2f})"
    )
    if quota_degraded:
        msg += "\n⚠️ 本次因配額降級：Gemini 不可用，已自動改走 Groq + 規則引擎。"
    if p.new_skill_proposal:
        msg += (
            "\n🧪 [研發提案]\n"
            f"- Skill: {p.new_skill_proposal.get('skill_name', '(unknown)')}\n"
            f"- 理由: {p.new_skill_proposal.get('reason', '(no reason)')}"
        )
    if consultant_tags:
        msg += "\n\n📌 Consultant Routing 建議：\n" + "\n".join(consultant_tags)
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
    args = parse_args()
    if (os.getenv("TRADE_ANALYST_TIME_GUARD_SELFTEST") or "").strip() == "1":
        _run_time_guard_selftest()
        print("[info] Time guard selftest passed")
        return 0

    if (os.getenv("TRADE_ANALYST_HOLIDAY_GUARD_SELFTEST") or "").strip() == "1":
        _run_holiday_guard_selftest()
        print("[info] Holiday guard selftest passed")
        return 0

    # --- Time Guard must stay at the very front ---
    now_tpe = get_taipei_now((os.getenv("TRADE_ANALYST_NOW") or "").strip() or None)
    if is_futures_market_closed_taipei(now_tpe):
        print("[Info] 目前為期貨休市時間 (每日保養或週末)，暫停分析與交易。")
        sys.exit(0)

    holidays_module = ensure_holidays_module()
    holiday_name = get_us_holiday_name_from_taipei(now_tpe, holidays_module=holidays_module)
    if holiday_name:
        print(f"[Info] 今日為美國國定假日 ({holiday_name})，期貨市場可能休市或提早收盤，暫停分析與交易。")
        sys.exit(0)

    yf = ensure_deps()
    rollover_decision = detect_rollover_and_active_contract(yf, now_tpe)
    indicator_t0 = time.perf_counter()
    s = fetch_snapshot(yf, rollover_decision)
    indicator_calc_seconds = time.perf_counter() - indicator_t0

    prev_active_contract = get_last_logged_active_contract()
    rollover_status = "ROLLED" if rollover_decision.switched_to_far else "NEAR"

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    perf_anomaly: Dict[str, Any] = {"abnormal": False, "reason": "not_evaluated"}
    quota_degraded = False
    gemini_degrade_cause = ""
    gemini_cooldown_state: Dict[str, Any] = {"active": False, "until": None, "reason": "", "set_at": None}
    conn = sqlite3.connect(DB_PATH)
    try:
        ensure_db(conn)
        ensure_risk_tables(conn)

        # Gemini cooldown lifecycle (persistent in SQLite risk_state)
        gemini_cooldown_state = get_gemini_cooldown_state(conn)
        if (not gemini_cooldown_state.get("active")) and gemini_cooldown_state.get("until"):
            clear_gemini_cooldown(conn, end_reason="expired")
            gemini_cooldown_state = get_gemini_cooldown_state(conn)

        today_tpe = now_tpe.strftime("%Y-%m-%d")
        cb_state = get_circuit_breaker_state(conn)
        manual_reset_done = False

        if args.reset_circuit_breaker:
            set_circuit_breaker_state(conn, active=False, reason="manual_reset", active_date=today_tpe, triggered_at=None)
            manual_reset_done = True
            send_telegram("🟢 [風控解除] 已手動解除 CIRCUIT_BREAKER，恢復可開倉模式。")
            cb_state = get_circuit_breaker_state(conn)

        if cb_state.get("active") and cb_state.get("active_date") != today_tpe:
            set_circuit_breaker_state(conn, active=False, reason="auto_daily_reset", active_date=today_tpe, triggered_at=None)
            send_telegram("🟢 [風控重置] CIRCUIT_BREAKER 已於新交易日自動重置，恢復可開倉模式。")
            cb_state = get_circuit_breaker_state(conn)

        closed = settle_open_trades(conn, s.close)
        winrate, _, history_summary = get_reflection(conn)

        daily_pnl, losing_streak = compute_daily_pnl_and_losing_streak(conn, today_tpe)
        upsert_risk_daily_stats(conn, today_tpe, daily_pnl, losing_streak)

        trigger_reason = evaluate_circuit_breaker(daily_pnl, losing_streak)
        if trigger_reason and not cb_state.get("active"):
            set_circuit_breaker_state(
                conn,
                active=True,
                reason=trigger_reason,
                active_date=today_tpe,
                triggered_at=now_iso(),
            )
            send_telegram(
                "🛑 [緊急停手] CIRCUIT_BREAKER 已啟動，立即停止新開倉/下單，僅允許監控、結算與報告。\n"
                f"觸發條件：{trigger_reason}"
            )
            cb_state = get_circuit_breaker_state(conn)

        monthly_progress = get_monthly_progress(conn, now_tpe)
        recent_stats = get_recent_closed_stats(conn, STABLE_WINRATE_LOOKBACK)
        mode_ctx = build_strategy_mode_context(monthly_progress, recent_stats)
        force_goal_optimization = recent_three_closed_all_loss(conn)

        # Tri-brain orchestration
        groq_result = step1_groq_risk_check(s, mode_ctx, monthly_progress)
        gemini_result = step2_gemini_strategy(
            s,
            winrate,
            history_summary,
            groq_result.get("normalized", {}),
            monthly_progress,
            mode_ctx,
            force_goal_optimization,
            gemini_cooldown_state=gemini_cooldown_state,
        )

        if bool(gemini_result.get("trigger_cooldown")):
            cooldown_seconds = int(gemini_result.get("cooldown_seconds") or 900)
            cooldown_until = datetime.now(timezone.utc) + timedelta(seconds=max(60, cooldown_seconds))
            set_gemini_cooldown(
                conn,
                until=cooldown_until,
                reason=str(gemini_result.get("reason") or "gemini_429"),
            )
            gemini_cooldown_state = get_gemini_cooldown_state(conn)

        quota_degraded = bool(gemini_result.get("degraded"))
        gemini_degrade_cause = str(gemini_result.get("degrade_cause") or "")

        plan, openai_result = step3_openai_arbitrate(s, groq_result.get("normalized", {}), gemini_result.get("raw_json", {}))

        prior_trade_status = get_prior_trade_status(conn, s.close)
        readonly_mode = bool(get_circuit_breaker_state(conn).get("active"))
        opened = maybe_open_trade(conn, plan, rollover_decision.active_contract, readonly_mode=readonly_mode)
        cb_state = get_circuit_breaker_state(conn)
        perf_anomaly = get_recent_performance_anomaly(conn)
    finally:
        conn.close()

    trend = trend_summary(s)

    if rollover_decision.switched_to_far and rollover_decision.active_contract != (prev_active_contract or ""):
        send_telegram(f"🔄 [換倉警報]：主力資金已轉向 {rollover_decision.active_contract}，系統已自動同步切換。")

    expected_profit_points = plan.tp - plan.entry if plan.action != "SELL" else plan.entry - plan.tp
    optimization_suggestion = str(gemini_result.get("raw_json", {}).get("optimization_suggestion", "")).strip()
    if not optimization_suggestion:
        if force_goal_optimization:
            optimization_suggestion = (
                "規則建議：連三虧後，接下來 3 筆交易僅允許趨勢同向訊號；"
                "單筆風險降至平常 70%，達到 +0.8R 即移動停損保本，"
                "並以『月獲利1000點』差距優先排序高品質機會，避免低勝率過度交易。"
            )
        else:
            optimization_suggestion = "依近期勝率動態調整進場過濾條件與停損帶寬。"

    tri_brain_status = {
        "groq": {"status": groq_result.get("status"), "reason": groq_result.get("reason"), "normalized": groq_result.get("normalized")},
        "gemini": {
            "status": gemini_result.get("status"),
            "reason": gemini_result.get("reason"),
            "degraded": quota_degraded,
            "degrade_cause": gemini_degrade_cause,
            "cooldown": gemini_cooldown_state,
        },
        "openai": {"status": openai_result.get("status"), "reason": openai_result.get("reason")},
    }
    risk_control = {
        "daily_pnl": daily_pnl,
        "losing_streak": losing_streak,
        "circuit_breaker_active": cb_state.get("active"),
        "circuit_breaker_reason": cb_state.get("reason"),
        "circuit_breaker_active_date": cb_state.get("active_date"),
        "manual_reset_done": manual_reset_done,
        "gemini_cooldown": gemini_cooldown_state,
        "quota_degraded": quota_degraded,
    }

    consultant_tags, consultant_notes = build_consultant_routing(
        risk_control=risk_control,
        error_review=str(plan.reflection_one_liner),
        tri_brain_status=tri_brain_status,
        new_skill_proposal=plan.new_skill_proposal,
        optimization_suggestion=optimization_suggestion,
        indicator_calc_seconds=indicator_calc_seconds,
        in_rollover_week=rollover_decision.in_rollover_week,
        perf_anomaly=perf_anomaly,
    )

    tg_state = send_telegram(
        telegram_summary(
            winrate,
            plan.reflection_one_liner,
            trend,
            plan,
            monthly_progress,
            active_contract=rollover_decision.active_contract,
            rollover_status=rollover_status,
            consultant_tags=consultant_tags,
            quota_degraded=quota_degraded,
        )
    )

    log_record: Dict[str, Any] = {
        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "active_contract": rollover_decision.active_contract,
        "rollover": {
            "near_contract": rollover_decision.near_contract,
            "far_contract": rollover_decision.far_contract,
            "near_volume": rollover_decision.near_volume,
            "far_volume": rollover_decision.far_volume,
            "switched_to_far": rollover_decision.switched_to_far,
            "in_rollover_week": rollover_decision.in_rollover_week,
            "decision_reason": rollover_decision.reason,
            "adjustment_method": rollover_decision.adjustment_method,
            "status": rollover_status,
        },
        "entry_price": float(plan.entry),
        "take_profit_price": float(plan.tp),
        "stop_loss_price": float(plan.sl),
        "expected_profit_points": float(expected_profit_points),
        "analysis_reasoning": str(plan.reason),
        "error_review": str(plan.reflection_one_liner),
        "optimization_suggestion": optimization_suggestion,
        "new_skill_proposal": plan.new_skill_proposal,
        "prior_trade_status": prior_trade_status,
        "monthly_progress": {
            "target_points": MONTHLY_TARGET_POINTS,
            "current_pnl": monthly_progress.current_pnl,
            "remaining": monthly_progress.remaining,
            "achievement_pct": monthly_progress.achievement_pct,
        },
        "risk_control": risk_control,
        "strategy_mode": {
            "mode": mode_ctx.mode,
            "context": mode_ctx.context,
            "recent_winrate": recent_stats.get("winrate"),
            "recent_count": recent_stats.get("count"),
            "recent_stable": recent_stats.get("stable"),
            "force_goal_optimization": force_goal_optimization,
        },
        "tri_brain_status": tri_brain_status,
        "consultant_tags": consultant_tags,
        "consultant_notes": consultant_notes,
        "performance_anomaly": perf_anomaly,
        "indicator_calc_seconds": float(indicator_calc_seconds),
    }
    log_count, json_path, csv_path = append_trade_log_and_export_csv(log_record)

    print(f"[info] Symbol={SYMBOL} active_contract={rollover_decision.active_contract} interval={INTERVAL} ts={s.ts}")
    print(
        f"[info] Rollover: status={rollover_status} near={rollover_decision.near_contract}({rollover_decision.near_volume}) "
        f"far={rollover_decision.far_contract}({rollover_decision.far_volume}) reason={rollover_decision.reason}"
    )
    print(
        "[info] Indicators: "
        f"Close={s.close:.2f} EMA20={s.ema20:.2f} EMA50={s.ema50:.2f} RSI14={s.rsi14:.2f} "
        f"MACD={s.macd:.3f}/{s.macd_signal:.3f} Body={s.candle_body:.2f} "
        f"UpperWick={s.upper_wick_ratio:.3f} LowerWick={s.lower_wick_ratio:.3f} "
        f"BiasEMA20={s.bias_ema20:.2f}% BiasEMA50={s.bias_ema50:.2f}%"
    )
    print(f"[info] Reflection winrate(last5)={winrate:.1f}%")
    print(
        f"[info] Monthly progress: pnl={monthly_progress.current_pnl:.2f}/{MONTHLY_TARGET_POINTS:.0f} "
        f"achievement={monthly_progress.achievement_pct:.2f}% remaining={monthly_progress.remaining:.2f} "
        f"mode={mode_ctx.mode} recent_winrate={safe_float(recent_stats.get('winrate'), 0.0):.1f}%"
    )
    print(f"[info] Groq risk: {json.dumps(groq_result.get('normalized', {}), ensure_ascii=False)}")
    print(f"[info] Gemini degrade: {quota_degraded} cause={gemini_degrade_cause} cooldown={json.dumps(gemini_cooldown_state, ensure_ascii=False)}")
    print(f"[info] Plan: sentiment_score={plan.sentiment_score} action={plan.action} entry={plan.entry:.2f} sl={plan.sl:.2f} tp={plan.tp:.2f}")
    print(f"[info] new_skill_proposal={json.dumps(plan.new_skill_proposal, ensure_ascii=False)}")
    print(f"[info] Risk: daily_pnl={daily_pnl:.2f} losing_streak={losing_streak} circuit_breaker_active={cb_state.get('active')} reason={cb_state.get('reason')}")
    print(f"[info] Trades: settled_open={closed}, opened_new={opened} readonly_mode={cb_state.get('active')}")
    print(f"[info] Consultant routing: tags={json.dumps(consultant_tags, ensure_ascii=False)} notes={json.dumps(consultant_notes, ensure_ascii=False)}")
    print(f"[info] Indicator calc seconds: {indicator_calc_seconds:.3f}")
    print(f"[info] Log export: records={log_count} json={json_path} csv={csv_path}")
    print(f"[info] Telegram: {tg_state}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
