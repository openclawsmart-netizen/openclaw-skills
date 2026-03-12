#!/usr/bin/env python3
"""Lightweight backtest engine for YM=F using trade_analyst-compatible local rules."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]
REPORTS_DIR = BASE_DIR / "reports"
DEFAULT_SYMBOL = "YM=F"


def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backtest engine for trade_analyst")
    p.add_argument("--csv", help="Path to yfinance historical CSV")
    p.add_argument("--symbol", default=DEFAULT_SYMBOL, help="Ticker for optional download")
    p.add_argument("--interval", default="15m", help="yfinance interval for optional download")
    p.add_argument("--period", default="60d", help="yfinance period for optional download")
    p.add_argument("--save-csv", help="When downloading data, optionally save CSV to this path")
    p.add_argument("--start", help="Start date YYYY-MM-DD")
    p.add_argument("--end", help="End date YYYY-MM-DD")
    p.add_argument("--days", type=int, help="Use recent N calendar days")
    p.add_argument("--use-ai", action="store_true", help="Allow AI path (warning only in lightweight engine)")
    p.add_argument("--save-report", action="store_true", help="Save report JSON to reports/backtest_*.json")
    return p.parse_args()


def load_data(args: argparse.Namespace) -> pd.DataFrame:
    if args.csv:
        df = pd.read_csv(args.csv)
    else:
        import yfinance as yf  # type: ignore

        df = yf.download(args.symbol, period=args.period, interval=args.interval, progress=False, auto_adjust=False)
        if hasattr(df, "to_csv") and args.save_csv:
            out = Path(args.save_csv)
            out.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(out)

    if df is None or df.empty:
        raise RuntimeError("No historical data loaded")

    if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1:
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

    if "Datetime" in df.columns:
        df["Datetime"] = pd.to_datetime(df["Datetime"], errors="coerce")
        df = df.set_index("Datetime")
    elif "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.set_index("Date")
    else:
        df.index = pd.to_datetime(df.index, errors="coerce")

    df = df.sort_index()
    return df


def apply_time_filter(df: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    out = df.copy()
    if args.days:
        end_dt = out.index.max()
        start_dt = end_dt - timedelta(days=max(1, args.days))
        out = out[out.index >= start_dt]

    if args.start:
        out = out[out.index >= pd.to_datetime(args.start)]
    if args.end:
        out = out[out.index <= pd.to_datetime(args.end) + timedelta(days=1)]

    return out


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["EMA20"] = out["Close"].ewm(span=20, adjust=False).mean()
    out["EMA50"] = out["Close"].ewm(span=50, adjust=False).mean()

    delta = out["Close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / 14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / 14, adjust=False).mean().replace(0, 1e-9)
    rs = avg_gain / avg_loss
    out["RSI14"] = 100 - (100 / (1 + rs))

    ema12 = out["Close"].ewm(span=12, adjust=False).mean()
    ema26 = out["Close"].ewm(span=26, adjust=False).mean()
    out["MACD"] = ema12 - ema26
    out["MACD_SIGNAL"] = out["MACD"].ewm(span=9, adjust=False).mean()

    out = out.dropna().copy()
    return out


def local_signal(row: pd.Series) -> str:
    bull = row["Close"] > row["EMA20"] > row["EMA50"] and row["MACD"] > row["MACD_SIGNAL"] and row["RSI14"] < 70
    bear = row["Close"] < row["EMA20"] < row["EMA50"] and row["MACD"] < row["MACD_SIGNAL"] and row["RSI14"] > 30
    if bull:
        return "BUY"
    if bear:
        return "SELL"
    return "HOLD"


def run_backtest(df: pd.DataFrame) -> Dict[str, Any]:
    trades: List[float] = []
    i = 0
    n = len(df)
    rows = df.reset_index()

    while i < n - 1:
        row = rows.iloc[i]
        action = local_signal(row)
        if action == "HOLD":
            i += 1
            continue

        entry = safe_float(row["Close"])
        risk_band = max(entry * 0.002, 25.0)
        if action == "BUY":
            sl = entry - risk_band
            tp = entry + risk_band * 1.6
        else:
            sl = entry + risk_band
            tp = entry - risk_band * 1.6

        pnl = 0.0
        closed = False
        j = i + 1
        while j < n:
            hi = safe_float(rows.iloc[j]["High"], safe_float(rows.iloc[j]["Close"]))
            lo = safe_float(rows.iloc[j]["Low"], safe_float(rows.iloc[j]["Close"]))
            close = safe_float(rows.iloc[j]["Close"])

            if action == "BUY":
                if lo <= sl:
                    pnl = sl - entry
                    closed = True
                    break
                if hi >= tp:
                    pnl = tp - entry
                    closed = True
                    break
            else:
                if hi >= sl:
                    pnl = entry - sl
                    closed = True
                    break
                if lo <= tp:
                    pnl = entry - tp
                    closed = True
                    break

            # safety exit after 8 bars
            if j - i >= 8:
                pnl = (close - entry) if action == "BUY" else (entry - close)
                closed = True
                break
            j += 1

        if not closed:
            final_close = safe_float(rows.iloc[-1]["Close"])
            pnl = (final_close - entry) if action == "BUY" else (entry - final_close)
            j = n - 1

        trades.append(float(pnl))
        i = max(j + 1, i + 1)

    total = len(trades)
    wins = [x for x in trades if x > 0]
    losses = [x for x in trades if x <= 0]
    win_rate = (len(wins) / total * 100.0) if total else 0.0
    total_points = sum(trades)
    avg_pnl = (total_points / total) if total else 0.0
    avg_win = (sum(wins) / len(wins)) if wins else 0.0
    avg_loss = (sum(losses) / len(losses)) if losses else 0.0
    expectancy = avg_pnl

    return {
        "total_trades": total,
        "win_rate_pct": round(win_rate, 2),
        "total_points": round(total_points, 2),
        "avg_pnl": round(avg_pnl, 4),
        "avg_win": round(avg_win, 4),
        "avg_loss": round(avg_loss, 4),
        "expectancy": round(expectancy, 4),
    }


def main() -> int:
    args = parse_args()

    if args.use_ai:
        print("[warn] --use-ai 已啟用：輕量回測引擎僅示意，不建議將外部 AI 決策用於回測一致性比較。")
    else:
        print("[info] 預設禁用外部 AI API，使用本地規則引擎回測。")

    df = load_data(args)
    df = apply_time_filter(df, args)
    df = add_indicators(df)

    if df.empty:
        raise RuntimeError("No rows after filtering/indicators")

    result = run_backtest(df)
    payload = {
        "generated_at": datetime.now().isoformat(),
        "use_ai": bool(args.use_ai),
        "data_rows": int(len(df)),
        "range_start": str(df.index.min()),
        "range_end": str(df.index.max()),
        "metrics": result,
    }

    print("[backtest] summary")
    print(json.dumps(payload, ensure_ascii=False, indent=2))

    if args.save_report:
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = REPORTS_DIR / f"backtest_{ts}.json"
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[info] report saved: {out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
