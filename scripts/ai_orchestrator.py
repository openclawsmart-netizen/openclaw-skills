#!/usr/bin/env python3
"""AI orchestrator for YM=F 15m with tri-syndicate handshake.

Flow:
1) Fetch YM=F 15m candles via yfinance.
2) Groq sentinel performs risk-first JSON handshake.
3) Gemini strategist outputs 15m strategy JSON (defensive mode when high risk).
4) OpenAI-style arbitration layer applies deterministic hedge/position rules.
5) Persist full run payload under logs/syndicate/<timestamp>.json.
6) Send Telegram summary (graceful skip when not configured).
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Tuple

SYMBOL = "YM=F"
INTERVAL = "15m"
PERIOD = "2d"

BASE_DIR = Path(__file__).resolve().parents[1]
TELEGRAM_SCRIPT = BASE_DIR / "proactive-agent" / "send_telegram.py"
LOG_DIR = BASE_DIR / "logs" / "syndicate"

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")


def eprint(msg: str) -> None:
    print(msg, file=sys.stderr)


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def safe_float(v: Any, default: float) -> float:
    try:
        return float(v)
    except Exception:
        return default


def extract_json_object(text: str) -> Dict[str, Any]:
    text = (text or "").strip()
    if not text:
        return {}

    # direct parse first
    try:
        loaded = json.loads(text)
        if isinstance(loaded, dict):
            return loaded
    except Exception:
        pass

    # attempt fenced / embedded JSON object
    m = re.search(r"\{[\s\S]*\}", text)
    if not m:
        return {}
    try:
        loaded = json.loads(m.group(0))
        if isinstance(loaded, dict):
            return loaded
    except Exception:
        return {}
    return {}


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
    range_pct = ((high - low) / close * 100.0) if close else 0.0

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
        "range_pct": round(range_pct, 3),
        "volume": round(vol, 2),
        "recent_12_up_candles": trend_up,
        "recent_12_total": 12,
    }

    summary = (
        f"{SYMBOL} {INTERVAL} @ {latest_ts} | O:{open_:.2f} H:{high:.2f} L:{low:.2f} C:{close:.2f} "
        f"({change:+.2f}, {change_pct:+.2f}%) Vol:{vol:.0f} | last12 up-candles={trend_up}/12"
    )
    return snapshot, summary


def quick_risk_assessment(snapshot: Dict[str, Any], reason: str = "fallback") -> Dict[str, Any]:
    change_pct = abs(safe_float(snapshot.get("change_pct"), 0.0))
    range_pct = abs(safe_float(snapshot.get("range_pct"), 0.0))
    up = int(safe_float(snapshot.get("recent_12_up_candles"), 6))
    imbalance = abs(up - 6)

    score = 3.0 + (change_pct * 2.2) + (range_pct * 1.8) + (imbalance * 0.2)
    risk_level = int(clamp(round(score), 0, 10))
    volatility_high = (change_pct >= 0.6) or (range_pct >= 1.0)

    return {
        "risk_level": risk_level,
        "volatility_flag": "high" if volatility_high else "normal",
        "allow_entry": "CONDITIONAL" if risk_level >= 6 else "YES",
        "risk_notes": [
            f"fallback_assessment(change_pct={change_pct:.3f}, range_pct={range_pct:.3f}, imbalance={imbalance})",
            reason,
        ],
    }


def normalize_groq_json(parsed: Dict[str, Any], degraded: bool = False) -> Dict[str, Any]:
    risk_level = int(clamp(round(safe_float(parsed.get("risk_level"), 5)), 0, 10))

    vol_raw = parsed.get("volatility_flag", "normal")
    vol_label = "normal"
    if isinstance(vol_raw, bool):
        vol_label = "high" if vol_raw else "normal"
    elif isinstance(vol_raw, str) and vol_raw.strip().lower() in {"high", "normal", "true", "false"}:
        v = vol_raw.strip().lower()
        vol_label = "high" if v in {"high", "true"} else "normal"
    else:
        degraded = True

    if "risk_level" not in parsed or "volatility_flag" not in parsed:
        degraded = True

    return {
        "risk_level": risk_level,
        "volatility_flag": vol_label,
        "degraded": degraded,
    }


def normalize_gemini_json(parsed: Dict[str, Any], degraded: bool = False) -> Dict[str, Any]:
    sig_raw = str(parsed.get("strategy_signal", "Neutral")).strip().lower()
    signal = "Neutral"
    if sig_raw in {"long", "bull", "buy"}:
        signal = "Long"
    elif sig_raw in {"short", "bear", "sell"}:
        signal = "Short"
    elif sig_raw in {"neutral", "hold", "flat"}:
        signal = "Neutral"
    else:
        degraded = True

    hint = str(parsed.get("opencl_logic_hint", "")).strip()
    if not hint:
        hint = "N/A"
        degraded = True

    psf = clamp(safe_float(parsed.get("position_size_factor"), 1.0), 0.1, 1.5)

    if "strategy_signal" not in parsed or "opencl_logic_hint" not in parsed:
        degraded = True

    return {
        "strategy_signal": signal,
        "opencl_logic_hint": hint,
        "position_size_factor": round(psf, 2),
        "degraded": degraded,
    }


def call_groq(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    key = (os.getenv("GROQ_API_KEY") or "").strip()
    if not key:
        fb = quick_risk_assessment(snapshot, "missing GROQ_API_KEY")
        norm = normalize_groq_json(fb, degraded=True)
        return {
            "status": "fallback",
            "reason": "missing GROQ_API_KEY",
            "raw_text": json.dumps(fb, ensure_ascii=False),
            "raw_json": fb,
            "normalized": norm,
        }

    try:
        import requests  # type: ignore
    except ImportError:
        fb = quick_risk_assessment(snapshot, "missing requests dependency")
        norm = normalize_groq_json(fb, degraded=True)
        return {
            "status": "fallback",
            "reason": "missing requests dependency",
            "raw_text": json.dumps(fb, ensure_ascii=False),
            "raw_json": fb,
            "normalized": norm,
        }

    system_prompt = (
        "You are a low-latency futures risk sentinel. "
        "Output STRICT JSON only, no markdown, no extra text."
    )
    user_prompt = (
        "請根據以下 YM=F 15m 快照，輸出嚴格 JSON，必要欄位：\n"
        "- risk_level: 0-10 integer\n"
        "- volatility_flag: boolean 或 high/normal\n"
        "可選：allow_entry(YES/NO/CONDITIONAL), risk_notes(array of string)。\n"
        f"市場快照: {json.dumps(snapshot, ensure_ascii=False)}"
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
            fb = quick_risk_assessment(snapshot, f"Groq HTTP {resp.status_code}")
            norm = normalize_groq_json(fb, degraded=True)
            return {
                "status": "fallback",
                "reason": f"Groq HTTP {resp.status_code}",
                "raw_text": json.dumps(fb, ensure_ascii=False),
                "raw_json": fb,
                "normalized": norm,
            }

        body = resp.json()
        text = body.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
        parsed = extract_json_object(text)
        norm = normalize_groq_json(parsed, degraded=(not bool(parsed)))
        return {
            "status": "ok" if parsed else "degraded",
            "reason": "" if parsed else "non-json Groq response",
            "raw_text": text,
            "raw_json": parsed,
            "normalized": norm,
        }
    except Exception as exc:
        fb = quick_risk_assessment(snapshot, f"Groq exception: {exc}")
        norm = normalize_groq_json(fb, degraded=True)
        return {
            "status": "fallback",
            "reason": f"Groq exception: {exc}",
            "raw_text": json.dumps(fb, ensure_ascii=False),
            "raw_json": fb,
            "normalized": norm,
        }


def fallback_strategy(snapshot: Dict[str, Any], groq_norm: Dict[str, Any], reason: str) -> Dict[str, Any]:
    change_pct = safe_float(snapshot.get("change_pct"), 0.0)
    up = int(safe_float(snapshot.get("recent_12_up_candles"), 6))
    risk = int(groq_norm.get("risk_level", 5))

    signal = "Neutral"
    if risk <= 7:
        if change_pct > 0.2 and up >= 7:
            signal = "Long"
        elif change_pct < -0.2 and up <= 5:
            signal = "Short"

    hint = f"fallback_rule_engine(change_pct={change_pct:.3f}, up={up}, risk={risk})"
    psf = 0.6 if risk > 7 else 0.8 if risk >= 6 else 1.0

    raw = {
        "strategy_signal": signal,
        "opencl_logic_hint": hint,
        "position_size_factor": psf,
        "fallback_reason": reason,
    }
    norm = normalize_gemini_json(raw, degraded=True)
    return {
        "status": "fallback",
        "reason": reason,
        "raw_text": json.dumps(raw, ensure_ascii=False),
        "raw_json": raw,
        "normalized": norm,
    }


def call_gemini(snapshot: Dict[str, Any], groq_norm: Dict[str, Any]) -> Dict[str, Any]:
    key = (os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or "").strip()
    if not key:
        return fallback_strategy(snapshot, groq_norm, "missing GEMINI_API_KEY/GOOGLE_API_KEY")

    try:
        import requests  # type: ignore
    except ImportError:
        return fallback_strategy(snapshot, groq_norm, "missing requests dependency")

    risk_level = int(groq_norm.get("risk_level", 5))
    vol = groq_norm.get("volatility_flag", "normal")
    defensive = risk_level > 7

    defensive_line = ""
    if defensive:
        defensive_line = "風險哨兵判定 risk_level > 7，必須採用『防禦性策略』：優先 Neutral/保守，若非必要避免追價。"

    prompt = (
        "你是『道瓊量化交易員』。根據 15m 快照與風險哨兵輸出，給策略 JSON。\n"
        "只輸出嚴格 JSON，不要 markdown，不要多餘文字。\n"
        "必要欄位：\n"
        "- strategy_signal: Long/Short/Neutral\n"
        "- opencl_logic_hint: string\n"
        "可選欄位：position_size_factor (0.1~1.5)。\n"
        f"風險資訊: risk_level={risk_level}, volatility_flag={vol}.\n"
        f"{defensive_line}\n"
        f"市場快照: {json.dumps(snapshot, ensure_ascii=False)}"
    )

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.1, "maxOutputTokens": 320},
    }

    try:
        resp = requests.post(url, params={"key": key}, json=payload, timeout=25)
        if resp.status_code != 200:
            return fallback_strategy(snapshot, groq_norm, f"Gemini HTTP {resp.status_code}")

        body = resp.json()
        text = (
            body.get("candidates", [{}])[0]
            .get("content", {})
            .get("parts", [{}])[0]
            .get("text", "")
            .strip()
        )
        parsed = extract_json_object(text)
        if not parsed:
            return fallback_strategy(snapshot, groq_norm, "Gemini non-json response")

        norm = normalize_gemini_json(parsed, degraded=False)
        return {
            "status": "ok" if not norm.get("degraded") else "degraded",
            "reason": "" if parsed else "empty Gemini response",
            "raw_text": text,
            "raw_json": parsed,
            "normalized": norm,
        }
    except Exception as exc:
        return fallback_strategy(snapshot, groq_norm, f"Gemini exception: {exc}")


def arbitrate_final_decision(snapshot: Dict[str, Any], groq_norm: Dict[str, Any], gemini_norm: Dict[str, Any]) -> Dict[str, Any]:
    signal = gemini_norm.get("strategy_signal", "Neutral")
    risk_level = int(groq_norm.get("risk_level", 5))
    vol_high = groq_norm.get("volatility_flag", "normal") == "high"

    psf = clamp(safe_float(gemini_norm.get("position_size_factor"), 1.0), 0.1, 1.5)
    hedge_adjusted = False
    hedge_note = ""

    if signal == "Long" and (risk_level > 7 or vol_high):
        psf = round(clamp(psf * 0.5, 0.1, 1.5), 2)
        hedge_adjusted = True
        hedge_note = "風險對沖調整"

    final_action = "HOLD" if signal == "Neutral" else signal.upper()
    mode = "partial" if (groq_norm.get("degraded") or gemini_norm.get("degraded")) else "full"

    return {
        "final_action": final_action,
        "position_size_factor": round(psf, 2),
        "risk_hedge_adjusted": hedge_adjusted,
        "risk_hedge_note": hedge_note,
        "mode": mode,
        "opencl_logic_hint": gemini_norm.get("opencl_logic_hint", "N/A"),
        "decision_reason": f"signal={signal}, risk_level={risk_level}, volatility={groq_norm.get('volatility_flag')}",
        "snapshot_ts": snapshot.get("timestamp"),
    }


def save_run_log(snapshot: Dict[str, Any], groq: Dict[str, Any], gemini: Dict[str, Any], final_decision: Dict[str, Any]) -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    out = LOG_DIR / f"{ts}.json"
    payload = {
        "timestamp": datetime.now().isoformat(),
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "snapshot": snapshot,
        "groq": {
            "status": groq.get("status"),
            "reason": groq.get("reason"),
            "raw_text": groq.get("raw_text"),
            "raw_json": groq.get("raw_json"),
            "normalized": groq.get("normalized"),
        },
        "gemini": {
            "status": gemini.get("status"),
            "reason": gemini.get("reason"),
            "raw_text": gemini.get("raw_text"),
            "raw_json": gemini.get("raw_json"),
            "normalized": gemini.get("normalized"),
        },
        "final_decision": final_decision,
        "metadata": {
            "models": {"groq": GROQ_MODEL, "gemini": GEMINI_MODEL},
            "runtime": "ai_orchestrator",
        },
    }
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


def build_final_report(
    market_summary: str,
    snapshot: Dict[str, Any],
    groq: Dict[str, Any],
    gemini: Dict[str, Any],
    final_decision: Dict[str, Any],
) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    gq = groq.get("normalized", {})
    gm = gemini.get("normalized", {})

    lines = [
        "🤖 AI Orchestrator 報告（YM=F 15m）",
        f"時間: {now}",
        f"模式: {final_decision.get('mode', 'partial')}",
        "",
        "【基本行情摘要】",
        market_summary,
        "",
        "【三位一體握手摘要】",
        f"Groq 風險: risk_level={gq.get('risk_level')} / volatility={gq.get('volatility_flag')}",
        f"Gemini 訊號: strategy_signal={gm.get('strategy_signal')}",
        f"最終決策: {final_decision.get('final_action')} / 倉位比例={final_decision.get('position_size_factor')}",
    ]

    if final_decision.get("risk_hedge_adjusted"):
        lines.append("⚠️ 風險對沖調整")

    lines += [
        "",
        "【策略提示】",
        str(final_decision.get("opencl_logic_hint", "N/A")),
        "",
        "【服務狀態】",
        f"Groq: {groq.get('status')} ({groq.get('reason', '')})",
        f"Gemini: {gemini.get('status')} ({gemini.get('reason', '')})",
    ]

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

    groq = call_groq(snapshot)
    gemini = call_gemini(snapshot, groq.get("normalized", {}))
    final_decision = arbitrate_final_decision(snapshot, groq.get("normalized", {}), gemini.get("normalized", {}))

    report = build_final_report(market_summary, snapshot, groq, gemini, final_decision)
    log_path = save_run_log(snapshot, groq, gemini, final_decision)

    print(report)
    print("\n---")
    print(f"[info] Groq status={groq.get('status')} reason={groq.get('reason', '')}")
    print(f"[info] Gemini status={gemini.get('status')} reason={gemini.get('reason', '')}")
    print(f"[info] Final action={final_decision.get('final_action')} position_size_factor={final_decision.get('position_size_factor')}")
    print(f"[info] Log written: {log_path}")

    tg = send_telegram(report)
    print(f"[info] Telegram: {tg}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
