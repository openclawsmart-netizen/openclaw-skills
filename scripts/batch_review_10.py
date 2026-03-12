#!/usr/bin/env python3
"""每 10 筆交易觸發多模型討論回顧。

功能：
1) 讀取 data/trade_logs.json
2) 依 state(data/batch_review_state.json) 判斷是否有新 10 筆批次
3) 計算批次統計（勝率、平均盈虧、expectancy、最大連虧、常見失敗理由）
4) 依可用 API 金鑰嘗試呼叫 Gemini/Groq/OpenAI 產生討論
5) 彙整共識/分歧/參數建議，輸出 reports/batch_review_10_<timestamp>.md
6) 有新批次才發 Telegram 摘要；無新批次不發訊息
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import requests  # type: ignore
except Exception:
    requests = None

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
REPORTS_DIR = BASE_DIR / "reports"
TRADE_LOG_PATH = DATA_DIR / "trade_logs.json"
STATE_PATH = DATA_DIR / "batch_review_state.json"
TELEGRAM_SCRIPT = BASE_DIR / "proactive-agent" / "send_telegram.py"

OPENAI_MODEL = (os.getenv("OPENAI_MODEL") or "gpt-4o-mini").strip()
GROQ_MODEL = (os.getenv("GROQ_MODEL") or "llama-3.1-8b-instant").strip()
GEMINI_MODEL = (os.getenv("GEMINI_MODEL") or "gemini-2.0-flash").strip()


@dataclass
class ModelReview:
    model: str
    status: str  # available | unavailable | error
    reason: str
    common_conclusion: str
    major_risks: List[str]
    parameter_suggestions: List[str]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_json_array(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"trade logs not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("trade_logs.json must be a JSON array")
    return [x for x in data if isinstance(x, dict)]


def load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"last_processed_count": 0, "updated_at": None, "history": []}
    try:
        with path.open("r", encoding="utf-8") as f:
            state = json.load(f)
        if not isinstance(state, dict):
            raise ValueError("state must be object")
        return {
            "last_processed_count": int(state.get("last_processed_count", 0) or 0),
            "updated_at": state.get("updated_at"),
            "history": state.get("history") if isinstance(state.get("history"), list) else [],
        }
    except Exception:
        return {"last_processed_count": 0, "updated_at": None, "history": []}


def save_state(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def _to_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _extract_closed_trade_outcome(record: Dict[str, Any]) -> Optional[Tuple[str, float]]:
    """從單筆 log 擷取上一筆已結案結果（WIN/LOSS + pnl）。"""
    prior = record.get("prior_trade_status")
    if not isinstance(prior, dict):
        return None

    status = str(prior.get("status") or "").upper().strip()
    if status not in {"WIN", "LOSS"}:
        return None

    pnl = _to_float(prior.get("profit_loss_points"))
    if pnl is None:
        return None
    return status, pnl


def _tokenize_reason(text: str) -> List[str]:
    text = (text or "").strip()
    if not text:
        return []
    parts = re.split(r"[，,。；;|\n]+", text)
    return [p.strip() for p in parts if p and len(p.strip()) >= 4]


def summarize_batch(batch: List[Dict[str, Any]]) -> Dict[str, Any]:
    outcomes: List[Tuple[str, float]] = []
    failure_reasons: Counter[str] = Counter()

    for r in batch:
        out = _extract_closed_trade_outcome(r)
        if out:
            outcomes.append(out)
            status, _pnl = out
            if status == "LOSS":
                err = str(r.get("error_review") or "")
                opt = str(r.get("optimization_suggestion") or "")
                for t in _tokenize_reason(err) + _tokenize_reason(opt):
                    failure_reasons[t] += 1

    total_closed = len(outcomes)
    wins = [p for s, p in outcomes if s == "WIN" and p > 0]
    losses = [p for s, p in outcomes if s == "LOSS" or p <= 0]
    win_count = len(wins)

    win_rate = (win_count / total_closed * 100.0) if total_closed else 0.0
    avg_pnl = (sum(p for _s, p in outcomes) / total_closed) if total_closed else 0.0
    expectancy = avg_pnl  # 每筆期望值

    streak = 0
    max_losing_streak = 0
    for s, _p in outcomes:
        if s == "LOSS":
            streak += 1
            max_losing_streak = max(max_losing_streak, streak)
        else:
            streak = 0

    common_failure_reasons = [x for x, _ in failure_reasons.most_common(5)]

    return {
        "batch_size": len(batch),
        "closed_trade_count": total_closed,
        "win_rate": round(win_rate, 2),
        "avg_pnl": round(avg_pnl, 4),
        "expectancy": round(expectancy, 4),
        "max_losing_streak": max_losing_streak,
        "common_failure_reasons": common_failure_reasons,
    }


def _extract_json(text: str) -> Dict[str, Any]:
    text = (text or "").strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except Exception:
        pass
    m = re.search(r"\{[\s\S]*\}", text)
    if not m:
        return {}
    try:
        return json.loads(m.group(0))
    except Exception:
        return {}


def _call_openai(prompt: str) -> ModelReview:
    key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not key:
        return ModelReview("openai", "unavailable", "missing OPENAI_API_KEY", "", [], [])
    if requests is None:
        return ModelReview("openai", "unavailable", "missing requests dependency", "", [], [])

    try:
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={
                "model": OPENAI_MODEL,
                "temperature": 0.2,
                "messages": [
                    {"role": "system", "content": "你是交易回顧助理。僅輸出 JSON。"},
                    {"role": "user", "content": prompt},
                ],
            },
            timeout=25,
        )
        if resp.status_code != 200:
            return ModelReview("openai", "error", f"OpenAI HTTP {resp.status_code}", "", [], [])
        data = resp.json()
        content = data["choices"][0]["message"]["content"]
        obj = _extract_json(content)
        return ModelReview(
            "openai",
            "available",
            "ok",
            str(obj.get("common_conclusion") or ""),
            [str(x) for x in (obj.get("major_risks") or []) if str(x).strip()],
            [str(x) for x in (obj.get("parameter_suggestions") or []) if str(x).strip()],
        )
    except Exception as exc:
        return ModelReview("openai", "error", f"OpenAI exception: {exc}", "", [], [])


def _call_groq(prompt: str) -> ModelReview:
    key = (os.getenv("GROQ_API_KEY") or "").strip()
    if not key:
        return ModelReview("groq", "unavailable", "missing GROQ_API_KEY", "", [], [])
    if requests is None:
        return ModelReview("groq", "unavailable", "missing requests dependency", "", [], [])

    try:
        resp = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={
                "model": GROQ_MODEL,
                "temperature": 0.2,
                "messages": [
                    {"role": "system", "content": "你是交易回顧助理。僅輸出 JSON。"},
                    {"role": "user", "content": prompt},
                ],
            },
            timeout=25,
        )
        if resp.status_code != 200:
            return ModelReview("groq", "error", f"Groq HTTP {resp.status_code}", "", [], [])
        data = resp.json()
        content = data["choices"][0]["message"]["content"]
        obj = _extract_json(content)
        return ModelReview(
            "groq",
            "available",
            "ok",
            str(obj.get("common_conclusion") or ""),
            [str(x) for x in (obj.get("major_risks") or []) if str(x).strip()],
            [str(x) for x in (obj.get("parameter_suggestions") or []) if str(x).strip()],
        )
    except Exception as exc:
        return ModelReview("groq", "error", f"Groq exception: {exc}", "", [], [])


def _call_gemini(prompt: str) -> ModelReview:
    key = (os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or "").strip()
    if not key:
        return ModelReview("gemini", "unavailable", "missing GEMINI_API_KEY/GOOGLE_API_KEY", "", [], [])
    if requests is None:
        return ModelReview("gemini", "unavailable", "missing requests dependency", "", [], [])

    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
        resp = requests.post(
            url,
            params={"key": key},
            headers={"Content-Type": "application/json"},
            json={
                "contents": [
                    {"parts": [{"text": "你是交易回顧助理。僅輸出 JSON。"}]},
                    {"parts": [{"text": prompt}]},
                ],
                "generationConfig": {"temperature": 0.2},
            },
            timeout=25,
        )
        if resp.status_code != 200:
            return ModelReview("gemini", "error", f"Gemini HTTP {resp.status_code}", "", [], [])
        data = resp.json()
        content = (
            data.get("candidates", [{}])[0]
            .get("content", {})
            .get("parts", [{}])[0]
            .get("text", "")
        )
        obj = _extract_json(content)
        return ModelReview(
            "gemini",
            "available",
            "ok",
            str(obj.get("common_conclusion") or ""),
            [str(x) for x in (obj.get("major_risks") or []) if str(x).strip()],
            [str(x) for x in (obj.get("parameter_suggestions") or []) if str(x).strip()],
        )
    except Exception as exc:
        return ModelReview("gemini", "error", f"Gemini exception: {exc}", "", [], [])


def run_multi_model_discussion(batch_summary: Dict[str, Any], batch_start: int, batch_end: int) -> List[ModelReview]:
    prompt = (
        "請根據以下交易批次摘要，給我 JSON："
        '{"common_conclusion": "...", "major_risks": ["..."], "parameter_suggestions": ["..."]}。\n'
        "要求：\n"
        "1) common_conclusion 為 1-2 句中文\n"
        "2) major_risks 最多 3 點\n"
        "3) parameter_suggestions 最多 3 點，僅提案，不可直接修改參數\n\n"
        f"批次範圍: {batch_start}-{batch_end}\n"
        f"批次統計: {json.dumps(batch_summary, ensure_ascii=False)}"
    )

    # 串行呼叫，避免速率限制
    return [_call_gemini(prompt), _call_groq(prompt), _call_openai(prompt)]


def aggregate_reviews(reviews: List[ModelReview]) -> Dict[str, Any]:
    available = [r for r in reviews if r.status == "available"]

    if not available:
        return {
            "consensus": "無可用模型（全部 unavailable/error），本次以統計結果供人工判讀。",
            "divergences": ["Gemini/Groq/OpenAI 皆無法參與"],
            "next_round_suggestions": ["先修復模型金鑰與配額，再進行多模型討論"],
        }

    conclusion_counter = Counter([r.common_conclusion.strip() for r in available if r.common_conclusion.strip()])
    consensus = conclusion_counter.most_common(1)[0][0] if conclusion_counter else "模型未給出明確共同結論"

    risk_counter: Counter[str] = Counter()
    sug_counter: Counter[str] = Counter()
    for r in available:
        for x in r.major_risks:
            risk_counter[x.strip()] += 1
        for x in r.parameter_suggestions:
            sug_counter[x.strip()] += 1

    shared_risks = [k for k, v in risk_counter.items() if v >= 2]
    top_suggestions = [k for k, _v in sug_counter.most_common(5)]

    divergences: List[str] = []
    unique_conclusions = list(dict.fromkeys([r.common_conclusion.strip() for r in available if r.common_conclusion.strip()]))
    if len(unique_conclusions) > 1:
        divergences.append("結論不完全一致：" + " / ".join(unique_conclusions[:3]))

    if not divergences:
        divergences = ["主要觀點一致，無顯著分歧"]

    return {
        "consensus": consensus,
        "divergences": divergences,
        "next_round_suggestions": top_suggestions[:3] if top_suggestions else ["維持風險控管，等待更多樣本後再調參"],
        "shared_risks": shared_risks,
    }


def render_report(
    report_path: Path,
    batch_start: int,
    batch_end: int,
    summary: Dict[str, Any],
    reviews: List[ModelReview],
    final_review: Dict[str, Any],
) -> None:
    lines: List[str] = []
    lines.append(f"# Batch Review 10 ({batch_start}-{batch_end})")
    lines.append("")
    lines.append(f"- Generated at: {now_iso()}")
    lines.append(f"- Batch size: {summary['batch_size']}")
    lines.append("")

    lines.append("## Batch Stats")
    lines.append("")
    lines.append(f"- 勝率: {summary['win_rate']}%")
    lines.append(f"- 平均盈虧: {summary['avg_pnl']}")
    lines.append(f"- Expectancy: {summary['expectancy']}")
    lines.append(f"- 最大連虧: {summary['max_losing_streak']}")
    lines.append(f"- 常見失敗理由: {', '.join(summary['common_failure_reasons']) if summary['common_failure_reasons'] else '無'}")
    lines.append("")

    lines.append("## Multi-model Discussion")
    lines.append("")
    for r in reviews:
        lines.append(f"### {r.model}")
        lines.append(f"- status: {r.status}")
        lines.append(f"- reason: {r.reason}")
        if r.status == "available":
            lines.append(f"- 共同結論: {r.common_conclusion or '無'}")
            lines.append(f"- 主要風險: {', '.join(r.major_risks) if r.major_risks else '無'}")
            lines.append(f"- 參數建議: {', '.join(r.parameter_suggestions) if r.parameter_suggestions else '無'}")
        lines.append("")

    lines.append("## Final Review")
    lines.append("")
    lines.append(f"- 共同結論（共識）: {final_review['consensus']}")
    lines.append(f"- 分歧點: {'; '.join(final_review['divergences'])}")
    lines.append(
        f"- 下一輪參數調整建議（提案）: {', '.join(final_review['next_round_suggestions']) if final_review['next_round_suggestions'] else '無'}"
    )

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def send_telegram_summary(batch_start: int, batch_end: int, final_review: Dict[str, Any]) -> str:
    if not TELEGRAM_SCRIPT.exists():
        return "skip: telegram script missing"

    msg = (
        f"📘 [Batch Review 10]\n"
        f"批次範圍: {batch_start}-{batch_end}\n"
        f"共識: {final_review.get('consensus', '')}\n"
        f"分歧: {'; '.join(final_review.get('divergences', []))}\n"
        f"建議: {', '.join(final_review.get('next_round_suggestions', []))}"
    )

    proc = subprocess.run(
        [sys.executable, str(TELEGRAM_SCRIPT), "--message", msg],
        capture_output=True,
        text=True,
    )
    if proc.returncode == 0:
        return "ok"
    if proc.returncode == 2:
        return "skip: telegram not configured"
    err = (proc.stderr or proc.stdout or "").strip()
    return f"warn: telegram failed rc={proc.returncode} {err}"


def main() -> int:
    try:
        logs = load_json_array(TRADE_LOG_PATH)
    except Exception as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 1

    state = load_state(STATE_PATH)
    last = int(state.get("last_processed_count", 0) or 0)
    total = len(logs)

    if total - last < 10:
        print(f"[info] no new batch: total={total}, last_processed={last}")
        return 0

    processed_any = False
    cursor = last

    while total - cursor >= 10:
        batch_start = cursor + 1
        batch_end = cursor + 10
        batch = logs[cursor:cursor + 10]

        summary = summarize_batch(batch)
        reviews = run_multi_model_discussion(summary, batch_start, batch_end)
        final_review = aggregate_reviews(reviews)

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = REPORTS_DIR / f"batch_review_10_{ts}_{batch_start}_{batch_end}.md"
        render_report(report_path, batch_start, batch_end, summary, reviews, final_review)

        tg = send_telegram_summary(batch_start, batch_end, final_review)

        state_history = state.get("history") if isinstance(state.get("history"), list) else []
        state_history.append(
            {
                "batch_start": batch_start,
                "batch_end": batch_end,
                "report": str(report_path.relative_to(BASE_DIR)),
                "telegram": tg,
                "processed_at": now_iso(),
            }
        )

        cursor += 10
        state["last_processed_count"] = cursor
        state["updated_at"] = now_iso()
        state["history"] = state_history[-50:]
        save_state(STATE_PATH, state)

        print(f"[ok] processed batch {batch_start}-{batch_end} report={report_path.name} telegram={tg}")
        processed_any = True

    if not processed_any:
        print(f"[info] no new batch: total={total}, last_processed={last}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
