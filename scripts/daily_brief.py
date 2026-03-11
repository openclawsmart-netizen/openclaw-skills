#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path
from typing import List

import requests
from bs4 import BeautifulSoup

DEFAULT_URL = "https://github.com/trending"
BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_TELEGRAM_SCRIPT = BASE_DIR / "proactive-agent" / "send_telegram.py"


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def fetch_titles(url: str, timeout: float = 15.0, limit: int = 5) -> List[str]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )
    }
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    titles: List[str] = []

    if "github.com/trending" in url:
        for h2 in soup.select("article.Box-row h2"):
            t = _clean(h2.get_text(" ", strip=True)).replace(" / ", "/")
            if t and t not in titles:
                titles.append(t)
                if len(titles) >= limit:
                    break
    else:
        for node in soup.select("h1, h2, h3"):
            t = _clean(node.get_text(" ", strip=True))
            if len(t) < 8:
                continue
            if t and t not in titles:
                titles.append(t)
                if len(titles) >= limit:
                    break

    if not titles:
        title = _clean(soup.title.get_text(" ", strip=True)) if soup.title else "(No title found)"
        titles = [title]

    return titles


def format_brief(url: str, items: List[str]) -> str:
    lines = ["📌 Daily Tech Brief", f"Source: {url}", ""]
    for idx, item in enumerate(items, start=1):
        lines.append(f"{idx}. {item}")
    return "\n".join(lines)


def send_telegram(message: str, script_path: Path) -> int:
    if not script_path.exists():
        print(f"[skip] Telegram sender not found: {script_path}")
        return 0

    proc = subprocess.run(
        [sys.executable, str(script_path), "--message", message],
        capture_output=True,
        text=True,
        check=False,
    )

    # send_telegram.py rc=2 when env missing -> graceful skip
    if proc.returncode in (0, 2):
        out = (proc.stdout or "").strip()
        err = (proc.stderr or "").strip()
        if out:
            print(out)
        if err:
            print(err)
        return 0

    detail = (proc.stderr or proc.stdout or "").strip()
    print(f"[warn] Telegram send failed (rc={proc.returncode}): {detail}")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch daily tech brief and notify Telegram")
    parser.add_argument("--url", default=DEFAULT_URL, help="News URL (default: GitHub Trending)")
    parser.add_argument("--top", type=int, default=5, help="How many titles to include")
    parser.add_argument("--timeout", type=float, default=15.0, help="HTTP timeout seconds")
    parser.add_argument("--telegram-script", default=str(DEFAULT_TELEGRAM_SCRIPT), help="Path to send_telegram.py")
    parser.add_argument("--print-only", action="store_true", help="Only print brief, do not send")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.top <= 0:
        print("[error] --top must be > 0", file=sys.stderr)
        return 2

    try:
        items = fetch_titles(args.url, timeout=args.timeout, limit=args.top)
    except Exception as e:
        print(f"[error] Failed to fetch brief from {args.url}: {e}", file=sys.stderr)
        return 1

    brief = format_brief(args.url, items)
    print(brief)

    if args.print_only:
        return 0

    return send_telegram(brief, Path(args.telegram_script))


if __name__ == "__main__":
    raise SystemExit(main())
