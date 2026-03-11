#!/usr/bin/env python3
"""Simple web fetcher for agent-browser.

Features:
- Fetch web pages with requests
- Parse HTML with BeautifulSoup (optional selectolax acceleration)
- Convert HTML to markdown/plain text summary (html2text optional)
- Return structured result: {title, clean_text}
- Demo task: fetch target URL, write title + first 500 chars to fetched_info.log,
  then notify via proactive-agent/send_telegram.py (graceful skip if env missing)
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, Optional

import requests
from bs4 import BeautifulSoup


DEFAULT_URL = "https://www.freedidi.com/23203.html"
BASE_DIR = Path(__file__).resolve().parent
DEFAULT_LOG_PATH = BASE_DIR / "fetched_info.log"
DEFAULT_TELEGRAM_SCRIPT = BASE_DIR.parent / "proactive-agent" / "send_telegram.py"


def _normalize_space(text: str) -> str:
    text = text.replace("\u00a0", " ")
    text = re.sub(r"\r\n?|\n", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _extract_with_selectolax(html: str) -> Optional[Dict[str, str]]:
    try:
        from selectolax.parser import HTMLParser  # type: ignore
    except Exception:
        return None

    tree = HTMLParser(html)
    title = (tree.css_first("title").text(strip=True) if tree.css_first("title") else "").strip()

    # Try to keep likely content nodes first.
    for selector in ["article", "main", ".post-content", ".entry-content", "#content", "body"]:
        node = tree.css_first(selector)
        if node:
            clean_text = _normalize_space(node.text(separator="\n", strip=True))
            if clean_text:
                return {"title": title, "clean_text": clean_text}

    return {"title": title, "clean_text": ""}


def _extract_main_html(soup: BeautifulSoup) -> str:
    for selector in ["article", "main", ".post-content", ".entry-content", "#content"]:
        node = soup.select_one(selector)
        if node:
            return str(node)
    body = soup.body
    return str(body) if body else str(soup)


def _html_to_markdown_or_text(html_fragment: str) -> str:
    try:
        import html2text  # type: ignore

        conv = html2text.HTML2Text()
        conv.ignore_links = True
        conv.ignore_images = True
        conv.ignore_emphasis = False
        conv.body_width = 0
        md = conv.handle(html_fragment)
        return _normalize_space(md)
    except Exception:
        # Fallback: plain text extraction via BeautifulSoup
        text_soup = BeautifulSoup(html_fragment, "html.parser")
        for tag in text_soup(["script", "style", "noscript", "iframe", "svg", "canvas"]):
            tag.decompose()
        return _normalize_space(text_soup.get_text(separator="\n", strip=True))


def fetch_web_content(url: str, timeout: float = 15.0) -> Dict[str, str]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )
    }
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()

    html = resp.text

    # Optional fast path with selectolax
    sel_result = _extract_with_selectolax(html)
    if sel_result and sel_result.get("clean_text"):
        return sel_result

    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "iframe", "svg", "canvas"]):
        tag.decompose()

    title = ""
    if soup.title and soup.title.string:
        title = soup.title.string.strip()

    main_html = _extract_main_html(soup)
    clean_text = _html_to_markdown_or_text(main_html)

    return {"title": title, "clean_text": clean_text}


def write_fetch_log(title: str, clean_text: str, log_path: Path) -> None:
    summary = clean_text[:500]
    content = f"title: {title}\nsummary_500: {summary}\n"
    log_path.write_text(content, encoding="utf-8")


def notify_telegram(title: str, telegram_script: Path) -> int:
    if not telegram_script.exists():
        print(f"[skip] Telegram sender not found: {telegram_script}")
        return 0

    message = f"已成功閱讀外部網頁：{title}"
    try:
        proc = subprocess.run(
            [sys.executable, str(telegram_script), "--message", message],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception as e:
        print(f"[skip] Telegram notify failed to run: {e}")
        return 0

    # send_telegram.py returns 2 when env missing: treat as graceful skip
    if proc.returncode in (0, 2):
        out = (proc.stdout or "").strip()
        err = (proc.stderr or "").strip()
        if out:
            print(out)
        if err:
            print(err)
        return 0

    err = (proc.stderr or proc.stdout or "").strip()
    print(f"[warn] Telegram notify failed (rc={proc.returncode}): {err}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch web page and emit structured summary")
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--log-path", default=str(DEFAULT_LOG_PATH))
    parser.add_argument("--timeout", type=float, default=15.0)
    parser.add_argument("--notify-telegram", action="store_true", default=True)
    parser.add_argument("--no-notify-telegram", dest="notify_telegram", action="store_false")
    parser.add_argument("--telegram-script", default=str(DEFAULT_TELEGRAM_SCRIPT))
    args = parser.parse_args()

    result = fetch_web_content(args.url, timeout=args.timeout)
    title = result.get("title", "").strip() or "(無標題)"
    clean_text = result.get("clean_text", "").strip()

    log_path = Path(args.log_path)
    write_fetch_log(title, clean_text, log_path)
    print(f"[ok] wrote log: {log_path}")

    if args.notify_telegram:
        notify_telegram(title, Path(args.telegram_script))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
