#!/usr/bin/env python3
import argparse
import os
import re
import sys
from pathlib import Path
from typing import Dict

DEFAULT_ENV_PATH = Path("/root/.openclaw_env")


_ENV_LINE_RE = re.compile(r"^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$")


def load_kv_env(env_path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    if not env_path.exists() or not env_path.is_file():
        return values

    for raw in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue

        match = _ENV_LINE_RE.match(line)
        if not match:
            continue

        key, val = match.group(1), match.group(2).strip()
        if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
            val = val[1:-1]

        values[key] = val

    return values


def parse_args():
    parser = argparse.ArgumentParser(description="Send Telegram message via Bot API")
    parser.add_argument("--message", required=True, help="Message text")
    parser.add_argument(
        "--env-file",
        default=os.getenv("OPENCLAW_ENV_FILE", str(DEFAULT_ENV_PATH)),
        help="Path to env file (default: OPENCLAW_ENV_FILE or /root/.openclaw_env)",
    )
    parser.add_argument("--timeout", type=float, default=10.0, help="Request timeout in seconds")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        import requests  # type: ignore
    except ImportError:
        print("[error] Missing dependency: requests. Install with: pip install requests", file=sys.stderr)
        return 3

    env_path = Path(args.env_file)
    env = load_kv_env(env_path)

    token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip() or env.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = (os.getenv("TELEGRAM_CHAT_ID") or "").strip() or env.get("TELEGRAM_CHAT_ID", "").strip()

    if not token or not chat_id:
        print(
            f"[skip] Telegram env not configured (need TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID; env file: {env_path})",
            file=sys.stderr,
        )
        return 2

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": args.message}

    try:
        response = requests.post(url, json=payload, timeout=args.timeout)
    except Exception as e:
        print(f"[error] Telegram request failed: {e}", file=sys.stderr)
        return 1

    if response.status_code != 200:
        print(f"[error] Telegram API returned HTTP {response.status_code}", file=sys.stderr)
        return 1

    try:
        body = response.json()
    except Exception:
        body = {}

    if not body.get("ok", False):
        print("[error] Telegram API response not ok", file=sys.stderr)
        return 1

    print("[ok] Telegram message sent")
    return 0


if __name__ == "__main__":
    sys.exit(main())
