#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional

# 沿用 daily_brief.py 的穩定路徑邏輯
BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_TELEGRAM_SCRIPT = BASE_DIR / "proactive-agent" / "send_telegram.py"
DEFAULT_ENV_FILE = Path(os.getenv("OPENCLAW_ENV_FILE", "/root/.openclaw_env"))


def bytes_to_mb(value: int) -> float:
    return value / (1024 * 1024)


def disk_usage(path: str = "/") -> shutil._ntuple_diskusage:
    return shutil.disk_usage(path)


def send_telegram(message: str, script_path: Path, env_file: Path) -> int:
    resolved_script = script_path if script_path.is_absolute() else (BASE_DIR / script_path).resolve()

    if not resolved_script.exists():
        print(f"[error] Telegram sender not found: {resolved_script}", file=sys.stderr)
        return 1

    proc = subprocess.run(
        [sys.executable, str(resolved_script), "--message", message, "--env-file", str(env_file)],
        capture_output=True,
        text=True,
        check=False,
        cwd=str(BASE_DIR),
    )

    out = (proc.stdout or "").strip()
    err = (proc.stderr or "").strip()
    if out:
        print(out)
    if err:
        print(err, file=sys.stderr)

    return proc.returncode


def run_cleanup(cleanup_command: Optional[str]) -> int:
    if not cleanup_command:
        # 預設做安全且可重複執行的輕量清理：刪除 proactive-agent 下暫存檔
        cleaned = 0
        target_dir = BASE_DIR / "proactive-agent"
        for p in target_dir.glob("*.tmp"):
            try:
                p.unlink()
                cleaned += 1
            except Exception:
                pass
        print(f"[cleanup] removed_tmp_files={cleaned}")
        return 0

    proc = subprocess.run(["bash", "-lc", cleanup_command], cwd=str(BASE_DIR), text=True)
    return proc.returncode


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="GitHub healer + disk cleanup notifier")
    parser.add_argument("--cleanup-command", default="", help="Optional cleanup shell command to run")
    parser.add_argument("--disk-path", default="/", help="Path used for disk usage check")
    parser.add_argument("--telegram-script", default=str(DEFAULT_TELEGRAM_SCRIPT), help="Path to send_telegram.py")
    parser.add_argument("--env-file", default=str(DEFAULT_ENV_FILE), help="Path to env file for Telegram credentials")
    parser.add_argument("--simulate-freed-mb", type=float, default=None, help="Test flag: override freed size (MB)")
    parser.add_argument("--simulate-post-disk-percent", type=float, default=None, help="Test flag: override post cleanup disk usage percent")
    parser.add_argument("--dry-run", action="store_true", help="Compute and print only; do not send Telegram")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    before = disk_usage(args.disk_path)
    before_used = int(before.used)
    before_total = int(before.total)
    before_percent = (before_used / before_total * 100.0) if before_total else 0.0

    rc = run_cleanup(args.cleanup_command)

    after = disk_usage(args.disk_path)
    after_used = int(after.used)
    after_total = int(after.total)
    after_percent_real = (after_used / after_total * 100.0) if after_total else 0.0

    real_freed_bytes = max(before_used - after_used, 0)
    freed_bytes = real_freed_bytes
    if args.simulate_freed_mb is not None:
        freed_bytes = max(int(args.simulate_freed_mb * 1024 * 1024), 0)

    post_percent = after_percent_real if args.simulate_post_disk_percent is None else float(args.simulate_post_disk_percent)

    print(
        "[disk] "
        f"before_used={before_used}B ({bytes_to_mb(before_used):.2f}MB) "
        f"after_used={after_used}B ({bytes_to_mb(after_used):.2f}MB) "
        f"freed={freed_bytes}B ({bytes_to_mb(freed_bytes):.2f}MB) "
        f"before_pct={before_percent:.2f}% after_pct={after_percent_real:.2f}% effective_after_pct={post_percent:.2f}%"
    )

    notify_rc = 0

    if freed_bytes > 500 * 1024 * 1024:
        size_text = f"{bytes_to_mb(freed_bytes):.2f} MB"
        msg = f"LIVA Z2 已完成磁碟清理，釋放了 {size_text} 空間。"
        if args.dry_run:
            print(f"[dry-run] would send: {msg}")
        else:
            notify_rc = send_telegram(msg, Path(args.telegram_script), Path(args.env_file))

    if post_percent > 90.0:
        urgent_msg = "⚠️ 警告：磁碟剩餘空間不足 10%！請考慮擴充硬體或手動檢查。"
        if args.dry_run:
            print(f"[dry-run] would send: {urgent_msg}")
        else:
            urgent_rc = send_telegram(urgent_msg, Path(args.telegram_script), Path(args.env_file))
            if notify_rc == 0:
                notify_rc = urgent_rc

    if rc != 0:
        return rc
    return notify_rc


if __name__ == "__main__":
    raise SystemExit(main())
