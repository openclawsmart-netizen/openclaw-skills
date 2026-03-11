#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Dict, Optional, Tuple

BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_TELEGRAM_SCRIPT = BASE_DIR / "proactive-agent" / "send_telegram.py"
DEFAULT_ENV_FILE = Path(os.getenv("OPENCLAW_ENV_FILE", "/root/.openclaw_env"))
THERMAL_PATH = Path("/sys/class/thermal/thermal_zone0/temp")


def read_temperature_c(path: Path = THERMAL_PATH) -> Optional[float]:
    try:
        raw = path.read_text(encoding="utf-8").strip()
        if not raw:
            return None
        value = float(raw)
        if value > 1000:
            value = value / 1000.0
        return value
    except Exception:
        return None


def read_loadavg() -> Tuple[float, float, float]:
    return os.getloadavg()


def _parse_meminfo(path: Path = Path("/proc/meminfo")) -> Optional[Tuple[int, int]]:
    try:
        content = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return None

    values_kb: Dict[str, int] = {}
    for line in content.splitlines():
        m = re.match(r"^(\w+):\s+(\d+)\s+kB$", line.strip())
        if not m:
            continue
        values_kb[m.group(1)] = int(m.group(2))

    total_kb = values_kb.get("MemTotal")
    if not total_kb:
        return None

    if "MemAvailable" in values_kb:
        avail_kb = values_kb["MemAvailable"]
        used_kb = max(total_kb - avail_kb, 0)
    else:
        free_kb = values_kb.get("MemFree", 0)
        buffers_kb = values_kb.get("Buffers", 0)
        cached_kb = values_kb.get("Cached", 0)
        used_kb = max(total_kb - free_kb - buffers_kb - cached_kb, 0)

    total_mb = total_kb // 1024
    used_mb = used_kb // 1024
    return used_mb, total_mb


def _fallback_free_m() -> Optional[Tuple[int, int]]:
    try:
        proc = subprocess.run(
            ["free", "-m"],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception:
        return None

    if proc.returncode != 0:
        return None

    for line in proc.stdout.splitlines():
        stripped = line.strip()
        if not stripped.lower().startswith("mem:"):
            continue
        parts = stripped.split()
        if len(parts) < 3:
            return None
        try:
            total_mb = int(parts[1])
            used_mb = int(parts[2])
            return used_mb, total_mb
        except ValueError:
            return None

    return None


def read_memory_usage() -> Tuple[int, int, int]:
    mem = _parse_meminfo()
    if mem is None:
        mem = _fallback_free_m()
    if mem is None:
        return 0, 0, 0

    used_mb, total_mb = mem
    percent = int(round((used_mb / total_mb) * 100)) if total_mb > 0 else 0
    return used_mb, total_mb, percent


def read_disk_percent(path: str = "/") -> int:
    usage = shutil.disk_usage(path)
    if usage.total <= 0:
        return 0
    return int(round((usage.used / usage.total) * 100))


def build_message() -> str:
    temp = read_temperature_c()
    temp_str = f"{temp:.1f}" if temp is not None else "N/A"

    used_mb, total_mb, mem_pct = read_memory_usage()
    load1, load5, load15 = read_loadavg()
    disk_pct = read_disk_percent("/")

    return "\n".join(
        [
            f"🌡 系統溫度：{temp_str}°C",
            f"🧠 記憶體使用：{used_mb}MB / {total_mb}MB ({mem_pct}%)",
            f"⚖️ 系統負載：{load1:.2f}, {load5:.2f}, {load15:.2f}",
            f"💾 磁碟狀態：{disk_pct}%",
        ]
    )


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
        print(err)

    if proc.returncode == 2:
        return 0

    return proc.returncode


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect system status and notify Telegram")
    parser.add_argument("--print-only", action="store_true", help="Only print status, do not send")
    parser.add_argument("--telegram-script", default=str(DEFAULT_TELEGRAM_SCRIPT), help="Path to send_telegram.py")
    parser.add_argument("--env-file", default=str(DEFAULT_ENV_FILE), help="Path to env file for Telegram credentials")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    msg = build_message()
    print(msg)

    if args.print_only:
        return 0

    return send_telegram(msg, Path(args.telegram_script), Path(args.env_file))


if __name__ == "__main__":
    raise SystemExit(main())
