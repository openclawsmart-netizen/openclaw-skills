#!/usr/bin/env python3
import json
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

REPORT_PATH = Path(__file__).resolve().parent / "health_report.json"
LOG_DIR = Path("/root/session-logs")
DISK_THRESHOLD = 90.0
LOG_STALE_HOURS = 24.0


def _safe_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default


def get_usage_with_psutil():
    try:
        import psutil  # type: ignore

        cpu = _safe_float(psutil.cpu_percent(interval=1.0))
        mem = _safe_float(psutil.virtual_memory().percent)
        disk = _safe_float(psutil.disk_usage("/").percent)
        return {"cpu_percent": cpu, "ram_percent": mem, "disk_percent": disk, "source": "psutil"}
    except Exception:
        return None


def get_usage_fallback():
    # CPU from /proc/stat (simple snapshot; 1s interval)
    def read_cpu_times():
        with open("/proc/stat", "r", encoding="utf-8") as f:
            line = f.readline().strip()
        parts = line.split()
        nums = list(map(int, parts[1:]))
        idle = nums[3] + (nums[4] if len(nums) > 4 else 0)
        total = sum(nums)
        return total, idle

    try:
        t1, i1 = read_cpu_times()
        import time

        time.sleep(1.0)
        t2, i2 = read_cpu_times()
        total_delta = max(t2 - t1, 1)
        idle_delta = max(i2 - i1, 0)
        cpu = 100.0 * (1.0 - (idle_delta / total_delta))
    except Exception:
        cpu = 0.0

    # RAM from /proc/meminfo
    mem = 0.0
    try:
        meminfo = {}
        with open("/proc/meminfo", "r", encoding="utf-8") as f:
            for line in f:
                key, val = line.split(":", 1)
                meminfo[key.strip()] = int(val.strip().split()[0])  # kB
        total = meminfo.get("MemTotal", 0)
        avail = meminfo.get("MemAvailable", 0)
        if total > 0:
            mem = 100.0 * (1.0 - (avail / total))
    except Exception:
        mem = 0.0

    # Disk from shutil
    disk = 0.0
    try:
        du = shutil.disk_usage("/")
        if du.total > 0:
            disk = 100.0 * (du.used / du.total)
    except Exception:
        # last-resort via df
        try:
            out = subprocess.check_output(["df", "-P", "/"], text=True)
            line = out.strip().splitlines()[-1]
            usep = line.split()[4].strip("%")
            disk = _safe_float(usep)
        except Exception:
            disk = 0.0

    return {"cpu_percent": round(cpu, 2), "ram_percent": round(mem, 2), "disk_percent": round(disk, 2), "source": "fallback"}


def get_latest_log_age_hours(log_dir: Path):
    if not log_dir.exists() or not log_dir.is_dir():
        return None, None

    latest_path = None
    latest_mtime = None
    for p in log_dir.rglob("*"):
        if p.is_file():
            try:
                mt = p.stat().st_mtime
            except Exception:
                continue
            if latest_mtime is None or mt > latest_mtime:
                latest_mtime = mt
                latest_path = p

    if latest_mtime is None:
        return None, None

    now = datetime.now(timezone.utc).timestamp()
    age_hours = (now - latest_mtime) / 3600.0
    return latest_path, round(age_hours, 2)


def main():
    usage = get_usage_with_psutil() or get_usage_fallback()
    latest_log, log_age_hours = get_latest_log_age_hours(LOG_DIR)

    warnings = []
    if usage["disk_percent"] > DISK_THRESHOLD:
        warnings.append(f"Disk usage high: {usage['disk_percent']}% > {DISK_THRESHOLD}%")
    if log_age_hours is None:
        warnings.append("No logs found in /root/session-logs")
    elif log_age_hours >= LOG_STALE_HOURS:
        warnings.append(f"Latest log is stale: {log_age_hours}h >= {LOG_STALE_HOURS}h")

    status = "warning" if warnings else "ok"
    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "metrics": usage,
        "log_check": {
            "log_dir": str(LOG_DIR),
            "latest_file": str(latest_log) if latest_log else None,
            "latest_file_age_hours": log_age_hours,
            "stale_threshold_hours": LOG_STALE_HOURS,
        },
        "thresholds": {"disk_percent_warning": DISK_THRESHOLD},
        "warnings": warnings,
    }

    REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    summary = (
        f"[health] status={status} cpu={usage['cpu_percent']}% ram={usage['ram_percent']}% "
        f"disk={usage['disk_percent']}% log_age_hours={log_age_hours if log_age_hours is not None else 'N/A'}"
    )
    print(summary)
    if warnings:
        for w in warnings:
            print(f"[warn] {w}")


if __name__ == "__main__":
    main()
