#!/usr/bin/env python3
import argparse
import json
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

BASE_DIR = Path(__file__).resolve().parent
REPORT_PATH = BASE_DIR / "health_report.json"
LOG_DIR = Path("/root/session-logs")
DISK_THRESHOLD = 85.0
LOG_STALE_HOURS = 24.0
OLD_LOG_DAYS = 7

# 安全範圍：僅允許在這些目錄內刪除「本層 *.log」檔案（不遞迴）
SAFE_LOG_CLEAN_DIRS = [
    BASE_DIR,  # 至少包含 proactive-agent/report_sync.log 所在路徑
    LOG_DIR,
]


def _safe_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _action_record(action: str, success: bool, details: str, **extra) -> Dict:
    payload = {
        "timestamp": _now_iso(),
        "action": action,
        "success": bool(success),
        "details": details,
    }
    payload.update(extra)
    return payload


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


def _is_old_enough(path: Path, min_age_seconds: float) -> bool:
    try:
        age_seconds = datetime.now(timezone.utc).timestamp() - path.stat().st_mtime
        return age_seconds >= min_age_seconds
    except Exception:
        return False


def cleanup_old_logs(clean_dirs: List[Path], days: int = OLD_LOG_DAYS) -> Dict:
    min_age_seconds = float(days * 24 * 3600)
    removed: List[str] = []
    skipped: List[str] = []
    errors: List[str] = []

    for directory in clean_dirs:
        try:
            if not directory.exists() or not directory.is_dir():
                skipped.append(f"{directory} (not found)")
                continue

            # 安全策略：只處理該目錄「本層」*.log，避免危險遞迴
            for log_file in directory.glob("*.log"):
                if not log_file.is_file():
                    continue
                if not _is_old_enough(log_file, min_age_seconds):
                    continue
                try:
                    log_file.unlink()
                    removed.append(str(log_file))
                except Exception as e:
                    errors.append(f"{log_file}: {e}")
        except Exception as e:
            errors.append(f"{directory}: {e}")

    return {
        "removed_count": len(removed),
        "removed_files": removed,
        "skipped_dirs": skipped,
        "errors": errors,
        "days_threshold": days,
    }


def check_gh_available() -> Tuple[bool, str]:
    try:
        result = subprocess.run(["gh", "--version"], capture_output=True, text=True, check=True)
        line = (result.stdout or "").strip().splitlines()[0] if result.stdout else "gh ok"
        return True, line
    except Exception as e:
        return False, f"gh check failed: {e}"


def recover_gh_env_and_recheck() -> Tuple[bool, str]:
    # 使用子 shell 重新載入環境後檢查 gh
    cmd = ""
    if Path("/root/.openclaw_env").exists():
        cmd += "source /root/.openclaw_env >/dev/null 2>&1 || true; "
    cmd += "source /root/.bashrc >/dev/null 2>&1 || true; gh --version"

    try:
        result = subprocess.run(["bash", "-lc", cmd], capture_output=True, text=True, check=True)
        line = (result.stdout or "").strip().splitlines()[0] if result.stdout else "gh recovered"
        return True, line
    except Exception as e:
        return False, f"gh recovery failed: {e}"


def parse_args():
    parser = argparse.ArgumentParser(description="Health monitor with self-healing actions")
    parser.add_argument(
        "--force-disk-high",
        action="store_true",
        help="Force treat disk usage as over threshold for testing cleanup flow",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    usage = get_usage_with_psutil() or get_usage_fallback()

    # 測試機制：可由 CLI 或環境變數強制觸發
    force_disk_high = args.force_disk_high or os.getenv("FORCE_DISK_HIGH", "").strip() in {"1", "true", "TRUE", "yes", "YES"}

    latest_log, log_age_hours = get_latest_log_age_hours(LOG_DIR)
    actions_taken: List[Dict] = []

    # Healer 1: 環境復原 (gh)
    gh_ok, gh_msg = check_gh_available()
    if not gh_ok:
        actions_taken.append(_action_record("gh_check", False, gh_msg))
        recovered, rec_msg = recover_gh_env_and_recheck()
        actions_taken.append(_action_record("gh_env_reload", recovered, rec_msg))
    else:
        actions_taken.append(_action_record("gh_check", True, gh_msg))

    # Healer 2: 磁碟保護 (舊 log 清理)
    disk_value_for_heal = max(usage["disk_percent"], DISK_THRESHOLD + 0.01) if force_disk_high else usage["disk_percent"]
    disk_triggered = disk_value_for_heal > DISK_THRESHOLD
    cleanup_result: Optional[Dict] = None

    if disk_triggered:
        cleanup_result = cleanup_old_logs(SAFE_LOG_CLEAN_DIRS, days=OLD_LOG_DAYS)
        details = (
            f"disk={usage['disk_percent']}% (effective={disk_value_for_heal}%) > {DISK_THRESHOLD}%, "
            f"removed={cleanup_result['removed_count']}, errors={len(cleanup_result['errors'])}"
        )
        actions_taken.append(
            _action_record(
                "disk_log_cleanup",
                len(cleanup_result["errors"]) == 0,
                details,
                cleanup=cleanup_result,
                forced=force_disk_high,
            )
        )

    warnings = []
    if usage["disk_percent"] > DISK_THRESHOLD:
        warnings.append(f"Disk usage high: {usage['disk_percent']}% > {DISK_THRESHOLD}%")
    elif force_disk_high:
        warnings.append("Disk high simulation enabled (forced trigger)")

    if log_age_hours is None:
        warnings.append("No logs found in /root/session-logs")
    elif log_age_hours >= LOG_STALE_HOURS:
        warnings.append(f"Latest log is stale: {log_age_hours}h >= {LOG_STALE_HOURS}h")

    if not gh_ok:
        # 若 recovery 成功則降為提示，失敗則保持警告
        rec_ok = any(a["action"] == "gh_env_reload" and a["success"] for a in actions_taken)
        if not rec_ok:
            warnings.append("gh command is unavailable after environment recovery")

    status = "warning" if warnings else "ok"
    report = {
        "timestamp": _now_iso(),
        "status": status,
        "metrics": usage,
        "log_check": {
            "log_dir": str(LOG_DIR),
            "latest_file": str(latest_log) if latest_log else None,
            "latest_file_age_hours": log_age_hours,
            "stale_threshold_hours": LOG_STALE_HOURS,
        },
        "thresholds": {"disk_percent_warning": DISK_THRESHOLD},
        "healer": {
            "force_disk_high": force_disk_high,
            "disk_triggered": disk_triggered,
            "cleanup_days": OLD_LOG_DAYS,
            "safe_clean_dirs": [str(p) for p in SAFE_LOG_CLEAN_DIRS],
        },
        "actions_taken": actions_taken,
        "warnings": warnings,
    }

    REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    summary = (
        f"[health] status={status} cpu={usage['cpu_percent']}% ram={usage['ram_percent']}% "
        f"disk={usage['disk_percent']}% log_age_hours={log_age_hours if log_age_hours is not None else 'N/A'}"
    )
    print(summary)
    if actions_taken:
        print(f"[heal] actions_taken={len(actions_taken)}")
    if warnings:
        for w in warnings:
            print(f"[warn] {w}")


if __name__ == "__main__":
    main()
