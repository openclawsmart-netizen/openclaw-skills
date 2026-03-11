#!/usr/bin/env python3
import argparse
import gzip
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
IMPORTANT_HISTORY_PATH = BASE_DIR / "important_history.log"
DISK_COMPRESS_THRESHOLD = 80.0
DISK_DELETE_THRESHOLD = 90.0
LOG_STALE_HOURS = 24.0
LOG_COMPRESS_DAYS = 3
GZ_DELETE_DAYS = 14

# 安全白名單：僅允許在這些目錄內處理「本層」檔案（不遞迴）
SAFE_RETENTION_DIRS = [
    BASE_DIR,
    LOG_DIR,
]

SNIPPET_KEYWORDS = ("SUCCESS", "Commit", "ERROR")


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

    mem = 0.0
    try:
        meminfo = {}
        with open("/proc/meminfo", "r", encoding="utf-8") as f:
            for line in f:
                key, val = line.split(":", 1)
                meminfo[key.strip()] = int(val.strip().split()[0])
        total = meminfo.get("MemTotal", 0)
        avail = meminfo.get("MemAvailable", 0)
        if total > 0:
            mem = 100.0 * (1.0 - (avail / total))
    except Exception:
        mem = 0.0

    disk = 0.0
    try:
        du = shutil.disk_usage("/")
        if du.total > 0:
            disk = 100.0 * (du.used / du.total)
    except Exception:
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


def _read_text_for_scan(path: Path) -> str:
    try:
        if path.suffix == ".gz":
            with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as f:
                return f.read()
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""


def backup_important_snippets(file_path: Path, history_path: Path) -> Dict:
    text = _read_text_for_scan(file_path)
    if not text:
        return {"matched": False, "match_count": 0, "history_appended": False, "lines": []}

    lines = text.splitlines()
    hit_indexes = [i for i, line in enumerate(lines) if any(k in line for k in SNIPPET_KEYWORDS)]
    if not hit_indexes:
        return {"matched": False, "match_count": 0, "history_appended": False, "lines": []}

    snippets: List[str] = []
    for idx in hit_indexes[:30]:
        start = max(0, idx - 1)
        end = min(len(lines), idx + 2)
        block = "\n".join(lines[start:end])
        snippets.append(block)

    history_path.parent.mkdir(parents=True, exist_ok=True)
    with history_path.open("a", encoding="utf-8") as out:
        out.write(f"\n==== {_now_iso()} | {file_path} ====\n")
        for s in snippets:
            out.write(s + "\n---\n")

    return {
        "matched": True,
        "match_count": len(hit_indexes),
        "history_appended": True,
        "lines": snippets,
    }


def compress_old_logs(clean_dirs: List[Path], days: int = LOG_COMPRESS_DAYS) -> Dict:
    min_age_seconds = float(days * 24 * 3600)
    compressed: List[Dict] = []
    skipped: List[str] = []
    errors: List[str] = []

    before_total = 0
    after_total = 0

    for directory in clean_dirs:
        try:
            if not directory.exists() or not directory.is_dir():
                skipped.append(f"{directory} (not found)")
                continue

            for log_file in directory.glob("*.log"):
                if not log_file.is_file() or not _is_old_enough(log_file, min_age_seconds):
                    continue
                gz_path = log_file.with_suffix(log_file.suffix + ".gz")
                if gz_path.exists():
                    skipped.append(f"{log_file} (target exists: {gz_path.name})")
                    continue

                try:
                    size_before = log_file.stat().st_size
                    with log_file.open("rb") as f_in, gzip.open(gz_path, "wb", compresslevel=6) as f_out:
                        shutil.copyfileobj(f_in, f_out)
                    size_after = gz_path.stat().st_size if gz_path.exists() else 0
                    log_file.unlink()

                    before_total += size_before
                    after_total += size_after
                    compressed.append(
                        {
                            "source": str(log_file),
                            "target": str(gz_path),
                            "size_before": size_before,
                            "size_after": size_after,
                        }
                    )
                except Exception as e:
                    errors.append(f"{log_file}: {e}")
        except Exception as e:
            errors.append(f"{directory}: {e}")

    saved = max(before_total - after_total, 0)
    ratio = round((saved / before_total) * 100, 2) if before_total > 0 else 0.0
    return {
        "compressed_count": len(compressed),
        "compressed_files": compressed,
        "skipped": skipped,
        "errors": errors,
        "days_threshold": days,
        "bytes_before": before_total,
        "bytes_after": after_total,
        "saved_bytes": saved,
        "saved_kb": round(saved / 1024, 2),
        "saved_ratio_percent": ratio,
    }


def delete_old_gz(clean_dirs: List[Path], days: int = GZ_DELETE_DAYS, history_path: Path = IMPORTANT_HISTORY_PATH) -> Dict:
    min_age_seconds = float(days * 24 * 3600)
    deleted: List[str] = []
    skipped: List[str] = []
    errors: List[str] = []
    backup_hits: List[Dict] = []

    for directory in clean_dirs:
        try:
            if not directory.exists() or not directory.is_dir():
                skipped.append(f"{directory} (not found)")
                continue

            for gz_file in directory.glob("*.gz"):
                if not gz_file.is_file() or not _is_old_enough(gz_file, min_age_seconds):
                    continue
                try:
                    backup_result = backup_important_snippets(gz_file, history_path)
                    backup_hits.append({"file": str(gz_file), **backup_result})
                    gz_file.unlink()
                    deleted.append(str(gz_file))
                except Exception as e:
                    errors.append(f"{gz_file}: {e}")
        except Exception as e:
            errors.append(f"{directory}: {e}")

    backed_up_files = sum(1 for item in backup_hits if item.get("matched"))
    backed_up_matches = sum(int(item.get("match_count", 0)) for item in backup_hits if item.get("matched"))

    return {
        "deleted_count": len(deleted),
        "deleted_files": deleted,
        "backup_hits": backup_hits,
        "backed_up_files": backed_up_files,
        "backed_up_matches": backed_up_matches,
        "skipped": skipped,
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
    parser = argparse.ArgumentParser(description="Health monitor with smart-retention actions")
    parser.add_argument(
        "--force-disk-high",
        action="store_true",
        help="Force treat disk usage as over threshold for testing retention flow",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    usage = get_usage_with_psutil() or get_usage_fallback()

    force_disk_high = args.force_disk_high or os.getenv("FORCE_DISK_HIGH", "").strip() in {"1", "true", "TRUE", "yes", "YES"}

    latest_log, log_age_hours = get_latest_log_age_hours(LOG_DIR)
    actions_taken: List[Dict] = []

    gh_ok, gh_msg = check_gh_available()
    if not gh_ok:
        actions_taken.append(_action_record("gh_check", False, gh_msg))
        recovered, rec_msg = recover_gh_env_and_recheck()
        actions_taken.append(_action_record("gh_env_reload", recovered, rec_msg))
    else:
        actions_taken.append(_action_record("gh_check", True, gh_msg))

    effective_disk = max(usage["disk_percent"], DISK_COMPRESS_THRESHOLD + 0.01) if force_disk_high else usage["disk_percent"]
    compress_triggered = effective_disk > DISK_COMPRESS_THRESHOLD
    delete_triggered = False
    compression_result: Optional[Dict] = None
    delete_result: Optional[Dict] = None

    if compress_triggered:
        compression_result = compress_old_logs(SAFE_RETENTION_DIRS, days=LOG_COMPRESS_DAYS)
        actions_taken.append(
            _action_record(
                "log_compress",
                len(compression_result["errors"]) == 0,
                (
                    f"disk={usage['disk_percent']}% (effective={effective_disk}%) > {DISK_COMPRESS_THRESHOLD}%, "
                    f"compressed={compression_result['compressed_count']}, errors={len(compression_result['errors'])}"
                ),
                compression=compression_result,
                forced=force_disk_high,
            )
        )

        post_usage = get_usage_with_psutil() or get_usage_fallback()
        post_effective_disk = max(post_usage["disk_percent"], DISK_DELETE_THRESHOLD + 0.01) if force_disk_high and usage["disk_percent"] > DISK_DELETE_THRESHOLD else post_usage["disk_percent"]
        delete_triggered = post_effective_disk > DISK_DELETE_THRESHOLD

        if delete_triggered:
            delete_result = delete_old_gz(SAFE_RETENTION_DIRS, days=GZ_DELETE_DAYS, history_path=IMPORTANT_HISTORY_PATH)
            actions_taken.append(
                _action_record(
                    "important_snippet_backup",
                    len(delete_result["errors"]) == 0,
                    (
                        f"backup_scan_before_delete files={delete_result['backed_up_files']} "
                        f"matches={delete_result['backed_up_matches']}"
                    ),
                    backup_summary={
                        "backed_up_files": delete_result["backed_up_files"],
                        "backed_up_matches": delete_result["backed_up_matches"],
                    },
                )
            )
            actions_taken.append(
                _action_record(
                    "old_gz_delete",
                    len(delete_result["errors"]) == 0,
                    (
                        f"disk_after_compress={post_usage['disk_percent']}% (effective={post_effective_disk}%) > {DISK_DELETE_THRESHOLD}%, "
                        f"deleted={delete_result['deleted_count']}, errors={len(delete_result['errors'])}"
                    ),
                    deletion=delete_result,
                )
            )

    warnings = []
    if usage["disk_percent"] > DISK_COMPRESS_THRESHOLD:
        warnings.append(f"Disk usage high: {usage['disk_percent']}% > {DISK_COMPRESS_THRESHOLD}%")
    elif force_disk_high:
        warnings.append("Disk high simulation enabled (forced trigger)")

    if log_age_hours is None:
        warnings.append("No logs found in /root/session-logs")
    elif log_age_hours >= LOG_STALE_HOURS:
        warnings.append(f"Latest log is stale: {log_age_hours}h >= {LOG_STALE_HOURS}h")

    if not gh_ok:
        rec_ok = any(a["action"] == "gh_env_reload" and a["success"] for a in actions_taken)
        if not rec_ok:
            warnings.append("gh command is unavailable after environment recovery")

    status = "warning" if warnings else "ok"
    compression_stats = {
        "enabled": True,
        "threshold_percent": DISK_COMPRESS_THRESHOLD,
        "delete_threshold_percent": DISK_DELETE_THRESHOLD,
        "compress_days": LOG_COMPRESS_DAYS,
        "delete_gz_days": GZ_DELETE_DAYS,
        "bytes_before": (compression_result or {}).get("bytes_before", 0),
        "bytes_after": (compression_result or {}).get("bytes_after", 0),
        "saved_kb": (compression_result or {}).get("saved_kb", 0.0),
        "saved_ratio_percent": (compression_result or {}).get("saved_ratio_percent", 0.0),
        "compressed_count": (compression_result or {}).get("compressed_count", 0),
    }

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
        "thresholds": {
            "disk_percent_compress": DISK_COMPRESS_THRESHOLD,
            "disk_percent_delete": DISK_DELETE_THRESHOLD,
        },
        "smart_retention": {
            "force_disk_high": force_disk_high,
            "compress_triggered": compress_triggered,
            "delete_triggered": delete_triggered,
            "safe_dirs": [str(p) for p in SAFE_RETENTION_DIRS],
            "history_log": str(IMPORTANT_HISTORY_PATH),
        },
        "compression_stats": compression_stats,
        "actions_taken": actions_taken,
        "warnings": warnings,
    }

    if compression_result is not None:
        report["compression_result"] = compression_result
    if delete_result is not None:
        report["delete_result"] = delete_result

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
