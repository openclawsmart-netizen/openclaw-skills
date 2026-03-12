#!/usr/bin/env python3
"""Local desktop monitor dashboard (tkinter, bilingual zh/en).

This GUI consumes the local web API served by scripts/live_monitor_web.py.
Endpoints:
- GET  /api/snapshot
- GET  /api/jobs
- POST /api/jobs/run|enable|disable
"""

from __future__ import annotations

import json
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

try:
    import tkinter as tk
    from tkinter import ttk, messagebox
except Exception:  # pragma: no cover
    tk = None  # type: ignore[assignment]
    ttk = None  # type: ignore[assignment]
    messagebox = None  # type: ignore[assignment]

API_BASE = "http://127.0.0.1:8787"
REFRESH_MS = 3000
TIMEOUT_SEC = 4


class DashboardApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("本地監控儀表板 / Local Monitor Dashboard")
        self.root.geometry("1200x760")

        self.last_jobs: List[Dict[str, Any]] = []
        self.job_map: Dict[str, Dict[str, Any]] = {}
        self._refresh_running = False

        self._build_ui()
        self._set_status("初始化中... / Initializing...", error=False)
        self._schedule_refresh(initial=True)

    def _build_ui(self) -> None:
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(2, weight=1)

        top = ttk.Frame(self.root, padding=10)
        top.grid(row=0, column=0, sticky="ew")
        top.columnconfigure(1, weight=1)

        ttk.Label(top, text="目前狀態 / Current Status:", font=("Segoe UI", 10, "bold")).grid(row=0, column=0, sticky="w")
        self.current_status_var = tk.StringVar(value="-")
        ttk.Label(top, textvariable=self.current_status_var).grid(row=0, column=1, sticky="w", padx=(8, 16))

        ttk.Label(top, text="健康分 / Health Score:", font=("Segoe UI", 10, "bold")).grid(row=0, column=2, sticky="w")
        self.health_score_var = tk.StringVar(value="-")
        ttk.Label(top, textvariable=self.health_score_var).grid(row=0, column=3, sticky="w", padx=(8, 0))

        trans = ttk.LabelFrame(self.root, text="任務透明度 / Task Transparency", padding=10)
        trans.grid(row=1, column=0, sticky="ew", padx=10, pady=(0, 10))
        trans.columnconfigure(1, weight=1)

        self.transparency_vars = {
            "what_happened": tk.StringVar(value="-"),
            "whats_the_job": tk.StringVar(value="-"),
            "progressing": tk.StringVar(value="-"),
            "ai_routing": tk.StringVar(value="-"),
        }
        labels = [
            ("what_happened", "發生什麼事 / What happened"),
            ("whats_the_job", "現在做什麼 / What's the job"),
            ("progressing", "進度 / Progressing"),
            ("ai_routing", "AI 路由 / AI routing"),
        ]
        for idx, (key, text) in enumerate(labels):
            ttk.Label(trans, text=text).grid(row=idx, column=0, sticky="nw", pady=2)
            ttk.Label(trans, textvariable=self.transparency_vars[key], wraplength=900, justify="left").grid(
                row=idx, column=1, sticky="w", padx=(8, 0), pady=2
            )

        jobs_frame = ttk.LabelFrame(self.root, text="工作清單 / Jobs", padding=10)
        jobs_frame.grid(row=2, column=0, sticky="nsew", padx=10, pady=(0, 10))
        jobs_frame.columnconfigure(0, weight=1)
        jobs_frame.rowconfigure(0, weight=1)

        cols = ("name", "description", "status", "last_run")
        self.tree = ttk.Treeview(jobs_frame, columns=cols, show="headings", height=16)
        self.tree.heading("name", text="名稱 / Name")
        self.tree.heading("description", text="說明 / Description")
        self.tree.heading("status", text="狀態 / Status")
        self.tree.heading("last_run", text="最後執行 / Last run")
        self.tree.column("name", width=180, anchor="w")
        self.tree.column("description", width=560, anchor="w")
        self.tree.column("status", width=120, anchor="center")
        self.tree.column("last_run", width=220, anchor="center")

        vbar = ttk.Scrollbar(jobs_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vbar.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        vbar.grid(row=0, column=1, sticky="ns")

        actions = ttk.Frame(self.root, padding=(10, 0, 10, 10))
        actions.grid(row=3, column=0, sticky="ew")
        ttk.Button(actions, text="Run", command=lambda: self._on_job_action("run")).pack(side="left", padx=(0, 8))
        ttk.Button(actions, text="Enable", command=lambda: self._on_job_action("enable")).pack(side="left", padx=(0, 8))
        ttk.Button(actions, text="Disable", command=lambda: self._on_job_action("disable")).pack(side="left", padx=(0, 16))

        self.updated_var = tk.StringVar(value="Updated: -")
        ttk.Label(actions, textvariable=self.updated_var).pack(side="left")

        self.status_var = tk.StringVar(value="-")
        self.status_label = ttk.Label(self.root, textvariable=self.status_var, padding=(10, 0, 10, 10))
        self.status_label.grid(row=4, column=0, sticky="ew")

    def _set_status(self, text: str, error: bool = False) -> None:
        self.status_var.set(text)
        self.status_label.configure(foreground=("#b00020" if error else "#1f6f43"))

    def _schedule_refresh(self, initial: bool = False) -> None:
        if initial:
            self._refresh_async()
        self.root.after(REFRESH_MS, self._schedule_refresh)
        if not self._refresh_running:
            self._refresh_async()

    def _refresh_async(self) -> None:
        self._refresh_running = True

        def worker() -> None:
            snapshot: Optional[Dict[str, Any]] = None
            jobs: Optional[Dict[str, Any]] = None
            err: Optional[str] = None
            try:
                snapshot = self._http_get_json("/api/snapshot")
                jobs = self._http_get_json("/api/jobs")
            except Exception as exc:
                err = str(exc)
            self.root.after(0, lambda: self._apply_refresh(snapshot, jobs, err))

        threading.Thread(target=worker, daemon=True).start()

    def _apply_refresh(
        self,
        snapshot: Optional[Dict[str, Any]],
        jobs_payload: Optional[Dict[str, Any]],
        err: Optional[str],
    ) -> None:
        self._refresh_running = False
        if err:
            self._set_status(f"API 連線失敗 / API connection failed: {err}", error=True)
            return
        if not snapshot or not jobs_payload:
            self._set_status("資料空白 / Empty response", error=True)
            return

        status_obj = snapshot.get("status") or {}
        zh = status_obj.get("zh") or "-"
        en = status_obj.get("en") or "-"
        self.current_status_var.set(f"{zh} ({en})")
        self.health_score_var.set(str(snapshot.get("health_score", "-")))

        for key in self.transparency_vars:
            val = snapshot.get(key, "-")
            self.transparency_vars[key].set(str(val))

        self._render_jobs(jobs_payload.get("jobs") or [])
        self.updated_var.set("Updated: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        self._set_status("已同步 / Synced", error=False)

    def _render_jobs(self, jobs: List[Dict[str, Any]]) -> None:
        self.last_jobs = jobs
        self.job_map = {}

        selected_name = self._get_selected_job_name()
        for item in self.tree.get_children():
            self.tree.delete(item)

        selected_iid = None
        for idx, job in enumerate(jobs):
            name = str(job.get("jobName", ""))
            desc = str(job.get("description", ""))
            status = str(job.get("status", ""))
            last_run = str(job.get("lastRun", ""))
            iid = f"job-{idx}"
            self.job_map[iid] = job
            self.tree.insert("", "end", iid=iid, values=(name, desc, status, last_run))
            if selected_name and selected_name == name:
                selected_iid = iid

        if selected_iid:
            self.tree.selection_set(selected_iid)

    def _get_selected_job_name(self) -> Optional[str]:
        sel = self.tree.selection()
        if not sel:
            return None
        item = self.job_map.get(sel[0], {})
        name = item.get("jobName")
        return str(name) if name else None

    def _on_job_action(self, action: str) -> None:
        job_name = self._get_selected_job_name()
        if not job_name:
            messagebox.showwarning("未選擇工作 / No job selected", "請先選擇一個工作列。\nPlease select a job row first.")
            return

        endpoint = f"/api/jobs/{action}"
        self._set_status(f"執行中 / Running: {action} {job_name}", error=False)

        def worker() -> None:
            err = None
            resp: Optional[Dict[str, Any]] = None
            try:
                resp = self._http_post_json(endpoint, {"jobName": job_name})
            except Exception as exc:
                err = str(exc)
            self.root.after(0, lambda: self._after_job_action(job_name, action, resp, err))

        threading.Thread(target=worker, daemon=True).start()

    def _after_job_action(self, job_name: str, action: str, resp: Optional[Dict[str, Any]], err: Optional[str]) -> None:
        if err:
            self._set_status(f"操作失敗 / Action failed: {err}", error=True)
            return

        ok = bool(resp and resp.get("ok", False))
        msg = (resp or {}).get("message") or "-"
        if ok:
            self._set_status(f"成功 / Success: {job_name} -> {action}. {msg}", error=False)
        else:
            self._set_status(f"失敗 / Failed: {job_name} -> {action}. {msg}", error=True)
        self._refresh_async()

    def _http_get_json(self, path: str) -> Dict[str, Any]:
        req = Request(API_BASE + path, method="GET")
        try:
            with urlopen(req, timeout=TIMEOUT_SEC) as resp:
                data = resp.read().decode("utf-8", errors="replace")
        except HTTPError as e:
            detail = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else str(e)
            raise RuntimeError(f"HTTP {e.code}: {detail}") from e
        except URLError as e:
            raise RuntimeError(f"Network error: {e.reason}") from e
        return json.loads(data)

    def _http_post_json(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        body = json.dumps(payload).encode("utf-8")
        req = Request(API_BASE + path, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        try:
            with urlopen(req, timeout=TIMEOUT_SEC) as resp:
                data = resp.read().decode("utf-8", errors="replace")
        except HTTPError as e:
            detail = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else str(e)
            raise RuntimeError(f"HTTP {e.code}: {detail}") from e
        except URLError as e:
            raise RuntimeError(f"Network error: {e.reason}") from e
        return json.loads(data)


def main() -> int:
    if tk is None:
        print("[ERROR] 缺少 tkinter / tkinter is not available in this Python.")
        print("Windows: reinstall Python 3 and enable Tcl/Tk option.")
        print("Linux (Debian/Ubuntu): sudo apt install python3-tk")
        return 1

    try:
        root = tk.Tk()
    except Exception as exc:
        print("[ERROR] tkinter 無法啟動 / tkinter failed to initialize:", exc)
        print("請確認 Python 安裝含 tkinter。On Windows, reinstall Python and enable tcl/tk option.")
        return 1

    style = ttk.Style(root)
    try:
        style.theme_use("clam")
    except Exception:
        pass

    DashboardApp(root)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
