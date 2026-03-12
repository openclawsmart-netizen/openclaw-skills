#!/usr/bin/env python3
"""
Generate syndicate Excel report from JSON logs.

Requirements implemented:
- Read all JSON files from logs/syndicate/
- Flatten nested fields with pandas.json_normalize
- Expand prior_trade_status into clear columns
- Add actual_outcome by comparing entry_price and current_price
- Export .xlsx with wrap text and P/L color formatting
"""

from __future__ import annotations

import glob
import json
import os
from datetime import datetime
from typing import Any

import pandas as pd
from openpyxl.styles import Alignment, PatternFill


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(ROOT_DIR, "logs", "syndicate")
REPORT_DIR = os.path.join(ROOT_DIR, "reports")


GREEN_FILL = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
RED_FILL = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")


def _load_json_rows(path: str) -> list[dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Support either one object per file or a list of objects.
    if isinstance(data, list):
        rows = [x for x in data if isinstance(x, dict)]
    elif isinstance(data, dict):
        rows = [data]
    else:
        rows = []

    for row in rows:
        row["source_file"] = os.path.basename(path)

    return rows


def _to_num(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _calc_actual_outcome(row: pd.Series) -> str:
    """
    actual_outcome rule:
    1) Infer predicted direction from take_profit_price vs entry_price:
       - take_profit_price > entry_price => LONG bias
       - take_profit_price < entry_price => SHORT bias
       - otherwise => NA
    2) Validate direction by comparing entry_price and current_price:
       - LONG: current_price >= entry_price => WIN, else LOSS
       - SHORT: current_price <= entry_price => WIN, else LOSS
    3) Missing required values => NA
    """
    entry = _to_num(row.get("entry_price"))
    tp = _to_num(row.get("take_profit_price"))

    # Prefer flat current_price if present; otherwise use expanded prior_current_price.
    current = _to_num(row.get("current_price"))
    if current is None:
        current = _to_num(row.get("prior_current_price"))

    if entry is None or tp is None or current is None:
        return "NA"

    if tp > entry:
        return "WIN" if current >= entry else "LOSS"
    if tp < entry:
        return "WIN" if current <= entry else "LOSS"

    return "NA"


def main() -> int:
    files = sorted(glob.glob(os.path.join(LOG_DIR, "*.json")))

    if not files:
        print(f"[warn] No JSON files found in: {LOG_DIR}")
        print("是否要透過 Telegram 發送檔案？")
        return 0

    all_rows: list[dict[str, Any]] = []
    for file_path in files:
        all_rows.extend(_load_json_rows(file_path))

    if not all_rows:
        print(f"[warn] No valid JSON objects found in: {LOG_DIR}")
        print("是否要透過 Telegram 發送檔案？")
        return 0

    df = pd.json_normalize(all_rows, sep="_")

    # Ensure prior_trade_status columns are explicit and clear.
    rename_map = {
        "prior_trade_status_date": "prior_date",
        "prior_trade_status_entry_price": "prior_entry_price",
        "prior_trade_status_status": "prior_status",
        "prior_trade_status_current_price": "prior_current_price",
        "prior_trade_status_profit_loss_points": "prior_profit_loss_points",
    }
    existing_renames = {k: v for k, v in rename_map.items() if k in df.columns}
    if existing_renames:
        df = df.rename(columns=existing_renames)

    df["actual_outcome"] = df.apply(_calc_actual_outcome, axis=1)

    os.makedirs(REPORT_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(REPORT_DIR, f"syndicate_report_{ts}.xlsx")

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="syndicate")
        ws = writer.book["syndicate"]

        header = [cell.value for cell in ws[1]]
        col_idx = {name: idx + 1 for idx, name in enumerate(header) if isinstance(name, str)}

        # Wrap long text columns.
        for text_col in ("analysis_reasoning", "error_review"):
            idx = col_idx.get(text_col)
            if not idx:
                continue
            for r in range(2, ws.max_row + 1):
                ws.cell(row=r, column=idx).alignment = Alignment(wrap_text=True, vertical="top")

        # Color profit/loss cells by sign. Support both field names.
        for pnl_col in ("profit_loss_points", "prior_profit_loss_points"):
            idx = col_idx.get(pnl_col)
            if not idx:
                continue
            for r in range(2, ws.max_row + 1):
                cell = ws.cell(row=r, column=idx)
                val = _to_num(cell.value)
                if val is None:
                    continue
                if val > 0:
                    cell.fill = GREEN_FILL
                elif val < 0:
                    cell.fill = RED_FILL

    print(out_path)
    print("是否要透過 Telegram 發送檔案？")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
