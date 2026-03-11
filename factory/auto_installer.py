#!/usr/bin/env python3
"""
Auto Installer (Safe Prototype)

Default behavior:
- Read source tutorial-like log file.
- Analyze whether content looks like a tutorial article.
- Conservatively extract shell commands and Python code blocks.
- Generate a SAFE python template containing only commented extracted snippets.
- Write dry-run report with source, decisions, and extraction summary.

Important:
- This script does NOT execute extracted commands/code.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple

ROOT = Path(__file__).resolve().parent.parent
SOURCE_PATH = ROOT / "agent-browser" / "fetched_info.log"
FACTORY_DIR = ROOT / "factory"
GENERATED_PATH = FACTORY_DIR / "generated_from_fetched.py"
REPORT_PATH = FACTORY_DIR / "dry_run_report.log"

TUTORIAL_KEYWORDS = [
    "教程", "教學", "新手", "安装", "安裝", "步骤", "步驟", "如何", "使用", "示例",
    "example", "guide", "how to", "install", "setup", "run", "powershell", "bash",
]

SHELL_LINE_RE = re.compile(r"^\s*([\w./:-]+\s+[-\w./:=\\]+(?:\s+[-\w./:=\\]+)*)\s*$")
FENCED_BLOCK_RE = re.compile(r"```(?P<lang>[a-zA-Z0-9_+-]*)\n(?P<body>[\s\S]*?)```", re.MULTILINE)

DANGEROUS_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"\brm\s+-rf\b",
        r"\bdel\s+",
        r"\bformat\b",
        r"\bmkfs\b",
        r"\bshutdown\b",
        r"\breboot\b",
        r"\bcurl\b.*\|",
        r"\bwget\b.*\|",
        r"\biex\b",
    ]
]


def classify_tutorial(text: str) -> Tuple[bool, List[str]]:
    lowered = text.lower()
    hits = [kw for kw in TUTORIAL_KEYWORDS if kw.lower() in lowered]
    is_tutorial = len(hits) >= 3
    return is_tutorial, hits


def looks_like_shell_command(line: str) -> bool:
    s = line.strip()
    if not s or len(s) < 4:
        return False
    if s.startswith(("title:", "summary_500:", "http://", "https://")):
        return False
    if s.endswith(("：", ":")):
        return False
    if " " not in s:
        return False
    if re.search(r"[\u4e00-\u9fff]", s):
        return False
    if s.count("|") > 1:
        return False
    return bool(SHELL_LINE_RE.match(s))


def extract_shell_commands(text: str) -> List[str]:
    commands = []
    for raw in text.splitlines():
        line = raw.strip()
        if looks_like_shell_command(line):
            commands.append(line)
    # de-duplicate while preserving order
    seen = set()
    out = []
    for c in commands:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def extract_python_blocks(text: str) -> List[str]:
    blocks: List[str] = []
    for m in FENCED_BLOCK_RE.finditer(text):
        lang = (m.group("lang") or "").strip().lower()
        body = (m.group("body") or "").strip("\n")
        if not body:
            continue
        if lang in {"py", "python"}:
            blocks.append(body)
        elif not lang:
            # Conservative fallback: include only if it strongly resembles Python.
            if re.search(r"\bdef\b|\bimport\b|\bclass\b", body) and ":" in body:
                blocks.append(body)
    return blocks


def mark_risk(cmd: str) -> str:
    for pat in DANGEROUS_PATTERNS:
        if pat.search(cmd):
            return "HIGH"
    return "LOW"


def build_generated_py(source: Path, is_tutorial: bool, keyword_hits: List[str], shell_cmds: List[str], py_blocks: List[str]) -> str:
    ts = datetime.now(timezone.utc).isoformat()
    lines = [
        "#!/usr/bin/env python3",
        '"""',
        "Generated from fetched content (SAFE MODE).",
        "",
        "This file intentionally does NOT execute extracted shell commands or code.",
        "All snippets are preserved as comments for manual review.",
        '"""',
        "",
        f"SOURCE_FILE = {source.as_posix()!r}",
        f"GENERATED_AT_UTC = {ts!r}",
        f"IS_TUTORIAL = {is_tutorial}",
        f"TUTORIAL_KEYWORDS_HIT = {keyword_hits!r}",
        "",
        "def main() -> None:",
        "    print('SAFE MODE: no extracted command/code will be executed.')",
        "    print('Review commented snippets in this file manually.')",
        "",
        "",
        "# ---- Extracted shell commands (commented out) ----",
    ]

    if shell_cmds:
        for i, cmd in enumerate(shell_cmds, 1):
            lines.append(f"# [{i}] {cmd}")
    else:
        lines.append("# (none)")

    lines.extend(["", "# ---- Extracted python blocks (commented out) ----"])
    if py_blocks:
        for i, block in enumerate(py_blocks, 1):
            lines.append(f"# [python-block-{i}] BEGIN")
            for bline in block.splitlines():
                lines.append(f"# {bline}")
            lines.append(f"# [python-block-{i}] END")
            lines.append("#")
    else:
        lines.append("# (none)")

    lines.extend([
        "",
        "if __name__ == '__main__':",
        "    main()",
        "",
    ])
    return "\n".join(lines)


def build_report(source: Path, is_tutorial: bool, keyword_hits: List[str], shell_cmds: List[str], py_blocks: List[str]) -> str:
    now_local = datetime.now().astimezone().isoformat()
    risk_lines = [f"- [{mark_risk(c)}] {c}" for c in shell_cmds] or ["- (none)"]

    report = [
        f"[dry-run] generated_at={now_local}",
        f"source_file={source.as_posix()}",
        f"source_exists={source.exists()}",
        "",
        "[tutorial_classification]",
        f"is_tutorial={is_tutorial}",
        f"keyword_hits_count={len(keyword_hits)}",
        f"keyword_hits={', '.join(keyword_hits) if keyword_hits else '(none)'}",
        "",
        "[extraction_summary]",
        f"shell_commands_count={len(shell_cmds)}",
        f"python_blocks_count={len(py_blocks)}",
        "",
        "[shell_commands]",
        *risk_lines,
        "",
        "[python_blocks_preview]",
    ]

    if py_blocks:
        for i, block in enumerate(py_blocks, 1):
            preview = " | ".join(line.strip() for line in block.splitlines()[:3])
            report.append(f"- block_{i}: {preview[:220]}")
    else:
        report.append("- (none)")

    report.extend([
        "",
        "[planned_behavior_if_enabled_in_future]",
        "- Could run reviewed installer steps in a guarded allowlist mode.",
        "- Could require explicit user confirmation before each command.",
        "- Could block HIGH-risk commands unless an override is provided.",
        "",
        "[current_behavior]",
        "- Analysis only.",
        "- Writes generated_from_fetched.py (commented snippets only).",
        "- Writes this dry_run_report.log.",
        "- Executes nothing from extracted content.",
        "",
    ])

    return "\n".join(report)


def main() -> int:
    FACTORY_DIR.mkdir(parents=True, exist_ok=True)

    if not SOURCE_PATH.exists():
        REPORT_PATH.write_text(
            "[dry-run] source missing\n"
            f"source_file={SOURCE_PATH.as_posix()}\n"
            "No action taken.\n",
            encoding="utf-8",
        )
        return 1

    text = SOURCE_PATH.read_text(encoding="utf-8", errors="replace")
    is_tutorial, keyword_hits = classify_tutorial(text)
    shell_cmds = extract_shell_commands(text)
    py_blocks = extract_python_blocks(text)

    generated = build_generated_py(SOURCE_PATH, is_tutorial, keyword_hits, shell_cmds, py_blocks)
    report = build_report(SOURCE_PATH, is_tutorial, keyword_hits, shell_cmds, py_blocks)

    GENERATED_PATH.write_text(generated, encoding="utf-8")
    REPORT_PATH.write_text(report, encoding="utf-8")

    print(f"[ok] wrote {GENERATED_PATH}")
    print(f"[ok] wrote {REPORT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
