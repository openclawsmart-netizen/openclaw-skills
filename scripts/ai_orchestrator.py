#!/usr/bin/env python3
"""DEPRECATED: ai_orchestrator has been retired.

Please use the unified quant entrypoint instead:
  ./run-skill.sh trade-analyst
"""

from __future__ import annotations

import sys


def main() -> int:
    print("[DEPRECATED] scripts/ai_orchestrator.py 已停用。")
    print("請改用：./run-skill.sh trade-analyst")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
