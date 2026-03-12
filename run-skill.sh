#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGISTRY="$BASE_DIR/registry.json"
VENV_ACTIVATE="$BASE_DIR/venv/bin/activate"

if [[ -f "$VENV_ACTIVATE" ]]; then
  # shellcheck disable=SC1090
  source "$VENV_ACTIVATE"
fi

load_env_file() {
  local f="$1"
  [[ -f "$f" ]] || return 0
  set -a
  # shellcheck disable=SC1090
  source "$f"
  set +a
}

# App-local env first, then global OpenClaw env as fallback/補充
load_env_file "$BASE_DIR/.env"
load_env_file "/root/.openclaw_env"

skill="${1:-}"
if [[ -z "$skill" ]]; then
  echo "Usage: $0 <skill-name> [args...]" >&2
  echo "Examples:" >&2
  echo "  $0 health-check" >&2
  echo "  $0 trade-analyst" >&2
  echo "  $0 trade-apprentice" >&2
  echo "  $0 trade-health-check" >&2
  echo "  $0 batch-review-10" >&2
  echo "  $0 monitor-dashboard" >&2
  exit 2
fi
shift || true

if [[ ! -f "$REGISTRY" ]]; then
  echo "[error] registry not found: $REGISTRY" >&2
  exit 1
fi

cmd=$(python3 - "$REGISTRY" "$skill" <<'PY'
import json, sys
p, skill = sys.argv[1], sys.argv[2]
with open(p, "r", encoding="utf-8") as f:
    data = json.load(f)
node = data.get(skill) or {}
command = (node.get("command") or "").strip()
if not command:
    print("")
    sys.exit(3)
print(command)
PY
) || {
  echo "[error] failed to resolve skill from registry: $skill" >&2
  exit 1
}

if [[ -z "$cmd" ]]; then
  echo "[error] unknown skill: $skill" >&2
  exit 1
fi

echo "[run] $skill"
exec bash -lc "$cmd \"\$@\"" -- "$@"
