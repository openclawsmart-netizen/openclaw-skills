#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGISTRY="$ROOT_DIR/registry.json"

usage() {
  cat <<'EOF'
Usage:
  ./run-skill.sh <skill-name> [args...]

Examples:
  ./run-skill.sh health-check
  ./run-skill.sh smart-cleanup
  ./run-skill.sh web-fetch https://github.com/trending
  ./run-skill.sh daily-brief
  ./run-skill.sh daily-brief --url https://news.ycombinator.com --top 3
  ./run-skill.sh system-status
  ./run-skill.sh trade-analyst
  ./run-skill.sh ai-orchestrator
  ./run-skill.sh report-generator
EOF
}

if [[ ! -f "$REGISTRY" ]]; then
  echo "[error] registry.json not found at: $REGISTRY" >&2
  exit 1
fi

if [[ $# -lt 1 ]]; then
  echo "[error] missing skill name" >&2
  usage
  exit 2
fi

SKILL_NAME="$1"
shift || true

if ! command -v python3 >/dev/null 2>&1; then
  echo "[error] python3 is required" >&2
  exit 1
fi

if ! command -v "$SKILL_NAME" >/dev/null 2>&1; then
  :
fi

CMD="$(python3 - "$REGISTRY" "$SKILL_NAME" <<'PY'
import json,sys
registry_path=sys.argv[1]
skill=sys.argv[2]
with open(registry_path,'r',encoding='utf-8') as f:
    data=json.load(f)
entry=data.get(skill)
if not entry:
    print('')
    sys.exit(0)
print((entry.get('command') or '').strip())
PY
)"

if [[ -z "$CMD" ]]; then
  echo "[error] unknown skill: $SKILL_NAME" >&2
  echo "[hint] available skills:" >&2
  python3 - "$REGISTRY" <<'PY'
import json,sys
with open(sys.argv[1],'r',encoding='utf-8') as f:
    data=json.load(f)
for name in sorted(data.keys()):
    print(f"  - {name}")
PY
  exit 3
fi

if [[ "$SKILL_NAME" == "web-fetch" && $# -gt 0 ]]; then
  set -- --url "$1" "${@:2}"
fi

echo "[run] $SKILL_NAME"
(
  cd "$ROOT_DIR"
  python3 - "$CMD" "$@" <<'PY'
import os,shlex,subprocess,sys
cmd=shlex.split(sys.argv[1])
extra=sys.argv[2:]
full=cmd+extra
proc=subprocess.run(full)
sys.exit(proc.returncode)
PY
)
