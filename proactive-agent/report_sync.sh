#!/usr/bin/env bash
set -euo pipefail

# Load shell environment for git/gh if available
if [[ -f /root/.bashrc ]]; then
  # shellcheck disable=SC1091
  set +u
  source /root/.bashrc
  set -u
fi

REPO_DIR="/root/openclaw-skills"
cd "$REPO_DIR"

python3 proactive-agent/monitor_health.py

# Stage required files
FILES=(
  "proactive-agent/health_report.json"
  "proactive-agent/monitor_health.py"
  "proactive-agent/report_sync.sh"
)

git add "${FILES[@]}"

if git diff --cached --quiet; then
  echo "[git] No changes to commit."
else
  msg="chore(proactive-agent): update health report $(date -u +'%Y-%m-%dT%H:%M:%SZ')"
  git commit -m "$msg"
fi

git push origin HEAD

echo "[done] report_sync completed"
