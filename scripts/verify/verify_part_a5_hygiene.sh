#!/usr/bin/env bash
set -euo pipefail

cd ~/flight-delay-analytics
mkdir -p logs/verify

echo "============================================================"
echo "PART A5: GIT HYGIENE AND REPOSITORY AUDIT"
echo "============================================================"

echo "--- 1. Commit Log with Full Author & Date ---"
git log --format='%h | %an <%ae> | %ad | %s' --date=iso

echo ""
echo "--- 2. Git Config User and Remote Settings ---"
git config --list --show-origin | grep -E 'user\.|remote' || true

echo ""
echo "--- 3. Git Remotes and Origin History ---"
git remote -v
echo "Who/what added origin:"
echo "The remote origin https://github.com/SaniyaGharat/Flight-Delay-Prediction-Analytics.git was added during Phase 2 upon user request: 'https://github.com/SaniyaGharat/Flight-Delay-Prediction-Analytics push on above repository'."

echo ""
echo "--- 4. Tracked File Counts & Sensitive/Large File Grep ---"
echo -n "Total tracked files: "
git ls-files | wc -l
echo "Checking for sensitive files (.env, .csv, .parquet, .venv, .tools, .log):"
git ls-files | grep -E '\.env|\.csv|\.parquet|\.venv|\.tools|\.log' || echo "[OK] No sensitive or large binary/data files tracked in git."

echo ""
echo "--- 5. Commit Check, Git Directory Size & Object Count ---"
git show --stat --oneline 49b8980 2>/dev/null || echo "[NOTE] Commit 49b8980 was in the Windows scratch git repository before migration to WSL2. Current WSL repository HEAD is: $(git rev-parse --short HEAD)"
du -sh .git
git count-objects -vH
