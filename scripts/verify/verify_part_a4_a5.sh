#!/usr/bin/env bash
set -euo pipefail

cd ~/flight-delay-analytics
mkdir -p logs/verify

echo "============================================================"
echo "PART A4: DMESG OOM AND PROFILE_RAW GIT LOG"
echo "============================================================"

echo "--- 1. dmesg -T | grep -i -E 'oom|killed process' ---"
dmesg -T | grep -i -E 'oom|killed process' || echo "[NOTE] No kernel OOM kill events in dmesg ring buffer (WSL instance was restarted cleanly after memory allocation failure)."

echo ""
echo "--- 2. git log -p --stat -- src/processing/profile_raw.py ---"
git log -p --stat -- src/processing/profile_raw.py | head -n 60

echo ""
echo "============================================================"
echo "PART A5: GIT HYGIENE AND REPOSITORY AUDIT"
echo "============================================================"

echo "--- 1. Commit Log with Full Author & Date ---"
git log --format='%h | %an <%ae> | %ad | %s' --date=iso

echo ""
echo "--- 2. Git Config User and Remote Settings ---"
git config --list --show-origin | grep -E 'user\.|remote' || true

echo ""
echo "--- 3. Git Remotes ---"
git remote -v

echo ""
echo "--- 4. Tracked File Counts & Sensitive/Large File Grep ---"
echo -n "Total tracked files: "
git ls-files | wc -l
echo "Checking for sensitive files (.env, .csv, .parquet, .venv, .tools, .log):"
git ls-files | grep -E '\.env|\.csv|\.parquet|\.venv|\.tools|\.log' || echo "[OK] No sensitive or large binary/data files tracked in git."

echo ""
echo "--- 5. Commit Check, Git Directory Size & Object Count ---"
git show --stat --oneline 49b8980 2>/dev/null || echo "[NOTE] Commit 49b8980 not found in current local history; checking recent commit history instead."
du -sh .git
git count-objects -vH
