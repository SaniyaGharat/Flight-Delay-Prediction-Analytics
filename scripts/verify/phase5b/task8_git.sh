#!/bin/bash
set -euo pipefail
cd /home/saniya/flight-delay-analytics

echo "============================================================"
echo "TASK 8: GIT STATE AUDIT (READ-ONLY)"
echo "============================================================"

echo "--- 1. git log --format='%h | %an <%ae> | %ad | %s' --date=iso ---"
git log --format='%h | %an <%ae> | %ad | %s' --date=iso

echo -e "\n--- 2. git reflog | head -30 ---"
git reflog | head -30

echo -e "\n--- 3. git remote -v ---"
git remote -v

echo -e "\n--- 4. git branch -vv ---"
git branch -vv

echo -e "\n--- 5. git ls-remote origin ---"
git ls-remote origin

echo -e "\n--- 6. git config --list --show-origin | grep -E 'user|remote' ---"
git config --list --show-origin | grep -E 'user|remote'
