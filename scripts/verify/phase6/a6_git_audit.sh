#!/bin/bash
set -euo pipefail
cd /home/saniya/flight-delay-analytics

echo "============================================================"
echo "A6: GIT REFLOG & REMOTE STATE AUDIT"
echo "============================================================"

echo "--- 1. Full git reflog ---"
git reflog

echo -e "\n--- 2. git ls-remote origin ---"
git ls-remote origin

echo -e "\n--- 3. Remote Tracking Status ---"
git branch -vv
