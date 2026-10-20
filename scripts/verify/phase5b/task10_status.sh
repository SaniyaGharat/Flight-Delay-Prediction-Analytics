#!/bin/bash
set -euo pipefail
cd /home/saniya/flight-delay-analytics

echo "============================================================"
echo "TASK 10: GIT STATUS & DIFF AUDIT (NO COMMIT)"
echo "============================================================"

echo "--- 1. git status --short ---"
git status --short

echo -e "\n--- 2. git diff --stat ---"
git diff --stat
