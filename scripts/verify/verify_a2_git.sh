#!/usr/bin/env bash
set -eo pipefail
cd ~/flight-delay-analytics
mkdir -p logs/verify

echo "=== 1. git config --global --list ==="
git config --global --list || true

echo -e "\n=== 2. git config --show-origin --get-all user.email ==="
git config --show-origin --get-all user.email || true

echo -e "\n=== 3. git branch -vv ==="
git branch -vv

echo -e "\n=== 4. git log origin/main --oneline ==="
git log origin/main --oneline

echo -e "\n=== 5. git reflog | head -30 ==="
git reflog | head -30
