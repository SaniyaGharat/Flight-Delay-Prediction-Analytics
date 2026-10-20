#!/usr/bin/env bash
set -eo pipefail
cd ~/flight-delay-analytics

echo "=== 1. Current config/config.yaml ==="
cat config/config.yaml

echo -e "\n=== 2. git show ff9f7fe:config/config.yaml (Phase 1 commit) ==="
git show ff9f7fe:config/config.yaml

echo -e "\n=== 3. git diff HEAD~1 -- config/config.yaml ==="
git diff HEAD~1 -- config/config.yaml
