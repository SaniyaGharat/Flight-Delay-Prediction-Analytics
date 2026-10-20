#!/usr/bin/env bash
# ==============================================================================
# scripts/verify_part_a4.sh - Phase 3 Part A4 Resources & Profiling Crash Audit
# ==============================================================================
set -euo pipefail

cd ~/flight-delay-analytics

echo "=== 1. free -h ==="
free -h

echo ""
echo "=== 2. nproc ==="
nproc

echo ""
echo "=== 3. Windows %UserProfile%\.wslconfig Check ==="
if [[ -f "/mnt/c/Users/Saniya Gharat/.wslconfig" ]]; then
    cat "/mnt/c/Users/Saniya Gharat/.wslconfig"
else
    echo "FILE_DOES_NOT_EXIST: Windows .wslconfig does not exist (WSL defaults apply)."
fi

echo ""
echo "=== 4. Spark Driver Memory Configured & Used ==="
grep -E "driver_memory|executor_memory" config/config.yaml
grep -n -C 5 "extra_spark_cfg" src/processing/profile_raw.py || true

echo ""
echo "=== 5. Git Log for profile_raw.py ==="
git log --oneline -n 5 -- src/processing/profile_raw.py
