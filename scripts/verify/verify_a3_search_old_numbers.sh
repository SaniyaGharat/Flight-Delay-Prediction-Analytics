#!/usr/bin/env bash
set -eo pipefail
cd ~/flight-delay-analytics
mkdir -p logs/verify

echo "=== Searching for old numbers in logs, output, scripts, src ==="
grep -rn "1292" logs/ output/ src/ scripts/ 2>/dev/null || true
grep -rn "15323" logs/ output/ src/ scripts/ 2>/dev/null || true
