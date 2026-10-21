#!/bin/bash
set -euo pipefail
cd /home/saniya/flight-delay-analytics

echo "=== Grep Ultra Long in Codebase ==="
grep -rn "Ultra Long" scripts/ src/ config/ || true

echo "=== Grep 2912583 in Codebase ==="
grep -rn "2912583" scripts/ src/ config/ || true

echo "=== Git Log of validate_enriched.py ==="
git log --oneline -p -n 3 src/processing/validate_enriched.py

echo "=== Task1 Config DISTANCE_BAND Section ==="
grep -A 10 "DISTANCE_BAND Distribution" logs/verify/phase5b/task1_config.txt
