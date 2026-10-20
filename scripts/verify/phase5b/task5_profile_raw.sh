#!/bin/bash
set -euo pipefail
cd /home/saniya/flight-delay-analytics

echo "============================================================"
echo "TASK 5: PROFILE_RAW.PY LINE COUNT AUDIT"
echo "============================================================"

echo "--- 1. git log --stat --oneline -- src/processing/profile_raw.py ---"
git log --stat --oneline -- src/processing/profile_raw.py

echo -e "\n--- 2. wc -l src/processing/profile_raw.py ---"
wc -l src/processing/profile_raw.py
