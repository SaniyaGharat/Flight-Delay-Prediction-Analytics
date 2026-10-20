#!/bin/bash
set -euo pipefail
cd /home/saniya/flight-delay-analytics

echo "============================================================"
echo "TASK 7: PHASE 3 CLEANING REPORT WRITER & SCRIPT AUDIT"
echo "============================================================"

echo "--- 1. File timestamp of output/cleaning/phase3_cleaning_report.md ---"
ls -l --time-style=full-iso output/cleaning/phase3_cleaning_report.md

echo -e "\n--- 2. Grep for phase3_cleaning_report across src/processing/*.py ---"
grep -n phase3_cleaning_report src/processing/*.py

echo -e "\n--- 3. Actual join keys in src/processing/build_airport_map.py ---"
sed -n '97,126p' src/processing/build_airport_map.py

echo -e "\n--- 4. Re-running write_cleaning_report.py ---"
.venv/bin/python src/processing/write_cleaning_report.py

echo -e "\n--- 5. Full cat of output/cleaning/phase3_cleaning_report.md ---"
cat output/cleaning/phase3_cleaning_report.md
