#!/bin/bash
set -euo pipefail
cd /home/saniya/flight-delay-analytics

echo "============================================================"
echo "A3: PHASE 3 CLEANING REPORT GENERATOR & RECONCILIATION"
echo "============================================================"

echo "--- 1. Code in write_cleaning_report.py for timestamp, runtimes, and per-ID reconciliation ---"
grep -n -C 5 -E 'datetime\.now|total_pipeline_runtime|SUM OF ROWS|Reconciliation Note' src/processing/write_cleaning_report.py

echo -e "\n--- 2. Timestamp of output/cleaning/phase3_cleaning_report.md BEFORE ---"
ls -l --time-style=full-iso output/cleaning/phase3_cleaning_report.md

echo -e "\n--- 3. Re-running write_cleaning_report.py ---"
.venv/bin/python src/processing/write_cleaning_report.py

echo -e "\n--- 4. Timestamp of output/cleaning/phase3_cleaning_report.md AFTER ---"
ls -l --time-style=full-iso output/cleaning/phase3_cleaning_report.md

echo -e "\n--- 5. Full cat of regenerated output/cleaning/phase3_cleaning_report.md ---"
cat output/cleaning/phase3_cleaning_report.md
