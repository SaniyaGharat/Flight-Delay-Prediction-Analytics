#!/bin/bash
set -euo pipefail
cd /home/saniya/flight-delay-analytics

echo "=== Phase 3 Report Before Regeneration ==="
ls -l --time-style=full-iso output/cleaning/phase3_cleaning_report.md

echo "=== Regenerating Phase 3 Report ==="
PYTHONPATH=. .venv/bin/python src/processing/write_cleaning_report.py

echo "=== Phase 3 Report After Regeneration ==="
ls -l --time-style=full-iso output/cleaning/phase3_cleaning_report.md

echo "=== Full Cat of Phase 3 Report ==="
cat output/cleaning/phase3_cleaning_report.md
