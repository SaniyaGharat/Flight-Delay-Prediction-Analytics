#!/bin/bash
set -euo pipefail
cd /home/saniya/flight-delay-analytics

echo "============================================================"
echo "B7: GENERATE PHASE 6 ML REPORT FROM CODE & METRICS"
echo "============================================================"

PYTHONPATH=. .venv/bin/python src/ml/write_ml_report.py

echo -e "
--- File Details & Timestamp of output/ml/phase6_report.md ---"
ls -l --time-style=full-iso output/ml/phase6_report.md

echo -e "
--- Full Cat of output/ml/phase6_report.md ---"
cat output/ml/phase6_report.md
