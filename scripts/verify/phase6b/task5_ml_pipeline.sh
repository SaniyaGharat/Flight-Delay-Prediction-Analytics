#!/bin/bash
set -euo pipefail
cd /home/saniya/flight-delay-analytics

echo "=== Running Machine Learning Pipeline ==="
PYTHONPATH=. .venv/bin/python src/ml/train_models.py

echo "=== Generating Phase 6 ML Report ==="
PYTHONPATH=. .venv/bin/python src/ml/write_ml_report.py

echo "=== Summary Metrics JSON ==="
cat logs/metrics/ml_test_summary.json

echo "=== Full Cat of Phase 6 Report ==="
cat output/ml/phase6_report.md
