#!/usr/bin/env bash
set -euo pipefail

~/flight-delay-analytics/scripts/start_hdfs.sh

echo "============================================================"
echo "Running src/processing/validate_clean.py..."
echo "============================================================"
.venv/bin/python src/processing/validate_clean.py

echo "============================================================"
echo "Validation finished successfully!"
echo "============================================================"
