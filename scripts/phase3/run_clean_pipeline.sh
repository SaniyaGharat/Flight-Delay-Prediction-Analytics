#!/usr/bin/env bash
set -euo pipefail

mkdir -p logs/verify logs/cleaning

~/flight-delay-analytics/scripts/start_hdfs.sh

echo "============================================================"
echo "Running src/processing/clean_flights.py..."
echo "============================================================"
.venv/bin/python src/processing/clean_flights.py

echo "============================================================"
echo "Cleaning pipeline finished successfully!"
echo "============================================================"
