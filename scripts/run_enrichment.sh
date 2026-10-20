#!/usr/bin/env bash
set -euo pipefail

~/flight-delay-analytics/scripts/start_hdfs.sh
mkdir -p logs/verify

echo "============================================================"
echo "Running src/processing/build_enriched.py..."
echo "============================================================"
.venv/bin/python src/processing/build_enriched.py

echo "============================================================"
echo "Enrichment pipeline finished successfully!"
echo "============================================================"
