#!/usr/bin/env bash
set -eo pipefail

~/flight-delay-analytics/scripts/start_hdfs.sh
mkdir -p logs/verify

echo "============================================================"
echo "Running Enriched Dataset & dim_date Validation (Phase 4 - B5)"
echo "============================================================"
start_ts=$(date +%s)
.venv/bin/python src/processing/validate_enriched.py
end_ts=$(date +%s)
echo "Enriched Validation completed in $((end_ts - start_ts))s"

