#!/bin/bash
set -euo pipefail
cd /home/saniya/flight-delay-analytics

echo "=== A1 Reconcile Log ==="
cat logs/verify/a1_a2_reconcile.txt

echo "=== A1 Ingestion Verify Log ==="
cat logs/verify/a1_ingestion_verify.txt

echo "=== Raw CSV Sizes and Line Counts ==="
ls -l --time-style=full-iso data/raw/*.csv
wc -c data/raw/*.csv
wc -l data/raw/*.csv

echo "=== HDFS Raw Directory Listing ==="
hdfs dfs -ls /user/saniya/flight_delay/raw

echo "=== Spark Distribution Queries on flights_enriched ==="
PYTHONPATH=. .venv/bin/python scripts/verify/phase6b/query_distributions.py
