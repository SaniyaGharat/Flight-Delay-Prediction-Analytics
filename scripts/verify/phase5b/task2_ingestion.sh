#!/bin/bash
set -euo pipefail
cd /home/saniya/flight-delay-analytics

echo "============================================================"
echo "TASK 2: INGESTION VERIFICATION & SCHEMA AUDIT"
echo "============================================================"

echo "--- 1. grep -n 'verify' src/ingestion/ingest_to_hdfs.py ---"
grep -n 'verify' src/ingestion/ingest_to_hdfs.py

echo -e "\n--- 2. Local raw file sizes (ls -l --time-style=full-iso) ---"
ls -l --time-style=full-iso data/raw/*.csv

echo -e "\n--- 3. Local raw byte counts (wc -c) ---"
wc -c data/raw/*.csv

echo -e "\n--- 4. Running ingest_to_hdfs.py --verify-only ---"
.venv/bin/python src/ingestion/ingest_to_hdfs.py --verify-only

echo -e "\n--- 5. Running validate_raw.py (PySpark all-string schema & row counts) ---"
.venv/bin/python src/ingestion/validate_raw.py
