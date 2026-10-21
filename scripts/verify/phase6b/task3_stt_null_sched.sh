#!/bin/bash
set -euo pipefail
cd /home/saniya/flight-delay-analytics

echo "=== Raw CSV Header ==="
head -n 1 data/raw/flights.csv

echo "=== Raw CSV Query: AA 803 on 2015-08-29 ==="
awk -F, 'NR==1 || ($2==8 && $3==29 && $5=="AA" && $6==803)' data/raw/flights.csv

echo "=== Raw CSV Query: Empty SCHEDULED_TIME (column 15) ==="
awk -F, 'NR==1 || (NR>1 && $15=="")' data/raw/flights.csv

echo "=== flights_clean Queries via Spark ==="
PYTHONPATH=. .venv/bin/python scripts/verify/phase6b/query_stt_clean.py
