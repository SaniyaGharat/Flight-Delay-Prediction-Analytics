#!/usr/bin/env bash
# ==============================================================================
# scripts/verify/phase7/user_check.sh
# Verification script for Phase 7 PowerBI Star-Schema Model & Exports
# RULE: ONLY commands, no echo of numbers or conclusions. Headers only.
# ==============================================================================
set -euo pipefail

cd /home/saniya/flight-delay-analytics

echo "=== CSV Files Word/Line Counts ==="
wc -lc output/powerbi/*.csv

echo "=== Sample Headers and Records (head -3) ==="
for f in output/powerbi/*.csv; do
    echo "--- File: $f ---"
    head -n 3 "$f"
done

echo "=== SHA256 Checksums of PowerBI CSV Files ==="
sha256sum output/powerbi/*.csv

echo "=== HDFS Disk Usage: processed/powerbi/ ==="
hdfs dfs -du -h /user/saniya/flight_delay/processed/powerbi

echo "=== HDFS Directory Listing: processed/powerbi/ ==="
hdfs dfs -ls -R /user/saniya/flight_delay/processed/powerbi

echo "=== HDFS FSCK Health Check for processed/powerbi ==="
hdfs fsck /user/saniya/flight_delay/processed/powerbi -blocks -files

echo "=== Run Ledger (Last 15 Entries) ==="
tail -n 15 logs/ledger.tsv

echo "=== PowerBI Model Validation Suite ==="
source .venv/bin/activate
bash scripts/run_logged.sh python3 src/processing/validate_powerbi_model.py

echo "=== Git Status Short ==="
git status --short
