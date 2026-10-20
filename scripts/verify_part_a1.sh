#!/usr/bin/env bash
# ==============================================================================
# scripts/verify_part_a1.sh - Phase 3 Part A1 Data Integrity Re-Verification
# ==============================================================================
set -euo pipefail

cd ~/flight-delay-analytics

# Ensure HDFS is up
if ! jps | grep -q "NameNode"; then
    echo "[INFO] HDFS not running. Starting via start_hdfs.sh..."
    ~/flight-delay-analytics/scripts/start_hdfs.sh
fi

echo "=== 1. Local data/raw/ File Listing with Full ISO Timestamps ==="
ls -l --time-style=full-iso data/raw/

echo ""
echo "=== 2. Byte (wc -c) and Line (wc -l) Counts for Local CSVs ==="
wc -c data/raw/*.csv
wc -l data/raw/*.csv

echo ""
echo "=== 3. HDFS Recursive Listing for /user/saniya/flight_delay/raw ==="
hdfs dfs -ls -R /user/saniya/flight_delay/raw

echo ""
echo "=== 4. HDFS Detailed File Stats (%n=name, %b=bytes, %y=timestamp) ==="
hdfs dfs -stat "%n %b %y" /user/saniya/flight_delay/raw/airlines/airlines.csv
hdfs dfs -stat "%n %b %y" /user/saniya/flight_delay/raw/airports/airports.csv
hdfs dfs -stat "%n %b %y" /user/saniya/flight_delay/raw/flights/flights.csv

echo ""
echo "=== 5. Checksum Verification: Local sha256sum vs HDFS sha256sum ==="
echo "Local Checksums:"
sha256sum data/raw/airlines.csv data/raw/airports.csv data/raw/flights.csv

echo ""
echo "HDFS Checksums (via hdfs dfs -cat):"
echo -n "HDFS airlines.csv: "
hdfs dfs -cat /user/saniya/flight_delay/raw/airlines/airlines.csv | sha256sum
echo -n "HDFS airports.csv: "
hdfs dfs -cat /user/saniya/flight_delay/raw/airports/airports.csv | sha256sum
echo -n "HDFS flights.csv:  "
hdfs dfs -cat /user/saniya/flight_delay/raw/flights/flights.csv | sha256sum
