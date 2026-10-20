#!/usr/bin/env bash
set -euo pipefail

~/flight-delay-analytics/scripts/start_hdfs.sh

echo "============================================================"
echo "B4 Storage Verification: HDFS processed/ Directory"
echo "============================================================"

echo "--- 1. Recursive Listing (hdfs dfs -ls -R) ---"
hdfs dfs -ls -R /user/saniya/flight_delay/processed

echo ""
echo "--- 2. Disk Usage Summary (hdfs dfs -du -h) ---"
hdfs dfs -du -h /user/saniya/flight_delay/processed

echo ""
echo "--- 3. FSCK Health & Block Summary ---"
hdfs fsck /user/saniya/flight_delay/processed -files -blocks -locations
