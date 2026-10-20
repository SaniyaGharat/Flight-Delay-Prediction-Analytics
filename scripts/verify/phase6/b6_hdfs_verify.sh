#!/bin/bash
set -euo pipefail
cd /home/saniya/flight-delay-analytics

echo "============================================================"
echo "B6: HDFS PERSISTENCE & HEALTH VERIFICATION"
echo "============================================================"

echo "--- 1. Models Directory Listing in HDFS ---"
hdfs dfs -ls -R /user/saniya/flight_delay/processed/ml/models

echo -e "
--- 2. Predictions Directory Listing in HDFS ---"
hdfs dfs -ls -R /user/saniya/flight_delay/processed/ml/predictions_test | head -15

echo -e "
--- 3. Metrics Table Directory Listing in HDFS ---"
hdfs dfs -ls -R /user/saniya/flight_delay/processed/ml/metrics

echo -e "
--- 4. Disk Usage of ML Artifacts ---"
hdfs dfs -du -h /user/saniya/flight_delay/processed/ml

echo -e "
--- 5. HDFS Health Check (fsck) ---"
hdfs fsck /user/saniya/flight_delay/processed/ml -blocks -locations -files
