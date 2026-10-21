#!/bin/bash
set -euo pipefail
cd /home/saniya/flight-delay-analytics

echo "=== HDFS FSCK Health Check ==="
hdfs fsck /user/saniya/flight_delay/processed/ml -blocks -locations -files

echo "=== HDFS Directory Listing: processed/ml/ ==="
hdfs dfs -ls -R /user/saniya/flight_delay/processed/ml

echo "=== HDFS Disk Usage: processed/ml/ ==="
hdfs dfs -du -h /user/saniya/flight_delay/processed/ml

echo "=== Git Status Short ==="
git status --short
