#!/usr/bin/env bash
# ==============================================================================
# scripts/verify_part_a3_restart.sh - Startup and HDFS check after wsl --shutdown
# ==============================================================================
set -euo pipefail

cd ~/flight-delay-analytics

echo "=== 1. Starting HDFS Daemons after Cold WSL Restart ==="
~/flight-delay-analytics/scripts/start_hdfs.sh

echo ""
echo "=== 2. Current JPS Listing ==="
jps

echo ""
echo "=== 3. HDFS Recursive Listing for Raw Data ==="
hdfs dfs -ls -R /user/saniya/flight_delay/raw
