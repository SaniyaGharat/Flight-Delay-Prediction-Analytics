#!/usr/bin/env bash
set -euo pipefail

~/flight-delay-analytics/scripts/start_hdfs.sh
mkdir -p logs/verify

echo "============================================================"
echo "B4 Storage Verification: HDFS processed/ Directory (Phase 4)"
echo "============================================================"

echo "--- 1. Summary: File Count per Month for flights_enriched ---"
for m in {1..12}; do
    count=$(hdfs dfs -ls "/user/saniya/flight_delay/processed/flights_enriched/MONTH=$m" 2>/dev/null | grep '\.parquet' | wc -l || true)
    size=$(hdfs dfs -du -h "/user/saniya/flight_delay/processed/flights_enriched/MONTH=$m" 2>/dev/null | awk '{print $1, $2}' || true)
    echo "  Month $m: $count Parquet file(s) ($size)"
done

echo ""
echo "--- 2. Disk Usage Summary (hdfs dfs -du -h /user/saniya/flight_delay/processed) ---"
hdfs dfs -du -h /user/saniya/flight_delay/processed

echo ""
echo "--- 3. FSCK Health & Block Summary for processed/ ---"
hdfs fsck /user/saniya/flight_delay/processed -files -blocks -locations
