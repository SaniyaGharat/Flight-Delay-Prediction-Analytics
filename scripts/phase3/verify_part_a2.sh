#!/usr/bin/env bash
# ==============================================================================
# scripts/verify_part_a2.sh - Phase 3 Part A2 Scripts & Environment Verification
# ==============================================================================
set -euo pipefail

cd ~/flight-delay-analytics

echo "=== 1. Git Log (--stat -5) ==="
git log --stat -5

echo ""
echo "=== 2. Git Remote (-v) ==="
git remote -v

echo ""
echo "=== 3. Git Branches (-a) ==="
git branch -a

echo ""
echo "=== 4. test_setup.py Listing, Header, and Git History ==="
ls -l --time-style=full-iso src/utils/test_setup.py
head -n 30 src/utils/test_setup.py
git log --oneline -- src/utils/test_setup.py

echo ""
echo "=== 5. ingest_to_hdfs.py Listing, Header, and Git History ==="
ls -l --time-style=full-iso src/ingestion/ingest_to_hdfs.py
head -n 30 src/ingestion/ingest_to_hdfs.py
git log --oneline -- src/ingestion/ingest_to_hdfs.py

# Ensure HDFS is running before executing test_setup.py and ingest_to_hdfs.py
if ! jps | grep -q "NameNode"; then
    echo ""
    echo "[INFO] HDFS not running. Starting via start_hdfs.sh..."
    ~/flight-delay-analytics/scripts/start_hdfs.sh
fi

echo ""
echo "=== 6. Running src/utils/test_setup.py ==="
.venv/bin/python src/utils/test_setup.py

echo ""
echo "=== 7. Running src/ingestion/ingest_to_hdfs.py --verify-only ==="
.venv/bin/python src/ingestion/ingest_to_hdfs.py --verify-only
