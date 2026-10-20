#!/usr/bin/env bash
set -eo pipefail
source ~/.bashrc || true
export PATH="$HOME/hadoop/bin:$HOME/hadoop/sbin:$PATH"
cd ~/flight-delay-analytics
mkdir -p logs/verify

echo "=== 1. Running ingest_to_hdfs.py --verify-only ==="
.venv/bin/python src/ingestion/ingest_to_hdfs.py --verify-only

echo -e "\n=== 2. Running validate_raw.py ==="
.venv/bin/python src/ingestion/validate_raw.py
