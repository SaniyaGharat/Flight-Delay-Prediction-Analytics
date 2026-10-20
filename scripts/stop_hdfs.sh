#!/usr/bin/env bash
# ==============================================================================
# stop_hdfs.sh - Idempotent HDFS shutdown script for WSL2
# ==============================================================================
set -euo pipefail

RUNNING_DAEMONS=$(jps | grep -E "NameNode|DataNode|SecondaryNameNode" || true)

if [[ -n "${RUNNING_DAEMONS}" ]]; then
    echo "[INFO] Stopping HDFS daemons..."
    stop-dfs.sh
else
    echo "[INFO] No HDFS daemons currently running."
fi

echo ""
echo "=== JPS Process Listing ==="
jps
