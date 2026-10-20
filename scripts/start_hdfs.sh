#!/usr/bin/env bash
# ==============================================================================
# start_hdfs.sh - Idempotent HDFS startup script for WSL2
# ==============================================================================
set -euo pipefail

echo "[INFO] Ensuring SSH service is running..."
sudo service ssh start >/dev/null 2>&1 || true

# Check if NameNode and DataNode are already running
NAMENODE_PID=$(jps | grep -w "NameNode" | awk '{print $1}' || true)
DATANODE_PID=$(jps | grep -w "DataNode" | awk '{print $1}' || true)

if [[ -n "${NAMENODE_PID}" && -n "${DATANODE_PID}" ]]; then
    echo "[INFO] HDFS daemons (NameNode PID ${NAMENODE_PID}, DataNode PID ${DATANODE_PID}) are already running."
else
    echo "[INFO] Starting HDFS daemons via start-dfs.sh..."
    start-dfs.sh
fi

# Wait until safemode reports OFF
echo "[INFO] Waiting for NameNode to exit safemode..."
MAX_WAIT_SEC=60
WAIT_COUNT=0
while true; do
    SAFEMODE_STATUS=$(hdfs dfsadmin -safemode get 2>/dev/null || true)
    if echo "${SAFEMODE_STATUS}" | grep -q "Safe mode is OFF"; then
        echo "[INFO] ${SAFEMODE_STATUS}"
        break
    fi
    sleep 1
    WAIT_COUNT=$((WAIT_COUNT + 1))
    if [[ ${WAIT_COUNT} -ge ${MAX_WAIT_SEC} ]]; then
        echo "[WARN] Timed out waiting for safemode OFF after ${MAX_WAIT_SEC}s. Current status: ${SAFEMODE_STATUS}"
        break
    fi
done

echo ""
echo "=== JPS Process Listing ==="
jps

echo ""
echo "=== HDFS Listing for /user/${USER}/flight_delay ==="
hdfs dfs -ls "/user/${USER}/flight_delay"
