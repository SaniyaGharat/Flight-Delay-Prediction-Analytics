#!/usr/bin/env bash
# scripts/export_to_frontend.sh
# Synchronizes backend Big Data outputs to the frontend public data directory

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "================================================================="
echo "   Connecting Backend Big Data Outputs to Frontend Dashboard     "
echo "================================================================="

cd "$PROJECT_ROOT"

# Run Python synchronization script
if [ -f ".venv/bin/python" ]; then
    .venv/bin/python src/export/sync_frontend.py
else
    python3 src/export/sync_frontend.py
fi

echo "[DONE] Backend outputs connected to frontend/public/data/"
