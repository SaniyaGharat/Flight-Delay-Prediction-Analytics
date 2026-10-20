#!/usr/bin/env bash
set -euo pipefail

~/flight-delay-analytics/scripts/start_hdfs.sh
mkdir -p logs/verify

.venv/bin/python scripts/verify_part_a1_a2.py
