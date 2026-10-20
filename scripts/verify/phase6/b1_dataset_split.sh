#!/bin/bash
set -euo pipefail
cd /home/saniya/flight-delay-analytics

echo "============================================================"
echo "B1: BUILD ML DATASET & CHRONOLOGICAL SPLITS"
echo "============================================================"

PYTHONPATH=. .venv/bin/python src/ml/build_ml_dataset.py

echo -e "
--- HDFS Directory Listing of ML Dataset ---"
hdfs dfs -ls /user/saniya/flight_delay/processed/ml/dataset
