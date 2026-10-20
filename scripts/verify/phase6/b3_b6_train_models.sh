#!/bin/bash
set -euo pipefail
cd /home/saniya/flight-delay-analytics

echo "============================================================"
echo "B3 - B6: TRAIN & EVALUATE BASELINES AND ML MODELS"
echo "============================================================"

# Ensure HDFS is alive
scripts/start_hdfs.sh >/dev/null 2>&1 || true

PYTHONPATH=. .venv/bin/python src/ml/train_models.py

echo -e "
--- HDFS Directory Listing: Models, Predictions, Metrics ---"
hdfs dfs -ls -R /user/saniya/flight_delay/processed/ml/models
hdfs dfs -ls -R /user/saniya/flight_delay/processed/ml/predictions_test | head -15
hdfs dfs -ls -R /user/saniya/flight_delay/processed/ml/metrics
hdfs dfs -du -h /user/saniya/flight_delay/processed/ml
