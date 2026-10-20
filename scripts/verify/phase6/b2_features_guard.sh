#!/bin/bash
set -euo pipefail
cd /home/saniya/flight-delay-analytics

echo "============================================================"
echo "B2: PRE-DEPARTURE FEATURES & FEATURE SELECTION GUARD"
echo "============================================================"

echo "--- 1. Running Unit Test for Feature Selection Guard ---"
PYTHONPATH=. .venv/bin/python scripts/verify/phase6/b2_test_guard.py

echo -e "
--- 2. Inspecting config/ml_features.yaml ---"
cat config/ml_features.yaml

echo -e "
--- 3. Verifying All Configured Features Against Guard ---"
PYTHONPATH=. .venv/bin/python -c '
import yaml
from src.ml.guard import validate_feature_selection

with open("config/ml_features.yaml") as f:
    cfg = yaml.safe_load(f)
feats = cfg["features"]["categorical"] + cfg["features"]["numeric"]
print(f"Validating {len(feats)} pre-departure features...")
validate_feature_selection(feats)
print("SUCCESS: All pre-departure features strictly comply with leakage and time-split rules!")
'
