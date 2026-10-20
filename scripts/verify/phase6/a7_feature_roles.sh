#!/bin/bash
set -euo pipefail
cd /home/saniya/flight-delay-analytics

echo "============================================================"
echo "A7: FEATURE ROLES VALIDATION (70 = 70)"
echo "============================================================"

echo "--- 1. Model Use Assignments in config/feature_roles.yaml ---"
.venv/bin/python - << 'EOF'
import yaml
with open("config/feature_roles.yaml") as f:
    d = yaml.safe_load(f)["columns"]
time_cols = ["MONTH", "MONTH_NAME", "DAY", "YEAR", "FLIGHT_DATE", "SCHED_DEP_TS"]
print("Excluded Time-Split Columns:")
for c in time_cols:
    mu = d[c].get("model_use")
    sr = d[c].get("split_reason")
    print(f"  {c:20s}: {mu} | reason: {sr}")

redundant_cols = [
    "AIRLINE_NAME", "ORIGIN_AIRPORT_NAME", "ORIGIN_CITY", "ORIGIN_STATE",
    "DEST_AIRPORT_NAME", "DEST_CITY", "DEST_STATE", "SCHEDULED_DEPARTURE",
    "SCHEDULED_ARRIVAL", "ROUTE", "DAY_OF_WEEK_NAME",
    "ORIGIN_LAT", "ORIGIN_LON", "DEST_LAT", "DEST_LON"
]
print("\nExcluded Redundant Columns:")
for c in redundant_cols:
    mu = d[c].get("model_use")
    rr = d[c].get("redundancy_reason")
    print(f"  {c:20s}: {mu} | reason: {rr}")
EOF

echo -e "\n--- 2. Running validate_feature_roles.py ---"
.venv/bin/python scripts/verify/validate_feature_roles.py
