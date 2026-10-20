#!/bin/bash
set -euo pipefail
cd /home/saniya/flight-delay-analytics

echo "============================================================"
echo "TASK 4: FEATURE ROLES SCHEMA VALIDATION"
echo "============================================================"

.venv/bin/python scripts/verify/validate_feature_roles.py
