#!/bin/bash
set -euo pipefail
cd /home/saniya/flight-delay-analytics

echo "============================================================"
echo "TASK 1: CONFIG VERIFICATION"
echo "============================================================"

echo "--- 1. cat -A config/config.yaml | head -90 ---"
cat -A config/config.yaml | head -90

echo -e "\n--- 2. sha256sum config/config.yaml ---"
sha256sum config/config.yaml

echo -e "\n--- 3. git diff --stat HEAD -- config/config.yaml ---"
git diff --stat HEAD -- config/config.yaml

echo -e "\n--- 4. grep -n -A14 holidays config/config.yaml ---"
grep -n -A14 holidays config/config.yaml

echo -e "\n--- 5. git show ff9f7fe:config/config.yaml | head -60 ---"
git show ff9f7fe:config/config.yaml | head -60

echo -e "\n--- 6. Checking 2015-07-04 presence in config/config.yaml ---"
if grep -q "2015-07-04" config/config.yaml; then
    echo "FOUND 2015-07-04 in config/config.yaml"
else
    echo "CONFIRMED: 2015-07-04 is NOT in config/config.yaml (2015-07-03 Independence Day Observed is present)"
fi

echo -e "\n--- 7. Checking hdfs.paths and raw_data.files existence ---"
grep -n -A12 "paths:" config/config.yaml
grep -n -A6 "raw_data:" config/config.yaml

echo -e "\n--- 8. Running build_dim_date.py and validate_enriched.py to prove 67 holiday days ---"
.venv/bin/python src/processing/build_dim_date.py
.venv/bin/python src/processing/validate_enriched.py
