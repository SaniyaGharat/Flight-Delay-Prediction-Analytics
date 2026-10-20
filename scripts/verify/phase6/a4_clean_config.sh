#!/bin/bash
set -euo pipefail
cd /home/saniya/flight-delay-analytics

echo "============================================================"
echo "A4: CLEAN CONFIG.YAML - REMOVE STRAY CR"
echo "============================================================"

echo "--- 1. cat -A config/config.yaml | tail -5 BEFORE ---"
cat -A config/config.yaml | tail -5

echo -e "\n--- 2. Stripping carriage returns with sed ---"
sed -i 's/\r$//' config/config.yaml

echo -e "\n--- 3. cat -A config/config.yaml | tail -5 AFTER ---"
cat -A config/config.yaml | tail -5

echo -e "\n--- 4. sha256sum and git diff ---"
sha256sum config/config.yaml
git diff --stat HEAD -- config/config.yaml
