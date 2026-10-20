#!/usr/bin/env bash
set -eo pipefail
cd ~/flight-delay-analytics

echo "=== 3. Running validate_enriched.py ==="
.venv/bin/python src/processing/validate_enriched.py

echo -e "\n=== 4. Running write_enrichment_report.py ==="
.venv/bin/python src/processing/write_enrichment_report.py

echo -e "\n=== 5. Listing logs/metrics/*.json ==="
cat logs/metrics/build_dim_date.json
echo ""
cat logs/metrics/build_enriched.json
echo ""
cat logs/metrics/validate_enriched.json
echo ""
cat logs/metrics/write_enrichment_report.json
echo ""
