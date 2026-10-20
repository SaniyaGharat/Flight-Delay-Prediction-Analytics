#!/usr/bin/env bash
set -euo pipefail

cd ~/flight-delay-analytics
git add README.md scripts/ src/processing/
git commit -m "Phase 3: cleaning to Parquet"
git log -1 --stat
