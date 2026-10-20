#!/bin/bash
set -euo pipefail
cd /home/saniya/flight-delay-analytics

echo "============================================================"
echo "A5: HONEST SPARK RUN LOG - PHASES 3, 4, 5"
echo "============================================================"

cat << 'EOF'
Summary of all failed, stalled, or retried Spark runs in Phases 3, 4, and 5:

1. Phase 3: src/processing/clean_flights.py
   - Run 1 (2026-10-20 14:56:24): Global dropDuplicates() executed across 31 unpartitioned columns on 5.82M rows.
     Execution hung/stalled for ~21 minutes at "Checking and removing exact duplicate records..." without log output.
   - Error / Reason: Cause unknown (no OOM killer or error traceback in logs/cleaning.log or dmesg). Process was manually terminated.
   - Fix / Retry: Modified clean_flights.py to repartition by MONTH before calling dropDuplicates(), which succeeded in 144.36s (15:18:13 - 15:23:07).
   - Source Log: logs/cleaning.log

2. Phase 4: src/processing/build_enriched.py
   - Run 1 (2026-10-20 16:00:20): Failed on startup due to Python indentation syntax error.
   - Error text: "IndentationError: unexpected indent" at line 49.
   - Source Log: logs/verify/phase4_measured_run.txt (line 49)
   - Fix / Retry: Corrected line indentation, re-ran immediately, and completed in 62.15s (logs/metrics/build_enriched.json).

3. Phase 5: src/processing/build_analytics.py & write_analytics_report.py
   - Indentation fix cycles:
     a) build_analytics.py: IndentationError in helper function during early script draft deployment; fixed before first complete run (52.91s).
     b) write_analytics_report.py: IndentationError during markdown table generator development; fixed before final run.
     c) validate_analytics.py: Indentation mismatch when adding Check 8 cross-validation; fixed before full validation run.

Zero kernel OOMs (dmesg -T | grep -i oom returned exit=1). No CARRIER_DELAY or FileNotFoundError errors ever occurred in logs.
EOF
