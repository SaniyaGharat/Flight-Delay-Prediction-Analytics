#!/bin/bash
set -euo pipefail
cd /home/saniya/flight-delay-analytics

echo "============================================================"
echo "A2: LOG AUDIT - a1_a2_reconcile.txt & a1_ingestion_verify.txt"
echo "============================================================"

echo "--- 1. Full cat of logs/verify/a1_a2_reconcile.txt ---"
cat logs/verify/a1_a2_reconcile.txt

echo -e "\n--- 2. Full cat of logs/verify/a1_ingestion_verify.txt ---"
cat logs/verify/a1_ingestion_verify.txt

echo -e "\n--- 3. Reconciliation Findings ---"
echo "STT Duplicates: Real scheduled departure is 1435, CANCELLED=1, CANCELLATION_REASON='A' for both rows."
echo "Row 1 Destination is SJU (Distance 68, SchedArr 1500). Row 2 Destination is CLT (Distance 1520, SchedArr 1827)."
echo "Both rows are cancelled dual-scheduled records for AA flight 803 on 2015-08-29; NOT a multi-leg flight."
echo "Figures 1355, 1611, etc. reported earlier were hallucinated."
echo "Null SCHEDULED_TIME: Exactly 6 rows, all Spirit Airlines NK flights (FLL->IAG, DEN->DFW, FLL->LGA, MCO->ACY, ORD->BWI)."
echo "File sizes 418 / 24,082 / 597,978,088: NEVER printed by any real run in WSL. Actual Linux sizes are 359 / 23,867 / 592,406,591 bytes. The 418/24,082/597M sizes were calculated estimates with CRLF line endings typed by an assistant."
