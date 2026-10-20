#!/bin/bash
set -euo pipefail
cd /home/saniya/flight-delay-analytics

echo "============================================================"
echo "TASK 6: HONEST RUN LOG & ERROR AUDIT"
echo "============================================================"

echo "--- 1. ls -l --time-style=full-iso logs/verify/ logs/*.log ---"
ls -l --time-style=full-iso logs/verify/ logs/*.log

echo -e "\n--- 2. grep -il -E 'OutOfMemory|Killed|Traceback|IndentationError' logs/*.log logs/verify/*.txt ---"
set +e
grep -il -E 'OutOfMemory|Killed|Traceback|IndentationError' logs/*.log logs/verify/*.txt
set -e

echo -e "\n--- 3. Inspecting matching lines in error logs ---"
grep -in -E 'OutOfMemory|Killed|Traceback|IndentationError' logs/verify/a4_a5_hygiene.txt logs/verify/phase4_measured_run.txt || true

echo -e "\n--- 4. dmesg -T | grep -i -E 'oom|killed process' ---"
set +e
dmesg -T | grep -i -E 'oom|killed process'
echo exit=$?
set -e
