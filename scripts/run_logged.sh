#!/usr/bin/env bash
# ==============================================================================
# scripts/run_logged.sh
# Logs command execution to logs/ledger.tsv
# Format: ISO_TIMESTAMP \t COMMAND \t EXIT_CODE \t DURATION_SECONDS
# ==============================================================================
set -u

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${PROJECT_ROOT}/logs"
LEDGER_FILE="${LOG_DIR}/ledger.tsv"

mkdir -p "${LOG_DIR}"

# Initialize header if ledger does not exist
if [[ ! -f "${LEDGER_FILE}" ]]; then
    printf "timestamp\tcommand\texit_code\tduration_sec\n" > "${LEDGER_FILE}"
fi

CMD_STR="$*"
START_TS=$(date +%s.%N)
ISO_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

# Execute command
"$@"
EXIT_CODE=$?

END_TS=$(date +%s.%N)
DURATION_SEC=$(python3 -c "print(f'{(${END_TS} - ${START_TS}):.2f}')" 2>/dev/null || echo "0.00")

# Append record to ledger.tsv
printf "%s\t%s\t%d\t%s\n" "${ISO_TIMESTAMP}" "${CMD_STR}" "${EXIT_CODE}" "${DURATION_SEC}" >> "${LEDGER_FILE}"

exit ${EXIT_CODE}
