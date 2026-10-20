#!/usr/bin/env bash
# ==============================================================================
# scripts/verify_part_a3.sh - Phase 3 Part A3 Environment & Persistence Check
# ==============================================================================
set -euo pipefail

echo "=== 1. sed -n 1,15p ~/.bashrc ==="
sed -n '1,15p' ~/.bashrc

echo ""
echo "=== 2. tail -n 30 ~/.bashrc ==="
tail -n 30 ~/.bashrc

echo ""
echo "=== 3. grep -n 'Deduplicate' ~/.bashrc ==="
grep -n 'Deduplicate' ~/.bashrc

echo ""
echo "=== 4. grep -n 'HADOOP\|JAVA_HOME' ~/.bashrc ==="
grep -n 'HADOOP\|JAVA_HOME' ~/.bashrc

echo ""
echo "=== 5. java -version ==="
java -version 2>&1

echo ""
echo "=== 6. cat /etc/wsl.conf ==="
cat /etc/wsl.conf

echo ""
echo "=== 7. grep -n KillUserProcesses /etc/systemd/logind.conf ==="
grep -n 'KillUserProcesses' /etc/systemd/logind.conf

echo ""
echo "=== 8. loginctl show-user saniya | grep Linger ==="
loginctl show-user saniya | grep Linger

echo ""
echo "=== 9. Running scripts/verify_env.sh ==="
~/flight-delay-analytics/scripts/verify_env.sh
