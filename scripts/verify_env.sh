#!/usr/bin/env bash
# ==============================================================================
# verify_env.sh - Reusable environment verification script
# ==============================================================================
set -e

echo "=== 1. USER AND HOME ==="
echo "HOME=$HOME USER=$USER"

echo ""
echo "=== 2. HADOOP_HOME AND JAVA_HOME DEFINITIONS ==="
# Search startup scripts
grep -n 'HADOOP_HOME\|JAVA_HOME' ~/.bashrc ~/.profile /etc/profile.d/*.sh /etc/bash.bashrc 2>/dev/null || true

echo ""
echo "=== 3. DUPLICATE PATH ENTRIES (Must be empty) ==="
DUPES=$(echo "$PATH" | tr ':' '
' | sort | uniq -d)
if [ -z "$DUPES" ]; then
    echo "[OK] No duplicate entries found in PATH."
else
    echo "[WARNING] Found duplicate PATH entries:"
    echo "$DUPES"
fi

echo ""
echo "=== 4. RUNTIME TOOL VERSIONS ==="
echo "Java Version:"
java -version 2>&1 | head -n 2

echo ""
echo "Hadoop Version:"
hadoop version 2>&1 | head -n 2

echo ""
echo "Python System Version:"
python3 --version

if [ -f "$HOME/flight-delay-analytics/.venv/bin/python" ]; then
    echo ""
    echo "Python Project Venv Version:"
    "$HOME/flight-delay-analytics/.venv/bin/python" --version
fi

echo ""
echo "=== 5. BASHRC INTEGRITY CHECK ==="
echo "Deduplicate PATH block count in ~/.bashrc:"
grep -c 'Deduplicate PATH' ~/.bashrc || true

echo "=== VERIFICATION COMPLETE ==="
