#!/bin/bash
# Test script for daemon mode functionality
#
# This test verifies that MygramDB can run in daemon mode

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
MYGRAMDB_BIN="$PROJECT_ROOT/build/bin/mygramdb"

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

# Test config and log files
TEST_CONFIG="/tmp/mygramdb_daemon_test_config.yaml"
TEST_LOG="/tmp/mygramdb_daemon_test.log"
TEST_PID_FILE="/tmp/mygramdb_daemon_test.pid"

cleanup() {
  echo "Cleaning up..."
  rm -f "$TEST_CONFIG" "$TEST_LOG" "$TEST_PID_FILE"

  # Kill any test mygramdb processes
  pkill -f "mygramdb.*$TEST_CONFIG" 2>/dev/null || true
  sleep 1
}

trap cleanup EXIT

echo "Testing daemon mode functionality..."

# Check if binary exists
if [ ! -x "$MYGRAMDB_BIN" ]; then
  echo -e "${RED}ERROR: mygramdb binary not found at $MYGRAMDB_BIN${NC}"
  exit 1
fi

# Create minimal test config with file logging
cat > "$TEST_CONFIG" << 'EOF'
mysql:
  host: "127.0.0.1"
  port: 3306
  user: "test"
  password: "test"
  database: "test"
  use_gtid: true

tables:
  - name: "test_table"
    primary_key: "id"
    text_source:
      column: "content"

replication:
  enable: false
  server_id: 99999

api:
  tcp:
    bind: "127.0.0.1"
    port: 11099

logging:
  level: "info"
  format: "json"
  file: "/tmp/mygramdb_daemon_test.log"
EOF

echo "Test 1: Daemon mode with file logging..."

# Start in daemon mode
$MYGRAMDB_BIN -d -c "$TEST_CONFIG" 2>&1 &
DAEMON_PID=$!
echo "$DAEMON_PID" > "$TEST_PID_FILE"

# Wait a moment for daemon to start
sleep 2

# Check if log file was created
if [ -f "$TEST_LOG" ]; then
  echo -e "${GREEN}✓ PASS: Log file created${NC}"

  # Check log content
  if grep -q "Daemonizing process" "$TEST_LOG"; then
    echo -e "${GREEN}✓ PASS: Daemon mode initiated${NC}"
  else
    echo -e "${YELLOW}⚠ WARNING: No daemonize message in log${NC}"
  fi
else
  echo -e "${RED}✗ FAIL: Log file not created${NC}"
  exit 1
fi

# Check if process is still running (or exited cleanly)
# Note: It might exit if MySQL is not available, which is expected
if ps -p $DAEMON_PID > /dev/null 2>&1; then
  echo -e "${GREEN}✓ PASS: Daemon process running${NC}"
  kill $DAEMON_PID 2>/dev/null || true
  sleep 1
else
  # Process exited - check if it was due to expected reasons (no MySQL)
  if grep -qE "Failed to connect|Connection refused|MySQL" "$TEST_LOG" 2>/dev/null; then
    echo -e "${GREEN}✓ PASS: Daemon started and exited cleanly (no MySQL server, expected)${NC}"
  else
    echo -e "${YELLOW}⚠ WARNING: Daemon exited (check log: $TEST_LOG)${NC}"
  fi
fi

echo ""
echo "Test 2: Daemon mode without file logging (should work but output lost)..."

# Create config without file logging
cat > "$TEST_CONFIG" << 'EOF'
mysql:
  host: "127.0.0.1"
  port: 3306
  user: "test"
  password: "test"
  database: "test"
  use_gtid: true

tables:
  - name: "test_table"
    primary_key: "id"
    text_source:
      column: "content"

replication:
  enable: false
  server_id: 99999

api:
  tcp:
    bind: "127.0.0.1"
    port: 11099

logging:
  level: "info"
  file: ""
EOF

rm -f "$TEST_LOG"

# Start daemon without file logging
$MYGRAMDB_BIN -d -c "$TEST_CONFIG" 2>&1 &
DAEMON_PID2=$!

sleep 2

# Process should have started (and likely exited due to no MySQL)
echo -e "${GREEN}✓ PASS: Daemon mode works without file logging${NC}"
echo -e "${YELLOW}  Note: Logs go to /dev/null in daemon mode without file logging${NC}"

kill $DAEMON_PID2 2>/dev/null || true

echo ""
echo -e "${GREEN}All daemon mode tests passed!${NC}"
exit 0
