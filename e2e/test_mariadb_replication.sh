#!/usr/bin/env bash
#
# MariaDB replication E2E test
#
# Tests that MygramDB can replicate from a MariaDB server using
# MariaDB GTID format (domain-server-seq) and COM_BINLOG_DUMP protocol.
#
# Usage:
#   ./e2e/test_mariadb_replication.sh
#
# Prerequisites:
#   - Docker installed and running
#   - MygramDB built (cmake --build build --parallel)
#

set -uo pipefail
# Note: -e is intentionally omitted; test assertions handle failure tracking

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker/docker-compose.mariadb.yml"
CONFIG_FILE="$SCRIPT_DIR/docker/mygramdb-test-mariadb.yaml"
MYGRAMDB_BIN="$PROJECT_ROOT/build/bin/mygramdb"
MYGRAMDB_CLI="$PROJECT_ROOT/build/bin/mygram-cli"
LOG_FILE="/tmp/mygramdb-mariadb-e2e.log"
DUMP_DIR="$SCRIPT_DIR/results/dumps-mariadb"

# Configurable via environment variables
MARIADB_VERSION="${MARIADB_VERSION:-11.4}"
MARIADB_HOST="127.0.0.1"
MARIADB_PORT=13306
MARIADB_USER="root"
MARIADB_PASS="test_root_password"
MARIADB_DB="testdb"

MYGRAMDB_TCP_PORT=11017
MYGRAMDB_HTTP_PORT=18081

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

pass_count=0
fail_count=0

log()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*"; }

assert_eq() {
  local desc="$1" expected="$2" actual="$3"
  if [[ "$expected" == "$actual" ]]; then
    log "PASS: $desc"
    ((pass_count++))
  else
    fail "FAIL: $desc (expected='$expected', actual='$actual')"
    ((fail_count++))
  fi
}

assert_contains() {
  local desc="$1" haystack="$2" needle="$3"
  if [[ "$haystack" == *"$needle"* ]]; then
    log "PASS: $desc"
    ((pass_count++))
  else
    fail "FAIL: $desc (expected to contain '$needle', got='$haystack')"
    ((fail_count++))
  fi
}

mysql_cmd() {
  if command -v mysql &>/dev/null; then
    mysql -h "$MARIADB_HOST" -P "$MARIADB_PORT" -u "$MARIADB_USER" -p"$MARIADB_PASS" "$MARIADB_DB" -N -e "$1" 2>/dev/null
  else
    docker exec inttest_mariadb mariadb -u "$MARIADB_USER" -p"$MARIADB_PASS" "$MARIADB_DB" -N -e "$1" 2>/dev/null
  fi
}

cli_cmd() {
  "$MYGRAMDB_CLI" -h 127.0.0.1 -p "$MYGRAMDB_TCP_PORT" "$@" 2>/dev/null || true
}

cleanup() {
  log "Cleaning up..."
  # Stop MygramDB
  if [[ -n "${MYGRAMDB_PID:-}" ]] && kill -0 "$MYGRAMDB_PID" 2>/dev/null; then
    kill "$MYGRAMDB_PID" 2>/dev/null || true
    wait "$MYGRAMDB_PID" 2>/dev/null || true
  fi
  # Stop MariaDB container
  docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
  rm -rf "$DUMP_DIR" 2>/dev/null || true
}

trap cleanup EXIT

# ===========================================================================
# Setup
# ===========================================================================

log "=== MariaDB Replication E2E Test (image: mariadb:${MARIADB_VERSION}) ==="

# Check prerequisites
if [[ ! -x "$MYGRAMDB_BIN" ]]; then
  fail "MygramDB binary not found at $MYGRAMDB_BIN"
  fail "Run: cmake --build build --parallel"
  exit 1
fi

if ! command -v docker &>/dev/null; then
  fail "Docker not found"
  exit 1
fi

if ! command -v mysql &>/dev/null; then
  warn "mysql CLI not found, will use docker exec for MariaDB commands"
fi

# Start MariaDB
log "Starting MariaDB container..."
docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
docker compose -f "$COMPOSE_FILE" up -d

# Wait for MariaDB to be ready
log "Waiting for MariaDB to be ready..."
for i in $(seq 1 30); do
  if mysql_cmd "SELECT 1" &>/dev/null; then
    break
  fi
  if [[ $i -eq 30 ]]; then
    fail "MariaDB failed to start within 30s"
    docker compose -f "$COMPOSE_FILE" logs
    exit 1
  fi
  sleep 1
done
log "MariaDB is ready"

# Verify MariaDB flavor
version=$(mysql_cmd "SELECT VERSION()")
assert_contains "Server is MariaDB" "$version" "MariaDB"
log "MariaDB version: $version"

# Verify GTID is working
gtid_pos=$(mysql_cmd "SELECT @@gtid_current_pos")
log "MariaDB gtid_current_pos: $gtid_pos"

# Create dump directory
mkdir -p "$DUMP_DIR"

# Start MygramDB
log "Starting MygramDB with MariaDB config..."
"$MYGRAMDB_BIN" -c "$CONFIG_FILE" > "$LOG_FILE" 2>&1 &
MYGRAMDB_PID=$!
log "MygramDB PID: $MYGRAMDB_PID"

# Wait for MygramDB to be ready
log "Waiting for MygramDB to accept connections..."
for i in $(seq 1 30); do
  if cli_cmd INFO | grep -q "tables"; then
    break
  fi
  if ! kill -0 "$MYGRAMDB_PID" 2>/dev/null; then
    fail "MygramDB process died. Log:"
    tail -50 "$LOG_FILE"
    exit 1
  fi
  if [[ $i -eq 30 ]]; then
    fail "MygramDB failed to start. Log:"
    tail -50 "$LOG_FILE"
    exit 1
  fi
  sleep 1
done
log "MygramDB is ready"

# ===========================================================================
# Bootstrap: SYNC to build initial snapshot and start replication
# ===========================================================================
log ""
log "=== Bootstrap: SYNC ==="

sync_result=$(cli_cmd SYNC articles)
log "SYNC result: $sync_result"

# Wait for sync to complete and replication to start
log "Waiting for replication to start..."
for i in $(seq 1 30); do
  repl_status=$(cli_cmd REPLICATION STATUS)
  if [[ "$repl_status" == *"running"* ]]; then
    break
  fi
  if [[ $i -eq 30 ]]; then
    warn "Replication not running after 30s"
  fi
  sleep 1
done

# ===========================================================================
# Test 1: Verify flavor detection and replication running
# ===========================================================================
log ""
log "=== Test: Flavor Detection ==="

repl_status=$(cli_cmd REPLICATION STATUS)
log "Replication status: $repl_status"
assert_contains "Replication is running" "$repl_status" "running"

# Helper: extract result count from search output "(N results, showing M)"
search_count() {
  local n
  n=$(echo "$1" | sed -n 's/.*(\([0-9]*\) results.*/\1/p' | head -1)
  echo "${n:-0}"
}

# ===========================================================================
# Test 2: INSERT replication
# ===========================================================================
log ""
log "=== Test: INSERT Replication ==="

mysql_cmd "INSERT INTO articles (title, content, status, category, enabled) VALUES ('MariaDB Test', 'This is a test article from MariaDB replication', 1, 'test', 1)"
log "Inserted test article into MariaDB"

# Wait for replication
sleep 3

# Search for the inserted article
search_result=$(cli_cmd SEARCH articles "MariaDB replication")
log "Search result: $search_result"
count=$(search_count "$search_result")
if [[ "$count" -ge 1 ]]; then
  log "PASS: INSERT replicated ($count results found)"
  ((pass_count++))
else
  fail "FAIL: INSERT not replicated (0 results)"
  ((fail_count++))
fi

# ===========================================================================
# Test 3: UPDATE replication
# ===========================================================================
log ""
log "=== Test: UPDATE Replication ==="

mysql_cmd "UPDATE articles SET content = 'Updated content for MariaDB test article' WHERE title = 'MariaDB Test'"
log "Updated test article"

sleep 3

# Search for the updated content
search_result=$(cli_cmd SEARCH articles "Updated content MariaDB")
log "Search result: $search_result"
count=$(search_count "$search_result")
if [[ "$count" -ge 1 ]]; then
  log "PASS: UPDATE replicated ($count results found)"
  ((pass_count++))
else
  fail "FAIL: UPDATE not replicated (0 results)"
  ((fail_count++))
fi

# Old content should not be found
search_old=$(cli_cmd SEARCH articles "This is a test article from MariaDB replication")
old_count=$(search_count "$search_old")
if [[ "$old_count" -eq 0 ]]; then
  log "PASS: Old content no longer matches"
  ((pass_count++))
else
  fail "FAIL: Old content still matches after UPDATE ($old_count results)"
  ((fail_count++))
fi

# ===========================================================================
# Test 4: DELETE replication
# ===========================================================================
log ""
log "=== Test: DELETE Replication ==="

mysql_cmd "DELETE FROM articles WHERE title = 'MariaDB Test'"
log "Deleted test article"

sleep 3

search_result=$(cli_cmd SEARCH articles "Updated content MariaDB")
count=$(search_count "$search_result")
if [[ "$count" -eq 0 ]]; then
  log "PASS: DELETE replicated (article no longer found)"
  ((pass_count++))
else
  fail "FAIL: Article still found after DELETE ($count results)"
  ((fail_count++))
fi

# ===========================================================================
# Test 5: Batch INSERT
# ===========================================================================
log ""
log "=== Test: Batch INSERT ==="

mysql_cmd "INSERT INTO articles (title, content, status, category, enabled) VALUES
  ('Batch1', 'First batch article content', 1, 'batch', 1),
  ('Batch2', 'Second batch article content', 1, 'batch', 1),
  ('Batch3', 'Third batch article content', 1, 'batch', 1)"
log "Inserted 3 batch articles"

sleep 3

search_result=$(cli_cmd SEARCH articles "batch article content")
log "Search result: $search_result"
count=$(search_count "$search_result")
if [[ "$count" -ge 3 ]]; then
  log "PASS: All batch articles replicated ($count results)"
  ((pass_count++))
else
  fail "FAIL: Expected >= 3 batch results, got $count"
  ((fail_count++))
fi

# ===========================================================================
# Test 6: Japanese text
# ===========================================================================
log ""
log "=== Test: Japanese Text Replication ==="

mysql_cmd "INSERT INTO articles (title, content, status, category, enabled) VALUES ('日本語テスト', 'MariaDBからの日本語レプリケーションテストです', 1, 'japanese', 1)"
log "Inserted Japanese article"

sleep 3

search_result=$(cli_cmd SEARCH articles "日本語レプリケーション")
log "Search result: $search_result"
count=$(search_count "$search_result")
if [[ "$count" -ge 1 ]]; then
  log "PASS: Japanese text replicated ($count results)"
  ((pass_count++))
else
  fail "FAIL: Japanese text not replicated (0 results)"
  ((fail_count++))
fi

# ===========================================================================
# Test 7: GTID tracking
# ===========================================================================
log ""
log "=== Test: GTID Tracking ==="

repl_status=$(cli_cmd REPLICATION STATUS)
# MariaDB GTID format: domain-server-seq
if echo "$repl_status" | grep -qE '[0-9]+-[0-9]+-[0-9]+'; then
  log "PASS: GTID is in MariaDB format (domain-server-seq)"
  ((pass_count++))
else
  fail "FAIL: GTID not in expected MariaDB format"
  fail "Status: $repl_status"
  ((fail_count++))
fi

# ===========================================================================
# Summary
# ===========================================================================
log ""
log "=== Results ==="
log "Passed: $pass_count"
if [[ $fail_count -gt 0 ]]; then
  fail "Failed: $fail_count"
  log ""
  fail "Some tests failed. Check $LOG_FILE for MygramDB logs."
  exit 1
else
  log "Failed: 0"
  log ""
  log "All MariaDB replication E2E tests passed!"
fi

exit $fail_count
