#!/usr/bin/env bash
# Verify that the real server reaches the post-fork daemon process.

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 /path/to/mygramdb" >&2
  exit 2
fi

MYGRAMDB_BIN=$1
if [[ ! -x "$MYGRAMDB_BIN" ]]; then
  echo "mygramdb binary is not executable: $MYGRAMDB_BIN" >&2
  exit 1
fi

TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/mygramdb-daemon-test.XXXXXX")
TEST_CONFIG="$TEST_DIR/config.yaml"
TEST_LOG="$TEST_DIR/mygramdb.log"
TEST_DUMP_DIR="$TEST_DIR/dumps"
TEST_PORT=$((20000 + $$ % 20000))

cleanup() {
  local pids
  pids=$(pgrep -f "$MYGRAMDB_BIN.*$TEST_CONFIG" 2>/dev/null || true)
  if [[ -n "$pids" ]]; then
    # shellcheck disable=SC2086 # pgrep intentionally returns a whitespace-separated PID list.
    kill $pids 2>/dev/null || true
  fi
  rm -rf "$TEST_DIR"
}
trap cleanup EXIT HUP INT TERM

cat >"$TEST_CONFIG" <<EOF
mysql:
  host: "127.0.0.1"
  port: 1
  user: "daemon_test"
  password: "daemon_test"
  database: "daemon_test"
  connect_timeout_ms: 100
tables:
  - name: "daemon_test"
    primary_key: "id"
    text_source:
      column: "content"
replication:
  enable: false
  auto_initial_snapshot: false
  server_id: 99999
api:
  tcp:
    bind: "127.0.0.1"
    port: $TEST_PORT
dump:
  dir: "$TEST_DUMP_DIR"
logging:
  level: "info"
  format: "json"
  file: "$TEST_LOG"
EOF

"$MYGRAMDB_BIN" --daemon --config "$TEST_CONFIG"

for _ in {1..100}; do
  DAEMON_PIDS=$(pgrep -f "$MYGRAMDB_BIN.*$TEST_CONFIG" 2>/dev/null || true)
  if [[ -n "$DAEMON_PIDS" ]]; then
    echo "daemon grandchild is running: $DAEMON_PIDS"
    exit 0
  fi
  sleep 0.05
done

echo "daemon grandchild was not observed after the launcher exited" >&2
if [[ -f "$TEST_LOG" ]]; then
  sed -n '1,120p' "$TEST_LOG" >&2
fi
exit 1
