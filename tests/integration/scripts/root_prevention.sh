#!/usr/bin/env bash
# Exercise the real executable's root-rejection path when the test runs as root.

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

if [[ $(id -u) -ne 0 ]]; then
  "$MYGRAMDB_BIN" --version | grep -q 'MygramDB'
  echo "root-rejection branch skipped because CTest is not running as root"
  exit 0
fi

TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/mygramdb-root-test.XXXXXX")
TEST_CONFIG="$TEST_DIR/config.yaml"
trap 'rm -rf "$TEST_DIR"' EXIT HUP INT TERM

cat >"$TEST_CONFIG" <<EOF
mysql:
  host: "127.0.0.1"
  port: 1
  user: "root_test"
  password: "root_test"
  database: "root_test"
tables:
  - name: "root_test"
    primary_key: "id"
    text_source:
      column: "content"
dump:
  dir: "$TEST_DIR/dumps"
EOF

set +e
OUTPUT=$("$MYGRAMDB_BIN" --config "$TEST_CONFIG" 2>&1)
STATUS=$?
set -e

if [[ $STATUS -eq 0 ]]; then
  echo "mygramdb unexpectedly accepted root execution" >&2
  exit 1
fi
if ! grep -q 'Running MygramDB as root is not allowed' <<<"$OUTPUT"; then
  echo "mygramdb failed for a reason other than root rejection" >&2
  printf '%s\n' "$OUTPUT" >&2
  exit 1
fi

echo "mygramdb rejected root execution"
