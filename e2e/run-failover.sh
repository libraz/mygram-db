#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$SCRIPT_DIR"

MYSQL_VERSION="${MYSQL_VERSION:-8.4}"
export MYSQL_VERSION

BINARY="$PROJECT_ROOT/build/bin/mygramdb"
if [ ! -x "$BINARY" ]; then
    echo "ERROR: MygramDB binary not found at $BINARY"
    echo "Build it first: cd $PROJECT_ROOT && cmake --build build --target mygramdb --parallel"
    exit 1
fi

echo "=== MygramDB Failover E2E Test ==="
echo "MySQL version: $MYSQL_VERSION"

docker compose -f docker/docker-compose.failover.yml up -d --wait --wait-timeout 120

# shellcheck source=python-env.sh
source "$SCRIPT_DIR/python-env.sh"
e2e_python_setup "$SCRIPT_DIR"

mkdir -p results/reports results/metrics results/dumps-failover

cleanup() {
    echo "Cleaning up failover MySQL containers..."
    docker compose -f docker/docker-compose.failover.yml down -v
}
trap cleanup EXIT

export MYSQL_PORT=23306
export ENABLE_FAILOVER_TESTS=1
export MYGRAMDB_TCP_PORT=11018
export MYGRAMDB_HTTP_PORT=20082
export MYGRAMDB_CONFIG="$SCRIPT_DIR/docker/mygramdb-test-failover.yaml"
export MYGRAMDB_LOG="/tmp/mygramdb-failover-e2e.log"
export MYGRAMDB_DUMP_DIR="$PROJECT_ROOT/e2e/results/dumps-failover"

"$E2E_PYTHON_BIN" -m pytest tests/resilience/test_mysql_failover.py "$@"
