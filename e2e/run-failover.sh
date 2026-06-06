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

if ! python3 -c "import pytest" 2>/dev/null; then
    echo "Installing Python dependencies..."
    pip3 install -q -e ".[dev]" 2>/dev/null || pip3 install -q -e .
fi

mkdir -p results/reports results/metrics results/dumps-failover

cleanup() {
    echo "Cleaning up failover MySQL containers..."
    docker compose -f docker/docker-compose.failover.yml down -v
}
trap cleanup EXIT

export MYSQL_PORT=23306
export MYGRAMDB_TCP_PORT=11018
export MYGRAMDB_HTTP_PORT=20082
export MYGRAMDB_CONFIG="$SCRIPT_DIR/docker/mygramdb-test-failover.yaml"
export MYGRAMDB_LOG="/tmp/mygramdb-failover-e2e.log"
export MYGRAMDB_DUMP_DIR="$PROJECT_ROOT/e2e/results/dumps-failover"

python3 -m pytest tests/resilience/test_mysql_failover.py "$@"
