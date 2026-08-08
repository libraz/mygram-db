#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$SCRIPT_DIR"

# Parse options
MYSQL_VERSION="${MYSQL_VERSION:-8.4}"
COMPOSE_PROJECT="mygramdb-e2e"
PYTEST_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mysql-version)
            MYSQL_VERSION="$2"
            shift 2
            ;;
        --mysql-version=*)
            MYSQL_VERSION="${1#*=}"
            shift
            ;;
        *)
            PYTEST_ARGS+=("$1")
            shift
            ;;
    esac
done

export MYSQL_VERSION

echo "=== MygramDB E2E Test Suite ==="
echo "MySQL version: $MYSQL_VERSION"

# Check MygramDB binary exists
BINARY="$PROJECT_ROOT/build/bin/mygramdb"
if [ ! -x "$BINARY" ]; then
    echo "ERROR: MygramDB binary not found at $BINARY"
    echo "Build it first: cd $PROJECT_ROOT && cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build --parallel"
    exit 1
fi

# Start MySQL test container.
#
# The project name is pinned so the throwaway test database can never be
# confused with a long-lived stack. Both this file and the benchmark compose
# file define a service called "mysql", so without an explicit project the
# teardown below would tear down whichever "mysql" the ambient
# COMPOSE_PROJECT_NAME points at.
echo "Starting MySQL test container..."
docker compose -p "$COMPOSE_PROJECT" -f docker/docker-compose.yml up -d --wait --wait-timeout 120

# shellcheck source=python-env.sh
source "$SCRIPT_DIR/python-env.sh"
e2e_python_setup "$SCRIPT_DIR"

# Create results directories
mkdir -p results/reports results/metrics

# Ensure cleanup happens even if pytest (or any later step) fails
cleanup() {
    echo "Cleaning up MySQL container..."
    docker compose -p "$COMPOSE_PROJECT" -f docker/docker-compose.yml down -v
}
trap cleanup EXIT

# Run tests (MygramDB binary is started/stopped by conftest.py)
echo "Running tests..."
set +e
"$E2E_PYTHON_BIN" -m pytest "${PYTEST_ARGS[@]}"
EXIT_CODE=$?
set -e

echo "=== E2E tests finished (exit code: $EXIT_CODE) ==="
exit $EXIT_CODE
