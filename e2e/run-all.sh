#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$SCRIPT_DIR"

# Parse options
MYSQL_VERSION="${MYSQL_VERSION:-8.4}"
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

# Start MySQL test container
echo "Starting MySQL test container..."
docker compose -f docker/docker-compose.yml up -d --wait --wait-timeout 120

# Install Python dependencies if needed
if ! python3 -c "import pytest" 2>/dev/null; then
    echo "Installing Python dependencies..."
    pip3 install -q -e ".[dev]" 2>/dev/null || pip3 install -q -e .
fi

# Create results directories
mkdir -p results/reports results/metrics

# Run tests (MygramDB binary is started/stopped by conftest.py)
echo "Running tests..."
python3 -m pytest tests/ "${PYTEST_ARGS[@]+"${PYTEST_ARGS[@]}"}"
EXIT_CODE=$?

# Cleanup
echo "Cleaning up MySQL container..."
docker compose -f docker/docker-compose.yml down -v

echo "=== E2E tests finished (exit code: $EXIT_CODE) ==="
exit $EXIT_CODE
