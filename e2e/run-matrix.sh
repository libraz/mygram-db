#!/bin/bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$SCRIPT_DIR"

# Parse options
PYTEST_ARGS=()
TARGETS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --only)
            IFS=',' read -ra TARGETS <<< "$2"
            shift 2
            ;;
        *)
            PYTEST_ARGS+=("$1")
            shift
            ;;
    esac
done

# Default: all targets
if [[ ${#TARGETS[@]} -eq 0 ]]; then
    TARGETS=("mysql:8.4" "mysql:9.4" "mariadb:10.11" "mariadb:11.4")
fi

# Check binary
BINARY="$PROJECT_ROOT/build/bin/mygramdb"
if [[ ! -x "$BINARY" ]]; then
    echo "ERROR: MygramDB binary not found at $BINARY"
    exit 1
fi

# Install deps
if ! python3 -c "import pytest" 2>/dev/null; then
    echo "Installing Python dependencies..."
    pip3 install -q -e ".[dev]" 2>/dev/null || pip3 install -q -e .
fi

mkdir -p results/reports

echo "============================================="
echo " MygramDB E2E Matrix Test"
echo " Targets: ${TARGETS[*]}"
echo "============================================="
echo ""

overall_pass=0
overall_fail=0
declare -a summary_lines

for target in "${TARGETS[@]}"; do
    flavor="${target%%:*}"
    version="${target##*:}"

    echo "============================================="
    echo " $flavor $version"
    echo "============================================="

    # Select compose file and env vars
    if [[ "$flavor" == "mariadb" ]]; then
        compose_file="docker/docker-compose.mariadb.yml"
        export MARIADB_VERSION="$version"
        export DB_FLAVOR="mariadb"
        export MYSQL_VERSION="$version"
    else
        compose_file="docker/docker-compose.yml"
        export MYSQL_VERSION="$version"
        export DB_FLAVOR="mysql"
        unset MARIADB_VERSION 2>/dev/null || true
    fi

    # Stop any previous containers
    docker compose -f "docker/docker-compose.yml" down -v 2>/dev/null || true
    docker compose -f "docker/docker-compose.mariadb.yml" down -v 2>/dev/null || true

    # Start database
    echo "Starting $flavor $version..."
    docker compose -f "$compose_file" up -d --wait --wait-timeout 120 2>&1

    if [[ $? -ne 0 ]]; then
        echo "FAIL: Could not start $flavor $version"
        summary_lines+=("FAIL  $flavor $version  (container start failed)")
        ((overall_fail++))
        continue
    fi

    # Run pytest
    echo "Running tests..."
    report_file="results/reports/${flavor}-${version}.xml"
    python3 -m pytest tests/ \
        --tb=short -q \
        --junitxml="$report_file" \
        "${PYTEST_ARGS[@]+"${PYTEST_ARGS[@]}"}" \
        2>&1
    rc=$?

    # Collect summary
    if [[ $rc -eq 0 ]]; then
        summary_lines+=("PASS  $flavor $version")
        ((overall_pass++))
    else
        summary_lines+=("FAIL  $flavor $version  (exit code $rc)")
        ((overall_fail++))
    fi

    # Cleanup
    docker compose -f "$compose_file" down -v 2>/dev/null || true
    echo ""
done

# Final summary
echo "============================================="
echo " Matrix Summary"
echo "============================================="
for line in "${summary_lines[@]}"; do
    echo "  $line"
done
echo ""
echo "  Pass: $overall_pass / $((overall_pass + overall_fail))"

if [[ $overall_fail -gt 0 ]]; then
    echo ""
    echo "FAILED: $overall_fail target(s) had failures."
    exit 1
fi

echo ""
echo "All targets passed!"
exit 0
