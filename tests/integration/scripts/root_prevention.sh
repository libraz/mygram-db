#!/bin/bash
# Test script for root execution prevention
#
# This test verifies that MygramDB refuses to run as root

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
MYGRAMDB_BIN="$PROJECT_ROOT/build/bin/mygramdb"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

echo "Testing root execution prevention..."

# Check if binary exists
if [ ! -x "$MYGRAMDB_BIN" ]; then
  echo -e "${RED}ERROR: mygramdb binary not found at $MYGRAMDB_BIN${NC}"
  exit 1
fi

# Check if running as root
if [ "$(id -u)" -eq 0 ]; then
  echo "Running as root, testing that mygramdb refuses to start..."

  # Try to run mygramdb with --version (should fail)
  if $MYGRAMDB_BIN --version 2>&1 | grep -q "ERROR.*root"; then
    echo -e "${GREEN}✓ PASS: mygramdb correctly refuses to run as root${NC}"
    exit 0
  else
    echo -e "${RED}✗ FAIL: mygramdb did not refuse root execution${NC}"
    exit 1
  fi
else
  echo "Not running as root, testing that mygramdb runs normally..."

  # Try to run mygramdb with --version (should succeed)
  if OUTPUT=$($MYGRAMDB_BIN --version 2>&1); then
    if echo "$OUTPUT" | grep -q "MygramDB"; then
      echo -e "${GREEN}✓ PASS: mygramdb runs normally as non-root user${NC}"
      echo "  Version: $OUTPUT"
      exit 0
    else
      echo -e "${RED}✗ FAIL: mygramdb output unexpected${NC}"
      echo "  Output: $OUTPUT"
      exit 1
    fi
  else
    echo -e "${RED}✗ FAIL: mygramdb failed to run as non-root user${NC}"
    exit 1
  fi
fi
