#!/bin/bash
#
# Run clang-tidy on all .cpp files in src/ directory
# Uses directory-specific .clang-tidy configurations
# Filters output to show only project code warnings
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get project root directory
PROJECT_DIR=$(git rev-parse --show-toplevel 2>/dev/null || pwd)

# Find clang-tidy with multiple fallback paths for macOS
CLANG_TIDY=""

# 1. Check if clang-tidy is in PATH
if command -v clang-tidy &> /dev/null; then
  CLANG_TIDY="clang-tidy"
# 2. Check Homebrew paths (Apple Silicon)
elif [ -f "/opt/homebrew/opt/llvm/bin/clang-tidy" ]; then
  CLANG_TIDY="/opt/homebrew/opt/llvm/bin/clang-tidy"
# 3. Check Homebrew paths (Intel Mac)
elif [ -f "/usr/local/opt/llvm/bin/clang-tidy" ]; then
  CLANG_TIDY="/usr/local/opt/llvm/bin/clang-tidy"
# 4. Check versioned LLVM installations (Apple Silicon)
elif [ -d "/opt/homebrew/opt" ]; then
  LLVM_VERSION=$(ls -d /opt/homebrew/opt/llvm@* 2>/dev/null | sort -V | tail -1)
  if [ -n "$LLVM_VERSION" ] && [ -f "$LLVM_VERSION/bin/clang-tidy" ]; then
    CLANG_TIDY="$LLVM_VERSION/bin/clang-tidy"
  fi
# 5. Check versioned LLVM installations (Intel Mac)
elif [ -d "/usr/local/opt" ]; then
  LLVM_VERSION=$(ls -d /usr/local/opt/llvm@* 2>/dev/null | sort -V | tail -1)
  if [ -n "$LLVM_VERSION" ] && [ -f "$LLVM_VERSION/bin/clang-tidy" ]; then
    CLANG_TIDY="$LLVM_VERSION/bin/clang-tidy"
  fi
fi

# Error if clang-tidy not found
if [ -z "$CLANG_TIDY" ]; then
  echo -e "${RED}Error: clang-tidy not found${NC}" >&2
  echo "" >&2
  echo "clang-tidy is required for static code analysis." >&2
  echo "" >&2
  echo "Installation instructions:" >&2
  echo "" >&2
  echo "  macOS (Homebrew):" >&2
  echo "    brew install llvm" >&2
  echo "" >&2
  echo "  Ubuntu/Debian:" >&2
  echo "    sudo apt-get install clang-tidy" >&2
  echo "" >&2
  echo "  Red Hat/CentOS:" >&2
  echo "    sudo yum install clang-tools-extra" >&2
  echo "" >&2
  echo "After installation, you may need to add LLVM to your PATH:" >&2
  echo "  export PATH=\"/opt/homebrew/opt/llvm/bin:\$PATH\"  # Apple Silicon" >&2
  echo "  export PATH=\"/usr/local/opt/llvm/bin:\$PATH\"     # Intel Mac" >&2
  echo "" >&2
  exit 1
fi

echo "Using clang-tidy: $CLANG_TIDY"
echo ""

# Check if build directory exists
if [ ! -d "$PROJECT_DIR/build" ]; then
  echo -e "${RED}Error: build directory not found${NC}"
  echo "Please run 'make' first to generate compile_commands.json"
  exit 1
fi

# Check if compile_commands.json exists
if [ ! -f "$PROJECT_DIR/build/compile_commands.json" ]; then
  echo -e "${RED}Error: build/compile_commands.json not found${NC}"
  echo "Please run 'make' first to generate compile_commands.json"
  exit 1
fi

# Create temporary files for output
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

LOG_FILE="$TEMP_DIR/clang-tidy-full-log.txt"
WARNINGS_FILE="$TEMP_DIR/clang-tidy-warnings.txt"

echo -e "${YELLOW}Running clang-tidy on source files (parallel)...${NC}"
echo ""

# Detect number of CPU cores
if command -v nproc &> /dev/null; then
  JOBS=$(nproc)
elif command -v sysctl &> /dev/null; then
  JOBS=$(sysctl -n hw.ncpu)
else
  JOBS=4  # Default fallback
fi

echo "Using $JOBS parallel jobs"
echo ""

# Check if run-clang-tidy is available (parallel version)
RUN_CLANG_TIDY=""
if command -v run-clang-tidy &> /dev/null; then
  RUN_CLANG_TIDY="run-clang-tidy"
elif [ -f "/opt/homebrew/opt/llvm@18/bin/run-clang-tidy" ]; then
  RUN_CLANG_TIDY="/opt/homebrew/opt/llvm@18/bin/run-clang-tidy"
elif [ -f "/opt/homebrew/opt/llvm/bin/run-clang-tidy" ]; then
  RUN_CLANG_TIDY="/opt/homebrew/opt/llvm/bin/run-clang-tidy"
elif [ -f "/usr/bin/run-clang-tidy" ]; then
  RUN_CLANG_TIDY="/usr/bin/run-clang-tidy"
fi

# Use parallel version if available, otherwise fallback to serial
if [ -n "$RUN_CLANG_TIDY" ]; then
  echo "Using parallel run-clang-tidy"
  # Run in parallel with explicit header filter to only check src/
  "$RUN_CLANG_TIDY" \
    -p "$PROJECT_DIR/build" \
    -header-filter="^$PROJECT_DIR/src/.*" \
    -config-file="$PROJECT_DIR/.clang-tidy" \
    -j "$JOBS" \
    "$PROJECT_DIR/src" \
    2>&1 | tee "$LOG_FILE"

  # Extract warnings from log
  grep -E "^$PROJECT_DIR/src/.*warning:" "$LOG_FILE" > "$WARNINGS_FILE" 2>/dev/null || true
else
  echo "run-clang-tidy not found, using serial execution"

  TOTAL_FILES=0
  # Run clang-tidy on each file individually
  for file in $(find "$PROJECT_DIR/src" -name "*.cpp" | sort); do
    TOTAL_FILES=$((TOTAL_FILES + 1))
    RELATIVE_FILE=${file#$PROJECT_DIR/}
    echo -n "Checking $RELATIVE_FILE... "

    # Run clang-tidy and capture output
    "$CLANG_TIDY" -p "$PROJECT_DIR/build" \
      --header-filter="^$PROJECT_DIR/src/.*" \
      --config-file="$PROJECT_DIR/.clang-tidy" \
      "$file" 2>&1 | tee -a "$LOG_FILE" | \
      grep -E "^$PROJECT_DIR/src/.*warning:" >> "$WARNINGS_FILE" || true

    # Count warnings for this file
    FILE_WARNINGS=$(grep -c "$RELATIVE_FILE.*warning:" "$WARNINGS_FILE" 2>/dev/null || echo "0")

    if [ "$FILE_WARNINGS" = "0" ]; then
      echo -e "${GREEN}OK${NC}"
    else
      echo -e "${YELLOW}$FILE_WARNINGS warning(s)${NC}"
    fi
  done
fi

# Count total files
TOTAL_FILES=$(find "$PROJECT_DIR/src" -name "*.cpp" | wc -l | tr -d ' ')

echo ""
echo "=================================================="

# Count total warnings from project code only
TOTAL_WARNINGS=0
if [ -f "$WARNINGS_FILE" ]; then
  TOTAL_WARNINGS=$(wc -l < "$WARNINGS_FILE" | tr -d ' ')
fi

echo "Total files checked: $TOTAL_FILES"
echo "Total warnings: $TOTAL_WARNINGS"

if [ "$TOTAL_WARNINGS" -gt 0 ]; then
  echo ""
  echo -e "${YELLOW}Warnings found:${NC}"
  echo ""
  cat "$WARNINGS_FILE"
  echo ""
  echo -e "${RED}clang-tidy found $TOTAL_WARNINGS warning(s)${NC}"
  exit 1
else
  echo ""
  echo -e "${GREEN}No warnings found! ✓${NC}"
  exit 0
fi
