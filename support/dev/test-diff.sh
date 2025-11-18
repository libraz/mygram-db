#!/bin/bash
# test-diff.sh - Run tests affected by git changes
#
# Usage:
#   ./support/dev/test-diff.sh [OPTIONS] [COMMIT_RANGE]
#
# Options:
#   -h, --help          Show this help message
#   -d, --dry-run       Show which tests would run without executing
#   -v, --verbose       Show detailed output
#   -a, --all           Run all tests (useful for critical file changes)
#   --staged            Compare staged changes (git diff --cached)
#   --unstaged          Compare unstaged changes (git diff)
#   --committed         Compare last commit (git diff HEAD~1 HEAD) [default]
#
# Examples:
#   ./support/dev/test-diff.sh                    # Test last commit
#   ./support/dev/test-diff.sh --staged           # Test staged changes
#   ./support/dev/test-diff.sh --dry-run          # See which tests would run
#   ./support/dev/test-diff.sh HEAD~3 HEAD        # Test last 3 commits

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BUILD_DIR="$PROJECT_ROOT/build"
CONFIG_FILE="$PROJECT_ROOT/.test-diff.conf"

# Options
DRY_RUN=false
VERBOSE=false
RUN_ALL=false
DIFF_MODE="committed"
COMMIT_RANGE=""

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $*" >&2
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $*" >&2
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $*" >&2
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*" >&2
}

log_verbose() {
    if [[ "$VERBOSE" == true ]]; then
        echo -e "${BLUE}[DEBUG]${NC} $*" >&2
    fi
}

# Show usage
show_help() {
    sed -n '2,/^$/p' "$0" | sed 's/^# \?//'
    exit 0
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_help
                ;;
            -d|--dry-run)
                DRY_RUN=true
                shift
                ;;
            -v|--verbose)
                VERBOSE=true
                shift
                ;;
            -a|--all)
                RUN_ALL=true
                shift
                ;;
            --staged)
                DIFF_MODE="staged"
                shift
                ;;
            --unstaged)
                DIFF_MODE="unstaged"
                shift
                ;;
            --committed)
                DIFF_MODE="committed"
                shift
                ;;
            -*)
                log_error "Unknown option: $1"
                exit 1
                ;;
            *)
                # Assume it's a commit range
                COMMIT_RANGE="$1"
                shift
                if [[ $# -gt 0 && ! "$1" =~ ^- ]]; then
                    COMMIT_RANGE="$COMMIT_RANGE $1"
                    shift
                fi
                DIFF_MODE="range"
                ;;
        esac
    done
}

# Check prerequisites
check_prerequisites() {
    # Check if we're in a git repository
    if ! git rev-parse --git-dir > /dev/null 2>&1; then
        log_error "Not a git repository"
        exit 1
    fi

    # Check if build directory exists
    if [[ ! -d "$BUILD_DIR" ]]; then
        log_error "Build directory not found: $BUILD_DIR"
        log_error "Please run 'cmake -B build' first"
        exit 1
    fi

    # Check if ctest is available
    if ! command -v ctest &> /dev/null; then
        log_error "ctest not found. Please install CMake"
        exit 1
    fi

    # Check if jq is available (for JSON parsing)
    if ! command -v jq &> /dev/null; then
        log_error "jq not found. Please install jq for JSON parsing"
        log_error "  macOS: brew install jq"
        log_error "  Linux: apt-get install jq / yum install jq"
        exit 1
    fi
}

# Get list of changed files
get_changed_files() {
    local files=""

    case "$DIFF_MODE" in
        staged)
            log_verbose "Getting staged changes (git diff --cached --name-only)"
            files=$(git diff --cached --name-only)
            ;;
        unstaged)
            log_verbose "Getting unstaged changes (git diff --name-only)"
            files=$(git diff --name-only)
            ;;
        committed)
            log_verbose "Getting last commit changes (git diff --name-only HEAD~1 HEAD)"
            files=$(git diff --name-only HEAD~1 HEAD 2>/dev/null || echo "")
            if [[ -z "$files" ]]; then
                # Fallback to first commit
                files=$(git diff --name-only $(git rev-list --max-parents=0 HEAD) HEAD 2>/dev/null || echo "")
            fi
            ;;
        range)
            log_verbose "Getting changes in range: $COMMIT_RANGE"
            files=$(git diff --name-only $COMMIT_RANGE)
            ;;
    esac

    if [[ -z "$files" ]]; then
        log_warning "No changes detected"
        return 1
    fi

    echo "$files"
}

# Map source file to test executable name
# src/utils/string_utils.cpp -> string_utils_test
# src/server/http_server.cpp -> http_server_test
# tests/server/health_endpoint_test.cpp -> health_endpoint_test
map_source_to_test() {
    local source_file="$1"
    local base_name=$(basename "$source_file")

    # Remove extensions
    base_name=${base_name%.cpp}
    base_name=${base_name%.h}
    base_name=${base_name%.cc}
    base_name=${base_name%.hpp}

    # If already ends with _test, use as is
    if [[ "$base_name" =~ _test$ ]]; then
        echo "$base_name"
    else
        # Add _test suffix for source files
        echo "${base_name}_test"
    fi
}

# Check if a file affects critical components
is_critical_file() {
    local file="$1"

    # CMakeLists.txt changes affect all tests
    if [[ "$file" == *"CMakeLists.txt"* ]]; then
        return 0
    fi

    # Core utility changes affect many tests
    if [[ "$file" =~ ^src/utils/ ]]; then
        return 0
    fi

    # Configuration changes affect many tests
    if [[ "$file" =~ ^src/config/ ]] && [[ ! "$file" =~ _test\.cpp$ ]]; then
        return 0
    fi

    return 1
}

# Find tests that include a header file
find_tests_including_header() {
    local header_file="$1"
    local header_name=$(basename "$header_file")
    local test_executables=()

    log_verbose "Searching for tests including: $header_name"

    # Search in src/ and tests/ directories
    local including_files=$(grep -r --include="*.cpp" --include="*.h" \
        -l "#include.*$header_name" "$PROJECT_ROOT/src" "$PROJECT_ROOT/tests" 2>/dev/null || true)

    for file in $including_files; do
        if [[ "$file" =~ tests/.*_test\.cpp$ ]]; then
            local test_name=$(map_source_to_test "$file")
            test_executables+=("$test_name")
        fi
    done

    if [[ ${#test_executables[@]} -gt 0 ]]; then
        printf '%s\n' "${test_executables[@]}" | sort -u
    fi
}

# Load custom mappings from config file
load_custom_mappings() {
    if [[ -f "$CONFIG_FILE" ]]; then
        log_verbose "Loading custom mappings from: $CONFIG_FILE"
        source "$CONFIG_FILE"
    fi
}

# Find test executables for changed files
find_affected_tests() {
    local changed_files="$1"
    local test_executables=()
    local critical_change=false

    log_info "Analyzing changed files..."

    while IFS= read -r file; do
        [[ -z "$file" ]] && continue

        log_verbose "Analyzing: $file"

        # Check if critical file
        if is_critical_file "$file"; then
            log_warning "Critical file changed: $file"
            critical_change=true
            continue
        fi

        # Test file directly changed
        if [[ "$file" =~ tests/.*_test\.cpp$ ]]; then
            local test_name=$(map_source_to_test "$file")
            test_executables+=("$test_name")
            log_verbose "  -> Test file: $test_name"

        # Source file changed
        elif [[ "$file" =~ src/.*\.(cpp|cc)$ ]]; then
            local test_name=$(map_source_to_test "$file")
            test_executables+=("$test_name")
            log_verbose "  -> Source file: $test_name"

        # Header file changed
        elif [[ "$file" =~ \.(h|hpp)$ ]]; then
            log_verbose "  -> Header file, searching dependencies..."
            local header_tests=$(find_tests_including_header "$file")
            if [[ -n "$header_tests" ]]; then
                while IFS= read -r test; do
                    test_executables+=("$test")
                    log_verbose "     Found dependent test: $test"
                done <<< "$header_tests"
            else
                # Fallback: try to find corresponding test
                local test_name=$(map_source_to_test "$file")
                test_executables+=("$test_name")
                log_verbose "     Fallback to: $test_name"
            fi
        fi
    done <<< "$changed_files"

    # If critical change or --all flag, return empty (run all tests)
    if [[ "$critical_change" == true ]] || [[ "$RUN_ALL" == true ]]; then
        log_warning "Running ALL tests due to critical changes or --all flag"
        return 1
    fi

    # Remove duplicates and sort
    if [[ ${#test_executables[@]} -gt 0 ]]; then
        printf '%s\n' "${test_executables[@]}" | sort -u
    fi
}

# Verify test executables exist
verify_test_executables() {
    local test_list="$1"
    local verified_tests=()
    local missing_tests=()

    while IFS= read -r test_name; do
        [[ -z "$test_name" ]] && continue

        local test_path="$BUILD_DIR/bin/$test_name"
        if [[ -f "$test_path" ]]; then
            verified_tests+=("$test_name")
            log_verbose "Found test executable: $test_name"
        else
            missing_tests+=("$test_name")
            log_verbose "Test executable not found: $test_name"
        fi
    done <<< "$test_list"

    if [[ ${#missing_tests[@]} -gt 0 ]]; then
        log_warning "Some test executables not found (may not exist):"
        for test in "${missing_tests[@]}"; do
            log_warning "  - $test"
        done
    fi

    if [[ ${#verified_tests[@]} -gt 0 ]]; then
        printf '%s\n' "${verified_tests[@]}"
    fi
}

# Convert snake_case_test to PascalCaseTest
# health_endpoint_test -> HealthEndpointTest
convert_to_test_suite_name() {
    local test_name="$1"

    # Remove _test suffix
    test_name=${test_name%_test}

    # Convert to PascalCase using awk (portable across BSD and GNU)
    echo "$test_name" | awk -F_ '{
        for (i=1; i<=NF; i++) {
            printf "%s", toupper(substr($i,1,1)) substr($i,2)
        }
        printf "Test\n"
    }'
}

# Build test filter regex for ctest
build_test_filter() {
    local test_list="$1"
    local filter=""

    while IFS= read -r test_name; do
        [[ -z "$test_name" ]] && continue

        # Extract test suite name (remove _test suffix and convert to TestCase format)
        # string_utils_test -> StringUtilsTest
        local suite_name=$(convert_to_test_suite_name "$test_name")

        if [[ -z "$filter" ]]; then
            filter="^${suite_name}\\..*"
        else
            filter="${filter}|^${suite_name}\\..*"
        fi
    done <<< "$test_list"

    echo "$filter"
}

# Run tests
run_tests() {
    local filter="$1"

    cd "$BUILD_DIR"

    if [[ -z "$filter" ]]; then
        log_info "Running ALL tests..."
        if [[ "$DRY_RUN" == true ]]; then
            log_info "[DRY RUN] Would run: ctest --output-on-failure"
            ctest --show-only
        else
            ctest --output-on-failure
        fi
    else
        log_info "Running filtered tests..."
        if [[ "$DRY_RUN" == true ]]; then
            log_info "[DRY RUN] Would run: ctest --output-on-failure -R \"$filter\""
            ctest --show-only -R "$filter"
        else
            ctest --output-on-failure -R "$filter"
        fi
    fi
}

# Main execution
main() {
    parse_args "$@"

    log_info "Test Diff Runner - Running tests for changed files"
    log_info "=================================================="

    check_prerequisites
    load_custom_mappings

    # Get changed files
    local changed_files
    if ! changed_files=$(get_changed_files); then
        log_warning "No tests to run"
        exit 0
    fi

    log_info "Changed files (mode: $DIFF_MODE):"
    echo "$changed_files" | sed 's/^/  - /' >&2
    echo "" >&2

    # Find affected tests
    local affected_tests
    if ! affected_tests=$(find_affected_tests "$changed_files"); then
        # Critical change or --all flag, run all tests
        run_tests ""
        exit $?
    fi

    if [[ -z "$affected_tests" ]]; then
        log_warning "No matching tests found for changed files"
        log_info "Consider running 'make test' to run all tests"
        exit 0
    fi

    # Verify test executables exist
    affected_tests=$(verify_test_executables "$affected_tests")

    if [[ -z "$affected_tests" ]]; then
        log_warning "None of the expected test executables were found"
        exit 0
    fi

    log_info "Affected test executables:"
    echo "$affected_tests" | sed 's/^/  - /' >&2
    echo "" >&2

    # Build filter and run tests
    local filter=$(build_test_filter "$affected_tests")
    run_tests "$filter"

    local exit_code=$?

    if [[ $exit_code -eq 0 ]]; then
        log_success "All tests passed!"
    else
        log_error "Some tests failed"
    fi

    exit $exit_code
}

main "$@"
