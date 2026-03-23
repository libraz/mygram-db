# MygramDB Makefile
# Convenience wrapper for CMake build system

.PHONY: help build test test-fast test-slow test-load test-all test-full test-sequential test-verbose clean rebuild install uninstall format format-check lint lint-diff lint-diff-main configure run e2e-test e2e-test-smoke e2e-test-load e2e-test-cleanup e2e-lint e2e-format e2e-fix e2e-benchmark docker-build docker-up docker-down docker-logs docker-test bench-up bench-down bench-logs docker-dev-build docker-dev-shell docker-build-linux docker-test-linux docker-lint-linux docker-lint-diff-linux docker-format-check-linux docker-clean-linux docker-ci-check

# Build directory
BUILD_DIR := build

# Install prefix (can be overridden: make PREFIX=/opt/mygramdb install)
PREFIX ?= /usr/local

# clang-format command (can be overridden: make CLANG_FORMAT=clang-format-18 format)
CLANG_FORMAT ?= clang-format

# Test options (can be overridden)
TEST_JOBS ?= 4          # Parallel jobs for tests (make test TEST_JOBS=2)
TEST_VERBOSE ?= 0       # Verbose output (make test TEST_VERBOSE=1)
TEST_DEBUG ?= 0         # Debug output (make test TEST_DEBUG=1)

# MySQL password for benchmark (can be overridden: make MYSQL_PASSWORD=secret bench-run)
MYSQL_PASSWORD ?= mygramdb

# Default target
.DEFAULT_GOAL := build

help:
	@echo "MygramDB Build System"
	@echo ""
	@echo "Available targets:"
	@echo "  make build          - Build the project (default)"
	@echo "  make test           - Run tests excluding SLOW (configurable with TEST_JOBS, TEST_VERBOSE, TEST_DEBUG)"
	@echo "  make test-fast      - Run tests excluding SLOW label (same as test, explicit name)"
	@echo "  make test-slow      - Run only SLOW-labeled tests"
	@echo "  make test-all       - Run ALL tests including SLOW"
	@echo "  make test-full      - Run all tests with full parallelism (same as TEST_JOBS=\$$(nproc))"
	@echo "  make test-sequential - Run tests sequentially (same as TEST_JOBS=1)"
	@echo "  make test-verbose   - Run tests with verbose output (same as TEST_VERBOSE=1)"
	@echo "  make clean          - Clean build directory"
	@echo "  make rebuild        - Clean and rebuild"
	@echo "  make install        - Install binaries and files"
	@echo "  make uninstall      - Uninstall binaries and files"
	@echo "  make format         - Format code with clang-format"
	@echo "  make format-check   - Check code formatting (CI)"
	@echo "  make lint           - Check all code with clang-tidy"
	@echo "  make lint-diff      - Check only changed files with clang-tidy (fast)"
	@echo "  make lint-diff-main - Check files changed from main branch (recommended for CI)"
	@echo "  make configure      - Configure CMake (for changing options)"
	@echo "  make run            - Build and run mygramdb"
	@echo "  make help           - Show this help message"
	@echo ""
	@echo "Test options (override with environment variables):"
	@echo "  TEST_JOBS=N        - Number of parallel test jobs (default: 4, use 1 for sequential)"
	@echo "  TEST_VERBOSE=1     - Enable verbose test output"
	@echo "  TEST_DEBUG=1       - Enable debug test output"
	@echo ""
	@echo "E2E integration tests (MySQL in Docker, MygramDB native):"
	@echo "  make e2e-test         - Run full E2E test suite"
	@echo "  make e2e-test-smoke   - Run only smoke tests"
	@echo "  make e2e-test-load    - Run only load tests"
	@echo "  make e2e-test-cleanup - Clean up E2E test environment"
	@echo "  make e2e-lint         - Lint E2E Python code"
	@echo "  make e2e-format       - Format E2E Python code"
	@echo "  make e2e-fix          - Fix E2E Python code"
	@echo "  make e2e-benchmark            - Quick MygramDB vs MySQL comparison"
	@echo "  make e2e-benchmark-full       - Full benchmark suite with JSON output"
	@echo "  make e2e-benchmark-saturation - Saturation test (find QPS ceiling)"
	@echo ""
	@echo "Docker targets:"
	@echo "  make docker-build - Build Docker image"
	@echo "  make docker-up    - Start services with docker-compose"
	@echo "  make docker-down  - Stop services with docker-compose"
	@echo "  make docker-logs  - View docker-compose logs"
	@echo "  make docker-test  - Test Docker environment"
	@echo ""
	@echo "Benchmark targets (1.1M Wikipedia dataset):"
	@echo "  make bench-up     - Start benchmark environment (downloads 203MB seed)"
	@echo "  make bench-down   - Stop benchmark environment and remove volumes"
	@echo "  make bench-logs   - View MySQL logs (monitor seed loading progress)"
	@echo ""
	@echo "Linux CI testing (Docker-based):"
	@echo "  make docker-dev-build       - Build Linux development image"
	@echo "  make docker-dev-shell       - Interactive shell in Linux container"
	@echo "  make docker-build-linux     - Build project in Linux container"
	@echo "  make docker-test-linux      - Run tests in Linux container"
	@echo "  make docker-lint-linux      - Run clang-tidy on all files in Linux container"
	@echo "  make docker-lint-diff-linux - Run clang-tidy on changed files only (fast, recommended)"
	@echo "  make docker-format-check-linux - Check formatting in Linux container"
	@echo "  make docker-clean-linux     - Clean build in Linux container"
	@echo "  make docker-ci-check        - Run full CI checks (format + build + lint + test)"
	@echo ""
	@echo "Examples:"
	@echo "  make                                  # Build the project"
	@echo "  make test                             # Run tests (j=4, default)"
	@echo "  make test-full                        # Run tests with full parallelism"
	@echo "  make test-sequential                  # Run tests sequentially"
	@echo "  make test-verbose                     # Run tests with verbose output"
	@echo "  make test TEST_JOBS=2                 # Run tests with 2 parallel jobs (custom)"
	@echo "  make test TEST_JOBS=1 TEST_VERBOSE=1  # Sequential verbose tests (combined)"
	@echo "  make install                          # Install to $(PREFIX) (default: /usr/local)"
	@echo "  make PREFIX=/opt/mygramdb install     # Install to custom location"
	@echo "  make CMAKE_OPTIONS=\"-DENABLE_ASAN=ON\" configure  # Enable AddressSanitizer"
	@echo "  make CMAKE_OPTIONS=\"-DBUILD_TESTS=OFF\" configure # Disable tests"
	@echo "  make docker-up                        # Start Docker environment"
	@echo "  make docker-logs                      # View logs"
	@echo "  make docker-ci-check                  # Run all CI checks in Linux (before git push)"
	@echo "  make docker-build-linux               # Test Linux build locally"

# Configure CMake
configure:
	@mkdir -p $(BUILD_DIR)
	cd $(BUILD_DIR) && cmake -DCMAKE_INSTALL_PREFIX=$(PREFIX) $(CMAKE_OPTIONS) ..

# Build the project
build: configure
	@echo "Building MygramDB..."
	$(MAKE) -C $(BUILD_DIR) -j$$(nproc)
	@echo "Build complete!"

# Run tests with configurable options (excludes SLOW tests by default)
test: build
	@echo "Running tests (jobs=$(TEST_JOBS), verbose=$(TEST_VERBOSE), debug=$(TEST_DEBUG))..."
	@echo "Note: Excluding SLOW tests. Use 'make test-all' to run all tests."
	@if [ "$(TEST_JOBS)" = "1" ]; then \
		echo "Running tests sequentially..."; \
	fi
	@cd $(BUILD_DIR) && \
		CTEST_FLAGS="--output-on-failure --parallel $(TEST_JOBS) --label-exclude SLOW"; \
		if [ "$(TEST_VERBOSE)" = "1" ]; then CTEST_FLAGS="$$CTEST_FLAGS --verbose"; fi; \
		if [ "$(TEST_DEBUG)" = "1" ]; then CTEST_FLAGS="$$CTEST_FLAGS --debug"; fi; \
		ctest $$CTEST_FLAGS
	@echo "Tests complete!"

# Run ALL tests including SLOW tests
test-all: build
	@echo "Running ALL tests including SLOW tests (jobs=$(TEST_JOBS))..."
	@cd $(BUILD_DIR) && \
		CTEST_FLAGS="--output-on-failure --parallel $(TEST_JOBS)"; \
		if [ "$(TEST_VERBOSE)" = "1" ]; then CTEST_FLAGS="$$CTEST_FLAGS --verbose"; fi; \
		if [ "$(TEST_DEBUG)" = "1" ]; then CTEST_FLAGS="$$CTEST_FLAGS --debug"; fi; \
		ctest $$CTEST_FLAGS
	@echo "All tests complete!"

# Run tests excluding SLOW and LOAD labels (fast feedback during development)
test-fast: build
	@echo "Running fast tests (excluding SLOW/LOAD, jobs=$(TEST_JOBS))..."
	@cd $(BUILD_DIR) && ctest -LE "SLOW|LOAD" --output-on-failure --parallel $(TEST_JOBS)
	@echo "Fast tests complete!"

# Run SLOW tests (concurrency/thread-safety unit tests, ~1-2 min total)
test-slow: build
	@echo "Running SLOW tests (jobs=$(TEST_JOBS))..."
	@cd $(BUILD_DIR) && ctest -L SLOW -LE LOAD --output-on-failure --parallel $(TEST_JOBS)
	@echo "SLOW tests complete!"

# Run LOAD tests (load/stress/benchmark tests, ~5-10 min total)
test-load: build
	@echo "Running LOAD tests..."
	@cd $(BUILD_DIR) && ctest -L LOAD --output-on-failure
	@echo "LOAD tests complete!"

# Convenience aliases for common test scenarios
test-full:
	@$(MAKE) test-all TEST_JOBS=$$(nproc)

test-sequential:
	@$(MAKE) test TEST_JOBS=1

test-verbose:
	@$(MAKE) test TEST_VERBOSE=1

# Clean build directory
clean:
	@echo "Cleaning build directory..."
	rm -rf $(BUILD_DIR)
	@echo "Clean complete!"

# Rebuild from scratch
rebuild: clean build

# Install binaries and files
install: build
	@echo "Installing MygramDB to $(PREFIX)..."
	$(MAKE) -C $(BUILD_DIR) install
	@echo ""
	@echo "Installation complete!"
	@echo "  Binaries:      $(PREFIX)/bin/mygramdb, $(PREFIX)/bin/mygram-cli"
	@echo "  Config examples:"
	@echo "    - $(PREFIX)/etc/mygramdb/config.yaml.example (full options)"
	@echo "    - $(PREFIX)/etc/mygramdb/config-minimal.yaml (minimal setup)"
	@echo "  Documentation: $(PREFIX)/share/doc/mygramdb/"
	@echo ""
	@echo "To run: $(PREFIX)/bin/mygramdb -c config.yaml"

# Uninstall
uninstall:
	@echo "Uninstalling MygramDB from $(PREFIX)..."
	rm -f $(PREFIX)/bin/mygramdb
	rm -f $(PREFIX)/bin/mygram-cli
	rm -rf $(PREFIX)/etc/mygramdb
	rm -rf $(PREFIX)/share/doc/mygramdb
	@echo "Uninstall complete!"

# Format code with clang-format
format:
	@echo "Formatting code..."
	@find src tests -type f \( -name "*.cpp" -o -name "*.h" \) ! -path "*/build/*" | xargs $(CLANG_FORMAT) -i
	@echo "Format complete!"

# Check code formatting (CI mode - fails on formatting issues)
format-check:
	@echo "Checking code formatting..."
	@find src tests -type f \( -name "*.cpp" -o -name "*.h" \) ! -path "*/build/*" | xargs $(CLANG_FORMAT) --dry-run --Werror
	@echo "Format check passed!"

# Check code with clang-tidy
lint: format build
	@bash support/dev/run-clang-tidy.sh

# Check only changed files with clang-tidy (fast)
lint-diff: format build
	@bash support/dev/run-clang-tidy.sh --diff

# Check files changed from main branch with clang-tidy
lint-diff-main: format build
	@bash support/dev/run-clang-tidy.sh --diff main

# Build and run mygramdb
run: build
	@echo "Running MygramDB..."
	$(BUILD_DIR)/bin/mygramdb

# Quick test (useful during development)
quick-test: build
	@echo "Running quick test..."
	cd $(BUILD_DIR) && ctest --output-on-failure --parallel $$(nproc) -R "StringUtils|Config"

# ============================================================================
# E2E Integration Tests
# ============================================================================

.PHONY: e2e-test e2e-test-smoke e2e-test-load e2e-test-cleanup e2e-lint e2e-format e2e-fix e2e-benchmark e2e-benchmark-full e2e-benchmark-saturation

# Run full e2e test suite (requires Docker)
e2e-test:
	@echo "Running E2E test suite..."
	@bash e2e/run-all.sh

# Run only smoke tests
e2e-test-smoke:
	@echo "Running E2E smoke tests..."
	@bash e2e/run-all.sh -m smoke

# Run only load tests
e2e-test-load:
	@echo "Running E2E load tests..."
	@bash e2e/run-all.sh -m load

# Clean up e2e test environment
e2e-test-cleanup:
	@echo "Cleaning up E2E test environment..."
	docker compose -f e2e/docker/docker-compose.yml down -v 2>/dev/null || true
	@echo "E2E cleanup complete!"

# Lint e2e Python code
e2e-lint:
	@echo "Linting E2E Python code..."
	cd e2e && ruff check . && mypy .

# Format e2e Python code
e2e-format:
	@echo "Formatting E2E Python code..."
	cd e2e && ruff format .

# Fix e2e Python code
e2e-fix:
	@echo "Fixing E2E Python code..."
	cd e2e && ruff check --fix . && ruff format .

# Benchmark suite: quick comparison (1, 4, 16 concurrency x 5s)
e2e-benchmark:
	@echo "Running benchmark suite (quick mode)..."
	cd e2e && python benchmark_suite.py --mode quick --compare

# Benchmark suite: full comparison (7 levels x 15s)
e2e-benchmark-full:
	@echo "Running full benchmark suite..."
	cd e2e && python benchmark_suite.py --mode standard --compare --json-output results/benchmark.json

# Benchmark suite: saturation test
e2e-benchmark-saturation:
	@echo "Running saturation benchmark..."
	cd e2e && python benchmark_suite.py --mode saturation --target mygramdb

# Docker targets
docker-build:
	@echo "Building Docker image..."
	docker build -t mygramdb:latest .
	@echo "Docker image built successfully!"

docker-up:
	@echo "Starting Docker services..."
	docker-compose up -d
	@echo "Services started!"
	@echo "Run 'make docker-logs' to view logs"

docker-down:
	@echo "Stopping Docker services..."
	docker-compose down
	@echo "Services stopped!"

docker-logs:
	@echo "Viewing Docker logs (Ctrl+C to exit)..."
	docker-compose logs -f

docker-test:
	@echo "Testing Docker environment..."
	@echo ""
	@echo "1. Checking if .env file exists..."
	@if [ ! -f .env ]; then \
		echo "   WARNING: .env file not found. Copying from .env.example..."; \
		cp .env.example .env; \
		echo "   Please edit .env and update passwords before running docker-up"; \
	else \
		echo "   OK: .env file found"; \
	fi
	@echo ""
	@echo "2. Building Docker image..."
	docker build -t mygramdb:test .
	@echo ""
	@echo "3. Testing entrypoint script..."
	docker run --rm -e MYSQL_HOST=testhost mygramdb:test test-config || echo "   Config test completed (expected to fail without MySQL)"
	@echo ""
	@echo "Docker environment test completed!"
	@echo "Run 'make docker-up' to start the full environment"

# ============================================================================
# Benchmark Environment (1.1M Wikipedia dataset)
# ============================================================================

bench-up:
	@docker compose version >/dev/null 2>&1 || { echo "Error: 'docker compose' is required. Install Docker Desktop: https://docs.docker.com/get-docker/"; exit 1; }
	@echo "Starting benchmark environment (1.1M Wikipedia dataset)..."
	@echo "First run downloads a 225MB seed file. This may take a few minutes."
	@echo ""
	docker compose -f docker-compose.bench.yml up -d
	@echo ""
	@echo "MySQL is loading seed data in the background."
	@echo "Run 'make bench-logs' to monitor progress."

bench-run:
	@command -v uv >/dev/null 2>&1 || { echo "Error: 'uv' is required. Install via: curl -LsSf https://astral.sh/uv/install.sh | sh"; exit 1; }
	@echo "Running benchmark (MygramDB vs MySQL FULLTEXT)..."
	uv run --with mysql-connector-python python support/seed/benchmark.py \
		--mysql-host 127.0.0.1 --mysql-port 3306 \
		--mysql-user root --mysql-password $(MYSQL_PASSWORD) --mysql-db mydb \
		--mygramdb-host 127.0.0.1 --mygramdb-port 11016

bench-down:
	@echo "Stopping benchmark environment..."
	docker compose -f docker-compose.bench.yml down -v
	@echo "Benchmark environment stopped and volumes removed."

bench-logs:
	docker compose -f docker-compose.bench.yml logs -f mysql

# ============================================================================
# Linux CI Testing (Docker-based)
# ============================================================================
# These targets allow you to test your code in the same Linux environment
# used by GitHub Actions CI, catching platform-specific issues before pushing.

# Image name for development container
DOCKER_DEV_IMAGE := mygramdb-dev:latest

# Build the Linux development Docker image
docker-dev-build:
	@echo "Building Linux development Docker image..."
	@echo "This may take a few minutes on first run (installing LLVM, etc.)"
	docker build -f support/dev/Dockerfile -t $(DOCKER_DEV_IMAGE) .
	@echo ""
	@echo "Development image built successfully!"
	@echo "Run 'make docker-dev-shell' to start an interactive shell"
	@echo "Run 'make docker-ci-check' to run full CI checks"

# Start an interactive shell in the Linux development container
docker-dev-shell: docker-dev-build
	@echo "Starting interactive shell in Linux development container..."
	@echo "Type 'exit' to leave the container"
	docker run --rm -it -v $$(pwd):/workspace -w /workspace $(DOCKER_DEV_IMAGE) bash

# Build the project in Linux container (mimics CI environment)
docker-build-linux: docker-dev-build
	@echo "Building project in Linux container..."
	docker run --rm -v $$(pwd):/workspace -w /workspace $(DOCKER_DEV_IMAGE) \
		bash -c "mkdir -p build && cd build && \
		cmake -DCMAKE_BUILD_TYPE=Debug \
		      -DBUILD_TESTS=ON \
		      -DENABLE_COVERAGE=ON \
		      -DUSE_ICU=ON \
		      -DUSE_MYSQL=ON \
		      .. && \
		make -j\$$(nproc)"
	@echo "Linux build completed successfully!"

# Run tests in Linux container
docker-test-linux: docker-build-linux
	@echo "Running tests in Linux container..."
	docker run --rm -v $$(pwd):/workspace -w /workspace $(DOCKER_DEV_IMAGE) \
		bash -c "cd build && ctest --output-on-failure --parallel \$$(nproc)"
	@echo "Linux tests completed successfully!"

# Run clang-tidy in Linux container
docker-lint-linux: docker-build-linux
	@echo "Running clang-tidy in Linux container..."
	docker run --rm -v $$(pwd):/workspace -w /workspace $(DOCKER_DEV_IMAGE) \
		bash -c "bash support/dev/run-clang-tidy.sh"
	@echo "Linux lint completed successfully!"

# Run clang-tidy on changed files only in Linux container (fast)
docker-lint-diff-linux: docker-build-linux
	@echo "Running clang-tidy (diff mode) in Linux container..."
	docker run --rm -v $$(pwd):/workspace -w /workspace $(DOCKER_DEV_IMAGE) \
		bash -c "bash support/dev/run-clang-tidy.sh --diff main"
	@echo "Linux lint (diff) completed successfully!"

# Check code formatting in Linux container
docker-format-check-linux: docker-dev-build
	@echo "Checking code formatting in Linux container..."
	docker run --rm -v $$(pwd):/workspace -w /workspace $(DOCKER_DEV_IMAGE) \
		bash -c "find src tests -type f \( -name '*.cpp' -o -name '*.h' \) ! -path '*/build/*' | xargs clang-format --dry-run --Werror"
	@echo "Format check passed!"

# Clean build directory in Linux container
docker-clean-linux:
	@echo "Cleaning build directory in Linux container..."
	docker run --rm -v $$(pwd):/workspace -w /workspace $(DOCKER_DEV_IMAGE) \
		bash -c "rm -rf build"
	@echo "Clean completed!"

# Run full CI checks (format + build + lint + test) - recommended before git push
docker-ci-check: docker-dev-build
	@echo "=========================================="
	@echo "Running full CI checks in Linux container"
	@echo "=========================================="
	@echo ""
	@echo "Step 1/4: Checking code formatting..."
	@$(MAKE) docker-format-check-linux
	@echo ""
	@echo "Step 2/4: Building project..."
	@$(MAKE) docker-build-linux
	@echo ""
	@echo "Step 3/4: Running clang-tidy..."
	@$(MAKE) docker-lint-linux
	@echo ""
	@echo "Step 4/4: Running tests..."
	@$(MAKE) docker-test-linux
	@echo ""
	@echo "=========================================="
	@echo "✓ All CI checks passed!"
	@echo "=========================================="
	@echo "Your code should pass GitHub Actions CI"
