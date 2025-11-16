# MygramDB Makefile
# Convenience wrapper for CMake build system

.PHONY: help build test test-parallel-full test-parallel-2 test-verbose test-sequential test-debug clean rebuild install uninstall format format-check lint configure run docker-build docker-up docker-down docker-logs docker-test

# Build directory
BUILD_DIR := build

# Install prefix (can be overridden: make PREFIX=/opt/mygramdb install)
PREFIX ?= /usr/local

# clang-format command (can be overridden: make CLANG_FORMAT=clang-format-18 format)
CLANG_FORMAT ?= clang-format

# Default target
.DEFAULT_GOAL := build

help:
	@echo "MygramDB Build System"
	@echo ""
	@echo "Available targets:"
	@echo "  make build     - Build the project (default)"
	@echo "  make test      - Run all tests (limited parallelism j=4, recommended)"
	@echo "  make test-parallel-2      - Run tests with j=2 (safer)"
	@echo "  make test-parallel-full   - Run tests with full parallelism (may hang)"
	@echo "  make test-verbose         - Run tests with verbose output (for debugging hangs)"
	@echo "  make test-sequential      - Run tests sequentially (for identifying hanging tests)"
	@echo "  make test-debug           - Run tests with debug output"
	@echo "  make clean     - Clean build directory"
	@echo "  make rebuild   - Clean and rebuild"
	@echo "  make install   - Install binaries and files"
	@echo "  make uninstall - Uninstall binaries and files"
	@echo "  make format       - Format code with clang-format"
	@echo "  make format-check - Check code formatting (CI)"
	@echo "  make lint         - Check code with clang-tidy"
	@echo "  make configure - Configure CMake (for changing options)"
	@echo "  make run       - Build and run mygramdb"
	@echo "  make help      - Show this help message"
	@echo ""
	@echo "Docker targets:"
	@echo "  make docker-build - Build Docker image"
	@echo "  make docker-up    - Start services with docker-compose"
	@echo "  make docker-down  - Stop services with docker-compose"
	@echo "  make docker-logs  - View docker-compose logs"
	@echo "  make docker-test  - Test Docker environment"
	@echo ""
	@echo "Examples:"
	@echo "  make                    # Build the project"
	@echo "  make test               # Run tests"
	@echo "  make test-sequential    # Run tests one at a time to identify hangs"
	@echo "  make install            # Install to $(PREFIX) (default: /usr/local)"
	@echo "  make PREFIX=/opt/mygramdb install  # Install to custom location"
	@echo "  make CMAKE_OPTIONS=\"-DENABLE_ASAN=ON\" configure  # Enable AddressSanitizer"
	@echo "  make CMAKE_OPTIONS=\"-DBUILD_TESTS=OFF\" configure # Disable tests"
	@echo "  make docker-up          # Start Docker environment"
	@echo "  make docker-logs        # View logs"

# Configure CMake
configure:
	@mkdir -p $(BUILD_DIR)
	cd $(BUILD_DIR) && cmake -DCMAKE_INSTALL_PREFIX=$(PREFIX) $(CMAKE_OPTIONS) ..

# Build the project
build: configure
	@echo "Building MygramDB..."
	$(MAKE) -C $(BUILD_DIR) -j$$(nproc)
	@echo "Build complete!"

# Run tests (limited parallelism to avoid resource conflicts)
test: build
	@echo "Running tests with limited parallelism (j=4)..."
	cd $(BUILD_DIR) && ctest --output-on-failure --parallel 4
	@echo "Tests complete!"

# Run tests with maximum parallelism (may hang due to resource conflicts)
test-parallel-full: build
	@echo "Running tests with full parallelism (j=$$(nproc))..."
	@echo "WARNING: This may hang due to resource conflicts"
	cd $(BUILD_DIR) && ctest --output-on-failure --parallel $$(nproc)
	@echo "Tests complete!"

# Run tests with minimal parallelism (safer)
test-parallel-2: build
	@echo "Running tests with minimal parallelism (j=2)..."
	cd $(BUILD_DIR) && ctest --output-on-failure --parallel 2
	@echo "Tests complete!"

# Run tests with verbose output to identify hanging tests
test-verbose: build
	@echo "Running tests with verbose output..."
	@echo "Press Ctrl+C if a test hangs to identify which one"
	cd $(BUILD_DIR) && ctest --verbose --parallel 4
	@echo "Tests complete!"

# Run tests sequentially with progress to identify hanging tests
test-sequential: build
	@echo "Running tests sequentially (no parallel execution)..."
	@echo "This will show which test is running when it hangs"
	cd $(BUILD_DIR) && ctest --verbose --output-on-failure
	@echo "Tests complete!"

# Run tests with debug output
test-debug: build
	@echo "Running tests with debug output..."
	cd $(BUILD_DIR) && ctest --debug --verbose --output-on-failure --parallel 4
	@echo "Tests complete!"

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
	@find src tests -name "*.cpp" -o -name "*.h" | xargs $(CLANG_FORMAT) -i
	@echo "Format complete!"

# Check code formatting (CI mode - fails on formatting issues)
format-check:
	@echo "Checking code formatting..."
	@find src tests -name "*.cpp" -o -name "*.h" | xargs $(CLANG_FORMAT) --dry-run --Werror
	@echo "Format check passed!"

# Check code with clang-tidy
lint: build
	@bash scripts/run-clang-tidy.sh

# Build and run mygramdb
run: build
	@echo "Running MygramDB..."
	$(BUILD_DIR)/bin/mygramdb

# Quick test (useful during development)
quick-test: build
	@echo "Running quick test..."
	cd $(BUILD_DIR) && ctest --output-on-failure --parallel $$(nproc) -R "StringUtils|Config"

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
